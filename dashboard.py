import subprocess
import sys
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import text, func, select

import backtest
import database as db
import signals as signals_module
from config import INSTRUMENTS

NOTE_FILE = Path("outputs/notes/weekly_note.md")

DISPLAY_MAP = {ticker: info["display_name"] for ticker, info in INSTRUMENTS.items()}
DISPLAY_TO_TICKER = {v: k for k, v in DISPLAY_MAP.items()}
TICKERS = list(INSTRUMENTS.keys())
DEFAULT_SELECTION = list(DISPLAY_MAP.values())


def get_close_prices(conn):
    stmt = db.daily_prices.select().where(db.daily_prices.c.ticker.in_(TICKERS)).order_by(db.daily_prices.c.date)
    df = pd.read_sql(stmt, conn)
    if df.empty:
        return pd.DataFrame()
    df["date"] = pd.to_datetime(df["date"])
    return df.pivot(index="date", columns="ticker", values="close")


def get_signals(conn):
    stmt = db.signal_events.select().order_by(db.signal_events.c.date)
    df = pd.read_sql(stmt, conn)
    if df.empty:
        return pd.DataFrame()
    df["date"] = pd.to_datetime(df["date"])
    return df


def get_latest_metrics(conn):
    close_df = get_close_prices(conn)
    latest = {}
    weekly = {}
    for ticker in TICKERS:
        if ticker not in close_df.columns:
            latest[ticker] = None
            weekly[ticker] = None
            continue
        series = close_df[ticker].dropna()
        latest[ticker] = series.iloc[-1] if len(series) else None
        if len(series) >= 6:
            weekly[ticker] = series.iloc[-1] / series.iloc[-6] - 1
        else:
            weekly[ticker] = None
    return latest, weekly


BACKTEST_RESULTS_FILE = Path("outputs/notes/backtest_results.md")

def get_database_stats(conn):
    total_rows = conn.execute(text("SELECT COUNT(*) FROM daily_prices")).scalar()
    last_date = conn.execute(text("SELECT MAX(date) FROM daily_prices")).scalar()
    signal_count = conn.execute(text("SELECT COUNT(*) FROM signal_events")).scalar()
    return total_rows, last_date, signal_count


def load_backtest_limitations():
    if not BACKTEST_RESULTS_FILE.exists():
        return []
    lines = BACKTEST_RESULTS_FILE.read_text().splitlines()
    output = []
    capture = False
    for line in lines:
        if line.startswith("## Limitations"):
            capture = True
            continue
        if capture:
            if line.startswith("## ") and not line.startswith("## Limitations"):
                break
            output.append(line)
    return output


def run_refresh():
    subprocess.run([sys.executable, "run_all.py"], check=True)


def build_normalised_chart(close_df, selected_tickers, date_range):
    if close_df.empty or not selected_tickers:
        return None
    df = close_df[selected_tickers].copy()
    df = df.loc[pd.to_datetime(date_range[0]) : pd.to_datetime(date_range[1])]
    rebased = df.divide(df.iloc[0]).multiply(100)
    rebased = rebased.rename(columns=DISPLAY_MAP)
    fig = px.line(rebased, template="plotly_dark", labels={"value": "Rebased price", "date": "Date", "variable": "Instrument"})
    fig.update_layout(title="Normalised Price Series — Rebased to 100")
    return fig


def build_spread_chart(close_df, signal_df, date_range):
    df = close_df[["BZ=F", "CL=F"]].copy()
    df = df.loc[pd.to_datetime(date_range[0]) : pd.to_datetime(date_range[1])]
    df = df.dropna()
    if df.empty:
        return None
    spread = df["BZ=F"] - df["CL=F"]
    mean = spread.mean()
    std = spread.std(ddof=0)
    chart_df = pd.DataFrame({"date": spread.index, "value": spread.values})
    fig = px.line(chart_df, x="date", y="value", template="plotly_dark", labels={"value": "Spread (USD)", "date": "Date"})
    fig.add_hline(y=mean, line_dash="dash", line_color="white", annotation_text="Mean")
    fig.add_hline(y=mean + 2 * std, line_dash="dash", line_color="yellow", annotation_text="Mean +2σ")
    fig.add_hline(y=mean - 2 * std, line_dash="dash", line_color="yellow", annotation_text="Mean -2σ")
    if not signal_df.empty:
        signals = signal_df[signal_df["signal_type"] == "SPREAD_DISLOCATION"]
        signals = signals[(signals["date"] >= pd.to_datetime(date_range[0])) & (signals["date"] <= pd.to_datetime(date_range[1]))]
        if not signals.empty:
            signal_dates = signals["date"]
            signal_values = spread.reindex(signal_dates).dropna()
            fig.add_scatter(x=signal_values.index, y=signal_values.values, mode="markers", marker=dict(color="red", size=8), name="SPREAD_DISLOCATION")
    fig.update_layout(title="Brent-WTI Spread with Dislocation Signals")
    return fig


def build_volatility_chart(conn, date_range):
    stmt = db.rolling_metrics.select().where(
        db.rolling_metrics.c.metric_name == "30d_annualised_volatility",
        db.rolling_metrics.c.ticker.in_(["BZ=F", "WDS.AX", "STO.AX"]),
    ).order_by(db.rolling_metrics.c.date)
    df = pd.read_sql(stmt, conn)
    if df.empty:
        return None
    df["date"] = pd.to_datetime(df["date"])
    df = df[(df["date"] >= pd.to_datetime(date_range[0])) & (df["date"] <= pd.to_datetime(date_range[1]))]
    fig = px.line(df, x="date", y="metric_value", color="ticker", template="plotly_dark", labels={"metric_value": "Volatility", "date": "Date", "ticker": "Instrument"})
    fig.update_traces(mode="lines")
    fig.update_layout(title="Rolling 30-Day Annualised Volatility (%)")
    fig.update_yaxes(ticksuffix="%")
    return fig


def build_correlation_chart(conn, date_range):
    stmt = db.rolling_metrics.select().where(
        db.rolling_metrics.c.metric_name == "60d_corr_with_BZ",
        db.rolling_metrics.c.ticker.in_(["WDS.AX", "STO.AX"]),
    ).order_by(db.rolling_metrics.c.date)
    df = pd.read_sql(stmt, conn)
    if df.empty:
        return None
    df["date"] = pd.to_datetime(df["date"])
    df = df[(df["date"] >= pd.to_datetime(date_range[0])) & (df["date"] <= pd.to_datetime(date_range[1]))]
    fig = px.line(df, x="date", y="metric_value", color="ticker", template="plotly_dark", labels={"metric_value": "Correlation", "date": "Date", "ticker": "Instrument"})
    fig.add_hline(y=0.3, line_dash="dash", line_color="yellow", annotation_text="0.3 threshold")
    fig.update_layout(title="Rolling 60-Day Correlation — Brent vs ASX Energy")
    return fig


def build_dual_axis_chart(close_df, date_range):
    df = close_df[["BZ=F", "AUDUSD=X"]].copy()
    df = df.loc[pd.to_datetime(date_range[0]) : pd.to_datetime(date_range[1])]
    df = df.dropna()
    if df.empty:
        return None
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df.index, y=df["BZ=F"], mode="lines", name="Brent Crude"))
    fig.add_trace(go.Scatter(x=df.index, y=df["AUDUSD=X"], mode="lines", name="AUD/USD", yaxis="y2"))
    fig.update_layout(template="plotly_dark", title="Brent Crude vs AUD/USD", xaxis_title="Date", yaxis_title="Brent Close", yaxis2=dict(title="AUD/USD", overlaying="y", side="right"))
    return fig


def build_signal_timeline(signal_df, date_range):
    if signal_df.empty:
        return None
    chart_df = signal_df.copy()
    chart_df = chart_df[(chart_df["date"] >= pd.to_datetime(date_range[0])) & (chart_df["date"] <= pd.to_datetime(date_range[1]))]
    if chart_df.empty:
        return None
    mapping = {
        "SPREAD_DISLOCATION": 0,
        "VOLATILITY_SPIKE": 1,
        "CORRELATION_BREAKDOWN": 2,
        "FX_DISLOCATION": 3,
    }
    chart_df["signal_y"] = chart_df["signal_type"].map(mapping)
    fig = px.scatter(chart_df, x="date", y="signal_y", color="signal_type", template="plotly_dark", labels={"date": "Date", "signal_y": "Signal", "signal_type": "Signal"}, hover_data=["signal_value", "market_context"])
    fig.update_yaxes(tickmode="array", tickvals=list(mapping.values()), ticktext=list(mapping.keys()))
    fig.update_layout(title="Signal Event Timeline")
    return fig


def format_price(value):
    return f"{value:,.2f}" if value is not None else "N/A"


def format_change(value):
    if value is None or pd.isna(value):
        return "N/A"
    return f"{value * 100:+.2f}%"


def build_sidebar(close_df):
    st.sidebar.header("Controls")
    if close_df.empty:
        return date.today(), date.today(), DEFAULT_SELECTION
    min_date = close_df.index.min().date()
    max_date = close_df.index.max().date()
    start_date = st.sidebar.date_input("Start date", min_date, min_value=min_date, max_value=max_date)
    end_date = st.sidebar.date_input("Last data date", max_date, min_value=min_date, max_value=max_date)
    st.sidebar.caption("Data updates each trading day when pipeline is run")
    if start_date > end_date:
        start_date, end_date = end_date, start_date
    selected = st.sidebar.multiselect("Show instruments", DEFAULT_SELECTION, default=DEFAULT_SELECTION)
    with db.get_connection() as stats_conn:
        total_rows = stats_conn.execute(text("SELECT COUNT(*) FROM daily_prices")).scalar()
        signal_count = stats_conn.execute(text("SELECT COUNT(*) FROM signal_events")).scalar()
        last_date = stats_conn.execute(text("SELECT MAX(date) FROM daily_prices")).scalar()
    st.sidebar.markdown("---")
    st.sidebar.subheader("Database stats")
    st.sidebar.metric("Daily prices rows", f"{total_rows}")
    st.sidebar.metric("Last data date", last_date if last_date is not None else "N/A")
    st.sidebar.metric("Signals fired", f"{signal_count}")
    return start_date, end_date, selected


def main():
    st.set_page_config(page_title="Australian Energy & Commodity Monitor", layout="wide")
    st.title("Australian Energy & Commodity Monitor")
    st.markdown("#### Oil, FX and ASX Energy Equities — Signal & Spread Analysis")

    if "last_refreshed" not in st.session_state:
        st.session_state["last_refreshed"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    col1, col2 = st.columns([3, 1])
    with col1:
        st.write(f"**Date:** {date.today().isoformat()}")
        st.write(f"**Last refreshed:** {st.session_state['last_refreshed']}")
    with col2:
        if st.button("Refresh Data"):
            with st.spinner("Refreshing full pipeline..."):
                try:
                    run_refresh()
                    st.session_state["last_refreshed"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    st.success("Data refresh complete.")
                except subprocess.CalledProcessError as exc:
                    st.error(f"Refresh failed: {exc}")

    with db.get_connection() as conn:
        close_df = get_close_prices(conn)
        signal_df = get_signals(conn)
        total_rows, last_date, signal_count = get_database_stats(conn)

    start_date, end_date, selected = build_sidebar(close_df)
    selected_tickers = [DISPLAY_TO_TICKER[name] for name in selected if name in DISPLAY_TO_TICKER]
    latest, weekly = get_latest_metrics(db.get_connection())

    st.markdown("---")
    st.subheader("Market snapshot")
    metrics = ["BZ=F", "CL=F", "AUDUSD=X", "WDS.AX", "STO.AX"]
    cols = st.columns(5)
    for col, ticker in zip(cols, metrics):
        with col:
            value = latest.get(ticker)
            delta = weekly.get(ticker)
            col.metric(DISPLAY_MAP[ticker], format_price(value), format_change(delta))

    st.markdown("---")
    st.subheader("Signal status")
    signal_info = {
        "SPREAD_DISLOCATION": "Spread dislocation may indicate pipeline constraints, storage stress or export arbitrage opportunity.",
        "VOLATILITY_SPIKE": "Volatility spikes often precede mean reversion and hedging flows.",
        "CORRELATION_BREAKDOWN": "Correlation breakdown suggests company-specific risk decoupling from commodity driver.",
        "FX_DISLOCATION": "FX dislocation vs commodities may signal AUD-denominated equity setups.",
    }
    recent_threshold = datetime.now() - pd.Timedelta(days=7)
    badges = st.columns(4)
    for idx, signal_name in enumerate(signal_info.keys()):
        with badges[idx]:
            recent = signal_df[signal_df["signal_type"] == signal_name]
            recent = recent[recent["date"] >= recent_threshold]
            if not recent.empty:
                latest_row = recent.sort_values("date").iloc[-1]
                st.markdown(
                    f"<div style='background:#b30000;padding:12px;border-radius:8px;color:white;'>**{signal_name}**<br>{latest_row['signal_value']:.3f}</div>",
                    unsafe_allow_html=True,
                )
            else:
                st.markdown(
                    f"<div style='background:#0d6d00;padding:12px;border-radius:8px;color:white;'>**{signal_name}**<br>Normal</div>",
                    unsafe_allow_html=True,
                )
            st.markdown(f"<small>{signal_info[signal_name]}</small>", unsafe_allow_html=True)

    st.markdown("---")
    st.subheader("Charts")
    if close_df.empty:
        st.warning("No price data available to render charts.")
        return
    date_range = (start_date, end_date)
    chart1 = build_normalised_chart(close_df, selected_tickers, date_range)
    chart2 = build_spread_chart(close_df, signal_df, date_range)
    chart3 = build_volatility_chart(db.get_connection(), date_range)
    chart4 = build_correlation_chart(db.get_connection(), date_range)
    chart5 = build_dual_axis_chart(close_df, date_range)
    chart6 = build_signal_timeline(signal_df, date_range)

    left, right = st.columns(2)
    with left:
        if chart1:
            st.plotly_chart(chart1, use_container_width=True)
        if chart3:
            st.plotly_chart(chart3, use_container_width=True)
        if chart5:
            st.plotly_chart(chart5, use_container_width=True)
    with right:
        if chart2:
            st.plotly_chart(chart2, use_container_width=True)
        if chart4:
            st.plotly_chart(chart4, use_container_width=True)
        if chart6:
            st.plotly_chart(chart6, use_container_width=True)

    st.markdown("---")
    st.subheader("Signal Backtesting")
    st.markdown(
        "### Event studies show each signal's metric path normalized to zero on the signal date. "
        "Thin grey lines are individual event trajectories and the bold coloured line is the average response."
    )
    with db.get_connection() as conn:
        total_signals = conn.execute(select(func.count()).select_from(db.signal_events)).scalar()
        type_counts = conn.execute(select(db.signal_events.c.signal_type, func.count()).group_by(db.signal_events.c.signal_type)).all()

        st.write("### Signal events from database before backtest")
        st.write(f"Total signal_events rows: {total_signals}")
        st.write(type_counts)

        if total_signals == 0:
            st.warning("No signal events found; running signals.py now to generate signals.")
            signals_module.run_signals()
            total_signals = conn.execute(select(func.count()).select_from(db.signal_events)).scalar()
            type_counts = conn.execute(select(db.signal_events.c.signal_type, func.count()).group_by(db.signal_events.c.signal_type)).all()
            st.write("Signal events after running signals:")
            st.write(f"Total signal_events rows: {total_signals}")
            st.write(type_counts)

        backtest_results = backtest.compute_backtest(conn)

    if not backtest_results:
        st.warning("Backtest data is not available. Run the pipeline or generate rolling metrics first.")
    else:
        signal_types = list(backtest.SIGNAL_METRIC_CONFIG.keys())
        row1 = st.columns(2)
        row2 = st.columns(2)
        for idx, signal_type in enumerate(signal_types):
            payload = backtest_results.get(signal_type)
            if payload is None:
                continue
            target_col = row1[idx] if idx < 2 else row2[idx - 2]
            with target_col:
                st.plotly_chart(payload["figure"], use_container_width=True)

        hit_table = ["| Signal | 10d | 20d | 40d |", "|---|---|---|---|"]
        for signal_type in signal_types:
            stats = backtest_results[signal_type]["horizon_stats"]
            row = [signal_type]
            for horizon in backtest.BACKTEST_HORIZONS:
                if horizon in stats:
                    row.append(f"{stats[horizon]['hit_rate'] * 100:.1f}%")
                else:
                    row.append("N/A")
            hit_table.append("| " + " | ".join(row) + " |")

        st.markdown("#### Hit Rate Summary")
        st.markdown("\n".join(hit_table))

        st.markdown("#### Limitations")
        limitations_lines = load_backtest_limitations()
        if limitations_lines:
            st.markdown("\n".join(limitations_lines))
        else:
            st.markdown("- Backtest limitations are unavailable; run the backtest first.")

    st.markdown("---")
    st.subheader("Weekly Desk Note")
    if NOTE_FILE.exists():
        note_text = NOTE_FILE.read_text()
        st.markdown(note_text)
        st.caption(f"Last generated: {datetime.fromtimestamp(NOTE_FILE.stat().st_mtime).strftime('%Y-%m-%d %H:%M:%S')}")
    else:
        st.warning("Weekly note not found. Run the pipeline to generate outputs/notes/weekly_note.md.")


if __name__ == "__main__":
    main()
