import numpy as np
import pandas as pd
from pathlib import Path
import plotly.graph_objects as go
from sqlalchemy import select, func

import database as db

OUTPUT_CHART_DIR = Path("outputs/charts")
OUTPUT_NOTE_DIR = Path("outputs/notes")
OUTPUT_CHART_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_NOTE_DIR.mkdir(parents=True, exist_ok=True)
BACKTEST_NOTE_FILE = OUTPUT_NOTE_DIR / "backtest_results.md"

BACKTEST_HORIZONS = [10, 20, 40]
EVENT_STUDY_WINDOW = 20
EVENT_STUDY_FILENAMES = {
    "SPREAD_DISLOCATION": "event_study_spread.png",
    "VOLATILITY_SPIKE": "event_study_volatility.png",
    "CORRELATION_BREAKDOWN": "event_study_correlation.png",
    "FX_DISLOCATION": "event_study_fx.png",
}

SIGNAL_METRIC_CONFIG = {
    "SPREAD_DISLOCATION": {
        "metric_name": "brent_wti_spread",
        "metric_ticker": "BZ=F",
        "source": "price_spread",
        "unit": "USD",
        "format": lambda v: f"{v:+.2f}",
        "hit_fn": lambda signal, future: future < signal,
        "measure_fn": lambda signal, future: future - signal,
        "display_name": "Brent-WTI Spread",
        "chart_color": "cyan",
    },
    "VOLATILITY_SPIKE": {
        "metric_name": "30d_annualised_volatility",
        "metric_ticker": "BZ=F",
        "source": "rolling_metric",
        "unit": "annualised vol",
        "format": lambda v: f"{v * 100:+.2f}%" if pd.notna(v) else "N/A",
        "hit_fn": lambda signal, future: future < signal,
        "measure_fn": lambda signal, future: future - signal,
        "display_name": "Brent 30-Day Volatility",
        "chart_color": "magenta",
    },
    "CORRELATION_BREAKDOWN": {
        "metric_name": "60d_corr_with_BZ",
        "metric_ticker": "WDS.AX",
        "source": "rolling_metric",
        "unit": "correlation",
        "format": lambda v: f"{v:+.3f}",
        "hit_fn": lambda signal, future: future > 0.3,
        "measure_fn": lambda signal, future: future - signal,
        "display_name": "Brent vs WDS.AX Correlation",
        "chart_color": "lime",
    },
    "FX_DISLOCATION": {
        "metric_name": "audusd_90d_zscore",
        "metric_ticker": "AUDUSD=X",
        "source": "rolling_metric",
        "unit": "z-score",
        "format": lambda v: f"{v:+.3f}",
        "hit_fn": lambda signal, future: abs(future) < abs(signal),
        "measure_fn": lambda signal, future: abs(signal) - abs(future),
        "display_name": "AUD/USD Z-score",
        "chart_color": "orange",
    },
}


def _to_timestamp(value):
    return pd.to_datetime(value).normalize()


def load_trading_calendar(conn):
    stmt = select(db.daily_prices.c.date).order_by(db.daily_prices.c.date)
    rows = conn.execute(stmt).scalars().all()
    if not rows:
        return pd.DatetimeIndex([])
    dates = pd.to_datetime(rows).normalize()
    dates = dates.drop_duplicates().sort_values()
    return pd.DatetimeIndex(dates)


def load_signal_events(conn):
    stmt = select(db.signal_events).order_by(db.signal_events.c.signal_type, db.signal_events.c.date)
    df = pd.read_sql(stmt, conn)
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    return df


def load_metric_series(conn, signal_type):
    config = SIGNAL_METRIC_CONFIG[signal_type]
    if config["source"] == "price_spread":
        return _load_spread_series(conn)
    return _load_rolling_metric_series(conn, config["metric_name"], config["metric_ticker"])


def _load_spread_series(conn):
    stmt = select(db.daily_prices).where(db.daily_prices.c.ticker.in_(["BZ=F", "CL=F"]))
    df = pd.read_sql(stmt, conn)
    if df.empty:
        return pd.Series(dtype="float64")
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    pivot = df.pivot(index="date", columns="ticker", values="close").sort_index()
    if "BZ=F" not in pivot.columns or "CL=F" not in pivot.columns:
        return pd.Series(dtype="float64")
    spread = pivot["BZ=F"] - pivot["CL=F"]
    return spread.dropna()


def _load_rolling_metric_series(conn, metric_name, ticker):
    stmt = select(db.rolling_metrics).where(
        db.rolling_metrics.c.metric_name == metric_name,
        db.rolling_metrics.c.ticker == ticker,
    ).order_by(db.rolling_metrics.c.date)
    df = pd.read_sql(stmt, conn)
    if df.empty:
        return pd.Series(dtype="float64")
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    series = df.set_index("date")["metric_value"].sort_index()
    return series


def filter_event_clusters(events, trading_calendar, cooldown_days=10):
    if events.empty:
        return events
    filtered = []
    date_index = {d: idx for idx, d in enumerate(trading_calendar)}

    for signal_type, group in events.groupby("signal_type"):
        last_index = -cooldown_days - 1
        for _, row in group.sort_values("date").iterrows():
            event_date = _to_timestamp(row["date"])
            if event_date not in date_index:
                continue
            current_index = date_index[event_date]
            if current_index <= last_index + cooldown_days:
                continue
            filtered.append(row)
            last_index = current_index

    if not filtered:
        return events.iloc[0:0]
    result = pd.DataFrame(filtered).reset_index(drop=True)
    result["date"] = pd.to_datetime(result["date"]).dt.normalize()
    return result


def _get_offset_date(trading_calendar, event_date, offset):
    event_date = _to_timestamp(event_date)
    if event_date not in trading_calendar:
        return None
    idx = trading_calendar.get_loc(event_date)
    target_idx = idx + offset
    if target_idx < 0 or target_idx >= len(trading_calendar):
        return None
    return trading_calendar[target_idx]


def compute_horizon_metrics(signal_type, signal_value, future_value):
    return SIGNAL_METRIC_CONFIG[signal_type]["measure_fn"](signal_value, future_value)


def compute_hit(signal_type, signal_value, future_value):
    return SIGNAL_METRIC_CONFIG[signal_type]["hit_fn"](signal_value, future_value)


def build_event_study(signal_type, event_dates, trading_calendar, series):
    if not event_dates or series.empty:
        return pd.DataFrame()
    event_study = {}
    pre = EVENT_STUDY_WINDOW
    post = EVENT_STUDY_WINDOW * 2
    for event_date in event_dates:
        if event_date not in trading_calendar:
            continue
        idx = trading_calendar.get_loc(event_date)
        start = idx - pre
        end = idx + post
        if start < 0 or end >= len(trading_calendar):
            continue
        window_dates = trading_calendar[start : end + 1]
        values = series.reindex(window_dates)
        if values.isna().any():
            continue
        normed = values - values.iloc[pre]
        event_study[event_date.strftime("%Y-%m-%d")] = normed.values

    if not event_study:
        return pd.DataFrame()
    index = list(range(-pre, post + 1))
    return pd.DataFrame(event_study, index=index)


def build_event_study_figure(signal_type, event_study_df):
    config = SIGNAL_METRIC_CONFIG[signal_type]
    fig = go.Figure()
    if event_study_df.empty:
        fig.add_annotation(text="No event study data available", xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False, font=dict(color="white", size=14))
        fig.update_layout(template="plotly_dark", xaxis_title="Trading days relative to signal date", yaxis_title="Normalised metric", title=f"{config['display_name']} — Event Study (n=0)")
        return fig

    for series_name in event_study_df.columns:
        fig.add_trace(
            go.Scatter(
                x=event_study_df.index,
                y=event_study_df[series_name],
                mode="lines",
                line=dict(color="lightgrey", width=1),
                opacity=0.65,
                showlegend=False,
                hoverinfo="skip",
            )
        )
    mean_series = event_study_df.mean(axis=1)
    fig.add_trace(
        go.Scatter(
            x=event_study_df.index,
            y=mean_series,
            mode="lines",
            line=dict(color=config["chart_color"], width=3),
            name="Average",
        )
    )
    fig.add_vline(x=0, line_dash="dash", line_color="white", opacity=0.8)
    fig.update_layout(
        template="plotly_dark",
        title=f"{config['display_name']} — Event Study (n={len(event_study_df.columns)})",
        xaxis_title="Trading days relative to signal date",
        yaxis_title="Normalised metric",
        legend=dict(bgcolor="rgba(0,0,0,0.5)", bordercolor="white", borderwidth=1),
    )
    return fig


def _compute_stats_for_horizon(records):
    if not records:
        return None
    df = pd.DataFrame(records)
    total = len(df)
    hits = df["hit"].sum()
    hit_rate = hits / total if total else 0.0
    correct = df[df["hit"]]
    incorrect = df[~df["hit"]]
    fmt = lambda v: v if pd.notna(v) else None
    return {
        "events": total,
        "hits": int(hits),
        "hit_rate": hit_rate,
        "avg_correct": correct["measure"].abs().mean() if not correct.empty else np.nan,
        "avg_incorrect": incorrect["measure"].abs().mean() if not incorrect.empty else np.nan,
        "best": df["measure"].min() if df["measure"].dtype.kind in "buif" else np.nan,
        "worst": df["measure"].max() if df["measure"].dtype.kind in "buif" else np.nan,
    }


def compute_backtest(conn, cooldown_days=10):
    trading_calendar = load_trading_calendar(conn)
    signal_events = load_signal_events(conn)
    if signal_events.empty or trading_calendar.empty:
        return {}

    filtered_signals = filter_event_clusters(signal_events, trading_calendar, cooldown_days)
    results = {}
    for signal_type, config in SIGNAL_METRIC_CONFIG.items():
        events = filtered_signals[filtered_signals["signal_type"] == signal_type].copy()
        if events.empty:
            results[signal_type] = {
                "filtered_events": events,
                "horizon_stats": {},
                "event_study": pd.DataFrame(),
                "figure": build_event_study_figure(signal_type, pd.DataFrame()),
                "summary": None,
            }
            continue

        metric_series = load_metric_series(conn, signal_type)
        if metric_series.empty:
            continue

        horizon_records = {h: [] for h in BACKTEST_HORIZONS}
        for _, event in events.iterrows():
            event_date = _to_timestamp(event["date"])
            if event_date not in trading_calendar or event_date not in metric_series.index:
                continue
            signal_value = metric_series.loc[event_date]
            for horizon in BACKTEST_HORIZONS:
                future_date = _get_offset_date(trading_calendar, event_date, horizon)
                if future_date is None or future_date not in metric_series.index:
                    continue
                future_value = metric_series.loc[future_date]
                if pd.isna(signal_value) or pd.isna(future_value):
                    continue
                measure = compute_horizon_metrics(signal_type, signal_value, future_value)
                hit = compute_hit(signal_type, signal_value, future_value)
                horizon_records[horizon].append(
                    {
                        "date": event_date,
                        "signal_value": float(signal_value),
                        "future_value": float(future_value),
                        "measure": float(measure),
                        "hit": bool(hit),
                    }
                )

        horizon_stats = {}
        for horizon, records in horizon_records.items():
            stats = _compute_stats_for_horizon(records)
            if stats is not None:
                horizon_stats[horizon] = stats

        event_study_df = build_event_study(signal_type, events["date"].tolist(), trading_calendar, metric_series)
        figure = build_event_study_figure(signal_type, event_study_df)
        results[signal_type] = {
            "filtered_events": events,
            "horizon_stats": horizon_stats,
            "event_study": event_study_df,
            "figure": figure,
        }

    return results


def _format_stat_value(signal_type, value):
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return "N/A"
    format_fn = SIGNAL_METRIC_CONFIG[signal_type]["format"]
    if isinstance(value, (int, np.integer)):
        return format_fn(float(value))
    return format_fn(float(value)) if pd.notna(value) else "N/A"


def _format_percentage(value):
    if value is None or np.isnan(value):
        return "N/A"
    return f"{value * 100:.1f}%"


def _make_interpretation(signal_type, stats):
    if not stats:
        return "Insufficient data to draw a reliable conclusion."
    hit_rates = [stats[h]["hit_rate"] for h in BACKTEST_HORIZONS if h in stats]
    if not hit_rates:
        return "Insufficient data to draw a reliable conclusion."
    average_hit = sum(hit_rates) / len(hit_rates)
    if average_hit >= 0.6:
        verdict = "historically produced a positive signal" 
    elif average_hit >= 0.5:
        verdict = "showed mixed results with a slight edge" 
    else:
        verdict = "did not show strong historical consistency" 
    return (
        f"{SIGNAL_METRIC_CONFIG[signal_type]['display_name']} had {len(stats)} evaluable horizon sets after clustering. "
        f"The average hit rate across 10/20/40 day horizons was {average_hit * 100:.1f}%, so the signal {verdict}."
    )


def save_backtest_markdown(results):
    lines = ["# Signal Backtesting Results", ""]
    for signal_type, payload in results.items():
        config = SIGNAL_METRIC_CONFIG[signal_type]
        lines.append(f"## {signal_type}")
        lines.append(f"*Metric: {config['display_name']} ({config['unit']})*")
        lines.append("")
        lines.append("| Horizon | Events | Hit rate | Avg magnitude when correct | Avg magnitude when incorrect | Best case | Worst case |")
        lines.append("|---|---|---|---|---|---|---|")
        if payload["horizon_stats"]:
            for horizon in BACKTEST_HORIZONS:
                stats = payload["horizon_stats"].get(horizon)
                if not stats:
                    lines.append(f"| {horizon} | 0 | N/A | N/A | N/A | N/A | N/A |")
                    continue
                lines.append(
                    f"| {horizon} | {stats['events']} | {_format_percentage(stats['hit_rate'])} | {_format_stat_value(signal_type, stats['avg_correct'])} | {_format_stat_value(signal_type, stats['avg_incorrect'])} | {_format_stat_value(signal_type, stats['best'])} | {_format_stat_value(signal_type, stats['worst'])} |"
                )
        else:
            lines.append(f"| 10 | 0 | N/A | N/A | N/A | N/A | N/A |")
            lines.append(f"| 20 | 0 | N/A | N/A | N/A | N/A | N/A |")
            lines.append(f"| 40 | 0 | N/A | N/A | N/A | N/A | N/A |")
        lines.append("")
        lines.append(f"**Interpretation:** {_make_interpretation(signal_type, payload['horizon_stats'])}")
        lines.append("")

    lines.append("## Limitations")
    lines.append("")
    lines.append("- Short data history and low event counts may make hit rates unstable.")
    lines.append("- The analysis measures metric outcomes, not tradable profit and loss, and excludes transaction costs.")
    lines.append("- We apply a 10-day cooldown to reduce double counting, but clustering can still bias results toward regimes.")
    lines.append("- The event study uses future price and rolling metric series; it does not model execution, carry, or liquidity risk.")
    lines.append("- Some events are excluded if there are insufficient pre- or post-event trading days.")
    lines.append("")

    BACKTEST_NOTE_FILE.write_text("\n".join(lines), encoding="utf-8")
    return BACKTEST_NOTE_FILE


def save_backtest_charts(results):
    chart_paths = {}
    for signal_type, payload in results.items():
        fig = payload["figure"]
        filename_base = OUTPUT_CHART_DIR / f"backtest_{signal_type.lower()}.png"
        fig.write_image(str(filename_base), width=1000, height=600, scale=2)
        chart_paths[signal_type] = {
            "default": filename_base,
        }
        event_study_name = EVENT_STUDY_FILENAMES.get(signal_type)
        if event_study_name:
            event_study_path = OUTPUT_CHART_DIR / event_study_name
            fig.write_image(str(event_study_path), width=1000, height=600, scale=2)
            chart_paths[signal_type]["event_study"] = event_study_path
    return chart_paths


def run_backtest(save_outputs=True):
    with db.get_connection() as conn:
        stmt = select(db.signal_events.c.signal_type, func.count()).group_by(db.signal_events.c.signal_type)
        counts = conn.execute(stmt).all()
        print("Signal event counts before backtest:")
        if counts:
            for signal_type, count in counts:
                print(f"- {signal_type}: {count}")
        else:
            print("- none")
        results = compute_backtest(conn)
    if save_outputs:
        save_backtest_charts(results)
        save_backtest_markdown(results)
    return results


if __name__ == "__main__":
    results = run_backtest(save_outputs=True)
    summary_lines = ["Backtest complete. Generated summary and event study charts:", ""]
    for signal_type, payload in results.items():
        events = len(payload["filtered_events"])
        summary_lines.append(f"- {signal_type}: {events} filtered events")
    summary_lines.append(f"Saved markdown: {BACKTEST_NOTE_FILE}")
    summary = "\n".join(summary_lines)
    print(summary)
