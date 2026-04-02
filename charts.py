import matplotlib.pyplot as plt
import pandas as pd
from pathlib import Path
from sqlalchemy import select

import database as db

OUTPUT_DIR = Path("outputs/charts")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def load_close_prices(conn, tickers):
    stmt = select(db.daily_prices).where(db.daily_prices.c.ticker.in_(tickers)).order_by(db.daily_prices.c.date)
    df = pd.read_sql(stmt, conn)
    if df.empty:
        return pd.DataFrame()
    df["date"] = pd.to_datetime(df["date"])
    pivot = df.pivot(index="date", columns="ticker", values="close")
    return pivot


def load_metric_series(conn, metric_name, tickers=None):
    stmt = select(db.rolling_metrics).where(db.rolling_metrics.c.metric_name == metric_name).order_by(db.rolling_metrics.c.date)
    df = pd.read_sql(stmt, conn)
    if df.empty:
        return pd.DataFrame()
    df["date"] = pd.to_datetime(df["date"])
    if tickers:
        df = df[df["ticker"].isin(tickers)]
    return df.pivot(index="date", columns="ticker", values="metric_value")


def load_signals(conn, signal_type=None):
    stmt = select(db.signal_events).order_by(db.signal_events.c.date)
    df = pd.read_sql(stmt, conn)
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"])
    if signal_type:
        df = df[df["signal_type"] == signal_type]
    return df


def normalised_prices_plot(close_df):
    fig, ax = plt.subplots(figsize=(12, 7))
    for ticker in close_df.columns:
        series = close_df[ticker].dropna()
        if series.empty:
            continue
        rebased = series / series.iloc[0] * 100
        ax.plot(rebased.index, rebased.values, label=ticker)

    ax.set_title("Normalised Price Series — Rebased to 100")
    ax.set_ylabel("Rebased Price")
    ax.set_xlabel("Date")
    ax.legend(loc="upper left")
    ax.grid(True, alpha=0.2)
    fig.savefig(OUTPUT_DIR / "normalised_prices.png", dpi=150, bbox_inches="tight")
    plt.close(fig)


def brent_wti_spread_plot(close_df, signal_df):
    series = close_df[["BZ=F", "CL=F"]].dropna()
    spread = series["BZ=F"] - series["CL=F"]
    mean = spread.mean()
    std = spread.std(ddof=0)

    fig, ax = plt.subplots(figsize=(12, 7))
    ax.plot(spread.index, spread.values, label="Spread (BZ - WTI)", color="cyan")
    ax.axhline(mean, linestyle="--", color="white", label="Mean")
    ax.axhline(mean + 2 * std, linestyle="--", color="yellow", label="Mean +2σ")
    ax.axhline(mean - 2 * std, linestyle="--", color="yellow", label="Mean -2σ")

    if not signal_df.empty:
        spread_signals = signal_df[signal_df["signal_type"] == "SPREAD_DISLOCATION"]
        signal_dates = pd.to_datetime(spread_signals["date"])
        signal_values = spread.reindex(signal_dates).values
        ax.scatter(signal_dates, signal_values, color="red", zorder=5, label="SPREAD_DISLOCATION")

    ax.set_title("Brent-WTI Spread with Dislocation Signals")
    ax.set_ylabel("Spread (USD)")
    ax.set_xlabel("Date")
    ax.legend(loc="upper left")
    ax.grid(True, alpha=0.2)
    fig.savefig(OUTPUT_DIR / "brent_wti_spread.png", dpi=150, bbox_inches="tight")
    plt.close(fig)


def rolling_volatility_plot(vol_df):
    fig, ax = plt.subplots(figsize=(12, 7))
    for ticker in vol_df.columns:
        ax.plot(vol_df.index, vol_df[ticker] * 100, label=ticker)

    ax.set_title("Rolling 30-Day Annualised Volatility (%)")
    ax.set_ylabel("Volatility (%)")
    ax.set_xlabel("Date")
    ax.legend(loc="upper left")
    ax.grid(True, alpha=0.2)
    fig.savefig(OUTPUT_DIR / "rolling_volatility.png", dpi=150, bbox_inches="tight")
    plt.close(fig)


def rolling_correlation_plot(corr_df):
    fig, ax = plt.subplots(figsize=(12, 7))
    for ticker in corr_df.columns:
        ax.plot(corr_df.index, corr_df[ticker], label=ticker)
    ax.axhline(0.3, linestyle="--", color="yellow", label="Breakdown Threshold")

    ax.set_title("Rolling 60-Day Correlation — Brent vs ASX Energy")
    ax.set_ylabel("Correlation")
    ax.set_xlabel("Date")
    ax.legend(loc="upper left")
    ax.grid(True, alpha=0.2)
    fig.savefig(OUTPUT_DIR / "rolling_correlation.png", dpi=150, bbox_inches="tight")
    plt.close(fig)


def audusd_vs_brent_plot(close_df):
    fig, ax1 = plt.subplots(figsize=(12, 7))
    ax2 = ax1.twinx()

    ax1.plot(close_df.index, close_df["BZ=F"], color="cyan", label="Brent")
    ax2.plot(close_df.index, close_df["AUDUSD=X"], color="orange", label="AUD/USD")

    ax1.set_title("Brent Crude vs AUD/USD")
    ax1.set_ylabel("Brent Close")
    ax2.set_ylabel("AUD/USD")
    ax1.set_xlabel("Date")

    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper left")
    ax1.grid(True, alpha=0.2)
    fig.savefig(OUTPUT_DIR / "audusd_vs_brent.png", dpi=150, bbox_inches="tight")
    plt.close(fig)


def signal_timeline_plot(signal_df):
    if signal_df.empty:
        return

    signal_types = ["SPREAD_DISLOCATION", "VOLATILITY_SPIKE", "CORRELATION_BREAKDOWN", "FX_DISLOCATION"]
    y_map = {signal: idx for idx, signal in enumerate(signal_types[::-1], start=1)}
    colors = {
        "SPREAD_DISLOCATION": "red",
        "VOLATILITY_SPIKE": "orange",
        "CORRELATION_BREAKDOWN": "magenta",
        "FX_DISLOCATION": "lime",
    }

    fig, ax = plt.subplots(figsize=(12, 7))
    for signal_type in signal_types:
        subset = signal_df[signal_df["signal_type"] == signal_type]
        if subset.empty:
            continue
        ax.scatter(subset["date"], [y_map[signal_type]] * len(subset), color=colors[signal_type], label=signal_type, s=40)

    ax.set_yticks(list(y_map.values()))
    ax.set_yticklabels(signal_types[::-1])
    ax.set_title("Signal Event Timeline")
    ax.set_xlabel("Date")
    ax.set_ylabel("Signal Type")
    ax.legend(loc="upper left")
    ax.grid(True, alpha=0.2)
    fig.savefig(OUTPUT_DIR / "signal_timeline.png", dpi=150, bbox_inches="tight")
    plt.close(fig)


def main():
    plt.style.use("dark_background")
    with db.get_connection() as conn:
        tickers = ["BZ=F", "CL=F", "AUDUSD=X", "WDS.AX", "STO.AX"]
        close_df = load_close_prices(conn, tickers)
        signal_df = load_signals(conn)
        vol_df = load_metric_series(conn, "30d_annualised_volatility", ["BZ=F", "WDS.AX", "STO.AX"])
        corr_df = load_metric_series(conn, "60d_corr_with_BZ", ["WDS.AX", "STO.AX"])

        normalised_prices_plot(close_df)
        brent_wti_spread_plot(close_df, signal_df)
        rolling_volatility_plot(vol_df)
        rolling_correlation_plot(corr_df)
        audusd_vs_brent_plot(close_df)
        signal_timeline_plot(signal_df)

        print("Saved 6 chart PNG files to outputs/charts/")


if __name__ == "__main__":
    main()
