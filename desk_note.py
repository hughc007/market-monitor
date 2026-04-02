import numpy as np
import pandas as pd
from pathlib import Path
from datetime import date, timedelta
from sqlalchemy import select, func

import backtest
import database as db

OUTPUT_FILE = Path("outputs/notes/weekly_note.md")
OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)


def load_close_prices(conn, tickers):
    stmt = select(db.daily_prices).where(db.daily_prices.c.ticker.in_(tickers)).order_by(db.daily_prices.c.date)
    df = pd.read_sql(stmt, conn)
    if df.empty:
        return pd.DataFrame()
    df["date"] = pd.to_datetime(df["date"])
    return df.pivot(index="date", columns="ticker", values="close")


def load_metric(conn, metric_name, ticker=None):
    stmt = select(db.rolling_metrics).where(db.rolling_metrics.c.metric_name == metric_name)
    if ticker:
        stmt = stmt.where(db.rolling_metrics.c.ticker == ticker)
    stmt = stmt.order_by(db.rolling_metrics.c.date)
    df = pd.read_sql(stmt, conn)
    if df.empty:
        return pd.DataFrame()
    df["date"] = pd.to_datetime(df["date"])
    if ticker:
        return df.set_index("date")["metric_value"].sort_index()
    return df


def load_recent_signals(conn, days=7):
    cutoff = date.today() - timedelta(days=days)
    stmt = select(db.signal_events).where(db.signal_events.c.date >= cutoff).order_by(db.signal_events.c.date.desc())
    df = pd.read_sql(stmt, conn)
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"]).dt.date
    return df


def calculate_weekly_returns(close_df):
    result = {}
    for ticker in close_df.columns:
        series = close_df[ticker].dropna()
        if len(series) < 6:
            result[ticker] = np.nan
            continue
        result[ticker] = series.iloc[-1] / series.iloc[-6] - 1
    return result


def current_spread_stats(close_df):
    pair = close_df[["BZ=F", "CL=F"]].dropna()
    if pair.empty:
        return {}
    spread = pair["BZ=F"] - pair["CL=F"]
    latest = spread.iloc[-1]
    mean = spread.tail(90).mean()
    std = spread.tail(90).std(ddof=0)
    return {
        "latest": float(latest),
        "mean": float(mean),
        "std": float(std),
        "zscore": float((latest - mean) / std) if std != 0 else np.nan,
    }


def current_vol_regime(conn):
    regimes = {}
    for ticker in ["BZ=F", "WDS.AX", "STO.AX"]:
        series = load_metric(conn, "30d_annualised_volatility", ticker)
        if series.empty:
            regimes[ticker] = {}
            continue
        latest = float(series.iloc[-1])
        average = float(series.tail(180).mean()) if len(series) >= 180 else float(series.mean())
        regimes[ticker] = {
            "latest": latest,
            "average": average,
            "status": "elevated" if latest > average else "normal",
        }
    return regimes


def current_correlations(conn):
    result = {}
    for ticker in ["WDS.AX", "STO.AX"]:
        series = load_metric(conn, "60d_corr_with_BZ", ticker)
        if series.empty:
            result[ticker] = np.nan
            continue
        result[ticker] = float(series.iloc[-1])
    return result


def render_note(weekly_returns, spread_stats, vol_regime, correlations, recent_signals, backtest_results):
    lines = [
        "# Weekly Desk Note",
        "",
        f"Date: {date.today().isoformat()}",
        "",
        "## Weekly Returns",
    ]

    for ticker, value in weekly_returns.items():
        if np.isnan(value):
            lines.append(f"- {ticker}: insufficient history")
        else:
            lines.append(f"- {ticker}: {value * 100:.2f}%")

    lines.extend(["", "## Spread Overview", ""])
    if spread_stats:
        lines.append(f"- Latest Brent-WTI spread: {spread_stats['latest']:.2f} USD")
        lines.append(f"- 90-day mean spread: {spread_stats['mean']:.2f} USD")
        lines.append(f"- 90-day spread standard deviation: {spread_stats['std']:.2f} USD")
        lines.append(f"- Current spread z-score: {spread_stats['zscore']:.2f}")
    else:
        lines.append("- Spread data unavailable.")

    lines.extend(["", "## Volatility Regime", ""])
    for ticker, info in vol_regime.items():
        if not info:
            lines.append(f"- {ticker}: no volatility series available")
            continue
        lines.append(
            f"- {ticker}: latest {info['latest'] * 100:.2f}% vs average {info['average'] * 100:.2f}% -> {info['status']}"
        )

    lines.extend(["", "## Correlation Update", ""])
    for ticker, corr in correlations.items():
        if np.isnan(corr):
            lines.append(f"- {ticker}: no correlation series available")
        else:
            lines.append(f"- Brent vs {ticker}: {corr:.2f}")

    lines.extend(["", "## Signals in the Last 7 Days", ""])
    if recent_signals.empty:
        lines.append("- No signals fired in the last 7 days.")
    else:
        for _, row in recent_signals.iterrows():
            lines.append(
                f"- {row['date']}: {row['signal_type']} ({row['signal_value']:.3f}) | {row['market_context']}"
            )

    lines.extend(["", "## Notes", "", "- Monitor Brent-WTI spread versus the 90-day mean and volatility regime signals.", "- Check AUD/USD dislocations for AUD-denominated energy equity exposure."])

    if backtest_results is not None:
        lines.extend(["", "## Backtest Summary", ""])
        if not backtest_results:
            lines.append("- Backtest results unavailable.")
        else:
            for signal_type, payload in backtest_results.items():
                stats = payload["horizon_stats"]
                if not stats:
                    lines.append(f"- {signal_type}: insufficient data for hit rate summary.")
                    continue
                hit_rates = [
                    f"{h}d {stats[h]['hit_rate'] * 100:.1f}%"
                    for h in backtest.BACKTEST_HORIZONS
                    if h in stats
                ]
                lines.append(f"- {signal_type}: {'; '.join(hit_rates)}")
            lines.extend([
                "", "### Backtest Limitations", "",
                "- Short data history and low event counts may make hit rates unstable.",
                "- The analysis measures metric outcomes, not tradable profit and loss, and excludes transaction costs.",
                "- We apply a 10-day cooldown to reduce double counting, but clustering can still bias results toward regimes.",
                "- The event study uses future price and rolling metric series; it does not model execution, carry, or liquidity risk.",
            ])

    return "\n".join(lines)


def main():
    with db.get_connection() as conn:
        tickers = ["BZ=F", "CL=F", "AUDUSD=X", "WDS.AX", "STO.AX"]
        close_df = load_close_prices(conn, tickers)
        weekly_returns = calculate_weekly_returns(close_df)
        spread_stats = current_spread_stats(close_df)
        vol_regime = current_vol_regime(conn)
        correlations = current_correlations(conn)
        recent_signals = load_recent_signals(conn, days=7)
        backtest_results = backtest.compute_backtest(conn)

    note = render_note(weekly_returns, spread_stats, vol_regime, correlations, recent_signals, backtest_results)
    OUTPUT_FILE.write_text(note)
    print(f"Written weekly note to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
