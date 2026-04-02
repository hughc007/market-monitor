import numpy as np
import pandas as pd
from datetime import datetime
from sqlalchemy import select, delete, and_, func

import database as db


def fetch_prices(conn, ticker: str) -> pd.DataFrame:
    stmt = select(db.daily_prices).where(db.daily_prices.c.ticker == ticker).order_by(db.daily_prices.c.date)
    df = pd.read_sql(stmt, conn)
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"]).dt.date
    return df.set_index("date")


def fetch_returns(conn, ticker: str) -> pd.Series:
    stmt = select(db.daily_returns).where(db.daily_returns.c.ticker == ticker).order_by(db.daily_returns.c.date)
    df = pd.read_sql(stmt, conn)
    if df.empty:
        return pd.Series(dtype="float64")
    df["date"] = pd.to_datetime(df["date"]).dt.date
    return df.set_index("date")["log_return"].sort_index()


def clear_metric(conn, ticker: str, metric_name: str, window: int | None):
    stmt = delete(db.rolling_metrics).where(
        and_(
            db.rolling_metrics.c.ticker == ticker,
            db.rolling_metrics.c.metric_name == metric_name,
            db.rolling_metrics.c.window == window,
        )
    )
    conn.execute(stmt)


def insert_metric_series(conn, ticker: str, metric_name: str, series: pd.Series, window: int | None):
    clear_metric(conn, ticker, metric_name, window)
    records = []
    for date_index, value in series.items():
        if pd.isna(value):
            continue
        records.append(
            {
                "ticker": ticker,
                "date": date_index,
                "metric_name": metric_name,
                "metric_value": float(value),
                "window": window,
                "created_at": datetime.utcnow(),
            }
        )
    if records:
        conn.execute(db.rolling_metrics.insert(), records)
    return len(records)


def calculate_30d_annual_vol(conn, ticker: str) -> int:
    returns = fetch_returns(conn, ticker)
    if returns.empty:
        return 0
    volatility = returns.rolling(30, min_periods=30).std(ddof=0) * np.sqrt(252)
    return insert_metric_series(conn, ticker, "30d_annualised_volatility", volatility, 30)


def calculate_bz_sma(conn) -> dict[str, int]:
    prices = fetch_prices(conn, "BZ=F")
    results = {}
    if prices.empty:
        return {"sma_20": 0, "sma_50": 0}
    close = prices["close"].sort_index()
    results["sma_20"] = insert_metric_series(conn, "BZ=F", "sma_20", close.rolling(20, min_periods=20).mean(), 20)
    results["sma_50"] = insert_metric_series(conn, "BZ=F", "sma_50", close.rolling(50, min_periods=50).mean(), 50)
    return results


def calculate_60d_correlations(conn) -> dict[str, int]:
    base = fetch_prices(conn, "BZ=F")
    if base.empty:
        return {}
    base_close = base["close"].sort_index()
    peers = ["WDS.AX", "STO.AX", "AUDUSD=X"]
    counts = {}
    for peer in peers:
        peer_prices = fetch_prices(conn, peer)
        if peer_prices.empty:
            counts[peer] = 0
            continue
        merged = pd.concat(
            [base_close, peer_prices["close"]],
            axis=1,
            join="inner",
            keys=["BZ=F", peer],
        )
        merged.columns = ["brent", "peer"]
        corr = merged["brent"].rolling(60, min_periods=60).corr(merged["peer"])
        metric_name = f"60d_corr_with_BZ"
        counts[peer] = insert_metric_series(conn, peer, metric_name, corr, 60)
    return counts


def calculate_spread_metrics(conn) -> dict[str, int]:
    brent = fetch_prices(conn, "BZ=F")
    wti = fetch_prices(conn, "CL=F")
    if brent.empty or wti.empty:
        return {"spread": 0, "spread_zscore": 0}

    spread = brent["close"].sort_index().subtract(wti["close"].sort_index(), fill_value=np.nan)
    spread = spread.dropna()
    spread_z = (spread - spread.rolling(90, min_periods=90).mean()) / spread.rolling(90, min_periods=90).std(ddof=0)
    spread_count = insert_metric_series(conn, "BZ=F", "brent_wti_spread", spread, 90)
    zscore_count = insert_metric_series(conn, "BZ=F", "brent_wti_spread_zscore", spread_z, 90)
    return {"spread": spread_count, "spread_zscore": zscore_count}


def calculate_audusd_zscore(conn) -> int:
    audusd = fetch_prices(conn, "AUDUSD=X")
    if audusd.empty:
        return 0
    close = audusd["close"].sort_index()
    zscore = (close - close.rolling(90, min_periods=90).mean()) / close.rolling(90, min_periods=90).std(ddof=0)
    return insert_metric_series(conn, "AUDUSD=X", "audusd_90d_zscore", zscore, 90)


def compute_metrics():
    db.create_tables()
    summary = {}
    with db.get_connection() as conn:
        with conn.begin():
            summary["volatility"] = {ticker: calculate_30d_annual_vol(conn, ticker) for ticker in ["BZ=F", "CL=F", "AUDUSD=X", "WDS.AX", "STO.AX"]}
            summary["bz_sma"] = calculate_bz_sma(conn)
            summary["correlations"] = calculate_60d_correlations(conn)
            summary["spread_metrics"] = calculate_spread_metrics(conn)
            summary["audusd_zscore"] = calculate_audusd_zscore(conn)
    return summary


def count_metrics_by_type(conn):
    stmt = select(db.rolling_metrics.c.metric_name, func.count()).group_by(db.rolling_metrics.c.metric_name)
    result = conn.execute(stmt).all()
    return {row[0]: row[1] for row in result}


def main():
    summary = compute_metrics()
    with db.get_connection() as conn:
        counts = count_metrics_by_type(conn)

    print("Metric ingest summary:")
    for section, values in summary.items():
        print(f"- {section}:")
        if isinstance(values, dict):
            for key, count in values.items():
                print(f"  - {key}: {count}")
        else:
            print(f"  - {values}")
    print("\nStored rolling metrics counts:")
    for metric_name, count in counts.items():
        print(f"- {metric_name}: {count}")


if __name__ == "__main__":
    main()
