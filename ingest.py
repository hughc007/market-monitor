import numpy as np
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta

from config import INSTRUMENTS, START_DATE, END_DATE
import database as db


def clean_price_data(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    if hasattr(df.columns, "nlevels") and df.columns.nlevels > 1:
        df.columns = df.columns.get_level_values(0)

    df.columns = [col.lower() for col in df.columns]
    df = df.ffill()
    df = df.dropna(how="all", subset=["open", "high", "low", "close", "volume"])
    return df


def calculate_log_returns(df: pd.DataFrame) -> pd.Series:
    if "close" not in df.columns:
        raise ValueError("Price data must contain a 'close' column for return calculations.")

    return np.log(df["close"] / df["close"].shift(1))


def insert_daily_prices(conn, ticker: str, df: pd.DataFrame):
    records = []
    for date_index, row in df.iterrows():
        records.append(
            {
                "ticker": ticker,
                "date": date_index.date(),
                "open": float(row.get("open", np.nan)) if not pd.isna(row.get("open")) else None,
                "high": float(row.get("high", np.nan)) if not pd.isna(row.get("high")) else None,
                "low": float(row.get("low", np.nan)) if not pd.isna(row.get("low")) else None,
                "close": float(row.get("close", np.nan)) if not pd.isna(row.get("close")) else None,
                "volume": float(row.get("volume", np.nan)) if not pd.isna(row.get("volume")) else None,
                "created_at": datetime.utcnow(),
            }
        )
    if records:
        conn.execute(db.daily_prices.insert(), records)
    return len(records)


def insert_daily_returns(conn, ticker: str, returns: pd.Series):
    records = []
    for date_index, value in returns.items():
        if pd.isna(value):
            continue
        records.append(
            {
                "ticker": ticker,
                "date": date_index.date(),
                "log_return": float(value),
                "created_at": datetime.utcnow(),
            }
        )
    if records:
        conn.execute(db.daily_returns.insert(), records)
    return len(records)


def run_pipeline():
    db.create_tables()
    db.insert_instruments(INSTRUMENTS)

    summary = []
    with db.get_connection() as conn:
        with conn.begin():
            for ticker, instrument in INSTRUMENTS.items():
                last_date = db.get_latest_date_for_ticker(ticker)
                if last_date is not None:
                    start_date = last_date + timedelta(days=1)
                    if start_date < START_DATE:
                        start_date = START_DATE
                    if start_date > END_DATE:
                        summary.append((ticker, 0, 0, "already current"))
                        continue
                else:
                    start_date = START_DATE

                download_end = END_DATE + timedelta(days=1)
                df = yf.download(ticker, start=start_date.isoformat(), end=download_end.isoformat(), progress=False)
                if df.empty:
                    summary.append((ticker, 0, 0, "no data downloaded"))
                    continue

                df = clean_price_data(df)
                if df.empty:
                    summary.append((ticker, 0, 0, "cleaned data empty"))
                    continue

                price_rows = insert_daily_prices(conn, ticker, df)
                returns = calculate_log_returns(df)
                return_rows = insert_daily_returns(conn, ticker, returns)
                summary.append((ticker, price_rows, return_rows, "loaded"))

    return summary


def main():
    summary = run_pipeline()
    print("Ingestion summary:")
    for ticker, price_rows, return_rows, status in summary:
        print(f"- {ticker}: prices={price_rows}, returns={return_rows}, status={status}")


if __name__ == "__main__":
    main()
