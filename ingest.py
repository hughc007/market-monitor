import numpy as np
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta

from config import INSTRUMENTS, START_DATE, END_DATE
import database as db


PRICE_SANITY_RANGES = {
    "BZ=F": (40.0, 200.0),
    "CL=F": (40.0, 200.0),
    "AUDUSD=X": (0.50, 0.90),
    "WDS.AX": (10.0, 60.0),
    "STO.AX": (3.0, 20.0),
}


def clean_price_data(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    if hasattr(df.columns, "nlevels") and df.columns.nlevels > 1:
        df.columns = df.columns.get_level_values(0)

    df.columns = [col.lower() for col in df.columns]
    df = df.ffill()
    df = df.dropna(how="all", subset=["open", "high", "low", "close", "volume"])
    return df


def validate_and_fix_price_data(df: pd.DataFrame, ticker: str, prior_close: float | None = None) -> pd.DataFrame:
    if df.empty or ticker not in PRICE_SANITY_RANGES:
        return df

    if "close" not in df.columns:
        return df

    low, high = PRICE_SANITY_RANGES[ticker]
    invalid = (df["close"] < low) | (df["close"] > high)

    # Detect large intraday contract rollover anomalies as well for futures, e.g. Brent front-month spike/dip near expiry.
    if ticker in ["BZ=F", "CL=F"]:
        prev_close_series = df["close"].shift(1)
        large_jump = (df["close"] / prev_close_series - 1).abs() > 0.20
        invalid = invalid | large_jump

        # Check first row against prior database close if available
        if prior_close is not None and len(df) > 0:
            first_close = df.iloc[0]["close"]
            if prior_close > 0 and abs(first_close / prior_close - 1) > 0.20:
                invalid.iloc[0] = True

    if not invalid.any():
        return df

    count = int(invalid.sum())
    print(f"Warning: {ticker} has {count} price anomaly values. Applying forward fill replacement.")

    df.loc[invalid, "close"] = np.nan
    df["close"] = df["close"].ffill()
    return df


def clean_existing_bad_data(conn):
    stmt = db.daily_prices.select().order_by(db.daily_prices.c.ticker, db.daily_prices.c.date, db.daily_prices.c.created_at)
    df = pd.read_sql(stmt, conn)
    if df.empty:
        print("No daily_prices rows found for cleaning.")
        return 0

    total_cleaned = 0

    for ticker, (low, high) in PRICE_SANITY_RANGES.items():
        ticker_df = df[df["ticker"] == ticker].sort_values("date")
        if ticker_df.empty:
            continue

        valid_mask = ticker_df["close"].between(low, high, inclusive="both")

        # Add large jump detection for strong futures rollover anomalies
        if ticker in ["BZ=F", "CL=F"]:
            prev_close = ticker_df["close"].shift(1)
            large_jump = (ticker_df["close"] / prev_close - 1).abs() > 0.20
            valid_mask = valid_mask & ~large_jump

        invalid_df = ticker_df[~valid_mask].copy()
        if invalid_df.empty:
            continue

        ticker_df.loc[~valid_mask, "close"] = np.nan
        ticker_df["close"] = ticker_df["close"].ffill()

        cleaned_rows = 0
        for idx, row in invalid_df.iterrows():
            new_close = ticker_df.loc[idx, "close"]
            if pd.isna(new_close):
                continue
            conn.execute(
                db.daily_prices.update()
                .where(db.daily_prices.c.id == int(row["id"]))
                .values(close=float(new_close))
            )
            cleaned_rows += 1

        total_cleaned += cleaned_rows
        print(f"Cleaned {cleaned_rows} rows for {ticker} (range [{low}, {high}]).")

    print(f"Total cleaned rows: {total_cleaned}")
    return total_cleaned


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
                # BZ=F is the Brent front month futures contract. Near expiry the price diverges from the active contract.
                # Use auto_adjust=True to handle rollovers automatically.
                df = yf.download(
                    ticker,
                    start=start_date.isoformat(),
                    end=download_end.isoformat(),
                    progress=False,
                    auto_adjust=True,
                )
                if df.empty:
                    summary.append((ticker, 0, 0, "no data downloaded"))
                    continue

                df = clean_price_data(df)
                if df.empty:
                    summary.append((ticker, 0, 0, "cleaned data empty"))
                    continue

                prior_close = None
                if last_date is not None:
                    prior_stmt = db.daily_prices.select().with_only_columns(db.daily_prices.c.close).where(db.daily_prices.c.ticker == ticker, db.daily_prices.c.date == last_date)
                    prior_close = conn.execute(prior_stmt).scalar_one_or_none()

                df = validate_and_fix_price_data(df, ticker, prior_close)

                price_rows = insert_daily_prices(conn, ticker, df)
                returns = calculate_log_returns(df)
                return_rows = insert_daily_returns(conn, ticker, returns)
                summary.append((ticker, price_rows, return_rows, "loaded"))

            # Deduplicate after inserting all prices for this run in the transaction
            db.dedupe_daily_prices(conn)

    return summary


def main():
    summary = run_pipeline()
    print("Ingestion summary:")
    for ticker, price_rows, return_rows, status in summary:
        print(f"- {ticker}: prices={price_rows}, returns={return_rows}, status={status}")


if __name__ == "__main__":
    main()
