import pandas as pd
from datetime import date, datetime, timedelta
from sqlalchemy import select, func, and_

import database as db
from config import (
    SPREAD_ZSCORE_THRESHOLD,
    VOL_SPIKE_MULTIPLIER,
    CORR_BREAKDOWN_THRESHOLD,
    FX_ZSCORE_THRESHOLD,
)

KEY_TICKERS = ["BZ=F", "CL=F", "AUDUSD=X", "WDS.AX", "STO.AX"]


def read_rolling_metrics(conn):
    stmt = select(db.rolling_metrics).order_by(db.rolling_metrics.c.date)
    df = pd.read_sql(stmt, conn)
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"]).dt.date
    return df


def get_price_map(conn, target_date: date) -> dict[str, float | None]:
    stmt = select(db.daily_prices.c.ticker, db.daily_prices.c.close).where(
        and_(
            db.daily_prices.c.date == target_date,
            db.daily_prices.c.ticker.in_(KEY_TICKERS),
        )
    )
    rows = conn.execute(stmt).all()
    return {ticker: close for ticker, close in rows}


def market_context_for_date(conn, target_date: date) -> str:
    prices = get_price_map(conn, target_date)
    context_parts = []
    for ticker in KEY_TICKERS:
        value = prices.get(ticker)
        formatted = f"{ticker}={value:.4f}" if value is not None else f"{ticker}=NA"
        context_parts.append(formatted)
    return ", ".join(context_parts)


def signal_exists(conn, ticker: str, event_date: date, signal_type: str) -> bool:
    stmt = select(func.count()).select_from(db.signal_events).where(
        and_(
            db.signal_events.c.ticker == ticker,
            db.signal_events.c.date == event_date,
            db.signal_events.c.signal_type == signal_type,
        )
    )
    return conn.execute(stmt).scalar_one() > 0


def log_signal(conn, ticker: str, event_date: date, signal_type: str, signal_value: float, threshold: float, market_context: str):
    if signal_exists(conn, ticker, event_date, signal_type):
        return False
    conn.execute(
        db.signal_events.insert().values(
            ticker=ticker,
            date=event_date,
            signal_type=signal_type,
            signal_value=signal_value,
            threshold=threshold,
            market_context=market_context,
            created_at=datetime.utcnow(),
        )
    )
    return True


def run_signals():
    db.create_tables()
    events = []
    today = date.today()
    with db.get_connection() as conn:
        with conn.begin():
            metrics = read_rolling_metrics(conn)
            if metrics.empty:
                return events

            # Signal 1 - SPREAD_DISLOCATION
            # Spread dislocation may indicate pipeline constraints, storage stress or export arbitrage opportunity
            spread_rows = metrics[metrics["metric_name"] == "brent_wti_spread_zscore"]
            for _, row in spread_rows.iterrows():
                zscore = float(row["metric_value"])
                if abs(zscore) > SPREAD_ZSCORE_THRESHOLD:
                    context = market_context_for_date(conn, row["date"])
                    inserted = log_signal(
                        conn,
                        "BZ=F",
                        row["date"],
                        "SPREAD_DISLOCATION",
                        zscore,
                        SPREAD_ZSCORE_THRESHOLD,
                        context,
                    )
                    if inserted:
                        events.append((row["date"], "SPREAD_DISLOCATION", zscore, context))

            # Signal 2 - VOLATILITY_SPIKE
            # Vol regime shift signals options repricing and hedger activity - watch for mean reversion
            vol_rows = metrics[(metrics["ticker"] == "BZ=F") & (metrics["metric_name"] == "30d_annualised_volatility")]
            if not vol_rows.empty:
                vol_rows = vol_rows.sort_values("date")
                vol_rows["threshold_value"] = vol_rows["metric_value"].rolling(180, min_periods=180).mean() * VOL_SPIKE_MULTIPLIER
                for _, row in vol_rows.iterrows():
                    threshold_value = row["threshold_value"]
                    if pd.notna(threshold_value) and row["metric_value"] > threshold_value:
                        context = market_context_for_date(conn, row["date"])
                        inserted = log_signal(
                            conn,
                            "BZ=F",
                            row["date"],
                            "VOLATILITY_SPIKE",
                            float(row["metric_value"]),
                            VOL_SPIKE_MULTIPLIER,
                            context,
                        )
                        if inserted:
                            events.append((row["date"], "VOLATILITY_SPIKE", float(row["metric_value"]), context))

            # Signal 3 - CORRELATION_BREAKDOWN
            # Correlation breakdown suggests company-specific risk decoupling from commodity driver
            corr_rows = metrics[(metrics["ticker"] == "WDS.AX") & (metrics["metric_name"] == "60d_corr_with_BZ")]
            for _, row in corr_rows.iterrows():
                corr_value = float(row["metric_value"])
                if corr_value < CORR_BREAKDOWN_THRESHOLD:
                    context = market_context_for_date(conn, row["date"])
                    inserted = log_signal(
                        conn,
                        "WDS.AX",
                        row["date"],
                        "CORRELATION_BREAKDOWN",
                        corr_value,
                        CORR_BREAKDOWN_THRESHOLD,
                        context,
                    )
                    if inserted:
                        events.append((row["date"], "CORRELATION_BREAKDOWN", corr_value, context))

            # Signal 4 - FX_DISLOCATION
            # FX dislocation vs commodity prices may indicate mean reversion setup for AUD-denominated energy equities
            fx_rows = metrics[(metrics["ticker"] == "AUDUSD=X") & (metrics["metric_name"] == "audusd_90d_zscore")]
            for _, row in fx_rows.iterrows():
                zscore = float(row["metric_value"])
                if abs(zscore) > FX_ZSCORE_THRESHOLD:
                    context = market_context_for_date(conn, row["date"])
                    inserted = log_signal(
                        conn,
                        "AUDUSD=X",
                        row["date"],
                        "FX_DISLOCATION",
                        zscore,
                        FX_ZSCORE_THRESHOLD,
                        context,
                    )
                    if inserted:
                        events.append((row["date"], "FX_DISLOCATION", zscore, context))

    return events


def summarize_signals():
    with db.get_connection() as conn:
        stmt = select(db.signal_events).order_by(db.signal_events.c.signal_type, db.signal_events.c.date.desc())
        df = pd.read_sql(stmt, conn)
        if df.empty:
            print("No signals detected.")
            return
        df["date"] = pd.to_datetime(df["date"]).dt.date
        counts = df["signal_type"].value_counts().to_dict()
        print("Signal summary:")
        for signal_type, count in counts.items():
            print(f"- {signal_type}: {count} fires")
            latest = df[df["signal_type"] == signal_type].iloc[0]
            print(f"  most recent: {latest['date']} | context: {latest['market_context']}")

        recent_cutoff = date.today() - timedelta(days=30)
        recent = df[df["date"] >= recent_cutoff]
        if recent.empty:
            print("\nNo signals in the last 30 days.")
        else:
            print("\nRecent signals (last 30 days):")
            for _, row in recent.iterrows():
                print(f"- {row['signal_type']} on {row['date']} (RECENT) | {row['market_context']}")


def main():
    created = run_signals()
    print(f"Logged {len(created)} new signal events.")
    summarize_signals()


if __name__ == "__main__":
    main()
