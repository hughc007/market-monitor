from datetime import datetime, date
from pathlib import Path
from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    Integer,
    String,
    Float,
    Date,
    DateTime,
    select,
    func,
    and_,
)

DATABASE_FILE = Path("market_monitor.db")
DATABASE_URL = f"sqlite:///{DATABASE_FILE}"
metadata = MetaData()

instruments = Table(
    "instruments",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("ticker", String, unique=True, nullable=False),
    Column("display_name", String, nullable=False),
    Column("asset_class", String, nullable=False),
    Column("created_at", DateTime, default=datetime.utcnow, nullable=False),
)

daily_prices = Table(
    "daily_prices",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("ticker", String, nullable=False, index=True),
    Column("date", Date, nullable=False, index=True),
    Column("open", Float),
    Column("high", Float),
    Column("low", Float),
    Column("close", Float),
    Column("volume", Float),
    Column("created_at", DateTime, default=datetime.utcnow, nullable=False),
)

daily_returns = Table(
    "daily_returns",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("ticker", String, nullable=False, index=True),
    Column("date", Date, nullable=False, index=True),
    Column("log_return", Float),
    Column("created_at", DateTime, default=datetime.utcnow, nullable=False),
)

rolling_metrics = Table(
    "rolling_metrics",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("ticker", String, nullable=False, index=True),
    Column("date", Date, nullable=False, index=True),
    Column("metric_name", String, nullable=False),
    Column("metric_value", Float),
    Column("window", Integer),
    Column("created_at", DateTime, default=datetime.utcnow, nullable=False),
)

signal_events = Table(
    "signal_events",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("ticker", String, nullable=False, index=True),
    Column("date", Date, nullable=False, index=True),
    Column("signal_type", String, nullable=False),
    Column("signal_value", Float),
    Column("threshold", Float),
    Column("market_context", String),
    Column("created_at", DateTime, default=datetime.utcnow, nullable=False),
)


def get_connection():
    engine = create_engine(DATABASE_URL, future=True)
    return engine.connect()


def create_tables():
    engine = create_engine(DATABASE_URL, future=True)
    metadata.create_all(engine)


def insert_instruments(instrument_map):
    if not instrument_map:
        return

    with get_connection() as conn:
        with conn.begin():
            for ticker, info in instrument_map.items():
                stmt = select(func.count()).select_from(instruments).where(instruments.c.ticker == ticker)
                existing = conn.execute(stmt).scalar_one()
                if existing:
                    continue
                conn.execute(
                    instruments.insert().values(
                        ticker=ticker,
                        display_name=info.get("display_name", ""),
                        asset_class=info.get("asset_class", ""),
                        created_at=datetime.utcnow(),
                    )
                )


def data_exists_for_range(ticker, start_date, end_date):
    if isinstance(start_date, str):
        start_date = date.fromisoformat(start_date)
    if isinstance(end_date, str):
        end_date = date.fromisoformat(end_date)

    with get_connection() as conn:
        stmt = (
            select(func.count())
            .select_from(daily_prices)
            .where(
                and_(
                    daily_prices.c.ticker == ticker,
                    daily_prices.c.date >= start_date,
                    daily_prices.c.date <= end_date,
                )
            )
        )
        count = conn.execute(stmt).scalar_one()
        return count > 0


def get_latest_date_for_ticker(ticker):
    with get_connection() as conn:
        stmt = select(func.max(daily_prices.c.date)).where(daily_prices.c.ticker == ticker)
        return conn.execute(stmt).scalar_one()
