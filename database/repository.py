import logging
from datetime import datetime

from utils import normalize_date


async def save_batch_to_db(pool, symbol, data_batch):
    """Save stock data into the database."""
    async with pool.acquire() as conn:
        async with conn.transaction():
            values = [(symbol, datetime.strptime(data[0], '%Y%m%d %H:%M:%S'), *data[1:]) for data in data_batch]
            await conn.executemany("""
                INSERT INTO stock_prices (symbol, timestamp, open, high, low, close, volume)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (symbol, timestamp) DO NOTHING;
            """, values)
    logging.info(f"Saved {len(data_batch)} records for {symbol}.")


async def save_financial_metrics_to_db(pool, symbol, data_batch):
    logging.info(f"Saving {len(data_batch)} financial metric records for {symbol}.")
    async with pool.acquire() as conn:
        async with conn.transaction():
            values = [
                (
                    ticker,
                    metric_name,
                    normalize_date(end),
                    year,
                    quarter,
                    value,
                    round(value / 1_000_000, 2)
                )
                for (ticker, metric_name, end, year, quarter, value) in data_batch
            ]

            await conn.executemany("""
                INSERT INTO financial_metrics (ticker, metric_name, end_date, year, quarter, value, value_millions)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (ticker, metric_name, end_date) DO NOTHING;
            """, values)

    logging.info(f"Saved {len(data_batch)} financial metric records for {symbol}.")
