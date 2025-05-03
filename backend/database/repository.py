import logging

import pandas as pd

from backend.utils import normalize_date, parse_date


async def save_batch_to_db(pool, symbol, data_batch):
    """Save stock data into the database."""
    async with pool.acquire() as conn:
        async with conn.transaction():
            values = [(symbol, parse_date(data[0]), *data[1:]) for data in data_batch]
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

async def fetch_combined_stock_data(pool):
    """Fetch all data from combined_stock_data."""
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT 
                symbol, timestamp, open, high, low, close, volume,
                revenue, gross_profit, assets, commercial_paper,
                shares_outstanding, long_term_debt_current,
                r_and_d_expense, dividends_paid, stockholders_equity,
                net_income_loss
            FROM combined_stock_data
            ORDER BY symbol, timestamp ASC;
        """)
    return rows

async def fetch_normalized_stock_data(pool):
    """Fetch all data from normalized_stock_data."""
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT 
                symbol, timestamp, open, high, low, close, volume,
                revenue, gross_profit, assets, commercial_paper,
                shares_outstanding, long_term_debt_current,
                r_and_d_expense, dividends_paid, stockholders_equity,
                net_income_loss
            FROM normalized_stock_data
            ORDER BY symbol, timestamp ASC;
        """)
    return rows



async def save_normalized_to_db(pool, data_batch):
    """Save normalized stock data into the normalized_stock_data table."""
    async with pool.acquire() as conn:
        async with conn.transaction():
            values = [
                (
                    row['symbol'],
                    row['timestamp'],
                    row['open'],
                    row['high'],
                    row['low'],
                    row['close'],
                    row['volume'],
                    row['revenue'],
                    row['gross_profit'],
                    row['assets'],
                    row['commercial_paper'],
                    row['shares_outstanding'],
                    row['long_term_debt_current'],
                    row['r_and_d_expense'],
                    row['dividends_paid'],
                    row['stockholders_equity'],
                    row['net_income_loss']
                ) for _, row in data_batch.iterrows()
            ]
            await conn.executemany("""
                INSERT INTO normalized_stock_data (
                    symbol, timestamp, open, high, low, close, volume,
                    revenue, gross_profit, assets, commercial_paper,
                    shares_outstanding, long_term_debt_current,
                    r_and_d_expense, dividends_paid, stockholders_equity,
                    net_income_loss
                ) 
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
                ON CONFLICT (symbol, timestamp) DO NOTHING;
            """, values)

    logging.info(f"Saved {len(data_batch)} normalized records to normalized_stock_data.")


async def get_stock_data_df(pool):
    rows = await fetch_combined_stock_data(pool)
    data = [dict(row) for row in rows]
    return pd.DataFrame(data)

async def get_normalized_data_df(pool):
    rows = await fetch_normalized_stock_data(pool)
    data = [dict(row) for row in rows]
    return pd.DataFrame(data)