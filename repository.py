import logging
from datetime import datetime

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
