import asyncpg
from datetime import datetime

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "user": "postgres",
    "password": "yourpassword",
    "database": "stocks"
}

async def save_to_db(symbol, data):
    """Save stock data to TimescaleDB asynchronously."""
    timestamp_str = data[0]
    timestamp = datetime.strptime(timestamp_str, '%Y%m%d %H:%M:%S')

    async with asyncpg.create_pool(**DB_CONFIG) as pool:
        async with pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO stock_prices (symbol, timestamp, open, high, low, close, volume)
                VALUES ($1, $2, $3, $4, $5, $6, $7);
            """, symbol, timestamp, data[1], data[2], data[3], data[4], data[5])

async def get_latest_timestamp(symbol):
    """Get the latest timestamp for the given symbol from the database."""
    async with asyncpg.create_pool(**DB_CONFIG) as pool:
        async with pool.acquire() as conn:
            result = await conn.fetch("""
                SELECT MAX(timestamp) FROM stock_prices WHERE symbol = $1;
            """, symbol)
            return result[0]['max'] if result else None
