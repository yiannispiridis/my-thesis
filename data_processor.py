import logging
from database import db
from config import BATCH_SIZE, RETRY_LIMIT
import asyncio
from asyncpg.exceptions import TooManyConnectionsError

batch_data = {}

async def on_historical_data(symbol, data):
    """Processes historical data and stores it in batch."""
    logging.info(f"Processing {len(data)} records for {symbol}")

    if symbol not in batch_data:
        batch_data[symbol] = []

    batch_data[symbol].extend(data)

    if len(batch_data[symbol]) >= BATCH_SIZE:
        await save_batch(symbol)

async def save_batch(symbol):
    """Saves the batch to the database."""
    if symbol not in batch_data or not batch_data[symbol]:
        return

    retries = 0
    while retries < RETRY_LIMIT:
        try:
            await db.save_batch_to_db(symbol, batch_data[symbol])
            batch_data[symbol] = []  # Clear batch after saving
            return
        except TooManyConnectionsError:
            logging.warning(f"Too many connections for {symbol}. Retrying...")
            retries += 1
            await asyncio.sleep(1)
        except Exception as e:
            logging.error(f"Error saving batch for {symbol}: {e}")
            break
