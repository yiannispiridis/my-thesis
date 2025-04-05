import logging
import threading

from database import db
from config import BATCH_SIZE, RETRY_LIMIT
import asyncio
from asyncpg.exceptions import TooManyConnectionsError
from repository import save_batch_to_db

batch_data = {}
lock = threading.Lock()

async def on_historical_data(symbol, data):
    """Callback function that processes historical data and stores it in batch."""
    logging.info(f"Processing {len(data)} records for {symbol}")

    if symbol not in batch_data:
        clear_batch_data(symbol)
    save_data(symbol, data)
    if len(batch_data[symbol]) >= BATCH_SIZE:
        await save_batch(symbol)

async def save_batch(symbol):
    """Saves the batch to the database."""

    if symbol not in batch_data or not batch_data[symbol]:
        return
    logging.info(f"To be saved batch data of symbol {symbol}")
    retries = 0
    while retries < RETRY_LIMIT:
        try:
            logging.info(f"Trying to save data for {symbol}")
            await save_batch_to_db(db.pool, symbol, batch_data[symbol])
            clear_batch_data(symbol)
            return
        except TooManyConnectionsError:
            logging.warning(f"Too many connections for {symbol}. Retrying...")
            retries += 1
            await asyncio.sleep(1)
        except Exception as e:
            logging.error(f"Error saving batch for {symbol}: {e}")
            break


def save_data(symbol, data):
    with lock:
        if symbol not in batch_data:
            batch_data[symbol] = []
        batch_data[symbol].extend(data)

def clear_batch_data(symbol):
    with lock:
        batch_data[symbol] = []