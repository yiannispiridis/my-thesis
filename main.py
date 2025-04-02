import asyncio
import logging

from ibkr_thread import start_api_thread
from ibkr_wrapper import IBApi
from database import db
from data_processor import on_historical_data, save_batch, batch_data
from ibkr_data import fetch_historical_data, create_contract


async def main():
    """Starts the IB API, fetches stock data, and saves it to the database."""
    app = IBApi()

    app.data_callbacks = {
        1: on_historical_data,
        2: on_historical_data,
        3: on_historical_data,
    }

    app.connect("127.0.0.1", 7497, clientId=1)
    await db.init_pool()
    start_api_thread(app)
    await asyncio.sleep(5)

    stocks = {
        1: create_contract("MSFT"),
        2: create_contract("GOOGL"),
        3: create_contract("AAPL"),
    }

    tasks = [fetch_historical_data(app, reqId, contract) for reqId, contract in stocks.items()]

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        logging.warning("Tasks were cancelled.")

    for symbol in batch_data:
        await save_batch(symbol)

    await asyncio.sleep(15)
    app.disconnect()
    await db.close_pool()


if __name__ == "__main__":
    asyncio.run(main())
