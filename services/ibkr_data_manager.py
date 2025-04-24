import asyncio
import logging

from ibkr.data import create_contract, fetch_historical_data
from ibkr.data_processor import batch_data, save_batch
from ibkr.thread import start_api_thread
from ibkr.wrapper import IBApi


async def ibkr_data_manager(tickers, pool):
    app = IBApi()
    app.connect("127.0.0.1", 7497, clientId=1)
    start_api_thread(app)

    await asyncio.sleep(5)

    stocks = {ticker: create_contract(ticker) for ticker in tickers}
    tasks = [fetch_historical_data(app, contract, pool) for contract in stocks.values()]

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        logging.warning("⚠️ IBKR data fetching was cancelled")

    for symbol in batch_data:
        await save_batch(symbol, pool)

    logging.info("✅ Finished IBKR data orchestration")

    if app.data_callbacks:
        logging.info("Remaining callbacks:")
        for reqId, callback in app.data_callbacks.items():
            symbol = app.get_symbol_from_reqId(reqId)
            logging.info(f"reqId: {reqId}, Symbol: {symbol}, Callback: {callback}")

    # Disconnect only after ensuring all callbacks have been processed
    app.disconnect()