import asyncio
import logging
from ibapi.client import EClient
from ibapi.wrapper import EWrapper

logging.basicConfig(level=logging.INFO)

class IBApi(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self.loop = asyncio.get_event_loop()  # Use the global event loop
        self.data_callbacks = {}  # Stores callbacks for each request ID
        self.data_store = {}  # Stores data per request ID

    def get_symbol_from_reqId(self, reqId):
        """Maps request IDs to stock symbols."""
        mapping = {1: "MSFT", 2: "GOOGL", 3: "AAPL"}
        return mapping.get(reqId, "UNKNOWN")

    def historicalData(self, reqId, bar):
        """Handles incoming historical data from IBKR API."""
        symbol = self.get_symbol_from_reqId(reqId)
        if reqId not in self.data_store:
            self.data_store[reqId] = []

        data = (bar.date, bar.open, bar.high, bar.low, bar.close, bar.volume)
        self.data_store[reqId].append(data)

        logging.info(f"Stored data for {symbol} (reqId {reqId}): {len(self.data_store[reqId])} records")

    def historicalDataEnd(self, reqId, start, end):
        """Triggered when historical data transmission is complete."""
        symbol = self.get_symbol_from_reqId(reqId)
        data = self.data_store.get(reqId, [])

        logging.info(f"Finished receiving data for {symbol} (reqId {reqId}) with {len(data)} records")

        if reqId in self.data_callbacks:
            callback = self.data_callbacks[reqId]
            logging.info(f"Executing callback {callback} for {symbol} with {len(data)} records")

            if asyncio.iscoroutinefunction(callback):
                # Run the coroutine in the same event loop
                self.loop.create_task(callback(symbol, data))
            else:
                # If it's a normal function, execute it asynchronously in the main thread
                self.loop.run_in_executor(None, callback, symbol, data)

    def error(self, reqId, errorCode, errorString):
        symbol = self.get_symbol_from_reqId(reqId)
        logging.error(f"Error for {symbol} (reqId {reqId}): {errorCode}, {errorString}")

    def connectionClosed(self):
        logging.warning("Connection to IBKR API closed.")
