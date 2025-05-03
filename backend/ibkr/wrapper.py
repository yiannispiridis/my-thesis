import asyncio
import logging
from ibapi.client import EClient
from ibapi.wrapper import EWrapper


class IBApi(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self.loop = asyncio.get_event_loop()  # Use the global event loop
        self.data_callbacks = {}  # Stores callbacks for each request ID
        self.data_store = {}  # Stores data per request ID
        self.mapping = {}  # Used for symbol-reqId mapping
        self.req_id_counter = 1  # Initialize the reqId counter

    def create_symbol_reqId_mapping(self, symbol):
        """Generates unique reqIds and links them to symbols."""
        req_id = self.req_id_counter
        self.mapping[req_id] = symbol
        self.req_id_counter += 1
        return req_id

    def get_symbol_from_reqId(self, reqId):
        """Maps request IDs to stock symbols."""
        return self.mapping.get(reqId, "UNKNOWN")

    def add_callback(self, reqId, callback):
        """Add a callback to the data_callbacks dictionary for a specific reqId."""
        if reqId not in self.data_callbacks:
            self.data_callbacks[reqId] = []
        logging.info(f"Adding callback {callback} with reqId {reqId}")
        self.data_callbacks[reqId].append(callback)

    def trigger_callbacks(self, reqId, data):
        """Trigger all callbacks associated with a specific reqId."""
        if reqId in self.data_callbacks:
            symbol = self.get_symbol_from_reqId(reqId)
            for callback in self.data_callbacks[reqId]:
                if asyncio.iscoroutinefunction(callback):
                    self.loop.create_task(callback(symbol, data))
                else:
                    self.loop.run_in_executor(None, callback, symbol, data)

    def historicalData(self, reqId, bar):
        """Handles incoming historical data from IBKR API."""
        symbol = self.get_symbol_from_reqId(reqId)
        if reqId not in self.data_store:
            self.data_store[reqId] = []

        data = (bar.date, bar.open, bar.high, bar.low, bar.close, bar.volume)
        self.data_store[reqId].append(data)

        logging.debug(f"Stored data for {symbol} (reqId {reqId}): {len(self.data_store[reqId])} records")

    def historicalDataEnd(self, reqId, start, end):
        """Triggered when historical data transmission is complete."""
        symbol = self.get_symbol_from_reqId(reqId)
        data = self.data_store.get(reqId, [])

        logging.info(f"Finished receiving data for {symbol} (reqId {reqId}) with {len(data)} records")

        if reqId in self.data_callbacks:
            callback = self.data_callbacks[reqId]
            logging.info(f"Executing callback {callback} for {symbol} with {len(data)} records and reqId{reqId}")
            self.trigger_callbacks(reqId, data)
            del self.data_callbacks[reqId]
        else:
            logging.warning(f"No callback registered for reqId {reqId}")


    def error(self, reqId, errorCode, errorString):
        symbol = self.get_symbol_from_reqId(reqId)
        logging.error(f"Error for {symbol} (reqId {reqId}): {errorCode}, {errorString}")

    def connectionClosed(self):
        logging.warning("Connection to IBKR API closed.")
