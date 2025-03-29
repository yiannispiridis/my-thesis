import threading
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract

class IBApi(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self.data_queues = {}

    def historicalData(self, reqId, bar):
        if reqId in self.data_queues:
            self.data_queues[reqId].put((bar.date, bar.open, bar.high, bar.low, bar.close, bar.volume))

    def error(self, reqId, errorCode, errorString):
        print(f"Error {reqId}: {errorCode}, {errorString}")

def create_contract(symbol):
    contract = Contract()
    contract.symbol = symbol
    contract.secType = "STK"
    contract.exchange = "SMART"
    contract.currency = "USD"
    return contract

def run_ib_api(app):
    app.run()

def start_api_thread(app):
    api_thread = threading.Thread(target=run_ib_api, args=(app,), daemon=True)
    api_thread.start()
    return api_thread
