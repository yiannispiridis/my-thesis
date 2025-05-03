import asyncio
import logging
from datetime import datetime

from backend.config import YEARS_OF_DATA
from backend.ibkr.data_processor import create_on_historical_data_callback
from backend.utils import get_end_date_time
from ibapi.contract import Contract

def create_contract(symbol):
    """Creates an IBKR contract for a stock symbol."""
    contract = Contract()
    contract.symbol = symbol
    contract.secType = "STK"
    contract.exchange = "SMART"
    contract.currency = "USD"
    return contract

async def fetch_historical_data(app,contract, pool):
    """Fetch historical data for a stock symbol from IBKR API."""
    current_year = datetime.now().year

    for year in range(current_year, current_year - YEARS_OF_DATA, -1):
        end_date = get_end_date_time(year)
        reqId = app.create_symbol_reqId_mapping(contract.symbol)
        a_callback = create_on_historical_data_callback(pool)
        app.add_callback(reqId, a_callback)
        try:
            app.reqHistoricalData(
                reqId=reqId,
                contract=contract,
                endDateTime=end_date,
                durationStr='1 Y',
                barSizeSetting='1 week',
                whatToShow='TRADES',
                useRTH=1,
                formatDate=1,
                keepUpToDate=False,
                chartOptions=[]
            )
            logging.info(f"Requested historical data for {contract.symbol} ({year} and request id {reqId})")
            await asyncio.sleep(10)
        except Exception as e:
            logging.error(f"Error requesting historical data: {e}")
