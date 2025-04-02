import asyncio
import logging
import threading
from datetime import datetime

from utils import get_end_date_time
from ibapi.contract import Contract


def create_contract(symbol):
    """Creates an IBKR contract for a stock symbol."""
    contract = Contract()
    contract.symbol = symbol
    contract.secType = "STK"
    contract.exchange = "SMART"
    contract.currency = "USD"
    return contract

async def fetch_historical_data(app, reqId, contract):
    """Fetch historical data for a stock symbol from IBKR API."""
    years_to_fetch = 30
    current_year = datetime.now().year

    for year in range(current_year, current_year - years_to_fetch, -1):
        end_date = get_end_date_time(year)

        try:
            app.reqHistoricalData(
                reqId=reqId,
                contract=contract,
                endDateTime=end_date,
                durationStr='1 Y',
                barSizeSetting='1 hour',
                whatToShow='TRADES',
                useRTH=1,
                formatDate=1,
                keepUpToDate=False,
                chartOptions=[]
            )
            logging.info(f"Requested historical data for {contract.symbol} ({year})")
            await asyncio.sleep(5)
        except Exception as e:
            logging.error(f"Error requesting historical data: {e}")
