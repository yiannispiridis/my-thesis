import asyncio
import logging

from config import tickers, tickers_with_cik, fundamental_concepts, YEARS_OF_DATA
from database.connection import db
from services.ibkr_data_manager import ibkr_data_manager
from services.sec_edgar_data_manager import sec_edgar_data_manager

logging.basicConfig(level=logging.INFO)

async def main():
    """Starts the IB API, fetches stock data, and saves it to the database."""
    await db.init_pool()
    await asyncio.sleep(5)

    await asyncio.gather(
        ibkr_data_manager(tickers, db.pool),
        #sec_edgar_data_manager(tickers_with_cik, fundamental_concepts, YEARS_OF_DATA, db.pool)
    )
    await db.close_pool()


if __name__ == "__main__":
    asyncio.run(main())
