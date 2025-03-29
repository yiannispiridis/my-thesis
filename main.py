import asyncio
from datetime import datetime
from api_client import IBApi, create_contract, start_api_thread
from database import save_to_db, get_latest_timestamp
from queue import Queue


async def fetch_historical_data(app, reqId, contract, start_date=None):
    loop = asyncio.get_running_loop()

    latest_timestamp = await get_latest_timestamp(contract.symbol)

    if start_date:
        start_date_str = start_date.strftime('%Y%m%d %H:%M:%S')
    else:
        start_date_str = latest_timestamp.strftime('%Y%m%d %H:%M:%S') if latest_timestamp else ""

    if latest_timestamp and start_date_str <= latest_timestamp.strftime('%Y%m%d %H:%M:%S'):
        print(f"Already have data up to {latest_timestamp}. Fetching older data from {start_date_str}")
    else:
        print(f"Fetching data starting from {start_date_str}")

    app.data_queues[reqId] = Queue()

    app.reqHistoricalData(
        reqId=reqId,
        contract=contract,
        endDateTime='',
        durationStr='1 Y',
        barSizeSetting='1 hour',
        whatToShow='MIDPOINT',
        useRTH=1,
        formatDate=1,
        keepUpToDate=False,
        chartOptions=[]
    )

    while True:
        try:
            data = await loop.run_in_executor(None, app.data_queues[reqId].get)
            if data:
                print(
                    f"[{contract.symbol}] {data[0]} | Open: {data[1]} | High: {data[2]} | Low: {data[3]} | Close: {data[4]} | Volume: {data[5]}")
                await save_to_db(contract.symbol, data)
        except asyncio.CancelledError:
            break


async def main():
    """Main async function to start IBAPI and fetch data for multiple stocks."""
    app = IBApi()
    app.connect("127.0.0.1", 7497, clientId=1)
    api_thread = start_api_thread(app)
    await asyncio.sleep(2)

    stocks = {
        1: create_contract("AAPL"),
        2: create_contract("GOOGL"),
        3: create_contract("MSFT"),
    }

    tasks = []
    for reqId, contract in stocks.items():
        tasks.append(asyncio.create_task(fetch_historical_data(app, reqId, contract, datetime(2025, 1, 1))))

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        print("Tasks were cancelled.")

    app.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
