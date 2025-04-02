import logging
from datetime import datetime

import pytz


def get_end_date_time(year):
    """Get the last day of the given year in UTC format for IBKR."""
    tz_utc = pytz.utc
    end_date = datetime(year, 12, 31, 23, 59, 0)
    end_date_utc = tz_utc.localize(end_date)

    # Convert to IBKR's UTC format (YYYYMMDD-HH:MM:SS)
    formatted_end_date = end_date_utc.strftime('%Y%m%d-%H:%M:%S')
    logging.info(f"Date is {formatted_end_date}")
    return formatted_end_date