import logging
from datetime import datetime, date

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

def normalize_date(end):
    if isinstance(end, date):
        return end  # Already a date
    if isinstance(end, datetime):
        return end.date()
    if isinstance(end, str):
        try:
            return datetime.strptime(end, '%Y-%m-%d').date()
        except ValueError:
            try:
                return datetime.strptime(end, '%Y-%m-%d %H:%M:%S').date()
            except ValueError:
                if len(end) == 4 and end.isdigit():  # e.g. "2020"
                    return datetime.strptime(end + '-12-31', '%Y-%m-%d').date()
                raise
    if isinstance(end, int):  # e.g. 2020
        return date(end, 12, 31)
    raise TypeError(f"Unsupported date format: {end} ({type(end)})")
