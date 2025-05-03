import pandas as pd
from sklearn.preprocessing import StandardScaler

from backend.database.repository import fetch_combined_stock_data, save_normalized_to_db


async def normalize_combined_stock_data(pool):
    """Fetch data, convert to DataFrame, and normalize."""
    rows = await fetch_combined_stock_data(pool)

    data = [dict(row) for row in rows]

    df = pd.DataFrame(data)

    columns_to_normalize = [
        'open', 'high', 'low', 'close', 'volume',
        'revenue', 'gross_profit', 'assets', 'commercial_paper',
        'shares_outstanding', 'long_term_debt_current',
        'r_and_d_expense', 'dividends_paid', 'stockholders_equity',
        'net_income_loss'
    ]

    scaler = StandardScaler()

    df[columns_to_normalize] = scaler.fit_transform(df[columns_to_normalize])
    df[columns_to_normalize] = df[columns_to_normalize].round(3)

    return df

async def save_normalized_data(pool):
    df = await normalize_combined_stock_data(pool)
    await save_normalized_to_db(pool, df)
