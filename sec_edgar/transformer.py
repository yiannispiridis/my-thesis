import pandas as pd
from datetime import datetime, timedelta

def extract_concept_data(data, concept, years):
    facts = data.get('facts', {}).get('us-gaap', {})
    if concept not in facts:
        return pd.DataFrame()

    cutoff_date = datetime.now() - timedelta(days=365 * years)
    entries = []
    for unit_data in facts[concept]['units'].values():
        entries.extend(unit_data)

    df = pd.DataFrame(entries)
    df = df[df['form'].isin(['10-Q', '10-K'])]
    df['end'] = pd.to_datetime(df['end'])
    df = df[df['end'] >= cutoff_date]
    df = df.sort_values('filed').drop_duplicates('end', keep='last').sort_values('end')
    df['quarter'] = df['end'].dt.quarter
    df['year'] = df['end'].dt.year
    df['val_millions'] = df['val'] / 1_000_000

    return df[['end', 'val', 'quarter', 'year', 'val_millions']]