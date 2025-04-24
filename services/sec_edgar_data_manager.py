import logging
from database.repository import save_financial_metrics_to_db
from sec_edgar.client import fetch_sec_data
from sec_edgar.transformer import extract_concept_data

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def convert_to_data_batch(symbol, metrics_dict):
    data_batch = []
    for metric_name, df in metrics_dict.items():
        for _, row in df.iterrows():
            data_batch.append((symbol, metric_name, row['end'], row['year'], row['quarter'], row['val']))
    return data_batch

async def process_ticker(symbol, cik, concepts, years, pool):
    data = fetch_sec_data(cik)
    metrics = {}

    for concept in concepts:
        df = extract_concept_data(data, concept, years)
        metrics[concept] = df

    batch = convert_to_data_batch(symbol, metrics)
    await save_financial_metrics_to_db(pool, symbol, batch)

    return metrics


async def sec_edgar_data_manager(tickers_with_cik, concepts, years, pool):
    for symbol, cik in tickers_with_cik.items():
        try:
            logging.info(f"\n⏳ Starting processing for {symbol} ({cik})")
            metrics = await process_ticker(symbol, cik, concepts, years, pool)
            for concept, df in metrics.items():
                if not df.empty:
                    logging.info(f"\n📊 {concept} for {symbol} (last {years} years):\n{df.to_string(index=False, float_format='{:,.2f}'.format)}")
                else:
                    logging.warning(f"⚠️  No data found for {concept} - {symbol}")
        except Exception as e:
            logging.error(f"❌ Error processing {symbol}: {e}", exc_info=True)

