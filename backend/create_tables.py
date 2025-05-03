import asyncio
import asyncpg
import logging

from config import DB_CONFIG

logging.basicConfig(level=logging.INFO)

CREATE_TABLES_SQL = [
    """
    CREATE TABLE IF NOT EXISTS stock_prices (
        symbol TEXT NOT NULL,
        timestamp TIMESTAMPTZ NOT NULL,
        open DOUBLE PRECISION NOT NULL,
        high DOUBLE PRECISION NOT NULL,
        low DOUBLE PRECISION NOT NULL,
        close DOUBLE PRECISION NOT NULL,
        volume INTEGER NOT NULL,
        CONSTRAINT stock_prices_pkey UNIQUE (symbol, timestamp)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS financial_metrics (
        ticker VARCHAR(10) NOT NULL,
        metric_name TEXT NOT NULL,
        end_date DATE NOT NULL,
        year INT NOT NULL,
        quarter INT NOT NULL,
        value BIGINT NOT NULL,
        value_millions NUMERIC(15, 2),
        CONSTRAINT financial_metrics_pkey UNIQUE (ticker, metric_name, end_date) 
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS combined_stock_data (
        symbol                        TEXT NOT NULL,
        timestamp                     TIMESTAMPTZ NOT NULL,
        open                          DOUBLE PRECISION NOT NULL,
        high                          DOUBLE PRECISION NOT NULL,
        low                           DOUBLE PRECISION NOT NULL,
        close                         DOUBLE PRECISION NOT NULL,
        volume                        INTEGER NOT NULL,
    
        revenue                       NUMERIC(15,2),
        gross_profit                  NUMERIC(15,2),
        assets                        NUMERIC(15,2),
        commercial_paper              NUMERIC(15,2),
        shares_outstanding            NUMERIC(15,2),
        long_term_debt_current        NUMERIC(15,2),
        r_and_d_expense               NUMERIC(15,2),
        dividends_paid                NUMERIC(15,2),
        stockholders_equity           NUMERIC(15,2),
        net_income_loss               NUMERIC(15,2),
    
        PRIMARY KEY (symbol, timestamp)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS normalized_stock_data (
        symbol                        TEXT NOT NULL,
        timestamp                     TIMESTAMPTZ NOT NULL,
        open                          DOUBLE PRECISION NOT NULL,
        high                          DOUBLE PRECISION NOT NULL,
        low                           DOUBLE PRECISION NOT NULL,
        close                         DOUBLE PRECISION NOT NULL,
        volume                        INTEGER NOT NULL,
    
        revenue                       NUMERIC(15,2),
        gross_profit                  NUMERIC(15,2),
        assets                        NUMERIC(15,2),
        commercial_paper              NUMERIC(15,2),
        shares_outstanding            NUMERIC(15,2),
        long_term_debt_current        NUMERIC(15,2),
        r_and_d_expense               NUMERIC(15,2),
        dividends_paid                NUMERIC(15,2),
        stockholders_equity           NUMERIC(15,2),
        net_income_loss               NUMERIC(15,2),
    
        PRIMARY KEY (symbol, timestamp)
    );
    """
]

MERGE_STOCK_DATA ="""
INSERT INTO combined_stock_data (
    symbol, timestamp, open, high, low, close, volume,
    revenue, gross_profit, assets, commercial_paper, shares_outstanding,
    long_term_debt_current, r_and_d_expense, dividends_paid, stockholders_equity, net_income_loss
)
SELECT 
    sp.symbol,
    sp.timestamp,
    sp.open,
    sp.high,
    sp.low,
    sp.close,
    sp.volume,
    fm.revenue,
    fm.gross_profit,
    fm.assets,
    fm.commercial_paper,
    fm.shares_outstanding,
    fm.long_term_debt_current,
    fm.r_and_d_expense,
    fm.dividends_paid,
    fm.stockholders_equity,
    fm.net_income_loss
FROM stock_prices sp
LEFT JOIN LATERAL (
    SELECT 
        MAX(CASE WHEN metric_name = 'RevenueFromContractWithCustomerExcludingAssessedTax' THEN value_millions END) AS revenue,
        MAX(CASE WHEN metric_name = 'GrossProfit' THEN value_millions END) AS gross_profit,
        MAX(CASE WHEN metric_name = 'Assets' THEN value_millions END) AS assets,
        MAX(CASE WHEN metric_name = 'CommercialPaper' THEN value_millions END) AS commercial_paper,
        MAX(CASE WHEN metric_name = 'CommonStockSharesOutstanding' THEN value_millions END) AS shares_outstanding,
        MAX(CASE WHEN metric_name = 'LongTermDebtCurrent' THEN value_millions END) AS long_term_debt_current,
        MAX(CASE WHEN metric_name = 'ResearchAndDevelopmentExpense' THEN value_millions END) AS r_and_d_expense,
        MAX(CASE WHEN metric_name = 'PaymentsOfDividends' THEN value_millions END) AS dividends_paid,
        MAX(CASE WHEN metric_name = 'StockholdersEquity' THEN value_millions END) AS stockholders_equity,
        MAX(CASE WHEN metric_name = 'NetIncomeLoss' THEN value_millions END) AS net_income_loss
    FROM financial_metrics fm
    WHERE fm.ticker = sp.symbol
      AND fm.end_date = (
          SELECT MAX(end_date)
          FROM financial_metrics
          WHERE ticker = sp.symbol
            AND end_date <= sp.timestamp::date
      )
) fm ON true;

"""

CREATE_HYPERTABLES_SQL = [
    """
    SELECT create_hypertable('stock_prices', 'timestamp', 'symbol', 10,
                             migrate_data => true, if_not_exists => true);
    """,
    """
    SELECT create_hypertable('financial_metrics', 'end_date', 
                             migrate_data => true, 
                             if_not_exists => true);
    """,
    """
    SELECT create_hypertable('combined_stock_data', 'timestamp', 'symbol', 10, if_not_exists => TRUE, migrate_data => true);
    """
]


async def execute_batch(conn, sql_batch):
    try:
        for command in sql_batch:
            await conn.execute(command)
        logging.info(f"✅ Batch executed successfully")
    except Exception as e:
        logging.error(f"❌ Error executing batch: {e}")


async def init_db():
    logging.info("Connecting to database...")
    conn = await asyncpg.connect(**DB_CONFIG)

    try:
        logging.info("Creating tables...")
        batch_size = 50
        for i in range(0, len(CREATE_TABLES_SQL), batch_size):
            batch = CREATE_TABLES_SQL[i:i + batch_size]
            await execute_batch(conn, batch)
            await execute_batch(conn, [MERGE_STOCK_DATA])

        logging.info("Creating hypertables...")
        for i in range(0, len(CREATE_HYPERTABLES_SQL), batch_size):
            batch = CREATE_HYPERTABLES_SQL[i:i + batch_size]
            await execute_batch(conn, batch)

        logging.info("Tables and hypertables created successfully.")

    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(init_db())
