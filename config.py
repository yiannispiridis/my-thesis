from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parent

BATCH_SIZE = 50
MAX_CONCURRENT_TASKS = 5
RETRY_LIMIT = 3
SEC_EDGAR_API_URL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
USER_AGENT = "Thesis Thesis thesis@gmail.com"
YEARS_OF_DATA = 10
SEC_DATA_DIR = PROJECT_ROOT/ "sec_data"

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "user": "postgres",
    "password": "yourpassword",
    "database": "stocks"
}

tickers = [
    # Large-Cap
    "AAPL", "MSFT", "AMZN", "GOOG", "NVDA",
    "META", "JPM", "JNJ", "PG", "XOM",

    # Mid-Cap
    "TWLO", "DDOG", "ROKU", "UBER", "ZM",
    "DOCU", "SNOW", "TEAM", "PLTR", "FSLY",

    # Small-Cap
    "LMND", "GPRO", "NKLAQ",
    "BARK", "OPEN", "SNDL", "RIOT", "MARA",

    # Diversified Sector Picks
    "COST", "WMT", "BA", "GE", "T",
    "VZ", "KO", "PFE", "MRK", "ORCL"
]

fundamental_concepts = ['RevenueFromContractWithCustomerExcludingAssessedTax', 'GrossProfit',
                        'Assets', 'CommercialPaper', 'CommonStockSharesOutstanding',
                        'LongTermDebtCurrent', 'ResearchAndDevelopmentExpense',
                        'PaymentsOfDividends', 'StockholdersEquity', 'NetIncomeLoss']  # old taxes 'TaxesPayableCurrent'

tickers_with_cik = {
    # Big Tech & AI
    "AAPL": "0000320193",  # Apple
    "MSFT": "0000789019",  # Microsoft
    "GOOG": "0001652044",  # Alphabet (Google Class C)
    "AMZN": "0001018724",  # Amazon
    "NVDA": "0001045810",  # NVIDIA
    "META": "0001326801",  # Meta Platforms
    "TSLA": "0001318605",  # Tesla
    "ORCL": "0001341439",  # Oracle
    "ADBE": "0000796343",  # Adobe
    "CRM": "0001108524",  # Salesforce

    # Financials & Fintech
    "JPM": "0000019617",  # JPMorgan Chase
    "MA": "0001141391",  # Mastercard
    "V": "0001403161",  # Visa
    "BAC": "0000070858",  # Bank of America
    "BRK.B": "0001067983",  # Berkshire Hathaway (Class B)
    "GS": "0000886982",  # Goldman Sachs
    "MS": "0000895421",  # Morgan Stanley
    "AXP": "0000004962",  # American Express
    "PYPL": "0001633917",  # PayPal
    "SCHW": "0000316709",  # Charles Schwab

    # Healthcare
    "JNJ": "0000200406",  # Johnson & Johnson
    "PFE": "0000078003",  # Pfizer
    "LLY": "0000059478",  # Eli Lilly
    "MRK": "0000310158",  # Merck & Co
    "UNH": "0000731766",  # UnitedHealth Group
    "ABBV": "0001551152",  # AbbVie
    "TMO": "0000097745",  # Thermo Fisher Scientific
    "ABT": "0000001800",  # Abbott Laboratories
    "BMY": "0000014272",  # Bristol-Myers Squibb

    # Consumer Goods & Retail
    "WMT": "0000104169",  # Walmart
    "PG": "0000080424",  # Procter & Gamble
    "PEP": "0000077476",  # PepsiCo
    "KO": "0000021344",  # Coca-Cola
    "COST": "0000909832",  # Costco
    "HD": "0000354950",  # Home Depot
    "MCD": "0000063908",  # McDonald's
    "NKE": "0000320187",  # Nike
    "TGT": "0000027419",  # Target
    "LOW": "0000060667",  # Lowe's

    # Energy & Industrials
    "XOM": "0000034088",  # ExxonMobil
    "CVX": "0000093410",  # Chevron
}
