import os
import json
import requests
from datetime import datetime
from config import SEC_EDGAR_API_URL, USER_AGENT, SEC_DATA_DIR

def fetch_sec_data(cik):
    headers = {'User-Agent': USER_AGENT}
    url = SEC_EDGAR_API_URL.format(cik=cik)
    filename = f"{SEC_DATA_DIR}/{cik}_{datetime.now().strftime('%Y-%m-%d')}.json"

    if os.path.exists(filename):
        with open(filename, 'r') as file:
            return json.load(file)
    else:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()

        with open(filename, 'w') as file:
            json.dump(data, file)

    return data