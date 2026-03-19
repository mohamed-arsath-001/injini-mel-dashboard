import pandas as pd
from pyairtable import Api
import os
from dotenv import load_dotenv

load_dotenv(override=True)
api = Api(os.getenv('AIRTABLE_PAT'))
BASE_IDS = {
    'Cohort 1': 'app5MKMARnZAInXVJ',
    'Cohort 2': 'app3KJMspt7z8qy9M',
    'Cohort 3': 'appBhlIJDu8JvaWxB',
    'Cohort 4': 'appzHpcS4aenhjZ8V'
}

for c, b in BASE_IDS.items():
    table = api.table(b, 'Monthly reporting')
    records = table.all()
    if records:
        # Check first 5 records just in case
        all_keys = set()
        for r in records[:5]:
            all_keys.update(r['fields'].keys())
        
        matches = [k for k in all_keys if 'school' in k.lower() or 'sa ' in k.lower() or 'south' in k.lower() or 'subscrip' in k.lower()]
        print(f"{c} matches: {matches}")
