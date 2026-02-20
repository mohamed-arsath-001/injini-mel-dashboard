import os
from pyairtable import Api
from dotenv import load_dotenv

load_dotenv(override=True)
api = Api(os.getenv('AIRTABLE_PAT'))

bases = {
    'Cohort 1': 'app5MKMARnZAInXVJ',
    'Cohort 2': 'app3KJMspt7z8qy9M',
    'Cohort 3': 'appBhlIJDu8JvaWxB',
    'Cohort 4': 'appzHpcS4aenhjZ8V'
}

with open('fields_report.txt', 'w', encoding='utf-8') as f:
    for name, bid in bases.items():
        try:
            table = api.table(bid, 'Monthly reporting')
            recs = table.all()
            if recs:
                all_fields = set()
                for r in recs:
                    all_fields.update(r['fields'].keys())
                f.write(f'\n=== {name} ({len(recs)} records, {len(all_fields)} fields) ===\n')
                for field in sorted(all_fields, key=str.lower):
                    f.write(f'  - {field}\n')
            else:
                f.write(f'\n=== {name}: 0 records ===\n')
        except Exception as e:
            f.write(f'\n=== {name}: ERROR: {e} ===\n')

print("Done! See fields_report.txt")
