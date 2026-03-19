import os
from pyairtable import Api
from dotenv import load_dotenv

load_dotenv(override=True)
api = Api(os.getenv('AIRTABLE_PAT'))
base_id = 'app3KJMspt7z8qy9M' # Cohort 2
table = api.table(base_id, 'Monthly reporting')
records = table.all()

for r in records[:10]:
    sa = r['fields'].get('Subscription - South African schools')
    if sa is not None:
        print(f"Value: {sa}, Type: {type(sa)}")
