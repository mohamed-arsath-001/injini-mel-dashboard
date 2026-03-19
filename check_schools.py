import os
from pyairtable import Api
from dotenv import load_dotenv

load_dotenv(override=True)
api = Api(os.getenv('AIRTABLE_PAT'))
BASE_IDS = {
    'C2': 'app3KJMspt7z8qy9M', 'C3': 'appBhlIJDu8JvaWxB', 'C4': 'appzHpcS4aenhjZ8V'
}

for c, b in BASE_IDS.items():
    table = api.table(b, 'Monthly reporting')
    records = table.all()
    total_sa = 0
    total_q13 = 0
    for r in records:
        fields = {k.strip(): v for k, v in r['fields'].items()}
        sa = fields.get('Subscription - South African schools', 0)
        q13 = fields.get('Subscription - Q1-3 schools', fields.get('Subscription- Q1-3 Schools Students', 0))
        if isinstance(sa, (int, float)): total_sa += sa
        if isinstance(q13, (int, float)): total_q13 += q13
    print(f"[{c}] SA Schools Total: {total_sa}, Q1-3 Schools Total: {total_q13}")
