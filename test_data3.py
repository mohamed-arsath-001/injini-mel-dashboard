from logic_engine import calculate_kpis
from data_fetcher import fetch_dashboard_data
import json

df = fetch_dashboard_data()
res = calculate_kpis(df)

c2 = res.get("Cohort 2", {})
print("SA Schools from Reach obj:", c2.get('reach', {}).get('sa_schools', []))
