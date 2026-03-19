from logic_engine import calculate_kpis
from data_fetcher import fetch_dashboard_data
import json

df = fetch_dashboard_data()
res = calculate_kpis(df)

c2 = res.get("Cohort 2", {})
print("Months:", c2.get('reach_months'))
print("Total Learners:", c2.get('reach_total_learners'))
print("SA Schools:", c2.get('reach_sa_schools'))
