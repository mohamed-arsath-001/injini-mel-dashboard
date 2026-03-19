from logic_engine import calculate_kpis
from data_fetcher import fetch_dashboard_data

df = fetch_dashboard_data()
res = calculate_kpis(df)
c2 = res.get("Cohort_Detail", {}).get("Cohort 2", {})
print("SA Schools:", c2.get('reach', {}).get('sa_schools', []))
