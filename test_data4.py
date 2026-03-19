from logic_engine import calculate_kpis
from data_fetcher import fetch_dashboard_data

df = fetch_dashboard_data()
res = calculate_kpis(df)
c2 = res.get("Cohort 2", {})
reach = c2.get('reach', {})
for k, v in reach.items():
    print(f"{k}: {v}")
