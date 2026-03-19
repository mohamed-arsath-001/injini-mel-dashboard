from logic_engine import calculate_kpis
from data_fetcher import fetch_dashboard_data
import json

df = fetch_dashboard_data()
res = calculate_kpis(df)

for i in range(1, 5):
    c_dat = res.get(f"Cohort {i}", {})
    print(f"Cohort {i} SA Schools: {c_dat.get('reach_sa_schools', [])}")
    print(f"Cohort {i} Q13 Schools: {c_dat.get('reach_q13_schools', [])}")
    
print(f"Program SA Schools: {res.get('program_series', {}).get('reach_sa_schools', [])}")
