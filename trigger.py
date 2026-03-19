import sys
import os
sys.path.append('c:\\Zestflow\\MEL')
from data_fetcher import fetch_dashboard_data

try:
    df = fetch_dashboard_data()
    print("Fetched successfully. df length:", len(df))
except Exception as e:
    import traceback
    with open('main_error.log', 'w', encoding='utf-8') as f:
        traceback.print_exc(file=f)
    print("Caught error in main. Check main_error.log")
