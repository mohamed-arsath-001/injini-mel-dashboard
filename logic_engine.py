import pandas as pd
import re
from datetime import datetime
from statistics import median


def parse_reporting_month(month_str):
    """
    Parses 'Report X - Month YYYY' into a datetime object for sorting.
    Example: 'Report 6 - September 2023' -> datetime(2023, 9, 1)
    """
    if not isinstance(month_str, str) or month_str == 'Unknown':
        return datetime.min

    match = re.search(r'(\w+)\s+(\d{4})', month_str)
    if match:
        try:
            date_str = f"{match.group(1)} {match.group(2)}"
            return datetime.strptime(date_str, '%B %Y')
        except ValueError:
            pass
    return datetime.min


# ──────────────────────────────────────────────
#  Tiered Growth Helpers
# ──────────────────────────────────────────────

def _tiered_sales_growth(series):
    """
    5-tier sales growth based on number of monthly data points.
    Returns float (percentage) or "Insufficient Data".
    """
    n = len(series)
    if n < 6:
        return "Insufficient Data"

    if n <= 11:
        # 6-11 months: last 3 vs first 3
        first = series.iloc[:3].mean()
        last = series.iloc[-3:].mean()
    elif n <= 17:
        # 12-17 months: last 6 vs first 6
        first = series.iloc[:6].mean()
        last = series.iloc[-6:].mean()
    elif n <= 23:
        # 18-23 months: annualised residual vs first 12
        first = series.iloc[:12].mean()
        residual_months = n - 12
        residual_sum = series.iloc[12:].sum()
        last = (residual_sum / residual_months) * 12 if residual_months > 0 else 0
    else:
        # 24+ months: last 12 vs first 12
        first = series.iloc[:12].mean()
        last = series.iloc[-12:].mean()

    if first == 0:
        return 0.0
    return round(((last - first) / first) * 100, 1)


def _tiered_profit_growth(series):
    """
    Same tiers as sales growth but uses ABS(denominator) for losses.
    """
    n = len(series)
    if n < 6:
        return "Insufficient Data"

    if n <= 11:
        first = series.iloc[:3].mean()
        last = series.iloc[-3:].mean()
    elif n <= 17:
        first = series.iloc[:6].mean()
        last = series.iloc[-6:].mean()
    elif n <= 23:
        first = series.iloc[:12].mean()
        residual_months = n - 12
        residual_sum = series.iloc[12:].sum()
        last = (residual_sum / residual_months) * 12 if residual_months > 0 else 0
    else:
        first = series.iloc[:12].mean()
        last = series.iloc[-12:].mean()

    denom = abs(first)
    if denom == 0:
        return 0.0
    return round(((last - first) / denom) * 100, 1)


# ──────────────────────────────────────────────
#  Main KPI Calculator
# ──────────────────────────────────────────────

def calculate_kpis(df):
    """
    Takes the raw DataFrame and returns the full indicator matrix:
    - Program_Overview (KPIs)
    - Venture_Data (per-venture cards)
    - Cohort_Summaries (per-cohort rollups)
    - Investment_Ledger (financial table)
    - Jobs_Summary (program-level jobs)
    - Reach_Summary (program-level reach)
    - Time_Series (charting data)
    - Red_Flags (alerts)
    """
    # Create a proper date column for sorting
    df['Date'] = df['Reporting Month'].apply(parse_reporting_month)

    # Group by Business
    businesses = df.groupby(['Cohort', 'Business Name'])

    venture_data = []
    all_sales_growth = []
    all_profit_growth = []
    total_net_jobs = 0
    grand_total_sales = 0

    # ─── Program-wide accumulators ───
    prog_total_jobs = 0
    prog_new_jobs = 0
    prog_female_jobs = 0
    prog_youth_jobs = 0
    prog_female_new = 0
    prog_youth_new = 0
    prog_total_subscribers = 0
    prog_new_subscribers = 0
    prog_total_schools = 0
    prog_female_students = 0
    prog_total_students = 0
    prog_rural_students = 0
    prog_disability_students = 0

    # ─── Time-series containers ───
    all_time_series = []  # per-venture monthly data
    red_flags = []

    for (cohort, business), group in businesses:
        # Sort by date
        group = group.sort_values('Date')
        n = len(group)

        # ── Sales ──
        total_sales = float(group['Monthly Sales (R)'].sum())
        grand_total_sales += total_sales

        sales_series = group['Monthly Sales (R)']
        profit_series = group['Monthly Net Profit']

        sales_growth = _tiered_sales_growth(sales_series)
        profit_growth = _tiered_profit_growth(profit_series)

        if isinstance(sales_growth, (int, float)):
            all_sales_growth.append(sales_growth)
        if isinstance(profit_growth, (int, float)):
            all_profit_growth.append(profit_growth)

        # ── Net Job Creation ──
        if n >= 2:
            first_jobs = float(group.iloc[0]['Total Jobs'])
            last_jobs = float(group.iloc[-1]['Total Jobs'])
            net_jobs = int(last_jobs - first_jobs)
        else:
            first_jobs = float(group.iloc[0]['Total Jobs']) if n > 0 else 0
            last_jobs = first_jobs
            net_jobs = 0
        total_net_jobs += net_jobs

        # ── Jobs indicators ──
        current_total_jobs = float(group.iloc[-1]['Total Jobs']) if n > 0 else 0
        current_female_jobs = float(group.iloc[-1]['Female Jobs']) if n > 0 else 0
        current_youth_jobs = float(group.iloc[-1]['Youth Jobs']) if n > 0 else 0

        first_female_jobs = float(group.iloc[0]['Female Jobs']) if n > 0 else 0
        first_youth_jobs = float(group.iloc[0]['Youth Jobs']) if n > 0 else 0
        new_female_jobs = int(current_female_jobs - first_female_jobs) if n >= 2 else 0
        new_youth_jobs = int(current_youth_jobs - first_youth_jobs) if n >= 2 else 0

        if n >= 2:
            prev_jobs = float(group.iloc[-2]['Total Jobs'])
            jobs_pct_change = round(((last_jobs - prev_jobs) / prev_jobs) * 100, 1) if prev_jobs != 0 else 0.0
        else:
            jobs_pct_change = 0.0

        prog_total_jobs += current_total_jobs
        prog_new_jobs += net_jobs
        prog_female_jobs += current_female_jobs
        prog_youth_jobs += current_youth_jobs
        prog_female_new += new_female_jobs
        prog_youth_new += new_youth_jobs

        # ── Reach indicators ──
        latest_students = float(group.iloc[-1]['Total Subscribers Students']) if n > 0 else 0
        latest_teachers = float(group.iloc[-1]['Total Subscribers Teachers']) if n > 0 else 0
        venture_total_subscribers = latest_students + latest_teachers

        venture_new_subs = float(group['New Subscribers Students'].sum()) + float(group['New Subscribers Teachers'].sum())
        venture_schools = float(group.iloc[-1]['Total Schools']) if n > 0 else 0

        latest_female_students = float(group.iloc[-1]['Female Students']) if n > 0 else 0
        latest_rural_students = float(group.iloc[-1]['Rural Students']) if n > 0 else 0
        latest_disability_students = float(group.iloc[-1]['Disability Students']) if n > 0 else 0

        prog_total_subscribers += venture_total_subscribers
        prog_new_subscribers += venture_new_subs
        prog_total_schools += venture_schools
        prog_female_students += latest_female_students
        prog_total_students += latest_students
        prog_rural_students += latest_rural_students
        prog_disability_students += latest_disability_students

        # ── Time-series for this venture ──
        for _, row in group.iterrows():
            ts_date = row['Date']
            if ts_date != datetime.min:
                all_time_series.append({
                    'cohort': cohort,
                    'business': business,
                    'month': ts_date.strftime('%Y-%m'),
                    'sales': float(row['Monthly Sales (R)']),
                    'profit': float(row['Monthly Net Profit']),
                    'jobs': float(row['Total Jobs']),
                })

        # ── Red flags ──
        flags = []
        if isinstance(sales_growth, (int, float)) and sales_growth < 0:
            flags.append('Negative Sales Growth')
        if isinstance(profit_growth, (int, float)) and profit_growth < 0:
            flags.append('Negative Profit Growth')
        # < 8000 new learners / year  →  check annualised new subscribers
        months_span = n if n > 0 else 1
        annualised_new_subs = (venture_new_subs / months_span) * 12
        if venture_total_subscribers > 0 and annualised_new_subs < 8000:
            flags.append('Low Learner Reach (<8,000/yr)')

        if flags:
            red_flags.append({
                'Business Name': business,
                'Cohort': cohort,
                'Flags': flags,
            })

        venture_data.append({
            'Business Name': business,
            'Cohort': cohort,
            'Total Sales (R)': total_sales,
            'Sales Growth %': sales_growth,
            'Profit Growth %': profit_growth,
            'Latest Jobs': net_jobs,
            'Jobs Pct Change': jobs_pct_change,
            'Female Jobs': int(current_female_jobs),
            'Youth Jobs': int(current_youth_jobs),
            'New Female Jobs': new_female_jobs,
            'New Youth Jobs': new_youth_jobs,
            'Total Subscribers': int(venture_total_subscribers),
            'New Subscribers': int(venture_new_subs),
            'Total Schools': int(venture_schools),
            'Months': n,
            'Red Flags': flags,
        })

    # ─── Program Overview ───
    avg_sales_growth = round(median(all_sales_growth), 1) if all_sales_growth else "Insufficient Data"
    avg_profit_growth = round(median(all_profit_growth), 1) if all_profit_growth else "Insufficient Data"

    # ─── Cohort Summaries ───
    cohort_summaries = []
    cohort_growth_data = {}  # for TWA

    for cohort_name, cohort_group in df.groupby('Cohort'):
        cohort_businesses_count = cohort_group['Business Name'].nunique()
        cohort_sales = float(cohort_group['Monthly Sales (R)'].sum())
        cohort_profit = float(cohort_group['Monthly Net Profit'].sum())
        cohort_jobs = float(cohort_group.groupby('Business Name').last()['Total Jobs'].sum())

        # Learners = subscribers (students + teachers) latest per venture
        cohort_learners_df = cohort_group.groupby('Business Name').last()
        cohort_learners = float(
            cohort_learners_df['Total Subscribers Students'].sum() +
            cohort_learners_df['Total Subscribers Teachers'].sum()
        )

        # Cohort median sales growth
        cohort_sg = []
        cohort_pg = []
        cohort_months = []
        for biz, biz_group in cohort_group.groupby('Business Name'):
            biz_sorted = biz_group.sort_values('Date')
            sg = _tiered_sales_growth(biz_sorted['Monthly Sales (R)'])
            pg = _tiered_profit_growth(biz_sorted['Monthly Net Profit'])
            if isinstance(sg, (int, float)):
                cohort_sg.append(sg)
            if isinstance(pg, (int, float)):
                cohort_pg.append(pg)
            cohort_months.append(len(biz_sorted))

        cohort_median_sg = round(median(cohort_sg), 1) if cohort_sg else "Insufficient Data"
        cohort_median_pg = round(median(cohort_pg), 1) if cohort_pg else "Insufficient Data"
        avg_months = sum(cohort_months) / len(cohort_months) if cohort_months else 0

        cohort_growth_data[cohort_name] = {
            'median_sg': cohort_median_sg,
            'exposure': avg_months,
        }

        cohort_summaries.append({
            'Cohort': cohort_name,
            'Ventures': cohort_businesses_count,
            'Total Sales': cohort_sales,
            'Total Profit': cohort_profit,
            'Total Jobs': cohort_jobs,
            'Total Learners': cohort_learners,
            'Median Sales Growth': cohort_median_sg,
            'Median Profit Growth': cohort_median_pg,
        })

    # ─── Program TWA ───
    twa_num = 0
    twa_den = 0
    for cname, cdata in cohort_growth_data.items():
        if isinstance(cdata['median_sg'], (int, float)):
            twa_num += cdata['median_sg'] * cdata['exposure']
            twa_den += cdata['exposure']
    program_twa = round(twa_num / twa_den, 1) if twa_den > 0 else "Insufficient Data"

    # ─── Investment Ledger ───
    investment_ledger = []
    for (cohort_name, biz), grp in businesses:
        total_profit = float(grp['Monthly Net Profit'].sum())
        total_grants = float(grp['Grants Value'].sum())
        total_sales_biz = float(grp['Monthly Sales (R)'].sum())
        investment_ledger.append({
            'Business Name': biz,
            'Cohort': cohort_name,
            'Total Sales': total_sales_biz,
            'Net Profit': total_profit,
            'Grants & Investments': total_grants,
        })

    # ─── Jobs Summary (program level) ───
    jobs_summary = {
        'Total Jobs': int(prog_total_jobs),
        'New Jobs': int(prog_new_jobs),
        'Female Jobs': int(prog_female_jobs),
        'Youth Jobs': int(prog_youth_jobs),
        'New Female Jobs': int(prog_female_new),
        'New Youth Jobs': int(prog_youth_new),
    }

    # ─── Reach Summary (program level) ───
    female_pct = round((prog_female_students / prog_total_students) * 100, 1) if prog_total_students > 0 else 0
    rural_pct = round((prog_rural_students / prog_total_students) * 100, 1) if prog_total_students > 0 else 0
    disability_pct = round((prog_disability_students / prog_total_students) * 100, 1) if prog_total_students > 0 else 0

    reach_summary = {
        'Total Subscribers': int(prog_total_subscribers),
        'New Subscribers': int(prog_new_subscribers),
        'Total Schools': int(prog_total_schools),
        'Female %': female_pct,
        'Rural %': rural_pct,
        'Disability %': disability_pct,
    }

    # ─── Build time-series for charts (cohort-level aggregation per month) ───
    ts_df = pd.DataFrame(all_time_series)
    cohort_time_series = {}
    if not ts_df.empty:
        for cohort_name in ts_df['cohort'].unique():
            ct = ts_df[ts_df['cohort'] == cohort_name]
            monthly = ct.groupby('month').agg({
                'sales': 'sum',
                'profit': 'sum',
                'jobs': 'sum',
            }).sort_index()
            cohort_time_series[cohort_name] = {
                'months': monthly.index.tolist(),
                'sales': monthly['sales'].tolist(),
                'profit': monthly['profit'].tolist(),
                'jobs': monthly['jobs'].tolist(),
            }

    # Program-level time series
    program_time_series = {'months': [], 'sales': [], 'profit': [], 'jobs': []}
    if not ts_df.empty:
        prog_monthly = ts_df.groupby('month').agg({
            'sales': 'sum',
            'profit': 'sum',
            'jobs': 'sum',
        }).sort_index()
        program_time_series = {
            'months': prog_monthly.index.tolist(),
            'sales': prog_monthly['sales'].tolist(),
            'profit': prog_monthly['profit'].tolist(),
            'jobs': prog_monthly['jobs'].tolist(),
        }

    # ── Cohort Detail for tabbed dashboard ──
    cohort_detail = {}
    for cohort_name in sorted(df['Cohort'].unique()):
        cohort_df = df[df['Cohort'] == cohort_name].copy()
        valid = cohort_df[cohort_df['Date'] != datetime.min]

        # 1. Per-fellow sales & profit time series (for line charts)
        fellows_sales = []
        fellows_profit = []
        for biz_name, biz_group in valid.groupby('Business Name'):
            biz_sorted = biz_group.sort_values('Date')
            months = biz_sorted['Date'].dt.strftime('%Y-%m').tolist()
            sales = [round(float(v)) for v in biz_sorted['Monthly Sales (R)'].tolist()]
            profit = [round(float(v)) for v in biz_sorted['Monthly Net Profit'].tolist()]
            fellows_sales.append({
                'name': biz_name,
                'data': [{'x': m, 'y': s} for m, s in zip(months, sales)]
            })
            fellows_profit.append({
                'name': biz_name,
                'data': [{'x': m, 'y': p} for m, p in zip(months, profit)]
            })

        # 2. Jobs clustered bar chart data (aggregated by month)
        if not valid.empty:
            jobs_month = valid.groupby(valid['Date'].dt.strftime('%Y-%m')).agg({
                'Total Jobs': 'sum',
                'Female Jobs': 'sum',
                'Youth Jobs': 'sum',
            }).sort_index()
            jobs_bar = {
                'months': jobs_month.index.tolist(),
                'total': [round(float(v)) for v in jobs_month['Total Jobs'].tolist()],
                'female': [round(float(v)) for v in jobs_month['Female Jobs'].tolist()],
                'youth': [round(float(v)) for v in jobs_month['Youth Jobs'].tolist()],
            }
        else:
            jobs_bar = {'months': [], 'total': [], 'female': [], 'youth': []}

        # 3. Jobs table (per fellow — latest snapshot)
        jobs_table = []
        for biz_name, biz_group in cohort_df.groupby('Business Name'):
            biz_sorted = biz_group.sort_values('Date')
            n = len(biz_sorted)
            ct = float(biz_sorted.iloc[-1]['Total Jobs']) if n > 0 else 0
            ft = float(biz_sorted.iloc[0]['Total Jobs']) if n > 0 else 0
            new_j = int(ct - ft) if n >= 2 else 0
            cf = float(biz_sorted.iloc[-1]['Female Jobs']) if n > 0 else 0
            ff = float(biz_sorted.iloc[0]['Female Jobs']) if n > 0 else 0
            new_f = int(cf - ff) if n >= 2 else 0
            cy = float(biz_sorted.iloc[-1]['Youth Jobs']) if n > 0 else 0
            if n >= 2:
                pt = float(biz_sorted.iloc[-2]['Total Jobs'])
                pct = round(((ct - pt) / pt) * 100, 1) if pt != 0 else 0.0
            else:
                pct = 0.0
            jobs_table.append({
                'name': biz_name, 'total': int(ct), 'new': new_j,
                'pct_change': pct, 'new_female': new_f, 'youth': int(cy),
            })

        # 4. Investments table (each non-zero grant row)
        investments_table = []
        for _, row in cohort_df.iterrows():
            gv = float(row.get('Grants Value', 0))
            if gv > 0:
                investments_table.append({
                    'name': row['Business Name'],
                    'value': gv,
                    'investor': str(row.get('Grant Funder', '') or 'Not specified'),
                    'month': str(row.get('Reporting Month', '')),
                })

        # 5. Reach time series (subscribers + schools, aggregated by month)
        if not valid.empty:
            reach_month = valid.groupby(valid['Date'].dt.strftime('%Y-%m')).agg({
                'Total Subscribers Students': 'sum',
                'Total Subscribers Teachers': 'sum',
                'New Subscribers Students': 'sum',
                'New Subscribers Teachers': 'sum',
                'SA Schools': 'sum',
                'Q1-3 Schools': 'sum',
            }).sort_index()
            cum_learners = reach_month['New Subscribers Students'].cumsum().tolist()
            cum_educators = reach_month['New Subscribers Teachers'].cumsum().tolist()
            reach = {
                'months': reach_month.index.tolist(),
                'total_learners': [round(float(v)) for v in reach_month['Total Subscribers Students'].tolist()],
                'total_educators': [round(float(v)) for v in reach_month['Total Subscribers Teachers'].tolist()],
                'new_learners_cum': [round(float(v)) for v in cum_learners],
                'new_educators_cum': [round(float(v)) for v in cum_educators],
                'sa_schools': [round(float(v)) for v in reach_month['SA Schools'].tolist()],
                'q13_schools': [round(float(v)) for v in reach_month['Q1-3 Schools'].tolist()],
            }
        else:
            reach = {'months': [], 'total_learners': [], 'total_educators': [],
                     'new_learners_cum': [], 'new_educators_cum': [],
                     'sa_schools': [], 'q13_schools': []}

        # 6. Learner disaggregation table (per fellow — latest values)
        disagg_table = []
        for biz_name, biz_group in cohort_df.groupby('Business Name'):
            biz_sorted = biz_group.sort_values('Date')
            n = len(biz_sorted)
            if n > 0:
                latest = biz_sorted.iloc[-1]
                disagg_table.append({
                    'name': biz_name,
                    'female': int(float(latest.get('Female Students', 0))),
                    'rural': int(float(latest.get('Rural Students', 0))),
                    'disability': int(float(latest.get('Disability Students', 0))),
                })

        cohort_detail[cohort_name] = {
            'fellows_sales': fellows_sales,
            'fellows_profit': fellows_profit,
            'jobs_bar': jobs_bar,
            'jobs_table': jobs_table,
            'investments_table': investments_table,
            'reach': reach,
            'disaggregation': disagg_table,
        }

    result = {
        'Program_Overview': {
            'Total_Sales_ZAR': grand_total_sales,
            'Net_Jobs_Created': int(total_net_jobs),
            'Average_Sales_Growth_%': avg_sales_growth,
            'Average_Profit_Growth_%': avg_profit_growth,
            'Program_TWA': program_twa,
        },
        'Venture_Data': venture_data,
        'Cohort_Summaries': cohort_summaries,
        'Investment_Ledger': investment_ledger,
        'Jobs_Summary': jobs_summary,
        'Reach_Summary': reach_summary,
        'Time_Series': {
            'cohort': cohort_time_series,
            'program': program_time_series,
        },
        'Red_Flags': red_flags,
        'Cohort_Detail': cohort_detail,
    }

    return result


if __name__ == '__main__':
    import json
    from data_fetcher import fetch_dashboard_data

    print("Fetching data...")
    raw_df = fetch_dashboard_data()

    print("\nCalculating KPIs...")
    result = calculate_kpis(raw_df)

    print("\n--- Program Overview ---")
    overview = result['Program_Overview']
    print(f"  Total Sales (YTD): R {overview['Total_Sales_ZAR']:,.2f}")
    print(f"  Net Jobs Created:  {overview['Net_Jobs_Created']}")
    print(f"  Avg Sales Growth:  {overview['Average_Sales_Growth_%']}")
    print(f"  Avg Profit Growth: {overview['Average_Profit_Growth_%']}")
    print(f"  Program TWA:       {overview['Program_TWA']}")

    print(f"\n--- Jobs Summary ---")
    js = result['Jobs_Summary']
    print(f"  Total: {js['Total Jobs']}, New: {js['New Jobs']}, Female: {js['Female Jobs']}, Youth: {js['Youth Jobs']}")

    print(f"\n--- Reach Summary ---")
    rs = result['Reach_Summary']
    print(f"  Subscribers: {rs['Total Subscribers']}, New: {rs['New Subscribers']}, Schools: {rs['Total Schools']}")
    print(f"  Female: {rs['Female %']}%, Rural: {rs['Rural %']}%, Disability: {rs['Disability %']}%")

    print(f"\n--- Red Flags ({len(result['Red_Flags'])}) ---")
    for rf in result['Red_Flags'][:5]:
        print(f"  {rf['Business Name']} ({rf['Cohort']}): {', '.join(rf['Flags'])}")

    print(f"\n--- Venture Data ({len(result['Venture_Data'])} businesses) ---")
    for v in result['Venture_Data'][:5]:
        growth = v['Sales Growth %']
        growth_str = f"{growth}%" if growth != "Insufficient Data" else growth
        print(f"  {v['Business Name']}: Sales=R {v['Total Sales (R)']:,.0f}, Jobs={v['Latest Jobs']}, Growth={growth_str}")
