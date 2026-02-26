import os
import pandas as pd
from pyairtable import Api
from dotenv import load_dotenv

# Load secrets from your .env file
load_dotenv(override=True)

# Initialize Airtable API
api_key = os.getenv('AIRTABLE_PAT')
api = Api(api_key)

# The exact Base IDs from your Airtable links
BASE_IDS = {
    'Cohort 1': 'app5MKMARnZAInXVJ',
    'Cohort 2': 'app3KJMspt7z8qy9M',
    'Cohort 3': 'appBhlIJDu8JvaWxB',
    'Cohort 4': 'appzHpcS4aenhjZ8V'
}

def fetch_dashboard_data():
    all_data = []
    
    for cohort, base_id in BASE_IDS.items():
        print(f"Fetching data for {cohort}...")
        try:
            table = api.table(base_id, 'Monthly reporting')
            records = table.all()

            for record in records:
                fields = record['fields']
                
                # Helper: find field value from multiple possible names
                def get_field(field_list):
                    for name in field_list:
                        if name in fields:
                            return fields[name]
                    return None

                # --- Business Identity ---
                business_name = get_field(['Business name', 'Company name']) or 'Unknown'
                if isinstance(business_name, list):
                    business_name = business_name[0]
                reporting_month = get_field(['Reporting month', 'Reporting Month']) or 'Unknown'

                # --- Business Indicators ---
                sales = get_field(['Monthly Sales', 'Monthly sales', '# Monthly sales']) or 0
                net_profit = get_field(['Monthly net profit', 'Monthly Net Profit']) or 0

                # --- Jobs ---
                total_jobs = get_field(['Operational jobs - Total', 'Total operational jobs', 'Total Jobs']) or 0
                female_jobs = get_field(['Operational jobs - female', 'Female operational jobs']) or 0
                youth_jobs = get_field(['Youth operational jobs']) or 0
                educ_jobs_total = get_field(['Educational resourcing jobs -Total', 'Total Educational resourcing jobs']) or 0
                educ_jobs_female = get_field(['Educational resourcing jobs - Female', 'Female educational resourcing Jobs']) or 0

                # --- Reach: Subscribers ---
                total_subscribers_students = get_field([
                    'Total Subscribers -Students', 'Total Subscribers - Students'
                ]) or 0
                total_subscribers_teachers = get_field([
                    'Total Subscribers - Teachers', 'Total subscribers - Teachers'
                ]) or 0
                new_subscribers_students = get_field([
                    'Net new monthly subscribers  - students',
                    'Net new monthly subscribers - students',
                    'New Monthly Subscribers - Students'
                ]) or 0
                new_subscribers_teachers = get_field([
                    'Net new monthly subscribers  - Teachers',
                    'Net new monthly subscribers - Teachers'
                ]) or 0
                
                # --- Reach: Active Users ---
                active_students = get_field([
                    'Active users Students - Broad Definition',
                    'Monthly Active users - Students'
                ]) or 0
                active_teachers = get_field([
                    'Active users teachers - Broad Definition',
                    'Monthly Active users - Teachers'
                ]) or 0

                # --- Reach: Demographics ---
                female_students = get_field([
                    'Subscribers - Female students', 'Subscribers - Female Students'
                ]) or 0
                female_teachers = get_field([
                    'Subscribers - Female teachers', 'Subscribers - Female Teachers'
                ]) or 0
                rural_students = get_field([
                    'Subscription - Rural Students'
                ]) or 0
                rural_teachers = get_field([
                    'Subscription - Rural Teachers', 'Subscription - Rural teachers'
                ]) or 0
                disability_students = get_field([
                    'Subscription - Students with disabilities'
                ]) or 0
                disability_teachers = get_field([
                    'Subscribers - Teachers with disabilities'
                ]) or 0

                # --- Reach: Schools ---
                q13_schools = get_field([
                    'Subscription- Q1-3 Schools Students',
                    'Subscription - Q1-3 schools'
                ]) or 0
                sa_schools = get_field(['Subscription - South African schools']) or 0
                total_schools = get_field([
                    'Total number of schools solution being tested in',
                    'Total subscribers (Schools/learning institutions)'
                ]) or (q13_schools + sa_schools)

                # --- Investments ---
                grants_value = get_field([
                    'Rand value of grant/investment',
                    'New grants and investments'
                ]) or 0
                grant_funder = get_field([
                    'If yes, please specify from whom this grant/ investment was made.'
                ]) or ''
                income_statement = get_field(['Income statement', 'Income Statement', 'Income statement  ']) or ''

                all_data.append({
                    'Cohort': cohort,
                    'Business Name': business_name,
                    'Reporting Month': reporting_month,
                    # Business
                    'Monthly Sales (R)': sales,
                    'Monthly Net Profit': net_profit,
                    # Jobs
                    'Total Jobs': total_jobs,
                    'Female Jobs': female_jobs,
                    'Youth Jobs': youth_jobs,
                    'Educ Jobs Total': educ_jobs_total,
                    'Educ Jobs Female': educ_jobs_female,
                    # Subscribers
                    'Total Subscribers Students': total_subscribers_students,
                    'Total Subscribers Teachers': total_subscribers_teachers,
                    'New Subscribers Students': new_subscribers_students,
                    'New Subscribers Teachers': new_subscribers_teachers,
                    # Active Users
                    'Active Students': active_students,
                    'Active Teachers': active_teachers,
                    # Demographics
                    'Female Students': female_students,
                    'Female Teachers': female_teachers,
                    'Rural Students': rural_students,
                    'Rural Teachers': rural_teachers,
                    'Disability Students': disability_students,
                    'Disability Teachers': disability_teachers,
                    # Schools
                    'Total Schools': total_schools,
                    'SA Schools': sa_schools,
                    'Q1-3 Schools': q13_schools,
                    # Investments
                    'Grants Value': grants_value,
                    'Grant Funder': grant_funder,
                    'Income Statement': income_statement,
                })
        except Exception as e:
            print(f"Failed to fetch {cohort}. Error: {e}")
            
    df = pd.DataFrame(all_data)
    
    # Numeric columns to clean
    numeric_cols = [
        'Monthly Sales (R)', 'Monthly Net Profit',
        'Total Jobs', 'Female Jobs', 'Youth Jobs', 'Educ Jobs Total', 'Educ Jobs Female',
        'Total Subscribers Students', 'Total Subscribers Teachers',
        'New Subscribers Students', 'New Subscribers Teachers',
        'Active Students', 'Active Teachers',
        'Female Students', 'Female Teachers',
        'Rural Students', 'Rural Teachers',
        'Disability Students', 'Disability Teachers',
        'Total Schools', 'SA Schools', 'Q1-3 Schools',
        'Grants Value'
    ]
    for col in numeric_cols:
        if col not in df.columns:
            df[col] = 0
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    
    return df

if __name__ == '__main__':
    print("Testing Airtable Connection...")
    df = fetch_dashboard_data()
    print(f"\nTotal records: {len(df)}")
    print(f"Columns: {list(df.columns)}")
    print(f"\nSample data:")
    print(df.head(5).to_string())
