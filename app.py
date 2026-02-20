import os
import io
import csv
import json
from flask import Flask, render_template, Response, request, jsonify
from dotenv import load_dotenv
import google.generativeai as genai

from data_fetcher import fetch_dashboard_data
from logic_engine import calculate_kpis

# Load environment variables
load_dotenv(override=True)

# Initialize Flask
app = Flask(__name__)

# Configure Gemini AI
genai.configure(api_key=os.getenv('GEMINI_API_KEY'))
model = genai.GenerativeModel('gemini-1.5-flash')

@app.route('/api/chat', methods=['POST'])
def chat_with_data():
    """API endpoint for the Injini AI chat interface."""
    import time
    
    try:
        data = request.json
        user_message = data.get('message', '')
        
        # Get fresh data for context
        raw_df, kpi_data = get_dashboard_data()
        
        # Construct context from KPI data (include venture-level details)
        context = {
            "Program_Overview": kpi_data['Program_Overview'],
            "Cohort_Summaries": kpi_data['Cohort_Summaries'],
            "Jobs_Summary": kpi_data['Jobs_Summary'],
            "Reach_Summary": kpi_data['Reach_Summary'],
            "Ventures": [{k: v for k, v in vent.items() if k != 'Red Flags'} for vent in kpi_data['Venture_Data']],
        }
        
        prompt = f"""Act as 'Injini AI', a helpful data assistant for the Injini EdTech accelerator.
Answer the user's question based strictly on the data below.

Context Data:
{json.dumps(context, indent=2, default=str)}

User Question: {user_message}

Guidelines:
- Be concise and friendly.
- Use South African Rands (R) for currency.
- If the answer isn't in the data, say "I don't have that information in the current dataset."
- Format key numbers in bold using Markdown (e.g., **R 500,000**)."""

        # Try primary model, then fallback
        models_to_try = ['gemini-2.0-flash-lite', 'gemini-2.0-flash']
        last_error = None
        
        for model_name in models_to_try:
            try:
                ai_model = genai.GenerativeModel(model_name)
                response = ai_model.generate_content(prompt)
                return jsonify({"response": response.text})
            except Exception as model_err:
                last_error = model_err
                print(f"Model {model_name} failed: {model_err}")
                if "429" in str(model_err):
                    time.sleep(2)  # Brief pause before trying next model
                    continue
                raise  # Non-rate-limit errors should propagate

        # If all models failed with rate limits, return a helpful message
        print(f"All models rate-limited: {last_error}")
        return jsonify({"response": "I'm currently at capacity. The free API has a usage limit — please wait about 60 seconds and try again. 🕐"})

    except Exception as e:
        print(f"Chat Error: {e}")
        error_msg = str(e)
        if "429" in error_msg:
            return jsonify({"response": "I'm currently at capacity. Please wait about 60 seconds and try again. 🕐"})
        return jsonify({"response": "I'm having trouble connecting right now. Please try again shortly."}), 500


class DotDict(dict):
    """A dict subclass that supports both dot notation and bracket notation."""
    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(key)


def get_dashboard_data():
    """Shared helper to fetch and calculate all dashboard data."""
    raw_df = fetch_dashboard_data()
    kpi_data = calculate_kpis(raw_df)
    return raw_df, kpi_data


@app.route('/')
def dashboard():
    """Main dashboard route."""
    raw_df, kpi_data = get_dashboard_data()
    # ai_summary removed in favor of chat interface

    kpis_for_template = DotDict(
        Program_Overview=DotDict(kpi_data['Program_Overview']),
        Venture_Data=kpi_data['Venture_Data']
    )

    # Serialize time-series for JS charts
    time_series_json = json.dumps(kpi_data['Time_Series'], default=str)

    return render_template(
        'dashboard.html',
        kpis=kpis_for_template,
        cohort_summaries=kpi_data['Cohort_Summaries'],
        investment_ledger=kpi_data['Investment_Ledger'],
        jobs_summary=kpi_data['Jobs_Summary'],
        reach_summary=kpi_data['Reach_Summary'],
        time_series_json=time_series_json,
        red_flags=kpi_data['Red_Flags'],
    )


@app.route('/export')
def export_csv():
    """Export all venture KPI data as a downloadable CSV file."""
    raw_df, kpi_data = get_dashboard_data()

    output = io.StringIO()
    writer = csv.writer(output)

    # Header row — all indicator columns
    writer.writerow([
        'Business Name', 'Cohort', 'Total Sales (R)', 'Sales Growth %',
        'Profit Growth %', 'Net Jobs Created', 'Jobs % Change',
        'Female Jobs', 'Youth Jobs', 'New Female Jobs', 'New Youth Jobs',
        'Total Subscribers', 'New Subscribers', 'Total Schools',
        'Net Profit (R)', 'Grants & Investments (R)', 'Red Flags'
    ])

    # Merge venture + investment data
    investment_map = {item['Business Name']: item for item in kpi_data['Investment_Ledger']}

    for v in kpi_data['Venture_Data']:
        inv = investment_map.get(v['Business Name'], {})
        writer.writerow([
            v['Business Name'],
            v['Cohort'],
            f"{v['Total Sales (R)']:.2f}",
            v['Sales Growth %'],
            v['Profit Growth %'],
            v['Latest Jobs'],
            v['Jobs Pct Change'],
            v['Female Jobs'],
            v['Youth Jobs'],
            v['New Female Jobs'],
            v['New Youth Jobs'],
            v['Total Subscribers'],
            v['New Subscribers'],
            v['Total Schools'],
            f"{inv.get('Net Profit', 0):.2f}",
            f"{inv.get('Grants & Investments', 0):.2f}",
            '; '.join(v.get('Red Flags', [])),
        ])

    response = Response(output.getvalue(), mimetype='text/csv')
    response.headers['Content-Disposition'] = 'attachment; filename=injini_mel_report.csv'
    return response


if __name__ == '__main__':
    app.run(debug=True, port=5000)
