import os
import io
import csv
import json
from flask import Flask, render_template, Response, request, jsonify
from dotenv import load_dotenv
from groq import Groq

from data_fetcher import fetch_dashboard_data
from logic_engine import calculate_kpis

# Load environment variables
load_dotenv(override=True)

# Initialize Flask
app = Flask(__name__)

# Configure Groq AI
groq_client = Groq(api_key=os.getenv('GROQ_API_KEY'))

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

        system_prompt = """Act as 'Injini AI', a helpful data assistant for the Injini EdTech accelerator.
Answer the user's question based strictly on the data provided.

Guidelines:
- Be concise and friendly.
- Use South African Rands (R) for currency.
- If the answer isn't in the data, say "I don't have that information in the current dataset."
- Format key numbers in bold using Markdown (e.g., **R 500,000**)."""

        user_prompt = f"""Context Data:
{json.dumps(context, indent=2, default=str)}

User Question: {user_message}"""

        # Try primary model, then fallback
        models_to_try = ['llama-3.3-70b-versatile', 'llama-3.1-8b-instant']
        last_error = None

        for model_name in models_to_try:
            try:
                response = groq_client.chat.completions.create(
                    model=model_name,
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt},
                    ],
                    temperature=0.3,
                    max_tokens=1024,
                )
                return jsonify({"response": response.choices[0].message.content})
            except Exception as model_err:
                last_error = model_err
                print(f"Model {model_name} failed: {model_err}")
                if "429" in str(model_err) or "rate_limit" in str(model_err).lower():
                    time.sleep(2)
                    continue
                raise

        print(f"All models failed: {last_error}")
        return jsonify({"response": "I'm currently at capacity. Please wait about 60 seconds and try again. 🕐"})

    except Exception as e:
        print(f"Chat Error: {e}")
        error_msg = str(e)
        if "429" in error_msg or "rate_limit" in error_msg.lower():
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

    # Serialize time-series and cohort detail for JS charts
    time_series_json = json.dumps(kpi_data['Time_Series'], default=str)
    cohort_detail_json = json.dumps(kpi_data['Cohort_Detail'], default=str)

    return render_template(
        'dashboard.html',
        kpis=kpis_for_template,
        cohort_summaries=kpi_data['Cohort_Summaries'],
        investment_ledger=kpi_data['Investment_Ledger'],
        jobs_summary=kpi_data['Jobs_Summary'],
        reach_summary=kpi_data['Reach_Summary'],
        time_series_json=time_series_json,
        red_flags=kpi_data['Red_Flags'],
        cohort_detail=kpi_data['Cohort_Detail'],
        cohort_detail_json=cohort_detail_json,
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
