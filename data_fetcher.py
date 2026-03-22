import os
import io
import csv
import json
import time
from flask import Flask, render_template, Response, request, jsonify
from dotenv import load_dotenv
from groq import Groq
 
from data_fetcher import fetch_dashboard_data
from logic_engine import calculate_kpis
 
load_dotenv(override=True)
 
app = Flask(__name__)
groq_client = Groq(api_key=os.getenv('GROQ_API_KEY'))
 
 
# ---------------------------------------------------------------------------
# JSON safety helper
# ---------------------------------------------------------------------------
def _safe_json(data) -> str:
    """
    Serialise data to JSON and escape sequences that would prematurely close
    a <script> tag in the browser (</ --> <\/).
    Without this, business names or text fields containing '</' break the
    entire JS block with "Unexpected token '{'".
    """
    raw = json.dumps(data, default=str)
    # Escape forward-slash after '<' so the browser never sees '</script>'
    raw = raw.replace('</', '<\\/')
    # Also escape HTML comment openers inside strings
    raw = raw.replace('<!--', '<\\!--')
    return raw
 
 
# ---------------------------------------------------------------------------
# Data cache (5-minute TTL)
# ---------------------------------------------------------------------------
_cache: dict = {'data': None, 'ts': 0}
CACHE_TTL = 300  # seconds
 
 
def get_dashboard_data():
    now = time.time()
    if _cache['data'] is None or (now - _cache['ts']) > CACHE_TTL:
        raw_df = fetch_dashboard_data()
        kpi_data = calculate_kpis(raw_df)
        _cache['data'] = (raw_df, kpi_data)
        _cache['ts'] = now
    return _cache['data']
 
 
# ---------------------------------------------------------------------------
# DotDict helper
# ---------------------------------------------------------------------------
class DotDict(dict):
    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(key)
 
 
# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
@app.route('/health')
def health():
    return 'ok', 200
 
 
@app.route('/')
def dashboard():
    raw_df, kpi_data = get_dashboard_data()
 
    kpis_for_template = DotDict(
        Program_Overview=DotDict(kpi_data['Program_Overview']),
        Venture_Data=kpi_data['Venture_Data'],
    )
 
    # FIX: Use _safe_json() not json.dumps() -- prevents Unexpected token '{'
    time_series_json   = _safe_json(kpi_data['Time_Series'])
    cohort_detail_json = _safe_json(kpi_data['Cohort_Detail'])
 
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
 
 
@app.route('/api/chat', methods=['POST'])
def chat_with_data():
    try:
        data = request.json
        user_message = data.get('message', '')
        raw_df, kpi_data = get_dashboard_data()
 
        context = {
            'Program_Overview':  kpi_data['Program_Overview'],
            'Cohort_Summaries':  kpi_data['Cohort_Summaries'],
            'Jobs_Summary':      kpi_data['Jobs_Summary'],
            'Reach_Summary':     kpi_data['Reach_Summary'],
            'Ventures': [
                {k: v for k, v in v.items() if k != 'Red Flags'}
                for v in kpi_data['Venture_Data']
            ],
        }
 
        system_prompt = (
            "Act as 'Injini AI', a helpful data assistant for the Injini EdTech accelerator. "
            "Answer the user's question based strictly on the data provided. "
            "Be concise and friendly. Use South African Rands (R) for currency. "
            "If the answer isn't in the data, say \"I don't have that information in the current dataset.\" "
            "Format key numbers in bold using Markdown (e.g., **R 500,000**)."
        )
        user_prompt = (
            f"Context Data:\n{json.dumps(context, indent=2, default=str)}\n\n"
            f"User Question: {user_message}"
        )
 
        models = ['llama-3.3-70b-versatile', 'llama-3.1-8b-instant']
        for model in models:
            try:
                resp = groq_client.chat.completions.create(
                    model=model,
                    messages=[
                        {'role': 'system', 'content': system_prompt},
                        {'role': 'user',   'content': user_prompt},
                    ],
                    temperature=0.3,
                    max_tokens=1024,
                )
                return jsonify({'response': resp.choices[0].message.content})
            except Exception as e:
                if '429' in str(e) or 'rate_limit' in str(e).lower():
                    time.sleep(2)
                    continue
                raise
 
        return jsonify({'response': "I'm currently at capacity. Please wait a moment and try again."})
 
    except Exception as e:
        print(f"Chat error: {e}")
        if '429' in str(e) or 'rate_limit' in str(e).lower():
            return jsonify({'response': "I'm currently at capacity. Please wait about 60 seconds."})
        return jsonify({'response': "I'm having trouble connecting right now."}), 500
 
 
@app.route('/export')
def export_csv():
    raw_df, kpi_data = get_dashboard_data()
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        'Business Name', 'Cohort', 'Total Sales (R)', 'Sales Growth %',
        'Profit Growth %', 'Net Jobs Created', 'Jobs % Change',
        'Female Jobs', 'Youth Jobs', 'New Female Jobs', 'New Youth Jobs',
        'Total Subscribers', 'New Subscribers', 'Total Schools', 'Red Flags',
    ])
    inv_map = {i['Business Name']: i for i in kpi_data['Investment_Ledger']}
    for v in kpi_data['Venture_Data']:
        writer.writerow([
            v['Business Name'], v['Cohort'],
            f"{v['Total Sales (R)']:.2f}", v['Sales Growth %'],
            v['Profit Growth %'], v['Latest Jobs'], v['Jobs Pct Change'],
            v['Female Jobs'], v['Youth Jobs'],
            v['New Female Jobs'], v['New Youth Jobs'],
            v['Total Subscribers'], v['New Subscribers'], v['Total Schools'],
            '; '.join(v.get('Red Flags', [])),
        ])
    response = Response(output.getvalue(), mimetype='text/csv')
    response.headers['Content-Disposition'] = 'attachment; filename=injini_mel_report.csv'
    return response
 
 
if __name__ == '__main__':
    app.run(debug=True, port=5000)
