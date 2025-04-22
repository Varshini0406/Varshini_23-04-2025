from flask import Flask, jsonify, request
import pandas as pd
import pytz
from datetime import datetime, timedelta
import uuid
import io
import csv
from threading import Thread

app = Flask(__name__)

# Load data with specified dtypes and parse dates
status_df = pd.read_csv('data/status.csv', dtype={'store_id': str, 'status': str}, parse_dates=['timestamp_utc'], index_col='timestamp_utc')
hours_df = pd.read_csv('data/business_hours.csv', dtype={'store_id': str})
tz_df = pd.read_csv('data/timezones.csv', dtype={'store_id': str})

# Check if already timezone-aware and localize
if status_df.index.tz is None:
    status_df.index = status_df.index.tz_localize('UTC')

hours_df.rename(columns={'dayOfWeek': 'day'}, inplace=True)

# Preload store hours and timezones
store_hours_dict = {}
for store_id in tz_df['store_id'].unique():
    store_hours_dict[store_id] = hours_df[hours_df['store_id'] == store_id]

tz_dict = tz_df.set_index('store_id')['timezone_str'].to_dict()

reports = {}

def calculate_uptime_downtime(store_id, time_window):
    """Calculates uptime and downtime in minutes."""
    now_utc = datetime.now(pytz.UTC)
    start_time_utc = now_utc - time_window

    if store_id not in tz_dict:
        return None, None

    local_tz = pytz.timezone(tz_dict[store_id])

    # Indexing to reduce search
    relevant_status = status_df[status_df['store_id'] == store_id]
    relevant_status = relevant_status[relevant_status.index >= start_time_utc]

    if relevant_status.empty:
        return 0, 0

    relevant_status['timestamp_local'] = relevant_status['timestamp_utc'].dt.tz_convert(local_tz)
    relevant_status['day'] = relevant_status['timestamp_local'].dt.dayofweek
    relevant_status['time'] = relevant_status['timestamp_local'].dt.time

    total_minutes = 0
    downtime = 0
    previous_time = None

    for _, row in relevant_status.iterrows():
        business_hours = hours_df[
            (hours_df['store_id'] == store_id) & 
            (hours_df['day'] == row['day'])
        ]

        if not business_hours.empty:
            start_str = business_hours['start_time_local'].iloc[0]
            end_str = business_hours['end_time_local'].iloc[0]

            start_time = datetime.strptime(start_str, '%H:%M:%S').time()
            end_time = datetime.strptime(end_str, '%H:%M:%S').time()

            row_time = row['timestamp_local']
            start_dt = local_tz.localize(datetime.combine(row_time.date(), start_time))
            end_dt = local_tz.localize(datetime.combine(row_time.date(), end_time))

            if start_dt <= row_time <= end_dt:
                current_time = row['timestamp_utc']
                if previous_time:
                    diff = (current_time - previous_time).total_seconds() / 60
                    total_minutes += diff
                    if row['status'] == 'inactive':
                        downtime += diff
                previous_time = current_time

    uptime = total_minutes - downtime
    return uptime, downtime

def generate_report_data():
    """Generates report data for all stores."""
    report_data = []
    for store_id in tz_df['store_id'].unique():
        uptime_hour, downtime_hour = calculate_uptime_downtime(store_id, timedelta(hours=1))
        uptime_day, downtime_day = calculate_uptime_downtime(store_id, timedelta(days=1))
        uptime_week, downtime_week = calculate_uptime_downtime(store_id, timedelta(weeks=1))

        if uptime_hour is not None:
            report_data.append({
                'store_id': store_id,
                'uptime_last_hour(in minutes)': round(uptime_hour, 2),
                'uptime_last_day(in hours)': round(uptime_day / 60, 2),
                'uptime_last_week(in hours)': round(uptime_week / 60, 2),
                'downtime_last_hour(in minutes)': round(downtime_hour, 2),
                'downtime_last_day(in hours)': round(downtime_day / 60, 2),
                'downtime_last_week(in hours)': round(downtime_week / 60, 2)
            })
    return report_data

def generate_csv_report(report_data):
    """Generates CSV data."""
    output = io.StringIO()
    fields = report_data[0].keys() if report_data else []
    writer = csv.DictWriter(output, fieldnames=fields)
    writer.writeheader()
    writer.writerows(report_data)
    return output.getvalue()

def generate_report(report_id):
    """Generates and stores report."""
    try:
        data = generate_report_data()
        csv_data = generate_csv_report(data)
        reports[report_id] = {'status': 'Complete', 'data': csv_data}
    except Exception as e:
        reports[report_id] = {'status': 'Error', 'data': str(e)}

@app.route('/trigger_report', methods=['POST'])
def trigger_report():
    """Triggers report generation."""
    report_id = str(uuid.uuid4())
    reports[report_id] = {'status': 'Running', 'data': None}
    Thread(target=generate_report, args=(report_id,)).start()
    return jsonify({'report_id': report_id}), 200

@app.route('/get_report', methods=['GET'])
def get_report():
    """Returns report status or data."""
    report_id = request.args.get('report_id')
    if not report_id or report_id not in reports:
        return jsonify({'error': 'Invalid report_id'}), 400
    
    report = reports[report_id]
    if report['status'] == 'Running':
        return jsonify({'status': 'Running'}), 200
    elif report['status'] == 'Complete':
        return jsonify({'status': 'Complete', 'csv_data': report['data']}), 200
    else:
        return jsonify({'status': 'Error', 'error': report['data']}), 500

@app.route('/report_data')
def display_report_data():
    """Displays report data in JSON."""
    return jsonify(generate_report_data())

@app.route('/')
def home():
    return "Store Monitoring System"

if __name__ == '__main__':
    app.run(debug=True)
