## Features

- **Report Generation**: Calculate uptime/downtime metrics for stores across three time windows:
  - Last hour (in minutes)
  - Last day (in hours)
  - Last week (in hours)
- **Asynchronous Processing**: Generate reports in background threads
- **CSV Export**: Download generated reports in CSV format
- **JSON API**: REST endpoints for triggering and checking reports

## Prerequisites

- Python 3.7+
- pip package manager

## Installation

1. Clone the repository:
2. Install Dependencies
3. Prepare the files
   data/
├── status.csv
├── business_hours.csv
└── timezones.csv
4. Run the app.py file using python app.py command

### API Endpoints
| `/trigger_report` | POST | Generate a new report |
| `/get_report` | GET | Check report status and download |
| `/report_data` | GET | View report data in JSON |

### Generating Reports

1. **Trigger a report**:
curl -X POST http://localhost:5000/trigger_report
Response:
{"report_id": "random_id"}
2. **Check report status**:
curl "http://localhost:5000/get_report?report_id=given_id"
3. **View report data**:
curl http://localhost:5000/report_data
