# VPI Jobs Tracker Dashboard

A beautiful, presentation-ready dashboard for tracking VPI jobs from BigChange.

![Dashboard Preview](https://via.placeholder.com/800x400?text=VPI+Jobs+Tracker+Dashboard)

## Quick Start

### 1. Set Your Credentials

**Linux/Mac:**
```bash
export BIGCHANGE_USERNAME="your_username"
export BIGCHANGE_PASSWORD="your_password"
export BIGCHANGE_KEY="your_company_key"
```

**Windows PowerShell:**
```powershell
$env:BIGCHANGE_USERNAME="your_username"
$env:BIGCHANGE_PASSWORD="your_password"
$env:BIGCHANGE_KEY="your_company_key"
```

**Windows CMD:**
```cmd
set BIGCHANGE_USERNAME=your_username
set BIGCHANGE_PASSWORD=your_password
set BIGCHANGE_KEY=your_company_key
```

### 2. Run the Dashboard

```bash
python app.py
```

### 3. Open in Browser

Go to: **http://localhost:5000**

## Features

- 📊 **Live Statistics** - Total jobs, Sent, Hold, New counts
- 📈 **Time Series Charts** - Daily/Weekly/Monthly trends
- 🎯 **Flag Breakdown** - Visual breakdown of job statuses
- 📋 **Jobs Table** - Searchable, filterable job list
- 🔄 **One-Click Sync** - Pull latest data from BigChange API
- 📤 **Export** - Export data as JSON/CSV

## Dashboard Preview

### Stats Cards
- Total VPI Jobs
- Sent (Report Sent Via AI, Report Sent To Client)
- On Hold (VPI Report On Hold - AI)
- New/Pending (New Report TKC VPI Automation)

### Charts
- Stacked bar chart showing job distribution over time
- Doughnut chart for flag breakdown
- Progress bars for each category

### Jobs Table
- Filter by flag category
- Sort by date
- View job details

## Configuration

Edit `app.py` to customize:

```python
CONFIG = {
    # VPI job type filter
    "VPI_JOB_TYPE_ID": 322563,
    
    # Flag classifications
    "SENT_FLAGS": ["Report Sent Via AI", "Report Sent To Client"],
    "HOLD_FLAGS": ["VPI Report On Hold - AI"],
    "NEW_FLAGS": ["New Report TKC VPI Automation"],
}
```

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /` | Dashboard HTML |
| `GET /api/stats` | Overall statistics |
| `GET /api/daily` | Daily summary |
| `GET /api/weekly` | Weekly summary |
| `GET /api/monthly` | Monthly summary |
| `GET /api/flags` | All flag values |
| `GET /api/jobs` | Recent jobs list |
| `POST /api/sync` | Trigger sync |

## Files

```
vpi_tracker_project/
├── app.py          # Flask backend + API
├── index.html      # Dashboard frontend
├── vpi_jobs.db     # SQLite database (created on run)
├── vpi_tracker.log # Log file
└── README.md       # This file
```

## Presenting to Managers

1. Run the dashboard locally
2. Click "Sync Now" to pull latest data
3. Use the date range selectors to focus on specific periods
4. Switch between Daily/Weekly/Monthly views
5. Export data for further analysis

## Troubleshooting

### "BigChange credentials not configured"
Make sure environment variables are set before running `python app.py`.

### No data showing
Click "Sync Now" and select a date range (e.g., January 2026).

### Connection errors
- Check your internet connection
- Verify BigChange credentials
- Check the company key is correct

## Tech Stack

- **Backend:** Python + Flask
- **Frontend:** Vanilla JS + Chart.js
- **Database:** SQLite
- **Styling:** Custom CSS (dark theme)
