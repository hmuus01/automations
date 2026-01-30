#!/usr/bin/env python3
"""
VPI Jobs Tracker - Complete Application
========================================

A web-based dashboard for tracking VPI jobs from BigChange API.

Usage:
    1. Set environment variables:
       export BIGCHANGE_USERNAME="your_username"
       export BIGCHANGE_PASSWORD="your_password"
       export BIGCHANGE_KEY="your_company_key"
    
    2. Run the application:
       python app.py
    
    3. Open browser to http://localhost:5000

"""

import os
import sys
import json
import sqlite3
import logging
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Tuple
from functools import wraps
import requests
from requests.auth import HTTPBasicAuth

# Try to import Flask, install if not present
try:
    from flask import Flask, jsonify, request, send_from_directory, render_template_string
    from flask_cors import CORS
except ImportError:
    print("Installing required packages...")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "flask", "flask-cors", "-q"])
    from flask import Flask, jsonify, request, send_from_directory, render_template_string
    from flask_cors import CORS

# ============================================================================
# CONFIGURATION
# ============================================================================

CONFIG = {
    # API Settings
    "BASE_URL": "https://webservice.bigchange.com/v01/services.ashx",
    
    # Auth from environment variables
    "USERNAME": os.environ.get("BIGCHANGE_USERNAME"),
    "PASSWORD": os.environ.get("BIGCHANGE_PASSWORD"),
    "COMPANY_KEY": os.environ.get("BIGCHANGE_KEY"),
    
    # VPI Filter
    "VPI_JOB_TYPE_ID": 322563,
    
    # CurrentFlag Classification
    "SENT_FLAGS": ["Report Sent Via AI", "Report Sent To Client"],
    "HOLD_FLAGS": ["VPI Report On Hold - AI"],
    "NEW_FLAGS": ["New Report TKC VPI Automation"],
    
    # Pagination
    "PAGE_SIZE": 5000,
    
    # Database
    "DB_PATH": "vpi_jobs.db",
    
    # Retry settings
    "MAX_RETRIES": 3,
    "RETRY_DELAY_SECONDS": 2,
    "REQUEST_DELAY_MS": 500,
}

# ============================================================================
# LOGGING
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("vpi_tracker.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# ============================================================================
# FLASK APP
# ============================================================================

app = Flask(__name__, static_folder='static')
CORS(app)

# ============================================================================
# DATABASE
# ============================================================================

def get_db():
    """Get database connection."""
    conn = sqlite3.connect(CONFIG["DB_PATH"])
    conn.row_factory = sqlite3.Row
    return conn

def init_database():
    """Initialize database with required tables."""
    conn = get_db()
    cursor = conn.cursor()
    
    cursor.execute("PRAGMA journal_mode=WAL")
    
    # Jobs raw table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS jobs_raw (
            job_id INTEGER PRIMARY KEY,
            job_ref TEXT,
            job_type TEXT,
            job_type_id INTEGER,
            job_category TEXT,
            job_category_id INTEGER,
            contact TEXT,
            contact_id INTEGER,
            contact_parent_id INTEGER,
            postcode TEXT,
            location TEXT,
            resource TEXT,
            status TEXT,
            status_id INTEGER,
            status_date DATETIME,
            status_comment TEXT,
            planned_start DATETIME,
            planned_end DATETIME,
            duration TEXT,
            real_start DATETIME,
            real_end DATETIME,
            real_duration TEXT,
            due_date DATETIME,
            created DATETIME,
            scheduled DATETIME,
            current_flag TEXT,
            flag_category TEXT,
            description TEXT,
            job_po TEXT,
            actioned TEXT,
            raw_json TEXT,
            last_synced DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Summary tables
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS jobs_daily_summary (
            status_date DATE PRIMARY KEY,
            total_jobs INTEGER DEFAULT 0,
            sent_count INTEGER DEFAULT 0,
            sent_ai_count INTEGER DEFAULT 0,
            sent_manual_count INTEGER DEFAULT 0,
            hold_count INTEGER DEFAULT 0,
            new_count INTEGER DEFAULT 0,
            other_count INTEGER DEFAULT 0,
            completed_count INTEGER DEFAULT 0,
            last_updated DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS jobs_weekly_summary (
            week_start DATE PRIMARY KEY,
            year_week TEXT,
            total_jobs INTEGER DEFAULT 0,
            sent_count INTEGER DEFAULT 0,
            sent_ai_count INTEGER DEFAULT 0,
            sent_manual_count INTEGER DEFAULT 0,
            hold_count INTEGER DEFAULT 0,
            new_count INTEGER DEFAULT 0,
            other_count INTEGER DEFAULT 0,
            completed_count INTEGER DEFAULT 0,
            last_updated DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS jobs_monthly_summary (
            month_start DATE PRIMARY KEY,
            year_month TEXT,
            total_jobs INTEGER DEFAULT 0,
            sent_count INTEGER DEFAULT 0,
            sent_ai_count INTEGER DEFAULT 0,
            sent_manual_count INTEGER DEFAULT 0,
            hold_count INTEGER DEFAULT 0,
            new_count INTEGER DEFAULT 0,
            other_count INTEGER DEFAULT 0,
            completed_count INTEGER DEFAULT 0,
            last_updated DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS flag_values (
            flag_value TEXT PRIMARY KEY,
            flag_category TEXT,
            first_seen DATETIME DEFAULT CURRENT_TIMESTAMP,
            last_seen DATETIME DEFAULT CURRENT_TIMESTAMP,
            job_count INTEGER DEFAULT 0
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sync_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_start DATETIME,
            run_end DATETIME,
            date_from DATE,
            date_to DATE,
            jobs_fetched INTEGER DEFAULT 0,
            jobs_inserted INTEGER DEFAULT 0,
            jobs_updated INTEGER DEFAULT 0,
            status TEXT,
            error_message TEXT
        )
    """)
    
    # Indexes
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_jobs_status_date ON jobs_raw(status_date)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_jobs_created ON jobs_raw(created)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_jobs_current_flag ON jobs_raw(current_flag)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_jobs_flag_category ON jobs_raw(flag_category)")

    conn.commit()
    conn.close()

    # Run migration to update existing data
    migrate_flag_categories()

    logger.info("Database initialized")


def migrate_flag_categories():
    """Migrate existing 'Sent' flag_category to 'Sent_AI' or 'Sent_Manual' and add new columns."""
    conn = get_db()
    cursor = conn.cursor()

    # Add new columns to summary tables if they don't exist
    for table in ['jobs_daily_summary', 'jobs_weekly_summary', 'jobs_monthly_summary']:
        try:
            cursor.execute(f"ALTER TABLE {table} ADD COLUMN sent_ai_count INTEGER DEFAULT 0")
        except sqlite3.OperationalError:
            pass  # Column already exists
        try:
            cursor.execute(f"ALTER TABLE {table} ADD COLUMN sent_manual_count INTEGER DEFAULT 0")
        except sqlite3.OperationalError:
            pass  # Column already exists

    # Update Sent_AI for "Report Sent Via AI"
    cursor.execute("""
        UPDATE jobs_raw
        SET flag_category = 'Sent_AI'
        WHERE current_flag = 'Report Sent Via AI' AND flag_category = 'Sent'
    """)
    ai_updated = cursor.rowcount

    # Update Sent_Manual for "Report Sent To Client"
    cursor.execute("""
        UPDATE jobs_raw
        SET flag_category = 'Sent_Manual'
        WHERE current_flag = 'Report Sent To Client' AND flag_category = 'Sent'
    """)
    manual_updated = cursor.rowcount

    conn.commit()
    conn.close()

    if ai_updated > 0 or manual_updated > 0:
        logger.info(f"Migrated flag categories: {ai_updated} to Sent_AI, {manual_updated} to Sent_Manual")
        # Refresh summaries after migration
        refresh_summaries()
    else:
        # Still refresh summaries to populate new columns
        refresh_summaries()

# ============================================================================
# BIGCHANGE API CLIENT
# ============================================================================

class BigChangeClient:
    """Client for BigChange API."""
    
    def __init__(self, config: dict):
        self.base_url = config["BASE_URL"]
        self.company_key = config["COMPANY_KEY"]
        self.page_size = config["PAGE_SIZE"]
        self.max_retries = config["MAX_RETRIES"]
        self.retry_delay = config["RETRY_DELAY_SECONDS"]
        
        username = config["USERNAME"]
        password = config["PASSWORD"]
        
        if not all([username, password, self.company_key]):
            raise ValueError("Missing BigChange credentials")
        
        self.auth = HTTPBasicAuth(username, password)
    
    def _make_request(self, params: dict) -> dict:
        """Make API request with retry logic."""
        import time
        
        params["key"] = self.company_key
        
        for attempt in range(self.max_retries):
            try:
                if attempt > 0:
                    time.sleep(self.retry_delay * attempt)
                
                response = requests.get(
                    self.base_url,
                    params=params,
                    auth=self.auth,
                    timeout=120
                )
                response.raise_for_status()
                data = response.json()
                
                if isinstance(data, dict) and "Code" in data and data.get("Code") != 0:
                    raise Exception(f"API Error: {data.get('Result', 'Unknown')}")
                
                return data
                
            except requests.exceptions.RequestException as e:
                logger.warning(f"Request failed (attempt {attempt + 1}): {e}")
                if attempt >= self.max_retries - 1:
                    raise
        
        return {}
    
    def get_jobs(self, start_date: str, end_date: str, page: int = 0) -> List[Dict]:
        """Fetch jobs from API."""
        params = {
            "action": "Jobslist",
            "Start": f"{start_date} 00:00:00",
            "End": f"{end_date} 23:59:59",
            "Page": page,
            "PageSize": self.page_size,
            "Format": "json",
            "Includetime": 1,
            "Allocated": 1,
            "Unallocated": 1,
            "Actioned": 1,
            "Unactioned": 1,
            "JobTypeId": CONFIG["VPI_JOB_TYPE_ID"],
            "DateOptionId": 2,  # 2 = Creation Date
        }
        
        result = self._make_request(params)
        
        if isinstance(result, list):
            return result
        return result.get("Result", [])
    
    def get_all_jobs(self, start_date: str, end_date: str) -> List[Dict]:
        """Fetch all jobs with pagination."""
        all_jobs = []
        page = 0
        
        while True:
            jobs = self.get_jobs(start_date, end_date, page)
            
            if not jobs:
                break
            
            all_jobs.extend(jobs)
            logger.info(f"Page {page}: {len(jobs)} jobs (total: {len(all_jobs)})")
            
            if len(jobs) < self.page_size:
                break
            
            page += 1
            if page > 100:
                break
        
        return all_jobs

# ============================================================================
# DATA PROCESSING
# ============================================================================

def classify_flag(current_flag: Optional[str]) -> str:
    """Classify CurrentFlag into category with AI/Manual distinction for Sent."""
    if not current_flag:
        return "Other"

    flag = current_flag.strip()

    # Distinguish between AI and Manual sent
    if flag == "Report Sent Via AI":
        return "Sent_AI"
    if flag == "Report Sent To Client":
        return "Sent_Manual"
    if flag in CONFIG["HOLD_FLAGS"]:
        return "Hold"
    if flag in CONFIG["NEW_FLAGS"]:
        return "New"

    return "Other"

def upsert_jobs(jobs: List[Dict]) -> Tuple[int, int]:
    """Insert or update jobs."""
    conn = get_db()
    cursor = conn.cursor()
    inserted = 0
    updated = 0
    
    for job in jobs:
        job_id = job.get("JobId")
        if not job_id:
            continue
        
        cursor.execute("SELECT 1 FROM jobs_raw WHERE job_id = ?", (job_id,))
        exists = cursor.fetchone() is not None
        
        current_flag = job.get("CurrentFlag")
        flag_category = classify_flag(current_flag)
        
        cursor.execute("""
            INSERT OR REPLACE INTO jobs_raw (
                job_id, job_ref, job_type, job_type_id, job_category, job_category_id,
                contact, contact_id, contact_parent_id, postcode, location, resource,
                status, status_id, status_date, status_comment,
                planned_start, planned_end, duration,
                real_start, real_end, real_duration, due_date,
                created, scheduled, current_flag, flag_category,
                description, job_po, actioned, raw_json, last_synced
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            job_id, job.get("Ref"), job.get("Type"), job.get("JobTypeId"),
            job.get("Category"), job.get("JobCategoryId"),
            job.get("Contact"), job.get("ContactId"), job.get("ContactParentId"),
            job.get("Postcode"), job.get("Location"), job.get("Resource"),
            job.get("Status"), job.get("StatusId"), job.get("StatusDate"),
            job.get("StatusComment"), job.get("PlannedStart"), job.get("PlannedEnd"),
            job.get("Duration"), job.get("RealStart"), job.get("RealEnd"),
            job.get("RealDuration"), job.get("DueDate"), job.get("Created"),
            job.get("Scheduled"), current_flag, flag_category,
            job.get("Description"), job.get("JobPO"), job.get("Actioned"),
            json.dumps(job), datetime.utcnow().isoformat()
        ))
        
        if exists:
            updated += 1
        else:
            inserted += 1
    
    conn.commit()
    conn.close()
    return inserted, updated

def refresh_summaries():
    """Refresh all summary tables."""
    conn = get_db()
    cursor = conn.cursor()

    # Daily - grouped by created date (Creation Date from BigChange)
    cursor.execute("DELETE FROM jobs_daily_summary")
    cursor.execute("""
        INSERT INTO jobs_daily_summary (status_date, total_jobs, sent_count, sent_ai_count, sent_manual_count, hold_count, new_count, other_count, completed_count)
        SELECT
            DATE(created),
            COUNT(*),
            SUM(CASE WHEN flag_category IN ('Sent_AI', 'Sent_Manual') THEN 1 ELSE 0 END),
            SUM(CASE WHEN flag_category = 'Sent_AI' THEN 1 ELSE 0 END),
            SUM(CASE WHEN flag_category = 'Sent_Manual' THEN 1 ELSE 0 END),
            SUM(CASE WHEN flag_category = 'Hold' THEN 1 ELSE 0 END),
            SUM(CASE WHEN flag_category = 'New' THEN 1 ELSE 0 END),
            SUM(CASE WHEN flag_category = 'Other' THEN 1 ELSE 0 END),
            SUM(CASE WHEN status_id IN (12, 13) THEN 1 ELSE 0 END)
        FROM jobs_raw WHERE created IS NOT NULL
        GROUP BY DATE(created)
    """)

    # Weekly - grouped by created date (Creation Date from BigChange)
    cursor.execute("DELETE FROM jobs_weekly_summary")
    cursor.execute("""
        INSERT INTO jobs_weekly_summary (week_start, year_week, total_jobs, sent_count, sent_ai_count, sent_manual_count, hold_count, new_count, other_count, completed_count)
        SELECT
            DATE(created, 'weekday 0', '-6 days'),
            STRFTIME('%Y-W%W', created),
            COUNT(*),
            SUM(CASE WHEN flag_category IN ('Sent_AI', 'Sent_Manual') THEN 1 ELSE 0 END),
            SUM(CASE WHEN flag_category = 'Sent_AI' THEN 1 ELSE 0 END),
            SUM(CASE WHEN flag_category = 'Sent_Manual' THEN 1 ELSE 0 END),
            SUM(CASE WHEN flag_category = 'Hold' THEN 1 ELSE 0 END),
            SUM(CASE WHEN flag_category = 'New' THEN 1 ELSE 0 END),
            SUM(CASE WHEN flag_category = 'Other' THEN 1 ELSE 0 END),
            SUM(CASE WHEN status_id IN (12, 13) THEN 1 ELSE 0 END)
        FROM jobs_raw WHERE created IS NOT NULL
        GROUP BY DATE(created, 'weekday 0', '-6 days')
    """)

    # Monthly - grouped by created date (Creation Date from BigChange)
    cursor.execute("DELETE FROM jobs_monthly_summary")
    cursor.execute("""
        INSERT INTO jobs_monthly_summary (month_start, year_month, total_jobs, sent_count, sent_ai_count, sent_manual_count, hold_count, new_count, other_count, completed_count)
        SELECT
            DATE(created, 'start of month'),
            STRFTIME('%Y-%m', created),
            COUNT(*),
            SUM(CASE WHEN flag_category IN ('Sent_AI', 'Sent_Manual') THEN 1 ELSE 0 END),
            SUM(CASE WHEN flag_category = 'Sent_AI' THEN 1 ELSE 0 END),
            SUM(CASE WHEN flag_category = 'Sent_Manual' THEN 1 ELSE 0 END),
            SUM(CASE WHEN flag_category = 'Hold' THEN 1 ELSE 0 END),
            SUM(CASE WHEN flag_category = 'New' THEN 1 ELSE 0 END),
            SUM(CASE WHEN flag_category = 'Other' THEN 1 ELSE 0 END),
            SUM(CASE WHEN status_id IN (12, 13) THEN 1 ELSE 0 END)
        FROM jobs_raw WHERE created IS NOT NULL
        GROUP BY DATE(created, 'start of month')
    """)
    
    # Flag values
    cursor.execute("""
        INSERT OR REPLACE INTO flag_values (flag_value, flag_category, last_seen, job_count)
        SELECT current_flag, flag_category, MAX(last_synced), COUNT(*)
        FROM jobs_raw WHERE current_flag IS NOT NULL
        GROUP BY current_flag
    """)
    
    conn.commit()
    conn.close()

# ============================================================================
# API ROUTES
# ============================================================================

@app.route('/')
def index():
    """Serve the dashboard."""
    return send_from_directory('.', 'index.html')

@app.route('/api/stats')
def get_stats():
    """Get overall statistics with optional date filtering."""
    start_date = request.args.get('start')
    end_date = request.args.get('end')

    conn = get_db()
    cursor = conn.cursor()

    # Build date filter clause - filter by created date (Creation Date)
    date_filter = ""
    params = []
    if start_date and end_date:
        date_filter = "WHERE DATE(created) >= ? AND DATE(created) <= ?"
        params = [start_date, end_date]
    elif start_date:
        date_filter = "WHERE DATE(created) >= ?"
        params = [start_date]
    elif end_date:
        date_filter = "WHERE DATE(created) <= ?"
        params = [end_date]

    # Total jobs
    cursor.execute(f"SELECT COUNT(*) FROM jobs_raw {date_filter}", params)
    total = cursor.fetchone()[0]

    # By flag category (raw categories including Sent_AI and Sent_Manual)
    cursor.execute(f"""
        SELECT flag_category, COUNT(*) as cnt
        FROM jobs_raw {date_filter}
        GROUP BY flag_category
    """, params)
    by_flag_raw = {row[0]: row[1] for row in cursor.fetchall()}

    # Compute combined Sent and breakdown
    sent_ai = by_flag_raw.get('Sent_AI', 0)
    sent_manual = by_flag_raw.get('Sent_Manual', 0)
    total_sent = sent_ai + sent_manual

    # Build the by_flag dict with both combined and individual metrics
    by_flag = {
        'Sent': total_sent,
        'Sent_AI': sent_ai,
        'Sent_Manual': sent_manual,
        'Hold': by_flag_raw.get('Hold', 0),
        'New': by_flag_raw.get('New', 0),
        'Other': by_flag_raw.get('Other', 0)
    }

    # AI Automation Rate
    ai_rate = (sent_ai / total_sent * 100) if total_sent > 0 else 0

    # By status
    cursor.execute(f"""
        SELECT status, COUNT(*) as cnt
        FROM jobs_raw {date_filter}
        GROUP BY status ORDER BY cnt DESC
    """, params)
    by_status = {row[0]: row[1] for row in cursor.fetchall()}

    # Last sync
    cursor.execute("""
        SELECT run_end, jobs_fetched, status
        FROM sync_log ORDER BY id DESC LIMIT 1
    """)
    last_sync = cursor.fetchone()

    conn.close()

    return jsonify({
        "total_jobs": total,
        "by_flag": by_flag,
        "by_status": by_status,
        "ai_automation_rate": round(ai_rate, 1),
        "date_filter": {
            "start": start_date,
            "end": end_date,
            "active": bool(start_date or end_date)
        },
        "last_sync": {
            "time": last_sync[0] if last_sync else None,
            "jobs_fetched": last_sync[1] if last_sync else 0,
            "status": last_sync[2] if last_sync else "never"
        }
    })

@app.route('/api/daily')
def get_daily():
    """Get daily summary with optional date filtering."""
    start_date = request.args.get('start')
    end_date = request.args.get('end')

    conn = get_db()
    cursor = conn.cursor()

    # Build query with optional date filter
    query = """
        SELECT status_date, total_jobs, sent_count, sent_ai_count, sent_manual_count,
               hold_count, new_count, other_count, completed_count
        FROM jobs_daily_summary
    """
    params = []

    if start_date and end_date:
        query += " WHERE status_date >= ? AND status_date <= ?"
        params = [start_date, end_date]
    elif start_date:
        query += " WHERE status_date >= ?"
        params = [start_date]
    elif end_date:
        query += " WHERE status_date <= ?"
        params = [end_date]

    query += " ORDER BY status_date"

    cursor.execute(query, params)
    data = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return jsonify(data)

@app.route('/api/weekly')
def get_weekly():
    """Get weekly summary with optional date filtering."""
    start_date = request.args.get('start')
    end_date = request.args.get('end')

    conn = get_db()
    cursor = conn.cursor()

    query = """
        SELECT week_start, year_week, total_jobs, sent_count, sent_ai_count, sent_manual_count,
               hold_count, new_count, other_count, completed_count
        FROM jobs_weekly_summary
    """
    params = []

    if start_date and end_date:
        query += " WHERE week_start >= ? AND week_start <= ?"
        params = [start_date, end_date]
    elif start_date:
        query += " WHERE week_start >= ?"
        params = [start_date]
    elif end_date:
        query += " WHERE week_start <= ?"
        params = [end_date]

    query += " ORDER BY week_start"

    cursor.execute(query, params)
    data = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return jsonify(data)

@app.route('/api/monthly')
def get_monthly():
    """Get monthly summary with optional date filtering."""
    start_date = request.args.get('start')
    end_date = request.args.get('end')

    conn = get_db()
    cursor = conn.cursor()

    query = """
        SELECT month_start, year_month, total_jobs, sent_count, sent_ai_count, sent_manual_count,
               hold_count, new_count, other_count, completed_count
        FROM jobs_monthly_summary
    """
    params = []

    if start_date and end_date:
        query += " WHERE month_start >= ? AND month_start <= ?"
        params = [start_date, end_date]
    elif start_date:
        query += " WHERE month_start >= ?"
        params = [start_date]
    elif end_date:
        query += " WHERE month_start <= ?"
        params = [end_date]

    query += " ORDER BY month_start"

    cursor.execute(query, params)
    data = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return jsonify(data)

@app.route('/api/flags')
def get_flags():
    """Get all flag values."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT flag_value, flag_category, job_count, last_seen
        FROM flag_values ORDER BY job_count DESC
    """)
    data = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return jsonify(data)

@app.route('/api/jobs')
def get_jobs():
    """Get recent jobs with optional date and flag filtering."""
    limit = request.args.get('limit', 100, type=int)
    flag = request.args.get('flag', None)
    start_date = request.args.get('start')
    end_date = request.args.get('end')

    conn = get_db()
    cursor = conn.cursor()

    # Build query - include created date for display
    query = """
        SELECT job_id, job_ref, contact, resource, status, current_flag, flag_category, created, status_date
        FROM jobs_raw
    """
    conditions = []
    params = []

    # Flag filter - handle combined "Sent" filter
    if flag:
        if flag == 'Sent':
            conditions.append("flag_category IN ('Sent_AI', 'Sent_Manual')")
        else:
            conditions.append("flag_category = ?")
            params.append(flag)

    # Date filters - filter by created date (Creation Date)
    if start_date:
        conditions.append("DATE(created) >= ?")
        params.append(start_date)
    if end_date:
        conditions.append("DATE(created) <= ?")
        params.append(end_date)

    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    query += " ORDER BY created DESC LIMIT ?"
    params.append(limit)

    cursor.execute(query, params)
    data = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return jsonify(data)

@app.route('/api/sync', methods=['POST'])
def sync_jobs():
    """Trigger a sync from BigChange API."""
    data = request.json or {}
    start_date = data.get('start', (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d'))
    end_date = data.get('end', datetime.now().strftime('%Y-%m-%d'))
    
    # Check credentials
    if not all([CONFIG["USERNAME"], CONFIG["PASSWORD"], CONFIG["COMPANY_KEY"]]):
        return jsonify({
            "success": False,
            "error": "Missing BigChange credentials. Set BIGCHANGE_USERNAME, BIGCHANGE_PASSWORD, BIGCHANGE_KEY environment variables."
        }), 400
    
    # Log sync start
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO sync_log (run_start, date_from, date_to, status) VALUES (?, ?, ?, ?)",
        (datetime.utcnow().isoformat(), start_date, end_date, "running")
    )
    sync_id = cursor.lastrowid
    conn.commit()
    conn.close()
    
    try:
        client = BigChangeClient(CONFIG)
        jobs = client.get_all_jobs(start_date, end_date)
        
        inserted, updated = upsert_jobs(jobs)
        refresh_summaries()
        
        # Update sync log
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE sync_log SET run_end = ?, jobs_fetched = ?, jobs_inserted = ?, jobs_updated = ?, status = ?
            WHERE id = ?
        """, (datetime.utcnow().isoformat(), len(jobs), inserted, updated, "success", sync_id))
        conn.commit()
        conn.close()
        
        return jsonify({
            "success": True,
            "jobs_fetched": len(jobs),
            "inserted": inserted,
            "updated": updated
        })
        
    except Exception as e:
        logger.error(f"Sync failed: {e}")
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE sync_log SET run_end = ?, status = ?, error_message = ? WHERE id = ?",
            (datetime.utcnow().isoformat(), "error", str(e), sync_id)
        )
        conn.commit()
        conn.close()
        
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/config')
def get_config():
    """Get current configuration (safe info only)."""
    return jsonify({
        "vpi_job_type_id": CONFIG["VPI_JOB_TYPE_ID"],
        "sent_flags": CONFIG["SENT_FLAGS"],
        "hold_flags": CONFIG["HOLD_FLAGS"],
        "new_flags": CONFIG["NEW_FLAGS"],
        "has_credentials": all([CONFIG["USERNAME"], CONFIG["PASSWORD"], CONFIG["COMPANY_KEY"]])
    })

# ============================================================================
# MAIN
# ============================================================================

if __name__ == '__main__':
    init_database()
    print("\n" + "="*60)
    print("VPI Jobs Tracker Dashboard")
    print("="*60)
    print(f"\n🌐 Open http://localhost:5000 in your browser")
    print(f"\n📊 Database: {CONFIG['DB_PATH']}")
    
    if all([CONFIG["USERNAME"], CONFIG["PASSWORD"], CONFIG["COMPANY_KEY"]]):
        print("✅ BigChange credentials configured")
    else:
        print("⚠️  Set environment variables:")
        print("   export BIGCHANGE_USERNAME='...'")
        print("   export BIGCHANGE_PASSWORD='...'")
        print("   export BIGCHANGE_KEY='...'")
    
    print("\n" + "="*60 + "\n")
    
    app.run(debug=True, host='0.0.0.0', port=5001)
