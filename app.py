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

import re
from flask import Flask, jsonify, request, send_from_directory, render_template_string, redirect, url_for, flash
from flask_cors import CORS
from flask_login import LoginManager, UserMixin, login_user, logout_user, login_required, current_user
from werkzeug.security import generate_password_hash, check_password_hash

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

    # Alarm Activation Filter
    "ALARM_JOB_TYPE_ID": 474681,
    
    # CurrentFlag Classification (VPI)
    "SENT_FLAGS": ["Report Sent Via AI", "Report Sent To Client"],
    "HOLD_FLAGS": ["VPI Report On Hold - AI"],
    "NEW_FLAGS": ["New Report TKC VPI Automation"],

    # CurrentFlag Classification (Alarm Activation)
    "ALARM_SENT_FLAGS": ["Alarm report sent by AI", "Report Sent To Client"],
    "ALARM_HOLD_FLAGS": ["Alarm report to be reviewed"],
    "ALARM_NEW_FLAGS": ["New Report TKC Alarm Activation Automation"],

    # Patrol Jobs - Real Time Filter
    "PATROL_JOB_TYPE_ID": 350775,

    # CurrentFlag Classification (Patrol)
    "PATROL_SENT_FLAGS": ["No patrol incident identified by AI", "Patrol Incident identified by AI", "Report Sent To Client"],
    "PATROL_HOLD_FLAGS": [],
    "PATROL_NEW_FLAGS": ["New Report TKC Patrol Job Automation"],
    
    # Pagination
    "PAGE_SIZE": 5000,
    
    # Database
    "DB_PATH": os.environ.get("DB_PATH", "vpi_jobs.db"),
    
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
app.secret_key = os.environ.get("SECRET_KEY", "dev-change-me-in-production")
app.permanent_session_lifetime = timedelta(hours=8)
CORS(app)

# ============================================================================
# AUTHENTICATION
# ============================================================================

login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = "login"


class User(UserMixin):
    """User model backed by SQLite."""

    def __init__(self, id, email, password_hash, name, is_admin=False):
        self.id = id
        self.email = email
        self.password_hash = password_hash
        self.name = name
        self.is_admin = bool(is_admin)

    def check_password(self, password):
        return check_password_hash(self.password_hash, password)

    @staticmethod
    def get_by_id(user_id):
        conn = get_db()
        row = conn.execute("SELECT id, email, password_hash, name, is_admin FROM users WHERE id = ?", (user_id,)).fetchone()
        conn.close()
        if row:
            return User(row[0], row[1], row[2], row[3], row[4])
        return None

    @staticmethod
    def get_by_email(email):
        conn = get_db()
        row = conn.execute("SELECT id, email, password_hash, name, is_admin FROM users WHERE email = ?", (email,)).fetchone()
        conn.close()
        if row:
            return User(row[0], row[1], row[2], row[3], row[4])
        return None


@login_manager.user_loader
def load_user(user_id):
    return User.get_by_id(int(user_id))


@login_manager.unauthorized_handler
def unauthorized():
    if request.path.startswith("/api/"):
        return jsonify({"error": "Authentication required"}), 401
    return redirect(url_for("login"))


LOGIN_PAGE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Login | FRG Automations</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=DM+Sans:opsz,wght@9..40,400;9..40,500;9..40,600;9..40,700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }

        body {
            font-family: 'DM Sans', -apple-system, sans-serif;
            background: #f5f5f7;
            color: #1a1a1a;
            display: flex;
            align-items: center;
            justify-content: center;
            min-height: 100vh;
            overflow: hidden;
        }

        .bg-pattern {
            position: fixed; top: 0; left: 0; width: 100%; height: 100%; z-index: 0;
            opacity: 0.6;
            background-image:
                radial-gradient(circle at 20% 80%, rgba(227, 24, 55, 0.03) 0%, transparent 50%),
                radial-gradient(circle at 80% 20%, rgba(0, 166, 81, 0.03) 0%, transparent 50%),
                radial-gradient(circle at 50% 50%, rgba(247, 148, 29, 0.02) 0%, transparent 70%);
        }

        .login-card {
            position: relative; z-index: 1;
            background: #ffffff;
            border: 1px solid #e0e0e0;
            border-radius: 16px;
            padding: 48px 40px;
            width: 100%; max-width: 420px;
            box-shadow: 0 8px 32px rgba(0, 0, 0, 0.06);
        }

        .logo-row {
            display: flex; align-items: center; justify-content: center; gap: 12px;
            margin-bottom: 8px;
        }
        .logo-row img { height: 40px; }
        .logo-row span {
            font-size: 1.25rem; font-weight: 700; color: #1a1a1a;
            letter-spacing: -0.02em;
        }

        .subtitle {
            text-align: center; color: #999999; font-size: 0.85rem;
            margin-bottom: 32px;
        }

        label {
            display: block; font-weight: 600; font-size: 0.8rem;
            color: #666666; margin-bottom: 6px;
            text-transform: uppercase; letter-spacing: 0.04em;
        }

        input[type="email"],
        input[type="password"] {
            width: 100%; padding: 12px 14px;
            background: #f5f5f7;
            border: 1px solid #e0e0e0;
            border-radius: 10px;
            font-size: 0.95rem; font-family: inherit;
            color: #1a1a1a;
            margin-bottom: 20px;
            transition: border-color 0.2s, box-shadow 0.2s;
        }
        input::placeholder { color: #999999; }
        input:focus {
            outline: none;
            border-color: #E31837;
            box-shadow: 0 0 0 3px rgba(227, 24, 55, 0.12);
        }

        button {
            width: 100%; padding: 14px;
            background: linear-gradient(135deg, #E31837 0%, #c01530 100%);
            color: #fff; border: none; border-radius: 10px;
            font-size: 1rem; font-weight: 600; cursor: pointer;
            font-family: inherit;
            transition: transform 0.15s, box-shadow 0.2s;
            box-shadow: 0 4px 14px rgba(227, 24, 55, 0.25);
        }
        button:hover {
            transform: translateY(-1px);
            box-shadow: 0 6px 20px rgba(227, 24, 55, 0.35);
        }
        button:active { transform: translateY(0); }

        .error {
            background: rgba(227, 24, 55, 0.08);
            color: #E31837;
            border: 1px solid rgba(227, 24, 55, 0.2);
            padding: 12px 16px;
            border-radius: 10px;
            text-align: center;
            font-size: 0.85rem; font-weight: 500;
            margin-bottom: 20px;
        }

        .footer {
            text-align: center; margin-top: 28px;
            font-size: 0.75rem; color: #999999;
        }

        .pw-wrapper {
            position: relative;
        }
        .pw-wrapper input {
            padding-right: 44px;
        }
        .pw-toggle {
            position: absolute; right: 12px; top: 50%; transform: translateY(-50%);
            background: none; border: none; cursor: pointer; padding: 4px;
            color: #999999; display: flex; align-items: center; width: auto;
            box-shadow: none;
        }
        .pw-toggle:hover { color: #666666; transform: translateY(-50%); box-shadow: none; }
        .pw-toggle svg { width: 20px; height: 20px; }
    </style>
</head>
<body>
    <div class="bg-pattern"></div>
    <div class="login-card">
        <div class="logo-row">
            <img src="/frg-logo.png" alt="FRG">
            <span>Automations</span>
        </div>
        <p class="subtitle">Sign in to access the dashboard</p>
        {% if error %}<div class="error">{{ error }}</div>{% endif %}
        <form method="POST">
            <label for="email">Email</label>
            <input type="email" id="email" name="email" required
                   placeholder="you@example.com" autofocus>
            <label for="password">Password</label>
            <div class="pw-wrapper">
                <input type="password" id="password" name="password" required
                       placeholder="Enter your password">
                <button type="button" class="pw-toggle" onclick="togglePassword()" aria-label="Show password">
                    <svg id="eye-open" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                        <path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"/>
                        <circle cx="12" cy="12" r="3"/>
                    </svg>
                    <svg id="eye-closed" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="display:none">
                        <path d="M17.94 17.94A10.07 10.07 0 0 1 12 20c-7 0-11-8-11-8a18.45 18.45 0 0 1 5.06-5.94"/>
                        <path d="M9.9 4.24A9.12 9.12 0 0 1 12 4c7 0 11 8 11 8a18.5 18.5 0 0 1-2.16 3.19"/>
                        <path d="M14.12 14.12a3 3 0 1 1-4.24-4.24"/>
                        <line x1="1" y1="1" x2="23" y2="23"/>
                    </svg>
                </button>
            </div>
            <button type="submit">Sign In</button>
        </form>
        <p class="footer">FRG Automations Dashboard</p>
    </div>
    <script>
    function togglePassword() {
        const input = document.getElementById('password');
        const open = document.getElementById('eye-open');
        const closed = document.getElementById('eye-closed');
        if (input.type === 'password') {
            input.type = 'text';
            open.style.display = 'none';
            closed.style.display = 'block';
        } else {
            input.type = 'password';
            open.style.display = 'block';
            closed.style.display = 'none';
        }
    }
    </script>
</body>
</html>
"""


_SQL_KEYWORDS_RE = re.compile(
    r'\b(DROP|INSERT|SELECT|DELETE|UPDATE|ALTER|UNION|EXEC)\b|--|;',
    re.IGNORECASE,
)


def _sanitise_login_email(raw):
    """Sanitise email input: strip, lowercase, enforce length, reject SQL keywords."""
    val = raw.strip().lower()
    if len(val) > 254:
        return None
    if _SQL_KEYWORDS_RE.search(val):
        return None
    if not re.match(r'^[^@\s]+@[^@\s]+\.[^@\s]+$', val):
        return None
    return val


def _sanitise_login_password(raw):
    """Sanitise password input: enforce max length."""
    if len(raw) > 128:
        return None
    return raw


@app.route("/login", methods=["GET", "POST"])
def login():
    if current_user.is_authenticated:
        return redirect(url_for("index"))
    error = None
    if request.method == "POST":
        email = _sanitise_login_email(request.form.get("email", ""))
        password = _sanitise_login_password(request.form.get("password", ""))
        if email and password:
            user = User.get_by_email(email)
            if user and user.check_password(password):
                from flask import session
                session.permanent = True
                login_user(user, remember=False)
                next_page = request.args.get("next") or url_for("index")
                return redirect(next_page)
        error = "Invalid email or password"
    return render_template_string(LOGIN_PAGE, error=error)


@app.route("/logout")
@login_required
def logout():
    logout_user()
    return redirect(url_for("login"))


@app.route('/setup-admin-xK9m2p')
def setup_admin():
    """One-time admin setup. Only works if no users exist yet.
    Reads credentials from ADMIN_EMAIL / ADMIN_PASSWORD env vars."""
    conn = get_db()
    existing = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    if existing > 0:
        conn.close()
        return "Already set up.", 403
    admin_email = os.environ.get("ADMIN_EMAIL", "hamza.muse@firstresponsegroup.com")
    admin_password = os.environ.get("ADMIN_PASSWORD")
    admin_name = os.environ.get("ADMIN_NAME", "Hamza Muse")
    if not admin_password:
        conn.close()
        return "Set ADMIN_PASSWORD environment variable first.", 400
    conn.execute(
        "INSERT INTO users (email, password_hash, name, is_admin) VALUES (?, ?, ?, 1)",
        (admin_email, generate_password_hash(admin_password), admin_name)
    )
    conn.commit()
    conn.close()
    return "Admin created. Go to /login", 200


@app.route("/api/me")
@login_required
def api_me():
    """Return current user info for nav bar rendering."""
    return jsonify({"is_admin": current_user.is_admin})


# ============================================================================
# ADMIN PANEL
# ============================================================================

ADMIN_PAGE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Admin Panel | FRG Automations</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=DM+Sans:opsz,wght@9..40,400;9..40,500;9..40,600;9..40,700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
    <style>
        :root {
            --frg-red: #E31837;
            --frg-orange: #F7941D;
            --frg-green: #00A651;
            --frg-green-light: #4DBD74;
            --bg-dark: #f5f5f7;
            --bg-card: #ffffff;
            --bg-card-hover: #fafafa;
            --border: #e0e0e0;
            --text-primary: #1a1a1a;
            --text-secondary: #666666;
            --text-muted: #999999;
        }
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: 'DM Sans', -apple-system, sans-serif;
            background: var(--bg-dark);
            color: var(--text-primary);
            min-height: 100vh;
        }
        .bg-pattern {
            position: fixed; top: 0; left: 0; width: 100%; height: 100%; z-index: -1;
            opacity: 0.6;
            background-image:
                radial-gradient(circle at 20% 80%, rgba(227,24,55,0.03) 0%, transparent 50%),
                radial-gradient(circle at 80% 20%, rgba(0,166,81,0.03) 0%, transparent 50%),
                radial-gradient(circle at 50% 50%, rgba(247,148,29,0.02) 0%, transparent 70%);
        }

        /* Header */
        header {
            background: var(--bg-card);
            border-bottom: 1px solid var(--border);
            padding: 1rem 3rem;
            display: flex; align-items: center; justify-content: space-between;
        }
        .header-left { display: flex; align-items: center; gap: 12px; }
        .header-left img { height: 36px; }
        .header-left h1 { font-size: 1.15rem; font-weight: 700; }
        .header-left p { font-size: 0.75rem; color: var(--text-muted); }
        .header-right { display: flex; align-items: center; gap: 12px; }
        .header-right span { font-size: 0.85rem; color: var(--text-secondary); }

        /* Nav */
        .nav-bar {
            display: flex; gap: 0;
            background: var(--bg-card);
            border-bottom: 1px solid var(--border);
            padding: 0 3rem;
        }
        .nav-link {
            padding: 0.75rem 1.5rem; font-size: 0.875rem; font-weight: 600;
            color: var(--text-secondary); text-decoration: none;
            border-bottom: 2px solid transparent; transition: all 0.2s;
        }
        .nav-link:hover { color: var(--text-primary); background: var(--bg-card-hover); }
        .nav-link.active { color: var(--frg-red); border-bottom-color: var(--frg-red); }

        .container { max-width: 1000px; margin: 2rem auto; padding: 0 2rem; }

        /* Flash messages */
        .flash { padding: 12px 18px; border-radius: 10px; font-size: 0.85rem; font-weight: 500; margin-bottom: 1.5rem; }
        .flash-success { background: rgba(0,166,81,0.08); color: var(--frg-green); border: 1px solid rgba(0,166,81,0.2); }
        .flash-error { background: rgba(227,24,55,0.08); color: var(--frg-red); border: 1px solid rgba(227,24,55,0.2); }

        /* Card */
        .card {
            background: var(--bg-card); border: 1px solid var(--border);
            border-radius: 16px; padding: 28px 32px;
            box-shadow: 0 4px 16px rgba(0,0,0,0.04); margin-bottom: 1.5rem;
        }
        .card h2 { font-size: 1.1rem; font-weight: 700; margin-bottom: 20px; }

        /* Table */
        table { width: 100%; border-collapse: collapse; }
        th {
            text-align: left; font-size: 0.75rem; font-weight: 600;
            color: var(--text-muted); text-transform: uppercase;
            letter-spacing: 0.04em; padding: 10px 12px;
            border-bottom: 1px solid var(--border);
        }
        td {
            padding: 14px 12px; font-size: 0.9rem;
            border-bottom: 1px solid var(--border);
            vertical-align: middle;
        }
        tr:last-child td { border-bottom: none; }
        tr:hover { background: var(--bg-card-hover); }
        .badge {
            display: inline-block; padding: 3px 10px; border-radius: 6px;
            font-size: 0.75rem; font-weight: 600;
        }
        .badge-yes { background: rgba(0,166,81,0.1); color: var(--frg-green); }
        .badge-no { background: rgba(102,102,102,0.1); color: var(--text-secondary); }
        .badge-you { background: rgba(247,148,29,0.1); color: var(--frg-orange); font-size: 0.7rem; margin-left: 6px; }

        /* Buttons */
        .btn {
            padding: 8px 16px; border-radius: 8px; font-size: 0.8rem;
            font-weight: 600; font-family: inherit; cursor: pointer;
            border: none; transition: all 0.15s; display: inline-flex;
            align-items: center; gap: 4px;
        }
        .btn-red { background: rgba(227,24,55,0.1); color: var(--frg-red); }
        .btn-red:hover { background: rgba(227,24,55,0.2); }
        .btn-blue { background: rgba(44,62,80,0.1); color: #2C3E50; }
        .btn-blue:hover { background: rgba(44,62,80,0.2); }
        .btn-primary {
            background: linear-gradient(135deg, var(--frg-green) 0%, var(--frg-green-light) 100%);
            color: #fff; padding: 12px 24px; font-size: 0.9rem;
            box-shadow: 0 4px 14px rgba(0,166,81,0.25);
        }
        .btn-primary:hover { transform: translateY(-1px); box-shadow: 0 6px 20px rgba(0,166,81,0.35); }
        .actions { display: flex; gap: 6px; align-items: center; }

        /* Form */
        .form-grid {
            display: grid; grid-template-columns: 1fr 1fr; gap: 16px;
        }
        .form-group { display: flex; flex-direction: column; }
        .form-group.full { grid-column: 1 / -1; }
        .form-group label {
            font-size: 0.8rem; font-weight: 600; color: var(--text-secondary);
            text-transform: uppercase; letter-spacing: 0.04em; margin-bottom: 6px;
        }
        .form-group input[type="text"],
        .form-group input[type="email"],
        .form-group input[type="password"] {
            padding: 11px 14px; background: var(--bg-dark);
            border: 1px solid var(--border); border-radius: 10px;
            font-size: 0.9rem; font-family: inherit; color: var(--text-primary);
            transition: border-color 0.2s, box-shadow 0.2s;
        }
        .form-group input:focus {
            outline: none; border-color: var(--frg-green);
            box-shadow: 0 0 0 3px rgba(0,166,81,0.12);
        }
        .checkbox-row {
            display: flex; align-items: center; gap: 8px;
            padding-top: 24px;
        }
        .checkbox-row input[type="checkbox"] {
            width: 18px; height: 18px; accent-color: var(--frg-green); cursor: pointer;
        }
        .checkbox-row label {
            text-transform: none; letter-spacing: 0; font-size: 0.9rem;
            color: var(--text-primary); cursor: pointer; margin-bottom: 0;
        }

        /* Inline password form */
        .pw-form {
            display: none; margin-top: 8px;
        }
        .pw-form.open { display: flex; gap: 8px; align-items: center; }
        .pw-form input {
            padding: 7px 12px; background: var(--bg-dark);
            border: 1px solid var(--border); border-radius: 8px;
            font-size: 0.82rem; font-family: inherit; color: var(--text-primary);
            width: 200px;
        }
        .pw-form input:focus { outline: none; border-color: var(--frg-green); }
        .pw-form button { padding: 7px 14px; font-size: 0.78rem; }

        @media (max-width: 768px) {
            header { padding: 1rem; }
            .nav-bar { padding: 0 1rem; overflow-x: auto; }
            .container { padding: 0 1rem; }
            .form-grid { grid-template-columns: 1fr; }
            .checkbox-row { padding-top: 0; }
            table { font-size: 0.82rem; }
            .actions { flex-direction: column; }
        }
    </style>
</head>
<body>
    <div class="bg-pattern"></div>
    <header>
        <div class="header-left">
            <img src="/frg-logo.png" alt="FRG">
            <div>
                <h1>Admin Panel</h1>
                <p>First Response Group</p>
            </div>
        </div>
        <div class="header-right">
            <span>{{ current_user.name }}</span>
            <a href="/logout" class="btn btn-blue">Logout</a>
        </div>
    </header>

    <nav class="nav-bar">
        <a href="/" class="nav-link">VPI Jobs</a>
        <a href="/alarm" class="nav-link">Alarm Activation</a>
        <a href="/patrol" class="nav-link">Patrol Jobs</a>
        <a href="/admin/users" class="nav-link active">Admin Panel</a>
    </nav>

    <div class="container">
        {% with messages = get_flashed_messages(with_categories=true) %}
        {% for category, message in messages %}
        <div class="flash flash-{{ category }}">{{ message }}</div>
        {% endfor %}
        {% endwith %}

        <!-- Users Table -->
        <div class="card">
            <h2>Users</h2>
            <table>
                <thead>
                    <tr>
                        <th>Name</th>
                        <th>Email</th>
                        <th>Admin</th>
                        <th>Actions</th>
                    </tr>
                </thead>
                <tbody>
                {% for user in users %}
                    <tr>
                        <td>
                            {{ user.name }}
                            {% if user.id == current_user.id %}<span class="badge badge-you">YOU</span>{% endif %}
                        </td>
                        <td>{{ user.email }}</td>
                        <td>
                            <span class="badge {{ 'badge-yes' if user.is_admin else 'badge-no' }}">
                                {{ "Yes" if user.is_admin else "No" }}
                            </span>
                        </td>
                        <td>
                            <div class="actions">
                                <button class="btn btn-blue" onclick="togglePw({{ user.id }})">Change Password</button>
                                {% if user.id != current_user.id %}
                                <form method="POST" action="/admin/users/delete/{{ user.id }}"
                                      onsubmit="return confirm('Delete {{ user.name }}?')">
                                    <button type="submit" class="btn btn-red">Delete</button>
                                </form>
                                {% endif %}
                            </div>
                            <form class="pw-form" id="pw-{{ user.id }}" method="POST" action="/admin/users/password/{{ user.id }}">
                                <input type="password" name="new_password" placeholder="New password (min 8)" required minlength="8">
                                <button type="submit" class="btn btn-primary" style="padding:7px 14px;font-size:0.78rem;">Save</button>
                            </form>
                        </td>
                    </tr>
                {% endfor %}
                </tbody>
            </table>
        </div>

        <!-- Add User Form -->
        <div class="card">
            <h2>Add New User</h2>
            <form method="POST" action="/admin/users/add">
                <div class="form-grid">
                    <div class="form-group">
                        <label for="name">Full Name</label>
                        <input type="text" id="name" name="name" required placeholder="John Smith">
                    </div>
                    <div class="form-group">
                        <label for="email">Email</label>
                        <input type="email" id="email" name="email" required placeholder="john@example.com">
                    </div>
                    <div class="form-group">
                        <label for="password">Password</label>
                        <input type="password" id="password" name="password" required minlength="8" placeholder="Min 8 characters">
                    </div>
                    <div class="form-group">
                        <div class="checkbox-row">
                            <input type="checkbox" id="is_admin" name="is_admin" value="1">
                            <label for="is_admin">Administrator</label>
                        </div>
                    </div>
                    <div class="form-group full">
                        <button type="submit" class="btn btn-primary">Add User</button>
                    </div>
                </div>
            </form>
        </div>
    </div>

    <script>
    function togglePw(id) {
        const el = document.getElementById('pw-' + id);
        el.classList.toggle('open');
        if (el.classList.contains('open')) el.querySelector('input').focus();
    }
    </script>
</body>
</html>
"""


@app.route("/admin/users")
@login_required
def admin_users():
    if not current_user.is_admin:
        flash("Access denied", "error")
        return redirect(url_for("index"))
    conn = get_db()
    rows = conn.execute("SELECT id, email, password_hash, name, is_admin FROM users ORDER BY id").fetchall()
    conn.close()
    users = [User(r[0], r[1], r[2], r[3], r[4]) for r in rows]
    return render_template_string(ADMIN_PAGE, users=users, current_user=current_user)


@app.route("/admin/users/add", methods=["POST"])
@login_required
def admin_add_user():
    if not current_user.is_admin:
        flash("Access denied", "error")
        return redirect(url_for("index"))

    name = request.form.get("name", "").strip()[:128]
    email = _sanitise_login_email(request.form.get("email", ""))
    password = request.form.get("password", "")
    is_admin = 1 if request.form.get("is_admin") == "1" else 0

    if not name:
        flash("Name is required", "error")
        return redirect(url_for("admin_users"))

    if not email:
        flash("Valid email is required (max 254 chars, no SQL keywords)", "error")
        return redirect(url_for("admin_users"))

    if len(password) < 8 or len(password) > 128:
        flash("Password must be 8-128 characters", "error")
        return redirect(url_for("admin_users"))

    conn = get_db()
    existing = conn.execute("SELECT id FROM users WHERE email = ?", (email,)).fetchone()
    if existing:
        conn.close()
        flash("A user with that email already exists", "error")
        return redirect(url_for("admin_users"))

    conn.execute(
        "INSERT INTO users (email, password_hash, name, is_admin) VALUES (?, ?, ?, ?)",
        (email, generate_password_hash(password), name, is_admin)
    )
    conn.commit()
    conn.close()
    flash(f"User {name} created", "success")
    return redirect(url_for("admin_users"))


@app.route("/admin/users/delete/<int:user_id>", methods=["POST"])
@login_required
def admin_delete_user(user_id):
    if not current_user.is_admin:
        flash("Access denied", "error")
        return redirect(url_for("index"))

    if user_id == current_user.id:
        flash("You cannot delete yourself", "error")
        return redirect(url_for("admin_users"))

    conn = get_db()
    conn.execute("DELETE FROM users WHERE id = ?", (user_id,))
    conn.commit()
    conn.close()
    flash("User deleted", "success")
    return redirect(url_for("admin_users"))


@app.route("/admin/users/password/<int:user_id>", methods=["POST"])
@login_required
def admin_change_password(user_id):
    if not current_user.is_admin:
        flash("Access denied", "error")
        return redirect(url_for("index"))

    new_password = request.form.get("new_password", "")
    if len(new_password) < 8 or len(new_password) > 128:
        flash("Password must be 8-128 characters", "error")
        return redirect(url_for("admin_users"))

    conn = get_db()
    conn.execute(
        "UPDATE users SET password_hash = ? WHERE id = ?",
        (generate_password_hash(new_password), user_id)
    )
    conn.commit()
    conn.close()
    flash("Password updated", "success")
    return redirect(url_for("admin_users"))


# ============================================================================
# DATABASE
# ============================================================================

_TURSO_URL = os.environ.get("TURSO_DATABASE_URL")
_TURSO_TOKEN = os.environ.get("TURSO_AUTH_TOKEN")


def _turso_url():
    """Convert libsql:// URL to https:// pipeline endpoint."""
    raw = _TURSO_URL
    if raw.startswith("libsql://"):
        host = raw[len("libsql://"):]
    elif raw.startswith("https://"):
        host = raw[len("https://"):]
    else:
        host = raw
    host = host.rstrip("/")
    return f"https://{host}/v2/pipeline"


def _turso_execute(url, token, sql, params=None):
    """Execute a single SQL statement via Turso HTTP API and return (columns, rows)."""
    args = []
    if params:
        for v in params:
            if v is None:
                args.append({"type": "null", "value": None})
            elif isinstance(v, int):
                args.append({"type": "integer", "value": str(v)})
            elif isinstance(v, float):
                args.append({"type": "float", "value": v})
            elif isinstance(v, bytes):
                import base64
                args.append({"type": "blob", "base64": base64.b64encode(v).decode()})
            else:
                args.append({"type": "text", "value": str(v)})

    body = {"requests": [
        {"type": "execute", "stmt": {"sql": sql, "args": args}},
        {"type": "close"},
    ]}
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    resp = requests.post(url, json=body, headers=headers, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    result = data["results"][0]
    if result["type"] == "error":
        raise RuntimeError(result["error"]["message"])

    resp_result = result["response"]["result"]
    cols = [c["name"] for c in resp_result.get("cols", [])]
    raw_rows = resp_result.get("rows", [])
    rows = []
    for raw in raw_rows:
        vals = []
        for cell in raw:
            if cell["type"] == "null":
                vals.append(None)
            elif cell["type"] == "integer":
                vals.append(int(cell["value"]))
            elif cell["type"] == "float":
                vals.append(float(cell["value"]))
            elif cell["type"] == "blob":
                import base64
                vals.append(base64.b64decode(cell["base64"]))
            else:
                vals.append(cell["value"])
        rows.append(vals)
    return cols, rows


class _TursoRow:
    """Row supporting row[0], row['col'], dict(row), and keys()."""
    __slots__ = ('_values', '_col_map', '_col_names')

    def __init__(self, values, col_names, col_map):
        self._values = values
        self._col_names = col_names
        self._col_map = col_map

    def __getitem__(self, key):
        if isinstance(key, (int, slice)):
            return self._values[key]
        return self._values[self._col_map[key]]

    def keys(self):
        return list(self._col_names)


class _TursoCursor:
    """Mimics a sqlite3 cursor over the Turso HTTP API."""

    def __init__(self, url, token):
        self._url = url
        self._token = token
        self._rows = []

    def execute(self, sql, params=None):
        cols, raw_rows = _turso_execute(self._url, self._token, sql, params)
        col_map = {name: i for i, name in enumerate(cols)}
        self._rows = [_TursoRow(r, cols, col_map) for r in raw_rows]
        return self

    def fetchone(self):
        if self._rows:
            return self._rows.pop(0)
        return None

    def fetchall(self):
        rows = self._rows
        self._rows = []
        return rows


class _TursoConn:
    """Wraps the Turso HTTP API to look like a sqlite3 Connection."""

    def __init__(self, url, token):
        self._url = url
        self._token = token

    def execute(self, sql, params=None):
        cursor = _TursoCursor(self._url, self._token)
        cursor.execute(sql, params)
        return cursor

    def cursor(self):
        return _TursoCursor(self._url, self._token)

    def commit(self):
        pass  # each HTTP request auto-commits

    def close(self):
        pass  # no persistent connection to close


def get_db():
    """Get database connection. Uses Turso if configured, otherwise local SQLite."""
    if _TURSO_URL and _TURSO_TOKEN:
        return _TursoConn(_turso_url(), _TURSO_TOKEN)

    conn = sqlite3.connect(CONFIG["DB_PATH"])
    conn.row_factory = sqlite3.Row
    return conn


def init_database():
    """Initialize database with required tables."""
    conn = get_db()
    cursor = conn.cursor()

    try:
        cursor.execute("PRAGMA journal_mode=WAL")
    except Exception:
        pass

    # Users table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            name TEXT NOT NULL,
            is_admin INTEGER NOT NULL DEFAULT 0
        )
    """)

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

    # Alarm Activation tables
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS alarm_jobs_raw (
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

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS alarm_jobs_daily_summary (
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
        CREATE TABLE IF NOT EXISTS alarm_jobs_weekly_summary (
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
        CREATE TABLE IF NOT EXISTS alarm_jobs_monthly_summary (
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
        CREATE TABLE IF NOT EXISTS alarm_flag_values (
            flag_value TEXT PRIMARY KEY,
            flag_category TEXT,
            first_seen DATETIME DEFAULT CURRENT_TIMESTAMP,
            last_seen DATETIME DEFAULT CURRENT_TIMESTAMP,
            job_count INTEGER DEFAULT 0
        )
    """)

    # Alarm indexes
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_alarm_jobs_status_date ON alarm_jobs_raw(status_date)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_alarm_jobs_created ON alarm_jobs_raw(created)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_alarm_jobs_current_flag ON alarm_jobs_raw(current_flag)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_alarm_jobs_flag_category ON alarm_jobs_raw(flag_category)")

    # Patrol Jobs - Real Time tables
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS patrol_jobs_raw (
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
            job_result TEXT,
            raw_json TEXT,
            last_synced DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS patrol_jobs_daily_summary (
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
        CREATE TABLE IF NOT EXISTS patrol_jobs_weekly_summary (
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
        CREATE TABLE IF NOT EXISTS patrol_jobs_monthly_summary (
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
        CREATE TABLE IF NOT EXISTS patrol_flag_values (
            flag_value TEXT PRIMARY KEY,
            flag_category TEXT,
            first_seen DATETIME DEFAULT CURRENT_TIMESTAMP,
            last_seen DATETIME DEFAULT CURRENT_TIMESTAMP,
            job_count INTEGER DEFAULT 0
        )
    """)

    # Patrol indexes
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_patrol_jobs_status_date ON patrol_jobs_raw(status_date)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_patrol_jobs_created ON patrol_jobs_raw(created)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_patrol_jobs_current_flag ON patrol_jobs_raw(current_flag)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_patrol_jobs_flag_category ON patrol_jobs_raw(flag_category)")

    # Migrate: add is_admin column to users if missing
    try:
        cursor.execute("ALTER TABLE users ADD COLUMN is_admin INTEGER NOT NULL DEFAULT 0")
    except sqlite3.OperationalError:
        pass  # Column already exists

    conn.commit()
    conn.close()

    # Run migration to update existing data
    migrate_flag_categories()
    alarm_migrate_flag_categories()
    patrol_migrate_flag_categories()

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


def alarm_migrate_flag_categories():
    """Migrate existing 'Sent' flag_category to 'Sent_AI' or 'Sent_Manual' and add new columns for alarm tables."""
    conn = get_db()
    cursor = conn.cursor()

    # Add new columns to alarm summary tables if they don't exist
    for table in ['alarm_jobs_daily_summary', 'alarm_jobs_weekly_summary', 'alarm_jobs_monthly_summary']:
        try:
            cursor.execute(f"ALTER TABLE {table} ADD COLUMN sent_ai_count INTEGER DEFAULT 0")
        except sqlite3.OperationalError:
            pass  # Column already exists
        try:
            cursor.execute(f"ALTER TABLE {table} ADD COLUMN sent_manual_count INTEGER DEFAULT 0")
        except sqlite3.OperationalError:
            pass  # Column already exists

    # Update Sent_AI for "Alarm report sent by AI"
    cursor.execute("""
        UPDATE alarm_jobs_raw
        SET flag_category = 'Sent_AI'
        WHERE current_flag = 'Alarm report sent by AI' AND flag_category = 'Sent'
    """)
    ai_updated = cursor.rowcount

    # Update Sent_Manual for "Report Sent To Client"
    cursor.execute("""
        UPDATE alarm_jobs_raw
        SET flag_category = 'Sent_Manual'
        WHERE current_flag = 'Report Sent To Client' AND flag_category = 'Sent'
    """)
    manual_updated = cursor.rowcount

    conn.commit()
    conn.close()

    if ai_updated > 0 or manual_updated > 0:
        logger.info(f"Alarm: Migrated flag categories: {ai_updated} to Sent_AI, {manual_updated} to Sent_Manual")
        alarm_refresh_summaries()
    else:
        alarm_refresh_summaries()


def patrol_migrate_flag_categories():
    """Migrate existing 'Sent' flag_category to 'Sent_AI' or 'Sent_Manual' and add new columns for patrol tables."""
    conn = get_db()
    cursor = conn.cursor()

    # Add new columns to patrol summary tables if they don't exist
    for table in ['patrol_jobs_daily_summary', 'patrol_jobs_weekly_summary', 'patrol_jobs_monthly_summary']:
        try:
            cursor.execute(f"ALTER TABLE {table} ADD COLUMN sent_ai_count INTEGER DEFAULT 0")
        except sqlite3.OperationalError:
            pass  # Column already exists
        try:
            cursor.execute(f"ALTER TABLE {table} ADD COLUMN sent_manual_count INTEGER DEFAULT 0")
        except sqlite3.OperationalError:
            pass  # Column already exists

    # Update Sent_AI for "No patrol incident identified by AI" and "Patrol Incident identified by AI"
    cursor.execute("""
        UPDATE patrol_jobs_raw
        SET flag_category = 'Sent_AI'
        WHERE current_flag IN ('No patrol incident identified by AI', 'Patrol Incident identified by AI') AND flag_category = 'Sent'
    """)
    ai_updated = cursor.rowcount

    # Update Sent_Manual for "Report Sent To Client"
    cursor.execute("""
        UPDATE patrol_jobs_raw
        SET flag_category = 'Sent_Manual'
        WHERE current_flag = 'Report Sent To Client' AND flag_category = 'Sent'
    """)
    manual_updated = cursor.rowcount

    # Backfill job_result from raw_json for rows where job_result is NULL
    cursor.execute("""
        UPDATE patrol_jobs_raw
        SET job_result = json_extract(raw_json, '$.JobResult')
        WHERE job_result IS NULL AND raw_json IS NOT NULL AND json_extract(raw_json, '$.JobResult') IS NOT NULL
    """)
    backfilled = cursor.rowcount

    conn.commit()
    conn.close()

    if backfilled > 0:
        logger.info(f"Patrol: Backfilled job_result for {backfilled} rows")

    if ai_updated > 0 or manual_updated > 0:
        logger.info(f"Patrol: Migrated flag categories: {ai_updated} to Sent_AI, {manual_updated} to Sent_Manual")
        patrol_refresh_summaries()
    else:
        patrol_refresh_summaries()


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
    
    def get_jobs(self, start_date: str, end_date: str, page: int = 0, job_type_id: Optional[int] = None) -> List[Dict]:
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
            "JobTypeId": job_type_id if job_type_id is not None else CONFIG["VPI_JOB_TYPE_ID"],
            "DateOptionId": 2,  # 2 = Creation Date
        }
        
        result = self._make_request(params)
        
        if isinstance(result, list):
            return result
        return result.get("Result", [])
    
    def get_all_jobs(self, start_date: str, end_date: str, job_type_id: Optional[int] = None) -> List[Dict]:
        """Fetch all jobs with pagination."""
        all_jobs = []
        page = 0

        while True:
            jobs = self.get_jobs(start_date, end_date, page, job_type_id=job_type_id)
            
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
    """Classify CurrentFlag into category with AI/Manual distinction for Sent (VPI)."""
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


def classify_alarm_flag(current_flag: Optional[str]) -> str:
    """Classify CurrentFlag into category for Alarm Activation jobs."""
    if not current_flag:
        return "Other"

    flag = current_flag.strip()

    # Distinguish between AI and Manual sent
    if flag == "Alarm report sent by AI":
        return "Sent_AI"
    if flag == "Report Sent To Client":
        return "Sent_Manual"
    if flag in CONFIG["ALARM_HOLD_FLAGS"]:
        return "Hold"
    if flag in CONFIG["ALARM_NEW_FLAGS"]:
        return "New"

    return "Other"


def classify_patrol_flag(current_flag: Optional[str]) -> str:
    """Classify CurrentFlag into category for Patrol jobs."""
    if not current_flag:
        return "Other"
    flag = current_flag.strip()
    if flag in ("No patrol incident identified by AI", "Patrol Incident identified by AI"):
        return "Sent_AI"
    if flag == "Report Sent To Client":
        return "Sent_Manual"
    if flag in CONFIG["PATROL_NEW_FLAGS"]:
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


def alarm_upsert_jobs(jobs: List[Dict]) -> Tuple[int, int]:
    """Insert or update alarm jobs."""
    conn = get_db()
    cursor = conn.cursor()
    inserted = 0
    updated = 0

    for job in jobs:
        job_id = job.get("JobId")
        if not job_id:
            continue

        cursor.execute("SELECT 1 FROM alarm_jobs_raw WHERE job_id = ?", (job_id,))
        exists = cursor.fetchone() is not None

        current_flag = job.get("CurrentFlag")
        flag_category = classify_alarm_flag(current_flag)

        cursor.execute("""
            INSERT OR REPLACE INTO alarm_jobs_raw (
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


def alarm_refresh_summaries():
    """Refresh all alarm summary tables."""
    conn = get_db()
    cursor = conn.cursor()

    # Daily - grouped by created date (Creation Date from BigChange)
    cursor.execute("DELETE FROM alarm_jobs_daily_summary")
    cursor.execute("""
        INSERT INTO alarm_jobs_daily_summary (status_date, total_jobs, sent_count, sent_ai_count, sent_manual_count, hold_count, new_count, other_count, completed_count)
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
        FROM alarm_jobs_raw WHERE created IS NOT NULL
        GROUP BY DATE(created)
    """)

    # Weekly - grouped by created date (Creation Date from BigChange)
    cursor.execute("DELETE FROM alarm_jobs_weekly_summary")
    cursor.execute("""
        INSERT INTO alarm_jobs_weekly_summary (week_start, year_week, total_jobs, sent_count, sent_ai_count, sent_manual_count, hold_count, new_count, other_count, completed_count)
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
        FROM alarm_jobs_raw WHERE created IS NOT NULL
        GROUP BY DATE(created, 'weekday 0', '-6 days')
    """)

    # Monthly - grouped by created date (Creation Date from BigChange)
    cursor.execute("DELETE FROM alarm_jobs_monthly_summary")
    cursor.execute("""
        INSERT INTO alarm_jobs_monthly_summary (month_start, year_month, total_jobs, sent_count, sent_ai_count, sent_manual_count, hold_count, new_count, other_count, completed_count)
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
        FROM alarm_jobs_raw WHERE created IS NOT NULL
        GROUP BY DATE(created, 'start of month')
    """)

    # Flag values
    cursor.execute("""
        INSERT OR REPLACE INTO alarm_flag_values (flag_value, flag_category, last_seen, job_count)
        SELECT current_flag, flag_category, MAX(last_synced), COUNT(*)
        FROM alarm_jobs_raw WHERE current_flag IS NOT NULL
        GROUP BY current_flag
    """)

    conn.commit()
    conn.close()


def patrol_upsert_jobs(jobs: List[Dict]) -> Tuple[int, int]:
    """Insert or update patrol jobs."""
    conn = get_db()
    cursor = conn.cursor()
    inserted = 0
    updated = 0

    for job in jobs:
        job_id = job.get("JobId")
        if not job_id:
            continue

        cursor.execute("SELECT 1 FROM patrol_jobs_raw WHERE job_id = ?", (job_id,))
        exists = cursor.fetchone() is not None

        current_flag = job.get("CurrentFlag")
        flag_category = classify_patrol_flag(current_flag)

        cursor.execute("""
            INSERT OR REPLACE INTO patrol_jobs_raw (
                job_id, job_ref, job_type, job_type_id, job_category, job_category_id,
                contact, contact_id, contact_parent_id, postcode, location, resource,
                status, status_id, status_date, status_comment,
                planned_start, planned_end, duration,
                real_start, real_end, real_duration, due_date,
                created, scheduled, current_flag, flag_category,
                description, job_po, actioned, job_result, raw_json, last_synced
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            job.get("JobResult"), json.dumps(job), datetime.utcnow().isoformat()
        ))

        if exists:
            updated += 1
        else:
            inserted += 1

    conn.commit()
    conn.close()
    return inserted, updated


def patrol_refresh_summaries():
    """Refresh all patrol summary tables."""
    conn = get_db()
    cursor = conn.cursor()

    # Daily - grouped by created date (Creation Date from BigChange)
    cursor.execute("DELETE FROM patrol_jobs_daily_summary")
    cursor.execute("""
        INSERT INTO patrol_jobs_daily_summary (status_date, total_jobs, sent_count, sent_ai_count, sent_manual_count, hold_count, new_count, other_count, completed_count)
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
        FROM patrol_jobs_raw WHERE created IS NOT NULL
        GROUP BY DATE(created)
    """)

    # Weekly - grouped by created date (Creation Date from BigChange)
    cursor.execute("DELETE FROM patrol_jobs_weekly_summary")
    cursor.execute("""
        INSERT INTO patrol_jobs_weekly_summary (week_start, year_week, total_jobs, sent_count, sent_ai_count, sent_manual_count, hold_count, new_count, other_count, completed_count)
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
        FROM patrol_jobs_raw WHERE created IS NOT NULL
        GROUP BY DATE(created, 'weekday 0', '-6 days')
    """)

    # Monthly - grouped by created date (Creation Date from BigChange)
    cursor.execute("DELETE FROM patrol_jobs_monthly_summary")
    cursor.execute("""
        INSERT INTO patrol_jobs_monthly_summary (month_start, year_month, total_jobs, sent_count, sent_ai_count, sent_manual_count, hold_count, new_count, other_count, completed_count)
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
        FROM patrol_jobs_raw WHERE created IS NOT NULL
        GROUP BY DATE(created, 'start of month')
    """)

    # Flag values
    cursor.execute("""
        INSERT OR REPLACE INTO patrol_flag_values (flag_value, flag_category, last_seen, job_count)
        SELECT current_flag, flag_category, MAX(last_synced), COUNT(*)
        FROM patrol_jobs_raw WHERE current_flag IS NOT NULL
        GROUP BY current_flag
    """)

    conn.commit()
    conn.close()


# ============================================================================
# API ROUTES
# ============================================================================

@app.route('/')
@login_required
def index():
    """Serve the dashboard."""
    return send_from_directory('.', 'index.html')

@app.route('/frg-logo.png')
def logo():
    """Serve the logo."""
    return send_from_directory('.', 'frg-logo.png')

@app.route('/api/stats')
@login_required
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

    # AI Automation Rate (of processed reports: AI + Manual + Hold)
    hold = by_flag.get('Hold', 0)
    processed = sent_ai + sent_manual + hold
    ai_rate = (sent_ai / processed * 100) if processed > 0 else 0

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
@login_required
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
@login_required
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
@login_required
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
@login_required
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
@login_required
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
@login_required
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
@login_required
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
# ALARM ACTIVATION ROUTES
# ============================================================================

@app.route('/alarm')
@login_required
def alarm_index():
    """Serve the alarm activation dashboard."""
    return send_from_directory('.', 'alarm.html')

@app.route('/api/alarm/stats')
@login_required
def alarm_get_stats():
    """Get overall alarm statistics with optional date filtering."""
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
    cursor.execute(f"SELECT COUNT(*) FROM alarm_jobs_raw {date_filter}", params)
    total = cursor.fetchone()[0]

    # By flag category (raw categories including Sent_AI and Sent_Manual)
    cursor.execute(f"""
        SELECT flag_category, COUNT(*) as cnt
        FROM alarm_jobs_raw {date_filter}
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

    # AI Automation Rate (of processed reports: AI + Manual + Hold)
    hold = by_flag.get('Hold', 0)
    processed = sent_ai + sent_manual + hold
    ai_rate = (sent_ai / processed * 100) if processed > 0 else 0

    # By status
    cursor.execute(f"""
        SELECT status, COUNT(*) as cnt
        FROM alarm_jobs_raw {date_filter}
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

@app.route('/api/alarm/daily')
@login_required
def alarm_get_daily():
    """Get alarm daily summary with optional date filtering."""
    start_date = request.args.get('start')
    end_date = request.args.get('end')

    conn = get_db()
    cursor = conn.cursor()

    query = """
        SELECT status_date, total_jobs, sent_count, sent_ai_count, sent_manual_count,
               hold_count, new_count, other_count, completed_count
        FROM alarm_jobs_daily_summary
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

@app.route('/api/alarm/weekly')
@login_required
def alarm_get_weekly():
    """Get alarm weekly summary with optional date filtering."""
    start_date = request.args.get('start')
    end_date = request.args.get('end')

    conn = get_db()
    cursor = conn.cursor()

    query = """
        SELECT week_start, year_week, total_jobs, sent_count, sent_ai_count, sent_manual_count,
               hold_count, new_count, other_count, completed_count
        FROM alarm_jobs_weekly_summary
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

@app.route('/api/alarm/monthly')
@login_required
def alarm_get_monthly():
    """Get alarm monthly summary with optional date filtering."""
    start_date = request.args.get('start')
    end_date = request.args.get('end')

    conn = get_db()
    cursor = conn.cursor()

    query = """
        SELECT month_start, year_month, total_jobs, sent_count, sent_ai_count, sent_manual_count,
               hold_count, new_count, other_count, completed_count
        FROM alarm_jobs_monthly_summary
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

@app.route('/api/alarm/flags')
@login_required
def alarm_get_flags():
    """Get all alarm flag values."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT flag_value, flag_category, job_count, last_seen
        FROM alarm_flag_values ORDER BY job_count DESC
    """)
    data = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return jsonify(data)

@app.route('/api/alarm/jobs')
@login_required
def alarm_get_jobs():
    """Get recent alarm jobs with optional date and flag filtering."""
    limit = request.args.get('limit', 100, type=int)
    flag = request.args.get('flag', None)
    start_date = request.args.get('start')
    end_date = request.args.get('end')

    conn = get_db()
    cursor = conn.cursor()

    query = """
        SELECT job_id, job_ref, contact, resource, status, current_flag, flag_category, created, status_date
        FROM alarm_jobs_raw
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

@app.route('/api/alarm/sync', methods=['POST'])
@login_required
def alarm_sync_jobs():
    """Trigger a sync of alarm activation jobs from BigChange API."""
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
        jobs = client.get_all_jobs(start_date, end_date, job_type_id=CONFIG["ALARM_JOB_TYPE_ID"])

        inserted, updated = alarm_upsert_jobs(jobs)
        alarm_refresh_summaries()

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
        logger.error(f"Alarm sync failed: {e}")
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE sync_log SET run_end = ?, status = ?, error_message = ? WHERE id = ?",
            (datetime.utcnow().isoformat(), "error", str(e), sync_id)
        )
        conn.commit()
        conn.close()

        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/alarm/config')
@login_required
def alarm_get_config():
    """Get current alarm configuration (safe info only)."""
    return jsonify({
        "alarm_job_type_id": CONFIG["ALARM_JOB_TYPE_ID"],
        "sent_flags": CONFIG["SENT_FLAGS"],
        "hold_flags": CONFIG["HOLD_FLAGS"],
        "new_flags": CONFIG["NEW_FLAGS"],
        "has_credentials": all([CONFIG["USERNAME"], CONFIG["PASSWORD"], CONFIG["COMPANY_KEY"]])
    })

# ============================================================================
# PATROL JOBS ROUTES
# ============================================================================

@app.route('/patrol')
@login_required
def patrol_index():
    """Serve the patrol jobs dashboard."""
    return send_from_directory('.', 'patrol.html')

@app.route('/api/patrol/stats')
@login_required
def patrol_get_stats():
    """Get overall patrol statistics with optional date filtering."""
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
    cursor.execute(f"SELECT COUNT(*) FROM patrol_jobs_raw {date_filter}", params)
    total = cursor.fetchone()[0]

    # By flag category (raw categories including Sent_AI and Sent_Manual)
    cursor.execute(f"""
        SELECT flag_category, COUNT(*) as cnt
        FROM patrol_jobs_raw {date_filter}
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

    # AI Automation Rate (of processed reports: AI + Manual + Hold)
    hold = by_flag.get('Hold', 0)
    processed = sent_ai + sent_manual + hold
    ai_rate = (sent_ai / processed * 100) if processed > 0 else 0

    # By status
    cursor.execute(f"""
        SELECT status, COUNT(*) as cnt
        FROM patrol_jobs_raw {date_filter}
        GROUP BY status ORDER BY cnt DESC
    """, params)
    by_status = {row[0]: row[1] for row in cursor.fetchall()}

    # Do Not Send count
    dns_filter = date_filter.replace("WHERE", "AND") if date_filter else ""
    cursor.execute(f"""
        SELECT COUNT(*) FROM patrol_jobs_raw
        WHERE status_comment = 'DO NOT SEND GOOD PATROL REPORT' {dns_filter}
    """, params)
    do_not_send_count = cursor.fetchone()[0]

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
        "do_not_send_count": do_not_send_count,
        "ai_automation_rate": round(ai_rate, 1),
        "patrol_job_type_id": CONFIG["PATROL_JOB_TYPE_ID"],
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

@app.route('/api/patrol/daily')
@login_required
def patrol_get_daily():
    """Get patrol daily summary with optional date filtering."""
    start_date = request.args.get('start')
    end_date = request.args.get('end')

    conn = get_db()
    cursor = conn.cursor()

    query = """
        SELECT status_date, total_jobs, sent_count, sent_ai_count, sent_manual_count,
               hold_count, new_count, other_count, completed_count
        FROM patrol_jobs_daily_summary
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

@app.route('/api/patrol/weekly')
@login_required
def patrol_get_weekly():
    """Get patrol weekly summary with optional date filtering."""
    start_date = request.args.get('start')
    end_date = request.args.get('end')

    conn = get_db()
    cursor = conn.cursor()

    query = """
        SELECT week_start, year_week, total_jobs, sent_count, sent_ai_count, sent_manual_count,
               hold_count, new_count, other_count, completed_count
        FROM patrol_jobs_weekly_summary
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

@app.route('/api/patrol/monthly')
@login_required
def patrol_get_monthly():
    """Get patrol monthly summary with optional date filtering."""
    start_date = request.args.get('start')
    end_date = request.args.get('end')

    conn = get_db()
    cursor = conn.cursor()

    query = """
        SELECT month_start, year_month, total_jobs, sent_count, sent_ai_count, sent_manual_count,
               hold_count, new_count, other_count, completed_count
        FROM patrol_jobs_monthly_summary
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

@app.route('/api/patrol/flags')
@login_required
def patrol_get_flags():
    """Get all patrol flag values."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT flag_value, flag_category, job_count, last_seen
        FROM patrol_flag_values ORDER BY job_count DESC
    """)
    data = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return jsonify(data)

@app.route('/api/patrol/jobs')
@login_required
def patrol_get_jobs():
    """Get recent patrol jobs with optional date and flag filtering."""
    limit = request.args.get('limit', 100, type=int)
    flag = request.args.get('flag', None)
    result_filter = request.args.get('result', None)
    start_date = request.args.get('start')
    end_date = request.args.get('end')

    conn = get_db()
    cursor = conn.cursor()

    query = """
        SELECT job_id, job_ref, contact, resource, status, current_flag, flag_category, created, status_date, status_comment as job_result
        FROM patrol_jobs_raw
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

    # Job result / status_comment filter
    if result_filter:
        conditions.append("status_comment = ?")
        params.append(result_filter)

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

@app.route('/api/patrol/sync', methods=['POST'])
@login_required
def patrol_sync_jobs():
    """Trigger a sync of patrol jobs from BigChange API."""
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
        jobs = client.get_all_jobs(start_date, end_date, job_type_id=CONFIG["PATROL_JOB_TYPE_ID"])

        if jobs:
            print("PATROL JOB KEYS:", list(jobs[0].keys()))
            print("PATROL JOB SAMPLE:", jobs[0])

        inserted, updated = patrol_upsert_jobs(jobs)
        patrol_refresh_summaries()

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
        logger.error(f"Patrol sync failed: {e}")
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE sync_log SET run_end = ?, status = ?, error_message = ? WHERE id = ?",
            (datetime.utcnow().isoformat(), "error", str(e), sync_id)
        )
        conn.commit()
        conn.close()

        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/patrol/config')
@login_required
def patrol_get_config():
    """Get current patrol configuration (safe info only)."""
    return jsonify({
        "patrol_job_type_id": CONFIG["PATROL_JOB_TYPE_ID"],
        "sent_flags": CONFIG["PATROL_SENT_FLAGS"],
        "hold_flags": CONFIG["PATROL_HOLD_FLAGS"],
        "new_flags": CONFIG["PATROL_NEW_FLAGS"],
        "has_credentials": all([CONFIG["USERNAME"], CONFIG["PASSWORD"], CONFIG["COMPANY_KEY"]])
    })

# ============================================================================
# INITIALIZE DATABASE ON IMPORT (needed for gunicorn)
# ============================================================================

init_database()

# ============================================================================
# MAIN
# ============================================================================

if __name__ == '__main__':
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
    
    port = int(os.environ.get("PORT", 5001))
    app.run(debug=False, host='0.0.0.0', port=port)
