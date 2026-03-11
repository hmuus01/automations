#!/usr/bin/env python3
"""Create an admin user from the command line.

Usage:
    python create_admin.py --email admin@example.com --password secret --name "Admin User"
"""

import argparse
import sqlite3
import os
import sys
from werkzeug.security import generate_password_hash

DB_PATH = os.environ.get("DB_PATH", "vpi_jobs.db")


def get_connection():
    turso_url = os.environ.get("TURSO_DATABASE_URL")
    turso_token = os.environ.get("TURSO_AUTH_TOKEN")

    if turso_url and turso_token:
        import libsql_experimental as libsql
        conn = libsql.connect(
            "local_replica.db",
            sync_url=turso_url,
            auth_token=turso_token,
        )
        conn.sync()
        return conn

    return sqlite3.connect(DB_PATH)


def create_user(email, password, name):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            name TEXT NOT NULL,
            is_admin INTEGER NOT NULL DEFAULT 0
        )
    """)

    try:
        cursor.execute("ALTER TABLE users ADD COLUMN is_admin INTEGER NOT NULL DEFAULT 0")
    except Exception:
        pass

    cursor.execute("SELECT id FROM users WHERE email = ?", (email,))
    if cursor.fetchone():
        print(f"Error: user with email '{email}' already exists.")
        conn.close()
        sys.exit(1)

    password_hash = generate_password_hash(password)
    cursor.execute(
        "INSERT INTO users (email, password_hash, name, is_admin) VALUES (?, ?, ?, 1)",
        (email, password_hash, name),
    )
    conn.commit()
    if hasattr(conn, 'sync'):
        conn.sync()
    print(f"Admin user created: {name} <{email}>")
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create an admin user")
    parser.add_argument("--email", required=True, help="User email address")
    parser.add_argument("--password", required=True, help="User password")
    parser.add_argument("--name", required=True, help="Display name")
    args = parser.parse_args()

    create_user(args.email.strip().lower(), args.password, args.name.strip())
