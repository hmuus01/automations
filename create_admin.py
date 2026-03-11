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


def create_user(email, password, name):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            name TEXT NOT NULL
        )
    """)

    cursor.execute("SELECT id FROM users WHERE email = ?", (email,))
    if cursor.fetchone():
        print(f"Error: user with email '{email}' already exists.")
        conn.close()
        sys.exit(1)

    password_hash = generate_password_hash(password)
    cursor.execute(
        "INSERT INTO users (email, password_hash, name) VALUES (?, ?, ?)",
        (email, password_hash, name),
    )
    conn.commit()
    print(f"Admin user created: {name} <{email}>")
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create an admin user")
    parser.add_argument("--email", required=True, help="User email address")
    parser.add_argument("--password", required=True, help="User password")
    parser.add_argument("--name", required=True, help="Display name")
    args = parser.parse_args()

    create_user(args.email.strip().lower(), args.password, args.name.strip())
