#!/bin/bash
# VPI Jobs Tracker - Start Script
# ================================

echo "╔════════════════════════════════════════════════════════════╗"
echo "║           VPI Jobs Tracker Dashboard                       ║"
echo "╚════════════════════════════════════════════════════════════╝"
echo ""

# Check for Python
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 not found. Please install Python 3.8+"
    exit 1
fi

# Check environment variables
if [ -z "$BIGCHANGE_USERNAME" ] || [ -z "$BIGCHANGE_PASSWORD" ] || [ -z "$BIGCHANGE_KEY" ]; then
    echo "⚠️  BigChange credentials not set!"
    echo ""
    echo "Please set these environment variables:"
    echo "  export BIGCHANGE_USERNAME='your_username'"
    echo "  export BIGCHANGE_PASSWORD='your_password'"
    echo "  export BIGCHANGE_KEY='your_company_key'"
    echo ""
    echo "Starting anyway (sync will be disabled)..."
    echo ""
fi

# Install dependencies if needed
echo "📦 Checking dependencies..."
pip3 install flask flask-cors requests -q 2>/dev/null

echo ""
echo "🚀 Starting server..."
echo "   Open http://localhost:5000 in your browser"
echo ""
echo "   Press Ctrl+C to stop"
echo ""

python3 app.py
