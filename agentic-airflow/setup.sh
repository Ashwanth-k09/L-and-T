#!/bin/bash
echo "🚀 Agentic Airflow - ERROR RECOVERY ENABLED!"

# Install system deps
apt update -qq && apt install -y python3-pip python3-venv python3-full build-essential curl

# Clean virtualenv
rm -rf agentic_env
python3 -m venv agentic_env
source agentic_env/bin/activate

# Fix pip + install deps
curl https://bootstrap.pypa.io/get-pip.py | python
pip install --upgrade pip
pip install pyautogen==0.4.0 openai flaml[automl]

# **REAL GROQ KEY** - Replace with your key or use free tier
export GROQ_API_KEY="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"

# Run self-healing agent
python airflow1_agent_simple.py
