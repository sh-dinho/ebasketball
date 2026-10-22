#!/usr/bin/env bash
set -euo pipefail

echo "Setting up eBasketball project structure..."
echo "Project root: $(pwd)"

# Directories
directories=(
    "src/ebasketball/sources"
    "src/ebasketball/ingestion"
    "src/ebasketball/repositories"
    "sql/migrations"
    "scripts"
    "dashboard"
    "tests"
    "debug_output"
)

for directory in "${directories[@]}"; do
    mkdir -p "$directory"
    echo "[DIR] $directory"
done

# Create files only when they do not already exist.
create_if_missing() {
    local file="$1"
    local content="$2"

    if [[ -e "$file" ]]; then
        echo "[KEEP] $file"
        return
    fi

    mkdir -p "$(dirname "$file")"
    printf '%s\n' "$content" > "$file"
    echo "[NEW] $file"
}

# Python package
create_if_missing "src/ebasketball/__init__.py" \
    '"""eBasketball data platform."""'
create_if_missing "src/ebasketball/config.py" \
    '"""Centralized application configuration."""'
create_if_missing "src/ebasketball/database.py" \
    '"""Database connections and transactions."""'
create_if_missing "src/ebasketball/analytics.py" \
    '"""eBasketball analytics."""'

# Source adapters
create_if_missing "src/ebasketball/sources/__init__.py" \
    '"""External data sources."""'
create_if_missing "src/ebasketball/sources/h2hgg.py" \
    '"""Primary H2HGG schedule adapter."""'
create_if_missing "src/ebasketball/sources/aceodds.py" \
    '"""Backup AceOdds schedule adapter."""'

# Ingestion pipeline
create_if_missing "src/ebasketball/ingestion/__init__.py" \
    '"""Ingestion pipeline components."""'
create_if_missing "src/ebasketball/ingestion/normalize.py" \
    '"""Normalize source records."""'
create_if_missing "src/ebasketball/ingestion/reconcile.py" \
    '"""Reconcile records across sources."""'
create_if_missing "src/ebasketball/ingestion/pipeline.py" \
    '"""Coordinate source ingestion."""'

# Database repositories
create_if_missing "src/ebasketball/repositories/__init__.py" \
    '"""Database repositories."""'
create_if_missing "src/ebasketball/repositories/matches.py" \
    '"""Match persistence."""'
create_if_missing "src/ebasketball/repositories/odds.py" \
    '"""Odds history persistence."""'
create_if_missing "src/ebasketball/repositories/players.py" \
    '"""Player persistence."""'

# SQL files
create_if_missing "sql/migrations/001_initial_schema.sql" \
    '-- Initial database schema migration.'
create_if_missing "sql/migrations/002_source_provenance.sql" \
    '-- Source provenance migration.'
create_if_missing "sql/schema.sql" \
    '-- Canonical database schema; existing files are preserved.'

# Scripts
create_if_missing "scripts/run_ingestion.py" \
    '"""Run the ingestion pipeline."""'
create_if_missing "scripts/seed_synthetic.py" \
    '"""Seed development-only synthetic data."""'

# Dashboard
create_if_missing "dashboard/app.py" \
    '"""Streamlit dashboard."""'

# Tests
create_if_missing "tests/test_analytics.py" \
    '"""Analytics tests."""'
create_if_missing "tests/test_normalize.py" \
    '"""Normalization tests."""'
create_if_missing "tests/test_reconcile.py" \
    '"""Reconciliation tests."""'
create_if_missing "tests/test_sources.py" \
    '"""Source adapter tests."""'
create_if_missing "tests/test_database.py" \
    '"""Database integration tests."""'

# Configuration templates
create_if_missing ".env.example" \
    '# Copy to .env and configure locally.
DB_HOST=localhost
DB_PORT=5433
DB_NAME=ebasket_db
DB_USER=postgres
DB_PASSWORD=change_me'

create_if_missing ".gitignore" \
    '# Secrets
.env
.env.*
!.env.example

# Python
.venv/
__pycache__/
*.py[cod]
.pytest_cache/
.coverage
htmlcov/

# Generated files
debug_output/
*.log

# Editors
.vscode/
.idea/
.DS_Store'

create_if_missing "README.md" \
    '# eBasketball Data Platform

Primary source: H2HGG.
Backup source: AceOdds.'

echo
echo "Project structure setup complete."
echo "Existing files were preserved."
echo "No database migrations were run."
