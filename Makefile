up:
	docker compose up -d

down:
	docker compose down

install:
	pip install -r requirements.txt

# -----------------------------
# Streamlit Cloud / SQLite mode
# -----------------------------
run-sqlite:
	DB_BACKEND=sqlite python -m src.main

streamlit:
	DB_BACKEND=sqlite streamlit run app.py

# -----------------------------
# Optional: Docker/Postgres mode
# -----------------------------
tables:
	# Load .env variables for host/port/user/db.
	# Defaults match docker-compose.yml port mapping (15432:5432).
	set -a; [ -f .env ] && . ./.env; set +a; \
	PGPASSWORD="$${DB_PASSWORD:-password}" \
	psql -h "$${DB_HOST:-127.0.0.1}" -p "$${DB_PORT:-15432}" -U "$${DB_USER:-postgres}" -d "$${DB_NAME:-portfolio_db}" -f sql/creates_tables.sql

generate:
	python -m src.ingestion.generate_fake_data

ingest:
	python -m src.ingestion.ingest_transactions

validate:
	python -m src.validation.run_validation

reconcile:
	python -m src.reconciliation.run_reconciliation

transform:
	python -m src.transformations.run_transformations

report:
	python -m src.reporting.run_reporting

run:
	python -m src.main