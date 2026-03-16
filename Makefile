up:
	docker compose up -d

down:
	docker compose down

install:
	pip install -r requirements.txt

tables:
	psql -U postgres -d portfolio_db -f sql/create_tables.sql

generate:
	python src/ingestion/generate_fake_data.py

ingest:
	python src/ingestion/ingest_transactions.py

validate:
	python src/validation/run_validation.py

reconcile:
	python src/reconciliation/run_reconciliation.py

transform:
	python src/transformations/run_transformations.py

report:
	python src/reporting/run_reporting.py

run:
	python src/main.py