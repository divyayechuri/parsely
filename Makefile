.PHONY: setup run test lint dbt-run dbt-test airflow-up airflow-down clean help

# ── Setup ─────────────────────────────────────────────
setup:  ## Install dependencies and download NLP model
	pip install -r requirements.txt
	python -m spacy download en_core_web_sm

# ── Run ───────────────────────────────────────────────
run:  ## Start the Streamlit web app
	streamlit run src/app/streamlit_app.py

parse:  ## Parse a sample invoice (usage: make parse FILE=data/samples/sample_invoice_01.pdf)
	python -m src.ingestion.pdf_parser --input $(FILE)

# ── Testing ───────────────────────────────────────────
test:  ## Run all unit tests
	pytest tests/ -v

test-cov:  ## Run tests with coverage report
	pytest tests/ -v --cov=src --cov-report=html

# ── Linting ───────────────────────────────────────────
lint:  ## Run code linting
	python -m py_compile src/**/*.py

# ── dbt ───────────────────────────────────────────────
dbt-run:  ## Run all dbt models
	cd dbt && dbt run

dbt-test:  ## Run all dbt tests
	cd dbt && dbt test

dbt-docs:  ## Generate and serve dbt documentation
	cd dbt && dbt docs generate && dbt docs serve

# ── Airflow ───────────────────────────────────────────
airflow-up:  ## Start Airflow services via Docker
	docker-compose up -d

airflow-down:  ## Stop Airflow services
	docker-compose down

# ── Cleanup ───────────────────────────────────────────
clean:  ## Remove Python cache and temp files
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	rm -rf .pytest_cache htmlcov .coverage

# ── Help ──────────────────────────────────────────────
help:  ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'
