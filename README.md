# Parsely

An end-to-end data engineering pipeline that parses business documents (PDF/Word), extracts structured data into a cloud data warehouse, and auto-fills a web form with a document summary.

---

## What It Does

1. **Upload** a PDF or text invoice via a Streamlit web app
2. **Extract** structured fields automatically — vendor, dates, line items, totals
3. **Validate** data quality against 10+ business rules
4. **Load** through a medallion architecture (Bronze → Silver → Gold)
5. **Transform** with dbt into a star schema (dimensions + facts)
6. **View** auto-filled form + document summary in the browser

## Architecture

```
  Upload (Streamlit)
       │
       ▼
  Parse & Extract (Python, pdfplumber, regex, spaCy)
       │
       ▼
  Validate (10 business rules, confidence scoring)
       │
       ▼
  Bronze → Silver → Gold (Snowflake)
       │                     │
       ▼                     ▼
  dbt Transform (8 models)   Streamlit Web App
  staging → intermediate → marts  (Auto-fill Form + Summary)
       │
       ▼
  Orchestrated by Apache Airflow (9-step DAG)
```

## Tech Stack

| Layer | Tools |
|-------|-------|
| Parsing | Python, pdfplumber, python-docx |
| Extraction | spaCy NER, regex, Pydantic schemas |
| Warehouse | Snowflake (Free Trial) — Bronze, Silver, and Gold layers |
| Transformations | dbt (8 SQL models + custom tests) |
| Orchestration | Apache Airflow |
| Data Quality | Custom validation framework, dbt tests |
| Frontend | Streamlit |
| CI/CD | GitHub Actions (lint + test on 3 Python versions) |
| Containerization | Docker, Docker Compose |

## Quick Start

```bash
# Clone and install
git clone https://github.com/<your-username>/parsely.git
cd parsely
pip install -r requirements.txt

# Run the web app
streamlit run src/app/streamlit_app.py

# Or use Docker
docker-compose up -d
# App: http://localhost:8501 | Airflow: http://localhost:8080
```

## Run Tests

```bash
pytest tests/ -v            # 78 tests
pytest tests/ -v --cov=src  # With coverage
```

## Project Structure

```
parsely/
├── src/
│   ├── ingestion/       # PDF parsing (pdfplumber)
│   ├── extraction/      # Field extraction (regex + spaCy NER)
│   ├── validation/      # Data quality rules
│   ├── loading/         # Snowflake loader (Bronze, Silver, Gold)
│   ├── summarization/   # Document summary generation
│   └── app/             # Streamlit web application
├── dbt/models/
│   ├── staging/         # Clean interface over Silver layer
│   ├── intermediate/    # Business logic & vendor deduplication
│   └── marts/           # Star schema (dim + fact tables)
├── airflow/dags/        # Pipeline orchestration DAG
├── snowflake/ddl/       # Database setup scripts
├── tests/               # 78 unit & integration tests
└── data/samples/        # Sample invoices for testing
```

## Roadmap

**V1 (current)** — PDF invoice parsing, field extraction, Snowflake integration (Bronze/Silver/Gold medallion architecture), dbt models, Streamlit auto-fill form, Airflow orchestration, CI/CD, Docker

**V2** — LLM-powered summarization (Claude API), DOCX & OCR support, multiple document types, data lineage visualization, Terraform IaC

## License

MIT License. See [LICENSE](LICENSE) for details.
