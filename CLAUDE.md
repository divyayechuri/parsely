# Parsely — Project Context

## What This Is
Document parsing ETL pipeline. Upload a PDF/text invoice → extract fields → validate → load to Snowflake (Bronze/Silver/Gold) → transform with dbt → display in Streamlit web app.

## Quick Commands
```bash
# Run the app
streamlit run src/app/streamlit_app.py

# Run tests (90 tests)
pytest tests/ -v

# Run dbt (requires .env with Snowflake credentials)
cd dbt && dbt run && dbt test

# Generate sample invoices
python data/generate_sample_invoices.py
```

## Architecture
- **Python** handles: parsing (pdfplumber), extraction (regex), validation, loading to Snowflake
- **Snowflake** stores all 3 layers: Bronze (raw), Silver (cleaned), Gold (dimensional)
- **dbt** transforms Silver → Gold (8 models, 32 tests)
- **Streamlit** is the frontend (upload, auto-fill form, insights, submit)
- **Airflow** DAG is written but not deployed — Streamlit triggers the pipeline on demand

## Key Files
- `src/extraction/regex_extractor.py` — core parsing logic, handles both .txt and .pdf text formats
- `src/loading/snowflake_loader.py` — loads all 3 layers (load_bronze, load_silver, load_invoice)
- `src/app/streamlit_app.py` — entire web app, uses session_state for form management
- `dbt/profiles.yml` — uses ACCOUNTADMIN role (not parsely_pipeline_role)
- `.env` — real Snowflake credentials (NEVER commit, gitignored)
- `project_planning.md` — detailed personal reference doc (gitignored)

## Conventions
- All file writes must use `encoding="utf-8"` (Windows compatibility)
- Regex patterns must handle both formats: `.txt` (no $, double-space columns) and `.pdf` ($signs, single-space columns)
- Tax rate formatting: use `:.2f` not `:.1f` (8.25% not 8.2%)
- Streamlit form values are managed via `st.session_state` with `st.rerun()` on new document load
- Processing results are cached in `st.session_state["_cached_invoice"]` etc. to survive reruns
- Loaders support `dry_run=True` for testing without Snowflake credentials

## Testing
- 90 Python tests in `tests/` — run with `pytest tests/ -v`
- 32 dbt tests — run with `cd dbt && dbt test`
- All tests must pass before committing
- Loader tests use dry-run mode (no cloud credentials needed)

## What NOT To Do
- Never commit `.env` or real credentials
- Never push `project_planning.md` to GitHub (it's gitignored)
- Don't add Databricks — it was removed intentionally (unnecessary for this project's scope)
- Don't add batch upload to Streamlit — it was removed intentionally (doesn't fit the review-then-submit flow)
- Don't use `:.1f` for tax rate display — causes rounding errors (use `:.2f`)
