Invoice parsing pipeline. Python → Snowflake (Bronze/Silver/Gold) → dbt → Streamlit.

Run: `streamlit run src/app/Upload_Invoice.py` | Tests: `pytest tests/ -v` (104 tests)

Rules: UTF-8 encoding on all writes. Regex handles both .txt and .pdf formats. Tax rate `:.2f` not `:.1f`. No spaCy, no Databricks, no batch upload. Never commit `.env`.
