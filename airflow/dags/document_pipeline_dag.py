"""
Airflow DAG: Parsely Document Processing Pipeline

Orchestrates the end-to-end flow:
    1. Detect new documents in the upload directory
    2. Parse each document (PDF/text extraction)
    3. Extract structured fields (regex + NER)
    4. Validate extracted data against business rules
    5. Load raw data to Databricks (Bronze layer)
    6. Load cleaned data to Databricks (Silver layer)
    7. Load analytics-ready data to Snowflake (Gold layer)
    8. Run dbt transformations
    9. Run dbt tests

If any step fails, downstream steps are skipped and the DAG
is marked as failed. Airflow handles retries and alerting.

Schedule: Runs on-demand (triggered by file upload via Streamlit)
          Can also be scheduled for batch processing (e.g., hourly)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup


# ── Default arguments for all tasks ──────────────────
default_args = {
    "owner": "parsely",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


# ── Task functions ───────────────────────────────────

def detect_new_documents(**context):
    """
    Scan the upload directory for new documents to process.
    Pushes file paths to XCom so downstream tasks can access them.
    """
    import os
    import glob

    upload_dir = os.environ.get("PARSELY_UPLOAD_DIR", "data/samples")
    patterns = ["*.pdf", "*.txt"]

    files = []
    for pattern in patterns:
        files.extend(glob.glob(os.path.join(upload_dir, pattern)))

    if not files:
        raise FileNotFoundError(f"No documents found in {upload_dir}")

    # Push file list to XCom for downstream tasks
    context["ti"].xcom_push(key="document_files", value=files)
    return f"Found {len(files)} documents"


def parse_document(**context):
    """Parse all detected documents and extract raw text."""
    from src.ingestion.pdf_parser import PDFParser

    files = context["ti"].xcom_pull(key="document_files", task_ids="detect_documents")
    parser = PDFParser()

    results = []
    for file_path in files:
        result = parser.parse(file_path)
        results.append(result)

    context["ti"].xcom_push(key="parse_results", value=results)
    return f"Parsed {len(results)} documents"


def extract_fields(**context):
    """Run field extraction (regex + NER) on parsed text."""
    from src.extraction.field_extractor import FieldExtractor

    parse_results = context["ti"].xcom_pull(key="parse_results", task_ids="parse_documents")
    extractor = FieldExtractor(use_ner=False)  # NER disabled for speed; enable if spaCy installed

    invoices = []
    for result in parse_results:
        invoice = extractor.extract(result["text"])
        invoices.append(invoice.model_dump(mode="json"))

    context["ti"].xcom_push(key="invoices", value=invoices)
    return f"Extracted fields from {len(invoices)} documents"


def validate_data(**context):
    """Validate all extracted invoices against business rules."""
    from src.extraction.schemas import InvoiceData
    from src.validation.validators import InvoiceValidator

    invoice_dicts = context["ti"].xcom_pull(key="invoices", task_ids="extract_fields")
    validator = InvoiceValidator()

    validations = []
    for inv_dict in invoice_dicts:
        invoice = InvoiceData(**inv_dict)
        result = validator.validate(invoice)
        validations.append({
            "invoice_number": invoice.invoice_number,
            "status": result.status.value,
            "errors": len(result.errors),
            "warnings": len(result.warnings),
        })

    context["ti"].xcom_push(key="validations", value=validations)
    return f"Validated {len(validations)} invoices"


def load_bronze(**context):
    """Load raw parsed data to Databricks Bronze layer."""
    from src.loading.databricks_loader import DatabricksLoader

    parse_results = context["ti"].xcom_pull(key="parse_results", task_ids="parse_documents")
    loader = DatabricksLoader(dry_run=True)  # Set to False when Databricks is configured

    doc_ids = []
    for result in parse_results:
        doc_id = loader.load_bronze(result)
        doc_ids.append(doc_id)

    context["ti"].xcom_push(key="document_ids", value=doc_ids)
    return f"Loaded {len(doc_ids)} documents to Bronze"


def load_silver(**context):
    """Load cleaned data to Databricks Silver layer."""
    from src.extraction.schemas import InvoiceData
    from src.loading.databricks_loader import DatabricksLoader
    from src.validation.validators import InvoiceValidator

    invoice_dicts = context["ti"].xcom_pull(key="invoices", task_ids="extract_fields")
    doc_ids = context["ti"].xcom_pull(key="document_ids", task_ids="load_to_bronze")
    loader = DatabricksLoader(dry_run=True)
    validator = InvoiceValidator()

    for inv_dict, doc_id in zip(invoice_dicts, doc_ids):
        invoice = InvoiceData(**inv_dict)
        validation = validator.validate(invoice)
        loader.load_silver(doc_id, invoice, validation)

    return f"Loaded {len(invoice_dicts)} invoices to Silver"


def load_gold(**context):
    """Load validated data to Snowflake Gold layer."""
    from src.extraction.schemas import InvoiceData
    from src.loading.snowflake_loader import SnowflakeLoader
    from src.validation.validators import InvoiceValidator

    invoice_dicts = context["ti"].xcom_pull(key="invoices", task_ids="extract_fields")
    loader = SnowflakeLoader(dry_run=True)  # Set to False when Snowflake is configured
    validator = InvoiceValidator()

    loaded = 0
    for inv_dict in invoice_dicts:
        invoice = InvoiceData(**inv_dict)
        validation = validator.validate(invoice)
        result = loader.load_invoice(invoice, validation)
        if result["status"] == "loaded":
            loaded += 1

    return f"Loaded {loaded} invoices to Gold"


# ── DAG Definition ───────────────────────────────────

with DAG(
    dag_id="parsely_document_pipeline",
    default_args=default_args,
    description="Parse documents, extract fields, validate, and load to warehouse",
    schedule_interval=None,  # Triggered manually or by file upload
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["parsely", "document-processing", "etl"],
) as dag:

    # ── Step 1: Detect new documents ─────────────────
    detect = PythonOperator(
        task_id="detect_documents",
        python_callable=detect_new_documents,
    )

    # ── Step 2: Parse documents ──────────────────────
    parse = PythonOperator(
        task_id="parse_documents",
        python_callable=parse_document,
    )

    # ── Step 3: Extract structured fields ────────────
    extract = PythonOperator(
        task_id="extract_fields",
        python_callable=extract_fields,
    )

    # ── Step 4: Validate extracted data ──────────────
    validate = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data,
    )

    # ── Step 5-7: Load to warehouse layers ───────────
    with TaskGroup("load_to_warehouse") as load_group:
        bronze = PythonOperator(
            task_id="load_to_bronze",
            python_callable=load_bronze,
        )
        silver = PythonOperator(
            task_id="load_to_silver",
            python_callable=load_silver,
        )
        gold = PythonOperator(
            task_id="load_to_gold",
            python_callable=load_gold,
        )

        bronze >> silver >> gold

    # ── Step 8: Run dbt transformations ──────────────
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/parsely/dbt && dbt run --profiles-dir .",
    )

    # ── Step 9: Run dbt tests ────────────────────────
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/parsely/dbt && dbt test --profiles-dir .",
    )

    # ── DAG dependency chain ─────────────────────────
    # detect → parse → extract → validate → load → dbt run → dbt test
    detect >> parse >> extract >> validate >> load_group >> dbt_run >> dbt_test
