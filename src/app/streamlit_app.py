"""
Parsely — Streamlit Web Application

The user-facing demo of the document parsing pipeline.
Upload a PDF or text invoice, see extracted fields auto-filled
into a form, review and edit, then view a document summary.

This is the "wow factor" of the project — it makes the data
engineering work visible and interactive.

Run with: streamlit run src/app/streamlit_app.py
"""

import os
import sys
import tempfile

import streamlit as st

# Add project root to path so we can import src modules
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.ingestion.pdf_parser import PDFParser
from src.extraction.field_extractor import FieldExtractor
from src.validation.validators import InvoiceValidator
from src.summarization.summarizer import InvoiceSummarizer
from src.loading.snowflake_loader import SnowflakeLoader
from src.loading.databricks_loader import DatabricksLoader


# ── Page Configuration ────────────────────────────────
st.set_page_config(
    page_title="Parsely - Document Parser",
    page_icon="P",
    layout="wide",
)


# ── Initialize Components (cached so they load once) ──
@st.cache_resource
def get_components():
    return {
        "parser": PDFParser(),
        "extractor": FieldExtractor(use_ner=False),
        "validator": InvoiceValidator(),
        "summarizer": InvoiceSummarizer(),
    }


components = get_components()


# ── App Header ────────────────────────────────────────
st.title("Parsely")
st.markdown("Upload an invoice document and watch the fields get extracted automatically.")
st.divider()


# ── Sidebar ───────────────────────────────────────────
with st.sidebar:
    st.header("About")
    st.markdown(
        """
        **Parsely** is a data engineering pipeline that:
        1. Parses PDF/text documents
        2. Extracts structured fields
        3. Validates data quality
        4. Loads into Snowflake & Databricks
        5. Transforms with dbt

        This app is the frontend demo.
        """
    )
    st.divider()
    st.markdown("**Pipeline Status**")
    st.markdown("Snowflake: Dry Run Mode")
    st.markdown("Databricks: Dry Run Mode")


# ── File Upload Section ──────────────────────────────
st.header("1. Upload Document")

uploaded_file = st.file_uploader(
    "Choose a PDF or text invoice",
    type=["pdf", "txt"],
    help="Upload a PDF or text file containing an invoice",
)

# Also allow selecting a sample file
use_sample = st.checkbox("Or use a sample invoice")
sample_choice = None
if use_sample:
    samples_dir = os.path.join(project_root, "data", "samples")
    sample_files = [f for f in os.listdir(samples_dir) if f.endswith((".pdf", ".txt"))]
    sample_choice = st.selectbox("Select a sample", sample_files)


# ── Processing ────────────────────────────────────────
if uploaded_file is not None or sample_choice is not None:

    # Parse the document
    with st.spinner("Parsing document..."):
        if uploaded_file:
            # Save uploaded file to temp location
            suffix = os.path.splitext(uploaded_file.name)[1]
            with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
                tmp.write(uploaded_file.getvalue())
                tmp_path = tmp.name
            parse_result = components["parser"].parse(tmp_path)
            os.unlink(tmp_path)
        else:
            file_path = os.path.join(project_root, "data", "samples", sample_choice)
            parse_result = components["parser"].parse(file_path)

    # Extract fields
    with st.spinner("Extracting fields..."):
        invoice = components["extractor"].extract(parse_result["text"])

    # Validate
    with st.spinner("Validating data..."):
        validation = components["validator"].validate(invoice)

    st.success(
        f"Document parsed successfully! "
        f"Confidence: {invoice.parse_confidence:.0%} | "
        f"Validation: {validation.status.value.upper()}"
    )

    st.divider()

    # ── Auto-Fill Form ────────────────────────────────
    st.header("2. Extracted Data (Auto-Filled)")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Invoice Details")
        invoice_number = st.text_input("Invoice Number", value=invoice.invoice_number or "")
        invoice_date = st.date_input(
            "Invoice Date",
            value=invoice.invoice_date,
        ) if invoice.invoice_date else st.text_input("Invoice Date", value="Not extracted")
        due_date = st.date_input(
            "Due Date",
            value=invoice.due_date,
        ) if invoice.due_date else st.text_input("Due Date", value="Not extracted")
        po_number = st.text_input("PO Number", value=invoice.po_number or "")

    with col2:
        st.subheader("Vendor Information")
        vendor_name = st.text_input("Vendor Name", value=invoice.vendor.name or "")
        vendor_address = st.text_input("Address", value=invoice.vendor.address or "")

        vcol1, vcol2, vcol3 = st.columns(3)
        with vcol1:
            vendor_city = st.text_input("City", value=invoice.vendor.city or "")
        with vcol2:
            vendor_state = st.text_input("State", value=invoice.vendor.state or "")
        with vcol3:
            vendor_zip = st.text_input("ZIP", value=invoice.vendor.zip or "")

        vendor_phone = st.text_input("Phone", value=invoice.vendor.phone or "")
        vendor_email = st.text_input("Email", value=invoice.vendor.email or "")

    # ── Line Items Table ──────────────────────────────
    st.subheader("Line Items")

    if invoice.line_items:
        line_data = []
        for item in invoice.line_items:
            line_data.append({
                "Description": item.description or "",
                "Quantity": item.quantity or 0,
                "Unit Price": item.unit_price or 0,
                "Amount": item.amount or 0,
            })

        edited_items = st.data_editor(
            line_data,
            num_rows="dynamic",
            use_container_width=True,
        )
    else:
        st.info("No line items extracted from this document.")

    # ── Financial Summary ─────────────────────────────
    st.subheader("Financial Summary")
    fcol1, fcol2, fcol3 = st.columns(3)
    with fcol1:
        st.metric("Subtotal", f"${invoice.subtotal:,.2f}" if invoice.subtotal else "N/A")
    with fcol2:
        tax_label = f"Tax ({invoice.tax_rate*100:.1f}%)" if invoice.tax_rate else "Tax"
        st.metric(tax_label, f"${invoice.tax_amount:,.2f}" if invoice.tax_amount else "N/A")
    with fcol3:
        st.metric("Total", f"${invoice.total_amount:,.2f}" if invoice.total_amount else "N/A")

    st.divider()

    # ── Document Summary ──────────────────────────────
    st.header("3. Document Summary")

    summary = components["summarizer"].summarize(invoice, validation)
    st.code(summary, language=None)

    brief = components["summarizer"].summarize_brief(invoice)
    st.info(f"Brief: {brief}")

    st.divider()

    # ── Validation Details ────────────────────────────
    with st.expander("Validation Details"):
        for rule in validation.rules:
            icon = "+" if rule.passed else "-"
            severity = rule.severity.value.upper()
            st.markdown(
                f"{'Pass' if rule.passed else 'FAIL'} | "
                f"{severity} | {rule.rule_name}: {rule.message}"
            )

    # ── Raw Text Preview ──────────────────────────────
    with st.expander("Raw Extracted Text"):
        st.code(parse_result["text"], language=None)

    # ── Submit Button ─────────────────────────────────
    st.divider()
    if st.button("Submit to Pipeline", type="primary", use_container_width=True):
        with st.spinner("Loading to warehouse (dry-run mode)..."):
            # Dry-run load to show the pipeline works end-to-end
            sf_loader = SnowflakeLoader(dry_run=True)
            db_loader = DatabricksLoader(dry_run=True)

            doc_id = db_loader.load_bronze(parse_result)
            db_loader.load_silver(doc_id, invoice, validation)
            sf_result = sf_loader.load_invoice(invoice, validation)

        st.success(
            f"Pipeline complete (dry-run mode). "
            f"Document ID: {doc_id[:8]}... | "
            f"Status: {sf_result['status']}"
        )
        st.balloons()

else:
    # Show instructions when no file is selected
    st.info("Upload a document above or select a sample invoice to get started.")
