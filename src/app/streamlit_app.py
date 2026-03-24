"""
Parsely — Streamlit Web Application

Upload an invoice, review auto-filled form, edit if needed, submit to Snowflake.

Run with: streamlit run src/app/streamlit_app.py
"""

import os
import sys
import tempfile
import time

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")

import streamlit as st

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from copy import deepcopy

from src.ingestion.pdf_parser import PDFParser
from src.extraction.field_extractor import FieldExtractor
from src.validation.validators import InvoiceValidator
from src.loading.snowflake_loader import SnowflakeLoader


# ── Page Config ───────────────────────────────────────
st.set_page_config(page_title="Parsely", page_icon="P", layout="wide")

# ── Styling ───────────────────────────────────────────
st.markdown("""
<style>
    .hero {
        background: linear-gradient(135deg, #0f172a 0%, #1e293b 40%, #0f3460 100%);
        padding: 2.2rem 2.8rem; border-radius: 14px; margin-bottom: 1.5rem;
        color: white; position: relative; overflow: hidden;
    }
    .hero::before {
        content: ''; position: absolute; top: 0; left: 0; right: 0; bottom: 0;
        background:
            linear-gradient(60deg, rgba(100,255,218,0.04) 25%, transparent 25%),
            linear-gradient(-60deg, rgba(100,255,218,0.04) 25%, transparent 25%),
            linear-gradient(60deg, transparent 75%, rgba(100,255,218,0.04) 75%),
            linear-gradient(-60deg, transparent 75%, rgba(100,255,218,0.04) 75%);
        background-size: 60px 104px;
        background-position: 0 0, 0 0, 30px 52px, 30px 52px;
        pointer-events: none;
    }
    .hero::after {
        content: ''; position: absolute; top: -50%; right: -10%;
        width: 300px; height: 300px;
        background: radial-gradient(circle, rgba(100,255,218,0.08) 0%, transparent 70%);
        pointer-events: none;
    }
    .hero-content { position: relative; z-index: 1; }
    .hero-logo { display: flex; align-items: center; gap: 14px; margin-bottom: 1rem; }
    .hero-logo-icon {
        width: 50px; height: 50px;
        background: linear-gradient(135deg, #64ffda, #0ea5e9);
        border-radius: 12px; display: flex; align-items: center; justify-content: center;
        flex-shrink: 0; box-shadow: 0 4px 18px rgba(100,255,218,0.3);
    }
    .hero-logo h1 {
        color: white; font-size: 2.1rem; margin: 0; line-height: 1;
        font-family: 'Georgia', serif; font-weight: 700; letter-spacing: 0.5px;
    }
    .hero p {
        color: #94a3b8; font-size: 1rem; margin-bottom: 0; white-space: nowrap;
        font-family: 'Segoe UI', sans-serif; font-weight: 300; font-style: italic;
    }
    .section-label {
        color: #0f3460; font-size: 1.1rem; font-weight: 700;
        letter-spacing: 0.5px; margin-bottom: 0.5rem;
        padding-bottom: 0.3rem; border-bottom: 2px solid #e2e8f0;
    }
    [data-testid="stFileUploader"] label p {
        font-size: 1.1rem !important; font-weight: 600 !important;
    }
    .submitted-banner {
        background: #e6f4ea; border-left: 4px solid #1e7e34;
        padding: 1rem 1.5rem; border-radius: 0 8px 8px 0; margin: 1rem 0;
    }
    .changes-banner {
        background: #fffbeb; border-left: 3px solid #f59e0b;
        padding: 0.8rem 1.2rem; border-radius: 0 8px 8px 0;
        margin: 0.5rem 0; line-height: 1.6; color: #78350f; font-size: 0.9rem;
    }

    /* Submit button — match page theme */
    div.stButton > button[kind="primary"] {
        background: linear-gradient(135deg, #0f3460, #1e293b) !important;
        border: none !important;
        color: white !important;
        font-weight: 600 !important;
        letter-spacing: 0.5px !important;
        padding: 0.6rem 1.5rem !important;
        transition: opacity 0.2s !important;
    }
    div.stButton > button[kind="primary"]:hover {
        opacity: 0.9 !important;
    }
    div.stButton > button[kind="primary"]:disabled {
        background: #94a3b8 !important;
        opacity: 0.5 !important;
    }

    /* Document Summary — stand out */
    .summary-card {
        background: linear-gradient(135deg, #f0f9ff, #e0f2fe);
        border: 1px solid #bae6fd;
        border-radius: 12px;
        padding: 1.5rem;
        margin: 0.5rem 0 1rem 0;
    }
    .summary-card-title {
        color: #0369a1; font-size: 1.1rem; font-weight: 700;
        letter-spacing: 0.5px; margin-bottom: 0.8rem;
    }

    /* Recent Submissions label */
    .submissions-label {
        color: #0f3460; font-size: 1.1rem; font-weight: 700;
        letter-spacing: 0.5px; margin-bottom: 0.3rem;
    }
</style>
""", unsafe_allow_html=True)


# ── Components ────────────────────────────────────────
@st.cache_resource
def get_components():
    return {
        "parser": PDFParser(),
        "extractor": FieldExtractor(),
        "validator": InvoiceValidator(),
    }

components = get_components()


# ── Hero Banner ───────────────────────────────────────
st.markdown("""
<div class="hero"><div class="hero-content">
    <div class="hero-logo">
        <div class="hero-logo-icon">
            <svg width="30" height="30" viewBox="0 0 30 30" fill="none">
                <rect x="3" y="2" width="14" height="18" rx="2" stroke="#0f172a" stroke-width="1.8" fill="rgba(15,23,42,0.1)"/>
                <path d="M12 2V7H17" stroke="#0f172a" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"/>
                <line x1="6" y1="10" x2="14" y2="10" stroke="#0f172a" stroke-width="1.5" stroke-linecap="round"/>
                <line x1="6" y1="13" x2="12" y2="13" stroke="#0f172a" stroke-width="1.5" stroke-linecap="round"/>
                <line x1="6" y1="16" x2="10" y2="16" stroke="#0f172a" stroke-width="1.5" stroke-linecap="round"/>
                <path d="M17 14L21 14" stroke="#0f172a" stroke-width="1.5" stroke-linecap="round"/>
                <path d="M19.5 12L22 14L19.5 16" stroke="#0f172a" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/>
                <ellipse cx="25.5" cy="21" rx="3.5" ry="1.8" stroke="#0f172a" stroke-width="1.5" fill="rgba(15,23,42,0.1)"/>
                <path d="M22 21V26C22 27 23.5 28 25.5 28C27.5 28 29 27 29 26V21" stroke="#0f172a" stroke-width="1.5"/>
                <path d="M22 23.5C22 24.5 23.5 25.3 25.5 25.3C27.5 25.3 29 24.5 29 23.5" stroke="#0f172a" stroke-width="1" opacity="0.5"/>
            </svg>
        </div>
        <h1>Parsely</h1>
    </div>
    <p>Turn invoices into insights &mdash; parse, validate, and load automatically.</p>
</div></div>
""", unsafe_allow_html=True)


# ── Upload ────────────────────────────────────────────
upload_col, sample_col = st.columns([3, 2])
with upload_col:
    uploaded_file = st.file_uploader("Upload an invoice (PDF or text)", type=["pdf", "txt"])
with sample_col:
    st.markdown("<br>", unsafe_allow_html=True)
    use_sample = st.checkbox("Use a sample invoice instead")
    sample_choice = None
    if use_sample:
        samples_dir = os.path.join(project_root, "data", "samples")
        sample_files = sorted([f for f in os.listdir(samples_dir) if f.endswith((".pdf", ".txt"))])
        sample_choice = st.selectbox("Select a sample", sample_files, label_visibility="collapsed")

has_document = uploaded_file is not None or sample_choice is not None


# ── Process document ──────────────────────────────────
invoice = None
validation = None
parse_result = None

if has_document:
    if uploaded_file:
        current_doc_id = f"upload_{uploaded_file.name}_{uploaded_file.size}"
    else:
        current_doc_id = f"sample_{sample_choice}"

    # Check if we need to process (new document) or reuse cached results
    # Also re-process if cached invoice is missing data (stale cache from code changes)
    cached_invoice = st.session_state.get("_cached_invoice")
    cache_is_stale = cached_invoice is not None and not cached_invoice.line_items and not cached_invoice.subtotal
    is_new_doc = current_doc_id != st.session_state.get("_loaded_doc_id") or cache_is_stale

    if is_new_doc:
        # New document — parse, extract, validate
        progress = st.progress(0, text="Reading document...")
        if uploaded_file:
            suffix = os.path.splitext(uploaded_file.name)[1]
            with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
                tmp.write(uploaded_file.getvalue())
                tmp_path = tmp.name
            parse_result = components["parser"].parse(tmp_path)
            os.unlink(tmp_path)
        else:
            file_path = os.path.join(project_root, "data", "samples", sample_choice)
            parse_result = components["parser"].parse(file_path)

        progress.progress(50, text="Extracting fields...")
        invoice = components["extractor"].extract(parse_result["text"])
        progress.progress(80, text="Validating...")
        validation = components["validator"].validate(invoice)
        progress.progress(100, text="Done")
        time.sleep(0.3)
        progress.empty()

        # Cache results in session_state so they survive rerun
        st.session_state["_loaded_doc_id"] = current_doc_id
        st.session_state["_cached_parse_result"] = parse_result
        st.session_state["_cached_invoice"] = invoice
        st.session_state["_cached_validation"] = validation

        # Seed form values
        st.session_state["form_invoice_number"] = invoice.invoice_number or ""
        st.session_state["form_po_number"] = invoice.po_number or ""
        st.session_state["form_vendor_name"] = invoice.vendor.name or ""
        st.session_state["form_vendor_address"] = invoice.vendor.address or ""
        st.session_state["form_vendor_city"] = invoice.vendor.city or ""
        st.session_state["form_vendor_state"] = invoice.vendor.state or ""
        st.session_state["form_vendor_zip"] = invoice.vendor.zip or ""
        st.session_state["form_vendor_phone"] = invoice.vendor.phone or ""
        st.session_state["form_vendor_email"] = invoice.vendor.email or ""
        if invoice.invoice_date:
            st.session_state["form_invoice_date"] = invoice.invoice_date
        if invoice.due_date:
            st.session_state["form_due_date"] = invoice.due_date

        # Originals for comparison
        st.session_state["_orig"] = {
            "Invoice Number": invoice.invoice_number or "",
            "PO Number": invoice.po_number or "",
            "Vendor Name": invoice.vendor.name or "",
            "Address": invoice.vendor.address or "",
            "City": invoice.vendor.city or "",
            "State": invoice.vendor.state or "",
            "ZIP": invoice.vendor.zip or "",
            "Phone": invoice.vendor.phone or "",
            "Email": invoice.vendor.email or "",
        }
        st.rerun()
    else:
        # Same document — reuse cached results
        parse_result = st.session_state.get("_cached_parse_result")
        invoice = st.session_state.get("_cached_invoice")
        validation = st.session_state.get("_cached_validation")

st.divider()

# ── Invoice Details ───────────────────────────────────
st.markdown('<div class="section-label">Invoice Details</div>', unsafe_allow_html=True)

inv_c1, inv_c2, inv_c3, inv_c4 = st.columns(4)
with inv_c1:
    form_invoice_number = st.text_input("Invoice Number", placeholder="e.g. INV-2026-001",
                                         disabled=not has_document, key="form_invoice_number")
with inv_c2:
    form_po_number = st.text_input("PO Number", placeholder="e.g. PO-2026-0455",
                                    disabled=not has_document, key="form_po_number")
with inv_c3:
    if has_document and "form_invoice_date" in st.session_state:
        form_invoice_date = st.date_input("Invoice Date", key="form_invoice_date")
    else:
        st.text_input("Invoice Date", value="", placeholder="Extracted from document", disabled=True)
        form_invoice_date = None
with inv_c4:
    if has_document and "form_due_date" in st.session_state:
        form_due_date = st.date_input("Due Date", key="form_due_date")
    else:
        st.text_input("Due Date", value="", placeholder="Extracted from document", disabled=True)
        form_due_date = None

# ── Vendor Information ────────────────────────────────
st.markdown('<div class="section-label">Vendor Information</div>', unsafe_allow_html=True)

ven_c1, ven_c2 = st.columns(2)
with ven_c1:
    form_vendor_name = st.text_input("Vendor Name", placeholder="e.g. Acme Corporation",
                                      disabled=not has_document, key="form_vendor_name")
with ven_c2:
    form_vendor_address = st.text_input("Address", placeholder="Street address",
                                         disabled=not has_document, key="form_vendor_address")

addr_c1, addr_c2, addr_c3, addr_c4, addr_c5 = st.columns([2, 1, 1, 2, 2])
with addr_c1:
    form_vendor_city = st.text_input("City", placeholder="City", disabled=not has_document, key="form_vendor_city")
with addr_c2:
    form_vendor_state = st.text_input("State", placeholder="ST", disabled=not has_document, key="form_vendor_state")
with addr_c3:
    form_vendor_zip = st.text_input("ZIP", placeholder="00000", disabled=not has_document, key="form_vendor_zip")
with addr_c4:
    form_vendor_phone = st.text_input("Phone", placeholder="(000) 000-0000", disabled=not has_document, key="form_vendor_phone")
with addr_c5:
    form_vendor_email = st.text_input("Email", placeholder="vendor@example.com", disabled=not has_document, key="form_vendor_email")

# ── Financial Summary ─────────────────────────────────
st.markdown('<div class="section-label">Financial Summary</div>', unsafe_allow_html=True)

fin_c1, fin_c2, fin_c3 = st.columns(3)
with fin_c1:
    st.metric("Subtotal", f"${invoice.subtotal:,.2f}" if invoice and invoice.subtotal is not None else "--")
with fin_c2:
    tax_label = f"Tax ({invoice.tax_rate*100:.2f}%)" if invoice and invoice.tax_rate else "Tax"
    st.metric(tax_label, f"${invoice.tax_amount:,.2f}" if invoice and invoice.tax_amount is not None else "--")
with fin_c3:
    st.metric("Total Due", f"${invoice.total_amount:,.2f}" if invoice and invoice.total_amount is not None else "--")

# ── Line Items ────────────────────────────────────────
st.divider()
st.markdown('<div class="section-label">Line Items</div>', unsafe_allow_html=True)

if invoice and invoice.line_items:
    line_data = [{
        "Description": item.description or "",
        "Qty": item.quantity or 0,
        "Unit Price ($)": item.unit_price or 0,
        "Amount ($)": item.amount or 0,
    } for item in invoice.line_items]

    st.data_editor(
        line_data, num_rows="dynamic", use_container_width=True,
        column_config={
            "Unit Price ($)": st.column_config.NumberColumn(format="%.2f"),
            "Amount ($)": st.column_config.NumberColumn(format="%.2f"),
        }
    )
else:
    st.dataframe(
        [{"Description": "", "Qty": "", "Unit Price ($)": "", "Amount ($)": ""}],
        use_container_width=True,
    )
    if not has_document:
        st.caption("Line items will appear here after uploading a document.")

# ── Document Summary (Insights) ──────────────────────
st.divider()

if invoice and invoice.line_items:
    amounts = [item.amount for item in invoice.line_items if item.amount]
    total_str = f"${invoice.total_amount:,.2f}" if invoice.total_amount else "an unspecified amount"
    insights = []

    if amounts:
        max_item = max(invoice.line_items, key=lambda x: x.amount or 0)
        min_item = min(invoice.line_items, key=lambda x: x.amount or 0)
        avg_amount = sum(amounts) / len(amounts)
        max_pct = (max_item.amount / invoice.total_amount * 100) if invoice.total_amount else 0

        if max_pct > 50:
            insights.append(
                f"<strong>Top spend:</strong> {max_item.description} drives {max_pct:.0f}% of this invoice "
                f"(${max_item.amount:,.2f} of {total_str})."
            )
        high_qty = [i for i in invoice.line_items if i.quantity and i.quantity >= 50]
        if high_qty:
            bulk_desc = ", ".join(f"{i.quantity:g}x {i.description}" for i in high_qty)
            insights.append(f"<strong>Bulk orders:</strong> {bulk_desc}.")
        if len(amounts) > 1 and max_item.amount != min_item.amount:
            insights.append(
                f"<strong>Line items</strong> range from ${min_item.amount:,.2f} to ${max_item.amount:,.2f} "
                f"(avg ${avg_amount:,.2f})."
            )
        if max_pct <= 50:
            insights.append(f"<strong>Largest item:</strong> {max_item.description} at ${max_item.amount:,.2f}.")

    if insights:
        st.markdown(f"""
        <div class="summary-card">
            <div class="summary-card-title">Document Summary</div>
            <div style="line-height: 1.8; color: #334155;">
                {'<br>'.join(insights[:3])}
            </div>
        </div>
        """, unsafe_allow_html=True)
    else:
        st.markdown("""
        <div class="summary-card">
            <div class="summary-card-title">Document Summary</div>
            <div style="color: #64748b;">No additional insights available for this invoice.</div>
        </div>
        """, unsafe_allow_html=True)
elif has_document:
    st.markdown("""
    <div class="summary-card">
        <div class="summary-card-title">Document Summary</div>
        <div style="color: #64748b;">No line items extracted from this document.</div>
    </div>
    """, unsafe_allow_html=True)
else:
    st.markdown("""
    <div class="summary-card">
        <div class="summary-card-title">Document Summary</div>
        <div style="color: #94a3b8;">A summary of the extracted data will appear here.</div>
    </div>
    """, unsafe_allow_html=True)

# ── Changes Comparison ────────────────────────────────
if has_document and "_orig" in st.session_state:
    current_vals = {
        "Invoice Number": form_invoice_number,
        "PO Number": form_po_number,
        "Vendor Name": form_vendor_name,
        "Address": form_vendor_address,
        "City": form_vendor_city,
        "State": form_vendor_state,
        "ZIP": form_vendor_zip,
        "Phone": form_vendor_phone,
        "Email": form_vendor_email,
    }
    orig = st.session_state["_orig"]
    changes = []
    for field, current in current_vals.items():
        original = orig.get(field, "")
        if current and original and current != original:
            changes.append(f"<strong>{field}:</strong> {original} &rarr; {current}")

    if changes:
        st.markdown(f"""
        <div class="changes-banner">
            <strong>Changes detected</strong> (original &rarr; edited)<br>
            {'<br>'.join(changes)}
        </div>
        """, unsafe_allow_html=True)

# ── Submit ────────────────────────────────────────────
st.divider()

if has_document and invoice:
    if st.button("Submit", type="primary", use_container_width=True):
        submit_invoice = deepcopy(invoice)
        submit_invoice.invoice_number = form_invoice_number or invoice.invoice_number
        submit_invoice.po_number = form_po_number or invoice.po_number
        if form_invoice_date:
            submit_invoice.invoice_date = form_invoice_date
        if form_due_date:
            submit_invoice.due_date = form_due_date
        submit_invoice.vendor.name = form_vendor_name or invoice.vendor.name
        submit_invoice.vendor.address = form_vendor_address or invoice.vendor.address
        submit_invoice.vendor.city = form_vendor_city or invoice.vendor.city
        submit_invoice.vendor.state = form_vendor_state or invoice.vendor.state
        submit_invoice.vendor.zip = form_vendor_zip or invoice.vendor.zip
        submit_invoice.vendor.phone = form_vendor_phone or invoice.vendor.phone
        submit_invoice.vendor.email = form_vendor_email or invoice.vendor.email

        with st.spinner("Loading to Snowflake..."):
            sf_loader = SnowflakeLoader(dry_run=False)
            sf_loader.connect()
            doc_id = sf_loader.load_bronze(parse_result)
            sf_loader.load_silver(doc_id, submit_invoice, validation)
            sf_result = sf_loader.load_invoice(submit_invoice, validation)
            sf_loader.close()

        inv_num = submit_invoice.invoice_number or "Invoice"
        vendor = submit_invoice.vendor.name or "Unknown vendor"

        st.markdown(f"""
        <div class="submitted-banner">
            <strong>{inv_num}</strong> from <strong>{vendor}</strong> has been submitted successfully.
        </div>
        """, unsafe_allow_html=True)
else:
    st.button("Submit", type="primary", use_container_width=True, disabled=True)

# ── Recent Submissions ────────────────────────────────
st.divider()

@st.cache_data(ttl=10)
def load_recent_submissions():
    try:
        from dotenv import load_dotenv
        load_dotenv()
        import snowflake.connector
        conn = snowflake.connector.connect(
            account=os.environ.get("SNOWFLAKE_ACCOUNT", ""),
            user=os.environ.get("SNOWFLAKE_USER", ""),
            password=os.environ.get("SNOWFLAKE_PASSWORD", ""),
            database=os.environ.get("SNOWFLAKE_DATABASE", "parsely"),
            warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        )
        cursor = conn.cursor()
        cursor.execute("""
            SELECT invoice_number, vendor_name, total_amount,
                   invoice_date, validation_status, parse_timestamp
            FROM silver.parsed_invoices ORDER BY parse_timestamp DESC LIMIT 10
        """)
        rows = cursor.fetchall()
        cols = ["Invoice #", "Vendor", "Total", "Invoice Date", "Status", "Submitted"]
        cursor.close()
        conn.close()
        return rows, cols
    except Exception:
        return None, None

rows, columns = load_recent_submissions()

if rows:
    import pandas as pd
    df = pd.DataFrame(rows, columns=columns)
    df["Total"] = df["Total"].apply(lambda x: f"${x:,.2f}" if x else "--")
    df["Invoice Date"] = df["Invoice Date"].apply(lambda x: str(x) if x else "--")
    df["Submitted"] = df["Submitted"].apply(lambda x: x.strftime("%b %d, %Y %I:%M %p") if x else "--")

    # Style the dataframe with colored header
    def style_table(styler):
        styler.set_table_styles([
            {"selector": "thead tr th", "props": [
                ("background-color", "#0f172a"), ("color", "#64ffda"),
                ("font-weight", "700"), ("font-size", "0.9rem"),
                ("padding", "10px 12px"), ("letter-spacing", "0.3px"),
            ]},
            {"selector": "tbody tr td", "props": [
                ("padding", "8px 12px"), ("font-size", "0.88rem"),
            ]},
            {"selector": "tbody tr:nth-child(even)", "props": [
                ("background-color", "#f1f5f9"),
            ]},
        ])
        return styler

    st.markdown('<div class="submissions-label">Recent Submissions</div>', unsafe_allow_html=True)
    st.dataframe(df, use_container_width=True, hide_index=True)
else:
    st.caption("No submissions yet. Upload and submit an invoice to see it here.")
