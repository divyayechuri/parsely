"""
Parsely Dashboard — Streamlit multi-page app page.

Queries the Gold layer in Snowflake and displays:
- Key metrics (invoices processed, total spend, unique vendors)
- Spend by vendor (horizontal bar chart)
- Invoices over time (line chart)
- Top 5 line items by amount (table)
"""

import os
import sys

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")

import streamlit as st

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

import pandas as pd

st.set_page_config(page_title="Dashboard", layout="wide")

# ── Styling ───────────────────────────────────────────
st.markdown("""
<style>
    /* Sidebar */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #0f172a, #1e293b);
    }
    [data-testid="stSidebar"] * {
        color: #e2e8f0 !important;
    }
    [data-testid="stSidebar"] [data-testid="stSidebarNavLink"] {
        font-size: 1rem !important;
        font-weight: 500 !important;
        padding: 0.6rem 1rem !important;
        border-radius: 8px !important;
        margin: 2px 8px !important;
    }
    [data-testid="stSidebar"] [data-testid="stSidebarNavLink"][aria-selected="true"] {
        background: rgba(100, 255, 218, 0.15) !important;
        color: #64ffda !important;
    }
    [data-testid="stSidebar"] [data-testid="stSidebarNavLink"]:hover {
        background: rgba(255, 255, 255, 0.08) !important;
    }

    .section-label {
        color: #0f3460; font-size: 1.1rem; font-weight: 700;
        letter-spacing: 0.5px; margin-bottom: 0.5rem;
        padding-bottom: 0.3rem; border-bottom: 2px solid #e2e8f0;
    }
</style>
""", unsafe_allow_html=True)


# ── Data Loading ─────────────────────────────────────


def _get_snowflake_connection():
    """Create a Snowflake connection using .env credentials."""
    from dotenv import load_dotenv
    load_dotenv()

    import snowflake.connector

    return snowflake.connector.connect(
        account=os.environ.get("SNOWFLAKE_ACCOUNT", ""),
        user=os.environ.get("SNOWFLAKE_USER", ""),
        password=os.environ.get("SNOWFLAKE_PASSWORD", ""),
        database=os.environ.get("SNOWFLAKE_DATABASE", "parsely"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
    )


@st.cache_data(ttl=30, show_spinner=False)
def load_invoice_summary() -> pd.DataFrame:
    """Load fact_invoice_summary joined with dim_vendors."""
    conn = _get_snowflake_connection()
    try:
        query = """
            SELECT
                s.document_key,
                v.vendor_name,
                s.total_amount,
                s.subtotal,
                s.tax_amount,
                s.total_line_items,
                s.invoice_date
            FROM gold.fact_invoice_summary s
            LEFT JOIN gold.dim_vendors v
                ON s.vendor_key = v.vendor_key
            ORDER BY s.invoice_date DESC
        """
        df = pd.read_sql(query, conn)
        return df
    finally:
        conn.close()


@st.cache_data(ttl=30, show_spinner=False)
def load_top_line_items() -> pd.DataFrame:
    """Load top 5 line items by amount from the Gold layer."""
    conn = _get_snowflake_connection()
    try:
        query = """
            SELECT
                li.description,
                li.quantity,
                li.unit_price,
                li.line_amount,
                v.vendor_name
            FROM gold.fact_invoice_line_items li
            LEFT JOIN gold.dim_vendors v
                ON li.vendor_key = v.vendor_key
            ORDER BY li.line_amount DESC
            LIMIT 5
        """
        df = pd.read_sql(query, conn)
        return df
    finally:
        conn.close()


# ── Page Content ─────────────────────────────────────

st.title("Dashboard")

with st.spinner(""):
    try:
        summary_df = load_invoice_summary()
        line_items_df = load_top_line_items()
    except Exception:
        st.warning("Unable to load data. Please check your connection.")
        st.stop()

if summary_df.empty:
    st.info("No invoice data found in the Gold layer yet. Submit an invoice to get started.")
    st.stop()

# Normalize column names to lowercase for consistent access
summary_df.columns = [c.lower() for c in summary_df.columns]
line_items_df.columns = [c.lower() for c in line_items_df.columns]

# ── Key Metrics ──────────────────────────────────────
st.markdown('<div class="section-label">Key Metrics</div>', unsafe_allow_html=True)

total_invoices = len(summary_df)
total_spend = summary_df["total_amount"].sum() if "total_amount" in summary_df.columns else 0
num_vendors = summary_df["vendor_name"].nunique() if "vendor_name" in summary_df.columns else 0

m1, m2, m3 = st.columns(3)
m1.metric("Total Invoices Processed", f"{total_invoices:,}")
m2.metric("Total Spend", f"${total_spend:,.2f}")
m3.metric("Number of Vendors", f"{num_vendors:,}")

st.divider()

# ── Charts ───────────────────────────────────────────
chart_left, chart_right = st.columns(2)

with chart_left:
    st.markdown('<div class="section-label">Spend by Vendor</div>', unsafe_allow_html=True)
    if "vendor_name" in summary_df.columns and "total_amount" in summary_df.columns:
        vendor_spend = (
            summary_df.groupby("vendor_name", as_index=False)["total_amount"]
            .sum()
            .sort_values("total_amount", ascending=True)
        )
        st.bar_chart(
            vendor_spend.set_index("vendor_name")["total_amount"],
            horizontal=True,
        )
    else:
        st.info("Vendor spend data not available.")

with chart_right:
    st.markdown('<div class="section-label">Invoices Over Time</div>', unsafe_allow_html=True)
    if "invoice_date" in summary_df.columns:
        time_df = summary_df.copy()
        time_df["invoice_date"] = pd.to_datetime(time_df["invoice_date"], errors="coerce")
        time_df = time_df.dropna(subset=["invoice_date"])
        if not time_df.empty:
            daily = (
                time_df.groupby("invoice_date", as_index=False)["total_amount"]
                .sum()
                .sort_values("invoice_date")
            )
            st.line_chart(daily.set_index("invoice_date")["total_amount"])
        else:
            st.info("No valid invoice dates to chart.")
    else:
        st.info("Invoice date data not available.")

st.divider()

# ── Top Line Items ───────────────────────────────────
st.markdown('<div class="section-label">Top 5 Line Items by Amount</div>', unsafe_allow_html=True)

if not line_items_df.empty:
    display_df = line_items_df.rename(columns={
        "description": "Description",
        "quantity": "Qty",
        "unit_price": "Unit Price",
        "line_amount": "Amount",
        "vendor_name": "Vendor",
    })
    st.dataframe(display_df, use_container_width=True, hide_index=True)
else:
    st.info("No line item data found.")
