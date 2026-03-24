"""
Generate real-world style invoice PDFs with DIFFERENT formats
than our standard samples. These test the extraction pipeline's
ability to handle varying layouts, column names, and structures.

Usage:
    python data/generate_real_world_invoices.py
"""

import os
from fpdf import FPDF


def generate_invoice_a(output_dir):
    """Janitorial services invoice - horizontal layout, service-based."""
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Helvetica", "B", 20)
    pdf.cell(0, 12, "TAX INVOICE", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(5)

    pdf.set_font("Helvetica", "", 10)
    y = pdf.get_y()
    pdf.cell(95, 5, "Pacific Coast Janitorial Services", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(95, 5, "2200 Harbor View Dr", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(95, 5, "San Diego, CA 92101", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(95, 5, "Tel: (619) 555-8834", new_x="LMARGIN", new_y="NEXT")
    y2 = pdf.get_y()

    pdf.set_y(y)
    pdf.set_x(110)
    pdf.cell(40, 5, "Invoice No:", new_x="END", new_y="LAST")
    pdf.cell(40, 5, "PCJS-4521", new_x="LMARGIN", new_y="NEXT")
    pdf.set_x(110)
    pdf.cell(40, 5, "Date:", new_x="END", new_y="LAST")
    pdf.cell(40, 5, "03/15/2026", new_x="LMARGIN", new_y="NEXT")
    pdf.set_x(110)
    pdf.cell(40, 5, "Due Date:", new_x="END", new_y="LAST")
    pdf.cell(40, 5, "04/14/2026", new_x="LMARGIN", new_y="NEXT")

    pdf.set_y(max(y2, pdf.get_y()) + 5)
    pdf.set_font("Helvetica", "B", 10)
    pdf.cell(0, 6, "Billed To: Oceanview Hotel Group", new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "", 10)
    pdf.cell(0, 5, "500 Beach Blvd, San Diego, CA 92109", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(5)

    # Line items with different column names (Hours, Rate/Hr instead of Qty, Unit Price)
    pdf.set_font("Helvetica", "B", 9)
    cols = [80, 25, 30, 30]
    pdf.cell(cols[0], 6, "Service Description", border=1)
    pdf.cell(cols[1], 6, "Hours", border=1, align="R")
    pdf.cell(cols[2], 6, "Rate/Hr", border=1, align="R")
    pdf.cell(cols[3], 6, "Total", border=1, align="R")
    pdf.ln()

    pdf.set_font("Helvetica", "", 9)
    items = [
        ("Lobby & Common Area Cleaning", "40", "$45.00", "$1,800.00"),
        ("Guest Room Deep Clean (12 rooms)", "36", "$55.00", "$1,980.00"),
        ("Window Washing - Exterior", "16", "$65.00", "$1,040.00"),
        ("Carpet Shampooing - Ballroom", "8", "$75.00", "$600.00"),
        ("Supplies & Materials", "", "", "$285.50"),
    ]
    for desc, hrs, rate, total in items:
        pdf.cell(cols[0], 6, desc, border=1)
        pdf.cell(cols[1], 6, hrs, border=1, align="R")
        pdf.cell(cols[2], 6, rate, border=1, align="R")
        pdf.cell(cols[3], 6, total, border=1, align="R")
        pdf.ln()

    pdf.ln(3)
    pdf.set_x(110)
    pdf.cell(50, 6, "Subtotal:", align="R")
    pdf.cell(30, 6, "$5,705.50", align="R")
    pdf.ln()
    pdf.set_x(110)
    pdf.cell(50, 6, "Sales Tax (7.75%):", align="R")
    pdf.cell(30, 6, "$442.18", align="R")
    pdf.ln()
    pdf.set_x(110)
    pdf.set_font("Helvetica", "B", 11)
    pdf.cell(50, 8, "Amount Due:", align="R")
    pdf.cell(30, 8, "$6,147.68", align="R")
    pdf.ln(10)
    pdf.set_font("Helvetica", "", 9)
    pdf.cell(0, 5, "Payment Terms: Net 30", new_x="LMARGIN", new_y="NEXT")

    path = os.path.join(output_dir, "real_invoice_A_janitorial.pdf")
    pdf.output(path)
    print(f"Created: {path}")


def generate_invoice_b(output_dir):
    """Electrical services invoice - minimal format, sparse layout."""
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Helvetica", "B", 14)
    pdf.cell(0, 10, "INVOICE", new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "", 10)
    pdf.cell(0, 6, "From: Bright Spark Electrical LLC", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 6, "789 Industrial Pkwy, Columbus, OH 43215", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 6, "Phone: (614) 555-3390", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(5)
    pdf.cell(0, 6, "To: Franklin County Public Library", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 6, "96 S Grant Ave, Columbus, OH 43215", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(5)
    pdf.cell(60, 6, "Invoice #: BSE-2026-0088")
    pdf.cell(60, 6, "Date: February 28, 2026")
    pdf.ln()
    pdf.cell(60, 6, "PO #: FCPL-2026-445")
    pdf.cell(60, 6, "Due: March 30, 2026")
    pdf.ln(8)

    pdf.set_font("Helvetica", "B", 10)
    pdf.cell(100, 6, "Description", border="B")
    pdf.cell(30, 6, "Qty", border="B", align="R")
    pdf.cell(30, 6, "Price", border="B", align="R")
    pdf.cell(30, 6, "Amount", border="B", align="R")
    pdf.ln()
    pdf.set_font("Helvetica", "", 10)

    items = [
        ("LED Panel Light 2x4 ft", "24", "$89.99", "$2,159.76"),
        ("Emergency Exit Sign - LED", "6", "$45.00", "$270.00"),
        ("Electrical Labor (per hour)", "12", "$95.00", "$1,140.00"),
        ("Wire & Conduit Materials", "1", "$380.00", "$380.00"),
    ]
    for desc, qty, price, amt in items:
        pdf.cell(100, 6, desc)
        pdf.cell(30, 6, qty, align="R")
        pdf.cell(30, 6, price, align="R")
        pdf.cell(30, 6, amt, align="R")
        pdf.ln()

    pdf.ln(5)
    pdf.cell(160, 6, "Subtotal: $3,949.76", align="R")
    pdf.ln()
    pdf.cell(160, 6, "Tax (7.50%): $296.23", align="R")
    pdf.ln()
    pdf.set_font("Helvetica", "B", 11)
    pdf.cell(160, 8, "Total Due: $4,245.99", align="R")

    path = os.path.join(output_dir, "real_invoice_B_electrical.pdf")
    pdf.output(path)
    print(f"Created: {path}")


def generate_invoice_c(output_dir):
    """Catering invoice - uses 'Statement' instead of 'Invoice', different labels."""
    pdf = FPDF()
    pdf.add_page()

    pdf.set_font("Helvetica", "B", 18)
    pdf.set_text_color(0, 51, 102)
    pdf.cell(0, 10, "Mountain View Catering Co.", new_x="LMARGIN", new_y="NEXT")
    pdf.set_text_color(0, 0, 0)
    pdf.set_font("Helvetica", "", 9)
    pdf.cell(0, 4, "1450 Alpine Road, Suite 200, Denver, CO 80202", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 4, "Phone: (303) 555-7712 | Email: billing@mvcc.example.com", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(8)

    pdf.set_font("Helvetica", "B", 16)
    pdf.cell(0, 10, "Statement of Services", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(3)

    pdf.set_font("Helvetica", "", 10)
    pdf.cell(40, 6, "Statement No.:", border=1, new_x="END")
    pdf.cell(55, 6, "MVC-2026-0312", border=1, new_x="END")
    pdf.cell(40, 6, "Client:", border=1, new_x="END")
    pdf.cell(55, 6, "Colorado State University", border=1)
    pdf.ln()
    pdf.cell(40, 6, "Statement Date:", border=1, new_x="END")
    pdf.cell(55, 6, "March 01, 2026", border=1, new_x="END")
    pdf.cell(40, 6, "Event:", border=1, new_x="END")
    pdf.cell(55, 6, "Spring Faculty Gala", border=1)
    pdf.ln()
    pdf.cell(40, 6, "Payment Due:", border=1, new_x="END")
    pdf.cell(55, 6, "March 31, 2026", border=1, new_x="END")
    pdf.cell(40, 6, "Ref:", border=1, new_x="END")
    pdf.cell(55, 6, "CSU-EVT-2026-15", border=1)
    pdf.ln(8)

    pdf.set_font("Helvetica", "B", 10)
    w = [70, 25, 25, 25, 25, 20]
    headers = ["Item", "Guests", "Per Person", "Subtotal", "Tax", "Total"]
    for i, h in enumerate(headers):
        pdf.cell(w[i], 7, h, border=1, align="C")
    pdf.ln()

    pdf.set_font("Helvetica", "", 9)
    items = [
        ("Dinner Buffet - Premium", "150", "$42.00", "$6,300.00", "$472.50", "$6,772.50"),
        ("Bar Service - Open (4hrs)", "150", "$28.00", "$4,200.00", "$315.00", "$4,515.00"),
        ("Dessert Station", "150", "$12.00", "$1,800.00", "$135.00", "$1,935.00"),
        ("Staff (servers x8, 6hrs)", "8", "$35.00", "$1,680.00", "$0.00", "$1,680.00"),
        ("Linen & Table Setup", "1", "$450.00", "$450.00", "$33.75", "$483.75"),
        ("Equipment Rental", "1", "$800.00", "$800.00", "$60.00", "$860.00"),
    ]
    for item in items:
        for i, val in enumerate(item):
            pdf.cell(w[i], 6, val, border=1, align="R" if i > 0 else "L")
        pdf.ln()

    pdf.ln(5)
    pdf.set_font("Helvetica", "B", 11)
    pdf.cell(140, 8, "Grand Total:", align="R")
    pdf.cell(50, 8, "$16,246.25", align="R")
    pdf.ln(10)
    pdf.set_font("Helvetica", "", 9)
    pdf.cell(0, 5, "Terms: Net 30.", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 5, "Thank you for choosing Mountain View Catering!", new_x="LMARGIN", new_y="NEXT")

    path = os.path.join(output_dir, "real_invoice_C_catering.pdf")
    pdf.output(path)
    print(f"Created: {path}")


if __name__ == "__main__":
    output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "samples", "real_world")
    os.makedirs(output_dir, exist_ok=True)
    generate_invoice_a(output_dir)
    generate_invoice_b(output_dir)
    generate_invoice_c(output_dir)
    print(f"\nDone! 3 real-world style invoices generated in {output_dir}/")
