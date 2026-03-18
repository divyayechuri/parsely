"""
Generate sample invoice PDFs for development and testing.

These are synthetic invoices using fake data — no real company
or personal information. Run this script to populate data/samples/.

Usage:
    python data/generate_sample_invoices.py
"""

import json
import os

# Sample invoice data — publicly safe, completely fictional
SAMPLE_INVOICES = [
    {
        "invoice_number": "INV-2026-001",
        "invoice_date": "2026-01-15",
        "due_date": "2026-02-14",
        "vendor": {
            "name": "Greenfield Office Supplies Co.",
            "address": "742 Commerce Blvd",
            "city": "Springfield",
            "state": "IL",
            "zip": "62704",
            "phone": "(217) 555-0142",
            "email": "billing@greenfieldoffice.example.com"
        },
        "bill_to": {
            "name": "Lakewood Municipal Services",
            "address": "100 City Hall Plaza",
            "city": "Lakewood",
            "state": "OH",
            "zip": "44107"
        },
        "po_number": "PO-2026-0455",
        "line_items": [
            {"description": "A4 Copy Paper (10 reams)", "quantity": 10, "unit_price": 8.50},
            {"description": "Black Ink Cartridge - HP 61", "quantity": 5, "unit_price": 24.99},
            {"description": "Ballpoint Pens - Box of 50", "quantity": 3, "unit_price": 12.75},
            {"description": "Manila File Folders - Pack of 100", "quantity": 2, "unit_price": 18.00},
        ],
        "tax_rate": 0.08
    },
    {
        "invoice_number": "INV-2026-002",
        "invoice_date": "2026-02-03",
        "due_date": "2026-03-05",
        "vendor": {
            "name": "Summit IT Solutions LLC",
            "address": "1500 Technology Park Dr, Suite 200",
            "city": "Austin",
            "state": "TX",
            "zip": "78759",
            "phone": "(512) 555-0198",
            "email": "accounts@summitit.example.com"
        },
        "bill_to": {
            "name": "Riverside County School District",
            "address": "3900 Education Way",
            "city": "Riverside",
            "state": "CA",
            "zip": "92501"
        },
        "po_number": "PO-EDU-2026-112",
        "line_items": [
            {"description": "Dell Latitude 5540 Laptop", "quantity": 15, "unit_price": 1149.00},
            {"description": "Logitech MK545 Keyboard & Mouse Combo", "quantity": 15, "unit_price": 49.99},
            {"description": "24-inch Dell Monitor P2423D", "quantity": 15, "unit_price": 279.99},
            {"description": "USB-C Docking Station", "quantity": 15, "unit_price": 189.00},
            {"description": "On-Site Setup & Configuration (per unit)", "quantity": 15, "unit_price": 75.00},
        ],
        "tax_rate": 0.0825
    },
    {
        "invoice_number": "INV-2026-003",
        "invoice_date": "2026-03-10",
        "due_date": "2026-04-09",
        "vendor": {
            "name": "Precision Industrial Parts Inc.",
            "address": "8800 Manufacturing Row",
            "city": "Detroit",
            "state": "MI",
            "zip": "48204",
            "phone": "(313) 555-0276",
            "email": "invoices@precisionparts.example.com"
        },
        "bill_to": {
            "name": "State of Michigan - Dept of Transportation",
            "address": "425 W Ottawa St",
            "city": "Lansing",
            "state": "MI",
            "zip": "48933"
        },
        "po_number": "MDOT-2026-03-0087",
        "line_items": [
            {"description": "Steel Guardrail Section - 12ft", "quantity": 200, "unit_price": 145.00},
            {"description": "Guardrail Post - Galvanized Steel", "quantity": 400, "unit_price": 38.50},
            {"description": "Reflective Road Marker - Amber", "quantity": 1000, "unit_price": 4.75},
            {"description": "Bolt Kit - Guardrail Assembly", "quantity": 200, "unit_price": 12.00},
            {"description": "Delivery & Freight (flatbed)", "quantity": 1, "unit_price": 2500.00},
        ],
        "tax_rate": 0.06
    },
]


def generate_text_invoices(output_dir: str) -> None:
    """
    Generate sample invoices as structured text files and JSON.
    These serve as test fixtures for the parsing pipeline.

    We generate both formats:
    - .txt: Simulates what pdfplumber would extract from a real PDF
    - .json: The expected structured output (ground truth for testing)
    """
    os.makedirs(output_dir, exist_ok=True)

    for i, invoice in enumerate(SAMPLE_INVOICES, start=1):
        # Calculate totals
        line_items = invoice["line_items"]
        for item in line_items:
            item["amount"] = round(item["quantity"] * item["unit_price"], 2)

        subtotal = round(sum(item["amount"] for item in line_items), 2)
        tax = round(subtotal * invoice["tax_rate"], 2)
        total = round(subtotal + tax, 2)

        # Generate text version (simulates raw PDF extraction)
        vendor = invoice["vendor"]
        bill_to = invoice["bill_to"]

        text = f"""
{'='*60}
                        INVOICE
{'='*60}

{vendor['name']}
{vendor['address']}
{vendor['city']}, {vendor['state']} {vendor['zip']}
Phone: {vendor['phone']}
Email: {vendor['email']}

{'─'*60}

Bill To:
{bill_to['name']}
{bill_to['address']}
{bill_to['city']}, {bill_to['state']} {bill_to['zip']}

Invoice Number:  {invoice['invoice_number']}
Invoice Date:    {invoice['invoice_date']}
Due Date:        {invoice['due_date']}
PO Number:       {invoice['po_number']}

{'─'*60}
{"Description":<35} {"Qty":>5} {"Unit Price":>12} {"Amount":>12}
{'─'*60}
"""
        for item in line_items:
            text += f"{item['description']:<35} {item['quantity']:>5} {item['unit_price']:>12,.2f} {item['amount']:>12,.2f}\n"

        text += f"""{'─'*60}
{"Subtotal:":<54} {subtotal:>12,.2f}
{"Tax (" + f"{invoice['tax_rate']*100:.1f}%" + "):":<54} {tax:>12,.2f}
{'═'*60}
{"TOTAL DUE:":<54} {total:>12,.2f}
{'═'*60}

Payment Terms: Net 30
Thank you for your business!
"""

        # Write text file
        txt_path = os.path.join(output_dir, f"sample_invoice_{i:02d}.txt")
        with open(txt_path, "w", encoding="utf-8") as f:
            f.write(text)

        # Write JSON ground truth (expected extraction output)
        ground_truth = {
            "invoice_number": invoice["invoice_number"],
            "invoice_date": invoice["invoice_date"],
            "due_date": invoice["due_date"],
            "po_number": invoice["po_number"],
            "vendor": vendor,
            "bill_to": bill_to,
            "line_items": line_items,
            "subtotal": subtotal,
            "tax_rate": invoice["tax_rate"],
            "tax_amount": tax,
            "total_amount": total,
            "currency": "USD"
        }

        json_path = os.path.join(output_dir, f"sample_invoice_{i:02d}_expected.json")
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(ground_truth, f, indent=2)

        print(f"Generated: {txt_path}")
        print(f"Generated: {json_path}")

    print(f"\nDone! {len(SAMPLE_INVOICES)} sample invoices generated in {output_dir}/")


if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    generate_text_invoices(os.path.join(script_dir, "samples"))
