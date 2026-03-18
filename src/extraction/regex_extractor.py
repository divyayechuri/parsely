"""
Regex-based field extraction for invoice documents.

Regex patterns match predictable text formats — invoice numbers, dates,
phone numbers, currency amounts, etc. This is the primary extraction
method because invoices follow fairly consistent formatting conventions.

Each function extracts one type of field and returns the match or None.
The field_extractor orchestrates these into a complete InvoiceData object.
"""

import re
from datetime import date, datetime
from typing import Optional

from src.extraction.schemas import BillToInfo, LineItem, VendorInfo


# ── Invoice Header Fields ─────────────────────────────────────

def extract_invoice_number(text: str) -> Optional[str]:
    """
    Extract invoice/document number.
    Matches patterns like: INV-2026-001, Invoice #12345, Invoice No. 456
    """
    patterns = [
        r"Invoice\s*(?:Number|No\.?|#)\s*:?\s*([A-Z0-9][\w\-]+)",
        r"(INV[\-\s]?\d{4}[\-\s]?\d{1,6})",
        r"Invoice\s*:\s*([A-Z0-9][\w\-]+)",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return match.group(1).strip()
    return None


def extract_date(text: str, field_name: str) -> Optional[date]:
    """
    Extract a date associated with a specific field label.

    Args:
        text: Full document text
        field_name: Label to search for (e.g., 'Invoice Date', 'Due Date')

    Supports formats: 2026-01-15, 01/15/2026, January 15, 2026, etc.
    """
    # Look for the field label followed by a date
    label_pattern = rf"{field_name}\s*:?\s*"

    date_formats = [
        (rf"{label_pattern}(\d{{4}}-\d{{2}}-\d{{2}})", "%Y-%m-%d"),
        (rf"{label_pattern}(\d{{2}}/\d{{2}}/\d{{4}})", "%m/%d/%Y"),
        (rf"{label_pattern}(\d{{2}}-\d{{2}}-\d{{4}})", "%m-%d-%Y"),
        (rf"{label_pattern}(\w+ \d{{1,2}},?\s*\d{{4}})", None),  # "January 15, 2026"
    ]

    for pattern, fmt in date_formats:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            date_str = match.group(1).strip()
            if fmt:
                try:
                    return datetime.strptime(date_str, fmt).date()
                except ValueError:
                    continue
            else:
                # Handle month name formats
                for month_fmt in ["%B %d, %Y", "%B %d %Y", "%b %d, %Y", "%b %d %Y"]:
                    try:
                        return datetime.strptime(date_str, month_fmt).date()
                    except ValueError:
                        continue
    return None


def extract_invoice_date(text: str) -> Optional[date]:
    """Extract the invoice issue date."""
    return extract_date(text, "Invoice Date")


def extract_due_date(text: str) -> Optional[date]:
    """Extract the payment due date."""
    return extract_date(text, "Due Date")


def extract_po_number(text: str) -> Optional[str]:
    """
    Extract purchase order number.
    Matches: PO-2026-0455, PO Number: 12345, P.O. #ABC-123
    """
    patterns = [
        r"(?:PO|P\.O\.)\s*(?:Number|No\.?|#)\s*:?\s*([A-Z0-9][\w\-]+)",
        r"(PO[\-\s]?[\w\-]+\d+)",
        r"(MDOT[\-\s][\w\-]+)",  # Government PO formats
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return match.group(1).strip()
    return None


# ── Vendor & Bill-To Extraction ───────────────────────────────

def extract_phone(text: str) -> Optional[str]:
    """
    Extract phone number.
    Matches: (217) 555-0142, 217-555-0142, 2175550142
    """
    pattern = r"\(?\d{3}\)?[\s\-\.]?\d{3}[\s\-\.]?\d{4}"
    match = re.search(pattern, text)
    if match:
        return match.group(0).strip()
    return None


def extract_email(text: str) -> Optional[str]:
    """Extract email address."""
    pattern = r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}"
    match = re.search(pattern, text)
    if match:
        return match.group(0).strip()
    return None


def extract_city_state_zip(line: str) -> tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Parse a 'City, ST 12345' line into components.
    Matches: Springfield, IL 62704 or Austin, TX 78759-1234
    """
    pattern = r"([A-Za-z\s]+),\s*([A-Z]{2})\s+(\d{5}(?:-\d{4})?)"
    match = re.search(pattern, line)
    if match:
        return match.group(1).strip(), match.group(2).strip(), match.group(3).strip()
    return None, None, None


def extract_vendor_info(text: str) -> VendorInfo:
    """
    Extract vendor information from the top section of the invoice.

    Strategy: The vendor info is typically the first block of text after
    the invoice title. We look for the section before 'Bill To' and
    extract name, address, city/state/zip, phone, and email from it.
    """
    # Get text before "Bill To" — this is typically the vendor section
    bill_to_split = re.split(r"Bill\s*To\s*:?", text, flags=re.IGNORECASE)
    vendor_section = bill_to_split[0] if bill_to_split else text

    # Remove header lines (INVOICE, separator lines)
    lines = [
        line.strip() for line in vendor_section.split("\n")
        if line.strip()
        and not re.match(r"^[=─\-═]+$", line.strip())
        and line.strip().upper() != "INVOICE"
    ]

    vendor = VendorInfo()

    if not lines:
        return vendor

    # First non-empty line after header is usually the vendor name
    vendor.name = lines[0] if lines else None

    # Second line is usually the street address
    if len(lines) > 1:
        vendor.address = lines[1]

    # Look for city, state, zip pattern in remaining lines
    for line in lines[2:]:
        city, state, zip_code = extract_city_state_zip(line)
        if city:
            vendor.city = city
            vendor.state = state
            vendor.zip = zip_code
            break

    # Extract phone and email from the vendor section
    vendor.phone = extract_phone(vendor_section)
    vendor.email = extract_email(vendor_section)

    return vendor


def extract_bill_to_info(text: str) -> BillToInfo:
    """
    Extract bill-to / recipient information.

    Strategy: Find the 'Bill To:' section and parse the lines that follow
    until we hit the next section (Invoice Number, a separator, etc.).
    """
    bill_to = BillToInfo()

    match = re.search(r"Bill\s*To\s*:?\s*\n(.*?)(?=Invoice\s*(?:Number|No|#|Date)|[─═\-]{10,})",
                       text, re.DOTALL | re.IGNORECASE)
    if not match:
        return bill_to

    section = match.group(1)
    lines = [line.strip() for line in section.split("\n") if line.strip()]

    if not lines:
        return bill_to

    # First line is the recipient name
    bill_to.name = lines[0]

    # Second line is the address
    if len(lines) > 1:
        bill_to.address = lines[1]

    # Look for city, state, zip
    for line in lines[1:]:
        city, state, zip_code = extract_city_state_zip(line)
        if city:
            bill_to.city = city
            bill_to.state = state
            bill_to.zip = zip_code
            break

    return bill_to


# ── Financial Data Extraction ─────────────────────────────────

def extract_line_items(text: str) -> list[LineItem]:
    """
    Extract line items from the invoice table.

    Strategy: Find rows that have a description followed by numeric columns
    (quantity, unit price, amount). Invoice tables vary in format, but
    typically follow the pattern: text, number, number, number.
    """
    items = []

    # Match lines with: description, quantity, unit_price, amount
    # The numbers may have commas (e.g., 1,149.00)
    pattern = r"^(.+?)\s{2,}(\d+(?:,\d{3})*(?:\.\d+)?)\s+([\d,]+\.\d{2})\s+([\d,]+\.\d{2})\s*$"

    for line in text.split("\n"):
        line = line.strip()
        if not line or re.match(r"^[=─\-═]+$", line):
            continue
        # Skip header row
        if re.match(r"Description\s+Qty", line, re.IGNORECASE):
            continue

        match = re.match(pattern, line)
        if match:
            description = match.group(1).strip()
            quantity = float(match.group(2).replace(",", ""))
            unit_price = float(match.group(3).replace(",", ""))
            amount = float(match.group(4).replace(",", ""))

            items.append(LineItem(
                description=description,
                quantity=quantity,
                unit_price=unit_price,
                amount=amount,
            ))

    return items


def extract_subtotal(text: str) -> Optional[float]:
    """Extract the subtotal amount."""
    pattern = r"Subtotal\s*:?\s*([\d,]+\.\d{2})"
    match = re.search(pattern, text, re.IGNORECASE)
    if match:
        return float(match.group(1).replace(",", ""))
    return None


def extract_tax(text: str) -> tuple[Optional[float], Optional[float]]:
    """
    Extract tax rate and tax amount.
    Returns (tax_rate_as_decimal, tax_amount).
    Matches: Tax (8.0%): 22.74
    """
    # Try to get both rate and amount
    pattern = r"Tax\s*\((\d+\.?\d*)%\)\s*:?\s*([\d,]+\.\d{2})"
    match = re.search(pattern, text, re.IGNORECASE)
    if match:
        rate = float(match.group(1)) / 100.0
        amount = float(match.group(2).replace(",", ""))
        return rate, amount

    # Try just the amount
    pattern = r"Tax\s*:?\s*([\d,]+\.\d{2})"
    match = re.search(pattern, text, re.IGNORECASE)
    if match:
        return None, float(match.group(1).replace(",", ""))

    return None, None


def extract_total(text: str) -> Optional[float]:
    """
    Extract the total amount due.

    Searches line-by-line to avoid matching subtotal or tax amounts.
    Looks for lines containing 'TOTAL' (but not 'Subtotal') followed
    by a dollar amount.
    """
    for line in text.split("\n"):
        stripped = line.strip()
        # Skip subtotal lines
        if re.match(r"Subtotal", stripped, re.IGNORECASE):
            continue
        # Match lines with TOTAL, Total Due, Amount Due, etc.
        match = re.match(
            r".*(?:TOTAL\s*(?:DUE|AMOUNT)?|Amount\s*Due)\s*:?\s*([\d,]+\.\d{2})",
            stripped,
            re.IGNORECASE,
        )
        if match:
            return float(match.group(1).replace(",", ""))
    return None
