"""
Tests for the regex-based field extractor.

Each test verifies that a specific regex function correctly
extracts the expected value from sample invoice text.
These tests use the ground truth JSON files as the source of truth.
"""

import json
import os
from datetime import date

from src.extraction.regex_extractor import (
    extract_bill_to_info,
    extract_due_date,
    extract_invoice_date,
    extract_invoice_number,
    extract_line_items,
    extract_po_number,
    extract_subtotal,
    extract_tax,
    extract_total,
    extract_vendor_info,
)

SAMPLES_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "samples")


def load_sample(invoice_num: int) -> tuple[str, dict]:
    """Load sample text and expected JSON for a given invoice number."""
    txt_path = os.path.join(SAMPLES_DIR, f"sample_invoice_{invoice_num:02d}.txt")
    json_path = os.path.join(SAMPLES_DIR, f"sample_invoice_{invoice_num:02d}_expected.json")
    with open(txt_path, "r", encoding="utf-8") as f:
        text = f.read()
    with open(json_path, "r", encoding="utf-8") as f:
        expected = json.load(f)
    return text, expected


class TestInvoiceNumber:
    def test_invoice_01(self):
        text, expected = load_sample(1)
        assert extract_invoice_number(text) == expected["invoice_number"]

    def test_invoice_02(self):
        text, expected = load_sample(2)
        assert extract_invoice_number(text) == expected["invoice_number"]

    def test_invoice_03(self):
        text, expected = load_sample(3)
        assert extract_invoice_number(text) == expected["invoice_number"]


class TestDates:
    def test_invoice_date_01(self):
        text, expected = load_sample(1)
        result = extract_invoice_date(text)
        assert result == date.fromisoformat(expected["invoice_date"])

    def test_due_date_01(self):
        text, expected = load_sample(1)
        result = extract_due_date(text)
        assert result == date.fromisoformat(expected["due_date"])

    def test_invoice_date_02(self):
        text, expected = load_sample(2)
        result = extract_invoice_date(text)
        assert result == date.fromisoformat(expected["invoice_date"])

    def test_due_date_03(self):
        text, expected = load_sample(3)
        result = extract_due_date(text)
        assert result == date.fromisoformat(expected["due_date"])


class TestPONumber:
    def test_standard_po(self):
        text, expected = load_sample(1)
        assert extract_po_number(text) == expected["po_number"]

    def test_education_po(self):
        text, expected = load_sample(2)
        assert extract_po_number(text) == expected["po_number"]

    def test_government_po(self):
        text, expected = load_sample(3)
        assert extract_po_number(text) == expected["po_number"]


class TestVendorInfo:
    def test_vendor_name(self):
        text, expected = load_sample(1)
        vendor = extract_vendor_info(text)
        assert vendor.name == expected["vendor"]["name"]

    def test_vendor_city_state_zip(self):
        text, expected = load_sample(1)
        vendor = extract_vendor_info(text)
        assert vendor.city == expected["vendor"]["city"]
        assert vendor.state == expected["vendor"]["state"]
        assert vendor.zip == expected["vendor"]["zip"]

    def test_vendor_phone(self):
        text, expected = load_sample(1)
        vendor = extract_vendor_info(text)
        assert vendor.phone == expected["vendor"]["phone"]

    def test_vendor_email(self):
        text, expected = load_sample(1)
        vendor = extract_vendor_info(text)
        assert vendor.email == expected["vendor"]["email"]


class TestBillToInfo:
    def test_bill_to_name(self):
        text, expected = load_sample(1)
        bill_to = extract_bill_to_info(text)
        assert bill_to.name == expected["bill_to"]["name"]

    def test_bill_to_city_state(self):
        text, expected = load_sample(1)
        bill_to = extract_bill_to_info(text)
        assert bill_to.city == expected["bill_to"]["city"]
        assert bill_to.state == expected["bill_to"]["state"]


class TestLineItems:
    def test_line_item_count_01(self):
        text, expected = load_sample(1)
        items = extract_line_items(text)
        assert len(items) == len(expected["line_items"])

    def test_line_item_amounts_01(self):
        text, expected = load_sample(1)
        items = extract_line_items(text)
        for parsed, exp in zip(items, expected["line_items"]):
            assert parsed.amount == exp["amount"]

    def test_line_item_count_02(self):
        text, expected = load_sample(2)
        items = extract_line_items(text)
        assert len(items) == len(expected["line_items"])

    def test_line_item_descriptions_01(self):
        text, expected = load_sample(1)
        items = extract_line_items(text)
        for parsed, exp in zip(items, expected["line_items"]):
            assert parsed.description == exp["description"]


class TestFinancials:
    def test_subtotal_01(self):
        text, expected = load_sample(1)
        assert extract_subtotal(text) == expected["subtotal"]

    def test_tax_01(self):
        text, expected = load_sample(1)
        tax_rate, tax_amount = extract_tax(text)
        assert tax_rate == expected["tax_rate"]
        assert tax_amount == expected["tax_amount"]

    def test_total_01(self):
        text, expected = load_sample(1)
        assert extract_total(text) == expected["total_amount"]

    def test_total_02(self):
        text, expected = load_sample(2)
        assert extract_total(text) == expected["total_amount"]

    def test_total_03(self):
        text, expected = load_sample(3)
        assert extract_total(text) == expected["total_amount"]
