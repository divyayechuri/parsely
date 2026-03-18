"""
Tests for Pydantic schemas.

Verifies that schemas validate data correctly — accepting valid data
and rejecting invalid data with clear error messages.
"""

import pytest
from datetime import date

from src.extraction.schemas import InvoiceData, LineItem, VendorInfo


class TestLineItem:
    def test_calculates_amount_when_missing(self):
        """Amount should auto-calculate from quantity * unit_price."""
        item = LineItem(description="Test", quantity=5, unit_price=10.00)
        assert item.amount == 50.00

    def test_preserves_explicit_amount(self):
        """If amount is provided, it should not be overwritten."""
        item = LineItem(description="Test", quantity=5, unit_price=10.00, amount=49.99)
        assert item.amount == 49.99

    def test_handles_missing_fields(self):
        """LineItem should accept partial data (all fields Optional)."""
        item = LineItem(description="Test")
        assert item.quantity is None
        assert item.amount is None


class TestVendorInfo:
    def test_accepts_complete_data(self):
        vendor = VendorInfo(
            name="Acme Corp",
            address="123 Main St",
            city="Springfield",
            state="IL",
            zip="62704",
        )
        assert vendor.name == "Acme Corp"

    def test_accepts_partial_data(self):
        vendor = VendorInfo(name="Acme Corp")
        assert vendor.city is None


class TestInvoiceData:
    def test_field_completion_rate_all_filled(self):
        invoice = InvoiceData(
            invoice_number="INV-001",
            invoice_date=date(2026, 1, 1),
            vendor=VendorInfo(name="Acme Corp"),
            total_amount=100.00,
        )
        assert invoice.field_completion_rate() == 1.0

    def test_field_completion_rate_none_filled(self):
        invoice = InvoiceData()
        assert invoice.field_completion_rate() == 0.0

    def test_field_completion_rate_partial(self):
        invoice = InvoiceData(
            invoice_number="INV-001",
            vendor=VendorInfo(name="Acme Corp"),
        )
        # 2 out of 4 key fields
        assert invoice.field_completion_rate() == 0.5

    def test_default_currency_is_usd(self):
        invoice = InvoiceData()
        assert invoice.currency == "USD"
