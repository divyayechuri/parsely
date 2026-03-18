"""
Tests for invoice data validation.

Verifies that validation rules correctly identify valid invoices,
catch errors in bad data, and produce the right status codes.
"""

from datetime import date

from src.extraction.schemas import InvoiceData, LineItem, VendorInfo
from src.validation.validators import (
    InvoiceValidator,
    RuleSeverity,
    ValidationStatus,
)


class TestInvoiceValidator:
    """Tests for the InvoiceValidator."""

    def setup_method(self):
        self.validator = InvoiceValidator()

    def _make_valid_invoice(self) -> InvoiceData:
        """Create a fully valid invoice for testing."""
        return InvoiceData(
            invoice_number="INV-2026-001",
            invoice_date=date(2026, 1, 15),
            due_date=date(2026, 2, 14),
            po_number="PO-2026-0455",
            vendor=VendorInfo(
                name="Acme Corp",
                address="123 Main St",
                city="Springfield",
                state="IL",
                zip="62704",
                phone="(217) 555-0142",
                email="billing@acme.example.com",
            ),
            line_items=[
                LineItem(description="Widget A", quantity=10, unit_price=8.50, amount=85.00),
                LineItem(description="Widget B", quantity=5, unit_price=25.00, amount=125.00),
            ],
            subtotal=210.00,
            tax_rate=0.08,
            tax_amount=16.80,
            total_amount=226.80,
        )

    def test_valid_invoice_passes(self):
        """A complete, consistent invoice should pass validation."""
        invoice = self._make_valid_invoice()
        result = self.validator.validate(invoice)
        assert result.status == ValidationStatus.PASSED

    def test_missing_invoice_number_fails(self):
        """Missing invoice number (required) should cause failure."""
        invoice = self._make_valid_invoice()
        invoice.invoice_number = None
        result = self.validator.validate(invoice)
        assert result.status == ValidationStatus.FAILED
        assert any(r.field_name == "invoice_number" for r in result.errors)

    def test_missing_vendor_name_fails(self):
        """Missing vendor name (required) should cause failure."""
        invoice = self._make_valid_invoice()
        invoice.vendor.name = None
        result = self.validator.validate(invoice)
        assert result.status == ValidationStatus.FAILED

    def test_missing_total_fails(self):
        """Missing total amount (required) should cause failure."""
        invoice = self._make_valid_invoice()
        invoice.total_amount = None
        result = self.validator.validate(invoice)
        assert result.status == ValidationStatus.FAILED

    def test_mismatched_total_fails(self):
        """Total that doesn't match subtotal + tax should fail."""
        invoice = self._make_valid_invoice()
        invoice.total_amount = 999.99  # Wrong total
        result = self.validator.validate(invoice)
        assert result.status == ValidationStatus.FAILED
        assert any(r.rule_name == "total_matches_components" for r in result.errors)

    def test_mismatched_subtotal_fails(self):
        """Subtotal that doesn't match sum of line items should fail."""
        invoice = self._make_valid_invoice()
        invoice.subtotal = 999.99  # Wrong subtotal
        result = self.validator.validate(invoice)
        assert any(r.rule_name == "subtotal_matches_line_items" for r in result.errors)

    def test_due_date_before_invoice_date_warns(self):
        """Due date before invoice date should generate a warning."""
        invoice = self._make_valid_invoice()
        invoice.due_date = date(2025, 12, 1)  # Before invoice date
        result = self.validator.validate(invoice)
        assert any(r.rule_name == "due_date_order" and not r.passed for r in result.rules)

    def test_missing_optional_field_warns(self):
        """Missing optional field (invoice_date) should warn, not fail."""
        invoice = self._make_valid_invoice()
        invoice.invoice_date = None
        result = self.validator.validate(invoice)
        # Should be REVIEW_NEEDED (warning), not FAILED
        assert result.status in (ValidationStatus.REVIEW_NEEDED, ValidationStatus.PASSED)

    def test_empty_invoice_fails(self):
        """Invoice with no data should fail validation."""
        invoice = InvoiceData()
        result = self.validator.validate(invoice)
        assert result.status == ValidationStatus.FAILED

    def test_result_summary(self):
        """Validation result should produce a summary dict."""
        invoice = self._make_valid_invoice()
        result = self.validator.validate(invoice)
        summary = result.summary()
        assert "status" in summary
        assert "total_rules" in summary
        assert "passed" in summary
        assert "errors" in summary
        assert "warnings" in summary

    def test_line_items_without_amounts_warns(self):
        """Line items missing amounts should generate a warning."""
        invoice = self._make_valid_invoice()
        invoice.line_items.append(LineItem(description="Incomplete item"))
        result = self.validator.validate(invoice)
        assert any(r.rule_name == "line_items_complete" for r in result.warnings)


class TestValidationWithSampleData:
    """Test validation against our sample invoices."""

    def setup_method(self):
        from src.extraction.field_extractor import FieldExtractor
        self.extractor = FieldExtractor(use_ner=False)
        self.validator = InvoiceValidator()

    def test_sample_invoice_01_passes(self, sample_invoice_text):
        """Sample invoice 01 should pass validation after extraction."""
        invoice = self.extractor.extract(sample_invoice_text)
        result = self.validator.validate(invoice)
        # Should pass or need review (not fail)
        assert result.status != ValidationStatus.FAILED, (
            f"Validation failed: {[e.message for e in result.errors]}"
        )
