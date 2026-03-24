"""
Tests for the document summarizer.
"""

from datetime import date

from src.extraction.schemas import InvoiceData, LineItem, VendorInfo
from src.summarization.summarizer import InvoiceSummarizer
from src.validation.validators import InvoiceValidator


class TestInvoiceSummarizer:

    def setup_method(self):
        self.summarizer = InvoiceSummarizer()

    def _make_invoice(self) -> InvoiceData:
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
            parse_confidence=0.95,
        )

    def test_summary_contains_invoice_number(self):
        invoice = self._make_invoice()
        summary = self.summarizer.summarize(invoice)
        assert "INV-2026-001" in summary

    def test_summary_contains_vendor_name(self):
        invoice = self._make_invoice()
        summary = self.summarizer.summarize(invoice)
        assert "Acme Corp" in summary

    def test_summary_contains_total(self):
        invoice = self._make_invoice()
        summary = self.summarizer.summarize(invoice)
        assert "226.80" in summary

    def test_summary_contains_line_items(self):
        invoice = self._make_invoice()
        summary = self.summarizer.summarize(invoice)
        assert "Widget A" in summary
        assert "2 items" in summary

    def test_brief_summary(self):
        invoice = self._make_invoice()
        brief = self.summarizer.summarize_brief(invoice)
        assert "INV-2026-001" in brief
        assert "Acme Corp" in brief
        assert "$226.80" in brief

    def test_summary_with_validation(self):
        invoice = self._make_invoice()
        validator = InvoiceValidator()
        validation = validator.validate(invoice)
        summary = self.summarizer.summarize(invoice, validation)
        assert "DATA QUALITY" in summary
        assert "PASSED" in summary

    def test_empty_invoice_summary(self):
        invoice = InvoiceData()
        summary = self.summarizer.summarize(invoice)
        assert "DOCUMENT SUMMARY" in summary

    def test_brief_empty_invoice(self):
        invoice = InvoiceData()
        brief = self.summarizer.summarize_brief(invoice)
        assert "not available" in brief.lower()

    def test_summary_with_sample_data(self, sample_invoice_text):
        """Test summarizer with actual extracted sample data."""
        from src.extraction.field_extractor import FieldExtractor
        extractor = FieldExtractor()
        invoice = extractor.extract(sample_invoice_text)
        summary = self.summarizer.summarize(invoice)
        assert "INV-2026-001" in summary
        assert "Greenfield" in summary
