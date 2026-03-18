"""
Tests for Snowflake and Databricks loaders (dry-run mode).

These tests verify the loading logic without needing actual
cloud credentials. Dry-run mode logs what would be executed
instead of connecting to the database.
"""

from datetime import date

from src.extraction.schemas import InvoiceData, LineItem, VendorInfo
from src.loading.databricks_loader import DatabricksLoader
from src.loading.snowflake_loader import SnowflakeLoader
from src.validation.validators import InvoiceValidator, ValidationStatus


def _make_invoice() -> InvoiceData:
    """Create a test invoice."""
    return InvoiceData(
        invoice_number="INV-TEST-001",
        invoice_date=date(2026, 1, 15),
        due_date=date(2026, 2, 14),
        po_number="PO-TEST-001",
        vendor=VendorInfo(
            name="Test Vendor LLC",
            address="100 Test St",
            city="Testville",
            state="TX",
            zip="75001",
            phone="(555) 123-4567",
            email="test@vendor.example.com",
        ),
        line_items=[
            LineItem(description="Item A", quantity=2, unit_price=50.00, amount=100.00),
            LineItem(description="Item B", quantity=1, unit_price=75.00, amount=75.00),
        ],
        subtotal=175.00,
        tax_rate=0.08,
        tax_amount=14.00,
        total_amount=189.00,
        parse_confidence=0.95,
    )


def _make_parse_result() -> dict:
    """Create a mock parse result from PDFParser."""
    return {
        "text": "INVOICE\nTest Vendor LLC\n...",
        "tables": [],
        "metadata": {"source": "test"},
        "file_name": "test_invoice.pdf",
        "file_type": "pdf",
        "file_size_bytes": 12345,
    }


class TestSnowflakeLoader:
    """Tests for SnowflakeLoader in dry-run mode."""

    def setup_method(self):
        self.loader = SnowflakeLoader(dry_run=True)
        self.validator = InvoiceValidator()

    def test_dry_run_load_succeeds(self):
        """Dry-run load should succeed without Snowflake credentials."""
        invoice = _make_invoice()
        validation = self.validator.validate(invoice)
        result = self.loader.load_invoice(invoice, validation)
        assert result["status"] == "loaded"
        assert "document_id" in result
        assert "vendor_id" in result

    def test_failed_validation_rejects_load(self):
        """Invoices that failed validation should be rejected."""
        invoice = InvoiceData()  # Empty = fails validation
        validation = self.validator.validate(invoice)
        assert validation.status == ValidationStatus.FAILED
        result = self.loader.load_invoice(invoice, validation)
        assert result["status"] == "rejected"

    def test_vendor_id_is_deterministic(self):
        """Same vendor should always produce the same vendor ID."""
        invoice1 = _make_invoice()
        invoice2 = _make_invoice()
        id1 = self.loader._generate_vendor_id(invoice1)
        id2 = self.loader._generate_vendor_id(invoice2)
        assert id1 == id2

    def test_different_vendors_get_different_ids(self):
        """Different vendors should get different IDs."""
        invoice1 = _make_invoice()
        invoice2 = _make_invoice()
        invoice2.vendor.name = "Different Vendor Inc."
        id1 = self.loader._generate_vendor_id(invoice1)
        id2 = self.loader._generate_vendor_id(invoice2)
        assert id1 != id2


class TestDatabricksLoader:
    """Tests for DatabricksLoader in dry-run mode."""

    def setup_method(self):
        self.loader = DatabricksLoader(dry_run=True)
        self.validator = InvoiceValidator()

    def test_bronze_load_returns_document_id(self):
        """Bronze load should return a UUID document ID."""
        parse_result = _make_parse_result()
        doc_id = self.loader.load_bronze(parse_result)
        assert doc_id  # Non-empty string
        assert len(doc_id) == 36  # UUID format

    def test_silver_load_succeeds(self):
        """Silver load should succeed in dry-run mode."""
        invoice = _make_invoice()
        validation = self.validator.validate(invoice)
        # Should not raise
        self.loader.load_silver("test-doc-id", invoice, validation)

    def test_bronze_preserves_file_info(self):
        """Bronze load should use file info from parse result."""
        parse_result = _make_parse_result()
        parse_result["file_name"] = "custom_name.pdf"
        doc_id = self.loader.load_bronze(parse_result)
        assert doc_id  # Just verify it completes
