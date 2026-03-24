"""
Tests for the Pipeline orchestrator.

Verifies the run/submit flow using sample invoice files and dry-run mode.
"""

import os
from datetime import date
from io import BytesIO
from unittest.mock import MagicMock

import pytest

from src.extraction.schemas import InvoiceData, LineItem, VendorInfo
from src.pipeline import Pipeline, PipelineResult
from src.validation.validators import ValidationResult, ValidationStatus


SAMPLES_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "samples")


# ── Helpers ──────────────────────────────────────────────


def _sample_txt_path():
    """Return path to sample_invoice_01.txt."""
    return os.path.join(SAMPLES_DIR, "sample_invoice_01.txt")


def _make_invoice() -> InvoiceData:
    """Create a minimal test invoice."""
    return InvoiceData(
        invoice_number="INV-PIPE-001",
        invoice_date=date(2026, 1, 15),
        due_date=date(2026, 2, 14),
        vendor=VendorInfo(
            name="Pipeline Vendor LLC",
            address="1 Pipeline Way",
            city="Austin",
            state="TX",
            zip="78701",
            phone="(555) 999-0000",
            email="pipe@vendor.example.com",
        ),
        line_items=[
            LineItem(description="Widget", quantity=3, unit_price=10.00, amount=30.00),
        ],
        subtotal=30.00,
        tax_rate=0.08,
        tax_amount=2.40,
        total_amount=32.40,
        parse_confidence=0.90,
    )


def _make_pipeline_result() -> PipelineResult:
    """Create a PipelineResult for submit tests."""
    invoice = _make_invoice()
    from src.validation.validators import InvoiceValidator

    validation = InvoiceValidator().validate(invoice)
    parse_result = {
        "text": "INVOICE\nPipeline Vendor LLC\n...",
        "tables": [],
        "metadata": {"source": "test"},
        "file_name": "test_pipeline.txt",
        "file_type": "txt",
        "file_size_bytes": 100,
    }
    return PipelineResult(
        parse_result=parse_result,
        invoice=invoice,
        validation=validation,
    )


# ── PipelineResult dataclass ────────────────────────────


class TestPipelineResult:
    def test_holds_all_fields(self):
        result = _make_pipeline_result()
        assert result.parse_result is not None
        assert isinstance(result.invoice, InvoiceData)
        assert isinstance(result.validation, ValidationResult)

    def test_invoice_accessible(self):
        result = _make_pipeline_result()
        assert result.invoice.invoice_number == "INV-PIPE-001"
        assert result.invoice.vendor.name == "Pipeline Vendor LLC"


# ── Pipeline.run() ──────────────────────────────────────


class TestPipelineRun:
    def test_run_with_file_path(self):
        pipeline = Pipeline(dry_run=True)
        result = pipeline.run(file_path=_sample_txt_path())

        assert isinstance(result, PipelineResult)
        assert result.parse_result["text"]
        assert result.invoice is not None
        assert result.validation is not None

    def test_run_extracts_invoice_number(self):
        pipeline = Pipeline(dry_run=True)
        result = pipeline.run(file_path=_sample_txt_path())

        # Sample invoice 01 should have an invoice number
        assert result.invoice.invoice_number is not None

    def test_run_validates(self):
        pipeline = Pipeline(dry_run=True)
        result = pipeline.run(file_path=_sample_txt_path())

        assert result.validation.status in (
            ValidationStatus.PASSED,
            ValidationStatus.REVIEW_NEEDED,
            ValidationStatus.FAILED,
        )
        assert len(result.validation.rules) > 0

    def test_run_with_uploaded_file(self):
        # Simulate a Streamlit-like uploaded file object
        txt_path = _sample_txt_path()
        with open(txt_path, "rb") as f:
            content = f.read()

        uploaded = MagicMock()
        uploaded.name = "sample_invoice_01.txt"
        uploaded.getvalue.return_value = content

        pipeline = Pipeline(dry_run=True)
        result = pipeline.run(uploaded_file=uploaded)

        assert isinstance(result, PipelineResult)
        assert result.invoice is not None

    def test_run_no_source_raises(self):
        pipeline = Pipeline(dry_run=True)
        with pytest.raises(ValueError, match="Provide either"):
            pipeline.run()

    def test_run_both_sources_raises(self):
        uploaded = MagicMock()
        uploaded.name = "test.txt"

        pipeline = Pipeline(dry_run=True)
        with pytest.raises(ValueError, match="only one"):
            pipeline.run(file_path=_sample_txt_path(), uploaded_file=uploaded)

    def test_run_missing_file_raises(self):
        pipeline = Pipeline(dry_run=True)
        with pytest.raises(FileNotFoundError):
            pipeline.run(file_path="/nonexistent/path/to/invoice.txt")


# ── Pipeline.submit() ──────────────────────────────────


class TestPipelineSubmit:
    def test_submit_dry_run(self):
        pipeline = Pipeline(dry_run=True)
        pr = _make_pipeline_result()

        status = pipeline.submit(pr)

        assert status["status"] == "success"
        assert "document_id" in status
        assert "gold_result" in status

    def test_submit_with_invoice_override(self):
        pipeline = Pipeline(dry_run=True)
        pr = _make_pipeline_result()

        override = _make_invoice()
        override.invoice_number = "OVERRIDE-001"

        status = pipeline.submit(pr, invoice_override=override)

        assert status["status"] == "success"

    def test_submit_uses_original_when_no_override(self):
        pipeline = Pipeline(dry_run=True)
        pr = _make_pipeline_result()

        # No override — should use the original invoice from pipeline_result
        status = pipeline.submit(pr)
        assert status["status"] == "success"

    def test_dry_run_flag_propagates(self):
        pipeline = Pipeline(dry_run=True)
        assert pipeline._dry_run is True

        pipeline2 = Pipeline(dry_run=False)
        assert pipeline2._dry_run is False


# ── End-to-end (run + submit, dry-run) ──────────────────


class TestPipelineEndToEnd:
    def test_run_then_submit(self):
        pipeline = Pipeline(dry_run=True)
        result = pipeline.run(file_path=_sample_txt_path())
        status = pipeline.submit(result)

        assert status["status"] == "success"
        assert status["document_id"]
