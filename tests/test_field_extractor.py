"""
Tests for the main field extractor (orchestrator).

Verifies the end-to-end extraction pipeline: raw text → InvoiceData.
These are integration tests that exercise the regex extraction pipeline.
"""

import json
import os

from src.extraction.field_extractor import FieldExtractor
from src.extraction.schemas import InvoiceData

SAMPLES_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "samples")


def load_sample(invoice_num: int) -> tuple[str, dict]:
    txt_path = os.path.join(SAMPLES_DIR, f"sample_invoice_{invoice_num:02d}.txt")
    json_path = os.path.join(SAMPLES_DIR, f"sample_invoice_{invoice_num:02d}_expected.json")
    with open(txt_path, "r", encoding="utf-8") as f:
        text = f.read()
    with open(json_path, "r", encoding="utf-8") as f:
        expected = json.load(f)
    return text, expected


class TestFieldExtractor:
    """End-to-end extraction tests."""

    def setup_method(self):
        self.extractor = FieldExtractor()

    def test_returns_invoice_data(self):
        """Extractor should return an InvoiceData object."""
        text, _ = load_sample(1)
        result = self.extractor.extract(text)
        assert isinstance(result, InvoiceData)

    def test_invoice_number_extracted(self):
        text, expected = load_sample(1)
        result = self.extractor.extract(text)
        assert result.invoice_number == expected["invoice_number"]

    def test_vendor_name_extracted(self):
        text, expected = load_sample(1)
        result = self.extractor.extract(text)
        assert result.vendor.name == expected["vendor"]["name"]

    def test_total_amount_extracted(self):
        text, expected = load_sample(1)
        result = self.extractor.extract(text)
        assert result.total_amount == expected["total_amount"]

    def test_line_items_extracted(self):
        text, expected = load_sample(1)
        result = self.extractor.extract(text)
        assert len(result.line_items) == len(expected["line_items"])

    def test_confidence_is_high_for_complete_invoice(self):
        """A well-formatted invoice should get high confidence."""
        text, _ = load_sample(1)
        result = self.extractor.extract(text)
        assert result.parse_confidence >= 0.8

    def test_all_invoices_extract_successfully(self):
        """All sample invoices should extract without errors."""
        for i in range(1, 4):
            text, expected = load_sample(i)
            result = self.extractor.extract(text)
            assert result.invoice_number == expected["invoice_number"]
            assert result.total_amount == expected["total_amount"]
            assert result.parse_confidence > 0.0

    def test_field_completion_rate(self):
        """Field completion rate should reflect extraction success."""
        text, _ = load_sample(1)
        result = self.extractor.extract(text)
        rate = result.field_completion_rate()
        assert rate >= 0.75, f"Expected >= 75% completion, got {rate}"

    def test_empty_text_returns_low_confidence(self):
        """Empty or garbage text should get low confidence."""
        result = self.extractor.extract("")
        assert result.parse_confidence < 0.5

    def test_raw_text_is_preserved(self):
        """Original text should be stored in the result for reference."""
        text, _ = load_sample(1)
        result = self.extractor.extract(text)
        assert result.raw_text == text
