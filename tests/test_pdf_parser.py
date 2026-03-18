"""
Tests for the PDF/document parser.

Verifies that the parser correctly reads files and returns
the expected structure (text, tables, metadata, file info).
"""

import os

from src.ingestion.pdf_parser import PDFParser


class TestPDFParser:
    """Tests for PDFParser class."""

    def setup_method(self):
        self.parser = PDFParser()
        self.samples_dir = os.path.join(
            os.path.dirname(__file__), "..", "data", "samples"
        )

    def test_parse_text_file_returns_content(self):
        """Parser should read text files and return non-empty text."""
        path = os.path.join(self.samples_dir, "sample_invoice_01.txt")
        result = self.parser.parse(path)

        assert result["text"], "Parsed text should not be empty"
        assert result["file_type"] == "txt"
        assert result["file_name"] == "sample_invoice_01.txt"
        assert result["file_size_bytes"] > 0

    def test_parse_returns_expected_keys(self):
        """Parser output should contain all required keys."""
        path = os.path.join(self.samples_dir, "sample_invoice_01.txt")
        result = self.parser.parse(path)

        expected_keys = {"text", "tables", "metadata", "file_name", "file_type", "file_size_bytes"}
        assert set(result.keys()) == expected_keys

    def test_parse_text_contains_invoice_data(self):
        """Parsed text should contain recognizable invoice content."""
        path = os.path.join(self.samples_dir, "sample_invoice_01.txt")
        result = self.parser.parse(path)

        assert "INV-2026-001" in result["text"]
        assert "Greenfield Office Supplies" in result["text"]

    def test_parse_nonexistent_file_raises_error(self):
        """Parser should raise FileNotFoundError for missing files."""
        import pytest
        with pytest.raises(FileNotFoundError):
            self.parser.parse("nonexistent_file.pdf")

    def test_parse_unsupported_format_raises_error(self):
        """Parser should raise ValueError for unsupported file types."""
        import pytest
        # Create a temp file with unsupported extension
        temp_path = os.path.join(self.samples_dir, "test.xyz")
        with open(temp_path, "w") as f:
            f.write("test")
        try:
            with pytest.raises(ValueError, match="Unsupported file type"):
                self.parser.parse(temp_path)
        finally:
            os.remove(temp_path)

    def test_parse_all_sample_invoices(self):
        """All sample invoices should parse without errors."""
        for i in range(1, 4):
            path = os.path.join(self.samples_dir, f"sample_invoice_{i:02d}.txt")
            result = self.parser.parse(path)
            assert result["text"], f"Invoice {i} should have text content"
