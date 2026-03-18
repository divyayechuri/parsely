"""
Tests to verify that the generated PDF invoices are readable by pdfplumber
and contain expected invoice content.
"""

import os
import pdfplumber
import pytest


SAMPLES_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "data", "samples"
)

EXPECTED_PDFS = [
    "sample_invoice_01.pdf",
    "sample_invoice_02.pdf",
    "sample_invoice_03.pdf",
]


@pytest.mark.parametrize("filename", EXPECTED_PDFS)
def test_pdf_exists(filename):
    """Each expected PDF file should exist."""
    path = os.path.join(SAMPLES_DIR, filename)
    assert os.path.isfile(path), f"{filename} not found in {SAMPLES_DIR}"


@pytest.mark.parametrize("filename", EXPECTED_PDFS)
def test_pdf_readable_by_pdfplumber(filename):
    """pdfplumber should be able to open and extract text from each PDF."""
    path = os.path.join(SAMPLES_DIR, filename)
    with pdfplumber.open(path) as pdf:
        assert len(pdf.pages) >= 1
        text = pdf.pages[0].extract_text()
        assert text is not None
        assert len(text) > 100  # Should have meaningful content


@pytest.mark.parametrize(
    "filename,expected_strings",
    [
        (
            "sample_invoice_01.pdf",
            ["INVOICE", "INV-2026-001", "Greenfield Office Supplies", "Net 30"],
        ),
        (
            "sample_invoice_02.pdf",
            ["INVOICE", "INV-2026-002", "Summit IT Solutions", "Net 30"],
        ),
        (
            "sample_invoice_03.pdf",
            ["INVOICE", "INV-2026-003", "Precision Industrial Parts", "Net 30"],
        ),
    ],
)
def test_pdf_contains_expected_content(filename, expected_strings):
    """Each PDF should contain key invoice fields."""
    path = os.path.join(SAMPLES_DIR, filename)
    with pdfplumber.open(path) as pdf:
        text = pdf.pages[0].extract_text()
        for expected in expected_strings:
            assert expected in text, f"Expected '{expected}' in {filename}"
