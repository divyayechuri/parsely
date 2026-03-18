"""
Shared test fixtures for the Parsely test suite.

Fixtures provide reusable test data (sample file paths, expected outputs)
so individual test files don't need to duplicate setup logic.
"""

import json
import os

import pytest

SAMPLES_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "samples")


@pytest.fixture
def samples_dir():
    """Path to the sample data directory."""
    return SAMPLES_DIR


@pytest.fixture
def sample_invoice_text():
    """Raw text content of sample invoice 01."""
    path = os.path.join(SAMPLES_DIR, "sample_invoice_01.txt")
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


@pytest.fixture
def sample_invoice_expected():
    """Expected extraction output for sample invoice 01."""
    path = os.path.join(SAMPLES_DIR, "sample_invoice_01_expected.json")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture
def all_sample_invoices():
    """List of (text, expected) tuples for all sample invoices."""
    invoices = []
    i = 1
    while True:
        txt_path = os.path.join(SAMPLES_DIR, f"sample_invoice_{i:02d}.txt")
        json_path = os.path.join(SAMPLES_DIR, f"sample_invoice_{i:02d}_expected.json")
        if not os.path.exists(txt_path):
            break
        with open(txt_path, "r", encoding="utf-8") as f:
            text = f.read()
        with open(json_path, "r", encoding="utf-8") as f:
            expected = json.load(f)
        invoices.append((text, expected))
        i += 1
    return invoices
