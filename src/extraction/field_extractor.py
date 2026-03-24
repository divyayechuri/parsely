"""
Main field extraction orchestrator.

Coordinates regex extractors to produce a complete InvoiceData
object from raw document text. This is the single entry point that the
rest of the pipeline calls.

Strategy:
    1. Run regex extraction (fast, high precision for known patterns)
    2. Calculate confidence score based on field completion

Usage:
    extractor = FieldExtractor()
    invoice = extractor.extract("...raw invoice text...")
    print(invoice.vendor.name)       # "Greenfield Office Supplies Co."
    print(invoice.total_amount)      # 306.94
    print(invoice.parse_confidence)  # 0.95
"""

import structlog

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
from src.extraction.schemas import InvoiceData

logger = structlog.get_logger()


class FieldExtractor:
    """
    Orchestrates field extraction from invoice text.

    Uses regex-based extraction to identify and extract structured fields
    from invoice documents.
    """

    def __init__(self):
        pass

    def extract(self, text: str) -> InvoiceData:
        """
        Extract all fields from invoice text and return structured data.

        Args:
            text: Raw text extracted from a PDF or document.

        Returns:
            InvoiceData with all fields populated where extraction succeeded.
        """
        logger.info("extraction_started", text_length=len(text))

        # ── Step 1: Regex extraction ──────────────────────
        invoice_number = extract_invoice_number(text)
        invoice_date = extract_invoice_date(text)
        due_date = extract_due_date(text)
        po_number = extract_po_number(text)
        vendor = extract_vendor_info(text)
        bill_to = extract_bill_to_info(text)
        line_items = extract_line_items(text)
        subtotal = extract_subtotal(text)
        tax_rate, tax_amount = extract_tax(text)
        total = extract_total(text)

        # ── Step 2: Build the InvoiceData object ──────────
        invoice = InvoiceData(
            invoice_number=invoice_number,
            invoice_date=invoice_date,
            due_date=due_date,
            po_number=po_number,
            vendor=vendor,
            bill_to=bill_to,
            line_items=line_items,
            subtotal=subtotal,
            tax_rate=tax_rate,
            tax_amount=tax_amount,
            total_amount=total,
            raw_text=text,
        )

        # ── Step 3: Calculate confidence ──────────────────
        invoice.parse_confidence = self._calculate_confidence(invoice)

        logger.info(
            "extraction_completed",
            invoice_number=invoice.invoice_number,
            vendor_name=invoice.vendor.name,
            line_items_count=len(invoice.line_items),
            confidence=invoice.parse_confidence,
        )

        return invoice

    def _calculate_confidence(self, invoice: InvoiceData) -> float:
        """
        Calculate an overall confidence score (0.0 to 1.0).

        Weights each field by importance:
        - Invoice number, vendor name, total: high weight
        - Dates, line items: medium weight
        - Bill-to, PO number, tax: lower weight

        Also checks internal consistency (do line items sum to subtotal?).
        """
        scores = []

        # High importance fields (weight: 3)
        scores.extend([
            (3, 1.0 if invoice.invoice_number else 0.0),
            (3, 1.0 if invoice.vendor.name else 0.0),
            (3, 1.0 if invoice.total_amount else 0.0),
        ])

        # Medium importance fields (weight: 2)
        scores.extend([
            (2, 1.0 if invoice.invoice_date else 0.0),
            (2, 1.0 if invoice.due_date else 0.0),
            (2, 1.0 if len(invoice.line_items) > 0 else 0.0),
        ])

        # Lower importance fields (weight: 1)
        scores.extend([
            (1, 1.0 if invoice.bill_to.name else 0.0),
            (1, 1.0 if invoice.po_number else 0.0),
            (1, 1.0 if invoice.subtotal else 0.0),
            (1, 1.0 if invoice.tax_amount is not None else 0.0),
        ])

        # Consistency check: do line items sum to subtotal?
        if invoice.line_items and invoice.subtotal:
            line_total = sum(item.amount for item in invoice.line_items if item.amount)
            if abs(line_total - invoice.subtotal) < 0.02:  # Allow 1 cent rounding
                scores.append((2, 1.0))  # Bonus for consistency
            else:
                scores.append((2, 0.0))

        # Weighted average
        total_weight = sum(w for w, _ in scores)
        weighted_sum = sum(w * s for w, s in scores)

        return round(weighted_sum / total_weight, 2) if total_weight > 0 else 0.0
