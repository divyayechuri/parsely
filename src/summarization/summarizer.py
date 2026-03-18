"""
Rule-based document summarizer (V1).

Generates a human-readable summary of a parsed invoice using
template-based logic. The summary gives the user a quick overview
of what was extracted without reading every field.

V2 will replace this with LLM-powered summarization (Claude API)
for richer, more natural summaries.

Usage:
    summarizer = InvoiceSummarizer()
    summary = summarizer.summarize(invoice_data)
    print(summary)
"""

from src.extraction.schemas import InvoiceData
from src.validation.validators import ValidationResult, ValidationStatus


class InvoiceSummarizer:
    """
    Generates summaries for parsed invoice data.

    V1 approach: template-based. Fills in a structured template
    with extracted values. Works well for invoices because they
    have a predictable set of fields.
    """

    def summarize(self, invoice: InvoiceData,
                  validation: ValidationResult = None) -> str:
        """
        Generate a summary string for an invoice.

        Args:
            invoice: Extracted invoice data.
            validation: Optional validation result to include status.

        Returns:
            Multi-line summary string.
        """
        sections = [
            self._header_section(invoice),
            self._vendor_section(invoice),
            self._financial_section(invoice),
            self._line_items_section(invoice),
        ]

        if validation:
            sections.append(self._validation_section(validation))

        sections.append(self._confidence_section(invoice))

        return "\n\n".join(section for section in sections if section)

    def summarize_brief(self, invoice: InvoiceData) -> str:
        """
        Generate a one-line brief summary.
        Useful for log messages and list views.
        """
        parts = []

        if invoice.invoice_number:
            parts.append(f"Invoice {invoice.invoice_number}")

        if invoice.vendor.name:
            parts.append(f"from {invoice.vendor.name}")

        if invoice.total_amount is not None:
            parts.append(f"for ${invoice.total_amount:,.2f}")

        if invoice.invoice_date:
            parts.append(f"dated {invoice.invoice_date.strftime('%b %d, %Y')}")

        return " ".join(parts) if parts else "Invoice details not available"

    def _header_section(self, invoice: InvoiceData) -> str:
        """Invoice identification section."""
        lines = ["DOCUMENT SUMMARY", "=" * 40]  # ASCII only for Windows compat

        if invoice.invoice_number:
            lines.append(f"Invoice Number:  {invoice.invoice_number}")
        if invoice.invoice_date:
            lines.append(f"Invoice Date:    {invoice.invoice_date.strftime('%B %d, %Y')}")
        if invoice.due_date:
            lines.append(f"Due Date:        {invoice.due_date.strftime('%B %d, %Y')}")

            # Calculate days until due
            if invoice.invoice_date:
                days = (invoice.due_date - invoice.invoice_date).days
                lines.append(f"Payment Terms:   Net {days}")

        if invoice.po_number:
            lines.append(f"PO Reference:    {invoice.po_number}")

        return "\n".join(lines)

    def _vendor_section(self, invoice: InvoiceData) -> str:
        """Vendor information section."""
        vendor = invoice.vendor
        if not vendor.name:
            return ""

        lines = ["VENDOR", "-" * 40]
        lines.append(f"Name:    {vendor.name}")

        address_parts = []
        if vendor.address:
            address_parts.append(vendor.address)
        city_state = []
        if vendor.city:
            city_state.append(vendor.city)
        if vendor.state:
            city_state.append(vendor.state)
        if city_state:
            cs = ", ".join(city_state)
            if vendor.zip:
                cs += f" {vendor.zip}"
            address_parts.append(cs)
        if address_parts:
            lines.append(f"Address: {', '.join(address_parts)}")

        if vendor.phone:
            lines.append(f"Phone:   {vendor.phone}")
        if vendor.email:
            lines.append(f"Email:   {vendor.email}")

        return "\n".join(lines)

    def _financial_section(self, invoice: InvoiceData) -> str:
        """Financial summary section."""
        lines = ["FINANCIAL SUMMARY", "-" * 40]

        if invoice.subtotal is not None:
            lines.append(f"Subtotal:    ${invoice.subtotal:>12,.2f}")
        if invoice.tax_amount is not None:
            tax_info = f"Tax:         ${invoice.tax_amount:>12,.2f}"
            if invoice.tax_rate is not None:
                tax_info += f"  ({invoice.tax_rate * 100:.2f}%)"
            lines.append(tax_info)
        if invoice.total_amount is not None:
            lines.append(f"{'=' * 30}")
            lines.append(f"TOTAL:       ${invoice.total_amount:>12,.2f}  {invoice.currency}")

        return "\n".join(lines)

    def _line_items_section(self, invoice: InvoiceData) -> str:
        """Line items summary section."""
        if not invoice.line_items:
            return ""

        lines = [f"LINE ITEMS ({len(invoice.line_items)} items)", "-" * 40]

        for i, item in enumerate(invoice.line_items, start=1):
            desc = item.description or "Unknown item"
            if item.quantity and item.amount:
                lines.append(
                    f"  {i}. {desc} — Qty: {item.quantity:g} x "
                    f"${item.unit_price:,.2f} = ${item.amount:,.2f}"
                )
            elif item.amount:
                lines.append(f"  {i}. {desc} — ${item.amount:,.2f}")
            else:
                lines.append(f"  {i}. {desc}")

        # Highest value item
        amounts = [item.amount for item in invoice.line_items if item.amount]
        if amounts:
            max_item = max(invoice.line_items, key=lambda x: x.amount or 0)
            lines.append(f"\n  Highest value item: {max_item.description} (${max_item.amount:,.2f})")

        return "\n".join(lines)

    def _validation_section(self, validation: ValidationResult) -> str:
        """Validation status section."""
        status_labels = {
            ValidationStatus.PASSED: "PASSED - All checks OK",
            ValidationStatus.FAILED: "FAILED - Errors found",
            ValidationStatus.REVIEW_NEEDED: "REVIEW NEEDED - Warnings present",
        }

        lines = ["DATA QUALITY", "-" * 40]
        lines.append(f"Status: {status_labels.get(validation.status, validation.status)}")

        summary = validation.summary()
        lines.append(f"Rules checked: {summary['total_rules']}")
        lines.append(f"Passed: {summary['passed']}")

        if validation.errors:
            lines.append(f"\nErrors ({len(validation.errors)}):")
            for err in validation.errors:
                lines.append(f"  - {err.message}")

        if validation.warnings:
            lines.append(f"\nWarnings ({len(validation.warnings)}):")
            for warn in validation.warnings:
                lines.append(f"  - {warn.message}")

        return "\n".join(lines)

    def _confidence_section(self, invoice: InvoiceData) -> str:
        """Extraction confidence section."""
        confidence = invoice.parse_confidence
        completion = invoice.field_completion_rate()

        if confidence >= 0.9:
            quality = "High"
        elif confidence >= 0.7:
            quality = "Medium"
        else:
            quality = "Low"

        lines = [
            "EXTRACTION QUALITY",
            "-" * 40,
            f"Confidence: {confidence:.0%} ({quality})",
            f"Key fields extracted: {completion:.0%}",
        ]

        return "\n".join(lines)
