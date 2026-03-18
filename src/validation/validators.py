"""
Data validation for extracted invoice data.

Validates extracted InvoiceData against business rules before loading
into the warehouse. Catches errors early — before bad data propagates
through the pipeline and corrupts downstream tables.

Validation rules are organized into three categories:
    1. Completeness — are required fields present?
    2. Accuracy — do the numbers add up?
    3. Consistency — are values in expected ranges/formats?

Each rule returns a ValidationResult with pass/fail status and a
human-readable message. The overall validation produces a summary
with a status of: passed, failed, or review_needed.

Usage:
    validator = InvoiceValidator()
    result = validator.validate(invoice_data)
    print(result.status)   # "passed" | "failed" | "review_needed"
    print(result.failures) # List of failed rules with explanations
"""

from dataclasses import dataclass, field
from datetime import date, timedelta
from enum import Enum
from typing import Optional

import structlog

from src.extraction.schemas import InvoiceData

logger = structlog.get_logger()


class ValidationStatus(str, Enum):
    """Overall validation outcome."""
    PASSED = "passed"
    FAILED = "failed"
    REVIEW_NEEDED = "review_needed"


class RuleSeverity(str, Enum):
    """
    How serious a validation failure is.
    - ERROR: Data cannot be loaded — must be fixed
    - WARNING: Data can be loaded but should be reviewed
    """
    ERROR = "error"
    WARNING = "warning"


@dataclass
class RuleResult:
    """Result of a single validation rule."""
    rule_name: str
    passed: bool
    severity: RuleSeverity
    message: str
    field_name: Optional[str] = None
    expected: Optional[str] = None
    actual: Optional[str] = None


@dataclass
class ValidationResult:
    """
    Overall validation result for an invoice.

    Contains the list of all rule results plus a summary status.
    The pipeline uses this to decide: load the data, reject it,
    or flag it for human review.
    """
    status: ValidationStatus
    rules: list[RuleResult] = field(default_factory=list)
    document_id: Optional[str] = None

    @property
    def failures(self) -> list[RuleResult]:
        """All rules that failed."""
        return [r for r in self.rules if not r.passed]

    @property
    def errors(self) -> list[RuleResult]:
        """Only ERROR-severity failures (block loading)."""
        return [r for r in self.rules if not r.passed and r.severity == RuleSeverity.ERROR]

    @property
    def warnings(self) -> list[RuleResult]:
        """Only WARNING-severity failures (allow loading with flag)."""
        return [r for r in self.rules if not r.passed and r.severity == RuleSeverity.WARNING]

    def summary(self) -> dict:
        """Summary dict for logging and display."""
        return {
            "status": self.status.value,
            "total_rules": len(self.rules),
            "passed": sum(1 for r in self.rules if r.passed),
            "errors": len(self.errors),
            "warnings": len(self.warnings),
        }


class InvoiceValidator:
    """
    Validates extracted invoice data against business rules.

    Runs all validation rules and produces a ValidationResult.
    Rules are methods prefixed with '_rule_' — adding a new rule
    is as simple as adding a new method.
    """

    # Fields that must be present for the invoice to be loadable
    REQUIRED_FIELDS = ["invoice_number", "vendor.name", "total_amount"]

    # Tolerance for floating-point comparison (1 cent)
    AMOUNT_TOLERANCE = 0.02

    # Maximum reasonable invoice age (invoices older than this are suspicious)
    MAX_INVOICE_AGE_DAYS = 365

    def validate(self, invoice: InvoiceData) -> ValidationResult:
        """
        Run all validation rules against an invoice.

        Args:
            invoice: Extracted invoice data to validate.

        Returns:
            ValidationResult with status and detailed rule results.
        """
        logger.info("validation_started", invoice_number=invoice.invoice_number)

        rules = [
            self._rule_required_fields(invoice),
            self._rule_invoice_number_format(invoice),
            self._rule_total_matches_line_items(invoice),
            self._rule_subtotal_matches_line_items(invoice),
            self._rule_tax_calculation(invoice),
            self._rule_dates_are_valid(invoice),
            self._rule_due_date_after_invoice_date(invoice),
            self._rule_amounts_are_positive(invoice),
            self._rule_line_items_have_amounts(invoice),
            self._rule_vendor_has_contact_info(invoice),
        ]

        # Flatten — some rules return lists
        flat_rules = []
        for r in rules:
            if isinstance(r, list):
                flat_rules.extend(r)
            else:
                flat_rules.append(r)

        # Determine overall status
        errors = [r for r in flat_rules if not r.passed and r.severity == RuleSeverity.ERROR]
        warnings = [r for r in flat_rules if not r.passed and r.severity == RuleSeverity.WARNING]

        if errors:
            status = ValidationStatus.FAILED
        elif warnings:
            status = ValidationStatus.REVIEW_NEEDED
        else:
            status = ValidationStatus.PASSED

        result = ValidationResult(status=status, rules=flat_rules)

        logger.info(
            "validation_completed",
            invoice_number=invoice.invoice_number,
            **result.summary(),
        )

        return result

    # ── Completeness Rules ────────────────────────────────────

    def _rule_required_fields(self, invoice: InvoiceData) -> list[RuleResult]:
        """Check that all required fields are present and non-empty."""
        results = []

        field_checks = {
            "invoice_number": invoice.invoice_number,
            "vendor.name": invoice.vendor.name,
            "total_amount": invoice.total_amount,
            "invoice_date": invoice.invoice_date,
        }

        for field_name, value in field_checks.items():
            is_required = field_name in self.REQUIRED_FIELDS
            passed = value is not None

            results.append(RuleResult(
                rule_name="required_field",
                passed=passed,
                severity=RuleSeverity.ERROR if is_required else RuleSeverity.WARNING,
                message=f"{'Required' if is_required else 'Expected'} field '{field_name}' is {'present' if passed else 'missing'}",
                field_name=field_name,
                actual="present" if passed else "missing",
            ))

        return results

    def _rule_invoice_number_format(self, invoice: InvoiceData) -> RuleResult:
        """Invoice number should match a reasonable format."""
        import re

        if not invoice.invoice_number:
            return RuleResult(
                rule_name="invoice_number_format",
                passed=True,  # Skip if missing (caught by required_fields)
                severity=RuleSeverity.WARNING,
                message="Invoice number not present, skipping format check",
            )

        # Should be alphanumeric with optional dashes/dots
        valid = bool(re.match(r"^[A-Za-z0-9\-\.\/]+$", invoice.invoice_number))

        return RuleResult(
            rule_name="invoice_number_format",
            passed=valid,
            severity=RuleSeverity.WARNING,
            message=f"Invoice number '{invoice.invoice_number}' {'matches' if valid else 'does not match'} expected format",
            field_name="invoice_number",
            actual=invoice.invoice_number,
        )

    # ── Accuracy Rules ────────────────────────────────────────

    def _rule_total_matches_line_items(self, invoice: InvoiceData) -> RuleResult:
        """
        Total should equal subtotal + tax.
        This is the most important accuracy check — if the total doesn't
        add up, either the extraction missed something or the document
        itself has an error.
        """
        if not invoice.total_amount or not invoice.subtotal:
            return RuleResult(
                rule_name="total_matches_components",
                passed=True,
                severity=RuleSeverity.WARNING,
                message="Cannot verify total (subtotal or total missing)",
            )

        tax = invoice.tax_amount or 0.0
        expected_total = round(invoice.subtotal + tax, 2)
        diff = abs(invoice.total_amount - expected_total)
        passed = diff <= self.AMOUNT_TOLERANCE

        return RuleResult(
            rule_name="total_matches_components",
            passed=passed,
            severity=RuleSeverity.ERROR,
            message=f"Total ({invoice.total_amount}) {'matches' if passed else 'does not match'} subtotal + tax ({expected_total})",
            field_name="total_amount",
            expected=str(expected_total),
            actual=str(invoice.total_amount),
        )

    def _rule_subtotal_matches_line_items(self, invoice: InvoiceData) -> RuleResult:
        """Subtotal should equal the sum of all line item amounts."""
        if not invoice.line_items or not invoice.subtotal:
            return RuleResult(
                rule_name="subtotal_matches_line_items",
                passed=True,
                severity=RuleSeverity.WARNING,
                message="Cannot verify subtotal (line items or subtotal missing)",
            )

        line_total = round(sum(item.amount for item in invoice.line_items if item.amount), 2)
        diff = abs(invoice.subtotal - line_total)
        passed = diff <= self.AMOUNT_TOLERANCE

        return RuleResult(
            rule_name="subtotal_matches_line_items",
            passed=passed,
            severity=RuleSeverity.ERROR,
            message=f"Subtotal ({invoice.subtotal}) {'matches' if passed else 'does not match'} sum of line items ({line_total})",
            field_name="subtotal",
            expected=str(line_total),
            actual=str(invoice.subtotal),
        )

    def _rule_tax_calculation(self, invoice: InvoiceData) -> RuleResult:
        """If tax rate and subtotal are known, verify the tax amount."""
        if not invoice.tax_rate or not invoice.subtotal or invoice.tax_amount is None:
            return RuleResult(
                rule_name="tax_calculation",
                passed=True,
                severity=RuleSeverity.WARNING,
                message="Cannot verify tax (rate, subtotal, or amount missing)",
            )

        expected_tax = round(invoice.subtotal * invoice.tax_rate, 2)
        diff = abs(invoice.tax_amount - expected_tax)
        passed = diff <= self.AMOUNT_TOLERANCE

        return RuleResult(
            rule_name="tax_calculation",
            passed=passed,
            severity=RuleSeverity.WARNING,
            message=f"Tax amount ({invoice.tax_amount}) {'matches' if passed else 'does not match'} expected ({expected_tax})",
            field_name="tax_amount",
            expected=str(expected_tax),
            actual=str(invoice.tax_amount),
        )

    # ── Consistency Rules ─────────────────────────────────────

    def _rule_dates_are_valid(self, invoice: InvoiceData) -> RuleResult:
        """Invoice date should be within a reasonable range."""
        if not invoice.invoice_date:
            return RuleResult(
                rule_name="date_range",
                passed=True,
                severity=RuleSeverity.WARNING,
                message="No invoice date to validate",
            )

        today = date.today()
        oldest_allowed = today - timedelta(days=self.MAX_INVOICE_AGE_DAYS)
        # Allow dates up to 30 days in the future (pre-dated invoices)
        newest_allowed = today + timedelta(days=30)

        passed = oldest_allowed <= invoice.invoice_date <= newest_allowed

        return RuleResult(
            rule_name="date_range",
            passed=passed,
            severity=RuleSeverity.WARNING,
            message=f"Invoice date {invoice.invoice_date} is {'within' if passed else 'outside'} expected range ({oldest_allowed} to {newest_allowed})",
            field_name="invoice_date",
            actual=str(invoice.invoice_date),
        )

    def _rule_due_date_after_invoice_date(self, invoice: InvoiceData) -> RuleResult:
        """Due date should be on or after the invoice date."""
        if not invoice.invoice_date or not invoice.due_date:
            return RuleResult(
                rule_name="due_date_order",
                passed=True,
                severity=RuleSeverity.WARNING,
                message="Cannot compare dates (one or both missing)",
            )

        passed = invoice.due_date >= invoice.invoice_date

        return RuleResult(
            rule_name="due_date_order",
            passed=passed,
            severity=RuleSeverity.WARNING,
            message=f"Due date ({invoice.due_date}) is {'on or after' if passed else 'before'} invoice date ({invoice.invoice_date})",
            field_name="due_date",
        )

    def _rule_amounts_are_positive(self, invoice: InvoiceData) -> RuleResult:
        """All monetary amounts should be positive."""
        amounts = {
            "subtotal": invoice.subtotal,
            "tax_amount": invoice.tax_amount,
            "total_amount": invoice.total_amount,
        }

        negatives = [name for name, val in amounts.items() if val is not None and val < 0]
        passed = len(negatives) == 0

        return RuleResult(
            rule_name="amounts_positive",
            passed=passed,
            severity=RuleSeverity.ERROR,
            message=f"All amounts are positive" if passed else f"Negative amounts found: {negatives}",
        )

    def _rule_line_items_have_amounts(self, invoice: InvoiceData) -> RuleResult:
        """Each line item should have a calculated amount."""
        if not invoice.line_items:
            return RuleResult(
                rule_name="line_items_complete",
                passed=True,
                severity=RuleSeverity.WARNING,
                message="No line items to validate",
            )

        missing = sum(1 for item in invoice.line_items if item.amount is None)
        passed = missing == 0

        return RuleResult(
            rule_name="line_items_complete",
            passed=passed,
            severity=RuleSeverity.WARNING,
            message=f"{'All' if passed else f'{missing} of'} {len(invoice.line_items)} line items have amounts",
            field_name="line_items",
        )

    def _rule_vendor_has_contact_info(self, invoice: InvoiceData) -> RuleResult:
        """Vendor should have at least a name and one contact method."""
        has_name = bool(invoice.vendor.name)
        has_contact = bool(invoice.vendor.phone or invoice.vendor.email)
        passed = has_name and has_contact

        return RuleResult(
            rule_name="vendor_contact_info",
            passed=passed,
            severity=RuleSeverity.WARNING,
            message=f"Vendor {'has' if passed else 'missing'} name and contact info",
            field_name="vendor",
        )
