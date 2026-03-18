"""
Pydantic schemas for structured invoice data.

These schemas serve as the data contract between the extraction layer
and everything downstream (validation, loading, dbt, Streamlit).
Pydantic enforces types at runtime — if a field is missing or wrong,
it raises a clear error immediately rather than failing silently.
"""

from datetime import date
from typing import Optional

from pydantic import BaseModel, Field, model_validator


class VendorInfo(BaseModel):
    """Vendor/supplier details extracted from the invoice header."""

    name: Optional[str] = Field(None, description="Company or organization name")
    address: Optional[str] = Field(None, description="Street address")
    city: Optional[str] = Field(None, description="City")
    state: Optional[str] = Field(None, description="State abbreviation")
    zip: Optional[str] = Field(None, description="ZIP or postal code")
    phone: Optional[str] = Field(None, description="Phone number")
    email: Optional[str] = Field(None, description="Email address")


class BillToInfo(BaseModel):
    """Bill-to / recipient details."""

    name: Optional[str] = Field(None, description="Recipient organization name")
    address: Optional[str] = Field(None, description="Street address")
    city: Optional[str] = Field(None, description="City")
    state: Optional[str] = Field(None, description="State abbreviation")
    zip: Optional[str] = Field(None, description="ZIP or postal code")


class LineItem(BaseModel):
    """A single line item from the invoice table."""

    description: Optional[str] = Field(None, description="Item description")
    quantity: Optional[float] = Field(None, ge=0, description="Quantity ordered")
    unit_price: Optional[float] = Field(None, ge=0, description="Price per unit")
    amount: Optional[float] = Field(None, ge=0, description="Line total (qty * unit_price)")

    @model_validator(mode="after")
    def calculate_amount_if_missing(self):
        """If amount is missing but qty and unit_price exist, calculate it."""
        if self.amount is None and self.quantity is not None and self.unit_price is not None:
            self.amount = round(self.quantity * self.unit_price, 2)
        return self


class InvoiceData(BaseModel):
    """
    Complete structured data extracted from a single invoice.

    This is the primary output of the extraction pipeline. Every field
    is Optional because extraction may fail for some fields — downstream
    validation decides which missing fields are acceptable.
    """

    # Document identifiers
    invoice_number: Optional[str] = Field(None, description="Invoice or document number")
    invoice_date: Optional[date] = Field(None, description="Date the invoice was issued")
    due_date: Optional[date] = Field(None, description="Payment due date")
    po_number: Optional[str] = Field(None, description="Purchase order reference number")

    # Parties
    vendor: VendorInfo = Field(default_factory=VendorInfo)
    bill_to: BillToInfo = Field(default_factory=BillToInfo)

    # Financial data
    line_items: list[LineItem] = Field(default_factory=list)
    subtotal: Optional[float] = Field(None, ge=0)
    tax_rate: Optional[float] = Field(None, ge=0, le=1, description="Tax rate as decimal (e.g., 0.08)")
    tax_amount: Optional[float] = Field(None, ge=0)
    total_amount: Optional[float] = Field(None, ge=0)
    currency: str = Field("USD", description="ISO 4217 currency code")

    # Metadata
    parse_confidence: float = Field(0.0, ge=0.0, le=1.0, description="Overall extraction confidence")
    raw_text: Optional[str] = Field(None, description="Original text used for extraction")

    def field_completion_rate(self) -> float:
        """
        Calculate what percentage of key fields were successfully extracted.
        Used as a simple measure of extraction quality.
        """
        key_fields = [
            self.invoice_number,
            self.invoice_date,
            self.vendor.name,
            self.total_amount,
        ]
        filled = sum(1 for f in key_fields if f is not None)
        return filled / len(key_fields)
