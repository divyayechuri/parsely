"""
Databricks data loader for Bronze and Silver layers.

Bronze layer: raw extracted data exactly as parsed from the document.
    - Preserves the original text and JSON for reprocessing.
    - Acts as the "source of truth" and audit trail.

Silver layer: cleaned and normalized data ready for transformation.
    - Standardized dates, phone numbers, addresses.
    - Validated and flagged with quality status.

Uses Databricks SQL connector for Community Edition compatibility.
Supports dry-run mode for local development.

Usage:
    loader = DatabricksLoader()              # Uses .env credentials
    loader = DatabricksLoader(dry_run=True)  # Local testing

    loader.load_bronze(document_id, parse_result, invoice_data)
    loader.load_silver(document_id, invoice_data, validation_result)
"""

import json
import os
import uuid
from datetime import datetime
from typing import Optional

import structlog

from src.extraction.schemas import InvoiceData
from src.validation.validators import ValidationResult

logger = structlog.get_logger()


class DatabricksLoader:
    """
    Loads data into Databricks Bronze and Silver layers.

    Bronze: raw data (text, JSON, metadata) — stored immediately after parsing
    Silver: cleaned data — stored after extraction and validation
    """

    def __init__(self, dry_run: bool = False):
        """
        Args:
            dry_run: If True, log operations without connecting to Databricks.
        """
        self._dry_run = dry_run
        self._connection = None

        if not dry_run:
            self._load_config()

    def _load_config(self):
        """Load Databricks connection config from environment variables."""
        try:
            from dotenv import load_dotenv
            load_dotenv()
        except ImportError:
            pass

        self._config = {
            "host": os.environ.get("DATABRICKS_HOST", ""),
            "token": os.environ.get("DATABRICKS_TOKEN", ""),
        }

    def connect(self):
        """Establish connection to Databricks."""
        if self._dry_run:
            logger.info("databricks_connect_dry_run")
            return

        from databricks import sql as databricks_sql

        self._connection = databricks_sql.connect(
            server_hostname=self._config["host"],
            http_path="/sql/1.0/warehouses/default",
            access_token=self._config["token"],
        )
        logger.info("databricks_connected", host=self._config["host"])

    def close(self):
        """Close the Databricks connection."""
        if self._connection:
            self._connection.close()
            self._connection = None
            logger.info("databricks_disconnected")

    def load_bronze(self, parse_result: dict, invoice: Optional[InvoiceData] = None) -> str:
        """
        Load raw parsed data into the Bronze layer.

        Called immediately after document parsing, before extraction.
        Stores the raw text and tables so we can reprocess if needed.

        Args:
            parse_result: Output from PDFParser.parse() containing
                          text, tables, metadata, file info.
            invoice: Optional InvoiceData if extraction already happened.

        Returns:
            document_id: UUID assigned to this document.
        """
        document_id = str(uuid.uuid4())

        sql = """
            INSERT INTO bronze.raw_documents (
                document_id, file_name, file_type, file_size_bytes,
                raw_text, raw_tables_json, metadata_json,
                ingestion_timestamp, source_path
            ) VALUES (
                %(document_id)s, %(file_name)s, %(file_type)s, %(file_size)s,
                %(raw_text)s, %(tables_json)s, %(metadata_json)s,
                CURRENT_TIMESTAMP(), %(source_path)s
            )
        """

        params = {
            "document_id": document_id,
            "file_name": parse_result.get("file_name", "unknown"),
            "file_type": parse_result.get("file_type", "unknown"),
            "file_size": parse_result.get("file_size_bytes", 0),
            "raw_text": parse_result.get("text", ""),
            "tables_json": json.dumps(parse_result.get("tables", []), default=str),
            "metadata_json": json.dumps(parse_result.get("metadata", {}), default=str),
            "source_path": parse_result.get("source_path"),
        }

        self._execute(sql, params, "bronze.raw_documents")

        logger.info(
            "bronze_loaded",
            document_id=document_id,
            file_name=parse_result.get("file_name"),
        )

        return document_id

    def load_silver(self, document_id: str, invoice: InvoiceData,
                    validation: ValidationResult) -> None:
        """
        Load cleaned and validated data into the Silver layer.

        Called after extraction and validation. Stores the structured
        invoice data with normalized fields and validation status.

        Args:
            document_id: UUID from the Bronze layer load.
            invoice: Extracted and structured invoice data.
            validation: Validation result with status.
        """
        # ── Load parsed_invoices (header data) ────────────────
        invoice_sql = """
            INSERT INTO silver.parsed_invoices (
                document_id, vendor_name, vendor_address, vendor_city,
                vendor_state, vendor_zip, vendor_phone, vendor_email,
                invoice_number, invoice_date, due_date, po_number,
                subtotal, tax_amount, total_amount, currency,
                parse_confidence, validation_status, parse_timestamp
            ) VALUES (
                %(document_id)s, %(vendor_name)s, %(vendor_address)s, %(vendor_city)s,
                %(vendor_state)s, %(vendor_zip)s, %(vendor_phone)s, %(vendor_email)s,
                %(invoice_number)s, %(invoice_date)s, %(due_date)s, %(po_number)s,
                %(subtotal)s, %(tax_amount)s, %(total_amount)s, %(currency)s,
                %(confidence)s, %(validation_status)s, CURRENT_TIMESTAMP()
            )
        """

        invoice_params = {
            "document_id": document_id,
            "vendor_name": invoice.vendor.name,
            "vendor_address": invoice.vendor.address,
            "vendor_city": invoice.vendor.city,
            "vendor_state": invoice.vendor.state,
            "vendor_zip": invoice.vendor.zip,
            "vendor_phone": invoice.vendor.phone,
            "vendor_email": invoice.vendor.email,
            "invoice_number": invoice.invoice_number,
            "invoice_date": str(invoice.invoice_date) if invoice.invoice_date else None,
            "due_date": str(invoice.due_date) if invoice.due_date else None,
            "po_number": invoice.po_number,
            "subtotal": invoice.subtotal,
            "tax_amount": invoice.tax_amount,
            "total_amount": invoice.total_amount,
            "currency": invoice.currency,
            "confidence": invoice.parse_confidence,
            "validation_status": validation.status.value,
        }

        self._execute(invoice_sql, invoice_params, "silver.parsed_invoices")

        # ── Load parsed_line_items ────────────────────────────
        for i, item in enumerate(invoice.line_items, start=1):
            line_sql = """
                INSERT INTO silver.parsed_line_items (
                    line_item_id, document_id, line_number,
                    description, quantity, unit_price, line_amount,
                    extraction_confidence
                ) VALUES (
                    %(line_item_id)s, %(document_id)s, %(line_number)s,
                    %(description)s, %(quantity)s, %(unit_price)s, %(amount)s,
                    %(confidence)s
                )
            """

            line_params = {
                "line_item_id": str(uuid.uuid4()),
                "document_id": document_id,
                "line_number": i,
                "description": item.description,
                "quantity": item.quantity,
                "unit_price": item.unit_price,
                "amount": item.amount,
                "confidence": invoice.parse_confidence,
            }

            self._execute(line_sql, line_params, f"silver.parsed_line_items[{i}]")

        logger.info(
            "silver_loaded",
            document_id=document_id,
            invoice_number=invoice.invoice_number,
            line_items=len(invoice.line_items),
            validation_status=validation.status.value,
        )

    def _execute(self, sql: str, params: dict, operation: str):
        """Execute SQL against Databricks or log in dry-run mode."""
        if self._dry_run:
            logger.info("dry_run_sql", operation=operation, params=params)
            return

        cursor = self._connection.cursor()
        try:
            cursor.execute(sql, params)
            logger.debug("sql_executed", operation=operation)
        except Exception as e:
            logger.error("sql_failed", operation=operation, error=str(e))
            raise
        finally:
            cursor.close()
