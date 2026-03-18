"""
Snowflake data loader for Bronze, Silver, and Gold layers.

Bronze layer: raw extracted data exactly as parsed from the document.
    - Preserves the original text and JSON for reprocessing.
    - Acts as the "source of truth" and audit trail.

Silver layer: cleaned and normalized data ready for transformation.
    - Standardized dates, phone numbers, addresses.
    - Validated and flagged with quality status.

Gold layer: validated invoice data in dimensional tables
    (dim_vendors, dim_documents, fact_invoice_line_items, fact_invoice_summary).

The loader handles:
    - Connection management (connect, execute, close)
    - Data transformation from Pydantic objects to SQL-ready rows
    - Upsert logic for dimension tables (don't duplicate vendors)
    - Batch inserts for fact tables

Supports dry-run mode for local development without Snowflake credentials.

Usage:
    loader = SnowflakeLoader()              # Uses .env credentials
    loader = SnowflakeLoader(dry_run=True)  # Local testing, no connection

    loader.load_bronze(parse_result)
    loader.load_silver(document_id, invoice_data, validation_result)
    loader.load_invoice(invoice_data, validation_result)
"""

import hashlib
import json
import os
import uuid
from datetime import datetime
from typing import Optional

import structlog

from src.extraction.schemas import InvoiceData
from src.validation.validators import ValidationResult, ValidationStatus

logger = structlog.get_logger()


class SnowflakeLoader:
    """
    Loads data into Snowflake Bronze, Silver, and Gold layer tables.

    In dry-run mode, logs what would be loaded without connecting
    to Snowflake. This lets you test the full pipeline locally.
    """

    def __init__(self, dry_run: bool = False):
        """
        Args:
            dry_run: If True, skip actual Snowflake operations and log instead.
                     Useful for local development and testing.
        """
        self._dry_run = dry_run
        self._connection = None

        if not dry_run:
            self._load_config()

    def _load_config(self):
        """Load Snowflake connection config from environment variables."""
        try:
            from dotenv import load_dotenv
            load_dotenv()
        except ImportError:
            pass

        self._config = {
            "account": os.environ.get("SNOWFLAKE_ACCOUNT", ""),
            "user": os.environ.get("SNOWFLAKE_USER", ""),
            "password": os.environ.get("SNOWFLAKE_PASSWORD", ""),
            "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
            "database": os.environ.get("SNOWFLAKE_DATABASE", "parsely"),
            "role": os.environ.get("SNOWFLAKE_ROLE", "parsely_pipeline_role"),
        }

    def connect(self):
        """Establish connection to Snowflake."""
        if self._dry_run:
            logger.info("snowflake_connect_dry_run")
            return

        import snowflake.connector

        self._connection = snowflake.connector.connect(
            account=self._config["account"],
            user=self._config["user"],
            password=self._config["password"],
            warehouse=self._config["warehouse"],
            database=self._config["database"],
            role=self._config["role"],
        )
        logger.info("snowflake_connected", database=self._config["database"])

    def close(self):
        """Close the Snowflake connection."""
        if self._connection:
            self._connection.close()
            self._connection = None
            logger.info("snowflake_disconnected")

    # ── Bronze Layer ─────────────────────────────────────

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

    # ── Silver Layer ─────────────────────────────────────

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

    # ── Gold Layer ───────────────────────────────────────

    def load_invoice(self, invoice: InvoiceData, validation: ValidationResult) -> dict:
        """
        Load a validated invoice into Snowflake Gold layer.

        Only loads invoices that passed validation or are flagged for review.
        Failed invoices are rejected.

        Args:
            invoice: Extracted and structured invoice data.
            validation: Validation result from InvoiceValidator.

        Returns:
            dict with load status and any generated IDs.
        """
        if validation.status == ValidationStatus.FAILED:
            logger.warning(
                "load_rejected",
                invoice_number=invoice.invoice_number,
                reason="validation_failed",
                errors=[e.message for e in validation.errors],
            )
            return {"status": "rejected", "reason": "validation_failed"}

        # Generate a unique document ID
        document_id = str(uuid.uuid4())

        # Generate a vendor ID (hash of name + address for deduplication)
        vendor_id = self._generate_vendor_id(invoice)

        logger.info(
            "loading_invoice",
            document_id=document_id,
            invoice_number=invoice.invoice_number,
            vendor=invoice.vendor.name,
        )

        # Load dimension tables first, then facts
        vendor_key = self._load_dim_vendor(invoice, vendor_id)
        document_key = self._load_dim_document(invoice, document_id, vendor_key)
        self._load_fact_line_items(invoice, document_key, vendor_key)
        self._load_fact_summary(invoice, document_key, vendor_key)

        logger.info(
            "invoice_loaded",
            document_id=document_id,
            invoice_number=invoice.invoice_number,
        )

        return {
            "status": "loaded",
            "document_id": document_id,
            "vendor_id": vendor_id,
        }

    def _generate_vendor_id(self, invoice: InvoiceData) -> str:
        """
        Generate a deterministic vendor ID from name + address.
        Same vendor always gets the same ID, enabling deduplication.
        """
        vendor = invoice.vendor
        key_string = f"{(vendor.name or '').lower().strip()}|{(vendor.address or '').lower().strip()}"
        return hashlib.sha256(key_string.encode()).hexdigest()[:16]

    def _load_dim_vendor(self, invoice: InvoiceData, vendor_id: str) -> Optional[int]:
        """
        Insert or update vendor in dim_vendors.
        Uses MERGE (upsert) to avoid duplicates.
        """
        vendor = invoice.vendor

        sql = """
            MERGE INTO gold.dim_vendors AS target
            USING (SELECT %(vendor_id)s AS vendor_id) AS source
            ON target.vendor_id = source.vendor_id
            WHEN MATCHED THEN UPDATE SET
                vendor_name = %(name)s,
                vendor_phone = %(phone)s,
                vendor_email = %(email)s,
                document_count = target.document_count + 1,
                updated_at = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN INSERT (
                vendor_id, vendor_name, vendor_address, vendor_city,
                vendor_state, vendor_zip, vendor_phone, vendor_email,
                first_seen_date, document_count
            ) VALUES (
                %(vendor_id)s, %(name)s, %(address)s, %(city)s,
                %(state)s, %(zip)s, %(phone)s, %(email)s,
                CURRENT_DATE(), 1
            )
        """

        params = {
            "vendor_id": vendor_id,
            "name": vendor.name,
            "address": vendor.address,
            "city": vendor.city,
            "state": vendor.state,
            "zip": vendor.zip,
            "phone": vendor.phone,
            "email": vendor.email,
        }

        return self._execute(sql, params, "dim_vendors")

    def _load_dim_document(self, invoice: InvoiceData, document_id: str,
                           vendor_key: Optional[int]) -> Optional[int]:
        """Insert document metadata into dim_documents."""
        sql = """
            INSERT INTO gold.dim_documents (
                document_id, vendor_key, document_type, file_name,
                invoice_number, invoice_date, due_date, po_number,
                subtotal, tax_amount, total_amount, currency,
                parse_confidence, upload_timestamp
            ) VALUES (
                %(document_id)s, %(vendor_key)s, %(doc_type)s, %(file_name)s,
                %(invoice_number)s, %(invoice_date)s, %(due_date)s, %(po_number)s,
                %(subtotal)s, %(tax_amount)s, %(total_amount)s, %(currency)s,
                %(confidence)s, CURRENT_TIMESTAMP()
            )
        """

        params = {
            "document_id": document_id,
            "vendor_key": vendor_key,
            "doc_type": "invoice",
            "file_name": None,  # Set by caller if available
            "invoice_number": invoice.invoice_number,
            "invoice_date": str(invoice.invoice_date) if invoice.invoice_date else None,
            "due_date": str(invoice.due_date) if invoice.due_date else None,
            "po_number": invoice.po_number,
            "subtotal": invoice.subtotal,
            "tax_amount": invoice.tax_amount,
            "total_amount": invoice.total_amount,
            "currency": invoice.currency,
            "confidence": invoice.parse_confidence,
        }

        return self._execute(sql, params, "dim_documents")

    def _load_fact_line_items(self, invoice: InvoiceData, document_key: Optional[int],
                              vendor_key: Optional[int]):
        """Insert all line items as fact rows."""
        if not invoice.line_items:
            return

        for i, item in enumerate(invoice.line_items, start=1):
            sql = """
                INSERT INTO gold.fact_invoice_line_items (
                    document_key, vendor_key, line_number,
                    description, quantity, unit_price, line_amount
                ) VALUES (
                    %(doc_key)s, %(vendor_key)s, %(line_num)s,
                    %(description)s, %(quantity)s, %(unit_price)s, %(amount)s
                )
            """

            params = {
                "doc_key": document_key,
                "vendor_key": vendor_key,
                "line_num": i,
                "description": item.description,
                "quantity": item.quantity,
                "unit_price": item.unit_price,
                "amount": item.amount,
            }

            self._execute(sql, params, f"fact_line_item_{i}")

    def _load_fact_summary(self, invoice: InvoiceData, document_key: Optional[int],
                           vendor_key: Optional[int]):
        """Insert aggregated invoice summary."""
        line_amounts = [item.amount for item in invoice.line_items if item.amount]
        avg_amount = round(sum(line_amounts) / len(line_amounts), 2) if line_amounts else None
        max_amount = max(line_amounts) if line_amounts else None

        sql = """
            INSERT INTO gold.fact_invoice_summary (
                document_key, vendor_key, total_line_items,
                subtotal, tax_amount, total_amount,
                avg_line_item_amount, max_line_item_amount, invoice_date
            ) VALUES (
                %(doc_key)s, %(vendor_key)s, %(line_count)s,
                %(subtotal)s, %(tax_amount)s, %(total_amount)s,
                %(avg_amount)s, %(max_amount)s, %(invoice_date)s
            )
        """

        params = {
            "doc_key": document_key,
            "vendor_key": vendor_key,
            "line_count": len(invoice.line_items),
            "subtotal": invoice.subtotal,
            "tax_amount": invoice.tax_amount,
            "total_amount": invoice.total_amount,
            "avg_amount": avg_amount,
            "max_amount": max_amount,
            "invoice_date": str(invoice.invoice_date) if invoice.invoice_date else None,
        }

        self._execute(sql, params, "fact_invoice_summary")

    def _execute(self, sql: str, params: dict, operation: str) -> Optional[int]:
        """
        Execute a SQL statement against Snowflake.
        In dry-run mode, logs the operation instead.
        """
        if self._dry_run:
            logger.info("dry_run_sql", operation=operation, params=params)
            return None

        cursor = self._connection.cursor()
        try:
            cursor.execute(sql, params)
            logger.debug("sql_executed", operation=operation, rows_affected=cursor.rowcount)
            return cursor.rowcount
        except Exception as e:
            logger.error("sql_failed", operation=operation, error=str(e))
            raise
        finally:
            cursor.close()
