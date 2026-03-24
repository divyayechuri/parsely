"""
Pipeline orchestrator for the Parsely invoice processing flow.

Chains together parsing, extraction, validation, and loading into a
single run/submit workflow. This is the programmatic entry point for
processing an invoice end-to-end without the Streamlit UI.

Usage:
    pipeline = Pipeline()
    result = pipeline.run(file_path="data/samples/sample_invoice_01.pdf")
    print(result.invoice.vendor.name)
    print(result.validation.status)

    # Submit to Snowflake (or dry-run)
    status = pipeline.submit(result)
"""

import os
import tempfile
from dataclasses import dataclass
from typing import Optional

import structlog

from src.extraction.field_extractor import FieldExtractor
from src.extraction.schemas import InvoiceData
from src.ingestion.pdf_parser import PDFParser
from src.loading.snowflake_loader import SnowflakeLoader
from src.validation.validators import InvoiceValidator, ValidationResult

logger = structlog.get_logger()


@dataclass
class PipelineResult:
    """Holds the combined output of parsing, extraction, and validation."""

    parse_result: dict
    invoice: InvoiceData
    validation: ValidationResult


class Pipeline:
    """
    Orchestrates the full invoice processing pipeline:
    parse -> extract -> validate -> load.

    Supports dry_run mode so the entire pipeline can be tested
    without Snowflake credentials.
    """

    def __init__(self, dry_run: bool = False):
        """
        Args:
            dry_run: If True, the submit step will use SnowflakeLoader
                     in dry-run mode (no actual database writes).
        """
        self._dry_run = dry_run
        self._parser = PDFParser()
        self._extractor = FieldExtractor()
        self._validator = InvoiceValidator()

    def run(
        self,
        file_path: Optional[str] = None,
        uploaded_file=None,
    ) -> PipelineResult:
        """
        Parse a document, extract fields, and validate.

        Provide exactly one of file_path or uploaded_file.

        Args:
            file_path: Path to a PDF or text file on disk.
            uploaded_file: A file-like object (e.g. Streamlit UploadedFile)
                           with .read()/.getvalue() and .name attributes.

        Returns:
            PipelineResult containing parse_result, invoice, and validation.

        Raises:
            ValueError: If neither or both sources are provided.
        """
        if file_path is None and uploaded_file is None:
            raise ValueError("Provide either file_path or uploaded_file")
        if file_path is not None and uploaded_file is not None:
            raise ValueError("Provide only one of file_path or uploaded_file, not both")

        # Step 1: Parse
        if uploaded_file is not None:
            suffix = os.path.splitext(uploaded_file.name)[1]
            with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
                data = (
                    uploaded_file.getvalue()
                    if hasattr(uploaded_file, "getvalue")
                    else uploaded_file.read()
                )
                tmp.write(data)
                tmp_path = tmp.name
            try:
                parse_result = self._parser.parse(tmp_path)
            finally:
                os.unlink(tmp_path)
        else:
            parse_result = self._parser.parse(file_path)

        logger.info("pipeline_parsed", file=parse_result.get("file_name"))

        # Step 2: Extract
        invoice = self._extractor.extract(parse_result["text"])
        logger.info(
            "pipeline_extracted",
            invoice_number=invoice.invoice_number,
            vendor=invoice.vendor.name,
        )

        # Step 3: Validate
        validation = self._validator.validate(invoice)
        logger.info(
            "pipeline_validated",
            status=validation.status.value,
        )

        return PipelineResult(
            parse_result=parse_result,
            invoice=invoice,
            validation=validation,
        )

    def submit(
        self,
        pipeline_result: PipelineResult,
        invoice_override: Optional[InvoiceData] = None,
    ) -> dict:
        """
        Load processed invoice data into Snowflake (Bronze, Silver, Gold).

        Args:
            pipeline_result: Output from run().
            invoice_override: Optional edited invoice to use instead of the
                              one produced by extraction (e.g. after user
                              edits in Streamlit).

        Returns:
            dict with keys: status, document_id, gold_result.
        """
        invoice = invoice_override if invoice_override is not None else pipeline_result.invoice
        validation = pipeline_result.validation
        parse_result = pipeline_result.parse_result

        loader = SnowflakeLoader(dry_run=self._dry_run)
        loader.connect()

        try:
            # Bronze — raw parse result
            document_id = loader.load_bronze(parse_result)
            logger.info("pipeline_bronze_loaded", document_id=document_id)

            # Silver — cleaned invoice + validation
            loader.load_silver(document_id, invoice, validation)
            logger.info("pipeline_silver_loaded", document_id=document_id)

            # Gold — dimensional model
            gold_result = loader.load_invoice(invoice, validation)
            logger.info("pipeline_gold_loaded", gold_result=gold_result)
        finally:
            loader.close()

        return {
            "status": "success",
            "document_id": document_id,
            "gold_result": gold_result,
        }
