"""
PDF document parser.

Reads PDF files using pdfplumber, extracts raw text and table data,
and returns them for downstream field extraction. Also handles plain
text files for development and testing.

pdfplumber was chosen over PyPDF2 because it handles complex table
layouts better — it can identify table boundaries and extract rows
and columns, which is critical for parsing invoice line items.

Usage:
    parser = PDFParser()
    result = parser.parse("data/samples/sample_invoice_01.pdf")
    print(result["text"])    # Full extracted text
    print(result["tables"])  # List of extracted tables
"""

import json
import os
from typing import Optional

import structlog

logger = structlog.get_logger()


class PDFParser:
    """
    Parses PDF and text documents, extracting raw text and tables.

    Supports:
    - .pdf files via pdfplumber
    - .txt files (direct read — used for development and testing)
    """

    def parse(self, file_path: str) -> dict:
        """
        Parse a document and return extracted content.

        Args:
            file_path: Path to the PDF or text file.

        Returns:
            dict with keys:
                - text: Full extracted text (str)
                - tables: List of tables, each as list of rows (list[list[list[str]]])
                - metadata: File metadata (dict)
                - file_name: Original file name (str)
                - file_type: File extension (str)
                - file_size_bytes: File size in bytes (int)
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Document not found: {file_path}")

        file_ext = os.path.splitext(file_path)[1].lower()
        file_name = os.path.basename(file_path)
        file_size = os.path.getsize(file_path)

        logger.info("parsing_document", file=file_name, type=file_ext, size=file_size)

        if file_ext == ".pdf":
            text, tables, metadata = self._parse_pdf(file_path)
        elif file_ext == ".txt":
            text, tables, metadata = self._parse_text(file_path)
        else:
            raise ValueError(f"Unsupported file type: {file_ext}. Supported: .pdf, .txt")

        result = {
            "text": text,
            "tables": tables,
            "metadata": metadata,
            "file_name": file_name,
            "file_type": file_ext.lstrip("."),
            "file_size_bytes": file_size,
        }

        logger.info(
            "parsing_completed",
            file=file_name,
            text_length=len(text),
            tables_found=len(tables),
        )

        return result

    def _parse_pdf(self, file_path: str) -> tuple[str, list, dict]:
        """
        Extract text and tables from a PDF using pdfplumber.

        pdfplumber works by analyzing the positioning of characters
        on each page to reconstruct text lines and table structures.
        """
        try:
            import pdfplumber
        except ImportError:
            raise ImportError(
                "pdfplumber is required for PDF parsing. "
                "Install it with: pip install pdfplumber"
            )

        text_parts = []
        all_tables = []
        metadata = {}

        with pdfplumber.open(file_path) as pdf:
            # Extract PDF metadata (author, creation date, etc.)
            metadata = pdf.metadata or {}

            for page_num, page in enumerate(pdf.pages, start=1):
                # Extract text
                page_text = page.extract_text()
                if page_text:
                    text_parts.append(page_text)

                # Extract tables
                page_tables = page.extract_tables()
                if page_tables:
                    for table in page_tables:
                        all_tables.append({
                            "page": page_num,
                            "data": table,
                        })

                logger.debug(
                    "page_parsed",
                    page=page_num,
                    text_length=len(page_text) if page_text else 0,
                    tables=len(page_tables) if page_tables else 0,
                )

        full_text = "\n\n".join(text_parts)
        return full_text, all_tables, metadata

    def _parse_text(self, file_path: str) -> tuple[str, list, dict]:
        """
        Read a plain text file. Used for development and testing
        with sample invoice text files.
        """
        with open(file_path, "r", encoding="utf-8") as f:
            text = f.read()

        metadata = {
            "source": "text_file",
            "encoding": "utf-8",
        }

        return text, [], metadata

    def parse_to_json(self, file_path: str) -> str:
        """
        Parse a document and return results as a JSON string.
        Useful for loading into Bronze layer or debugging.
        """
        result = self.parse(file_path)
        # Remove raw text from JSON output to keep it compact
        result_for_json = {k: v for k, v in result.items() if k != "text"}
        result_for_json["text_length"] = len(result.get("text", ""))
        return json.dumps(result_for_json, indent=2, default=str)
