"""
spaCy-based Named Entity Recognition (NER) for invoice extraction.

spaCy's pre-trained models can identify entities like organizations (ORG),
dates (DATE), monetary amounts (MONEY), and locations in free text.

This module complements the regex extractor — regex handles structured
patterns (invoice numbers, phone numbers), while NER handles entities
that may appear in unpredictable formats (company names, dates written
in natural language).

Usage:
    extractor = NERExtractor()
    entities = extractor.extract_entities("Invoice from Acme Corp dated Jan 5, 2026")
    # entities = {"ORG": ["Acme Corp"], "DATE": ["Jan 5, 2026"], ...}
"""

from typing import Optional

import structlog

logger = structlog.get_logger()


class NERExtractor:
    """
    Extracts named entities from document text using spaCy.

    Loads the model once on initialization and reuses it for all
    subsequent extractions — avoids the cost of loading the model
    on every function call.
    """

    def __init__(self, model_name: str = "en_core_web_sm"):
        """
        Initialize with a spaCy model.

        Args:
            model_name: spaCy model to load. 'en_core_web_sm' is a small,
                        fast English model suitable for basic NER.
        """
        self._nlp = None
        self._model_name = model_name

    def _load_model(self):
        """Lazy-load the spaCy model on first use."""
        if self._nlp is None:
            try:
                import spacy
                self._nlp = spacy.load(self._model_name)
                logger.info("spacy_model_loaded", model=self._model_name)
            except OSError:
                logger.warning(
                    "spacy_model_not_found",
                    model=self._model_name,
                    hint="Run: python -m spacy download en_core_web_sm",
                )
                raise

    def extract_entities(self, text: str) -> dict[str, list[str]]:
        """
        Extract all named entities from text, grouped by entity type.

        Returns a dict like:
            {
                "ORG": ["Greenfield Office Supplies Co."],
                "DATE": ["2026-01-15", "2026-02-14"],
                "MONEY": ["$284.20", "$306.94"],
                "GPE": ["Springfield", "Lakewood"],  # cities/states
                "PERSON": [],
            }

        Key spaCy entity types for invoices:
            ORG    — Organizations/companies (vendor names)
            DATE   — Dates in any format
            MONEY  — Monetary amounts
            GPE    — Geopolitical entities (cities, states, countries)
            PERSON — Person names (less common in invoices)
        """
        self._load_model()

        doc = self._nlp(text)

        entities: dict[str, list[str]] = {}
        for ent in doc.ents:
            label = ent.label_
            if label not in entities:
                entities[label] = []
            # Avoid duplicates
            if ent.text.strip() not in entities[label]:
                entities[label].append(ent.text.strip())

        return entities

    def extract_organizations(self, text: str) -> list[str]:
        """Extract organization names (potential vendor/customer names)."""
        entities = self.extract_entities(text)
        return entities.get("ORG", [])

    def extract_dates(self, text: str) -> list[str]:
        """Extract date mentions from text."""
        entities = self.extract_entities(text)
        return entities.get("DATE", [])

    def extract_money(self, text: str) -> list[str]:
        """Extract monetary amounts from text."""
        entities = self.extract_entities(text)
        return entities.get("MONEY", [])

    def extract_locations(self, text: str) -> list[str]:
        """Extract location names (cities, states)."""
        entities = self.extract_entities(text)
        return entities.get("GPE", [])

    def get_vendor_name_candidates(self, text: str) -> list[str]:
        """
        Get candidate vendor names using NER.

        Strategy: The first ORG entity in the document is often the vendor.
        Returns all ORG entities ranked by position in text (earliest first).
        """
        self._load_model()
        doc = self._nlp(text)

        candidates = []
        for ent in doc.ents:
            if ent.label_ == "ORG" and ent.text.strip() not in candidates:
                candidates.append(ent.text.strip())

        return candidates
