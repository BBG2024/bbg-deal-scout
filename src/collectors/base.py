"""Base collector class for all data sources."""

import logging
import re
from abc import ABC, abstractmethod
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)


class BaseCollector(ABC):
    """Abstract base for all listing collectors."""

    name: str = "base"

    def __init__(self, config: dict, filters: dict):
        self.config = config
        self.filters = filters
        self.results: List[Dict] = []
        self.errors: List[str] = []

    @abstractmethod
    def collect(self, region_key: str, region_config: dict) -> List[Dict]:
        """
        Collect listings for a given region.
        Returns list of dicts matching Listing model fields.
        """
        pass

    def passes_filters(self, listing: dict) -> bool:
        """Check if a listing passes basic filter criteria."""
        # Unit count filter
        units = listing.get("num_units")
        if units is not None:
            if units < self.filters.get("min_units", 5):
                return False
            if units > self.filters.get("max_units", 50):
                return False

        # Price filter
        price = listing.get("asking_price")
        if price is not None:
            if price < self.filters.get("min_price", 0):
                return False
            if price > self.filters.get("max_price", float("inf")):
                return False

        return True

    @staticmethod
    def extract_units(text: str) -> Optional[int]:
        """Try to extract unit count from text."""
        patterns = [
            r"(\d+)\s*-?\s*(?:unit|suite|logement|appartement|apt)",
            r"(\d+)\s*-?\s*(?:plex|plexe)",
            r"(\d+)\s*-?\s*(?:door|porte)",
        ]
        for pat in patterns:
            m = re.search(pat, text, re.IGNORECASE)
            if m:
                val = int(m.group(1))
                if 2 <= val <= 200:  # sanity check
                    return val
        return None

    @staticmethod
    def extract_price(text: str) -> Optional[float]:
        """Try to extract price from text."""
        # Match patterns like $1,500,000 or $1.5M or 1 500 000$
        patterns = [
            r"\$\s*([\d,]+(?:\.\d+)?)\s*(?:M|million)",
            r"\$\s*([\d,]+(?:\.\d{2})?)",
            r"([\d\s]+(?:\.\d+)?)\s*\$",
            r"(?:asking|price|listed)\s*(?:at|:)?\s*\$?\s*([\d,]+)",
        ]
        for pat in patterns:
            m = re.search(pat, text, re.IGNORECASE)
            if m:
                raw = m.group(1).replace(",", "").replace(" ", "")
                try:
                    val = float(raw)
                    # Handle "M" suffix
                    if "M" in text[m.start():m.end()] or "million" in text[m.start():m.end()].lower():
                        val *= 1_000_000
                    if 100_000 <= val <= 100_000_000:  # sanity
                        return val
                except ValueError:
                    continue
        return None

    @staticmethod
    def extract_cap_rate(text: str) -> Optional[float]:
        """Try to extract cap rate from text."""
        patterns = [
            r"cap\s*(?:rate)?\s*(?:of|:)?\s*(\d+\.?\d*)\s*%",
            r"(\d+\.?\d*)\s*%\s*cap",
            r"taux\s*(?:de)?\s*capitalisation\s*(?:de|:)?\s*(\d+\.?\d*)\s*%",
        ]
        for pat in patterns:
            m = re.search(pat, text, re.IGNORECASE)
            if m:
                val = float(m.group(1))
                if 1.0 <= val <= 15.0:  # sanity
                    return val
        return None

    @staticmethod
    def classify_region(text: str) -> Optional[str]:
        """Classify which BBG target region a listing belongs to."""
        text_lower = text.lower()
        edmonton_markers = [
            "edmonton", "sherwood park", "st. albert", "st albert",
            "spruce grove", "leduc", "fort saskatchewan", "beaumont",
            "stony plain", "devon",
        ]
        montreal_markers = [
            "montreal", "montréal", "laval", "longueuil", "brossard",
            "terrebonne", "repentigny", "west island", "île des soeurs",
            "verdun", "lasalle", "lachine", "dorval", "pointe-claire",
            "saint-laurent", "ahuntsic", "rosemont", "plateau",
        ]

        for marker in edmonton_markers:
            if marker in text_lower:
                return "greater_edmonton"
        for marker in montreal_markers:
            if marker in text_lower:
                return "greater_montreal"
        return None
