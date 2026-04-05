"""Base collector class for all data sources."""

import logging
import re
import time
from abc import ABC, abstractmethod
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)

# Known real estate listing platforms — safe to fetch individual pages from
LISTING_DOMAINS = [
    "realtor.ca", "centris.ca", "loopnet.com", "remax.ca",
    "royallepage.ca", "kijiji.ca", "marcusmillichap.com",
    "cbre.ca", "colliers.com", "naicommercial", "point2homes",
    "zolo.ca", "duproprio.com", "commercialx.ca", "icx.ca",
    "propertyguys.com", "comfree.com", "jll.ca",
]

# Keywords in title/text that indicate a small or non-qualifying property
SMALL_PROPERTY_KEYWORDS = [
    "duplex", "triplex", "quadruplex", "quadplex", "fourplex",
    "semi-detached", "semi detached", "single family", "single-family",
    "bungalow", "house for sale", "maison à vendre", "townhouse",
    "town house", "cottage", "chalet", "commercial land", "vacant lot",
    "terrain", "warehouse", "retail", "office", "bureau",
]

# Word-form plex names → unit count
PLEX_WORDS = {
    "duplex": 2,
    "triplex": 3,
    "quadruplex": 4,
    "quadplex": 4,
    "fourplex": 4,
    "4-plex": 4,
    "fiveplex": 5,
    "5-plex": 5,
    "sixplex": 6,
    "6-plex": 6,
    "cinqplex": 5,
    "sixplex": 6,
}


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
        min_units = self.filters.get("min_units", 5)
        max_units = self.filters.get("max_units", 50)

        # Unit count filter — hard reject when units are known and out of range
        units = listing.get("num_units")
        if units is not None:
            if units < min_units:
                logger.debug(f"Rejected (units {units} < {min_units}): {listing.get('title', '')[:60]}")
                return False
            if units > max_units:
                logger.debug(f"Rejected (units {units} > {max_units}): {listing.get('title', '')[:60]}")
                return False
        else:
            # Units unknown — reject if title/snippet flags a small or non-qualifying property
            combined = (
                (listing.get("title") or "") + " " +
                (listing.get("address") or "") + " " +
                (listing.get("source_url") or "")
            ).lower()
            for kw in SMALL_PROPERTY_KEYWORDS:
                if kw in combined:
                    logger.debug(f"Rejected (keyword '{kw}'): {listing.get('title', '')[:60]}")
                    return False

        # Price filter
        price = listing.get("asking_price")
        if price is not None:
            if price < self.filters.get("min_price", 0):
                return False
            if price > self.filters.get("max_price", float("inf")):
                return False

        return True

    def enrich_from_url(self, listing: dict) -> dict:
        """
        Fetch the listing's source URL and extract structured data from the full page.
        Only fetches pages from known listing platforms to avoid hammering random sites.
        Enriches: num_units, asking_price, listed_cap_rate, address from full page text.
        """
        url = listing.get("source_url", "")
        if not url:
            return listing

        # Only fetch from known listing platforms
        if not any(domain in url.lower() for domain in LISTING_DOMAINS):
            return listing

        try:
            import trafilatura
            import requests as _req

            headers = {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
                "Accept-Language": "en-CA,en;q=0.9,fr-CA;q=0.8",
            }

            resp = _req.get(url, headers=headers, timeout=12, allow_redirects=True)
            resp.raise_for_status()

            # Use trafilatura to extract clean text from HTML
            page_text = trafilatura.extract(resp.text, include_tables=True, no_fallback=False)
            if not page_text:
                # Fallback: raw text from BeautifulSoup
                from bs4 import BeautifulSoup
                page_text = BeautifulSoup(resp.text, "lxml").get_text(" ", strip=True)[:8000]

            if page_text:
                full_text = page_text[:10000]  # cap to avoid slow regex on huge pages

                # Enrich units — only if not already set
                if not listing.get("num_units"):
                    units = self.extract_units(full_text)
                    if units:
                        listing["num_units"] = units
                        logger.debug(f"Page-extracted units={units} from {url[:60]}")

                # Enrich price
                if not listing.get("asking_price"):
                    price = self.extract_price(full_text)
                    if price:
                        listing["asking_price"] = price

                # Enrich cap rate
                if not listing.get("listed_cap_rate"):
                    cap = self.extract_cap_rate(full_text)
                    if cap:
                        listing["listed_cap_rate"] = cap

                # Enrich address
                if not listing.get("address"):
                    addr = self.extract_address(full_text)
                    if addr:
                        listing["address"] = addr

            time.sleep(0.5)  # polite rate limiting

        except Exception as e:
            logger.debug(f"Could not enrich from URL {url[:60]}: {e}")

        return listing

    @staticmethod
    def extract_units(text: str) -> Optional[int]:
        """Try to extract unit count from text (snippet or full page)."""
        text_lower = text.lower()

        # Check word-form plex names first (duplex=2, triplex=3…)
        for word, count in PLEX_WORDS.items():
            if word in text_lower:
                return count

        # Numeric patterns
        patterns = [
            r"(\d+)\s*-?\s*(?:unit|suite|logement|appartement|apt)s?\b",
            r"(\d+)\s*-?\s*(?:plex|plexe)\b",
            r"(\d+)\s*-?\s*(?:door|porte)s?\b",
            # French patterns
            r"(\d+)\s*(?:logements?|appartements?|unités?)\b",
            # "Building with 12 suites"
            r"(?:building|immeuble|complex)\s+(?:of|with|de|avec)\s+(\d+)",
            # "12-suite apartment building"
            r"(\d+)\s*(?:suite|appartement|logement|unit)s?",
        ]
        for pat in patterns:
            m = re.search(pat, text, re.IGNORECASE)
            if m:
                val = int(m.group(1))
                if 2 <= val <= 500:  # sanity check
                    return val
        return None

    @staticmethod
    def extract_address(text: str) -> Optional[str]:
        """Try to extract a street address from text."""
        # Match patterns like "123 Main Street NW" or "1234 rue des Érables"
        patterns = [
            r"\b(\d{1,5}\s+[A-Za-zÀ-ÿ\s]{3,40}(?:Street|St|Avenue|Ave|Boulevard|Blvd|Drive|Dr|Road|Rd|Way|Lane|Ln|Court|Ct|Place|Pl|Crescent|Cres|NW|NE|SW|SE|N|S|E|W)\b)",
            r"\b(\d{1,5}\s+(?:rue|avenue|boulevard|chemin|route|place|voie)\s+[A-Za-zÀ-ÿ\s\-]{3,40})",
        ]
        for pat in patterns:
            m = re.search(pat, text, re.IGNORECASE)
            if m:
                addr = m.group(1).strip()
                if len(addr) > 8:
                    return addr
        return None

    @staticmethod
    def extract_price(text: str) -> Optional[float]:
        """Try to extract asking price from text (snippet or full page)."""
        patterns = [
            # $1.5M or $1,500,000
            r"\$\s*([\d,]+(?:\.\d+)?)\s*(?:M|million)",
            r"\$\s*([\d,]+(?:\.\d{2})?)\b",
            # Trailing dollar: 1 500 000 $
            r"([\d\s,]+)\s*\$",
            # "Price: $X" or "Asking: $X"
            r"(?:asking|list|price|prix)[^\d]{0,20}([\d,\s]+)",
        ]
        for pat in patterns:
            m = re.search(pat, text, re.IGNORECASE)
            if m:
                raw = m.group(1).replace(",", "").replace(" ", "")
                try:
                    val = float(raw)
                    segment = text[m.start():m.end()].lower()
                    if "m" == segment.rstrip()[-1] or "million" in segment:
                        val *= 1_000_000
                    if 200_000 <= val <= 100_000_000:  # sanity: $200K–$100M
                        return val
                except (ValueError, IndexError):
                    continue
        return None

    @staticmethod
    def extract_cap_rate(text: str) -> Optional[float]:
        """Try to extract cap rate from text."""
        patterns = [
            r"cap\s*(?:rate)?\s*(?:of|:)?\s*(\d+\.?\d*)\s*%",
            r"(\d+\.?\d*)\s*%\s*cap(?:\s*rate)?",
            r"taux\s*(?:de)?\s*capitalisation\s*(?:de|:)?\s*(\d+\.?\d*)\s*%",
            r"(\d+\.?\d*)\s*%\s*(?:cap|capitalisation)",
            # "Capitalization: 5.5%"
            r"(?:capitaliz|cap)\w*\s*:?\s*(\d+\.?\d*)\s*%",
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
