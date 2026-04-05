"""URL Watcher: monitors listing pages for changes and extracts new listings."""

import logging
import hashlib
import re
import requests
from typing import List, Dict, Optional
from datetime import datetime
from bs4 import BeautifulSoup

from .base import BaseCollector
from ..database import WatchURLState

logger = logging.getLogger(__name__)

# Reasonable headers to avoid bot detection
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-CA,en;q=0.9,fr-CA;q=0.8",
}


class URLWatcherCollector(BaseCollector):
    """Monitors URLs for content changes and extracts listing-like data."""

    name = "url_watch"

    def collect(self, region_key: str, region_config: dict) -> List[Dict]:
        """Check all watch URLs for a region."""
        watch_urls = region_config.get("watch_urls", [])

        # Also include custom watch URLs from config
        custom = self.config.get("custom_watch_urls", [])

        all_urls = watch_urls + custom
        if not all_urls:
            logger.debug(f"No watch URLs configured for {region_key}")
            return []

        listings = []

        for url_cfg in all_urls:
            url = url_cfg.get("url", "")
            label = url_cfg.get("label", url[:60])

            try:
                results = self._check_url(url, label, region_key)
                if results:
                    listings.extend(results)
            except Exception as e:
                logger.error(f"URL watch failed for {label}: {e}")
                self.errors.append(f"URLWatch: {label} — {str(e)}")

        logger.info(f"URL watcher for {region_key}: {len(all_urls)} URLs → {len(listings)} items")
        return listings

    def _check_url(self, url: str, label: str, region_key: str) -> List[Dict]:
        """Fetch URL, check for changes, extract listings if changed."""
        try:
            resp = requests.get(url, headers=HEADERS, timeout=20, allow_redirects=True)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.warning(f"Could not fetch {label}: {e}")
            return []

        content = resp.text
        content_hash = hashlib.sha256(content.encode()).hexdigest()

        # Get or create state record
        state, created = WatchURLState.get_or_create(
            url=url,
            defaults={"label": label, "last_hash": None}
        )

        state.last_checked = datetime.utcnow()
        state.check_count += 1

        if not created and state.last_hash == content_hash:
            # No change
            state.save()
            logger.debug(f"No change detected: {label}")
            return []

        # Content changed — extract listings
        logger.info(f"Change detected: {label}")
        state.last_hash = content_hash
        state.last_changed = datetime.utcnow()
        state.save()

        return self._extract_listings(content, url, label, region_key)

    def _extract_listings(
        self, html: str, source_url: str, label: str, region_key: str
    ) -> List[Dict]:
        """Extract listing-like items from HTML content."""
        soup = BeautifulSoup(html, "lxml")
        listings = []

        # Strategy 1: Look for common listing card patterns
        # Many real estate sites use cards with price, address, details
        card_selectors = [
            "div[class*='listing']",
            "div[class*='property']",
            "div[class*='card']",
            "article[class*='listing']",
            "li[class*='listing']",
            "div[class*='result']",
        ]

        cards = []
        for selector in card_selectors:
            found = soup.select(selector)
            if found:
                cards = found
                break

        if cards:
            for card in cards:
                listing = self._parse_card(card, source_url, label, region_key)
                if listing:
                    # Fetch individual listing page to fill in missing fields
                    listing = self.enrich_from_url(listing)
                    if self.passes_filters(listing):
                        listings.append(listing)
        else:
            # Strategy 2: Extract links that look like listing URLs
            for link in soup.find_all("a", href=True):
                href = link.get("href", "")
                text = link.get_text(strip=True)
                if self._looks_like_listing_link(href, text):
                    listing = {
                        "source": self.name,
                        "source_url": self._resolve_url(source_url, href),
                        "source_label": f"Watch: {label}",
                        "title": text[:300] if text else f"Listing from {label}",
                        "region": region_key,
                        "num_units": self.extract_units(text),
                        "asking_price": self.extract_price(text),
                        "listed_cap_rate": self.extract_cap_rate(text),
                        "discovered_at": datetime.utcnow(),
                    }
                    # Fetch individual listing page to fill in missing fields
                    listing = self.enrich_from_url(listing)
                    if self.passes_filters(listing):
                        listings.append(listing)

        return listings

    def _parse_card(
        self, card, source_url: str, label: str, region_key: str
    ) -> Optional[Dict]:
        """Parse a single listing card element."""
        text = card.get_text(" ", strip=True)
        if len(text) < 20:
            return None

        # Find the primary link
        link_el = card.find("a", href=True)
        link = self._resolve_url(source_url, link_el["href"]) if link_el else source_url

        # Try to find a title
        title_el = card.find(["h2", "h3", "h4", "a"])
        title = title_el.get_text(strip=True) if title_el else text[:200]

        # Try to find address
        addr_el = card.find(attrs={"class": re.compile(r"address|location|addr", re.I)})
        address = addr_el.get_text(strip=True) if addr_el else None

        listing = {
            "source": self.name,
            "source_url": link,
            "source_label": f"Watch: {label}",
            "title": title,
            "address": address,
            "region": region_key,
            "num_units": self.extract_units(text),
            "asking_price": self.extract_price(text),
            "listed_cap_rate": self.extract_cap_rate(text),
            "city": None,
            "discovered_at": datetime.utcnow(),
        }

        detected = self.classify_region(text)
        if detected:
            listing["region"] = detected

        return listing

    @staticmethod
    def _looks_like_listing_link(href: str, text: str) -> bool:
        """Heuristic: does this link look like an individual property listing?

        Requirements:
        1. URL or text must contain a listing signal
        2. URL should look like an individual listing (has a numeric ID), OR
           the link text is long and descriptive enough to suggest a real property
        3. Reject nav/category links that match on broad terms like 'proprietes'
        """
        combined = f"{href} {text}".lower()
        h = href.lower()

        # Reject links that are clearly category/nav pages (no numeric property ID)
        # e.g. /proprietes/multi-logements  /proprietes/hotel-hebergement
        if re.search(r"/propri[eé]t[eé]s?/[a-z]", h):
            # Category page pattern: /proprietes/<word> with no trailing number
            if not re.search(r"\d{4,}", h):
                return False

        # Must have a listing-positive signal in URL or text
        signals = ["property", "listing", "detail", "fiche", "immobilier",
                   "real-estate", "immeuble", "appartement", "apartment",
                   "multifamily", "multi-family", "revenue", "logement"]

        has_signal = any(s in combined for s in signals)
        if not has_signal:
            return False

        # Individual listing URL: has a numeric ID segment
        has_id = bool(re.search(r"/\d{5,}", h))

        # Long descriptive text also acceptable even without a numeric ID
        has_description = len(text.strip()) > 25

        return has_id or has_description

    @staticmethod
    def _resolve_url(base: str, relative: str) -> str:
        """Resolve a relative URL against a base."""
        if relative.startswith("http"):
            return relative
        from urllib.parse import urljoin
        return urljoin(base, relative)
