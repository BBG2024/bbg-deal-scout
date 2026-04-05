"""Bing Web Search API collector for finding multifamily listings."""

import logging
import time
import requests
from typing import List, Dict
from datetime import datetime

from .base import BaseCollector

logger = logging.getLogger(__name__)


class BingSearchCollector(BaseCollector):
    """Finds listings via Bing Web Search API."""

    name = "bing_search"

    def __init__(self, config: dict, filters: dict):
        super().__init__(config, filters)
        self.api_key = config.get("bing_search", {}).get("api_key", "")
        self.endpoint = config.get("bing_search", {}).get(
            "endpoint", "https://api.bing.microsoft.com/v7.0/search"
        )
        self.max_results = config.get("bing_search", {}).get("max_results_per_query", 10)
        self.daily_limit = config.get("bing_search", {}).get("daily_query_limit", 30)
        self._query_count = 0

    def collect(self, region_key: str, region_config: dict) -> List[Dict]:
        """Run search queries for a region and return parsed listings."""
        if not self.api_key or self.api_key == "YOUR_BING_API_KEY":
            logger.warning("Bing API key not configured — skipping web search")
            return []

        queries = region_config.get("search_queries", [])
        listings = []

        for query in queries:
            if self._query_count >= self.daily_limit:
                logger.warning(f"Daily query limit ({self.daily_limit}) reached — stopping")
                break

            try:
                results = self._search(query, region_key)
                listings.extend(results)
                self._query_count += 1
                time.sleep(0.5)  # Rate limiting
            except Exception as e:
                logger.error(f"Bing search failed for '{query}': {e}")
                self.errors.append(f"Bing: {query} — {str(e)}")

        logger.info(
            f"Bing search for {region_key}: "
            f"{len(queries)} queries → {len(listings)} raw results"
        )
        return listings

    def _search(self, query: str, region_key: str) -> List[Dict]:
        """Execute a single Bing search and parse results."""
        headers = {"Ocp-Apim-Subscription-Key": self.api_key}
        params = {
            "q": query,
            "count": self.max_results,
            "mkt": "en-CA",
            "freshness": "Week",  # Only recent results
            "responseFilter": "Webpages",
        }

        resp = requests.get(self.endpoint, headers=headers, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        pages = data.get("webPages", {}).get("value", [])
        listings = []

        for page in pages:
            title = page.get("name", "")
            snippet = page.get("snippet", "")
            url = page.get("url", "")
            combined_text = f"{title} {snippet}"

            # Try to extract structured data from title/snippet
            listing = {
                "source": self.name,
                "source_url": url,
                "source_label": f"Bing: {query[:60]}",
                "title": title,
                "region": region_key,
                "num_units": self.extract_units(combined_text),
                "asking_price": self.extract_price(combined_text),
                "listed_cap_rate": self.extract_cap_rate(combined_text),
                "discovered_at": datetime.utcnow(),
            }

            # Try to extract city from text
            detected_region = self.classify_region(combined_text)
            if detected_region:
                listing["region"] = detected_region

            # Extract city name heuristically
            listing["city"] = self._extract_city(combined_text, region_key)

            # Only keep if it looks like a real property listing
            if self._is_listing_like(title, snippet, url):
                # Enrich: fetch the actual listing page to get units/price/cap rate
                listing = self.enrich_from_url(listing)
                if self.passes_filters(listing):
                    listings.append(listing)

        return listings

    def _is_listing_like(self, title: str, snippet: str, url: str) -> bool:
        """Heuristic: does this search result look like a property listing?"""
        combined = f"{title} {snippet} {url}".lower()

        # Positive signals
        listing_signals = [
            "for sale", "à vendre", "asking", "price",
            "unit", "suite", "logement", "plex",
            "apartment", "multifamily", "multi-family",
            "revenue", "investment property", "immeuble",
            "cap rate", "noi", "income",
        ]

        # Negative signals — these are articles, not listings
        noise_signals = [
            "news", "article", "blog", "guide", "how to",
            "tips for", "market report", "forecast",
            "wikipedia", "reddit.com",
        ]

        positive = sum(1 for s in listing_signals if s in combined)
        negative = sum(1 for s in noise_signals if s in combined)

        # Listing platform URLs are strong signals
        listing_domains = [
            "realtor.ca", "centris.ca", "loopnet.com", "remax.ca",
            "royallepage.ca", "kijiji.ca", "marcusmillichap.com",
            "cbre.ca", "colliers.com", "naicommercial",
            "point2homes", "zolo.ca", "duproprio",
        ]
        domain_match = any(d in url.lower() for d in listing_domains)

        return (positive >= 2 and negative == 0) or domain_match

    @staticmethod
    def _extract_city(text: str, default_region: str) -> str:
        """Try to extract city name from text."""
        city_map = {
            "edmonton": "Edmonton",
            "sherwood park": "Sherwood Park",
            "st. albert": "St. Albert",
            "st albert": "St. Albert",
            "spruce grove": "Spruce Grove",
            "leduc": "Leduc",
            "fort saskatchewan": "Fort Saskatchewan",
            "beaumont": "Beaumont",
            "montreal": "Montreal",
            "montréal": "Montreal",
            "laval": "Laval",
            "longueuil": "Longueuil",
            "brossard": "Brossard",
            "terrebonne": "Terrebonne",
        }
        text_lower = text.lower()
        for key, city in city_map.items():
            if key in text_lower:
                return city
        return "Edmonton" if "edmonton" in default_region else "Montreal"
