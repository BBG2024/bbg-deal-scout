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
        """Run search queries for a region and return parsed listings.

        Uses Bing API when configured; falls back to DuckDuckGo HTML search
        (no API key required) so Edmonton and other regions always have coverage.
        """
        queries = region_config.get("search_queries", [])
        listings = []

        use_bing = bool(self.api_key and self.api_key != "YOUR_BING_API_KEY")
        engine = "Bing" if use_bing else "DDG"

        if not use_bing:
            logger.info(
                f"Bing API key not set — using DuckDuckGo HTML fallback for {region_key}"
            )

        # Limit DDG queries more conservatively to avoid rate limiting.
        # Use a local counter (not self._query_count) so each region gets
        # its own quota — otherwise Edmonton exhausts the counter and
        # Montreal receives zero queries.
        query_limit = self.daily_limit if use_bing else min(self.daily_limit, 4)
        region_query_count = 0

        for query in queries[:query_limit]:
            if region_query_count >= query_limit:
                break

            try:
                if use_bing:
                    results = self._search(query, region_key)
                else:
                    results = self._search_ddg(query, region_key)
                listings.extend(results)
                self._query_count += 1
                region_query_count += 1
                time.sleep(1.0 if use_bing else 2.0)  # DDG needs more backoff
            except Exception as e:
                logger.error(f"{engine} search failed for '{query}': {e}")
                self.errors.append(f"{engine}: {query} — {str(e)}")

        logger.info(
            f"{engine} search for {region_key}: "
            f"{self._query_count} queries → {len(listings)} raw results"
        )
        return listings

    def _search_ddg(self, query: str, region_key: str) -> List[Dict]:
        """Search via DuckDuckGo HTML endpoint — no API key required.

        Uses html.duckduckgo.com/html which returns static HTML results
        (no JavaScript rendering needed). Rate limit: ~1 req/2s.
        """
        from bs4 import BeautifulSoup

        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-CA,en;q=0.9",
        }
        params = {"q": query, "kl": "ca-en", "kf": "-1"}  # Canada, no safe-search filter

        try:
            resp = requests.get(
                "https://html.duckduckgo.com/html/",
                params=params, headers=headers, timeout=15
            )
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"DDG request failed: {e}")
            return []

        soup = BeautifulSoup(resp.text, "lxml")
        listings = []

        for result in soup.select(".result, .web-result"):
            title_el = result.select_one(".result__title a, .result__a")
            snippet_el = result.select_one(".result__snippet")
            if not title_el:
                continue

            title = title_el.get_text(strip=True)
            url = title_el.get("href", "")
            # DDG wraps URLs — unwrap if needed
            if "duckduckgo.com/l/?uddg=" in url:
                from urllib.parse import unquote, urlparse, parse_qs
                parsed = urlparse(url)
                uddg = parse_qs(parsed.query).get("uddg", [""])[0]
                url = unquote(uddg) if uddg else url

            snippet = snippet_el.get_text(strip=True) if snippet_el else ""
            combined_text = f"{title} {snippet}"

            listing = {
                "source": "ddg_search",
                "source_url": url,
                "source_label": f"Search: {query[:50]}",
                "title": title,
                "region": region_key,
                "num_units": self.extract_units(combined_text),
                "asking_price": self.extract_price(combined_text),
                "listed_cap_rate": self.extract_cap_rate(combined_text),
                "discovered_at": datetime.utcnow(),
            }

            detected_region = self.classify_region(combined_text)
            if detected_region:
                listing["region"] = detected_region
            listing["city"] = self._extract_city(combined_text, region_key)

            if self._is_listing_like(title, snippet, url):
                listing = self.enrich_from_url(listing)
                if self.passes_filters(listing):
                    listings.append(listing)

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
