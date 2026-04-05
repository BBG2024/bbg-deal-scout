"""RSS/Atom feed collector for brokerage listing feeds."""

import logging
import feedparser
from typing import List, Dict
from datetime import datetime

from .base import BaseCollector

logger = logging.getLogger(__name__)


class RSSCollector(BaseCollector):
    """Monitors RSS/Atom feeds for new listings."""

    name = "rss_feed"

    def collect(self, region_key: str, region_config: dict) -> List[Dict]:
        """Parse RSS feeds configured for a region."""
        feeds = region_config.get("rss_feeds", [])
        if not feeds:
            logger.debug(f"No RSS feeds configured for {region_key}")
            return []

        listings = []

        for feed_cfg in feeds:
            feed_url = feed_cfg if isinstance(feed_cfg, str) else feed_cfg.get("url", "")
            label = feed_cfg.get("label", feed_url[:60]) if isinstance(feed_cfg, dict) else feed_url[:60]

            try:
                results = self._parse_feed(feed_url, label, region_key)
                listings.extend(results)
            except Exception as e:
                logger.error(f"RSS feed failed for {label}: {e}")
                self.errors.append(f"RSS: {label} — {str(e)}")

        logger.info(f"RSS feeds for {region_key}: {len(feeds)} feeds → {len(listings)} items")
        return listings

    def _pre_filter_rss(self, listing: dict, combined_text: str) -> bool:
        """Fast pre-filter on RSS snippet data before fetching the full listing page.

        Rejects obvious non-qualifiers without making an HTTP request.
        Returns True if the listing should be enriched (fetched), False to skip.
        """
        from .base import SMALL_PROPERTY_KEYWORDS

        # Hard reject if title/URL/snippet contains small-property keywords
        url = (listing.get("source_url") or "").lower()
        text = combined_text.lower()

        for kw in SMALL_PROPERTY_KEYWORDS:
            if kw in text or kw in url:
                logger.debug(f"RSS pre-filter rejected ('{kw}'): {listing.get('title','')[:60]}")
                return False

        # Hard reject if price is extracted and below floor
        price = listing.get("asking_price")
        min_price = self.filters.get("min_price", 0)
        if price and price < min_price:
            return False

        # Hard reject if unit count extracted and clearly out of range
        units = listing.get("num_units")
        min_units = self.filters.get("min_units", 5)
        max_units = self.filters.get("max_units", 50)
        if units is not None and (units < min_units or units > max_units):
            return False

        return True

    def _parse_feed(self, url: str, label: str, region_key: str) -> List[Dict]:
        """Parse a single RSS/Atom feed."""
        feed = feedparser.parse(url)

        if feed.bozo and not feed.entries:
            logger.warning(f"Feed parse error for {label}: {feed.bozo_exception}")
            return []

        listings = []

        for entry in feed.entries:
            title = entry.get("title", "")
            link = entry.get("link", "")
            summary = entry.get("summary", entry.get("description", ""))
            combined = f"{title} {summary}"

            listing = {
                "source": self.name,
                "source_url": link,
                "source_label": f"RSS: {label}",
                "title": title,
                "region": region_key,
                "num_units": self.extract_units(combined),
                "asking_price": self.extract_price(combined),
                "listed_cap_rate": self.extract_cap_rate(combined),
                "city": None,
                "discovered_at": datetime.utcnow(),
            }

            # Classify region from content
            detected = self.classify_region(combined)
            if detected:
                listing["region"] = detected

            # Pre-filter on RSS snippet data (fast reject before fetching page)
            if not self._pre_filter_rss(listing, combined):
                continue

            # Fetch the actual listing page to populate full financial data
            if link:
                listing = self.enrich_from_url(listing)

            if self.passes_filters(listing):
                listings.append(listing)

        return listings
