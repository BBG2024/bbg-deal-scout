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

            if self.passes_filters(listing):
                listings.append(listing)

        return listings
