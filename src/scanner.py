"""BBG Deal Scout — Main Scanner Orchestrator.

This is the core engine. It runs all collectors, deduplicates,
scores listings, logs results, and sends notifications.
"""

import logging
import json
from datetime import datetime
from typing import List, Dict

from .config import get_config, get_regions, get_filters
from .database import (
    init_db, upsert_listing, get_new_listings,
    ScanLog, Listing, db
)
from .scoring import score_listing
from .sources import (
    init_source_tables, get_managed_watch_urls, get_managed_rss_feeds,
    get_managed_email_senders, log_search, increment_hit_count,
)
from .collectors.web_search import BingSearchCollector
from .collectors.rss_monitor import RSSCollector
from .collectors.url_watcher import URLWatcherCollector
from .collectors.email_parser import EmailAlertCollector
from .collectors.folder_watcher import FolderWatcherCollector
from .collectors.realtor_ca import RealtorCaCollector
from .notifications.email_digest import send_email_digest
from .notifications.slack_notify import send_slack_notification
from .onedrive_sync import sync_to_onedrive

logger = logging.getLogger(__name__)


class DealScanner:
    """Orchestrates the full daily scan pipeline."""

    def __init__(self, config: dict = None):
        self.config = config or get_config()
        self.filters = self.config.get("filters", {})
        self.regions = self.config.get("regions", {})

        # Initialize collectors
        self.collectors = [
            RealtorCaCollector(self.config, self.filters),   # Direct API — most reliable
            BingSearchCollector(self.config, self.filters),
            RSSCollector(self.config, self.filters),
            URLWatcherCollector(self.config, self.filters),
            EmailAlertCollector(self.config, self.filters),
            FolderWatcherCollector(self.config, self.filters),
        ]

        # Stats for this run
        self.stats = {
            "queries_run": 0,
            "total_found": 0,
            "new_listings": 0,
            "duplicates": 0,
            "scored": 0,
            "errors": 0,
            "error_details": [],
            "dashboard_port": self.config.get("dashboard", {}).get("port", 8050),
        }

    def run(self) -> Dict:
        """Execute the full scan pipeline. Returns stats dict."""
        logger.info("=" * 60)
        logger.info("BBG Deal Scout — Starting daily scan")
        logger.info(f"Regions: {list(self.regions.keys())}")
        logger.info(f"Collectors: {[c.name for c in self.collectors]}")
        logger.info("=" * 60)

        # Initialize database
        db_path = self.config.get("general", {}).get("database_path", "data/deal_scout.db")
        init_db(db_path)
        init_source_tables()

        # Merge managed sources (from DB) into region configs
        self._merge_managed_sources()

        # Create scan log entry
        scan_log = ScanLog.create(
            started_at=datetime.utcnow(),
            status="running",
        )

        scan_start = datetime.utcnow()

        try:
            # Phase 1: Collect from all sources across all regions
            raw_listings = self._collect_all()

            # Phase 2: Deduplicate and store
            new_listings = self._store_listings(raw_listings)

            # Phase 3: Score new listings
            self._score_listings(new_listings)

            # Phase 4: Send notifications
            self._notify(new_listings)

            # Update scan log
            scan_log.status = "completed"

        except Exception as e:
            logger.error(f"Scan failed: {e}", exc_info=True)
            self.stats["errors"] += 1
            self.stats["error_details"].append(f"Fatal: {str(e)}")
            scan_log.status = "failed"

        # Finalize scan log
        scan_log.completed_at = datetime.utcnow()
        scan_log.total_found = self.stats["total_found"]
        scan_log.new_listings = self.stats["new_listings"]
        scan_log.duplicates_skipped = self.stats["duplicates"]
        scan_log.errors = self.stats["errors"]
        scan_log.error_details = json.dumps(self.stats["error_details"]) if self.stats["error_details"] else None
        scan_log.save()

        elapsed = (datetime.utcnow() - scan_start).total_seconds()
        logger.info("=" * 60)
        logger.info(f"Scan complete in {elapsed:.1f}s")
        logger.info(
            f"Found: {self.stats['total_found']} | "
            f"New: {self.stats['new_listings']} | "
            f"Dupes: {self.stats['duplicates']} | "
            f"Scored: {self.stats['scored']} | "
            f"Errors: {self.stats['errors']}"
        )
        logger.info("=" * 60)

        return self.stats

    def _merge_managed_sources(self):
        """Merge user-added sources from DB into the region configs."""
        try:
            for region_key in self.regions:
                # Merge watch URLs
                managed_urls = get_managed_watch_urls(region_key)
                existing_watch = self.regions[region_key].get("watch_urls", [])
                existing_url_set = {u.get("url") for u in existing_watch}
                for mu in managed_urls:
                    if mu["url"] not in existing_url_set:
                        existing_watch.append(mu)
                self.regions[region_key]["watch_urls"] = existing_watch

                # Merge RSS feeds
                managed_rss = get_managed_rss_feeds(region_key)
                existing_rss = self.regions[region_key].get("rss_feeds", [])
                existing_rss_set = set()
                for r in existing_rss:
                    if isinstance(r, dict):
                        existing_rss_set.add(r.get("url", ""))
                    else:
                        existing_rss_set.add(r)
                for mr in managed_rss:
                    if mr["url"] not in existing_rss_set:
                        existing_rss.append(mr)
                self.regions[region_key]["rss_feeds"] = existing_rss

            # Merge email senders
            managed_senders = get_managed_email_senders()
            existing_senders = self.config.get("email_parsing", {}).get("alert_senders", [])
            for ms in managed_senders:
                if ms not in existing_senders:
                    existing_senders.append(ms)

            logger.info(f"Merged managed sources into config")
        except Exception as e:
            logger.warning(f"Could not merge managed sources: {e}")

    def _collect_all(self) -> List[Dict]:
        """Run all collectors across all regions."""
        all_listings = []

        for region_key, region_config in self.regions.items():
            logger.info(f"\n--- Scanning region: {region_config.get('label', region_key)} ---")

            for collector in self.collectors:
                try:
                    logger.info(f"  Running collector: {collector.name}")
                    results = collector.collect(region_key, region_config)
                    all_listings.extend(results)
                    self.stats["total_found"] += len(results)
                    logger.info(f"  → {collector.name}: {len(results)} results")

                    # Aggregate errors
                    if collector.errors:
                        self.stats["errors"] += len(collector.errors)
                        self.stats["error_details"].extend(collector.errors)
                        collector.errors.clear()

                except Exception as e:
                    logger.error(f"  Collector {collector.name} failed for {region_key}: {e}")
                    self.stats["errors"] += 1
                    self.stats["error_details"].append(
                        f"{collector.name}/{region_key}: {str(e)}"
                    )

        logger.info(f"\nTotal raw results across all sources: {len(all_listings)}")
        return all_listings

    def _store_listings(self, raw_listings: List[Dict]) -> List:
        """Deduplicate and store listings in database."""
        new_listings = []

        for data in raw_listings:
            try:
                listing, is_new = upsert_listing(data)
                if is_new:
                    new_listings.append(listing)
                    self.stats["new_listings"] += 1
                else:
                    self.stats["duplicates"] += 1
            except Exception as e:
                logger.error(f"Failed to store listing '{data.get('title', '?')[:50]}': {e}")
                self.stats["errors"] += 1

        logger.info(
            f"Storage: {len(new_listings)} new, "
            f"{self.stats['duplicates']} duplicates skipped"
        )
        return new_listings

    def _score_listings(self, listings: List):
        """Score new listings against Tier 1 Scorecard."""
        scored_count = 0

        for listing in listings:
            try:
                result = score_listing(listing)
                listing.tier1_score = result["tier1_score"]
                listing.tier1_details = result["tier1_details"]
                listing.score_status = result["score_status"]
                listing.save()
                scored_count += 1
            except Exception as e:
                logger.error(f"Scoring failed for listing {listing.id}: {e}")

        self.stats["scored"] = scored_count
        logger.info(f"Scoring: {scored_count}/{len(listings)} listings scored")

    def _notify(self, new_listings: List):
        """Send notifications and sync results to OneDrive."""
        # Sort by score descending
        sorted_listings = sorted(
            new_listings,
            key=lambda x: (x.tier1_score or -1),
            reverse=True,
        )

        if new_listings:
            logger.info(f"Sending notifications for {len(sorted_listings)} new listings")

            try:
                send_email_digest(self.config, sorted_listings, self.stats)
            except Exception as e:
                logger.error(f"Email digest failed: {e}")

            try:
                send_slack_notification(self.config, sorted_listings, self.stats)
            except Exception as e:
                logger.error(f"Slack notification failed: {e}")
        else:
            logger.info("No new listings — skipping email/slack notifications")

        # Always sync to OneDrive (even if no new listings, keeps CSV/report fresh)
        try:
            sync_to_onedrive(self.config, sorted_listings, self.stats)
        except Exception as e:
            logger.error(f"OneDrive sync failed: {e}")


def run_scan(config: dict = None) -> Dict:
    """Convenience function to run a single scan."""
    scanner = DealScanner(config)
    return scanner.run()
