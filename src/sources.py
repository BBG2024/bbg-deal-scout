"""Source Manager: add, remove, and list data sources at runtime.

Sources are stored in SQLite so they persist without editing YAML.
The scanner merges DB-stored sources with config.yaml sources at runtime.
"""

import logging
import json
from datetime import datetime
from typing import List, Dict, Optional

from peewee import (
    Model, CharField, TextField, BooleanField,
    DateTimeField, AutoField, IntegerField, FloatField
)
from .database import db, BaseModel

logger = logging.getLogger(__name__)


class ManagedSource(BaseModel):
    """A user-added data source (URL, RSS feed, email sender, folder path)."""
    id = AutoField()
    source_type = CharField(max_length=30)  # "watch_url", "rss_feed", "email_sender", "folder_path"
    url = TextField()                        # URL, email address, or folder path
    label = CharField(max_length=200)
    region = CharField(max_length=50, default="all")  # "greater_edmonton", "greater_montreal", "all"
    enabled = BooleanField(default=True)
    notes = TextField(null=True)
    added_by = CharField(max_length=50, null=True)
    added_at = DateTimeField(default=datetime.utcnow)
    last_checked = DateTimeField(null=True)
    hit_count = IntegerField(default=0)       # How many listings found from this source

    class Meta:
        table_name = "managed_sources"


class SearchHistory(BaseModel):
    """Tracks search queries and results for audit/history."""
    id = AutoField()
    query = TextField()                       # The search query or source identifier
    source_type = CharField(max_length=30)    # "bing_search", "url_watch", etc.
    region = CharField(max_length=50, null=True)
    results_count = IntegerField(default=0)
    new_listings = IntegerField(default=0)
    executed_at = DateTimeField(default=datetime.utcnow)
    duration_seconds = FloatField(null=True)
    error = TextField(null=True)

    class Meta:
        table_name = "search_history"


def init_source_tables():
    """Create source management tables."""
    db.create_tables([ManagedSource, SearchHistory], safe=True)


# --- Source CRUD ---

def add_source(
    source_type: str,
    url: str,
    label: str,
    region: str = "all",
    notes: str = None,
    added_by: str = None,
) -> ManagedSource:
    """Add a new managed source."""
    valid_types = {"watch_url", "rss_feed", "email_sender", "folder_path"}
    if source_type not in valid_types:
        raise ValueError(f"source_type must be one of {valid_types}")

    # Check for duplicates
    existing = ManagedSource.select().where(
        (ManagedSource.source_type == source_type) &
        (ManagedSource.url == url)
    ).first()

    if existing:
        logger.warning(f"Source already exists: {label} ({url})")
        return existing

    source = ManagedSource.create(
        source_type=source_type,
        url=url,
        label=label,
        region=region,
        notes=notes,
        added_by=added_by,
    )
    logger.info(f"Added source: [{source_type}] {label}")
    return source


def remove_source(source_id: int) -> bool:
    """Remove a managed source by ID."""
    try:
        source = ManagedSource.get_by_id(source_id)
        source.delete_instance()
        logger.info(f"Removed source #{source_id}: {source.label}")
        return True
    except ManagedSource.DoesNotExist:
        return False


def toggle_source(source_id: int) -> Optional[bool]:
    """Toggle a source's enabled state. Returns new state or None if not found."""
    try:
        source = ManagedSource.get_by_id(source_id)
        source.enabled = not source.enabled
        source.save()
        logger.info(f"Source #{source_id} {'enabled' if source.enabled else 'disabled'}")
        return source.enabled
    except ManagedSource.DoesNotExist:
        return None


def get_sources(
    source_type: str = None,
    region: str = None,
    enabled_only: bool = True,
) -> List[ManagedSource]:
    """Get managed sources with optional filters."""
    query = ManagedSource.select()
    if source_type:
        query = query.where(ManagedSource.source_type == source_type)
    if region:
        query = query.where(
            (ManagedSource.region == region) | (ManagedSource.region == "all")
        )
    if enabled_only:
        query = query.where(ManagedSource.enabled == True)
    return list(query.order_by(ManagedSource.added_at.desc()))


def get_all_sources() -> List[ManagedSource]:
    """Get all sources regardless of filters."""
    return list(ManagedSource.select().order_by(ManagedSource.source_type, ManagedSource.label))


def get_managed_watch_urls(region: str = None) -> List[Dict]:
    """Get watch URLs from managed sources (format compatible with URL watcher)."""
    sources = get_sources(source_type="watch_url", region=region)
    return [{"url": s.url, "label": s.label} for s in sources]


def get_managed_rss_feeds(region: str = None) -> List[Dict]:
    """Get RSS feeds from managed sources."""
    sources = get_sources(source_type="rss_feed", region=region)
    return [{"url": s.url, "label": s.label} for s in sources]


def get_managed_email_senders() -> List[str]:
    """Get email senders from managed sources."""
    sources = get_sources(source_type="email_sender")
    return [s.url for s in sources]


def increment_hit_count(source_type: str, url: str, count: int = 1):
    """Increment the hit count for a source."""
    try:
        source = ManagedSource.get(
            (ManagedSource.source_type == source_type) &
            (ManagedSource.url == url)
        )
        source.hit_count += count
        source.last_checked = datetime.utcnow()
        source.save()
    except ManagedSource.DoesNotExist:
        pass


# --- Search History ---

def log_search(
    query: str,
    source_type: str,
    region: str = None,
    results_count: int = 0,
    new_listings: int = 0,
    duration_seconds: float = None,
    error: str = None,
) -> SearchHistory:
    """Log a search execution for history tracking."""
    return SearchHistory.create(
        query=query,
        source_type=source_type,
        region=region,
        results_count=results_count,
        new_listings=new_listings,
        duration_seconds=duration_seconds,
        error=error,
    )


def get_search_history(
    limit: int = 100,
    source_type: str = None,
    region: str = None,
) -> List[SearchHistory]:
    """Get search history entries."""
    query = SearchHistory.select()
    if source_type:
        query = query.where(SearchHistory.source_type == source_type)
    if region:
        query = query.where(SearchHistory.region == region)
    return list(query.order_by(SearchHistory.executed_at.desc()).limit(limit))


def get_source_performance() -> List[Dict]:
    """Get performance stats per source type."""
    from peewee import fn

    results = (
        SearchHistory
        .select(
            SearchHistory.source_type,
            fn.COUNT(SearchHistory.id).alias("total_runs"),
            fn.SUM(SearchHistory.results_count).alias("total_results"),
            fn.SUM(SearchHistory.new_listings).alias("total_new"),
            fn.AVG(SearchHistory.duration_seconds).alias("avg_duration"),
        )
        .group_by(SearchHistory.source_type)
    )

    return [
        {
            "source_type": r.source_type,
            "total_runs": r.total_runs,
            "total_results": r.total_results or 0,
            "total_new": r.total_new or 0,
            "avg_duration": round(r.avg_duration or 0, 2),
        }
        for r in results
    ]
