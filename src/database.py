"""Database models and operations for BBG Deal Scout."""

import logging
import hashlib
from datetime import datetime, date
from pathlib import Path

from peewee import (
    SqliteDatabase, Model, CharField, FloatField, IntegerField,
    DateTimeField, DateField, TextField, BooleanField, AutoField
)

logger = logging.getLogger(__name__)

db = SqliteDatabase(None)  # Deferred init


class BaseModel(Model):
    class Meta:
        database = db


class Listing(BaseModel):
    """A single property listing discovered by any collector."""
    id = AutoField()
    fingerprint = CharField(unique=True, max_length=64)  # SHA-256 dedup key

    # Source info
    source = CharField(max_length=50)          # "bing_search", "rss", "url_watch", "email"
    source_url = TextField(null=True)
    source_label = CharField(max_length=200, null=True)

    # Property basics
    title = TextField()
    address = CharField(max_length=500, null=True)
    city = CharField(max_length=100, null=True)
    region = CharField(max_length=50)           # "greater_edmonton" or "greater_montreal"
    province = CharField(max_length=20, null=True)

    # Financials (nullable — score if available, skip if not)
    asking_price = FloatField(null=True)
    price_per_unit = FloatField(null=True)
    listed_cap_rate = FloatField(null=True)
    estimated_noi = FloatField(null=True)

    # Property details
    num_units = IntegerField(null=True)
    building_sf = FloatField(null=True)
    year_built = IntegerField(null=True)
    occupancy = FloatField(null=True)
    asset_type = CharField(max_length=100, null=True)

    # Scoring
    tier1_score = IntegerField(null=True)       # 0-7, number of checks passed
    tier1_details = TextField(null=True)        # JSON string of individual checks
    score_status = CharField(max_length=20, default="unscored")  # unscored, scored, insufficient_data

    # Status tracking
    status = CharField(max_length=20, default="new")  # new, reviewed, shortlisted, passed, dead
    notes = TextField(null=True)
    flagged = BooleanField(default=False)

    # Timestamps
    discovered_at = DateTimeField(default=datetime.utcnow)
    last_seen_at = DateTimeField(default=datetime.utcnow)
    reviewed_at = DateTimeField(null=True)

    class Meta:
        table_name = "listings"


class ScanLog(BaseModel):
    """Log of each daily scan run."""
    id = AutoField()
    scan_date = DateField(default=date.today)
    started_at = DateTimeField(default=datetime.utcnow)
    completed_at = DateTimeField(null=True)
    total_found = IntegerField(default=0)
    new_listings = IntegerField(default=0)
    duplicates_skipped = IntegerField(default=0)
    errors = IntegerField(default=0)
    error_details = TextField(null=True)
    status = CharField(max_length=20, default="running")  # running, completed, failed

    class Meta:
        table_name = "scan_logs"


class WatchURLState(BaseModel):
    """Tracks content hash of watched URLs for change detection."""
    id = AutoField()
    url = TextField(unique=True)
    label = CharField(max_length=200, null=True)
    last_hash = CharField(max_length=64, null=True)
    last_checked = DateTimeField(null=True)
    last_changed = DateTimeField(null=True)
    check_count = IntegerField(default=0)

    class Meta:
        table_name = "watch_url_states"


def init_db(db_path: str = "data/deal_scout.db"):
    """Initialize the database connection and create tables."""
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    db.init(str(path), pragmas={
        "journal_mode": "wal",
        "cache_size": -64 * 1000,  # 64MB
        "foreign_keys": 1,
        "synchronous": "normal",
    })

    db.connect()
    db.create_tables([Listing, ScanLog, WatchURLState], safe=True)
    logger.info(f"Database initialized at {path}")


def generate_fingerprint(title: str, address: str = None, source_url: str = None) -> str:
    """Generate a deduplication fingerprint for a listing.

    Priority:
    1. If source_url is available, use URL alone — it uniquely identifies the
       property regardless of whether address/title was enriched later.
       Strip query params and fragments so ?view=Thumbnail doesn't create dupes.
    2. Fall back to title + address when no URL (e.g. folder-watch docs).
    """
    if source_url:
        # Normalise URL: strip query string, fragment, trailing slash
        from urllib.parse import urlparse, urlunparse
        p = urlparse(source_url.lower().strip())
        normalised = urlunparse((p.scheme, p.netloc, p.path.rstrip("/"), "", "", ""))
        return hashlib.sha256(normalised.encode()).hexdigest()

    raw = f"{title or ''}|{address or ''}"
    raw = raw.lower().strip()
    return hashlib.sha256(raw.encode()).hexdigest()


def upsert_listing(data: dict) -> tuple:
    """
    Insert a new listing or update last_seen if duplicate.
    Returns (listing, is_new).
    """
    fp = data.get("fingerprint") or generate_fingerprint(
        data.get("title", ""),
        data.get("address"),
        data.get("source_url"),
    )
    data["fingerprint"] = fp

    try:
        existing = Listing.get(Listing.fingerprint == fp)
        existing.last_seen_at = datetime.utcnow()
        existing.save()
        return existing, False
    except Listing.DoesNotExist:
        data.setdefault("discovered_at", datetime.utcnow())
        data.setdefault("last_seen_at", datetime.utcnow())
        listing = Listing.create(**data)
        return listing, True


def get_new_listings(since: datetime = None) -> list:
    """Get listings discovered since a given time."""
    query = Listing.select().where(Listing.status == "new")
    if since:
        query = query.where(Listing.discovered_at >= since)
    return list(query.order_by(Listing.discovered_at.desc()))


def get_listings(
    region: str = None,
    status: str = None,
    min_score: int = None,
    limit: int = 100,
    offset: int = 0,
) -> list:
    """Get listings with optional filters."""
    query = Listing.select()
    if region:
        query = query.where(Listing.region == region)
    if status:
        query = query.where(Listing.status == status)
    if min_score is not None:
        query = query.where(Listing.tier1_score >= min_score)
    return list(query.order_by(Listing.discovered_at.desc()).limit(limit).offset(offset))


def get_scan_logs(limit: int = 30) -> list:
    """Get recent scan logs."""
    return list(ScanLog.select().order_by(ScanLog.started_at.desc()).limit(limit))


def get_stats() -> dict:
    """Get summary statistics."""
    total = Listing.select().count()
    new = Listing.select().where(Listing.status == "new").count()
    shortlisted = Listing.select().where(Listing.status == "shortlisted").count()
    scored = Listing.select().where(Listing.score_status == "scored").count()
    edmonton = Listing.select().where(Listing.region == "greater_edmonton").count()
    montreal = Listing.select().where(Listing.region == "greater_montreal").count()

    return {
        "total": total,
        "new": new,
        "shortlisted": shortlisted,
        "scored": scored,
        "by_region": {"greater_edmonton": edmonton, "greater_montreal": montreal},
    }
