#!/usr/bin/env python3
"""BBG Deal Scout — CLI Entry Point.

Usage:
    python -m src.cli scan              Run a single scan now
    python -m src.cli dashboard         Start the web dashboard
    python -m src.cli schedule          Run on a daily schedule (foreground)
    python -m src.cli setup             Interactive setup wizard
    python -m src.cli stats             Print current stats
    python -m src.cli export            Export listings to CSV
    python -m src.cli sources           List all managed sources
    python -m src.cli add-source        Add a new source interactively
    python -m src.cli history           Show recent search history
"""

import sys
import os
import logging
import csv
import shutil
from datetime import datetime
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config import load_config, get_config
from src.database import init_db, get_listings, get_stats, Listing


def setup_logging(level: str = "INFO"):
    """Configure logging with rich formatting if available."""
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    log_file = log_dir / f"deal_scout_{datetime.now().strftime('%Y%m%d')}.log"

    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(str(log_file)),
        ],
    )


def cmd_scan():
    """Run a single scan."""
    config = load_config()
    setup_logging(config.get("general", {}).get("log_level", "INFO"))

    from src.scanner import run_scan
    stats = run_scan(config)

    print("\n--- Scan Results ---")
    print(f"  Total found:  {stats['total_found']}")
    print(f"  New listings: {stats['new_listings']}")
    print(f"  Duplicates:   {stats['duplicates']}")
    print(f"  Scored:       {stats['scored']}")
    print(f"  Errors:       {stats['errors']}")

    if stats['error_details']:
        print("\n  Error details:")
        for err in stats['error_details'][:10]:
            print(f"    - {err}")


def cmd_dashboard():
    """Start the web dashboard."""
    config = load_config()
    setup_logging(config.get("general", {}).get("log_level", "INFO"))

    from src.dashboard.app import run_dashboard
    run_dashboard(config)


def cmd_schedule():
    """Run scanner on a daily schedule (blocking)."""
    import schedule
    import time

    config = load_config()
    setup_logging(config.get("general", {}).get("log_level", "INFO"))

    logger = logging.getLogger(__name__)

    run_hour = config.get("general", {}).get("run_hour", 7)
    run_time = f"{run_hour:02d}:00"

    def daily_job():
        logger.info("Scheduled scan triggered")
        from src.scanner import run_scan
        run_scan(config)

    schedule.every().day.at(run_time).do(daily_job)

    logger.info(f"Deal Scout scheduler started — daily scan at {run_time}")
    logger.info("Press Ctrl+C to stop")

    # Run immediately on first launch
    logger.info("Running initial scan...")
    daily_job()

    while True:
        schedule.run_pending()
        time.sleep(60)


def cmd_stats():
    """Print current database stats."""
    config = load_config()
    db_path = config.get("general", {}).get("database_path", "data/deal_scout.db")
    init_db(db_path)

    stats = get_stats()
    print("\n=== BBG Deal Scout — Current Stats ===")
    print(f"  Total listings:  {stats['total']}")
    print(f"  New (unreviewed): {stats['new']}")
    print(f"  Shortlisted:     {stats['shortlisted']}")
    print(f"  Scored:          {stats['scored']}")
    print(f"  Edmonton region: {stats['by_region']['greater_edmonton']}")
    print(f"  Montreal region: {stats['by_region']['greater_montreal']}")


def cmd_export():
    """Export all listings to CSV."""
    config = load_config()
    db_path = config.get("general", {}).get("database_path", "data/deal_scout.db")
    init_db(db_path)

    listings = get_listings(limit=10000)

    if not listings:
        print("No listings to export.")
        return

    filename = f"deal_scout_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    filepath = Path("data") / filename

    fields = [
        "id", "title", "source", "source_url", "source_label",
        "address", "city", "region", "province",
        "asking_price", "price_per_unit", "listed_cap_rate",
        "num_units", "year_built", "occupancy",
        "tier1_score", "score_status", "status",
        "notes", "flagged", "discovered_at", "last_seen_at",
    ]

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for l in listings:
            row = {field: getattr(l, field, "") for field in fields}
            writer.writerow(row)

    print(f"Exported {len(listings)} listings to {filepath}")


def cmd_setup():
    """Interactive setup wizard."""
    print("\n=== BBG Deal Scout — Setup Wizard ===\n")

    example = Path("config.yaml.example")
    target = Path("config.yaml")

    if target.exists():
        resp = input("config.yaml already exists. Overwrite? [y/N]: ").strip().lower()
        if resp != "y":
            print("Keeping existing config.yaml")
        else:
            shutil.copy(example, target)
            print("Created fresh config.yaml from template")
    else:
        if example.exists():
            shutil.copy(example, target)
            print("Created config.yaml from template")
        else:
            print("ERROR: config.yaml.example not found!")
            return

    # Create inbox directory
    inbox = Path("data/inbox")
    inbox.mkdir(parents=True, exist_ok=True)
    (inbox / "processed").mkdir(exist_ok=True)
    print(f"Created broker inbox folder: {inbox}/")

    print("\n--- Required Setup Steps ---\n")
    print("1. BING SEARCH API KEY (free tier = 1,000 searches/month)")
    print("   → Go to: https://portal.azure.com")
    print("   → Create resource: 'Bing Search v7'")
    print("   → Copy the API key into config.yaml under bing_search.api_key")
    print("   → Or set env var: BBG_BING_API_KEY=your_key")
    print()
    print("2. EMAIL ALERTS (optional but recommended)")
    print("   → Create a dedicated Gmail: bbg.dealalerts@gmail.com")
    print("   → Enable 2FA, then create an App Password")
    print("   → Set up email alerts on Realtor.ca, Centris.ca, LoopNet.com")
    print("   → Point all alerts to that Gmail address")
    print("   → Update config.yaml with email credentials")
    print("   → Or set env var: BBG_EMAIL_PASSWORD=your_app_password")
    print()
    print("3. BROKER INBOX")
    print(f"   → Drop PDFs, docs, or forwarded emails from brokers into: {inbox.absolute()}/")
    print("   → Deal Scout extracts listing data automatically on each scan")
    print("   → Processed files are moved to: processed/")
    print()
    print("4. ADD SOURCES (brokerage URLs, RSS feeds)")
    print("   → Via dashboard: http://localhost:8050/sources")
    print("   → Via CLI: python -m src.cli add-source")
    print("   → Sources you add are stored in the database — no YAML editing needed")
    print()
    print("5. SLACK (optional)")
    print("   → Create a Slack incoming webhook")
    print("   → Paste the webhook URL into config.yaml")
    print()
    print("6. DASHBOARD PASSWORDS")
    print("   → Edit config.yaml → dashboard → users")
    print("   → Change the default passwords!")
    print()
    print("7. CRON JOB (for daily automated runs)")
    print("   → Run: crontab -e")
    print("   → Add: 0 7 * * * cd /path/to/bbg-deal-scout && /usr/bin/python3 -m src.cli scan >> logs/cron.log 2>&1")
    print()
    print("8. TEST IT")
    print("   → python -m src.cli scan     # run a scan")
    print("   → python -m src.cli dashboard # start the dashboard")
    print()
    print("Setup complete. Edit config.yaml with your credentials and you're ready.")


def cmd_sources():
    """List all managed sources."""
    config = load_config()
    db_path = config.get("general", {}).get("database_path", "data/deal_scout.db")
    init_db(db_path)

    from src.sources import init_source_tables, get_all_sources
    init_source_tables()
    sources = get_all_sources()

    if not sources:
        print("\nNo custom sources added yet.")
        print("Add sources via: python -m src.cli add-source")
        print("Or via dashboard: http://localhost:8050/sources")
        return

    print(f"\n=== BBG Deal Scout — Managed Sources ({len(sources)}) ===\n")
    for s in sources:
        status = "✓ Active" if s.enabled else "✗ Disabled"
        region = "Both" if s.region == "all" else s.region
        print(f"  #{s.id}  [{s.source_type}]  {s.label}")
        print(f"       {s.url[:80]}")
        print(f"       Region: {region} | {status} | Hits: {s.hit_count}")
        print()


def cmd_add_source():
    """Add a new source interactively."""
    config = load_config()
    db_path = config.get("general", {}).get("database_path", "data/deal_scout.db")
    init_db(db_path)

    from src.sources import init_source_tables, add_source
    init_source_tables()

    print("\n=== Add New Source ===\n")
    print("Source types:")
    print("  1. watch_url    — Brokerage listing page (checked for changes daily)")
    print("  2. rss_feed     — RSS/Atom feed URL")
    print("  3. email_sender — Email address that sends listing alerts")
    print("  4. folder_path  — Local folder to watch for dropped files")
    print()

    type_map = {"1": "watch_url", "2": "rss_feed", "3": "email_sender", "4": "folder_path"}
    choice = input("Type [1-4]: ").strip()
    source_type = type_map.get(choice)
    if not source_type:
        print("Invalid choice.")
        return

    url = input("URL / Email / Path: ").strip()
    if not url:
        print("Cannot be empty.")
        return

    label = input("Label (e.g. 'CBRE Edmonton MF'): ").strip()
    if not label:
        label = url[:50]

    print("Region:")
    print("  1. Both regions")
    print("  2. Greater Edmonton")
    print("  3. Greater Montreal")
    region_choice = input("Region [1-3, default 1]: ").strip()
    region_map = {"1": "all", "2": "greater_edmonton", "3": "greater_montreal", "": "all"}
    region = region_map.get(region_choice, "all")

    notes = input("Notes (optional): ").strip() or None

    source = add_source(
        source_type=source_type,
        url=url,
        label=label,
        region=region,
        notes=notes,
        added_by="cli",
    )
    print(f"\n✓ Added source #{source.id}: [{source_type}] {label}")


def cmd_history():
    """Show recent search history."""
    config = load_config()
    db_path = config.get("general", {}).get("database_path", "data/deal_scout.db")
    init_db(db_path)

    from src.sources import init_source_tables, get_search_history, get_source_performance
    init_source_tables()

    history = get_search_history(limit=30)
    performance = get_source_performance()

    if performance:
        print("\n=== Source Performance ===\n")
        for p in performance:
            print(f"  {p['source_type']:15s}  Runs: {p['total_runs']:4d}  Results: {p['total_results']:5d}  New: {p['total_new']:4d}  Avg: {p['avg_duration']:.1f}s")

    if not history:
        print("\nNo search history yet. Run a scan first.")
        return

    print(f"\n=== Recent Search History (last {len(history)}) ===\n")
    for h in history[:20]:
        new_flag = f" → {h.new_listings} NEW" if h.new_listings > 0 else ""
        error_flag = f" [ERR: {h.error[:40]}]" if h.error else ""
        print(
            f"  {h.executed_at.strftime('%b %d %H:%M')}  "
            f"[{h.source_type:12s}]  "
            f"{h.query[:50]:50s}  "
            f"→ {h.results_count} results{new_flag}{error_flag}"
        )


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    command = sys.argv[1].lower()

    commands = {
        "scan": cmd_scan,
        "dashboard": cmd_dashboard,
        "schedule": cmd_schedule,
        "setup": cmd_setup,
        "stats": cmd_stats,
        "export": cmd_export,
        "sources": cmd_sources,
        "add-source": cmd_add_source,
        "history": cmd_history,
    }

    if command in commands:
        try:
            commands[command]()
        except KeyboardInterrupt:
            print("\nShutdown requested. Goodbye.")
        except FileNotFoundError as e:
            print(f"\nConfiguration error: {e}")
            print("Run 'python -m src.cli setup' first.")
            sys.exit(1)
    else:
        print(f"Unknown command: {command}")
        print(__doc__)
        sys.exit(1)


if __name__ == "__main__":
    main()
