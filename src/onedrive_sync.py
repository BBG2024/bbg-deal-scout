"""OneDrive sync: copies scan results to a shared OneDrive folder for team access.

After each scan, this module:
1. Exports the latest listings to CSV
2. Copies the CSV + database to OneDrive shared folder
3. Generates a summary HTML report for easy viewing

The OneDrive folder auto-syncs to all team members who have access.
"""

import logging
import shutil
import csv
import os
import platform
from datetime import datetime, date
from pathlib import Path
from typing import List, Optional

logger = logging.getLogger(__name__)


def get_onedrive_path() -> Optional[Path]:
    """Detect the OneDrive folder path on this machine."""
    system = platform.system()

    if system == "Windows":
        # Common OneDrive paths on Windows
        candidates = [
            Path(os.environ.get("OneDrive", "")) if os.environ.get("OneDrive") else None,
            Path(os.environ.get("OneDriveCommercial", "")) if os.environ.get("OneDriveCommercial") else None,
            Path.home() / "OneDrive",
            Path.home() / "OneDrive - Blue Bear Group Corp",
            Path.home() / "OneDrive - BBG Corp",
        ]
    elif system == "Darwin":  # macOS
        candidates = [
            Path.home() / "Library" / "CloudStorage" / "OneDrive-Personal",
            Path.home() / "OneDrive",
        ]
    else:  # Linux
        candidates = [
            Path.home() / "OneDrive",
        ]

    for path in candidates:
        if path and path.exists():
            return path

    return None


def get_sync_folder(config: dict) -> Optional[Path]:
    """Get or create the BBG Deal Scout sync folder inside OneDrive."""
    # Check config for explicit path
    sync_path = config.get("onedrive", {}).get("sync_folder")
    if sync_path:
        folder = Path(sync_path)
        folder.mkdir(parents=True, exist_ok=True)
        return folder

    # Auto-detect OneDrive
    onedrive = get_onedrive_path()
    if not onedrive:
        logger.warning("OneDrive folder not detected. Set onedrive.sync_folder in config.yaml")
        return None

    folder = onedrive / "BBG Deal Scout"
    folder.mkdir(parents=True, exist_ok=True)
    return folder


def sync_to_onedrive(config: dict, new_listings: List, scan_stats: dict):
    """Sync scan results to OneDrive shared folder."""
    sync_folder = get_sync_folder(config)
    if not sync_folder:
        logger.info("OneDrive sync skipped — no sync folder configured or detected")
        return

    logger.info(f"Syncing results to OneDrive: {sync_folder}")

    try:
        # 1. Export latest listings to CSV
        csv_path = _export_daily_csv(sync_folder)

        # 2. Copy database for dashboard access
        db_path = config.get("general", {}).get("database_path", "data/deal_scout.db")
        if Path(db_path).exists():
            dest_db = sync_folder / "deal_scout.db"
            shutil.copy2(db_path, dest_db)
            logger.debug(f"Database synced to {dest_db}")

        # 3. Generate HTML summary report
        _generate_summary_report(sync_folder, new_listings, scan_stats)

        logger.info(f"OneDrive sync complete → {sync_folder}")

    except Exception as e:
        logger.error(f"OneDrive sync failed: {e}")


def _export_daily_csv(sync_folder: Path) -> Optional[Path]:
    """Export today's listings to a dated CSV in the sync folder."""
    from .database import get_listings

    today = date.today().strftime("%Y-%m-%d")
    csv_path = sync_folder / f"listings_{today}.csv"

    # Also maintain a "latest" file that always has current data
    latest_path = sync_folder / "listings_latest.csv"

    listings = get_listings(limit=10000)
    if not listings:
        return None

    fields = [
        "id", "title", "source", "source_url", "source_label",
        "address", "city", "region", "province",
        "asking_price", "price_per_unit", "listed_cap_rate",
        "num_units", "year_built", "occupancy",
        "tier1_score", "score_status", "status",
        "notes", "flagged", "discovered_at", "last_seen_at",
    ]

    for target in [csv_path, latest_path]:
        with open(target, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            for l in listings:
                row = {field: getattr(l, field, "") for field in fields}
                writer.writerow(row)

    logger.debug(f"CSV exported: {csv_path}")
    return csv_path


def _generate_summary_report(sync_folder: Path, new_listings: List, stats: dict):
    """Generate an HTML summary report viewable by team in OneDrive."""
    today = date.today().strftime("%B %d, %Y")
    now = datetime.now().strftime("%I:%M %p")

    rows_html = ""
    for l in new_listings[:30]:
        title = l.title if hasattr(l, "title") else l.get("title", "N/A")
        url = l.source_url if hasattr(l, "source_url") else l.get("source_url", "#")
        region = l.region if hasattr(l, "region") else l.get("region", "")
        price = l.asking_price if hasattr(l, "asking_price") else l.get("asking_price")
        units = l.num_units if hasattr(l, "num_units") else l.get("num_units")
        cap = l.listed_cap_rate if hasattr(l, "listed_cap_rate") else l.get("listed_cap_rate")
        score = l.tier1_score if hasattr(l, "tier1_score") else l.get("tier1_score")
        status = l.status if hasattr(l, "status") else l.get("status", "new")

        price_str = f"${price:,.0f}" if price else "—"
        units_str = str(units) if units else "—"
        cap_str = f"{cap:.1f}%" if cap else "—"
        score_str = f"{score}/7" if score is not None else "—"
        region_str = "Edmonton" if "edmonton" in region else "Montreal" if "montreal" in region else region

        rows_html += f"""
        <tr>
            <td><a href="{url}" target="_blank">{title[:70]}</a></td>
            <td>{region_str}</td>
            <td style="text-align:right;">{price_str}</td>
            <td style="text-align:center;">{units_str}</td>
            <td style="text-align:center;">{cap_str}</td>
            <td style="text-align:center;">{score_str}</td>
            <td>{status}</td>
        </tr>"""

    if not rows_html:
        rows_html = '<tr><td colspan="7" style="text-align:center;padding:20px;color:#888;">No new listings found today.</td></tr>'

    html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>BBG Deal Scout — {today}</title>
    <style>
        body {{ font-family: Calibri, -apple-system, sans-serif; max-width: 900px; margin: 20px auto; padding: 0 20px; color: #333; }}
        h1 {{ color: #005AAA; font-size: 22px; border-bottom: 3px solid #005AAA; padding-bottom: 8px; }}
        .meta {{ color: #888; font-size: 13px; margin-bottom: 20px; }}
        .stats {{ display: flex; gap: 20px; margin-bottom: 20px; }}
        .stat {{ background: #f0f4f8; padding: 12px 16px; border-radius: 6px; text-align: center; }}
        .stat .num {{ font-size: 24px; font-weight: 700; color: #005AAA; }}
        .stat .lbl {{ font-size: 11px; color: #888; text-transform: uppercase; }}
        table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
        th {{ background: #f8fafc; padding: 8px; text-align: left; border-bottom: 2px solid #005AAA; font-size: 11px; text-transform: uppercase; color: #888; }}
        td {{ padding: 8px; border-bottom: 1px solid #eee; }}
        a {{ color: #005AAA; text-decoration: none; }}
        a:hover {{ text-decoration: underline; }}
        .footer {{ margin-top: 30px; font-size: 11px; color: #aaa; text-align: center; }}
    </style>
</head>
<body>
    <h1>BBG Deal Scout — Daily Report</h1>
    <div class="meta">{today} at {now} | Auto-generated by Deal Scout</div>

    <div class="stats">
        <div class="stat"><div class="num">{len(new_listings)}</div><div class="lbl">New Today</div></div>
        <div class="stat"><div class="num">{stats.get('total_found', 0)}</div><div class="lbl">Total Scanned</div></div>
        <div class="stat"><div class="num">{stats.get('duplicates', 0)}</div><div class="lbl">Duplicates</div></div>
        <div class="stat"><div class="num">{stats.get('errors', 0)}</div><div class="lbl">Errors</div></div>
    </div>

    <h2 style="font-size:16px;color:#333;">New Listings</h2>
    <table>
        <thead>
            <tr><th>Listing</th><th>Region</th><th style="text-align:right;">Price</th><th style="text-align:center;">Units</th><th style="text-align:center;">Cap Rate</th><th style="text-align:center;">Score</th><th>Status</th></tr>
        </thead>
        <tbody>
            {rows_html}
        </tbody>
    </table>

    <p style="margin-top:16px;font-size:12px;color:#888;">
        Full data available in <strong>listings_latest.csv</strong> in this folder.<br>
        Open the Deal Scout dashboard for filtering, scoring, and team notes.
    </p>

    <div class="footer">
        Blue Bear Group Corp. — Deal Scout Automated Report<br>
        Review all listings before taking action. This is not investment advice.
    </div>
</body>
</html>"""

    report_path = sync_folder / "daily_report.html"
    report_path.write_text(html, encoding="utf-8")
    logger.debug(f"Summary report generated: {report_path}")
