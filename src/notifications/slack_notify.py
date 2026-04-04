"""Slack notification sender for BBG Deal Scout."""

import logging
import json
import requests
from typing import List
from datetime import date

logger = logging.getLogger(__name__)


def send_slack_notification(config: dict, listings: List, scan_stats: dict):
    """Send a Slack notification with new listings summary."""
    slack_cfg = config.get("notifications", {}).get("slack", {})

    if not slack_cfg.get("enabled"):
        logger.debug("Slack notifications disabled")
        return

    webhook_url = slack_cfg.get("webhook_url", "")
    if not webhook_url or "YOUR" in webhook_url:
        logger.warning("Slack webhook not configured — skipping")
        return

    today = date.today().strftime("%b %d, %Y")
    n = len(listings)

    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"🏢 Deal Scout — {n} New Listing{'s' if n != 1 else ''} — {today}",
            },
        },
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": (
                        f"Queries: {scan_stats.get('queries_run', 0)} | "
                        f"Dupes skipped: {scan_stats.get('duplicates', 0)} | "
                        f"Errors: {scan_stats.get('errors', 0)}"
                    ),
                }
            ],
        },
        {"type": "divider"},
    ]

    # Add up to 10 listings (Slack block limit)
    for l in listings[:10]:
        title = l.title if hasattr(l, "title") else l.get("title", "N/A")
        url = l.source_url if hasattr(l, "source_url") else l.get("source_url", "#")
        region = l.region if hasattr(l, "region") else l.get("region", "")
        price = l.asking_price if hasattr(l, "asking_price") else l.get("asking_price")
        units = l.num_units if hasattr(l, "num_units") else l.get("num_units")
        cap = l.listed_cap_rate if hasattr(l, "listed_cap_rate") else l.get("listed_cap_rate")
        score = l.tier1_score if hasattr(l, "tier1_score") else l.get("tier1_score")

        region_label = "Edmonton" if "edmonton" in region else "Montreal" if "montreal" in region else region

        details = []
        if price:
            details.append(f"💰 ${price:,.0f}")
        if units:
            details.append(f"🏠 {units} units")
        if cap:
            details.append(f"📊 {cap:.1f}% cap")
        if score is not None:
            details.append(f"⭐ {score}/7")

        detail_str = " | ".join(details) if details else "No details extracted"

        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*<{url}|{title[:60]}>*\n📍 {region_label}\n{detail_str}",
            },
        })

    if n > 10:
        blocks.append({
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": f"_...and {n - 10} more. Check the dashboard._"}],
        })

    dashboard_port = scan_stats.get("dashboard_port", 8050)
    blocks.append({"type": "divider"})
    blocks.append({
        "type": "actions",
        "elements": [
            {
                "type": "button",
                "text": {"type": "plain_text", "text": "Open Dashboard"},
                "url": f"http://localhost:{dashboard_port}",
            }
        ],
    })

    payload = {"blocks": blocks}

    try:
        resp = requests.post(webhook_url, json=payload, timeout=10)
        resp.raise_for_status()
        logger.info("Slack notification sent")
    except Exception as e:
        logger.error(f"Slack notification failed: {e}")
