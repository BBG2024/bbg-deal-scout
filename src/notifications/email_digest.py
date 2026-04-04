"""Email digest sender for BBG Deal Scout."""

import logging
import smtplib
import json
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, date
from typing import List

logger = logging.getLogger(__name__)


def send_email_digest(config: dict, listings: List, scan_stats: dict):
    """Send a morning digest email with new listings found."""
    email_cfg = config.get("notifications", {}).get("email", {})

    if not email_cfg.get("enabled"):
        logger.debug("Email notifications disabled")
        return

    recipients = email_cfg.get("recipients", [])
    if not recipients:
        logger.warning("No email recipients configured")
        return

    smtp_server = email_cfg.get("smtp_server", "smtp.gmail.com")
    smtp_port = email_cfg.get("smtp_port", 587)
    sender = email_cfg.get("sender_email", "")
    password = email_cfg.get("sender_password", "")

    if not sender or password == "YOUR_APP_PASSWORD":
        logger.warning("Email credentials not configured — skipping digest")
        return

    prefix = email_cfg.get("subject_prefix", "BBG Deal Scout")
    today = date.today().strftime("%b %d, %Y")
    new_count = len(listings)

    subject = f"{prefix} — {new_count} New Listing{'s' if new_count != 1 else ''} — {today}"

    html_body = _build_html_digest(listings, scan_stats, today)

    for recipient in recipients:
        try:
            msg = MIMEMultipart("alternative")
            msg["Subject"] = subject
            msg["From"] = f"BBG Deal Scout <{sender}>"
            msg["To"] = recipient

            # Plain text fallback
            text_body = _build_text_digest(listings, scan_stats, today)
            msg.attach(MIMEText(text_body, "plain"))
            msg.attach(MIMEText(html_body, "html"))

            with smtplib.SMTP(smtp_server, smtp_port) as server:
                server.ehlo()
                server.starttls()
                server.login(sender, password)
                server.send_message(msg)

            logger.info(f"Digest email sent to {recipient}")

        except Exception as e:
            logger.error(f"Failed to send email to {recipient}: {e}")


def _build_html_digest(listings: List, stats: dict, today: str) -> str:
    """Build HTML email body."""
    rows = ""
    for l in listings:
        # Parse score details if available
        score_badge = ""
        if hasattr(l, "tier1_score") and l.tier1_score is not None:
            score = l.tier1_score
            color = "#27ae60" if score >= 4 else "#f39c12" if score >= 2 else "#e74c3c"
            score_badge = f'<span style="background:{color};color:white;padding:2px 8px;border-radius:3px;font-size:12px;">{score}/7</span>'
        else:
            score_badge = '<span style="background:#bdc3c7;color:white;padding:2px 8px;border-radius:3px;font-size:12px;">—</span>'

        title = l.title if hasattr(l, "title") else l.get("title", "N/A")
        url = l.source_url if hasattr(l, "source_url") else l.get("source_url", "#")
        region = l.region if hasattr(l, "region") else l.get("region", "")
        source = l.source_label if hasattr(l, "source_label") else l.get("source_label", "")
        price = l.asking_price if hasattr(l, "asking_price") else l.get("asking_price")
        units = l.num_units if hasattr(l, "num_units") else l.get("num_units")
        cap = l.listed_cap_rate if hasattr(l, "listed_cap_rate") else l.get("listed_cap_rate")

        price_str = f"${price:,.0f}" if price else "—"
        units_str = f"{units} units" if units else "—"
        cap_str = f"{cap:.1f}%" if cap else "—"
        region_label = "Edmonton" if "edmonton" in region else "Montreal" if "montreal" in region else region

        rows += f"""
        <tr style="border-bottom:1px solid #eee;">
            <td style="padding:12px 8px;">
                <a href="{url}" style="color:#005AAA;text-decoration:none;font-weight:600;">{title[:80]}</a>
                <br><small style="color:#888;">{source}</small>
            </td>
            <td style="padding:12px 8px;text-align:center;"><span style="background:#f0f4f8;padding:3px 8px;border-radius:3px;font-size:12px;">{region_label}</span></td>
            <td style="padding:12px 8px;text-align:right;">{price_str}</td>
            <td style="padding:12px 8px;text-align:center;">{units_str}</td>
            <td style="padding:12px 8px;text-align:center;">{cap_str}</td>
            <td style="padding:12px 8px;text-align:center;">{score_badge}</td>
        </tr>
        """

    if not rows:
        rows = '<tr><td colspan="6" style="padding:20px;text-align:center;color:#888;">No new listings found today.</td></tr>'

    dashboard_url = f"http://localhost:{stats.get('dashboard_port', 8050)}"

    return f"""
    <html>
    <body style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;max-width:800px;margin:0 auto;padding:20px;">
        <div style="background:#005AAA;color:white;padding:20px;border-radius:8px 8px 0 0;">
            <h1 style="margin:0;font-size:20px;">BBG Deal Scout — Daily Digest</h1>
            <p style="margin:5px 0 0;opacity:0.9;">{today} — {len(listings)} new listing{'s' if len(listings) != 1 else ''} found</p>
        </div>

        <div style="background:#f8f9fa;padding:15px;border:1px solid #e0e0e0;">
            <table style="width:100%;font-size:13px;color:#555;">
                <tr>
                    <td>Queries run: <strong>{stats.get('queries_run', 0)}</strong></td>
                    <td>Duplicates skipped: <strong>{stats.get('duplicates', 0)}</strong></td>
                    <td>Errors: <strong>{stats.get('errors', 0)}</strong></td>
                </tr>
            </table>
        </div>

        <table style="width:100%;border-collapse:collapse;font-size:14px;margin-top:10px;">
            <thead>
                <tr style="background:#f0f4f8;border-bottom:2px solid #005AAA;">
                    <th style="padding:10px 8px;text-align:left;">Listing</th>
                    <th style="padding:10px 8px;text-align:center;">Region</th>
                    <th style="padding:10px 8px;text-align:right;">Price</th>
                    <th style="padding:10px 8px;text-align:center;">Units</th>
                    <th style="padding:10px 8px;text-align:center;">Cap Rate</th>
                    <th style="padding:10px 8px;text-align:center;">Score</th>
                </tr>
            </thead>
            <tbody>
                {rows}
            </tbody>
        </table>

        <div style="margin-top:20px;padding:15px;background:#f0f4f8;border-radius:4px;text-align:center;">
            <a href="{dashboard_url}" style="color:#005AAA;text-decoration:none;font-weight:600;">
                Open Deal Scout Dashboard →
            </a>
        </div>

        <p style="margin-top:20px;font-size:11px;color:#aaa;text-align:center;">
            Blue Bear Group Corp. — Deal Scout Automated Alert<br>
            This email was generated automatically. Review all listings before taking action.
        </p>
    </body>
    </html>
    """


def _build_text_digest(listings: List, stats: dict, today: str) -> str:
    """Build plain text fallback."""
    lines = [
        f"BBG Deal Scout — Daily Digest — {today}",
        f"{len(listings)} new listing(s) found",
        f"Queries: {stats.get('queries_run', 0)} | Dupes: {stats.get('duplicates', 0)} | Errors: {stats.get('errors', 0)}",
        "",
        "=" * 60,
    ]

    for l in listings:
        title = l.title if hasattr(l, "title") else l.get("title", "N/A")
        url = l.source_url if hasattr(l, "source_url") else l.get("source_url", "#")
        price = l.asking_price if hasattr(l, "asking_price") else l.get("asking_price")
        units = l.num_units if hasattr(l, "num_units") else l.get("num_units")

        price_str = f"${price:,.0f}" if price else "N/A"
        units_str = f"{units} units" if units else "N/A"

        lines.extend([
            f"\n{title[:80]}",
            f"  Price: {price_str} | Units: {units_str}",
            f"  URL: {url}",
        ])

    return "\n".join(lines)
