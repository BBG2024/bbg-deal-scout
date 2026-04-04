"""Email Alert Parser: parses listing alerts from Realtor.ca, Centris, LoopNet."""

import logging
import email
import re
from typing import List, Dict, Optional
from datetime import datetime, timedelta
from email.header import decode_header
from bs4 import BeautifulSoup

try:
    from imapclient import IMAPClient
    HAS_IMAP = True
except ImportError:
    HAS_IMAP = False

from .base import BaseCollector

logger = logging.getLogger(__name__)


class EmailAlertCollector(BaseCollector):
    """Parses property listing email alerts via IMAP."""

    name = "email_alert"

    def __init__(self, config: dict, filters: dict):
        super().__init__(config, filters)
        self.email_cfg = config.get("email_parsing", {})

    def collect(self, region_key: str, region_config: dict) -> List[Dict]:
        """Connect to IMAP and parse recent alert emails."""
        if not self.email_cfg.get("enabled"):
            logger.debug("Email parsing disabled in config")
            return []

        if not HAS_IMAP:
            logger.error("imapclient not installed — pip install imapclient")
            return []

        server = self.email_cfg.get("imap_server", "")
        port = self.email_cfg.get("imap_port", 993)
        user = self.email_cfg.get("email_address", "")
        password = self.email_cfg.get("email_password", "")

        if not all([server, user, password]) or password == "YOUR_APP_PASSWORD":
            logger.warning("Email credentials not configured — skipping email parsing")
            return []

        listings = []
        senders = self.email_cfg.get("alert_senders", [])

        try:
            with IMAPClient(server, port=port, ssl=True) as client:
                client.login(user, password)
                client.select_folder("INBOX", readonly=True)

                # Search for emails from alert senders in last 24 hours
                since_date = (datetime.now() - timedelta(days=1)).date()

                for sender in senders:
                    try:
                        msg_ids = client.search([
                            "FROM", sender,
                            "SINCE", since_date,
                            "UNSEEN",
                        ])

                        if not msg_ids:
                            logger.debug(f"No new alerts from {sender}")
                            continue

                        # Fetch up to 50 most recent
                        msg_ids = msg_ids[-50:]
                        messages = client.fetch(msg_ids, ["RFC822"])

                        for msg_id, data in messages.items():
                            raw = data[b"RFC822"]
                            parsed = self._parse_alert_email(raw, sender, region_key)
                            listings.extend(parsed)

                    except Exception as e:
                        logger.error(f"Failed to process emails from {sender}: {e}")
                        self.errors.append(f"Email: {sender} — {str(e)}")

        except Exception as e:
            logger.error(f"IMAP connection failed: {e}")
            self.errors.append(f"Email IMAP: {str(e)}")

        logger.info(f"Email alerts: {len(listings)} listings parsed")
        return listings

    def _parse_alert_email(
        self, raw_email: bytes, sender: str, region_key: str
    ) -> List[Dict]:
        """Parse a single alert email into listings."""
        msg = email.message_from_bytes(raw_email)
        html_body = self._get_html_body(msg)

        if not html_body:
            return []

        soup = BeautifulSoup(html_body, "lxml")

        if "realtor.ca" in sender:
            return self._parse_realtor_ca(soup, region_key)
        elif "centris.ca" in sender:
            return self._parse_centris(soup, region_key)
        elif "loopnet.com" in sender:
            return self._parse_loopnet(soup, region_key)
        else:
            return self._parse_generic(soup, region_key)

    def _parse_realtor_ca(self, soup: BeautifulSoup, region_key: str) -> List[Dict]:
        """Parse Realtor.ca alert email format."""
        listings = []

        # Realtor.ca alerts typically have property cards with links
        for link in soup.find_all("a", href=re.compile(r"realtor\.ca.*listing", re.I)):
            href = link.get("href", "")
            # Get surrounding text for details
            parent = link.find_parent(["td", "div", "tr"])
            text = parent.get_text(" ", strip=True) if parent else link.get_text(strip=True)

            listing = {
                "source": self.name,
                "source_url": href,
                "source_label": "Email: Realtor.ca Alert",
                "title": link.get_text(strip=True)[:300] or "Realtor.ca listing",
                "region": region_key,
                "num_units": self.extract_units(text),
                "asking_price": self.extract_price(text),
                "listed_cap_rate": self.extract_cap_rate(text),
                "discovered_at": datetime.utcnow(),
            }

            detected = self.classify_region(text)
            if detected:
                listing["region"] = detected

            if self.passes_filters(listing):
                listings.append(listing)

        return listings

    def _parse_centris(self, soup: BeautifulSoup, region_key: str) -> List[Dict]:
        """Parse Centris.ca alert email format."""
        listings = []

        for link in soup.find_all("a", href=re.compile(r"centris\.ca", re.I)):
            href = link.get("href", "")
            parent = link.find_parent(["td", "div", "tr"])
            text = parent.get_text(" ", strip=True) if parent else link.get_text(strip=True)

            listing = {
                "source": self.name,
                "source_url": href,
                "source_label": "Email: Centris Alert",
                "title": link.get_text(strip=True)[:300] or "Centris listing",
                "region": region_key,
                "num_units": self.extract_units(text),
                "asking_price": self.extract_price(text),
                "listed_cap_rate": self.extract_cap_rate(text),
                "discovered_at": datetime.utcnow(),
            }

            detected = self.classify_region(text)
            if detected:
                listing["region"] = detected

            if self.passes_filters(listing):
                listings.append(listing)

        return listings

    def _parse_loopnet(self, soup: BeautifulSoup, region_key: str) -> List[Dict]:
        """Parse LoopNet alert email format."""
        listings = []

        for link in soup.find_all("a", href=re.compile(r"loopnet\.com", re.I)):
            href = link.get("href", "")
            parent = link.find_parent(["td", "div", "tr"])
            text = parent.get_text(" ", strip=True) if parent else link.get_text(strip=True)

            listing = {
                "source": self.name,
                "source_url": href,
                "source_label": "Email: LoopNet Alert",
                "title": link.get_text(strip=True)[:300] or "LoopNet listing",
                "region": region_key,
                "num_units": self.extract_units(text),
                "asking_price": self.extract_price(text),
                "listed_cap_rate": self.extract_cap_rate(text),
                "discovered_at": datetime.utcnow(),
            }

            detected = self.classify_region(text)
            if detected:
                listing["region"] = detected

            if self.passes_filters(listing):
                listings.append(listing)

        return listings

    def _parse_generic(self, soup: BeautifulSoup, region_key: str) -> List[Dict]:
        """Generic parser for unknown email formats."""
        listings = []

        for link in soup.find_all("a", href=True):
            href = link.get("href", "")
            text = link.get_text(strip=True)
            if len(text) < 10:
                continue

            combined = f"{text} {href}"
            if not any(
                kw in combined.lower()
                for kw in ["property", "listing", "sale", "vendre", "unit", "plex"]
            ):
                continue

            listing = {
                "source": self.name,
                "source_url": href,
                "source_label": "Email: Generic Alert",
                "title": text[:300],
                "region": region_key,
                "num_units": self.extract_units(combined),
                "asking_price": self.extract_price(combined),
                "listed_cap_rate": self.extract_cap_rate(combined),
                "discovered_at": datetime.utcnow(),
            }

            if self.passes_filters(listing):
                listings.append(listing)

        return listings

    @staticmethod
    def _get_html_body(msg) -> Optional[str]:
        """Extract HTML body from email message."""
        if msg.is_multipart():
            for part in msg.walk():
                ctype = part.get_content_type()
                if ctype == "text/html":
                    payload = part.get_payload(decode=True)
                    charset = part.get_content_charset() or "utf-8"
                    return payload.decode(charset, errors="replace")
        elif msg.get_content_type() == "text/html":
            payload = msg.get_payload(decode=True)
            charset = msg.get_content_charset() or "utf-8"
            return payload.decode(charset, errors="replace")
        return None
