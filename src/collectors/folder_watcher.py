"""Folder Watcher: monitors a local directory for broker packages, PDFs, listing docs.

Drop a PDF, DOCX, or TXT file from a broker into the watch folder
and Deal Scout will extract listing data from it on the next scan.
"""

import logging
import hashlib
import re
import os
from typing import List, Dict, Optional
from datetime import datetime
from pathlib import Path

from .base import BaseCollector

logger = logging.getLogger(__name__)

# Supported file types
SUPPORTED_EXTENSIONS = {".pdf", ".txt", ".csv", ".html", ".htm", ".docx", ".msg", ".eml"}


class FolderWatcherCollector(BaseCollector):
    """Monitors a local folder for dropped listing documents."""

    name = "folder_watch"

    def __init__(self, config: dict, filters: dict):
        super().__init__(config, filters)
        folder_cfg = config.get("folder_watch", {})
        self.watch_dir = Path(folder_cfg.get("directory", "data/inbox"))
        self.processed_dir = Path(folder_cfg.get("processed_directory", "data/inbox/processed"))
        self.enabled = folder_cfg.get("enabled", True)

    def collect(self, region_key: str, region_config: dict) -> List[Dict]:
        """Scan watch folder for new files and extract listing data."""
        if not self.enabled:
            return []

        # Only run once (not per-region) — use a flag
        if hasattr(self, "_already_ran") and self._already_ran:
            return []
        self._already_ran = True

        # Create directories if they don't exist
        self.watch_dir.mkdir(parents=True, exist_ok=True)
        self.processed_dir.mkdir(parents=True, exist_ok=True)

        listings = []

        for filepath in self.watch_dir.iterdir():
            if not filepath.is_file():
                continue
            if filepath.suffix.lower() not in SUPPORTED_EXTENSIONS:
                continue
            if filepath.name.startswith("."):
                continue

            try:
                file_listings = self._process_file(filepath, region_key)
                listings.extend(file_listings)

                # Move to processed folder
                dest = self.processed_dir / filepath.name
                if dest.exists():
                    # Add timestamp to avoid overwrite
                    stem = filepath.stem
                    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                    dest = self.processed_dir / f"{stem}_{ts}{filepath.suffix}"
                filepath.rename(dest)
                logger.info(f"Processed and moved: {filepath.name} → processed/")

            except Exception as e:
                logger.error(f"Failed to process {filepath.name}: {e}")
                self.errors.append(f"FolderWatch: {filepath.name} — {str(e)}")

        if listings:
            logger.info(f"Folder watcher: {len(listings)} listings extracted from inbox")
        return listings

    def _process_file(self, filepath: Path, default_region: str) -> List[Dict]:
        """Extract listing data from a single file."""
        ext = filepath.suffix.lower()
        text = ""

        if ext == ".txt":
            text = filepath.read_text(encoding="utf-8", errors="replace")

        elif ext in (".html", ".htm"):
            from bs4 import BeautifulSoup
            raw = filepath.read_text(encoding="utf-8", errors="replace")
            soup = BeautifulSoup(raw, "lxml")
            text = soup.get_text(" ", strip=True)

        elif ext == ".csv":
            text = filepath.read_text(encoding="utf-8", errors="replace")

        elif ext == ".pdf":
            text = self._extract_pdf_text(filepath)

        elif ext == ".docx":
            text = self._extract_docx_text(filepath)

        elif ext in (".eml", ".msg"):
            text = self._extract_email_text(filepath)

        if not text or len(text) < 50:
            logger.warning(f"No usable text extracted from {filepath.name}")
            return []

        return self._parse_text_to_listings(text, filepath, default_region)

    def _extract_pdf_text(self, filepath: Path) -> str:
        """Extract text from PDF using available libraries."""
        try:
            import subprocess
            result = subprocess.run(
                ["pdftotext", "-layout", str(filepath), "-"],
                capture_output=True, text=True, timeout=30,
            )
            if result.returncode == 0 and result.stdout.strip():
                return result.stdout
        except (FileNotFoundError, subprocess.TimeoutExpired):
            pass

        # Fallback: try PyPDF2 or pdfplumber
        try:
            from PyPDF2 import PdfReader
            reader = PdfReader(str(filepath))
            pages = [page.extract_text() or "" for page in reader.pages]
            return "\n".join(pages)
        except ImportError:
            pass

        try:
            import pdfplumber
            with pdfplumber.open(filepath) as pdf:
                pages = [page.extract_text() or "" for page in pdf.pages]
                return "\n".join(pages)
        except ImportError:
            pass

        logger.warning(f"No PDF library available for {filepath.name}. Install PyPDF2 or pdfplumber.")
        return ""

    def _extract_docx_text(self, filepath: Path) -> str:
        """Extract text from DOCX."""
        try:
            from docx import Document
            doc = Document(str(filepath))
            return "\n".join(p.text for p in doc.paragraphs)
        except ImportError:
            logger.warning("python-docx not installed. Run: pip install python-docx")
            return ""

    def _extract_email_text(self, filepath: Path) -> str:
        """Extract text from .eml files."""
        import email
        with open(filepath, "rb") as f:
            msg = email.message_from_binary_file(f)

        parts = []
        if msg.is_multipart():
            for part in msg.walk():
                if part.get_content_type() == "text/plain":
                    payload = part.get_payload(decode=True)
                    charset = part.get_content_charset() or "utf-8"
                    parts.append(payload.decode(charset, errors="replace"))
        else:
            payload = msg.get_payload(decode=True)
            charset = msg.get_content_charset() or "utf-8"
            parts.append(payload.decode(charset, errors="replace"))

        return "\n".join(parts)

    def _parse_text_to_listings(
        self, text: str, filepath: Path, default_region: str
    ) -> List[Dict]:
        """Parse extracted text into one or more listing dicts."""
        listings = []

        # Try to detect multiple listings in the document
        # Split on common delimiters
        sections = re.split(
            r"(?:^|\n)[-=]{10,}|(?:^|\n)(?:Listing|Property|MLS)\s*#?\s*\d+|(?:^|\n)\d+\.\s+(?=\d+\s+(?:unit|suite))",
            text, flags=re.IGNORECASE
        )

        if len(sections) <= 1:
            sections = [text]

        for section in sections:
            if len(section.strip()) < 30:
                continue

            listing = self._extract_listing_from_text(section, filepath, default_region)
            if listing:
                listings.append(listing)

        # If nothing was extracted with structured parsing, create a single entry
        if not listings and len(text) >= 50:
            listing = {
                "source": self.name,
                "source_url": f"file://{filepath}",
                "source_label": f"Inbox: {filepath.name}",
                "title": f"[Document] {filepath.stem[:200]}",
                "region": default_region,
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

    def _extract_listing_from_text(
        self, text: str, filepath: Path, default_region: str
    ) -> Optional[Dict]:
        """Extract a single listing from a text section."""
        units = self.extract_units(text)
        price = self.extract_price(text)
        cap = self.extract_cap_rate(text)

        # Need at least some signal that this is a property listing
        if units is None and price is None and cap is None:
            # Check for listing keywords
            keywords = ["for sale", "à vendre", "revenue", "multifamily", "apartment", "plex", "logement"]
            if not any(kw in text.lower() for kw in keywords):
                return None

        # Try to extract an address
        address = self._extract_address(text)

        # Build title from available data
        title_parts = []
        if address:
            title_parts.append(address[:100])
        if units:
            title_parts.append(f"{units}-unit")
        if not title_parts:
            # Use first meaningful line of text
            first_line = text.strip().split("\n")[0][:150]
            title_parts.append(first_line)

        title = " — ".join(title_parts) or f"Listing from {filepath.name}"

        listing = {
            "source": self.name,
            "source_url": f"file://{filepath}",
            "source_label": f"Inbox: {filepath.name}",
            "title": title[:300],
            "address": address,
            "region": default_region,
            "num_units": units,
            "asking_price": price,
            "listed_cap_rate": cap,
            "discovered_at": datetime.utcnow(),
        }

        detected = self.classify_region(text)
        if detected:
            listing["region"] = detected

        return listing

    @staticmethod
    def _extract_address(text: str) -> Optional[str]:
        """Try to extract a street address from text."""
        # Canadian address patterns
        patterns = [
            r"(\d{1,5}\s+[\w\s]{2,30}(?:Street|St|Avenue|Ave|Boulevard|Blvd|Road|Rd|Drive|Dr|Way|Crescent|Cres|Place|Pl|Court|Ct|Lane|Ln)\.?(?:\s*(?:#|Unit|Apt|Suite)?\s*\d{0,5})?)",
            r"(\d{1,5}\s+(?:rue|avenue|boulevard|chemin|place|côte)\s+[\w\s\-']{2,40})",
        ]
        for pat in patterns:
            m = re.search(pat, text, re.IGNORECASE)
            if m:
                return m.group(1).strip()
        return None
