"""Base collector class for all data sources."""

import json
import logging
import re
import time
from abc import ABC, abstractmethod
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)

# Known real estate listing platforms — safe to fetch individual pages from
LISTING_DOMAINS = [
    "realtor.ca", "centris.ca", "loopnet.com", "remax.ca",
    "royallepage.ca", "kijiji.ca", "marcusmillichap.com",
    "cbre.ca", "colliers.com", "naicommercial", "point2homes",
    "zolo.ca", "duproprio.com", "commercialx.ca", "icx.ca",
    "propertyguys.com", "comfree.com", "jll.ca",
]

# Keywords in title/text that flag a non-qualifying property
SMALL_PROPERTY_KEYWORDS = [
    "duplex", "triplex", "quadruplex", "quadplex", "fourplex",
    "semi-detached", "semi detached", "single family", "single-family",
    "bungalow", "house for sale", "maison à vendre", "townhouse",
    "town house", "cottage", "chalet", "commercial land", "vacant lot",
    "terrain à vendre", "warehouse", "retail space", "office space", "bureau",
]

# Word-form plex names → unit count
PLEX_WORDS = {
    "duplex": 2, "triplex": 3, "quadruplex": 4, "quadplex": 4,
    "fourplex": 4, "4-plex": 4, "fiveplex": 5, "5-plex": 5,
    "sixplex": 6, "6-plex": 6, "cinqplex": 5,
}

_FETCH_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-CA,en;q=0.9,fr-CA;q=0.8",
}


class BaseCollector(ABC):
    """Abstract base for all listing collectors."""

    name: str = "base"

    def __init__(self, config: dict, filters: dict):
        self.config = config
        self.filters = filters
        self.results: List[Dict] = []
        self.errors: List[str] = []

    @abstractmethod
    def collect(self, region_key: str, region_config: dict) -> List[Dict]:
        pass

    # ------------------------------------------------------------------
    # Filter
    # ------------------------------------------------------------------

    def passes_filters(self, listing: dict) -> bool:
        """Check if a listing passes basic filter criteria."""
        min_units = self.filters.get("min_units", 5)
        max_units = self.filters.get("max_units", 50)

        units = listing.get("num_units")
        if units is not None:
            if units < min_units:
                logger.debug(f"Rejected (units {units} < {min_units}): {listing.get('title','')[:60]}")
                return False
            if units > max_units:
                logger.debug(f"Rejected (units {units} > {max_units}): {listing.get('title','')[:60]}")
                return False
        else:
            combined = (
                (listing.get("title") or "") + " " +
                (listing.get("address") or "") + " " +
                (listing.get("source_url") or "")
            ).lower()
            for kw in SMALL_PROPERTY_KEYWORDS:
                if kw in combined:
                    logger.debug(f"Rejected (keyword '{kw}'): {listing.get('title','')[:60]}")
                    return False

        price = listing.get("asking_price")
        if price is not None:
            if price < self.filters.get("min_price", 0):
                return False
            if price > self.filters.get("max_price", float("inf")):
                return False

        return True

    # ------------------------------------------------------------------
    # Page enrichment — main entry point
    # ------------------------------------------------------------------

    def enrich_from_url(self, listing: dict) -> dict:
        """
        Fetch the listing's source page and populate all financial / property fields.

        Extraction order:
          1. Site-specific API  (realtor.ca, centris.ca)
          2. JSON-LD structured data  (<script type="application/ld+json">)
          3. Open Graph / meta tags
          4. Embedded JSON in <script> tags (__NEXT_DATA__, window.__data__, etc.)
          5. Full-text regex patterns (trafilatura clean text)

        Populates: num_units, asking_price, listed_cap_rate, estimated_noi,
                   price_per_unit, building_sf, year_built, occupancy,
                   asset_type, address, city.
        """
        url = listing.get("source_url", "")
        if not url:
            return listing
        if not any(domain in url.lower() for domain in LISTING_DOMAINS):
            return listing

        try:
            import requests as _req
            resp = _req.get(url, headers=_FETCH_HEADERS, timeout=14, allow_redirects=True)
            resp.raise_for_status()
            html = resp.text

            # 1. Site-specific APIs
            if "realtor.ca" in url.lower():
                listing = self._enrich_realtor_ca(listing, url, html)
            elif "centris.ca" in url.lower():
                listing = self._enrich_centris(listing, html)

            # 2. JSON-LD
            listing = self._enrich_from_jsonld(listing, html)

            # 3. Open Graph / meta
            listing = self._enrich_from_meta(listing, html)

            # 4. Embedded JS data blobs
            listing = self._enrich_from_script_json(listing, html)

            # 5. Full-text patterns (trafilatura or BS4)
            page_text = self._extract_page_text(html)
            if page_text:
                listing = self._enrich_from_text(listing, page_text)

            # 6. Compute derived fields
            listing = self._compute_derived(listing)

            time.sleep(0.5)

        except Exception as e:
            logger.debug(f"enrich_from_url failed for {url[:60]}: {e}")

        return listing

    # ------------------------------------------------------------------
    # Site-specific extractors
    # ------------------------------------------------------------------

    def _enrich_realtor_ca(self, listing: dict, url: str, html: str) -> dict:
        """Use realtor.ca public PropertyDetails API when possible."""
        try:
            import requests as _req
            # Extract PropertyID from URL  (e.g. /real-estate/25823456/...)
            m = re.search(r"/real-estate/(\d{6,9})", url)
            if not m:
                return listing
            prop_id = m.group(1)
            api_url = (
                f"https://api2.realtor.ca/Listing.svc/PropertyDetails"
                f"?PropertyID={prop_id}&lang=en-CA&ApplicationId=1"
            )
            r = _req.get(api_url, headers={**_FETCH_HEADERS, "Referer": "https://www.realtor.ca/"}, timeout=10)
            if not r.ok:
                return listing
            data = r.json()

            prop = data.get("Property", {})
            bldg = data.get("Building", {})
            land = data.get("Land", {})

            # Price
            if not listing.get("asking_price"):
                price_str = prop.get("Price", "")
                price = self._parse_price_str(price_str)
                if price:
                    listing["asking_price"] = price

            # Units
            if not listing.get("num_units"):
                units = bldg.get("UnitTotal") or bldg.get("TotalFinishedArea")
                if isinstance(units, (int, float)) and 2 <= units <= 500:
                    listing["num_units"] = int(units)

            # Address
            if not listing.get("address"):
                addr = prop.get("Address", {})
                if isinstance(addr, dict):
                    street = addr.get("AddressText", "")
                    if street:
                        listing["address"] = street.split("|")[0].strip()

            # Building details
            if not listing.get("year_built"):
                yr = bldg.get("ConstructedDate") or bldg.get("YearBuilt")
                if yr:
                    try:
                        listing["year_built"] = int(str(yr)[:4])
                    except Exception:
                        pass

            if not listing.get("building_sf"):
                sf = bldg.get("SizeInterior")
                if sf:
                    val = self._parse_sqft(str(sf))
                    if val:
                        listing["building_sf"] = val

            # Asset type
            if not listing.get("asset_type"):
                bt = bldg.get("Type", "") or prop.get("Type", "")
                if bt:
                    listing["asset_type"] = str(bt)

            logger.debug(f"Enriched from realtor.ca API: PropID={prop_id}")

        except Exception as e:
            logger.debug(f"realtor.ca API enrichment failed: {e}")

        return listing

    def _enrich_centris(self, listing: dict, html: str) -> dict:
        """Extract centris.ca-specific structured data from the page."""
        try:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html, "lxml")

            # Centris embeds price in a specific element
            price_el = soup.select_one("[itemprop='price'], .property-price, .asking-price, #ListingPrice")
            if price_el and not listing.get("asking_price"):
                price = self._parse_price_str(price_el.get_text())
                if price:
                    listing["asking_price"] = price

            # Units often in a spec table
            for el in soup.select("[class*='carac'], [class*='spec'], [class*='detail']"):
                text = el.get_text(" ", strip=True)
                if not listing.get("num_units"):
                    units = self.extract_units(text)
                    if units:
                        listing["num_units"] = units

        except Exception as e:
            logger.debug(f"Centris enrichment failed: {e}")

        return listing

    # ------------------------------------------------------------------
    # Generic extractors
    # ------------------------------------------------------------------

    def _enrich_from_jsonld(self, listing: dict, html: str) -> dict:
        """Extract data from JSON-LD <script> blocks."""
        try:
            for m in re.finditer(r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', html, re.DOTALL | re.IGNORECASE):
                try:
                    data = json.loads(m.group(1))
                    items = data if isinstance(data, list) else [data]
                    for item in items:
                        schema_type = item.get("@type", "")
                        if not any(t in str(schema_type) for t in ["RealEstate", "Property", "Product", "Apartment", "House"]):
                            continue

                        # Price
                        if not listing.get("asking_price"):
                            for key in ("price", "lowPrice", "highPrice"):
                                val = item.get(key) or item.get("offers", {}).get(key)
                                if val:
                                    p = self._parse_price_str(str(val))
                                    if p:
                                        listing["asking_price"] = p
                                        break

                        # Address
                        if not listing.get("address"):
                            addr = item.get("address")
                            if isinstance(addr, dict):
                                parts = [addr.get("streetAddress"), addr.get("addressLocality"), addr.get("addressRegion")]
                                addr_str = ", ".join(p for p in parts if p)
                                if addr_str:
                                    listing["address"] = addr_str
                            elif isinstance(addr, str) and addr:
                                listing["address"] = addr

                        # Number of rooms → approximate units
                        if not listing.get("num_units"):
                            nu = item.get("numberOfRooms") or item.get("numberOfBedrooms")
                            if nu and isinstance(nu, (int, float)) and nu >= 5:
                                listing["num_units"] = int(nu)

                        # Floor size
                        if not listing.get("building_sf"):
                            fs = item.get("floorSize")
                            if isinstance(fs, dict):
                                val = self._parse_sqft(str(fs.get("value", "")))
                                if val:
                                    listing["building_sf"] = val

                        # Year built
                        if not listing.get("year_built"):
                            yr = item.get("yearBuilt")
                            if yr:
                                try:
                                    listing["year_built"] = int(str(yr)[:4])
                                except Exception:
                                    pass

                except (json.JSONDecodeError, Exception):
                    continue

        except Exception as e:
            logger.debug(f"JSON-LD enrichment failed: {e}")

        return listing

    def _enrich_from_meta(self, listing: dict, html: str) -> dict:
        """Extract from Open Graph and standard meta tags."""
        try:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html, "lxml")

            def meta(name_or_prop):
                el = soup.find("meta", attrs={"property": name_or_prop}) or \
                     soup.find("meta", attrs={"name": name_or_prop})
                return el.get("content", "").strip() if el else ""

            # Price from og tags
            if not listing.get("asking_price"):
                for tag in ("og:price:amount", "product:price:amount", "twitter:data1"):
                    val = meta(tag)
                    if val:
                        p = self._parse_price_str(val)
                        if p:
                            listing["asking_price"] = p
                            break

            # Description often has unit count and financials
            desc = meta("og:description") or meta("description")
            if desc:
                if not listing.get("num_units"):
                    units = self.extract_units(desc)
                    if units:
                        listing["num_units"] = units
                if not listing.get("listed_cap_rate"):
                    cap = self.extract_cap_rate(desc)
                    if cap:
                        listing["listed_cap_rate"] = cap
                if not listing.get("estimated_noi"):
                    noi = self.extract_noi(desc)
                    if noi:
                        listing["estimated_noi"] = noi

            # Title can contain price
            title = meta("og:title") or ""
            if title and not listing.get("asking_price"):
                p = self._parse_price_str(title)
                if p:
                    listing["asking_price"] = p

        except Exception as e:
            logger.debug(f"Meta tag enrichment failed: {e}")

        return listing

    def _enrich_from_script_json(self, listing: dict, html: str) -> dict:
        """Extract from embedded JS data blobs (__NEXT_DATA__, window.__data__, etc.)."""
        try:
            # Next.js __NEXT_DATA__
            m = re.search(r'<script[^>]+id=["\']__NEXT_DATA__["\'][^>]*>(.*?)</script>', html, re.DOTALL)
            if m:
                data = json.loads(m.group(1))
                # Flatten the nested structure and look for financial keys
                flat = json.dumps(data)
                listing = self._extract_from_json_blob(listing, flat)
                return listing

            # Generic window.__data__ or similar
            for pat in [r'window\.__(?:data|state|props|listing)__\s*=\s*({.*?});', r'var\s+listing\s*=\s*({.*?});']:
                m = re.search(pat, html, re.DOTALL)
                if m:
                    try:
                        blob = json.loads(m.group(1))
                        listing = self._extract_from_json_blob(listing, json.dumps(blob))
                    except Exception:
                        pass

        except Exception as e:
            logger.debug(f"Script JSON enrichment failed: {e}")

        return listing

    def _extract_from_json_blob(self, listing: dict, json_str: str) -> dict:
        """Scan a JSON string for property-related keys."""
        try:
            # Look for price patterns
            if not listing.get("asking_price"):
                for key in ('"price":', '"Price":', '"askingPrice":', '"listPrice":'):
                    idx = json_str.find(key)
                    if idx >= 0:
                        snippet = json_str[idx:idx+40]
                        m = re.search(r':\s*"?([\d,\.]+)"?', snippet)
                        if m:
                            p = self._parse_price_str(m.group(1))
                            if p:
                                listing["asking_price"] = p
                                break

            # Unit count
            if not listing.get("num_units"):
                for key in ('"unitTotal":', '"UnitTotal":', '"numberOfUnits":', '"unitCount":'):
                    idx = json_str.find(key)
                    if idx >= 0:
                        snippet = json_str[idx:idx+30]
                        m = re.search(r':\s*"?(\d+)"?', snippet)
                        if m:
                            u = int(m.group(1))
                            if 2 <= u <= 500:
                                listing["num_units"] = u
                                break

        except Exception:
            pass

        return listing

    def _enrich_from_text(self, listing: dict, text: str) -> dict:
        """Extract all financial fields from clean page text using regex."""
        full = text[:15000]

        if not listing.get("num_units"):
            u = self.extract_units(full)
            if u:
                listing["num_units"] = u

        if not listing.get("asking_price"):
            p = self.extract_price(full)
            if p:
                listing["asking_price"] = p

        if not listing.get("listed_cap_rate"):
            c = self.extract_cap_rate(full)
            if c:
                listing["listed_cap_rate"] = c

        if not listing.get("estimated_noi"):
            noi = self.extract_noi(full)
            if noi:
                listing["estimated_noi"] = noi

        if not listing.get("address"):
            addr = self.extract_address(full)
            if addr:
                listing["address"] = addr

        if not listing.get("year_built"):
            yr = self.extract_year_built(full)
            if yr:
                listing["year_built"] = yr

        if not listing.get("building_sf"):
            sf = self.extract_sqft(full)
            if sf:
                listing["building_sf"] = sf

        if not listing.get("occupancy"):
            occ = self.extract_occupancy(full)
            if occ is not None:
                listing["occupancy"] = occ

        if not listing.get("asset_type"):
            at = self.extract_asset_type(full)
            if at:
                listing["asset_type"] = at

        return listing

    def _compute_derived(self, listing: dict) -> dict:
        """Compute price_per_unit and other derived fields."""
        price = listing.get("asking_price")
        units = listing.get("num_units")
        if price and units and units > 0 and not listing.get("price_per_unit"):
            listing["price_per_unit"] = round(price / units, 0)
        return listing

    # ------------------------------------------------------------------
    # Field extractors
    # ------------------------------------------------------------------

    @staticmethod
    def extract_units(text: str) -> Optional[int]:
        """Extract unit count from text, including word-form plex names."""
        text_lower = text.lower()

        # Word-form plex names (duplex=2, triplex=3, etc.)
        for word, count in PLEX_WORDS.items():
            if word in text_lower:
                return count

        patterns = [
            r"(\d+)\s*-?\s*(?:unit|suite|logement|appartement|apt)s?\b",
            r"(\d+)\s*-?\s*(?:plex|plexe)\b",
            r"(\d+)\s*-?\s*(?:door|porte)s?\b",
            r"(\d+)\s*(?:logements?|appartements?|unités?|suites?)\b",
            r"(?:building|immeuble|complex)\s+(?:of|with|de|avec)\s+(\d+)\s*(?:unit|suite|apt|logement)",
            r"(\d+)\s*-?\s*unit\s+(?:apartment|building|complex|rental)",
            r"(\d+)\s*(?:residential|rental)\s+unit",
            # "Total units: 12"
            r"(?:total|nombre)\s+(?:d[e']?\s*)?(?:units?|logements?|suites?|appartements?)\s*:?\s*(\d+)",
        ]
        for pat in patterns:
            m = re.search(pat, text, re.IGNORECASE)
            if m:
                try:
                    val = int(m.group(1))
                    if 2 <= val <= 500:
                        return val
                except Exception:
                    pass
        return None

    @staticmethod
    def extract_price(text: str) -> Optional[float]:
        """Extract asking price from text."""
        patterns = [
            r"\$\s*([\d,]+(?:\.\d+)?)\s*(?:M|million)\b",
            r"\$\s*([\d,]+(?:\.\d{2})?)\b",
            r"([\d\s,]+)\s*\$",
            r"(?:asking|list(?:ing)?|price|prix)\s*(?:at|:)?\s*\$?\s*([\d,\s]+)",
        ]
        for pat in patterns:
            m = re.search(pat, text, re.IGNORECASE)
            if m:
                raw = m.group(1).replace(",", "").replace(" ", "")
                try:
                    val = float(raw)
                    seg = text[max(0, m.start()-2):m.end()+10].lower()
                    if "million" in seg or (seg.rstrip()[-1:] == "m" and val < 1000):
                        val *= 1_000_000
                    if 200_000 <= val <= 100_000_000:
                        return val
                except (ValueError, IndexError):
                    continue
        return None

    @staticmethod
    def extract_cap_rate(text: str) -> Optional[float]:
        """Extract capitalization rate from text."""
        patterns = [
            r"cap\s*(?:rate)?\s*(?:of|:)?\s*(\d+\.?\d*)\s*%",
            r"(\d+\.?\d*)\s*%\s*cap(?:\s*rate)?",
            r"taux\s*(?:de)?\s*capitalisation\s*(?:de|:)?\s*(\d+\.?\d*)\s*%",
            r"(\d+\.?\d*)\s*%\s*(?:capitalisation|capitalization)",
            r"(?:cap|capitaliz)\w*\s*:?\s*(\d+\.?\d*)\s*%",
        ]
        for pat in patterns:
            m = re.search(pat, text, re.IGNORECASE)
            if m:
                try:
                    val = float(m.group(1))
                    if 1.0 <= val <= 15.0:
                        return val
                except Exception:
                    pass
        return None

    @staticmethod
    def extract_noi(text: str) -> Optional[float]:
        """Extract Net Operating Income (NOI) or annual revenue from text."""
        patterns = [
            # NOI / Net Operating Income
            r"(?:NOI|net\s+operating\s+income|revenu\s+net)\s*(?:of|:)?\s*\$?\s*([\d,]+(?:\.\d+)?)\s*(?:K|M)?",
            # Annual gross income / revenue
            r"(?:annual|yearly|gross)\s+(?:income|revenue|rental\s+income|loyer)\s*(?:of|:)?\s*\$?\s*([\d,]+(?:\.\d+)?)\s*(?:K|M)?",
            r"(?:revenus?\s+(?:bruts?|annuels?))\s*(?:de|:)?\s*\$?\s*([\d,]+(?:\.\d+)?)\s*(?:K|M)?",
            # "Income: $120,000"
            r"(?:income|revenue|loyer\s+brut)\s*:?\s*\$?\s*([\d,]+(?:\.\d+)?)\s*(?:K|M|000)?",
            # Gross rent multiplier clue: "Annual rents $X"
            r"(?:annual|yearly)\s+(?:rent|rents|rental)\s*(?:of|:)?\s*\$?\s*([\d,]+(?:\.\d+)?)",
        ]
        for pat in patterns:
            m = re.search(pat, text, re.IGNORECASE)
            if m:
                try:
                    raw = m.group(1).replace(",", "")
                    val = float(raw)
                    seg = text[m.start():m.end()+5].lower()
                    if "k" in seg and val < 10_000:
                        val *= 1_000
                    elif "m" in seg and val < 100:
                        val *= 1_000_000
                    elif val < 1_000:     # assume in thousands if very small
                        val *= 1_000
                    if 10_000 <= val <= 20_000_000:
                        return val
                except Exception:
                    continue
        return None

    @staticmethod
    def extract_address(text: str) -> Optional[str]:
        """Extract a street address from text."""
        patterns = [
            r"\b(\d{1,5}\s+[A-Za-zÀ-ÿ][A-Za-zÀ-ÿ\s\-]{2,35}(?:Street|St|Avenue|Ave|Boulevard|Blvd|Drive|Dr|Road|Rd|Way|Lane|Ln|Court|Ct|Place|Pl|Crescent|Cres|NW|NE|SW|SE)\b)",
            r"\b(\d{1,5}\s+(?:rue|avenue|boulevard|chemin|route|place)\s+[A-Za-zÀ-ÿ\s\-]{2,35})",
        ]
        for pat in patterns:
            m = re.search(pat, text, re.IGNORECASE)
            if m:
                addr = m.group(1).strip()
                if len(addr) > 8:
                    return addr
        return None

    @staticmethod
    def extract_year_built(text: str) -> Optional[int]:
        """Extract year of construction from text."""
        patterns = [
            r"(?:built|constructed|year\s+built|année\s+de\s+construction|construit)\s*(?:in|en|:)?\s*(1[89]\d{2}|20[012]\d)",
            r"(?:vintage|year)\s*:?\s*(1[89]\d{2}|20[012]\d)",
            r"(1[89]\d{2}|20[012]\d)\s*(?:construction|built|construit|vintage)",
        ]
        for pat in patterns:
            m = re.search(pat, text, re.IGNORECASE)
            if m:
                try:
                    yr = int(m.group(1))
                    if 1900 <= yr <= 2025:
                        return yr
                except Exception:
                    pass
        return None

    @staticmethod
    def extract_sqft(text: str) -> Optional[float]:
        """Extract building square footage from text."""
        patterns = [
            r"([\d,]+)\s*(?:sq\.?\s*ft\.?|sqft|pi²|square\s*feet)",
            r"([\d,]+)\s*(?:m²|sq\.?\s*m\.?|square\s*met)",
        ]
        for pat in patterns:
            m = re.search(pat, text, re.IGNORECASE)
            if m:
                try:
                    val = float(m.group(1).replace(",", ""))
                    # Convert m² to sqft if needed
                    if "m²" in text[m.start():m.end()+3] or "sq. m" in text[m.start():m.end()+6].lower():
                        val *= 10.764
                    if 500 <= val <= 500_000:
                        return round(val, 0)
                except Exception:
                    pass
        return None

    @staticmethod
    def extract_occupancy(text: str) -> Optional[float]:
        """Extract occupancy rate from text."""
        patterns = [
            r"(?:occupan(?:cy|t)|occupied|taux\s+d['']occupation)\s*(?:rate|:)?\s*(\d+\.?\d*)\s*%",
            r"(\d+\.?\d*)\s*%\s*(?:occupied|occupan(?:cy|t))",
            r"(?:vacancy|vacant)\s*(?:rate)?\s*(\d+\.?\d*)\s*%",  # flip: occupancy = 100 - vacancy
        ]
        for i, pat in enumerate(patterns):
            m = re.search(pat, text, re.IGNORECASE)
            if m:
                try:
                    val = float(m.group(1))
                    if 0 <= val <= 100:
                        # Vacancy pattern → invert to occupancy
                        return round(100.0 - val, 1) if i == 2 else val
                except Exception:
                    pass
        return None

    @staticmethod
    def extract_asset_type(text: str) -> Optional[str]:
        """Classify asset type from text."""
        text_lower = text.lower()
        types = {
            "Apartment Building": ["apartment building", "immeuble à appartements", "résidentiel à logements"],
            "Mixed-Use": ["mixed-use", "mixed use", "usage mixte", "commercial residential"],
            "Multi-Family": ["multi-family", "multifamily", "multi-unit", "multilogement", "multi-logement"],
            "Low-Rise": ["low-rise", "low rise", "faible densité"],
            "High-Rise": ["high-rise", "high rise", "tour"],
        }
        for label, keywords in types.items():
            if any(kw in text_lower for kw in keywords):
                return label
        return None

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_page_text(html: str) -> Optional[str]:
        """Get clean plain text from HTML using trafilatura, falling back to BS4."""
        try:
            import trafilatura
            text = trafilatura.extract(html, include_tables=True, no_fallback=False)
            if text:
                return text
        except Exception:
            pass
        try:
            from bs4 import BeautifulSoup
            return BeautifulSoup(html, "lxml").get_text(" ", strip=True)[:12000]
        except Exception:
            return None

    @staticmethod
    def _parse_price_str(s: str) -> Optional[float]:
        """Parse a price string like '$1,250,000' or '1.5M' or '1250000'."""
        if not s:
            return None
        s = str(s).strip()
        # Remove currency symbols
        s = re.sub(r"[,$\s]", "", s)
        try:
            if s.lower().endswith("m"):
                val = float(s[:-1]) * 1_000_000
            elif s.lower().endswith("k"):
                val = float(s[:-1]) * 1_000
            else:
                val = float(s)
            if 200_000 <= val <= 100_000_000:
                return val
        except ValueError:
            pass
        return None

    @staticmethod
    def _parse_sqft(s: str) -> Optional[float]:
        """Parse a square footage string."""
        if not s:
            return None
        m = re.search(r"([\d,\.]+)", str(s))
        if m:
            try:
                val = float(m.group(1).replace(",", ""))
                if 500 <= val <= 500_000:
                    return val
            except ValueError:
                pass
        return None

    @staticmethod
    def classify_region(text: str) -> Optional[str]:
        """Classify which BBG target region a listing belongs to."""
        text_lower = text.lower()
        edmonton_markers = [
            "edmonton", "sherwood park", "st. albert", "st albert",
            "spruce grove", "leduc", "fort saskatchewan", "beaumont",
            "stony plain", "devon",
        ]
        montreal_markers = [
            "montreal", "montréal", "laval", "longueuil", "brossard",
            "terrebonne", "repentigny", "west island",
            "verdun", "lasalle", "lachine", "dorval", "pointe-claire",
            "saint-laurent", "ahuntsic", "rosemont",
        ]
        for marker in edmonton_markers:
            if marker in text_lower:
                return "greater_edmonton"
        for marker in montreal_markers:
            if marker in text_lower:
                return "greater_montreal"
        return None
