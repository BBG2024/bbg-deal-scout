"""Realtor.ca API Collector — uses the public REST API behind realtor.ca map search.

This bypasses the JS-rendered frontend entirely by calling the same JSON API
that the realtor.ca browser app calls. No scraping, no headless browser needed.
"""

import logging
import re
import requests
from typing import List, Dict, Optional
from datetime import datetime

from .base import BaseCollector

logger = logging.getLogger(__name__)

# Realtor.ca public API (same endpoint used by their web app)
SEARCH_URL = "https://api2.realtor.ca/Listing.svc/PropertySearch_Post"
DETAIL_URL = "https://api2.realtor.ca/Listing.svc/PropertyDetails"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "en-CA,en;q=0.9",
    "Referer": "https://www.realtor.ca/",
    "Origin": "https://www.realtor.ca",
}

# Realtor.ca building type IDs
BUILDING_TYPE_APARTMENT = 17
BUILDING_TYPE_LOWRISE = 7
BUILDING_TYPE_HIGHRISE = 8
BUILDING_TYPE_MULTIUNIT = 22

# Property type group — 5 = Multi-family
PROPERTY_TYPE_MULTIFAMILY = 5

# Transaction type — 2 = Sale
TRANSACTION_SALE = 2


# Region bounding boxes
REGION_BOUNDS = {
    "greater_edmonton": {
        "LatitudeMax": 53.74,
        "LatitudeMin": 53.35,
        "LongitudeMax": -113.10,
        "LongitudeMin": -113.88,
        "GeoName": "Edmonton, AB",
        "ZoomLevel": 10,
    },
    "greater_montreal": {
        "LatitudeMax": 45.76,
        "LatitudeMin": 45.37,
        "LongitudeMax": -73.35,
        "LongitudeMin": -73.98,
        "GeoName": "Montreal, QC",
        "ZoomLevel": 10,
    },
}


class RealtorCaCollector(BaseCollector):
    """Pulls multifamily listings directly from realtor.ca REST API."""

    name = "realtor_ca"

    def __init__(self, config: dict, filters: dict):
        super().__init__(config, filters)
        self.records_per_page = 50  # Max per request
        self.max_pages = 3          # Up to 150 listings per region per scan
        self._session = None        # Shared requests session with cookies

    def _get_session(self) -> requests.Session:
        """Return a requests session pre-warmed with realtor.ca cookies.

        realtor.ca's API requires a valid browser session (cookies set by
        visiting the homepage) — raw POST without cookies gets 403.
        """
        if self._session is not None:
            return self._session

        session = requests.Session()
        session.headers.update(_HEADERS)
        try:
            # Visit the homepage to obtain session cookies
            r = session.get("https://www.realtor.ca/", timeout=15)
            r.raise_for_status()
            logger.debug(f"Realtor.ca session established — cookies: {list(session.cookies.keys())}")
        except Exception as e:
            logger.warning(f"Could not establish realtor.ca session: {e}")

        self._session = session
        return session

    def collect(self, region_key: str, region_config: dict) -> List[Dict]:
        """Fetch multifamily listings for this region via the realtor.ca API."""
        bounds = REGION_BOUNDS.get(region_key)
        if not bounds:
            logger.debug(f"No realtor.ca bounds configured for {region_key} — skipping")
            return []

        listings = []
        min_units = self.filters.get("min_units", 5)
        min_price = self.filters.get("min_price", 500000)
        max_price = self.filters.get("max_price", 15000000)

        for page in range(1, self.max_pages + 1):
            try:
                results = self._search_page(bounds, region_key, page)
                if not results:
                    break  # No more pages

                for raw in results:
                    listing = self._parse_result(raw, region_key)
                    if listing:
                        if self.passes_filters(listing):
                            listings.append(listing)

                logger.info(
                    f"Realtor.ca {region_key} page {page}: "
                    f"{len(results)} raw → {len(listings)} qualifying so far"
                )

                if len(results) < self.records_per_page:
                    break  # Last page

            except requests.HTTPError as e:
                if e.response is not None and e.response.status_code == 403:
                    # 403 = bot detection / session issue. Log at info, not error.
                    # Don't keep retrying — realtor.ca needs JS cookies we can't get server-side.
                    logger.info(
                        f"Realtor.ca API blocked (403) for {region_key} — "
                        f"their bot detection requires browser cookies. Skipping."
                    )
                else:
                    logger.error(f"Realtor.ca HTTP error for {region_key} page {page}: {e}")
                    self.errors.append(f"RealtorCA/{region_key}/p{page}: {str(e)}")
                break
            except Exception as e:
                logger.error(f"Realtor.ca search failed for {region_key} page {page}: {e}")
                self.errors.append(f"RealtorCA/{region_key}/p{page}: {str(e)}")
                break

        logger.info(f"Realtor.ca {region_key}: {len(listings)} qualifying listings")
        return listings

    def _search_page(self, bounds: dict, region_key: str, page: int) -> List[Dict]:
        """Call the realtor.ca PropertySearch_Post endpoint."""
        payload = {
            "ZoomLevel": str(bounds["ZoomLevel"]),
            "LatitudeMax": str(bounds["LatitudeMax"]),
            "LongitudeMax": str(bounds["LongitudeMax"]),
            "LatitudeMin": str(bounds["LatitudeMin"]),
            "LongitudeMin": str(bounds["LongitudeMin"]),
            "Sort": "6-D",                  # Newest first
            "PropertyTypeGroupID": str(PROPERTY_TYPE_MULTIFAMILY),
            "TransactionTypeId": str(TRANSACTION_SALE),
            "BuildingTypeId": str(BUILDING_TYPE_APARTMENT),
            "Currency": "CAD",
            "RecordsPerPage": str(self.records_per_page),
            "CurrentPage": str(page),
            "ApplicationId": "1",
            "CultureId": "1",               # English
            "Version": "7.0",
            "lang": "en-CA",
        }

        session = self._get_session()
        resp = session.post(
            SEARCH_URL, data=payload, timeout=20
        )
        resp.raise_for_status()
        data = resp.json()

        results = data.get("Results", [])
        logger.debug(
            f"  API returned {len(results)} results "
            f"({data.get('Paging', {}).get('TotalRecords', '?')} total)"
        )
        return results

    def _parse_result(self, raw: dict, region_key: str) -> Optional[Dict]:
        """Parse a single realtor.ca API result into a standardized listing dict."""
        property_id = raw.get("Id", "")
        mls_number = raw.get("MlsNumber", "")

        # Build the canonical realtor.ca URL
        slug = raw.get("RelativeDetailsURL", "")
        if slug:
            source_url = f"https://www.realtor.ca{slug}"
        elif property_id:
            source_url = f"https://www.realtor.ca/real-estate/{property_id}/listing"
        else:
            return None

        # Address
        address_dict = raw.get("Property", {}).get("Address", {})
        address = address_dict.get("AddressText", "")
        city = None
        if address:
            parts = address.split("|")
            if len(parts) >= 2:
                city = parts[1].strip().split(",")[0].strip()

        # Price
        price_str = raw.get("Property", {}).get("Price", "")
        asking_price = self._parse_price(price_str)

        # Units from Building
        building = raw.get("Building", {})
        bedroom_str = building.get("BathroomTotal") or building.get("Bedrooms") or ""
        unit_count_str = building.get("UnitTotal") or ""
        num_units = None
        if unit_count_str:
            try:
                num_units = int(str(unit_count_str).replace(",", "").strip())
            except (ValueError, TypeError):
                pass

        if num_units is None and bedroom_str:
            # Some listings put unit count in Bedrooms field for multifamily
            try:
                num_units = int(str(bedroom_str).strip())
            except (ValueError, TypeError):
                pass

        # Title
        prop_type = raw.get("Building", {}).get("Type", "")
        title = f"{prop_type} — {address}" if prop_type and address else address or mls_number or "Listing"
        title = title[:300]

        # Sq ft
        size_str = raw.get("Land", {}).get("SizeTotal") or building.get("SizeInterior") or ""
        sqft = self._parse_sqft_str(size_str)

        # Year built
        year_built = None
        constructed = building.get("ConstructedDate") or ""
        if constructed:
            match = re.search(r"(19|20)\d{2}", str(constructed))
            if match:
                year_built = int(match.group())

        listing = {
            "source": self.name,
            "source_url": source_url,
            "source_label": f"Realtor.ca ({region_key})",
            "mls_number": mls_number,
            "title": title,
            "address": address.split("|")[0].strip() if "|" in address else address,
            "city": city,
            "region": region_key,
            "num_units": num_units,
            "asking_price": asking_price,
            "sqft": sqft,
            "year_built": year_built,
            "listed_cap_rate": None,
            "noi": None,
            "occupancy": None,
            "asset_type": prop_type or None,
            "discovered_at": datetime.utcnow(),
        }

        # Detect region from address text
        detected = self.classify_region(address)
        if detected:
            listing["region"] = detected

        # Enrich with full details from PropertyDetails API (if we have a property ID)
        if property_id:
            try:
                listing = self._fetch_property_details(listing, property_id)
            except Exception as e:
                logger.debug(f"PropertyDetails fetch failed for {property_id}: {e}")

        # Final price-per-unit calculation
        listing = self._compute_derived(listing)
        return listing

    def _fetch_property_details(self, listing: dict, property_id: str) -> dict:
        """Fetch full property details from realtor.ca API."""
        params = {
            "PropertyID": property_id,
            "lang": "en-CA",
            "ApplicationId": "1",
            "Version": "7.0",
        }
        resp = requests.get(DETAIL_URL, params=params, headers=_HEADERS, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        detail = data.get("PropertyDetails", {})
        if not detail:
            return listing

        # Units
        if listing.get("num_units") is None:
            unit_str = detail.get("Building", {}).get("UnitTotal") or ""
            if unit_str:
                try:
                    listing["num_units"] = int(str(unit_str).strip())
                except (ValueError, TypeError):
                    pass

        # Year built
        if listing.get("year_built") is None:
            constructed = detail.get("Building", {}).get("ConstructedDate") or ""
            match = re.search(r"(19|20)\d{2}", str(constructed))
            if match:
                listing["year_built"] = int(match.group())

        # Sq ft from interior size
        if listing.get("sqft") is None:
            size_str = detail.get("Building", {}).get("SizeInterior") or ""
            listing["sqft"] = self._parse_sqft_str(size_str)

        # Asset type
        if not listing.get("asset_type"):
            listing["asset_type"] = detail.get("Building", {}).get("Type") or None

        # Description — run our text extractors over it
        description = detail.get("PublicRemarks") or ""
        if description:
            if listing.get("noi") is None:
                listing["noi"] = self.extract_noi(description)
            if listing.get("listed_cap_rate") is None:
                listing["listed_cap_rate"] = self.extract_cap_rate(description)
            if listing.get("occupancy") is None:
                listing["occupancy"] = self.extract_occupancy(description)
            if listing.get("num_units") is None:
                listing["num_units"] = self.extract_units(description)

        return listing

    @staticmethod
    def _parse_price(price_str: str) -> Optional[float]:
        """Parse realtor.ca price string like '$1,250,000' → 1250000.0"""
        if not price_str:
            return None
        cleaned = re.sub(r"[^\d.]", "", str(price_str))
        try:
            val = float(cleaned)
            return val if val > 0 else None
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _parse_sqft_str(size_str: str) -> Optional[float]:
        """Parse size string like '8,500 sqft' or '790 m²'."""
        if not size_str:
            return None
        text = str(size_str).lower()
        match = re.search(r"([\d,]+(?:\.\d+)?)", text)
        if not match:
            return None
        try:
            val = float(match.group(1).replace(",", ""))
            if "m²" in text or "m2" in text or "sqm" in text or "mètre" in text:
                val = val * 10.7639  # Convert m² → sqft
            return val if val > 0 else None
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _compute_derived(listing: dict) -> dict:
        """Compute price_per_unit if we have both price and units."""
        price = listing.get("asking_price")
        units = listing.get("num_units")
        if price and units and units > 0:
            listing["price_per_unit"] = round(price / units, 0)
        return listing
