"""Tier 1 Scorecard scoring engine for BBG Deal Scout."""

import json
import logging
from typing import Dict, Optional

from .config import get_scoring

logger = logging.getLogger(__name__)


# CMHC-informed market benchmarks (2023–2024 Rental Market Reports).
# These are defaults — override via config.yaml scoring.market_benchmarks.
# Update annually from https://www.cmhc-schl.gc.ca/
_DEFAULT_BENCHMARKS = {
    "greater_edmonton": {
        "avg_price_per_unit": 145_000,   # ~$145K/unit mid-market resale, 6–20 units
        "avg_cap_rate": 5.2,             # Edmonton apartment cap rate, CMHC 2023
    },
    "greater_montreal": {
        "avg_price_per_unit": 185_000,   # ~$185K/unit Longueuil/Laval 5–12 plex
        "avg_cap_rate": 4.8,             # Montreal apartment cap rate, CMHC 2023
    },
}


def _load_benchmarks() -> dict:
    """Load market benchmarks: config.yaml overrides, otherwise CMHC defaults.

    config.yaml is gitignored (contains secrets) so defaults must be in code.
    To override: add scoring.market_benchmarks.<region> to config.yaml.
    """
    try:
        cfg = get_scoring()
        user_benchmarks = cfg.get("market_benchmarks", {})
        # Deep merge: user values override defaults region-by-region
        merged = dict(_DEFAULT_BENCHMARKS)
        for region, vals in user_benchmarks.items():
            merged[region] = {**merged.get(region, {}), **vals}
        return merged
    except Exception:
        return _DEFAULT_BENCHMARKS


def score_listing(listing, thresholds: Dict = None) -> Dict:
    """
    Score a listing against BBG's Tier 1 Scorecard.

    Returns dict with:
      - tier1_score: int (0-7)
      - tier1_details: JSON string of individual check results
      - score_status: "scored" | "insufficient_data"
    """
    if thresholds is None:
        try:
            thresholds = get_scoring()
        except Exception:
            thresholds = {
                "min_cap_rate": 5.0,
                "min_dscr": 1.20,
                "min_occupancy": 85.0,
                "target_geographies": [
                    "Edmonton", "St. Albert", "Sherwood Park", "Spruce Grove",
                    "Leduc", "Fort Saskatchewan", "Beaumont",
                    "Montreal", "Laval", "Longueuil", "Brossard", "Terrebonne",
                ],
            }
    region = listing.region if hasattr(listing, "region") else listing.get("region", "")
    benchmarks = _load_benchmarks().get(region, {})

    checks = {}
    scored_count = 0
    passed_count = 0

    # 1. Cap rate >= threshold
    cap_rate = _get_val(listing, "listed_cap_rate")
    min_cap = thresholds.get("min_cap_rate", 5.0)
    if cap_rate is not None:
        passed = cap_rate >= min_cap
        checks["cap_rate"] = {
            "label": f"Cap rate ≥ {min_cap}%",
            "value": f"{cap_rate:.1f}%",
            "passed": passed,
        }
        scored_count += 1
        if passed:
            passed_count += 1
    else:
        checks["cap_rate"] = {"label": f"Cap rate ≥ {min_cap}%", "value": "N/A", "passed": None}

    # 2. DSCR >= threshold (estimated)
    dscr = _estimate_dscr(listing)
    min_dscr = thresholds.get("min_dscr", 1.20)
    if dscr is not None:
        passed = dscr >= min_dscr
        checks["dscr"] = {
            "label": f"DSCR ≥ {min_dscr}x (est.)",
            "value": f"{dscr:.2f}x",
            "passed": passed,
        }
        scored_count += 1
        if passed:
            passed_count += 1
    else:
        checks["dscr"] = {"label": f"DSCR ≥ {min_dscr}x (est.)", "value": "N/A", "passed": None}

    # 3. Price per unit at/below market
    ppu = _get_val(listing, "price_per_unit")
    if ppu is None:
        # Try to calculate from price and units
        price = _get_val(listing, "asking_price")
        units = _get_val(listing, "num_units")
        if price and units and units > 0:
            ppu = price / units

    mkt_ppu = benchmarks.get("avg_price_per_unit")
    if ppu is not None and mkt_ppu:
        passed = ppu <= mkt_ppu * 1.05  # 5% tolerance
        checks["price_per_unit"] = {
            "label": f"Price/unit ≤ market (${mkt_ppu:,.0f})",
            "value": f"${ppu:,.0f}",
            "passed": passed,
        }
        scored_count += 1
        if passed:
            passed_count += 1
    else:
        checks["price_per_unit"] = {
            "label": "Price/unit at/below market",
            "value": f"${ppu:,.0f}" if ppu else "N/A",
            "passed": None,
        }

    # 4. Occupancy >= threshold
    occ = _get_val(listing, "occupancy")
    min_occ = thresholds.get("min_occupancy", 85.0)
    if occ is not None:
        passed = occ >= min_occ
        checks["occupancy"] = {
            "label": f"Occupancy ≥ {min_occ}%",
            "value": f"{occ:.0f}%",
            "passed": passed,
        }
        scored_count += 1
        if passed:
            passed_count += 1
    else:
        checks["occupancy"] = {"label": f"Occupancy ≥ {min_occ}%", "value": "N/A", "passed": None}

    # 5. No environmental red flags (default pass — no data source for this yet)
    checks["environmental"] = {
        "label": "No environmental red flags",
        "value": "Not checked",
        "passed": None,
    }

    # 6. In target geography
    target_geos = thresholds.get("target_geographies", [])
    city = _get_val(listing, "city") or ""
    address = _get_val(listing, "address") or ""
    combined_location = f"{city} {address}".lower()

    geo_match = any(g.lower() in combined_location for g in target_geos)
    if combined_location.strip():
        checks["geography"] = {
            "label": "In target geography",
            "value": city or region,
            "passed": geo_match,
        }
        scored_count += 1
        if geo_match:
            passed_count += 1
    else:
        checks["geography"] = {
            "label": "In target geography",
            "value": region,
            "passed": None,
        }

    # 7. Value-add potential (heuristic based on age and occupancy)
    value_add = _assess_value_add(listing, benchmarks)
    checks["value_add"] = {
        "label": "Value-add potential identified",
        "value": value_add.get("reason", "N/A"),
        "passed": value_add.get("identified"),
    }
    if value_add.get("identified") is not None:
        scored_count += 1
        if value_add["identified"]:
            passed_count += 1

    # Determine status
    score_status = "scored" if scored_count >= 2 else "insufficient_data"

    return {
        "tier1_score": passed_count,
        "tier1_details": json.dumps(checks),
        "score_status": score_status,
    }


def _get_val(obj, field):
    """Get value from either a model instance or dict."""
    if hasattr(obj, field):
        return getattr(obj, field)
    elif isinstance(obj, dict):
        return obj.get(field)
    return None


def _estimate_dscr(listing) -> Optional[float]:
    """
    Rough DSCR estimate from available data.
    DSCR = NOI / Annual Debt Service
    """
    noi = _get_val(listing, "estimated_noi")
    price = _get_val(listing, "asking_price")
    cap_rate = _get_val(listing, "listed_cap_rate")

    # If we have NOI directly, use it
    if noi is None and price and cap_rate:
        noi = price * (cap_rate / 100)

    if noi is None or price is None:
        return None

    # Assume 75% LTV, 5.5% mortgage rate, 25-year amortization (Canadian semi-annual compounding)
    ltv = 0.75
    loan = price * ltv
    # Effective monthly rate for semi-annual compounding
    annual_rate = 0.055
    semi_annual_rate = annual_rate / 2
    effective_monthly = (1 + semi_annual_rate) ** (1 / 6) - 1
    n_payments = 25 * 12

    if effective_monthly <= 0:
        return None

    monthly_payment = loan * (
        effective_monthly * (1 + effective_monthly) ** n_payments
    ) / ((1 + effective_monthly) ** n_payments - 1)

    annual_debt_service = monthly_payment * 12

    if annual_debt_service <= 0:
        return None

    return noi / annual_debt_service


def _assess_value_add(listing, benchmarks: dict) -> Dict:
    """Heuristic value-add assessment."""
    reasons = []

    year_built = _get_val(listing, "year_built")
    if year_built and year_built < 2000:
        reasons.append(f"Built {year_built} — renovation upside")

    occ = _get_val(listing, "occupancy")
    if occ is not None and occ < 90:
        reasons.append(f"Occupancy {occ:.0f}% — lease-up opportunity")

    cap_rate = _get_val(listing, "listed_cap_rate")
    mkt_cap = benchmarks.get("avg_cap_rate")
    if cap_rate and mkt_cap and cap_rate > mkt_cap + 1.0:
        reasons.append("Above-market cap rate — mgmt improvement potential")

    if reasons:
        return {"identified": True, "reason": "; ".join(reasons)}
    elif year_built or occ is not None:
        return {"identified": False, "reason": "No obvious value-add signals"}
    else:
        return {"identified": None, "reason": "Insufficient data"}
