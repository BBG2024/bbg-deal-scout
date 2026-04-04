"""CCA (Capital Cost Allowance) — Canadian Depreciation Rules for Rental Property.

Capital Cost Allowance is the Canadian tax mechanism for depreciating the cost
of capital assets over their useful life. For real estate investors, CCA is a
powerful non-cash deduction that reduces taxable income from rental properties.

KEY CRA RULES FOR RENTAL REAL ESTATE:

1. CCA CLASSES FOR BUILDINGS:
   - Class 1 (4% declining balance): Most buildings acquired after 1987
   - Class 3 (5% declining balance): Buildings acquired before 1988
   - Class 6 (10%): Frame, log, or stucco buildings (if specific conditions met)
   - Class 1 is by far the most common for multifamily rental

2. THE HALF-YEAR RULE (Section 1100(2)):
   In the year a property is acquired, only HALF of the normal CCA
   rate can be claimed. This prevents full-year deductions for assets
   purchased late in the year.

   Example: Class 1 building ($1M depreciable cost)
   Year 1: 4% × $1,000,000 × 50% = $20,000 (half-year rule)
   Year 2: 4% × ($1,000,000 - $20,000) = $39,200

3. LAND IS NOT DEPRECIABLE:
   Only the building portion of the property can be depreciated.
   The land value must be separated from the purchase price.
   Typical split: 15-30% land, 70-85% building (varies by location).

4. RENOVATION / IMPROVEMENT COSTS:
   Capital improvements (not repairs) are added to the UCC pool
   and depreciated at the same CCA rate. The half-year rule applies
   to additions in the year they are made.

5. RECAPTURE ON DISPOSITION:
   When a building is sold for more than its UCC (undepreciated capital
   cost), the difference is "recaptured" and included in income.
   This is taxed as regular income, not capital gains.

   If sold for less than UCC: a "terminal loss" deduction may be available.

6. RENTAL PROPERTY RESTRICTION:
   CCA on rental properties can only be used to reduce rental income
   to zero — it CANNOT create or increase a non-capital loss.
   This means CCA cannot shelter other income (employment, business).

7. ACCELERATED INVESTMENT INCENTIVE (AIIP) — 2024+:
   For properties acquired after November 20, 2018, the AIIP provides
   an enhanced first-year allowance. Instead of the half-year rule,
   the full CCA rate applies to 1.5x the net addition in Year 1.
   However, this incentive is being phased out:
   - 2024-2025: Full AIIP (1.5x factor)
   - 2026: Reduced to 1.25x factor
   - 2027+: Returns to standard half-year rule

   For simplicity and conservatism, this module uses the standard
   half-year rule. The AIIP can be toggled on for properties
   acquired during the incentive period.
"""

from dataclasses import dataclass, field
from typing import List, Dict, Optional


# ────────────────────────────────────────────────────────────
# CCA CLASSES
# ────────────────────────────────────────────────────────────

CCA_CLASSES = {
    1: {
        "rate": 0.04,    # 4% declining balance
        "description": "Buildings acquired after 1987 (most common for rental)",
        "method": "declining_balance",
    },
    3: {
        "rate": 0.05,    # 5% declining balance
        "description": "Buildings acquired before 1988",
        "method": "declining_balance",
    },
    6: {
        "rate": 0.10,    # 10% declining balance
        "description": "Frame, log, stucco buildings (special conditions)",
        "method": "declining_balance",
    },
    8: {
        "rate": 0.20,    # 20% declining balance
        "description": "Furniture, fixtures, equipment (in furnished rentals)",
        "method": "declining_balance",
    },
}


@dataclass
class CCAInput:
    """Input parameters for CCA calculation."""
    purchase_price: float = 0.0
    land_value: float = 0.0                # Must be separated — not depreciable
    land_percent: float = 20.0             # If land_value not set, use % of purchase price
    renovation_cost: float = 0.0           # Capital improvements (depreciable)
    renovation_year: int = 1               # Year renovation costs are added

    cca_class: int = 1                     # CCA class (1, 3, 6, or 8)
    use_aiip: bool = False                 # Use Accelerated Investment Incentive
    aiip_factor: float = 1.5              # AIIP multiplier (1.5 for 2024-25, 1.25 for 2026)

    hold_years: int = 10                   # Projection period
    marginal_tax_rate: float = 50.0        # Combined federal + provincial rate (%)

    # Sale assumptions (for recapture calculation)
    sale_price: float = 0.0               # Expected sale price
    sale_land_value: float = 0.0          # Land value at sale (if different from purchase)


@dataclass
class CCAYearRow:
    """One year of the CCA schedule."""
    year: int = 0
    opening_ucc: float = 0.0              # Undepreciated Capital Cost at start of year
    additions: float = 0.0                 # Capital additions during the year
    dispositions: float = 0.0              # Proceeds from dispositions
    net_additions: float = 0.0            # Additions - Dispositions
    cca_claimed: float = 0.0              # CCA deduction for the year
    closing_ucc: float = 0.0              # UCC at end of year
    cumulative_cca: float = 0.0           # Total CCA claimed to date
    tax_savings: float = 0.0              # CCA × marginal tax rate


@dataclass
class CCASchedule:
    """Complete CCA schedule with summary metrics."""
    # Schedule
    years: List[CCAYearRow] = field(default_factory=list)

    # Summary
    total_depreciable_cost: float = 0.0    # Building + renovations (excluding land)
    land_value: float = 0.0
    cca_class: int = 1
    cca_rate: float = 0.04
    total_cca_claimed: float = 0.0
    total_tax_savings: float = 0.0
    final_ucc: float = 0.0

    # Recapture on sale (if applicable)
    recapture_amount: float = 0.0          # Taxable recapture
    recapture_tax: float = 0.0             # Tax on recapture
    terminal_loss: float = 0.0             # Deductible terminal loss
    net_tax_impact_on_sale: float = 0.0    # Net tax effect at disposition


def compute_cca_schedule(inputs: CCAInput) -> CCASchedule:
    """
    Compute the full CCA schedule for a rental property.

    This builds a year-by-year declining balance depreciation schedule
    following CRA rules, including the half-year rule (or AIIP), and
    calculates recapture/terminal loss on disposition.
    """
    schedule = CCASchedule()

    # Determine CCA rate
    cca_info = CCA_CLASSES.get(inputs.cca_class, CCA_CLASSES[1])
    rate = cca_info["rate"]
    schedule.cca_class = inputs.cca_class
    schedule.cca_rate = rate

    # Determine depreciable cost (purchase price minus land)
    if inputs.land_value > 0:
        land = inputs.land_value
    else:
        land = inputs.purchase_price * (inputs.land_percent / 100)

    building_cost = inputs.purchase_price - land
    if building_cost < 0:
        building_cost = 0

    schedule.land_value = land
    schedule.total_depreciable_cost = building_cost + inputs.renovation_cost

    tax_rate = inputs.marginal_tax_rate / 100
    ucc = 0.0
    cumulative_cca = 0.0

    for year in range(1, inputs.hold_years + 1):
        row = CCAYearRow(year=year)
        row.opening_ucc = ucc

        # Additions
        additions = 0.0
        if year == 1:
            additions += building_cost
        if year == inputs.renovation_year:
            additions += inputs.renovation_cost

        row.additions = additions

        # Net additions for the year
        net_additions = additions
        row.net_additions = net_additions

        # CCA calculation with half-year rule
        # Standard rule: CCA = rate × (opening UCC + net additions × 0.5)
        # AIIP rule: CCA = rate × (opening UCC + net additions × 1.5) for first year only

        if net_additions > 0:
            if inputs.use_aiip and year <= 2:
                # AIIP: enhanced first-year deduction on new additions
                cca = rate * (ucc + net_additions * inputs.aiip_factor)
            else:
                # Standard half-year rule on net additions
                cca = rate * (ucc + net_additions * 0.5)
        else:
            # No new additions — full rate on existing UCC
            cca = rate * ucc

        # CCA cannot exceed UCC + additions (can't go below zero)
        max_cca = ucc + additions
        cca = min(cca, max_cca)
        cca = max(cca, 0)

        row.cca_claimed = round(cca, 2)
        row.tax_savings = round(cca * tax_rate, 2)

        # Update UCC
        ucc = ucc + additions - cca
        row.closing_ucc = round(ucc, 2)

        cumulative_cca += cca
        row.cumulative_cca = round(cumulative_cca, 2)

        schedule.years.append(row)

    schedule.total_cca_claimed = round(cumulative_cca, 2)
    schedule.total_tax_savings = round(cumulative_cca * tax_rate, 2)
    schedule.final_ucc = round(ucc, 2)

    # Recapture / Terminal Loss on Disposition
    if inputs.sale_price > 0:
        # Determine the building portion of the sale price
        sale_land = inputs.sale_land_value if inputs.sale_land_value > 0 else land
        sale_building = inputs.sale_price - sale_land

        # Lesser of: original cost or sale proceeds allocated to building
        proceeds = min(sale_building, schedule.total_depreciable_cost)

        if proceeds > ucc:
            # RECAPTURE: proceeds exceed UCC → taxable income
            schedule.recapture_amount = round(proceeds - ucc, 2)
            schedule.recapture_tax = round(schedule.recapture_amount * tax_rate, 2)
            schedule.net_tax_impact_on_sale = -schedule.recapture_tax
        elif proceeds < ucc and ucc > 0:
            # TERMINAL LOSS: UCC exceeds proceeds → deductible loss
            schedule.terminal_loss = round(ucc - proceeds, 2)
            schedule.net_tax_impact_on_sale = round(schedule.terminal_loss * tax_rate, 2)
        # If proceeds == UCC: no recapture, no loss

    return schedule


def compute_after_tax_returns(
    pre_tax_cashflows: List[float],
    cca_schedule: CCASchedule,
    rental_income_by_year: List[float],
    marginal_tax_rate: float = 50.0,
) -> Dict:
    """
    Compute after-tax cash flows incorporating CCA deductions.

    CCA reduces taxable rental income, creating tax savings.
    However, CCA can only reduce rental income to zero (rental
    property restriction) — it cannot create losses.

    Args:
        pre_tax_cashflows: Annual pre-tax cash flows (levered)
        cca_schedule: Computed CCA schedule
        rental_income_by_year: Annual taxable rental income (NOI - interest)
        marginal_tax_rate: Combined federal + provincial rate (%)

    Returns:
        Dict with after-tax cash flows, tax savings, and effective returns
    """
    tax_rate = marginal_tax_rate / 100
    years = len(pre_tax_cashflows)

    after_tax_cfs = []
    annual_tax_savings = []

    for i in range(years):
        year_idx = i  # 0-indexed
        pre_tax_cf = pre_tax_cashflows[i]

        # Get CCA for this year
        cca = 0.0
        if year_idx < len(cca_schedule.years):
            cca = cca_schedule.years[year_idx].cca_claimed

        # Taxable rental income (before CCA)
        taxable_income = rental_income_by_year[i] if i < len(rental_income_by_year) else 0

        # Apply CCA deduction (limited to reducing income to zero)
        cca_applied = min(cca, max(taxable_income, 0))
        taxable_after_cca = max(taxable_income - cca_applied, 0)

        # Tax payable
        tax_without_cca = max(taxable_income, 0) * tax_rate
        tax_with_cca = taxable_after_cca * tax_rate
        tax_saving = tax_without_cca - tax_with_cca

        # After-tax cash flow = pre-tax + tax savings from CCA
        after_tax_cf = pre_tax_cf + tax_saving

        after_tax_cfs.append(round(after_tax_cf, 2))
        annual_tax_savings.append(round(tax_saving, 2))

    return {
        "after_tax_cashflows": after_tax_cfs,
        "annual_tax_savings": annual_tax_savings,
        "total_tax_savings": sum(annual_tax_savings),
        "recapture_on_sale": cca_schedule.recapture_amount,
        "recapture_tax": cca_schedule.recapture_tax,
        "net_tax_benefit": sum(annual_tax_savings) - cca_schedule.recapture_tax,
    }
