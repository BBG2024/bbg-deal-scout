"""CMHC Financing Programs for Canadian Multifamily.

This module implements two CMHC mortgage insurance programs used for
multifamily (5+ unit) rental properties in Canada:

1. STANDARD CMHC INSURED (up to 40-year amortization)
   - Available for existing and new construction rental properties
   - Down payment options: 25% (75% LTV) and 35% (65% LTV)
   - Insurance premium is a % of the loan amount, paid upfront or added to loan
   - Rates are set by CMHC and vary by LTV band

2. MLI SELECT (up to 50-year amortization)
   - Available for properties meeting energy efficiency, affordability,
     or accessibility criteria (scored on a points system)
   - Down payment options: 10% (90% LTV), 15% (85% LTV), 20% (80% LTV)
   - Lower premiums for higher MLI Select scores
   - 50-year amortization significantly reduces monthly debt service,
     improving DSCR and cash flow

KEY CONCEPT — How CMHC insurance works:
    Unlike residential mortgages where the borrower pays CMHC insurance,
    for multifamily (5+ units) the INSURANCE PREMIUM is typically:
    - Calculated as a percentage of the loan amount
    - Either paid upfront in cash OR rolled into the loan amount
    - The premium percentage depends on the LTV ratio and program
    - Interest rates on CMHC-insured loans are typically lower than
      conventional because the lender's risk is reduced

CMHC PREMIUM RATES (Multifamily Rental, as of 2025):
    These are approximate and should be verified against current CMHC
    published rates at the time of analysis. The module uses a lookup
    table that can be updated.

    Standard Program:
        65% LTV (35% down): 1.50% of loan
        75% LTV (25% down): 2.40% of loan

    MLI Select (varies by score, these are typical):
        80% LTV (20% down): 2.75-4.00% of loan
        85% LTV (15% down): 3.25-4.50% of loan
        90% LTV (10% down): 4.00-5.00% of loan

INTEREST RATE ADVANTAGE:
    CMHC-insured loans typically get 50-150 bps lower interest rates
    than conventional mortgages because the government guarantee
    removes default risk for the lender. This is a critical factor
    in the analysis — the lower rate often more than offsets the
    insurance premium cost.
"""

from dataclasses import dataclass, field
from typing import List, Dict, Optional, Tuple
from enum import Enum


class CMHCProgram(Enum):
    """Available CMHC financing programs."""
    CONVENTIONAL = "conventional"          # No CMHC insurance
    STANDARD_25_DOWN = "standard_25"       # 40yr amort, 25% down (75% LTV)
    STANDARD_35_DOWN = "standard_35"       # 40yr amort, 35% down (65% LTV)
    MLI_SELECT_10_DOWN = "mli_10"          # 50yr amort, 10% down (90% LTV)
    MLI_SELECT_15_DOWN = "mli_15"          # 50yr amort, 15% down (85% LTV)
    MLI_SELECT_20_DOWN = "mli_20"          # 50yr amort, 20% down (80% LTV)


@dataclass
class CMHCPremiumSchedule:
    """
    CMHC insurance premium rates by program and LTV.

    These rates are published by CMHC and updated periodically.
    The rates below reflect the 2025 published schedule for
    multifamily rental properties (5+ units).

    UPDATE THESE VALUES when CMHC publishes new rates.
    Source: https://www.cmhc-schl.gc.ca/professionals/project-funding-and-mortgage-financing/mortgage-loan-insurance
    """
    # Standard CMHC Program — Multifamily Rental
    # Format: {ltv_percent: premium_percent_of_loan}
    standard_premiums: Dict[float, float] = field(default_factory=lambda: {
        65.0: 1.50,   # 35% down payment
        75.0: 2.40,   # 25% down payment
    })

    # MLI Select Program — Multifamily Rental
    # Premiums vary by LTV and MLI Select score (points).
    # These are BASELINE rates; properties with higher MLI scores
    # may receive premium reductions of 25-50%.
    # Format: {ltv_percent: premium_percent_of_loan}
    mli_select_premiums: Dict[float, float] = field(default_factory=lambda: {
        80.0: 3.60,   # 20% down payment
        85.0: 4.25,   # 15% down payment
        90.0: 4.75,   # 10% down payment
    })

    # Interest rate advantage (basis points below conventional)
    # CMHC-insured loans typically get lower rates from lenders
    standard_rate_advantage_bps: float = 75    # 0.75% lower than conventional
    mli_select_rate_advantage_bps: float = 100  # 1.00% lower than conventional

    # Maximum amortization periods
    standard_max_amort_years: int = 40
    mli_select_max_amort_years: int = 50

    # Last updated date (for display purposes)
    rates_as_of: str = "2025-Q1"


# Default schedule instance
DEFAULT_CMHC_RATES = CMHCPremiumSchedule()


@dataclass
class CMHCOption:
    """A single CMHC financing option with all computed values."""
    program: CMHCProgram = CMHCProgram.CONVENTIONAL
    program_label: str = ""
    program_description: str = ""

    # Inputs
    purchase_price: float = 0.0
    down_payment_percent: float = 0.0
    ltv_percent: float = 0.0
    amortization_years: int = 0
    amortization_months: int = 0

    # Insurance
    insurance_premium_percent: float = 0.0
    insurance_premium_amount: float = 0.0

    # Loan
    base_loan_amount: float = 0.0          # Before insurance premium
    total_loan_amount: float = 0.0         # After adding insurance premium
    interest_rate_conventional: float = 0.0 # What a conventional loan would cost
    interest_rate_cmhc: float = 0.0        # CMHC-insured rate (lower)
    rate_advantage_bps: float = 0.0

    # Computed payments
    monthly_payment: float = 0.0
    annual_debt_service: float = 0.0

    # Equity
    down_payment_amount: float = 0.0
    total_equity_required: float = 0.0     # Down payment + any cash costs

    # Key ratios
    effective_ltv: float = 0.0             # After insurance rolled in


def compute_cmhc_options(
    purchase_price: float,
    conventional_rate: float = 4.25,
    renovation_cost: float = 0.0,
    acquisition_fee: float = 0.0,
    rates: CMHCPremiumSchedule = None,
) -> List[CMHCOption]:
    """
    Compute all available CMHC financing options for a given purchase price.

    This is the main function the analyst engine calls. It returns a list
    of CMHCOption objects — one for each program variant — so the user
    can compare them side by side in the dashboard.

    Args:
        purchase_price: Property purchase price in CAD
        conventional_rate: Current conventional mortgage rate (%)
        renovation_cost: Total renovation budget (for total cost calculation)
        acquisition_fee: Acquisition fees (for total cost calculation)
        rates: CMHC premium schedule (uses defaults if None)

    Returns:
        List of CMHCOption objects, one per program variant
    """
    if rates is None:
        rates = DEFAULT_CMHC_RATES

    from .amortization import monthly_payment, effective_monthly_rate

    options = []

    # Define all program variants with their parameters
    program_configs = [
        {
            "program": CMHCProgram.CONVENTIONAL,
            "label": "Conventional (No CMHC)",
            "desc": "Standard mortgage, no insurance. Higher rate, flexible terms.",
            "down_pct": 25.0,  # Typical conventional minimum
            "ltv_pct": 75.0,
            "amort_years": 25,  # Typical conventional max
            "premium_pct": 0.0,
            "rate_advantage": 0,
        },
        {
            "program": CMHCProgram.STANDARD_35_DOWN,
            "label": "CMHC Standard — 35% Down",
            "desc": f"40-year amortization. Insurance premium: {rates.standard_premiums.get(65.0, 1.50):.2f}% of loan. Lower rate than conventional.",
            "down_pct": 35.0,
            "ltv_pct": 65.0,
            "amort_years": rates.standard_max_amort_years,
            "premium_pct": rates.standard_premiums.get(65.0, 1.50),
            "rate_advantage": rates.standard_rate_advantage_bps,
        },
        {
            "program": CMHCProgram.STANDARD_25_DOWN,
            "label": "CMHC Standard — 25% Down",
            "desc": f"40-year amortization. Insurance premium: {rates.standard_premiums.get(75.0, 2.40):.2f}% of loan. Lower rate than conventional.",
            "down_pct": 25.0,
            "ltv_pct": 75.0,
            "amort_years": rates.standard_max_amort_years,
            "premium_pct": rates.standard_premiums.get(75.0, 2.40),
            "rate_advantage": rates.standard_rate_advantage_bps,
        },
        {
            "program": CMHCProgram.MLI_SELECT_20_DOWN,
            "label": "MLI Select — 20% Down",
            "desc": f"50-year amortization. Insurance premium: {rates.mli_select_premiums.get(80.0, 3.60):.2f}% of loan. Requires energy/affordability criteria.",
            "down_pct": 20.0,
            "ltv_pct": 80.0,
            "amort_years": rates.mli_select_max_amort_years,
            "premium_pct": rates.mli_select_premiums.get(80.0, 3.60),
            "rate_advantage": rates.mli_select_rate_advantage_bps,
        },
        {
            "program": CMHCProgram.MLI_SELECT_15_DOWN,
            "label": "MLI Select — 15% Down",
            "desc": f"50-year amortization. Insurance premium: {rates.mli_select_premiums.get(85.0, 4.25):.2f}% of loan. Requires energy/affordability criteria.",
            "down_pct": 15.0,
            "ltv_pct": 85.0,
            "amort_years": rates.mli_select_max_amort_years,
            "premium_pct": rates.mli_select_premiums.get(85.0, 4.25),
            "rate_advantage": rates.mli_select_rate_advantage_bps,
        },
        {
            "program": CMHCProgram.MLI_SELECT_10_DOWN,
            "label": "MLI Select — 10% Down",
            "desc": f"50-year amortization. Insurance premium: {rates.mli_select_premiums.get(90.0, 4.75):.2f}% of loan. Requires energy/affordability criteria.",
            "down_pct": 10.0,
            "ltv_pct": 90.0,
            "amort_years": rates.mli_select_max_amort_years,
            "premium_pct": rates.mli_select_premiums.get(90.0, 4.75),
            "rate_advantage": rates.mli_select_rate_advantage_bps,
        },
    ]

    for cfg in program_configs:
        opt = CMHCOption()
        opt.program = cfg["program"]
        opt.program_label = cfg["label"]
        opt.program_description = cfg["desc"]
        opt.purchase_price = purchase_price
        opt.down_payment_percent = cfg["down_pct"]
        opt.ltv_percent = cfg["ltv_pct"]
        opt.amortization_years = cfg["amort_years"]
        opt.amortization_months = cfg["amort_years"] * 12

        # Loan calculation
        opt.base_loan_amount = purchase_price * (cfg["ltv_pct"] / 100)

        # Insurance premium (calculated on base loan, then added to loan)
        opt.insurance_premium_percent = cfg["premium_pct"]
        opt.insurance_premium_amount = opt.base_loan_amount * (cfg["premium_pct"] / 100)

        # Total loan = base loan + insurance premium (rolled into mortgage)
        opt.total_loan_amount = opt.base_loan_amount + opt.insurance_premium_amount

        # Interest rates
        opt.interest_rate_conventional = conventional_rate
        opt.rate_advantage_bps = cfg["rate_advantage"]
        opt.interest_rate_cmhc = conventional_rate - (cfg["rate_advantage"] / 100)

        # Use CMHC rate for insured, conventional rate for conventional
        effective_rate = opt.interest_rate_cmhc if cfg["premium_pct"] > 0 else conventional_rate

        # Monthly payment (Canadian semi-annual compounding)
        opt.monthly_payment = monthly_payment(
            loan_amount=opt.total_loan_amount,
            annual_rate_pct=effective_rate,
            amortization_months=opt.amortization_months,
            interest_only=False,
            canadian=True,
        )
        opt.annual_debt_service = opt.monthly_payment * 12

        # Equity required
        opt.down_payment_amount = purchase_price * (cfg["down_pct"] / 100)
        # Total equity = down payment + renovation + fees - (insurance is in the loan)
        opt.total_equity_required = opt.down_payment_amount + renovation_cost + acquisition_fee

        # Effective LTV (after insurance rolled into loan)
        if purchase_price > 0:
            opt.effective_ltv = (opt.total_loan_amount / purchase_price) * 100

        options.append(opt)

    return options


def format_cmhc_comparison(options: List[CMHCOption]) -> List[Dict]:
    """
    Format CMHC options into a comparison-friendly list of dicts
    for display in the dashboard.
    """
    rows = []
    for opt in options:
        rows.append({
            "program": opt.program.value,
            "label": opt.program_label,
            "description": opt.program_description,
            "down_payment_pct": f"{opt.down_payment_percent:.0f}%",
            "down_payment_amt": f"${opt.down_payment_amount:,.0f}",
            "ltv": f"{opt.ltv_percent:.0f}%",
            "amort_years": opt.amortization_years,
            "insurance_pct": f"{opt.insurance_premium_percent:.2f}%",
            "insurance_amt": f"${opt.insurance_premium_amount:,.0f}",
            "total_loan": f"${opt.total_loan_amount:,.0f}",
            "interest_rate": f"{opt.interest_rate_cmhc:.2f}%",
            "monthly_pmt": f"${opt.monthly_payment:,.0f}",
            "annual_ds": f"${opt.annual_debt_service:,.0f}",
            "equity_required": f"${opt.total_equity_required:,.0f}",
            "effective_ltv": f"{opt.effective_ltv:.1f}%",
            # Raw values for sorting/calculation
            "_monthly_pmt": opt.monthly_payment,
            "_annual_ds": opt.annual_debt_service,
            "_equity": opt.total_equity_required,
            "_total_loan": opt.total_loan_amount,
            "_rate": opt.interest_rate_cmhc,
            "_amort_months": opt.amortization_months,
            "_insurance": opt.insurance_premium_amount,
        })
    return rows
