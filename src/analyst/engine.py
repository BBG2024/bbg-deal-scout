"""BBG Deal Analyst — Main Engine Orchestrator.

This is the central entry point. It takes a DealInput, runs all calculation
modules in sequence, and returns a complete AnalysisOutput.

Pipeline:
    1. Validate and normalize inputs
    2. Compute loan amount from LTV
    3. Build monthly cash flow projections
    4. Aggregate into annual summaries
    5. Compute return metrics (IRR, EMx, CoC, etc.)
    6. Run waterfall distribution
    7. (Optional) Run sensitivity analysis
"""

import logging
from datetime import datetime
from typing import Optional

from .models import DealInput, AnalysisOutput
from .cashflow import project_monthly_cashflows, summarize_annual
from .returns import compute_returns
from .waterfall import compute_waterfall
from .sensitivity import run_sensitivity, run_scenarios

logger = logging.getLogger(__name__)


def run_analysis(
    deal: DealInput,
    include_sensitivity: bool = False,
) -> AnalysisOutput:
    """
    Run the complete deal analysis pipeline.

    This is the single function you call to get everything.
    Returns an AnalysisOutput with all computed data.
    """
    output = AnalysisOutput()
    output.deal_input = deal
    output.computed_at = datetime.utcnow()

    # --- Step 1: Validate & normalize ---
    _validate_inputs(deal)
    deal.compute_loan_amount()

    # --- Step 2: Sources & Uses ---
    output.debt_amount = deal.financing.loan_amount
    output.equity_amount = deal.equity_required
    output.total_uses = deal.total_acquisition_cost
    output.total_sources = output.debt_amount + output.equity_amount

    # --- Step 3: Monthly cash flows ---
    output.monthly_cashflows = project_monthly_cashflows(deal)

    # --- Step 4: Annual summaries ---
    output.annual_summaries = summarize_annual(output.monthly_cashflows, deal)

    # --- Step 5: Return metrics ---
    output.returns = compute_returns(deal, output.monthly_cashflows, output.annual_summaries)

    # --- Step 6: Waterfall ---
    output.waterfall = compute_waterfall(deal, output.annual_summaries)

    # --- Step 7: Sale info ---
    if output.monthly_cashflows:
        last_month = output.monthly_cashflows[-1]
        output.sale_price = last_month.sale_price
        output.net_sale_proceeds = last_month.net_sale_proceeds

    # --- Step 8: CMHC Financing Comparison ---
    try:
        from .cmhc import compute_cmhc_options, format_cmhc_comparison
        cmhc_opts = compute_cmhc_options(
            purchase_price=deal.purchase_price,
            conventional_rate=deal.financing.interest_rate,
            renovation_cost=deal.renovation_cost,
            acquisition_fee=deal.acquisition_fee,
        )
        output.cmhc_options = format_cmhc_comparison(cmhc_opts)
    except Exception as e:
        logger.warning(f"CMHC comparison failed: {e}")

    # --- Step 9: CCA Depreciation Schedule ---
    try:
        from .cca import CCAInput, compute_cca_schedule, compute_after_tax_returns
        cca_input = CCAInput(
            purchase_price=deal.purchase_price,
            land_value=deal.land_value,
            land_percent=20.0,  # Default 20% land allocation
            renovation_cost=deal.renovation_cost,
            hold_years=len(output.annual_summaries),
            marginal_tax_rate=50.0,  # Combined federal + provincial
            sale_price=output.sale_price,
        )
        cca_sched = compute_cca_schedule(cca_input)

        # Store CCA schedule as serializable dict
        output.cca_schedule = {
            "total_depreciable_cost": cca_sched.total_depreciable_cost,
            "land_value": cca_sched.land_value,
            "cca_class": cca_sched.cca_class,
            "cca_rate_pct": cca_sched.cca_rate * 100,
            "total_cca_claimed": cca_sched.total_cca_claimed,
            "total_tax_savings": cca_sched.total_tax_savings,
            "final_ucc": cca_sched.final_ucc,
            "recapture_amount": cca_sched.recapture_amount,
            "recapture_tax": cca_sched.recapture_tax,
            "years": [
                {
                    "year": y.year,
                    "opening_ucc": y.opening_ucc,
                    "additions": y.additions,
                    "cca_claimed": y.cca_claimed,
                    "closing_ucc": y.closing_ucc,
                    "cumulative_cca": y.cumulative_cca,
                    "tax_savings": y.tax_savings,
                }
                for y in cca_sched.years
            ],
        }

        # Compute after-tax returns using CCA
        pre_tax_cfs = [s.cash_flow_after_debt for s in output.annual_summaries]
        # Taxable rental income = NOI - interest portion of debt service
        taxable_income = []
        for s in output.annual_summaries:
            # Approximate interest portion (conservative: use full debt service as proxy)
            # In reality, only the interest portion is deductible, not principal
            interest_approx = s.debt_service * 0.7  # ~70% of early payments are interest
            taxable = s.noi - interest_approx
            taxable_income.append(taxable)

        at_returns = compute_after_tax_returns(
            pre_tax_cfs, cca_sched, taxable_income, marginal_tax_rate=50.0
        )
        output.after_tax_returns = at_returns

    except Exception as e:
        logger.warning(f"CCA calculation failed: {e}")

    # --- Step 10: Exit Strategy Analysis (Sell/Refinance at Year 5 and 10) ---
    try:
        from .exit_strategies import (
            compute_exit_strategies, format_exit_comparison, GrowthAssumptions
        )
        growth = GrowthAssumptions(
            value_appreciation_pct=getattr(deal, '_appreciation_pct', 5.0),
            expense_increase_pct=deal.expense_growth_percent,
            rent_increase_pct=deal.rent_growth_percent,
            refi_ltv_pct=getattr(deal, '_refi_ltv', 75.0),
            refi_interest_rate=getattr(deal, '_refi_rate', 4.50),
            refi_amort_years=getattr(deal, '_refi_amort_years', 25),
            refi_costs_pct=getattr(deal, '_refi_costs_pct', 1.5),
        )
        exit_result = compute_exit_strategies(deal, output.annual_summaries, growth)
        output.exit_analysis = {
            "scenarios": format_exit_comparison(exit_result),
            "best_irr": exit_result.best_irr_scenario,
            "best_emx": exit_result.best_emx_scenario,
            "best_cash_out": exit_result.best_cash_out_scenario,
            "growth": {
                "appreciation": growth.value_appreciation_pct,
                "rent_increase": growth.rent_increase_pct,
                "expense_increase": growth.expense_increase_pct,
                "refi_ltv": growth.refi_ltv_pct,
                "refi_rate": growth.refi_interest_rate,
                "refi_amort_years": growth.refi_amort_years,
            },
        }
    except Exception as e:
        logger.warning(f"Exit strategy analysis failed: {e}")

    # --- Step 11: Sensitivity (optional, expensive) ---
    if include_sensitivity:
        # Use a wrapper that calls run_analysis without sensitivity
        # to avoid infinite recursion
        def _engine_no_sensitivity(d: DealInput) -> AnalysisOutput:
            return run_analysis(d, include_sensitivity=False)

        sens = run_sensitivity(deal, _engine_no_sensitivity)
        scenarios = run_scenarios(deal, _engine_no_sensitivity)
        output.sensitivity = scenarios + sens

    logger.info(
        f"Analysis complete: {deal.project_name or 'Unnamed'} | "
        f"IRR: {output.returns.levered_irr:.1f}% | "
        f"EMx: {output.returns.levered_emx:.2f}x | "
        f"GP IRR: {output.waterfall.gp_irr:.1f}% | "
        f"LP IRR: {output.waterfall.lp_irr:.1f}%"
    )

    return output


def run_quick_analysis(deal: DealInput) -> AnalysisOutput:
    """Run analysis without sensitivity — faster for dashboard previews."""
    return run_analysis(deal, include_sensitivity=False)


def run_full_analysis(deal: DealInput) -> AnalysisOutput:
    """Run analysis with sensitivity — for PDF reports."""
    return run_analysis(deal, include_sensitivity=True)


def _validate_inputs(deal: DealInput):
    """Basic validation of deal inputs."""
    if deal.purchase_price <= 0:
        raise ValueError("Purchase price must be positive")

    if not deal.units:
        raise ValueError("At least one unit is required")

    if deal.total_units <= 0:
        raise ValueError("Total unit count must be positive")

    if deal.exit_month <= 0:
        raise ValueError("Hold period must be positive")

    if deal.financing.ltv_percent < 0 or deal.financing.ltv_percent > 100:
        raise ValueError("LTV must be between 0% and 100%")

    if deal.terminal_cap_rate <= 0:
        raise ValueError("Terminal cap rate must be positive")

    # Normalize percentage fields
    if deal.vacancy_percent < 0:
        deal.vacancy_percent = 0
    if deal.rent_growth_percent < -10:
        deal.rent_growth_percent = -10


# --- Helper to create a DealInput from a Deal Scout listing ---

def deal_from_listing(listing) -> DealInput:
    """
    Create a pre-filled DealInput from a Deal Scout listing.

    Fills in whatever data the listing has; leaves the rest at defaults
    for the user to complete in the dashboard.
    """
    deal = DealInput()

    deal.project_name = listing.title if hasattr(listing, "title") else ""
    deal.address = listing.address if hasattr(listing, "address") else ""
    deal.listing_id = listing.id if hasattr(listing, "id") else None

    # Price
    price = listing.asking_price if hasattr(listing, "asking_price") else None
    if price:
        deal.purchase_price = price

    # Units
    num_units = listing.num_units if hasattr(listing, "num_units") else None
    if num_units and num_units > 0:
        # Create placeholder units with zero rent — user fills in actual amounts
        for i in range(num_units):
            deal.units.append(
                UnitInput(
                    name=f"Unit {i + 1}",
                    count=1,
                    current_rent=0,
                    post_reno_rent=0,
                )
            )

    # Cap rate — if available from listing, note it for reference but don't
    # back-calculate rents (user enters actual rent roll data)

    return deal


# Need to import UnitInput at the top level for deal_from_listing
from .models import UnitInput
