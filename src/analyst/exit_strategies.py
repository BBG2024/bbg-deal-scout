"""Exit Strategy Engine — Sell and Refinance Scenarios.

Every deal needs a clear exit plan. This module computes four exit scenarios
that answer the two fundamental questions: "When do we sell?" and
"Should we refinance and hold instead?"

EXIT SCENARIOS:
    1. Sell at Year 5
    2. Sell at Year 10
    3. Refinance at Year 5 (pull equity, continue holding)
    4. Refinance at Year 10 (pull equity, continue holding)

GROWTH ASSUMPTIONS (configurable, applied to all calculations):
    - Property Value Appreciation: default 5% annual
    - Expense Increase: default 2% annual
    - Rent Increase: default 3% annual

These growth rates determine:
    - Sale price at exit (appreciation-based or NOI/cap-based, whichever is higher)
    - Refinance appraised value (appreciation-based)
    - Cash flow projections year by year
    - Cumulative cash flow at each exit point
    - IRR and equity multiple for each scenario

REFINANCE MECHANICS:
    When you refinance, you're replacing the existing mortgage with a new one
    based on the property's current appraised value. If the property has
    appreciated, the new loan will be larger than the remaining balance on the
    old loan. The difference (minus refinance costs) is "cash out" — equity
    you pull out of the deal without selling.

    After refinancing, the deal continues with:
    - New (larger) mortgage payment
    - Same operating income/expenses (continuing to grow)
    - Potentially negative cash flow if you over-leverage

    The key question is: does the cash-out plus ongoing cash flow justify
    holding longer, or would selling outright produce better total returns?
"""

import math
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Tuple

from .models import DealInput, AnnualSummary
from .amortization import (
    build_amortization_schedule, monthly_payment, get_loan_balance_at_month
)
from .returns import compute_irr


# ────────────────────────────────────────────────────────────
# GROWTH ASSUMPTIONS
# ────────────────────────────────────────────────────────────

@dataclass
class GrowthAssumptions:
    """Configurable growth rates that flow through all calculations.

    These defaults represent a moderate Canadian multifamily market scenario.
    The user can override any of these in the dashboard form.
    """
    value_appreciation_pct: float = 5.0    # Annual property value appreciation (%)
    expense_increase_pct: float = 2.0      # Annual operating expense increase (%)
    rent_increase_pct: float = 3.0         # Annual rental income increase (%)

    # Refinance-specific assumptions
    refi_ltv_pct: float = 75.0             # LTV on refinance (%)
    refi_interest_rate: float = 4.50       # Interest rate on new loan (%)
    refi_amort_years: int = 25             # Amortization on new loan (years)
    refi_costs_pct: float = 1.5            # Refinance closing costs (% of new loan)

    # Sale-specific assumptions (override deal-level if set)
    exit_cap_rate_year5: float = 5.25      # Cap rate for Year 5 sale
    exit_cap_rate_year10: float = 5.50     # Cap rate for Year 10 sale
    selling_costs_pct: float = 5.0         # Broker + closing costs on sale (%)

    # Post-refinance hold period (how long after refi before eventual sale)
    post_refi_hold_years: int = 5          # Hold 5 more years after refi


# ────────────────────────────────────────────────────────────
# EXIT SCENARIO OUTPUT
# ────────────────────────────────────────────────────────────

@dataclass
class ExitScenario:
    """Result of a single exit scenario (sell or refinance at a specific year)."""
    # Identification
    scenario_type: str = ""                # "sell" or "refinance"
    exit_year: int = 0                     # Year of exit/refinance (5 or 10)
    label: str = ""                        # Display label

    # Property value at exit
    appreciated_value: float = 0.0         # Purchase price × (1 + appreciation)^years
    noi_based_value: float = 0.0           # Forward NOI / exit cap rate
    exit_value_used: float = 0.0           # Higher of the two (conservative: use NOI-based)

    # Loan position at exit
    loan_balance_at_exit: float = 0.0      # Remaining mortgage balance

    # --- SELL scenario outputs ---
    gross_sale_price: float = 0.0
    selling_costs: float = 0.0
    net_sale_proceeds: float = 0.0         # After selling costs + loan payoff
    cumulative_cash_flow: float = 0.0      # Total operating CF received during hold
    total_profit: float = 0.0              # Net proceeds + cumulative CF - initial equity

    # --- REFINANCE scenario outputs ---
    new_appraised_value: float = 0.0
    new_loan_amount: float = 0.0           # Appraised value × refi LTV
    refinance_costs: float = 0.0
    cash_out: float = 0.0                  # New loan - old balance - refi costs
    new_monthly_payment: float = 0.0
    new_annual_debt_service: float = 0.0
    post_refi_annual_cf: float = 0.0       # Estimated first-year CF after refi

    # Post-refinance eventual sale (to compute full-cycle returns)
    eventual_sale_year: int = 0            # Total years from acquisition to eventual sale
    eventual_sale_price: float = 0.0
    eventual_net_proceeds: float = 0.0

    # --- Return metrics ---
    initial_equity: float = 0.0
    levered_irr: float = 0.0
    equity_multiple: float = 0.0
    avg_cash_on_cash: float = 0.0
    total_distributions: float = 0.0       # Everything the investor gets back

    # Full cash flow series (for IRR calculation)
    annual_cashflows: List[float] = field(default_factory=list)

    # Growth rates used
    appreciation_rate: float = 0.0
    rent_growth_rate: float = 0.0
    expense_growth_rate: float = 0.0


@dataclass
class ExitAnalysis:
    """Complete exit analysis with all four scenarios."""
    growth_assumptions: GrowthAssumptions = field(default_factory=GrowthAssumptions)
    scenarios: List[ExitScenario] = field(default_factory=list)

    # Quick comparison (which exit is best?)
    best_irr_scenario: str = ""
    best_emx_scenario: str = ""
    best_cash_out_scenario: str = ""


# ────────────────────────────────────────────────────────────
# COMPUTATION ENGINE
# ────────────────────────────────────────────────────────────

def compute_exit_strategies(
    deal: DealInput,
    annual_summaries: List[AnnualSummary],
    growth: GrowthAssumptions = None,
) -> ExitAnalysis:
    """
    Compute all four exit scenarios for a deal.

    This function takes the deal inputs and the already-computed annual summaries,
    then projects forward using the growth assumptions to determine what happens
    at each exit point.

    The annual_summaries provide the actual cash flow data. The growth assumptions
    are used to project property value appreciation and (for refinance scenarios)
    the post-refinance cash flow.
    """
    if growth is None:
        growth = GrowthAssumptions()

    result = ExitAnalysis(growth_assumptions=growth)
    deal.compute_loan_amount()
    equity = deal.equity_required

    # Build the amortization schedule to get loan balances at any point
    amort = build_amortization_schedule(
        loan_amount=deal.financing.loan_amount,
        annual_rate_pct=deal.financing.interest_rate,
        amortization_months=deal.financing.amortization_months,
        hold_months=max(120, deal.exit_month),  # At least 10 years
        canadian=True,
    )

    # ── SELL SCENARIOS ──

    for exit_year in [5, 10]:
        scenario = _compute_sell_scenario(
            deal=deal,
            annual_summaries=annual_summaries,
            amort=amort,
            exit_year=exit_year,
            growth=growth,
            equity=equity,
        )
        result.scenarios.append(scenario)

    # ── REFINANCE SCENARIOS ──

    for refi_year in [5, 10]:
        scenario = _compute_refinance_scenario(
            deal=deal,
            annual_summaries=annual_summaries,
            amort=amort,
            refi_year=refi_year,
            growth=growth,
            equity=equity,
        )
        result.scenarios.append(scenario)

    # ── DETERMINE BEST SCENARIOS ──

    if result.scenarios:
        by_irr = max(result.scenarios, key=lambda s: s.levered_irr)
        result.best_irr_scenario = by_irr.label

        by_emx = max(result.scenarios, key=lambda s: s.equity_multiple)
        result.best_emx_scenario = by_emx.label

        refis = [s for s in result.scenarios if s.scenario_type == "refinance"]
        if refis:
            by_cash = max(refis, key=lambda s: s.cash_out)
            result.best_cash_out_scenario = by_cash.label

    return result


def _compute_sell_scenario(
    deal: DealInput,
    annual_summaries: List[AnnualSummary],
    amort: List[dict],
    exit_year: int,
    growth: GrowthAssumptions,
    equity: float,
) -> ExitScenario:
    """Compute a sell scenario at a given year."""
    s = ExitScenario()
    s.scenario_type = "sell"
    s.exit_year = exit_year
    s.label = f"Sell at Year {exit_year}"
    s.initial_equity = equity
    s.appreciation_rate = growth.value_appreciation_pct
    s.rent_growth_rate = growth.rent_increase_pct
    s.expense_growth_rate = growth.expense_increase_pct

    # Property value at exit (appreciation-based)
    s.appreciated_value = deal.purchase_price * (1 + growth.value_appreciation_pct / 100) ** exit_year

    # NOI-based value (forward NOI / exit cap rate)
    # Use the exit year's NOI from annual summaries if available, else project
    exit_cap = growth.exit_cap_rate_year5 if exit_year <= 5 else growth.exit_cap_rate_year10
    if exit_year <= len(annual_summaries):
        exit_noi = annual_summaries[exit_year - 1].noi
    else:
        # Project NOI forward using rent and expense growth
        base_noi = annual_summaries[-1].noi if annual_summaries else 0
        years_forward = exit_year - len(annual_summaries)
        exit_noi = base_noi * (1 + growth.rent_increase_pct / 100) ** years_forward

    # Forward NOI (next year's projected) for cap rate valuation
    forward_noi = exit_noi * (1 + growth.rent_increase_pct / 100)
    if exit_cap > 0:
        s.noi_based_value = forward_noi / (exit_cap / 100)

    # Use the NOI-based value (standard practice for income properties)
    s.exit_value_used = s.noi_based_value
    s.gross_sale_price = s.exit_value_used

    # Loan balance at exit
    exit_month = exit_year * 12
    s.loan_balance_at_exit = get_loan_balance_at_month(amort, exit_month)

    # Sale proceeds
    s.selling_costs = s.gross_sale_price * (growth.selling_costs_pct / 100)
    s.net_sale_proceeds = s.gross_sale_price - s.selling_costs - s.loan_balance_at_exit

    # Cumulative operating cash flow during hold period
    hold_years = min(exit_year, len(annual_summaries))
    s.cumulative_cash_flow = sum(
        annual_summaries[y].cash_flow_after_debt for y in range(hold_years)
    )

    # Total profit
    s.total_profit = s.net_sale_proceeds + s.cumulative_cash_flow - equity
    s.total_distributions = s.net_sale_proceeds + s.cumulative_cash_flow

    # Build cash flow series for IRR: [-equity, cf1, cf2, ..., cfN + sale_proceeds]
    cfs = [-equity]
    for y in range(hold_years):
        cf = annual_summaries[y].cash_flow_after_debt
        if y == hold_years - 1:
            # Add sale proceeds in the final year
            cf += s.net_sale_proceeds
        cfs.append(cf)
    s.annual_cashflows = cfs

    # IRR and equity multiple
    s.levered_irr = compute_irr(cfs) * 100
    if equity > 0:
        s.equity_multiple = s.total_distributions / equity
        if hold_years > 0:
            s.avg_cash_on_cash = (s.cumulative_cash_flow / hold_years / equity) * 100

    return s


def _compute_refinance_scenario(
    deal: DealInput,
    annual_summaries: List[AnnualSummary],
    amort: List[dict],
    refi_year: int,
    growth: GrowthAssumptions,
    equity: float,
) -> ExitScenario:
    """Compute a refinance scenario at a given year.

    The refinance scenario has two phases:
        Phase 1: Hold from acquisition to refinance year (operating cash flow)
        Phase 2: Post-refi hold until eventual sale (operating CF with new debt service)

    The IRR is computed on the full cycle including the eventual sale.
    """
    s = ExitScenario()
    s.scenario_type = "refinance"
    s.exit_year = refi_year
    s.label = f"Refinance at Year {refi_year}"
    s.initial_equity = equity
    s.appreciation_rate = growth.value_appreciation_pct
    s.rent_growth_rate = growth.rent_increase_pct
    s.expense_growth_rate = growth.expense_increase_pct

    # Property value at refinance (appreciation-based, used for new appraisal)
    s.new_appraised_value = deal.purchase_price * (1 + growth.value_appreciation_pct / 100) ** refi_year
    s.appreciated_value = s.new_appraised_value

    # Old loan balance at refinance
    refi_month = refi_year * 12
    s.loan_balance_at_exit = get_loan_balance_at_month(amort, refi_month)

    # New loan based on appraised value and refi LTV
    s.new_loan_amount = s.new_appraised_value * (growth.refi_ltv_pct / 100)
    s.refinance_costs = s.new_loan_amount * (growth.refi_costs_pct / 100)

    # Cash out = new loan - old balance - refi costs
    s.cash_out = s.new_loan_amount - s.loan_balance_at_exit - s.refinance_costs
    # Cash out can't go below zero (if property hasn't appreciated enough)
    s.cash_out = max(s.cash_out, 0)

    # New monthly payment on the refinanced loan
    s.new_monthly_payment = monthly_payment(
        loan_amount=s.new_loan_amount,
        annual_rate_pct=growth.refi_interest_rate,
        amortization_months=growth.refi_amort_years * 12,
        canadian=True,
    )
    s.new_annual_debt_service = s.new_monthly_payment * 12

    # Cumulative cash flow during Phase 1 (acquisition to refi)
    hold_years_phase1 = min(refi_year, len(annual_summaries))
    phase1_cf = sum(
        annual_summaries[y].cash_flow_after_debt for y in range(hold_years_phase1)
    )
    s.cumulative_cash_flow = phase1_cf

    # Post-refi cash flow estimate (Phase 2)
    # Use the refi-year NOI and grow it forward for the post-refi hold period
    if refi_year <= len(annual_summaries):
        refi_year_noi = annual_summaries[refi_year - 1].noi
    else:
        refi_year_noi = annual_summaries[-1].noi if annual_summaries else 0

    s.post_refi_annual_cf = refi_year_noi - s.new_annual_debt_service

    # Eventual sale (after post-refi hold)
    total_hold = refi_year + growth.post_refi_hold_years
    s.eventual_sale_year = total_hold

    # Eventual sale price based on appreciation from original purchase
    s.eventual_sale_price = deal.purchase_price * (1 + growth.value_appreciation_pct / 100) ** total_hold

    # New loan balance at eventual sale
    post_refi_amort = build_amortization_schedule(
        loan_amount=s.new_loan_amount,
        annual_rate_pct=growth.refi_interest_rate,
        amortization_months=growth.refi_amort_years * 12,
        hold_months=growth.post_refi_hold_years * 12,
        canadian=True,
    )
    refi_loan_balance_at_sale = get_loan_balance_at_month(
        post_refi_amort, growth.post_refi_hold_years * 12
    )

    selling_costs = s.eventual_sale_price * (growth.selling_costs_pct / 100)
    s.eventual_net_proceeds = s.eventual_sale_price - selling_costs - refi_loan_balance_at_sale

    # Build full cash flow series for IRR calculation
    # Year 0: -equity
    # Years 1 to refi_year: operating CF (Phase 1)
    # Year refi_year: + cash_out from refinance
    # Years refi_year+1 to total_hold-1: post-refi operating CF (Phase 2)
    # Year total_hold: post-refi CF + eventual sale proceeds

    cfs = [-equity]

    # Phase 1: pre-refi operating cash flow
    for y in range(hold_years_phase1):
        cf = annual_summaries[y].cash_flow_after_debt
        if y == refi_year - 1:
            # Add cash-out from refinance in the refi year
            cf += s.cash_out
        cfs.append(cf)

    # Phase 2: post-refi operating cash flow + eventual sale
    for y in range(growth.post_refi_hold_years):
        # Project NOI growth from refi year
        year_from_refi = y + 1
        projected_noi = refi_year_noi * (1 + growth.rent_increase_pct / 100) ** year_from_refi
        post_refi_cf = projected_noi - s.new_annual_debt_service

        if y == growth.post_refi_hold_years - 1:
            # Final year: add eventual sale proceeds
            post_refi_cf += s.eventual_net_proceeds

        cfs.append(post_refi_cf)

    s.annual_cashflows = cfs

    # Total distributions = everything the investor gets back
    s.total_distributions = sum(cf for cf in cfs if cf > 0)

    # IRR and equity multiple
    s.levered_irr = compute_irr(cfs) * 100
    if equity > 0:
        s.equity_multiple = s.total_distributions / equity
        total_years = len(cfs) - 1  # Exclude year 0
        operating_cf = sum(cf for cf in cfs[1:]) - s.eventual_net_proceeds - s.cash_out
        if total_years > 0:
            s.avg_cash_on_cash = (operating_cf / total_years / equity) * 100

    return s


def format_exit_comparison(exit_analysis: ExitAnalysis) -> List[Dict]:
    """Format exit scenarios into a comparison-friendly list for the dashboard."""
    rows = []
    for s in exit_analysis.scenarios:
        row = {
            "label": s.label,
            "type": s.scenario_type,
            "exit_year": s.exit_year,
            "appreciated_value": s.appreciated_value,
            "loan_balance": s.loan_balance_at_exit,
            "levered_irr": s.levered_irr,
            "equity_multiple": s.equity_multiple,
            "avg_coc": s.avg_cash_on_cash,
            "total_profit": s.total_profit if s.scenario_type == "sell" else None,
            "net_sale_proceeds": s.net_sale_proceeds if s.scenario_type == "sell" else None,
            "cumulative_cf": s.cumulative_cash_flow,
            "cash_out": s.cash_out if s.scenario_type == "refinance" else None,
            "new_loan": s.new_loan_amount if s.scenario_type == "refinance" else None,
            "new_monthly_pmt": s.new_monthly_payment if s.scenario_type == "refinance" else None,
            "post_refi_cf": s.post_refi_annual_cf if s.scenario_type == "refinance" else None,
            "eventual_sale_year": s.eventual_sale_year if s.scenario_type == "refinance" else None,
            "eventual_sale_price": s.eventual_sale_price if s.scenario_type == "refinance" else None,
            "eventual_net_proceeds": s.eventual_net_proceeds if s.scenario_type == "refinance" else None,
            "appreciation_rate": s.appreciation_rate,
            "rent_growth": s.rent_growth_rate,
            "expense_growth": s.expense_growth_rate,
        }
        rows.append(row)
    return rows
