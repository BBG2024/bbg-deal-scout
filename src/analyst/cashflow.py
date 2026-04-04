"""Cash flow projection engine.

Builds month-by-month cash flow projections for the full hold period,
matching the BBG TEMPLATE WATERFALL structure exactly. Income grows
annually by rent_growth_percent; expenses grow by expense_growth_percent
starting at the boundary of each new year (month 13, 25, 37, etc.).
"""

import math
from typing import List, Dict
from .models import DealInput, MonthlyRow, AnnualSummary
from .amortization import (
    build_amortization_schedule, get_payment_split, get_loan_balance_at_month
)


def project_monthly_cashflows(deal: DealInput) -> List[MonthlyRow]:
    """
    Generate month-by-month cash flows for the full hold period.

    This replicates the Monthly CF sheet in the BBG waterfall template:
    - Rent grows annually (applied at year boundary)
    - Expenses grow annually (applied at year boundary, starting Year 4 in template)
    - Vacancy as % of gross rent
    - Debt service from amortization schedule
    - Sale proceeds in exit month
    """
    deal.compute_loan_amount()
    hold = deal.exit_month  # Total months (e.g., 120)

    # Build amortization schedule for primary mortgage
    amort = build_amortization_schedule(
        loan_amount=deal.financing.loan_amount,
        annual_rate_pct=deal.financing.interest_rate,
        amortization_months=deal.financing.amortization_months,
        hold_months=hold,
        interest_only=deal.financing.interest_only,
    )

    # Precompute rent schedule per unit per month (with annual growth)
    rent_schedule = _build_rent_schedule(deal, hold)

    # Precompute expense schedule (with annual growth, starting Year 4)
    expense_schedule = _build_expense_schedule(deal, hold)

    rows = []

    for m in range(1, hold + 1):
        year = math.ceil(m / 12)
        row = MonthlyRow(month=m, year=year)

        # --- Income ---
        row.gross_rent = rent_schedule[m]
        row.other_income = deal.other_income_monthly
        row.vacancy = row.gross_rent * (deal.vacancy_percent / 100)
        row.effective_gross_income = row.gross_rent - row.vacancy + row.other_income

        # --- Expenses ---
        row.expense_breakdown = expense_schedule[m]
        row.total_expenses = sum(row.expense_breakdown.values())

        # --- NOI ---
        row.noi = row.effective_gross_income - row.total_expenses
        row.capex_reserve = deal.capex_reserve_monthly
        row.cash_flow_from_ops = row.noi - row.capex_reserve

        # --- Debt Service ---
        if m <= len(amort):
            pmt, princ, interest = get_payment_split(amort, m)
            row.debt_service = pmt
            row.principal_paid = princ
            row.interest_paid = interest
            row.loan_balance = get_loan_balance_at_month(amort, m)
        else:
            row.debt_service = 0.0
            row.loan_balance = 0.0

        # --- Cash Flow After Debt ---
        row.cash_flow_after_debt = row.cash_flow_from_ops - row.debt_service

        # --- Risk Metrics ---
        if row.debt_service > 0:
            row.dscr = row.noi / row.debt_service
        if deal.financing.loan_amount > 0:
            row.debt_yield = (row.noi * 12) / deal.financing.loan_amount

        # --- Sale (exit month only) ---
        if m == hold:
            # Sale price = Forward NOI (next year projected) / Terminal Cap Rate
            # BBG convention: sell based on next year's projected NOI, not trailing
            forward_noi = row.noi * 12 * (1 + deal.rent_growth_percent / 100)
            if deal.terminal_cap_rate > 0:
                row.sale_price = forward_noi / (deal.terminal_cap_rate / 100)
            row.selling_costs = row.sale_price * (deal.selling_cost_percent / 100)
            row.loan_payoff = row.loan_balance
            row.net_sale_proceeds = row.sale_price - row.selling_costs - row.loan_payoff

        rows.append(row)

    return rows


def summarize_annual(monthly: List[MonthlyRow], deal: DealInput) -> List[AnnualSummary]:
    """Aggregate monthly cash flows into annual summaries."""
    if not monthly:
        return []

    # Determine number of years
    max_year = max(r.year for r in monthly)
    summaries = []

    for y in range(1, max_year + 1):
        months_in_year = [r for r in monthly if r.year == y]
        if not months_in_year:
            continue

        s = AnnualSummary(year=y)
        s.gross_rent = sum(r.gross_rent for r in months_in_year)
        s.vacancy = sum(r.vacancy for r in months_in_year)
        s.other_income = sum(r.other_income for r in months_in_year)
        s.effective_gross_income = sum(r.effective_gross_income for r in months_in_year)
        s.total_expenses = sum(r.total_expenses for r in months_in_year)
        s.noi = sum(r.noi for r in months_in_year)
        s.debt_service = sum(r.debt_service for r in months_in_year)
        s.cash_flow_after_debt = sum(r.cash_flow_after_debt for r in months_in_year)

        # End-of-year loan balance
        last_month = months_in_year[-1]
        s.loan_balance_eoy = last_month.loan_balance

        # DSCR and debt yield (annual)
        if s.debt_service > 0:
            s.dscr = s.noi / s.debt_service
        if deal.financing.loan_amount > 0:
            s.debt_yield = s.noi / deal.financing.loan_amount

        # Unlevered cash flow = NOI (no debt service)
        s.unlevered_cash_flow = s.noi
        if deal.total_acquisition_cost > 0:
            s.free_and_clear_return = s.noi / deal.total_acquisition_cost

        # Levered cash flow
        s.levered_cash_flow = s.cash_flow_after_debt
        equity = deal.equity_required
        if equity > 0:
            s.cash_on_cash = s.cash_flow_after_debt / equity

        # Sale year
        if last_month.sale_price > 0:
            s.sale_price = last_month.sale_price
            s.net_sale_proceeds = last_month.net_sale_proceeds

        summaries.append(s)

    return summaries


def _build_rent_schedule(deal: DealInput, hold_months: int) -> Dict[int, float]:
    """
    Build month-by-month total rent, applying annual rent growth at year boundaries.

    Rents step up at month 13, 25, 37, etc. — matching the BBG template where
    rents grow annually at the start of each new year.
    """
    growth = deal.rent_growth_percent / 100
    schedule = {}

    for m in range(1, hold_months + 1):
        year = math.ceil(m / 12)
        # Year 1 = base rent, Year 2 = base * (1+g), etc.
        growth_factor = (1 + growth) ** (year - 1)

        monthly_rent = 0.0
        for unit in deal.units:
            # Use post-reno rent (if reno complete) or current rent
            if m >= unit.reno_end_month and unit.reno_end_month > 0:
                base_rent = unit.post_reno_rent
            else:
                base_rent = unit.post_reno_rent  # Using stabilized rent
            monthly_rent += base_rent * unit.count * growth_factor

        schedule[m] = monthly_rent

    return schedule


def _build_expense_schedule(deal: DealInput, hold_months: int) -> Dict[int, Dict[str, float]]:
    """
    Build month-by-month expenses with annual growth.

    In the BBG template, expenses are flat for Years 1-3, then grow
    annually starting Year 4. We implement configurable growth starting
    at the year boundary.
    """
    growth = deal.expense_growth_percent / 100
    expense_growth_start_year = 4  # Match BBG template: expenses grow from Year 4

    schedule = {}

    for m in range(1, hold_months + 1):
        year = math.ceil(m / 12)

        # Apply growth only from year 4 onward (matching BBG template)
        if year >= expense_growth_start_year:
            years_of_growth = year - expense_growth_start_year + 1
            growth_factor = (1 + growth) ** years_of_growth
        else:
            growth_factor = 1.0

        month_expenses = {}
        for exp in deal.expenses:
            month_expenses[exp.name] = exp.monthly_amount * growth_factor

        schedule[m] = month_expenses

    return schedule
