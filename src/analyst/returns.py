"""Returns calculator — IRR, equity multiple, cash-on-cash, and all DealCheck metrics.

IRR is computed using Newton-Raphson iteration on the NPV equation.
All other metrics are algebraic from the cash flow projections.
"""

from typing import List, Optional
from .models import DealInput, MonthlyRow, AnnualSummary, ReturnMetrics


def compute_irr(cashflows: List[float], periods_per_year: int = 1,
                guess: float = 0.10, max_iter: int = 1000,
                tolerance: float = 1e-7) -> float:
    """
    Compute IRR using Newton-Raphson method.

    cashflows: list of cash flows where index 0 is the initial investment (negative)
    periods_per_year: 1 for annual, 12 for monthly
    Returns annualized IRR as a decimal (e.g., 0.08 = 8%)
    """
    if not cashflows or len(cashflows) < 2:
        return 0.0

    # Check if all cashflows are zero or same sign
    non_zero = [cf for cf in cashflows if cf != 0]
    if not non_zero:
        return 0.0
    if all(cf >= 0 for cf in non_zero) or all(cf <= 0 for cf in non_zero):
        return 0.0

    rate = guess

    for _ in range(max_iter):
        npv = 0.0
        dnpv = 0.0  # Derivative of NPV w.r.t. rate

        for t, cf in enumerate(cashflows):
            discount = (1 + rate) ** t
            if discount == 0:
                continue
            npv += cf / discount
            if t > 0:
                dnpv -= t * cf / ((1 + rate) ** (t + 1))

        if abs(dnpv) < 1e-12:
            break

        new_rate = rate - npv / dnpv

        # Guard against divergence
        if new_rate < -0.99:
            new_rate = -0.99
        if new_rate > 10.0:
            new_rate = 10.0

        if abs(new_rate - rate) < tolerance:
            rate = new_rate
            break

        rate = new_rate

    # Annualize if periods are monthly
    if periods_per_year > 1:
        annual_rate = (1 + rate) ** periods_per_year - 1
        return annual_rate

    return rate


def compute_returns(
    deal: DealInput,
    monthly: List[MonthlyRow],
    annual: List[AnnualSummary],
) -> ReturnMetrics:
    """Compute all return metrics from projected cash flows."""
    metrics = ReturnMetrics()

    if not monthly or not annual:
        return metrics

    equity = deal.equity_required
    total_cost = deal.total_acquisition_cost

    # --- Entry Cap Rate ---
    if deal.purchase_price > 0 and annual:
        year1_noi = annual[0].noi
        metrics.cap_rate_entry = (year1_noi / deal.purchase_price) * 100

    # --- Exit Cap Rate ---
    metrics.cap_rate_exit = deal.terminal_cap_rate

    # --- LTV ---
    if deal.purchase_price > 0:
        metrics.ltv_at_purchase = (deal.financing.loan_amount / deal.purchase_price) * 100

    # --- Unlevered IRR ---
    # Cash flows: -total_cost at t=0, then annual NOI, plus sale proceeds in final year
    unlevered_cfs = [-total_cost]
    for s in annual:
        cf = s.noi
        if s.sale_price > 0:
            cf += s.sale_price - (s.sale_price * (deal.selling_cost_percent / 100))
        unlevered_cfs.append(cf)
    metrics.unlevered_irr = compute_irr(unlevered_cfs) * 100

    # --- Unlevered Equity Multiple ---
    total_unlevered_in = sum(cf for cf in unlevered_cfs if cf > 0)
    if total_cost > 0:
        metrics.unlevered_emx = total_unlevered_in / total_cost

    # --- Levered IRR ---
    # Cash flows: -equity at t=0, then annual cash flow after debt, plus net sale proceeds
    levered_cfs = [-equity]
    for s in annual:
        cf = s.cash_flow_after_debt
        if s.net_sale_proceeds > 0:
            cf += s.net_sale_proceeds
        levered_cfs.append(cf)
    metrics.levered_irr = compute_irr(levered_cfs) * 100

    # --- Levered Equity Multiple ---
    total_levered_in = sum(cf for cf in levered_cfs if cf > 0)
    if equity > 0:
        metrics.levered_emx = total_levered_in / equity

    # --- Average Cash on Cash ---
    hold_years = len(annual)
    if equity > 0 and hold_years > 0:
        # Exclude the exit year's sale proceeds from CoC calculation
        operating_coc_list = []
        for s in annual:
            coc = s.cash_flow_after_debt / equity if equity > 0 else 0
            operating_coc_list.append(coc)
        metrics.avg_cash_on_cash = (sum(operating_coc_list) / hold_years) * 100

    # --- Total Profit ---
    metrics.total_profit = sum(cf for cf in levered_cfs)

    # --- ROI ---
    if equity > 0:
        metrics.roi = (metrics.total_profit / equity) * 100

    # --- DSCR ---
    dscr_values = [s.dscr for s in annual if s.dscr > 0]
    if dscr_values:
        metrics.avg_dscr = sum(dscr_values) / len(dscr_values)
        metrics.min_dscr = min(dscr_values)

    # --- Debt Yield ---
    dy_values = [s.debt_yield for s in annual if s.debt_yield > 0]
    if dy_values:
        metrics.avg_debt_yield = (sum(dy_values) / len(dy_values)) * 100

    # --- Break-Even Ratio ---
    if annual and annual[0].gross_rent > 0:
        year1 = annual[0]
        ber = (year1.total_expenses + year1.debt_service) / year1.gross_rent
        metrics.break_even_ratio = ber * 100

    return metrics
