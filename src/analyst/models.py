"""Data models for the BBG Deal Analyst.

These dataclasses mirror the TEMPLATE WATERFALL 5 years.xlsx structure exactly.
Every input field maps to a cell in the Variables sheet; every output maps to
a computed cell in the Cash Flow, Return Matrix, or Waterfall sheets.
"""

from dataclasses import dataclass, field
from typing import List, Optional, Dict
from datetime import datetime


# ────────────────────────────────────────────────────────────
# INPUT MODELS — What the user fills in
# ────────────────────────────────────────────────────────────

@dataclass
class UnitInput:
    """A single rental unit."""
    name: str = "Unit 1"              # Display label
    count: int = 1                     # Number of this unit type
    current_rent: float = 0.0          # In-place monthly rent
    post_reno_rent: float = 0.0        # Market/post-renovation rent
    sf: float = 0.0                    # Square footage
    reno_capex: float = 0.0            # Renovation cost for this unit
    reno_start_month: int = 0          # Month renovation starts (0 = at close)
    reno_end_month: int = 0            # Month renovation ends


@dataclass
class FinancingInput:
    """Mortgage / loan terms."""
    loan_amount: float = 0.0           # Computed from LTV if not set directly
    ltv_percent: float = 60.0          # Loan-to-value (% of purchase price)
    interest_rate: float = 4.25        # Annual interest rate (%)
    amortization_months: int = 480     # Amortization period (months)
    mortgage_insurance: float = 0.0    # Upfront PMI amount ($)
    interest_only: bool = False        # Interest-only loan flag
    start_month: int = 0               # Month loan begins
    # Second mortgage (optional)
    has_second_mortgage: bool = False
    second_loan_amount: float = 0.0
    second_interest_rate: float = 0.0
    second_amortization_months: int = 480


@dataclass
class ExpenseInput:
    """Operating expense line item."""
    name: str = ""
    monthly_amount: float = 0.0


@dataclass
class WaterfallTier:
    """A single tier in the waterfall distribution."""
    name: str = ""
    gp_share: float = 0.0             # GP's share (decimal, e.g., 0.20 = 20%)
    lp_share: float = 0.0             # LP's share (decimal, e.g., 0.80 = 80%)
    irr_hurdle: Optional[float] = None # IRR threshold to reach this tier (decimal)


@dataclass
class DealInput:
    """Complete deal analysis input — maps to the Variables sheet."""
    # --- Property Info ---
    project_name: str = ""
    address: str = ""
    company_name: str = "Blue Bear Group Corp."

    # --- Acquisition ---
    purchase_price: float = 0.0
    land_value: float = 0.0            # For depreciation calculation
    renovation_cost: float = 0.0       # Total (also sum of unit-level renos)
    acquisition_fee_percent: float = 3.0  # % of purchase price
    closing_costs: float = 0.0         # Additional closing costs

    # --- Units ---
    units: List[UnitInput] = field(default_factory=list)

    # --- Financing ---
    financing: FinancingInput = field(default_factory=FinancingInput)

    # --- Operating Expenses (monthly amounts) ---
    # Empty by default — user fills in from their own deal data
    expenses: List[ExpenseInput] = field(default_factory=lambda: [
        ExpenseInput("Insurance", 0),
        ExpenseInput("Municipal Taxes", 0),
        ExpenseInput("School Taxes", 0),
        ExpenseInput("Snow Removal", 0),
        ExpenseInput("Lawn/Landscaping", 0),
        ExpenseInput("Utility", 0),
        ExpenseInput("Maintenance", 0),
        ExpenseInput("Management", 0),
        ExpenseInput("Concierge", 0),
    ])

    # --- Growth Assumptions ---
    rent_growth_percent: float = 2.0      # Annual rent increase (%)
    expense_growth_percent: float = 2.0   # Annual expense increase (%)
    vacancy_percent: float = 3.0          # Vacancy allowance (%)
    other_income_monthly: float = 0.0     # Parking, laundry, etc.
    capex_reserve_monthly: float = 0.0    # Monthly CapEx reserve

    # --- Exit Assumptions ---
    exit_month: int = 120                 # Hold period (months)
    terminal_cap_rate: float = 5.0        # Exit cap rate (%)
    selling_cost_percent: float = 5.0     # Selling costs (% of sale price)

    # --- Partnership Structure ---
    gp_equity_percent: float = 20.0       # GP's equity share (%)
    lp_equity_percent: float = 80.0       # LP's equity share (%)

    # --- Waterfall Tiers (BBG default: 3-tier) ---
    waterfall_tiers: List[WaterfallTier] = field(default_factory=lambda: [
        WaterfallTier("Preferred Return", 0.20, 0.80, 0.08),
        WaterfallTier("Hurdle 2", 0.50, 0.50, 0.08),
        WaterfallTier("Above Hurdle", 0.70, 0.30, None),
    ])

    # --- Metadata ---
    listing_id: Optional[int] = None      # Link to Deal Scout listing
    created_at: Optional[datetime] = None
    created_by: Optional[str] = None

    @property
    def total_units(self) -> int:
        return sum(u.count for u in self.units)

    @property
    def total_monthly_rent(self) -> float:
        return sum(u.post_reno_rent * u.count for u in self.units)

    @property
    def acquisition_fee(self) -> float:
        return self.purchase_price * (self.acquisition_fee_percent / 100)

    @property
    def total_acquisition_cost(self) -> float:
        return (self.purchase_price + self.renovation_cost +
                self.acquisition_fee + self.financing.mortgage_insurance +
                self.closing_costs)

    @property
    def equity_required(self) -> float:
        return self.total_acquisition_cost - self.financing.loan_amount

    @property
    def gp_equity(self) -> float:
        return self.equity_required * (self.gp_equity_percent / 100)

    @property
    def lp_equity(self) -> float:
        return self.equity_required * (self.lp_equity_percent / 100)

    @property
    def entry_cap_rate(self) -> float:
        """Entry cap rate = Stabilized NOI / Purchase Price."""
        monthly_expenses = sum(e.monthly_amount for e in self.expenses)
        monthly_rent = self.total_monthly_rent
        vacancy = monthly_rent * (self.vacancy_percent / 100)
        monthly_noi = monthly_rent - vacancy + self.other_income_monthly - monthly_expenses
        annual_noi = monthly_noi * 12
        if self.purchase_price <= 0:
            return 0.0
        return (annual_noi / self.purchase_price) * 100

    def compute_loan_amount(self):
        """Set loan amount from LTV if not explicitly set.
        BBG convention: loan = purchase_price * LTV% + mortgage_insurance."""
        if self.financing.loan_amount <= 0:
            self.financing.loan_amount = (
                self.purchase_price * (self.financing.ltv_percent / 100)
                + self.financing.mortgage_insurance
            )


# ────────────────────────────────────────────────────────────
# OUTPUT MODELS — What the engine computes
# ────────────────────────────────────────────────────────────

@dataclass
class MonthlyRow:
    """One month of the cash flow projection."""
    month: int = 0
    year: int = 0

    # Income
    gross_rent: float = 0.0
    other_income: float = 0.0
    vacancy: float = 0.0
    effective_gross_income: float = 0.0

    # Expenses
    total_expenses: float = 0.0
    expense_breakdown: Dict[str, float] = field(default_factory=dict)

    # NOI
    noi: float = 0.0
    capex_reserve: float = 0.0
    cash_flow_from_ops: float = 0.0

    # Debt service
    debt_service: float = 0.0
    principal_paid: float = 0.0
    interest_paid: float = 0.0

    # After debt
    cash_flow_after_debt: float = 0.0

    # Risk metrics
    loan_balance: float = 0.0
    dscr: float = 0.0
    debt_yield: float = 0.0

    # Sale (only in exit month)
    sale_price: float = 0.0
    selling_costs: float = 0.0
    loan_payoff: float = 0.0
    net_sale_proceeds: float = 0.0


@dataclass
class AnnualSummary:
    """Annual rollup of monthly data."""
    year: int = 0
    gross_rent: float = 0.0
    vacancy: float = 0.0
    other_income: float = 0.0
    effective_gross_income: float = 0.0
    total_expenses: float = 0.0
    noi: float = 0.0
    debt_service: float = 0.0
    cash_flow_after_debt: float = 0.0
    loan_balance_eoy: float = 0.0
    dscr: float = 0.0
    debt_yield: float = 0.0
    # Unlevered
    unlevered_cash_flow: float = 0.0
    free_and_clear_return: float = 0.0
    # Levered
    levered_cash_flow: float = 0.0
    cash_on_cash: float = 0.0
    # Sale year extras
    sale_price: float = 0.0
    net_sale_proceeds: float = 0.0


@dataclass
class ReturnMetrics:
    """Computed investment return metrics."""
    # Unlevered
    unlevered_irr: float = 0.0
    unlevered_emx: float = 0.0          # Equity multiple

    # Levered
    levered_irr: float = 0.0
    levered_emx: float = 0.0
    avg_cash_on_cash: float = 0.0

    # Property-level
    cap_rate_entry: float = 0.0
    cap_rate_exit: float = 0.0
    total_profit: float = 0.0
    roi: float = 0.0

    # Risk ratios
    avg_dscr: float = 0.0
    min_dscr: float = 0.0
    avg_debt_yield: float = 0.0
    ltv_at_purchase: float = 0.0
    break_even_ratio: float = 0.0


@dataclass
class WaterfallResult:
    """Waterfall distribution output."""
    # GP
    gp_contribution: float = 0.0
    gp_distributions: float = 0.0
    gp_profit: float = 0.0
    gp_irr: float = 0.0
    gp_emx: float = 0.0
    gp_annual_cashflows: List[float] = field(default_factory=list)

    # LP
    lp_contribution: float = 0.0
    lp_distributions: float = 0.0
    lp_profit: float = 0.0
    lp_irr: float = 0.0
    lp_emx: float = 0.0
    lp_annual_cashflows: List[float] = field(default_factory=list)

    # Per-tier breakdown
    tier_distributions: List[Dict] = field(default_factory=list)


@dataclass
class SensitivityResult:
    """Result of a sensitivity/scenario analysis."""
    scenario_name: str = ""
    variable_changed: str = ""
    variable_value: float = 0.0
    levered_irr: float = 0.0
    unlevered_irr: float = 0.0
    cash_on_cash: float = 0.0
    equity_multiple: float = 0.0
    dscr: float = 0.0
    gp_irr: float = 0.0
    lp_irr: float = 0.0


@dataclass
class AnalysisOutput:
    """Complete analysis output — the full model result."""
    deal_input: DealInput = field(default_factory=DealInput)

    # Sources & Uses
    total_sources: float = 0.0
    total_uses: float = 0.0
    debt_amount: float = 0.0
    equity_amount: float = 0.0

    # Monthly cash flows (120 months)
    monthly_cashflows: List[MonthlyRow] = field(default_factory=list)

    # Annual summaries
    annual_summaries: List[AnnualSummary] = field(default_factory=list)

    # Return metrics
    returns: ReturnMetrics = field(default_factory=ReturnMetrics)

    # Waterfall distributions
    waterfall: WaterfallResult = field(default_factory=WaterfallResult)

    # Sensitivity analyses
    sensitivity: List[SensitivityResult] = field(default_factory=list)

    # Sale info
    sale_price: float = 0.0
    net_sale_proceeds: float = 0.0

    # CMHC financing comparison
    cmhc_options: List[Dict] = field(default_factory=list)

    # CCA depreciation schedule
    cca_schedule: Optional[Dict] = None
    after_tax_returns: Optional[Dict] = None

    # Exit strategy analysis (sell/refinance at Year 5 and 10)
    exit_analysis: Optional[Dict] = None

    # Metadata
    computed_at: Optional[datetime] = None
