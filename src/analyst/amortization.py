"""Amortization engine — Canadian mortgage math.

Canadian mortgages use semi-annual compounding, which differs from US monthly
compounding. This module handles both conventions.

The effective monthly rate for Canadian semi-annual compounding:
    semi_annual_rate = annual_rate / 2
    effective_monthly = (1 + semi_annual_rate)^(1/6) - 1
"""

import math
from typing import List, Tuple
from .models import FinancingInput


def effective_monthly_rate(annual_rate_pct: float, canadian: bool = True) -> float:
    """
    Convert an annual interest rate to an effective monthly rate.

    For Canadian mortgages (semi-annual compounding):
        monthly = (1 + annual/2)^(1/6) - 1

    For US mortgages (monthly compounding):
        monthly = annual / 12
    """
    annual = annual_rate_pct / 100
    if annual <= 0:
        return 0.0

    if canadian:
        semi = annual / 2
        return (1 + semi) ** (1 / 6) - 1
    else:
        return annual / 12


def monthly_payment(
    loan_amount: float,
    annual_rate_pct: float,
    amortization_months: int,
    interest_only: bool = False,
    canadian: bool = True,
) -> float:
    """Calculate the fixed monthly mortgage payment."""
    if loan_amount <= 0 or amortization_months <= 0:
        return 0.0

    r = effective_monthly_rate(annual_rate_pct, canadian)

    if interest_only or r <= 0:
        return loan_amount * r

    # Standard amortization formula: PMT = P * r(1+r)^n / ((1+r)^n - 1)
    n = amortization_months
    numerator = r * (1 + r) ** n
    denominator = (1 + r) ** n - 1

    if denominator == 0:
        return 0.0

    return loan_amount * (numerator / denominator)


def build_amortization_schedule(
    loan_amount: float,
    annual_rate_pct: float,
    amortization_months: int,
    hold_months: int,
    interest_only: bool = False,
    canadian: bool = True,
) -> List[dict]:
    """
    Build a month-by-month amortization schedule.

    Returns list of dicts with:
        month, beginning_balance, payment, principal, interest, ending_balance
    """
    if loan_amount <= 0:
        return []

    r = effective_monthly_rate(annual_rate_pct, canadian)
    pmt = monthly_payment(loan_amount, annual_rate_pct, amortization_months,
                          interest_only, canadian)

    schedule = []
    balance = loan_amount

    # Generate schedule for hold period + 1 month (for exit month payoff)
    for m in range(1, hold_months + 2):
        if balance <= 0:
            break

        interest = balance * r

        if interest_only:
            principal = 0.0
            payment = interest
        else:
            payment = min(pmt, balance + interest)  # Don't overpay on last month
            principal = payment - interest

        ending_balance = balance - principal

        # Prevent tiny negative balances from floating point
        if ending_balance < 0.01:
            ending_balance = 0.0

        schedule.append({
            "month": m,
            "beginning_balance": round(balance, 2),
            "payment": round(payment, 2),
            "principal": round(principal, 2),
            "interest": round(interest, 2),
            "ending_balance": round(ending_balance, 2),
        })

        balance = ending_balance

    return schedule


def get_loan_balance_at_month(schedule: List[dict], month: int) -> float:
    """Get the remaining loan balance at a specific month."""
    if not schedule or month < 1:
        return 0.0
    if month > len(schedule):
        return schedule[-1]["ending_balance"] if schedule else 0.0
    return schedule[month - 1]["ending_balance"]


def get_payment_split(schedule: List[dict], month: int) -> Tuple[float, float, float]:
    """Get (total_payment, principal, interest) for a specific month."""
    if not schedule or month < 1 or month > len(schedule):
        return (0.0, 0.0, 0.0)
    row = schedule[month - 1]
    return (row["payment"], row["principal"], row["interest"])
