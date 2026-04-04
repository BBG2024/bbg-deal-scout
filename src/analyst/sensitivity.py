"""Sensitivity and scenario analysis engine.

Runs the deal model multiple times with varied assumptions to show how
returns change under different conditions. Produces both single-variable
sensitivity tables and multi-scenario comparisons.
"""

import copy
from typing import List, Dict, Tuple
from .models import DealInput, SensitivityResult


def run_sensitivity(
    base_deal: DealInput,
    engine_func,  # The run_analysis function from engine.py
) -> List[SensitivityResult]:
    """
    Run sensitivity analysis across key variables.

    Varies each variable independently while holding others at base case.
    Returns a flat list of SensitivityResult objects.
    """
    results = []

    # Define sensitivity ranges for each variable
    sensitivity_vars = {
        "vacancy_percent": {
            "label": "Vacancy Rate",
            "values": [0, 2, 3, 5, 8, 10, 15],
            "field": "vacancy_percent",
        },
        "rent_growth_percent": {
            "label": "Annual Rent Growth",
            "values": [0, 1, 2, 3, 4, 5],
            "field": "rent_growth_percent",
        },
        "interest_rate": {
            "label": "Interest Rate",
            "values": [3.0, 3.5, 4.0, 4.25, 4.5, 5.0, 5.5, 6.0],
            "field_path": ("financing", "interest_rate"),
        },
        "terminal_cap_rate": {
            "label": "Exit Cap Rate",
            "values": [4.0, 4.5, 5.0, 5.5, 6.0, 6.5, 7.0],
            "field": "terminal_cap_rate",
        },
        "purchase_price": {
            "label": "Purchase Price Variance",
            "multipliers": [0.90, 0.95, 1.00, 1.05, 1.10, 1.15],
            "field": "purchase_price",
        },
        "expense_growth_percent": {
            "label": "Annual Expense Growth",
            "values": [0, 1, 2, 3, 4, 5],
            "field": "expense_growth_percent",
        },
    }

    for var_key, var_config in sensitivity_vars.items():
        label = var_config["label"]

        if "multipliers" in var_config:
            # Price variance — apply multiplier to base value
            base_val = getattr(base_deal, var_config["field"])
            test_values = [(m, base_val * m) for m in var_config["multipliers"]]
        elif "values" in var_config:
            test_values = [(v, v) for v in var_config["values"]]
        else:
            continue

        for display_val, actual_val in test_values:
            try:
                # Deep copy the deal and modify the variable
                test_deal = _deep_copy_deal(base_deal)

                if "field_path" in var_config:
                    # Nested field (e.g., financing.interest_rate)
                    obj = test_deal
                    path = var_config["field_path"]
                    for p in path[:-1]:
                        obj = getattr(obj, p)
                    setattr(obj, path[-1], actual_val)
                else:
                    setattr(test_deal, var_config["field"], actual_val)

                # Run the analysis
                output = engine_func(test_deal)

                results.append(SensitivityResult(
                    scenario_name=f"{label}: {display_val}",
                    variable_changed=var_key,
                    variable_value=display_val,
                    levered_irr=output.returns.levered_irr,
                    unlevered_irr=output.returns.unlevered_irr,
                    cash_on_cash=output.returns.avg_cash_on_cash,
                    equity_multiple=output.returns.levered_emx,
                    dscr=output.returns.avg_dscr,
                    gp_irr=output.waterfall.gp_irr,
                    lp_irr=output.waterfall.lp_irr,
                ))

            except Exception:
                # If a scenario fails (e.g., negative loan), skip it
                continue

    return results


def run_scenarios(
    base_deal: DealInput,
    engine_func,
) -> List[SensitivityResult]:
    """
    Run pre-defined scenarios: Base Case, Best Case, Worst Case, Stress Test.
    """
    scenarios = [
        {
            "name": "Base Case",
            "changes": {},
        },
        {
            "name": "Best Case",
            "changes": {
                "vacancy_percent": 2.0,
                "rent_growth_percent": 4.0,
                "terminal_cap_rate": 4.5,
                "expense_growth_percent": 1.5,
            },
        },
        {
            "name": "Conservative",
            "changes": {
                "vacancy_percent": 8.0,
                "rent_growth_percent": 1.0,
                "terminal_cap_rate": 6.0,
                "expense_growth_percent": 3.0,
            },
        },
        {
            "name": "Stress Test",
            "changes": {
                "vacancy_percent": 15.0,
                "rent_growth_percent": 0.0,
                "terminal_cap_rate": 7.0,
                "expense_growth_percent": 4.0,
                "financing.interest_rate": base_deal.financing.interest_rate + 1.5,
            },
        },
    ]

    results = []

    for scenario in scenarios:
        try:
            test_deal = _deep_copy_deal(base_deal)

            for key, val in scenario["changes"].items():
                if "." in key:
                    parts = key.split(".")
                    obj = test_deal
                    for p in parts[:-1]:
                        obj = getattr(obj, p)
                    setattr(obj, parts[-1], val)
                else:
                    setattr(test_deal, key, val)

            output = engine_func(test_deal)

            results.append(SensitivityResult(
                scenario_name=scenario["name"],
                variable_changed="scenario",
                variable_value=0,
                levered_irr=output.returns.levered_irr,
                unlevered_irr=output.returns.unlevered_irr,
                cash_on_cash=output.returns.avg_cash_on_cash,
                equity_multiple=output.returns.levered_emx,
                dscr=output.returns.avg_dscr,
                gp_irr=output.waterfall.gp_irr,
                lp_irr=output.waterfall.lp_irr,
            ))

        except Exception:
            continue

    return results


def build_sensitivity_matrix(
    results: List[SensitivityResult],
    row_variable: str,
    metric: str = "levered_irr",
) -> Dict:
    """
    Build a 2D sensitivity matrix for a single variable.

    Returns dict with: variable_name, values, metric_name, data
    """
    filtered = [r for r in results if r.variable_changed == row_variable]
    if not filtered:
        return {}

    values = [r.variable_value for r in filtered]
    data = [getattr(r, metric, 0) for r in filtered]

    return {
        "variable": row_variable,
        "values": values,
        "metric": metric,
        "data": data,
    }


def _deep_copy_deal(deal: DealInput) -> DealInput:
    """Deep copy a DealInput, handling nested dataclasses."""
    return copy.deepcopy(deal)
