"""Database model for persisting deal analyses."""

import json
import logging
from datetime import datetime

from peewee import (
    Model, CharField, FloatField, IntegerField,
    DateTimeField, TextField, AutoField, ForeignKeyField
)
from ..database import db, BaseModel

logger = logging.getLogger(__name__)


class DealAnalysis(BaseModel):
    """A saved deal analysis — links to a listing and stores the full model output."""
    id = AutoField()
    listing_id = IntegerField(null=True, index=True)

    # Deal info
    project_name = CharField(max_length=300, default="")
    address = CharField(max_length=500, null=True)
    status = CharField(max_length=30, default="draft")  # draft, final, archived

    # Key inputs (denormalized for quick display)
    purchase_price = FloatField(default=0)
    total_units = IntegerField(default=0)
    entry_cap_rate = FloatField(null=True)

    # Key outputs (denormalized for quick display)
    levered_irr = FloatField(null=True)
    unlevered_irr = FloatField(null=True)
    equity_multiple = FloatField(null=True)
    avg_cash_on_cash = FloatField(null=True)
    avg_dscr = FloatField(null=True)
    gp_irr = FloatField(null=True)
    lp_irr = FloatField(null=True)
    total_profit = FloatField(null=True)

    # Full serialized input and output (JSON)
    input_json = TextField(default="{}")
    output_json = TextField(default="{}")

    # Metadata
    created_by = CharField(max_length=50, null=True)
    created_at = DateTimeField(default=datetime.utcnow)
    updated_at = DateTimeField(default=datetime.utcnow)
    notes = TextField(null=True)

    class Meta:
        table_name = "deal_analyses"


def init_analyst_tables():
    """Create analyst tables."""
    db.create_tables([DealAnalysis], safe=True)


def save_analysis(deal_input, output, user: str = None) -> DealAnalysis:
    """Save a deal analysis to the database."""
    from .models import AnalysisOutput
    from dataclasses import asdict

    # Serialize input and output to JSON
    input_dict = _serialize_dataclass(deal_input)
    output_dict = _serialize_output(output)

    record = DealAnalysis.create(
        listing_id=deal_input.listing_id,
        project_name=deal_input.project_name,
        address=deal_input.address,
        purchase_price=deal_input.purchase_price,
        total_units=deal_input.total_units,
        entry_cap_rate=deal_input.entry_cap_rate,
        levered_irr=output.returns.levered_irr,
        unlevered_irr=output.returns.unlevered_irr,
        equity_multiple=output.returns.levered_emx,
        avg_cash_on_cash=output.returns.avg_cash_on_cash,
        avg_dscr=output.returns.avg_dscr,
        gp_irr=output.waterfall.gp_irr,
        lp_irr=output.waterfall.lp_irr,
        total_profit=output.returns.total_profit,
        input_json=json.dumps(input_dict),
        output_json=json.dumps(output_dict),
        created_by=user,
    )

    logger.info(f"Analysis saved: #{record.id} — {deal_input.project_name}")
    return record


def get_analyses(limit: int = 50, listing_id: int = None) -> list:
    """Get saved analyses."""
    query = DealAnalysis.select()
    if listing_id:
        query = query.where(DealAnalysis.listing_id == listing_id)
    return list(query.order_by(DealAnalysis.created_at.desc()).limit(limit))


def get_analysis(analysis_id: int):
    """Get a single analysis by ID."""
    try:
        return DealAnalysis.get_by_id(analysis_id)
    except DealAnalysis.DoesNotExist:
        return None


def _serialize_dataclass(obj) -> dict:
    """Recursively serialize a dataclass to a JSON-safe dict."""
    from dataclasses import fields, is_dataclass
    if is_dataclass(obj):
        result = {}
        for f in fields(obj):
            val = getattr(obj, f.name)
            result[f.name] = _serialize_dataclass(val)
        return result
    elif isinstance(obj, list):
        return [_serialize_dataclass(item) for item in obj]
    elif isinstance(obj, dict):
        return {k: _serialize_dataclass(v) for k, v in obj.items()}
    elif isinstance(obj, datetime):
        return obj.isoformat()
    else:
        return obj


def _serialize_output(output) -> dict:
    """Serialize AnalysisOutput to a compact JSON-safe dict."""
    return {
        "returns": _serialize_dataclass(output.returns),
        "waterfall": _serialize_dataclass(output.waterfall),
        "annual_summaries": [_serialize_dataclass(s) for s in output.annual_summaries],
        "sale_price": output.sale_price,
        "net_sale_proceeds": output.net_sale_proceeds,
        "total_sources": output.total_sources,
        "total_uses": output.total_uses,
        "debt_amount": output.debt_amount,
        "equity_amount": output.equity_amount,
        "sensitivity": [_serialize_dataclass(s) for s in output.sensitivity],
        "cmhc_options": output.cmhc_options,
        "cca_schedule": output.cca_schedule,
        "after_tax_returns": output.after_tax_returns,
        "exit_analysis": output.exit_analysis,
    }
