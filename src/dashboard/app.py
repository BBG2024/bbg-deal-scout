"""BBG Deal Scout — Web Dashboard (FastAPI)."""

import json
import logging
import secrets
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI, Request, Depends, HTTPException, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware

from ..database import (
    init_db, get_listings, get_stats, get_scan_logs,
    Listing, db
)
from ..config import get_config

logger = logging.getLogger(__name__)

app = FastAPI(title="BBG Deal Scout", docs_url=None, redoc_url=None)
app.add_middleware(SessionMiddleware, secret_key=secrets.token_hex(32))

templates_dir = Path(__file__).parent / "templates"
templates = Jinja2Templates(directory=str(templates_dir))


def get_current_user(request: Request):
    """Simple session-based auth."""
    user = request.session.get("user")
    if not user:
        raise HTTPException(status_code=303, headers={"Location": "/login"})
    return user


_startup_error: str = None


@app.on_event("startup")
async def startup():
    global _startup_error
    try:
        cfg = get_config()
        init_db(cfg.get("general", {}).get("database_path", "data/deal_scout.db"))
    except Exception as e:
        import traceback
        _startup_error = traceback.format_exc()
        logger.error(f"Startup error: {_startup_error}")


@app.get("/debug")
async def debug_info():
    """Diagnostic endpoint — shows container state."""
    import os, sys
    info = {
        "templates_dir": str(templates_dir),
        "templates_dir_exists": templates_dir.exists(),
        "login_html_exists": (templates_dir / "login.html").exists(),
        "template_files": os.listdir(str(templates_dir)) if templates_dir.exists() else [],
        "config_yaml_exists": Path("/app/config.yaml").exists(),
        "cwd": os.getcwd(),
        "file": __file__,
        "sys_path_0": sys.path[0] if sys.path else None,
        "startup_error": _startup_error,
        "env_db_path": os.environ.get("BBG_DB_PATH"),
        "env_admin_pw_set": bool(os.environ.get("BBG_ADMIN_PASSWORD")),
    }
    # Test template load
    try:
        t = templates.get_template("login.html")
        info["template_load_test"] = "OK: " + t.name
    except Exception as e:
        info["template_load_test"] = "ERROR: " + str(e)
    return info


@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    return templates.TemplateResponse("login.html", {"request": request, "error": None})


@app.post("/login")
async def login(request: Request, username: str = Form(...), password: str = Form(...)):
    cfg = get_config()
    users = cfg.get("dashboard", {}).get("users", {})

    if username in users and users[username] == password:
        request.session["user"] = username
        return RedirectResponse(url="/", status_code=303)
    return templates.TemplateResponse(
        "login.html", {"request": request, "error": "Invalid credentials"}
    )


@app.get("/logout")
async def logout(request: Request):
    request.session.clear()
    return RedirectResponse(url="/login")


@app.get("/", response_class=HTMLResponse)
async def dashboard(
    request: Request,
    region: str = None,
    status: str = None,
    min_score: int = None,
    page: int = 1,
    user: str = Depends(get_current_user),
):
    per_page = 25
    offset = (page - 1) * per_page

    listings = get_listings(
        region=region, status=status, min_score=min_score,
        limit=per_page, offset=offset,
    )

    # Parse tier1_details for display
    for l in listings:
        if l.tier1_details:
            try:
                l._details_parsed = json.loads(l.tier1_details)
            except Exception:
                l._details_parsed = {}
        else:
            l._details_parsed = {}

    stats = get_stats()
    scan_logs = get_scan_logs(limit=5)

    return templates.TemplateResponse("index.html", {
        "request": request,
        "user": user,
        "listings": listings,
        "stats": stats,
        "scan_logs": scan_logs,
        "filters": {"region": region, "status": status, "min_score": min_score},
        "page": page,
        "per_page": per_page,
    })


@app.post("/listings/{listing_id}/status")
async def update_status(
    listing_id: int,
    status: str = Form(...),
    notes: str = Form(None),
    user: str = Depends(get_current_user),
):
    try:
        listing = Listing.get_by_id(listing_id)
        listing.status = status
        if notes:
            existing = listing.notes or ""
            timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M")
            listing.notes = f"{existing}\n[{timestamp} — {user}] {notes}".strip()
        if status == "reviewed":
            listing.reviewed_at = datetime.utcnow()
        listing.save()
    except Listing.DoesNotExist:
        raise HTTPException(status_code=404, detail="Listing not found")

    return RedirectResponse(url="/", status_code=303)


@app.post("/listings/{listing_id}/flag")
async def toggle_flag(listing_id: int, user: str = Depends(get_current_user)):
    try:
        listing = Listing.get_by_id(listing_id)
        listing.flagged = not listing.flagged
        listing.save()
    except Listing.DoesNotExist:
        raise HTTPException(status_code=404, detail="Listing not found")

    return RedirectResponse(url="/", status_code=303)


# --- Source Management ---

@app.get("/sources", response_class=HTMLResponse)
async def sources_page(request: Request, user: str = Depends(get_current_user)):
    from ..sources import get_all_sources, get_source_performance, init_source_tables
    init_source_tables()
    sources = get_all_sources()
    performance = get_source_performance()
    return templates.TemplateResponse("sources.html", {
        "request": request,
        "user": user,
        "sources": sources,
        "performance": performance,
    })


@app.post("/sources/add")
async def add_source_route(
    request: Request,
    source_type: str = Form(...),
    url: str = Form(...),
    label: str = Form(...),
    region: str = Form("all"),
    notes: str = Form(None),
    user: str = Depends(get_current_user),
):
    from ..sources import add_source, init_source_tables
    init_source_tables()
    add_source(
        source_type=source_type,
        url=url,
        label=label,
        region=region,
        notes=notes,
        added_by=user,
    )
    return RedirectResponse(url="/sources", status_code=303)


@app.post("/sources/{source_id}/delete")
async def delete_source_route(source_id: int, user: str = Depends(get_current_user)):
    from ..sources import remove_source
    remove_source(source_id)
    return RedirectResponse(url="/sources", status_code=303)


@app.post("/sources/{source_id}/toggle")
async def toggle_source_route(source_id: int, user: str = Depends(get_current_user)):
    from ..sources import toggle_source
    toggle_source(source_id)
    return RedirectResponse(url="/sources", status_code=303)


# --- Search History ---

@app.get("/history", response_class=HTMLResponse)
async def history_page(
    request: Request,
    source_type: str = None,
    user: str = Depends(get_current_user),
):
    from ..sources import get_search_history, init_source_tables
    init_source_tables()
    history = get_search_history(limit=200, source_type=source_type)
    return templates.TemplateResponse("history.html", {
        "request": request,
        "user": user,
        "history": history,
        "filter_type": source_type,
    })


# --- Listing Search ---

@app.get("/search", response_class=HTMLResponse)
async def search_listings(
    request: Request,
    q: str = "",
    user: str = Depends(get_current_user),
):
    results = []
    if q and len(q) >= 2:
        results = list(
            Listing.select()
            .where(
                (Listing.title.contains(q)) |
                (Listing.address.contains(q)) |
                (Listing.city.contains(q)) |
                (Listing.source_label.contains(q)) |
                (Listing.notes.contains(q))
            )
            .order_by(Listing.discovered_at.desc())
            .limit(50)
        )
        for l in results:
            if l.tier1_details:
                try:
                    l._details_parsed = json.loads(l.tier1_details)
                except Exception:
                    l._details_parsed = {}
            else:
                l._details_parsed = {}

    return templates.TemplateResponse("search.html", {
        "request": request,
        "user": user,
        "query": q,
        "results": results,
    })


def run_dashboard(config: dict = None):
    """Run the dashboard server."""
    import uvicorn

    if config is None:
        config = get_config()

    init_db(config.get("general", {}).get("database_path", "data/deal_scout.db"))

    from ..sources import init_source_tables
    init_source_tables()

    from ..analyst.storage import init_analyst_tables
    init_analyst_tables()

    host = config.get("dashboard", {}).get("host", "0.0.0.0")
    port = config.get("dashboard", {}).get("port", 8050)

    logger.info(f"Starting dashboard at http://{host}:{port}")
    uvicorn.run(app, host=host, port=port, log_level="info")


# --- Analyst Routes ---

@app.get("/analyst", response_class=HTMLResponse)
async def analyst_list(request: Request, user: str = Depends(get_current_user)):
    """List all saved analyses."""
    from ..analyst.storage import init_analyst_tables, get_analyses
    init_analyst_tables()
    analyses = get_analyses(limit=50)
    return templates.TemplateResponse("analyst_list.html", {
        "request": request, "user": user, "analyses": analyses,
    })


@app.get("/analyst/new", response_class=HTMLResponse)
async def analyst_new(
    request: Request,
    listing_id: int = None,
    user: str = Depends(get_current_user),
):
    """New analysis form — optionally pre-filled from a listing."""
    prefill = {}
    if listing_id:
        try:
            listing = Listing.get_by_id(listing_id)
            prefill = {
                "project_name": listing.title[:200] if listing.title else "",
                "address": listing.address or "",
                "purchase_price": listing.asking_price or 0,
                "num_units": listing.num_units or 5,
                "cap_rate": listing.listed_cap_rate or 0,
                "listing_id": listing_id,
            }
        except Listing.DoesNotExist:
            pass

    return templates.TemplateResponse("analyst_form.html", {
        "request": request, "user": user, "prefill": prefill,
    })


@app.post("/analyst/run")
async def analyst_run(request: Request, user: str = Depends(get_current_user)):
    """Run a deal analysis from form data."""
    from ..analyst.models import DealInput, UnitInput, FinancingInput, ExpenseInput
    from ..analyst.engine import run_full_analysis
    from ..analyst.storage import init_analyst_tables, save_analysis

    init_analyst_tables()
    form = await request.form()

    # Safe parser for form values — handles empty strings
    def _f(key, default=0.0):
        val = form.get(key, "")
        if val == "" or val is None:
            return float(default)
        try:
            return float(val)
        except (ValueError, TypeError):
            return float(default)

    def _i(key, default=0):
        val = form.get(key, "")
        if val == "" or val is None:
            return int(default)
        try:
            return int(float(val))
        except (ValueError, TypeError):
            return int(default)

    # Parse form data into DealInput — no template data, all from user input
    deal = DealInput(
        project_name=form.get("project_name", ""),
        address=form.get("address", ""),
        purchase_price=_f("purchase_price"),
        land_value=_f("land_value"),
        renovation_cost=_f("renovation_cost"),
        acquisition_fee_percent=_f("acquisition_fee_pct", 3.0),
        closing_costs=_f("closing_costs"),
        listing_id=_i("listing_id") or None,
        rent_growth_percent=_f("rent_growth", 3.0),
        expense_growth_percent=_f("expense_growth", 2.0),
        vacancy_percent=_f("vacancy", 5.0),
        other_income_monthly=_f("other_income"),
        exit_month=_i("exit_month", 120),
        terminal_cap_rate=_f("exit_cap_rate", 5.0),
        selling_cost_percent=_f("selling_cost_pct", 5.0),
        gp_equity_percent=_f("gp_equity_pct", 20.0),
        lp_equity_percent=_f("lp_equity_pct", 80.0),
        financing=FinancingInput(
            ltv_percent=_f("ltv_pct", 60.0),
            interest_rate=_f("interest_rate", 4.25),
            amortization_months=_i("amort_months", 300),
            mortgage_insurance=_f("mortgage_insurance"),
        ),
    )

    # Store growth assumptions for exit strategy engine
    deal._appreciation_pct = _f("appreciation_pct", 5.0)
    deal._refi_ltv = _f("refi_ltv", 75)
    deal._refi_rate = _f("refi_rate", 4.50)
    deal._refi_amort_years = _i("refi_amort_years", 25)
    deal._refi_costs_pct = _f("refi_costs_pct", 1.5)

    # Parse units — all rents from user input, no defaults
    num_units = _i("num_units", 0)
    deal.units = []
    for i in range(num_units):
        rent = _f(f"unit_rent_{i}")
        deal.units.append(UnitInput(
            name=f"Unit {i+1}", count=1,
            current_rent=rent, post_reno_rent=rent,
        ))

    # Parse expenses — all amounts from user input, no defaults
    expense_names = ["Insurance", "Municipal Taxes", "School Taxes", "Snow Removal",
                     "Lawn/Landscaping", "Utility", "Maintenance", "Management", "Concierge"]
    deal.expenses = []
    for i, name in enumerate(expense_names):
        amt = _f(f"expense_{i}")
        deal.expenses.append(ExpenseInput(name, amt))

    # Run analysis with sensitivity
    output = run_full_analysis(deal)

    # Save to database
    record = save_analysis(deal, output, user=user)

    return RedirectResponse(url=f"/analyst/{record.id}", status_code=303)


@app.get("/analyst/{analysis_id}", response_class=HTMLResponse)
async def analyst_view(
    request: Request, analysis_id: int,
    user: str = Depends(get_current_user),
):
    """View a completed analysis."""
    from ..analyst.storage import init_analyst_tables, get_analysis
    init_analyst_tables()

    record = get_analysis(analysis_id)
    if not record:
        raise HTTPException(status_code=404, detail="Analysis not found")

    output_data = json.loads(record.output_json) if record.output_json else {}
    input_data = json.loads(record.input_json) if record.input_json else {}

    return templates.TemplateResponse("analyst_view.html", {
        "request": request, "user": user, "record": record,
        "output": output_data, "input": input_data,
    })


@app.get("/analyst/{analysis_id}/pdf")
async def analyst_pdf(analysis_id: int, user: str = Depends(get_current_user)):
    """Generate and download a BBG-branded PDF report."""
    from ..analyst.storage import init_analyst_tables, get_analysis
    from ..analyst.pdf_report import generate_pdf_report
    from fastapi.responses import FileResponse

    init_analyst_tables()
    record = get_analysis(analysis_id)
    if not record:
        raise HTTPException(status_code=404, detail="Analysis not found")

    path = generate_pdf_report(record)
    if not path:
        raise HTTPException(status_code=500, detail="Report generation failed")

    # Determine media type based on output format
    if path.endswith(".pdf"):
        return FileResponse(path, media_type="application/pdf",
                           filename=f"BBG_Analysis_{record.project_name or analysis_id}.pdf")
    else:
        return FileResponse(path, media_type="text/html",
                           filename=f"BBG_Analysis_{record.project_name or analysis_id}.html")
