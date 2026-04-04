"""PDF report generator for deal analyses.

Generates a BBG-branded PDF report from an AnalysisOutput.
Uses HTML → PDF conversion via weasyprint (if available) or
falls back to a simpler text-based PDF via reportlab/fpdf.
"""

import json
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)


def generate_pdf_report(record, output_path: str = None) -> Optional[str]:
    """
    Generate a BBG-branded PDF report from a saved analysis record.

    Args:
        record: DealAnalysis database record
        output_path: Where to save the PDF (default: data/reports/)

    Returns:
        Path to the generated PDF, or None if generation failed.
    """
    if output_path is None:
        reports_dir = Path("data/reports")
        reports_dir.mkdir(parents=True, exist_ok=True)
        safe_name = "".join(c if c.isalnum() or c in " -_" else "" for c in (record.project_name or "analysis"))[:50]
        output_path = str(reports_dir / f"{safe_name}_{record.id}_{datetime.now().strftime('%Y%m%d')}.pdf")

    output_data = json.loads(record.output_json) if record.output_json else {}
    input_data = json.loads(record.input_json) if record.input_json else {}

    html = _build_report_html(record, input_data, output_data)

    # Try weasyprint first (best quality)
    try:
        from weasyprint import HTML
        HTML(string=html).write_pdf(output_path)
        logger.info(f"PDF generated (weasyprint): {output_path}")
        return output_path
    except ImportError:
        pass

    # Fallback: save as HTML (user can print to PDF from browser)
    html_path = output_path.replace(".pdf", ".html")
    Path(html_path).write_text(html, encoding="utf-8")
    logger.info(f"HTML report generated (install weasyprint for PDF): {html_path}")
    return html_path


def _build_report_html(record, input_data: dict, output_data: dict) -> str:
    """Build the full BBG-branded report as HTML."""
    ret = output_data.get("returns", {})
    wf = output_data.get("waterfall", {})
    annuals = output_data.get("annual_summaries", [])
    sensitivity = output_data.get("sensitivity", [])

    now = datetime.now().strftime("%B %d, %Y")

    # Annual rows
    annual_rows = ""
    for s in annuals:
        annual_rows += f"""
        <tr>
            <td>Year {s.get('year','')}</td>
            <td class="r">${s.get('gross_rent',0):,.0f}</td>
            <td class="r">${s.get('noi',0):,.0f}</td>
            <td class="r">${s.get('debt_service',0):,.0f}</td>
            <td class="r">${s.get('cash_flow_after_debt',0):,.0f}</td>
            <td class="c">{s.get('dscr',0):.2f}x</td>
        </tr>"""

    # Scenario rows
    scenario_rows = ""
    scenarios = [s for s in sensitivity if s.get("variable_changed") == "scenario"]
    for s in scenarios:
        scenario_rows += f"""
        <tr>
            <td style="font-weight:600;">{s.get('scenario_name','')}</td>
            <td class="c">{s.get('levered_irr',0):.1f}%</td>
            <td class="c">{s.get('equity_multiple',0):.2f}x</td>
            <td class="c">{s.get('cash_on_cash',0):.1f}%</td>
            <td class="c">{s.get('dscr',0):.2f}x</td>
            <td class="c">{s.get('gp_irr',0):.1f}%</td>
            <td class="c">{s.get('lp_irr',0):.1f}%</td>
        </tr>"""

    # Waterfall tier rows
    tier_rows = ""
    for t in wf.get("tier_distributions", []):
        tier_rows += f"""
        <tr>
            <td style="font-weight:600;">{t.get('name','')}</td>
            <td class="c">{t.get('gp_share_pct',0):.0f}%</td>
            <td class="c">{t.get('lp_share_pct',0):.0f}%</td>
            <td class="r">${t.get('gp_total',0):,.0f}</td>
            <td class="r">${t.get('lp_total',0):,.0f}</td>
        </tr>"""

    return f"""<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<style>
    @page {{ margin: 0.75in; size: letter; }}
    body {{ font-family: Calibri, -apple-system, sans-serif; color: #1A1A2E; font-size: 11pt; line-height: 1.4; }}
    .header {{ border-bottom: 3px solid #005AAA; padding-bottom: 12px; margin-bottom: 20px; }}
    .header h1 {{ color: #005AAA; font-size: 22pt; margin: 0; }}
    .header .sub {{ color: #636466; font-size: 11pt; }}
    .header .date {{ color: #808080; font-size: 9pt; float: right; }}
    h2 {{ color: #005AAA; font-size: 14pt; border-bottom: 2px solid #005AAA; padding-bottom: 4px; margin: 20px 0 10px; }}
    h3 {{ color: #002060; font-size: 12pt; margin: 14px 0 8px; }}
    table {{ width: 100%; border-collapse: collapse; margin-bottom: 14px; font-size: 10pt; }}
    th {{ background: #005AAA; color: white; padding: 6px 8px; text-align: left; font-size: 9pt; text-transform: uppercase; }}
    td {{ padding: 5px 8px; border-bottom: 1px solid #e0e0e0; }}
    .r {{ text-align: right; }}
    .c {{ text-align: center; }}
    .kpi-grid {{ display: flex; gap: 12px; margin-bottom: 16px; flex-wrap: wrap; }}
    .kpi {{ background: #F5F7FA; padding: 10px 14px; border-radius: 4px; text-align: center; min-width: 120px; border-left: 3px solid #005AAA; }}
    .kpi .val {{ font-size: 18pt; font-weight: 700; color: #005AAA; }}
    .kpi .lbl {{ font-size: 8pt; color: #808080; text-transform: uppercase; }}
    .two-col {{ display: flex; gap: 20px; }}
    .two-col > div {{ flex: 1; }}
    .wf-th {{ background: #002060; }}
    .sens-th {{ background: #0070C0; }}
    .footer {{ margin-top: 30px; border-top: 1px solid #ddd; padding-top: 8px; font-size: 8pt; color: #808080; text-align: center; }}
    .disclaimer {{ font-size: 8pt; color: #808080; margin-top: 20px; padding: 10px; background: #f8f8f8; }}
    .page-break {{ page-break-before: always; }}
</style>
</head>
<body>

<div class="header">
    <span class="date">{now}</span>
    <h1>{record.project_name or 'Deal Analysis'}</h1>
    <div class="sub">{record.address or ''} — Blue Bear Group Corp.</div>
</div>

<div class="kpi-grid">
    <div class="kpi"><div class="val">{ret.get('levered_irr',0):.1f}%</div><div class="lbl">Levered IRR</div></div>
    <div class="kpi"><div class="val">{ret.get('levered_emx',0):.2f}x</div><div class="lbl">Equity Multiple</div></div>
    <div class="kpi"><div class="val">{ret.get('avg_cash_on_cash',0):.1f}%</div><div class="lbl">Avg Cash on Cash</div></div>
    <div class="kpi"><div class="val">{ret.get('avg_dscr',0):.2f}x</div><div class="lbl">Avg DSCR</div></div>
    <div class="kpi"><div class="val">${output_data.get('sale_price',0):,.0f}</div><div class="lbl">Exit Sale Price</div></div>
</div>

<h2>Investment Summary</h2>
<div class="two-col">
<div>
<h3>Sources of Funds</h3>
<table>
    <tr><td>Debt</td><td class="r">${output_data.get('debt_amount',0):,.0f}</td></tr>
    <tr><td>Equity</td><td class="r">${output_data.get('equity_amount',0):,.0f}</td></tr>
    <tr style="font-weight:700;border-top:2px solid #005AAA;"><td>Total</td><td class="r">${output_data.get('total_sources',0):,.0f}</td></tr>
</table>
</div>
<div>
<h3>Return Metrics</h3>
<table>
    <tr><td>Unlevered IRR</td><td class="r">{ret.get('unlevered_irr',0):.2f}%</td></tr>
    <tr><td>Levered IRR</td><td class="r" style="font-weight:700;">{ret.get('levered_irr',0):.2f}%</td></tr>
    <tr><td>Entry Cap Rate</td><td class="r">{ret.get('cap_rate_entry',0):.2f}%</td></tr>
    <tr><td>Break-Even Ratio</td><td class="r">{ret.get('break_even_ratio',0):.1f}%</td></tr>
    <tr><td>Min DSCR</td><td class="r">{ret.get('min_dscr',0):.2f}x</td></tr>
    <tr><td>Total Profit</td><td class="r">${ret.get('total_profit',0):,.0f}</td></tr>
</table>
</div>
</div>

<h2>Annual Cash Flow Projection</h2>
<table>
<thead><tr><th>Year</th><th class="r">Gross Rent</th><th class="r">NOI</th><th class="r">Debt Service</th><th class="r">Cash Flow</th><th class="c">DSCR</th></tr></thead>
<tbody>{annual_rows}</tbody>
</table>

<div class="page-break"></div>

<h2>Waterfall Distribution</h2>
<div class="two-col">
<div>
<h3>GP (Sponsor)</h3>
<table>
    <tr><td>Equity Contribution</td><td class="r">${wf.get('gp_contribution',0):,.0f}</td></tr>
    <tr><td>Total Distributions</td><td class="r">${wf.get('gp_distributions',0):,.0f}</td></tr>
    <tr><td>Net Profit</td><td class="r" style="font-weight:700;">${wf.get('gp_profit',0):,.0f}</td></tr>
    <tr><td>IRR</td><td class="r" style="font-weight:700;">{wf.get('gp_irr',0):.2f}%</td></tr>
    <tr><td>Equity Multiple</td><td class="r">{wf.get('gp_emx',0):.2f}x</td></tr>
</table>
</div>
<div>
<h3>LP (Investor)</h3>
<table>
    <tr><td>Equity Contribution</td><td class="r">${wf.get('lp_contribution',0):,.0f}</td></tr>
    <tr><td>Total Distributions</td><td class="r">${wf.get('lp_distributions',0):,.0f}</td></tr>
    <tr><td>Net Profit</td><td class="r" style="font-weight:700;">${wf.get('lp_profit',0):,.0f}</td></tr>
    <tr><td>IRR</td><td class="r" style="font-weight:700;">{wf.get('lp_irr',0):.2f}%</td></tr>
    <tr><td>Equity Multiple</td><td class="r">{wf.get('lp_emx',0):.2f}x</td></tr>
</table>
</div>
</div>

{f'''<h3>Tier Breakdown</h3>
<table>
<thead class="wf-th"><tr><th>Tier</th><th class="c">GP Share</th><th class="c">LP Share</th><th class="r">GP Total</th><th class="r">LP Total</th></tr></thead>
<tbody>{tier_rows}</tbody>
</table>''' if tier_rows else ''}

{f'''<h2>Scenario Analysis</h2>
<table>
<thead class="sens-th"><tr><th>Scenario</th><th class="c">Levered IRR</th><th class="c">EMx</th><th class="c">Avg CoC</th><th class="c">DSCR</th><th class="c">GP IRR</th><th class="c">LP IRR</th></tr></thead>
<tbody>{scenario_rows}</tbody>
</table>''' if scenario_rows else ''}

<div class="disclaimer">
    <strong>Disclaimer:</strong> This analysis is prepared for internal use by Blue Bear Group Corporation and its authorized partners.
    All projections are based on assumptions that may not reflect actual market conditions.
    Past performance does not guarantee future results. This document does not constitute investment advice.
    Verify all inputs and consult qualified professionals before making investment decisions.
</div>

<div class="footer">
    Blue Bear Group Corporation — 2967 Dundas St. W. #965, Toronto, ON M6P 1Z2 — info@bluebeargroup.ca<br>
    Analysis #{record.id} — Generated {now}
</div>

</body>
</html>"""
