"""CMC credits, route health, websocket gaps and lake retention."""

from __future__ import annotations

from datetime import timedelta

import streamlit as st

from hdt.console import components as ui
from hdt.console.readmodels import Available, route_counts
from hdt.console.views._shared import heading, panel
from hdt.core.clock import utcnow

heading("Data & credits", "Recorder quota, stale data and storage health.")
models = ui.read_models()
credit = models.credit_status()
if isinstance(credit, Available) and credit.value is not None:
    c = credit.value
    ui.render(
        ui.kpis(
            [
                ui.Kpi("Credits used", f"{ui.fmt_int(c.used)} / {ui.fmt_int(c.limit)}"),
                ui.Kpi("Cycle remaining", f"{c.days_left} days"),
                ui.Kpi("Projected usage", ui.fmt_pct(c.projection_fraction)),
            ]
        )
    )
    if c.projection_fraction > 1:
        st.warning("Current pace exceeds the monthly quota. Review enabled routes and cadence.")
else:
    panel("CMC key and quota", credit)
panel("Daily credit use", models.credit_days((utcnow() - timedelta(days=30)).date()))
routes = models.route_health()
panel("REST route health", routes)
if isinstance(routes, Available) and routes.value:
    counts = route_counts(routes.value)
    ui.render(
        ui.kpis(
            [
                ui.Kpi("Routes ok", ui.fmt_int(counts.ok)),
                ui.Kpi("Late or failing", ui.fmt_int(counts.late_or_failing)),
                ui.Kpi("Not on current plan", ui.fmt_int(len(counts.not_on_plan))),
            ]
        )
    )
    if counts.not_on_plan:
        st.info(
            "Not on the current CMC plan (left empty, not counted as late or failing; they record again "
            "by themselves after a plan upgrade): " + ", ".join(counts.not_on_plan)
        )
panel("WebSocket health", models.ws_health())
panel("Lake and retention", models.lake_status())
