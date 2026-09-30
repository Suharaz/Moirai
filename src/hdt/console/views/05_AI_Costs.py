"""Actual model cost and request volume; no invented zero-usage metrics."""

from __future__ import annotations

import streamlit as st

from hdt.console import components as ui
from hdt.console.readmodels import Available
from hdt.console.views._shared import heading, panel

heading("AI costs", "OpenRouter spend by day, role and pipeline.")
result = ui.read_models().cost_overview()
if isinstance(result, Available):
    cost = result.value
    ui.render(
        ui.kpis(
            [
                ui.Kpi("Today", ui.fmt_usd(cost.today.cost_usd), f"{cost.today.calls} calls"),
                ui.Kpi("Last 30 days", ui.fmt_usd(cost.last_30d.cost_usd), f"{cost.last_30d.calls} calls"),
                ui.Kpi("Council cost per event", ui.fmt_usd(cost.cost_per_event)),
            ]
        )
    )
    if cost.by_day:
        st.bar_chart([{"day": d.day, "USD": d.cost_usd} for d in cost.by_day], x="day", y="USD")
    panel("Spend by pipeline", Available(cost.by_pipeline))
    panel("Spend by model role", Available(cost.by_role))
else:
    panel("AI cost observations", result)
