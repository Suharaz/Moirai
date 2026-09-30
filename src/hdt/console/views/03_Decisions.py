"""Inspect council decisions with pinned config, claims and risk verdict."""

from __future__ import annotations

import streamlit as st

from hdt.console import components as ui
from hdt.console.readmodels import Available, DecisionFilters
from hdt.console.views._shared import heading, panel, plain

heading("Decisions", "Trace each candidate through forecasts, debate, manager and risk.")
account = (
    st.segmented_control("Account", ["paper", "testnet", "live"], default="paper", key="decisions-account")
    or "paper"
)
col1, col2, col3 = st.columns(3)
query = col1.text_input("Symbol or event", key="decision-query")
source = col2.text_input("Source", key="decision-source")
outcome = col3.text_input("Outcome", key="decision-outcome")
page = st.number_input("Page", min_value=1, value=1, key="decisions-page")
models = ui.read_models()
result = models.decisions(
    account, DecisionFilters(query=query, source=source, outcome=outcome), limit=25, offset=(page - 1) * 25
)
if isinstance(result, Available):
    st.caption(f"{result.value.total} matching decisions")
    rows = result.value.rows
    panel("Decision log", Available(rows))
    if rows:
        event_id = st.selectbox("Inspect event", [row.event_id for row in rows])
        card = models.decision_card(event_id, account)
        if isinstance(card, Available) and card.value is not None:
            detail = card.value
            st.subheader(f"{detail.symbol} | {detail.outcome}")
            st.write(detail.summary)
            st.caption(f"Event {detail.event_id} | config pins {detail.config_version_ids}")
            for title, value in (
                ("Consensus", detail.consensus),
                ("Manager rule", detail.manager_rule),
                ("Forecasts", detail.forecasts),
                ("Claims", detail.claims),
                ("Timeline", detail.timeline),
                ("Risk verdict", detail.verdict),
                ("Levels", detail.levels),
                ("Fills", detail.fills),
            ):
                with st.expander(title):
                    st.json(plain(value))
            if detail.levels_problem:
                st.warning(detail.levels_problem)
        else:
            panel("Decision detail", card)
else:
    panel("Decision log", result)
