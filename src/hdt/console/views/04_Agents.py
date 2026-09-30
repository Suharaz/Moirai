"""Agent weights, reliability and calibration by prediction target."""

from __future__ import annotations

import streamlit as st

from hdt.console import components as ui
from hdt.console.readmodels import Available
from hdt.console.views._shared import heading, panel

heading("Agents", "Weight changes follow resolved trades, not operator preference.")
target = st.selectbox("Prediction target", ["direction", "stop_first", "tail_risk"], key="agent-target")
models = ui.read_models()
panel("Agent weights and reliability", models.agents(target))
weights = models.weight_series(target)
if isinstance(weights, Available) and weights.value[0]:
    points, changes = weights.value
    st.line_chart(
        [{"day": p.day, "agent": p.agent, "weight": p.w} for p in points], x="day", y="weight", color="agent"
    )
    panel("Version boundaries", Available(changes))
else:
    panel("Weight history", weights)
agent_names = models.calibration_agents(target)
if isinstance(agent_names, Available) and agent_names.value:
    agent = st.selectbox("Calibration for agent", agent_names.value)
    panel("Calibration and scoring bins", models.calibration(target, agent))
else:
    panel("Calibration agents", agent_names)
