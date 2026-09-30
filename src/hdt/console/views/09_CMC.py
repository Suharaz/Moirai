"""CMC route cadence, enabled routes and quota projection before saving."""

from __future__ import annotations

import json

import streamlit as st

from hdt.console import auth
from hdt.console.api import ApiError, describe_error
from hdt.console.views._shared import config_editor, version_history
from hdt.settings.schemas import Section

client = auth.get_client()
try:
    current = client.get_config(Section.CMC.value)
except ApiError as exc:
    st.error(describe_error(exc))
else:
    if current is not None:
        st.subheader("Monthly credit projection")
        st.caption("Change the candidate JSON to estimate monthly usage before saving the route schedule.")
        source = st.text_area(
            "Candidate route JSON",
            value=json.dumps(current.payload, indent=2),
            height=260,
            key=f"projection-{current.id}",
        )
        if st.button("Calculate projection"):
            try:
                candidate = json.loads(source)
                if not isinstance(candidate, dict):
                    raise ValueError("routes must be a JSON object")
                estimate = client.cmc_projection(candidate)
            except (ValueError, ApiError) as exc:
                st.error(describe_error(exc) if isinstance(exc, ApiError) else str(exc))
            else:
                st.metric(
                    "Monthly projected credits",
                    f"{estimate.monthly_projection:,.0f}",
                    f"{estimate.fraction:.0%} of {estimate.quota:,.0f} quota",
                )
                st.dataframe(estimate.per_route, width="stretch", hide_index=True)
                if estimate.blocked:
                    st.error("Projection is over the hard quota. This configuration cannot be saved.")
config_editor(
    Section.CMC, description="Route schedules and universe size are versioned; over-quota plans fail closed."
)
version_history(Section.CMC)
