"""Audit trail filters and immutable configuration history."""

from __future__ import annotations

import streamlit as st

from hdt.console import auth
from hdt.console.api import ApiError, describe_error
from hdt.console.views._shared import heading, version_history
from hdt.settings.schemas import Section

heading("Audit & versions", "Filter privileged actions and inspect each immutable config snapshot.")
client = auth.get_client()
a, b, c = st.columns(3)
section = a.selectbox("Section filter", ["all", *[s.value for s in Section], "controls", "keys", "auth"])
user = b.text_input("Username filter")
action = c.text_input("Action filter")
try:
    events = client.audit(
        section=None if section == "all" else section,
        user=user.strip() or None,
        action=action.strip() or None,
    )
except ApiError as exc:
    st.error(describe_error(exc))
else:
    st.dataframe([row.model_dump(mode="json") for row in events], width="stretch", hide_index=True)
selected = st.selectbox("Inspect version history", list(Section), format_func=lambda item: item.value.title())
version_history(selected)
