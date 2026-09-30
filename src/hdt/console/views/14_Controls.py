"""Audited pause, resume, kill and flatten command dispatch."""

from __future__ import annotations

from typing import cast

import streamlit as st

from hdt.console import auth
from hdt.console import components as ui
from hdt.console.api import Account, ApiError, ControlAction, describe_error
from hdt.console.views._shared import heading

heading(
    "Controls",
    "Commands are queued for execution; inspect exchange and reconcile state to verify application.",
)
client = auth.get_client()
account = st.selectbox("Account", ["paper", "testnet", "live"])
action = st.selectbox("Action", ["pause", "resume", "kill", "flatten"])
if action in {"kill", "flatten"}:
    st.warning(
        "Flatten submits an emergency close. Kill prevents new positions; confirm exchange state separately."
    )
reason = st.text_input("Operational reason")
word = action.upper() if action in {"kill", "flatten"} else ""
confirmed = ui.typed_confirmation(word, key=f"control-{action}") if word else True
totp = ui.totp_input("Fresh authenticator code", key=f"control-totp-{action}") if action != "pause" else ""
if st.button(
    "Queue command",
    disabled=not reason.strip() or not confirmed or (action != "pause" and not ui.is_totp(totp)),
    type="primary",
):
    try:
        if action != "pause":
            command_id = auth.with_step_up(
                client,
                totp,
                lambda: str(
                    client.control(cast(Account, account), cast(ControlAction, action), reason.strip(), word)[
                        "command_id"
                    ]
                ),
            )
        else:
            command_id = str(
                client.control(cast(Account, account), cast(ControlAction, action), reason.strip())[
                    "command_id"
                ]
            )
    except ApiError as exc:
        st.error(describe_error(exc))
    else:
        ui.notify(f"Command {command_id} queued. Verify execution in positions and the command log.")
        st.rerun()
st.subheader("Recent commands")
try:
    log = client.controls_log()
except ApiError as exc:
    st.error(describe_error(exc))
else:
    st.dataframe([row.model_dump(mode="json") for row in log], width="stretch", hide_index=True)
