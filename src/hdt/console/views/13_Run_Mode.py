"""Mode changes require explicit approval; live remains fail-closed behind all prerequisites."""

from __future__ import annotations

from typing import cast

import streamlit as st

from hdt.console import auth
from hdt.console import components as ui
from hdt.console.api import ApiError, RunMode, describe_error
from hdt.console.views._shared import heading, version_history
from hdt.settings.ceilings import PAPER_SIZE_MULTIPLIER, SIZE_MULTIPLIER_MAX
from hdt.settings.schemas import Section

heading("Run mode", "Live trading is blocked unless gate G4, key validation and every go-live check pass.")
client = auth.get_client()
try:
    current = client.get_mode()
    checklist = client.golive_checklist()
except ApiError as exc:
    st.error(describe_error(exc))
    st.stop()
st.info(f"Current mode: {current.mode.upper()} | size multiplier: {current.size_multiplier:.2f}")
gate = current.live_gate
st.write(
    {
        "G4 passed": gate.g4_passed,
        "Live key valid": gate.live_key_valid,
        "Checklist complete": gate.checklist_complete,
        "Missing prerequisites": gate.missing,
    }
)
st.subheader(f"Go-live checklist ({checklist.done}/{checklist.total})")
checked = {
    item.id: st.checkbox(
        item.label or item.id,
        value=item.checked,
        help=f"Checked by {item.checked_by} at {item.checked_at}" if item.checked else None,
        key=f"golive-{item.id}-{checklist.version_id}",
    )
    for item in checklist.items
}
check_reason = st.text_input("Checklist change reason")
if st.button("Save checklist version", disabled=not check_reason.strip()):
    try:
        saved = client.save_golive_checklist(checked, check_reason.strip())
    except ApiError as exc:
        st.error(describe_error(exc))
    else:
        ui.notify(f"Saved checklist version {saved.version_id}")
        st.rerun()
st.subheader("Change trading mode")
mode = st.selectbox(
    "Target mode", ["paper", "testnet", "live"], index=["paper", "testnet", "live"].index(current.mode)
)
if mode == "paper":
    st.caption(
        f"Paper always runs at size multiplier {PAPER_SIZE_MULTIPLIER:.2f} so gate G4 measures full size."
    )
    multi = PAPER_SIZE_MULTIPLIER
else:
    multi = st.number_input(
        "Size multiplier",
        min_value=0.01,
        max_value=SIZE_MULTIPLIER_MAX,
        value=min(float(current.size_multiplier), SIZE_MULTIPLIER_MAX),
        step=0.05,
    )
reason = st.text_input("Mode change reason")
if mode == "live":
    st.warning(
        "Live requires a fresh authenticator code and all server-side prerequisites. This action is audited."
    )
    totp = ui.totp_input("Fresh authenticator code for live", key="mode-totp")
else:
    totp = ""
if st.button(
    "Request mode change",
    type="primary",
    disabled=not reason.strip() or (mode == "live" and not ui.is_totp(totp)),
):
    try:
        if mode == "live":
            auth.with_step_up(
                client, totp, lambda: client.set_mode(cast(RunMode, mode), multi, reason.strip()).mode
            )
        else:
            client.set_mode(cast(RunMode, mode), multi, reason.strip())
    except ApiError as exc:
        st.error(describe_error(exc))
    else:
        ui.invalidate_top_bar()
        ui.notify(f"Changed mode to {mode}")
        st.rerun()
version_history(Section.MODE)
version_history(Section.GOLIVE)
