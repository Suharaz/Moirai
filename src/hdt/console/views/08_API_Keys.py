"""Write-only API key rotation: plaintext exists in the browser component only."""

from __future__ import annotations

import streamlit as st

from hdt.console import auth
from hdt.console import components as ui
from hdt.console.api import ApiError, describe_error
from hdt.console.sealbox import SECRET_SPECS, sealbox
from hdt.console.theme import TOKENS
from hdt.console.views._shared import heading

heading("API keys", "Keys are sealed in your browser; only the owning service can decrypt them.")
client = auth.get_client()
try:
    rows = client.secrets()
    public_keys = client.public_keys()
except ApiError as exc:
    st.error(describe_error(exc))
    st.stop()
st.dataframe(
    [
        {
            "scope": row.scope,
            "name": row.name,
            "version": row.version,
            "last4": row.last4,
            "status": row.status,
            "checked": row.checked_at,
            "reason": row.reason,
        }
        for row in rows
    ],
    width="stretch",
    hide_index=True,
)
spec = st.selectbox(
    "Credential to add or rotate", SECRET_SPECS, format_func=lambda item: f"{item.title} ({item.scope})"
)
record = next((row for row in rows if row.scope == spec.scope and row.name == spec.name), None)
if record and record.version is not None:
    st.caption(f"Latest version {record.version}: {record.status} | ending {record.last4 or '-'}")
else:
    st.caption("No version saved yet.")
st.info(spec.owner_check)
key = public_keys.get(spec.scope)
if key is None:
    st.error("No public key registered for this service. The key cannot be saved.")
    st.stop()
nonce_key = f"secret-nonce-{spec.scope}-{spec.name}"
sealed = sealbox(
    spec,
    key,
    key=f"secret-form-{spec.scope}-{spec.name}",
    tokens=TOKENS[ui.current_theme()],
    nonce=st.session_state.get(nonce_key, 0),
)
if sealed is not None:
    st.caption(f"Sealed credential ending ...{sealed.last4}. Plaintext has been cleared from the form.")
    payload = sealed.model_dump()
    totp = ui.totp_input("Fresh authenticator code", key=f"secret-totp-{spec.scope}-{spec.name}")
    if st.button("Save sealed credential", disabled=not ui.is_totp(totp), type="primary"):
        try:
            result = auth.with_step_up(
                client,
                totp,
                lambda: str(
                    client.write_secret(
                        spec.scope,
                        spec.name,
                        sealed_blob=payload["sealed_blob"],
                        last4=payload["last4"],
                        fingerprint=payload["fingerprint"],
                    ).version
                ),
            )
        except ApiError as exc:
            st.error(describe_error(exc))
        else:
            st.session_state[nonce_key] = st.session_state.get(nonce_key, 0) + 1
            ui.notify(f"Credential version {result} submitted to the owner for validation")
            st.rerun()
if (
    record
    and record.status == "active"
    and st.button("Retest active credential", key=f"retest-{spec.scope}-{spec.name}")
):
    try:
        response = client.retest_secret(spec.scope, spec.name)
    except ApiError as exc:
        st.error(describe_error(exc))
    else:
        st.info(f"Owner validation queued: {response.get('request_id', 'pending')}")
