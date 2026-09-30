"""Small, explicit presentation helpers for monitoring and versioned configuration."""

from __future__ import annotations

import json
from dataclasses import asdict, is_dataclass
from typing import Any

import streamlit as st

from hdt.console import auth
from hdt.console import components as ui
from hdt.console.api import ApiError, describe_error
from hdt.console.api import Section as ApiSection
from hdt.console.readmodels import Available, Unavailable
from hdt.settings.schemas import Section


def heading(title: str, description: str) -> None:
    ui.render(ui.page_head(title, description))


def plain(value: Any) -> Any:
    if is_dataclass(value) and not isinstance(value, type):
        return {key: plain(item) for key, item in asdict(value).items()}
    if isinstance(value, (tuple, list)):
        return [plain(item) for item in value]
    if isinstance(value, dict):
        return {key: plain(item) for key, item in value.items()}
    return value


def panel(title: str, result: Available[Any] | Unavailable) -> Any | None:
    st.subheader(title)
    if isinstance(result, Unavailable):
        ui.render(ui.source_unavailable(title, result.reasons))
        return None
    value = result.value
    if value is None or value == ():
        st.info("No observations recorded yet.")
    elif isinstance(value, (tuple, list)):
        st.dataframe([plain(row) for row in value], width="stretch", hide_index=True)
    elif is_dataclass(value):
        st.json(plain(value))
    else:
        st.write(value)
    return value


def config_editor(section: Section, *, description: str = "") -> None:
    """Edit one immutable section with optimistic parent check and a reason for the audit trail."""
    client = auth.get_client()
    api_section: ApiSection = section.value
    try:
        current = client.get_config(api_section)
        schema = client.config_schema(api_section)
    except ApiError as exc:
        st.error(describe_error(exc))
        return
    heading(section.value.replace("_", " ").title(), description)
    if current is None:
        st.warning("No active version. Save a validated configuration before running this service.")
        return
    st.caption(f"Active version {current.id} | author: {current.author} | {current.created_at}")
    with st.expander("Field schema and hard bounds"):
        st.json(schema)
    with st.form(f"edit-{section}"):
        source = st.text_area(
            "Configuration JSON",
            value=json.dumps(current.payload, indent=2, sort_keys=True),
            height=340,
            key=f"config-{section}-{current.id}",
        )
        reason = st.text_input("Reason for change", key=f"reason-{section}")
        submitted = st.form_submit_button("Validate and save new version", type="primary")
    if submitted:
        try:
            payload = json.loads(source)
            if not isinstance(payload, dict):
                raise ValueError("configuration must be a JSON object")
            if not reason.strip():
                raise ValueError("a reason is required")
            saved = client.save_config(api_section, payload, reason.strip(), current.id)
        except (ValueError, ApiError) as exc:
            st.error(describe_error(exc) if isinstance(exc, ApiError) else str(exc))
        else:
            ui.invalidate_top_bar()
            ui.notify(f"Saved {section.value} version {saved.id}")
            st.rerun()


def version_history(section: Section) -> None:
    client = auth.get_client()
    api_section: ApiSection = section.value
    st.subheader("Immutable versions")
    try:
        history = client.config_versions(api_section)
    except ApiError as exc:
        st.error(describe_error(exc))
        return
    if not history.versions:
        st.info("No versions recorded.")
        return
    st.dataframe(
        [
            {
                "version": v.id,
                "active": v.id == history.active_version_id,
                "created": v.created_at,
                "author": v.author,
                "reason": v.reason,
                "parent": v.parent_id,
            }
            for v in history.versions
        ],
        width="stretch",
        hide_index=True,
    )
    candidates = [v for v in history.versions if v.id != history.active_version_id]
    if not candidates:
        return
    version_id = st.selectbox("Version to restore", [v.id for v in candidates], key=f"rollback-{section}")
    selected = next(v for v in candidates if v.id == version_id)
    with st.expander("Inspect selected payload"):
        st.json(selected.payload)
    reason = st.text_input("Rollback reason", key=f"rollback-reason-{section}")
    if st.button(
        "Create a new version from selected payload",
        disabled=not reason.strip(),
        key=f"do-rollback-{section}",
    ):
        try:
            restored = client.rollback(api_section, version_id, reason.strip())
        except ApiError as exc:
            st.error(describe_error(exc))
        else:
            ui.invalidate_top_bar()
            ui.notify(f"Restored as new version {restored.id}")
            st.rerun()
