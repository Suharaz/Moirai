"""Model catalogs (OpenRouter, DeepSeek), per-role config and owner-executed model test results."""

from __future__ import annotations

import json
from typing import Any, get_args

import streamlit as st
from pydantic import ValidationError

from hdt.console import auth
from hdt.console.api import ApiError, describe_error
from hdt.console.views._shared import config_editor, version_history
from hdt.core.config import LlmRole
from hdt.settings.schemas import (
    DEEPSEEK_MODELS,
    Gateway,
    ModelsSection,
    RoleModelConfig,
    Section,
    Thinking,
    deepseek_profile,
)

GATEWAY_TITLES: dict[Gateway, str] = {"openrouter": "OpenRouter", "deepseek": "DeepSeek"}
DEEPSEEK_TESTS_KEY = "hdt-deepseek-tests"

client = auth.get_client()
st.subheader("Model catalog")
gateway: Gateway = st.radio(
    "Gateway", get_args(Gateway), format_func=lambda g: GATEWAY_TITLES[g], horizontal=True
)
try:
    catalog = client.model_catalog(gateway)
except ApiError as exc:
    st.error(describe_error(exc))
else:
    if gateway == "deepseek":
        st.caption(
            f"Prices read {catalog.fetched_at} from config/deepseek.yaml; USD per token at the peak rate "
            "(cache-miss input). Use the model name with gateway 'deepseek' in a role config."
        )
    else:
        st.caption(f"Fetched {catalog.fetched_at}; pinned slugs only. Price is USD per token in the catalog.")
    search = st.text_input("Filter model slug or developer")
    matches = [m for m in catalog.models if search.lower() in (m.slug + " " + m.developer).lower()]
    st.dataframe(
        [
            {
                "slug": m.slug,
                "developer": m.developer,
                "context": m.context_length,
                "prompt": m.pricing.prompt,
                "completion": m.pricing.completion,
            }
            for m in matches[:250]
        ],
        width="stretch",
        hide_index=True,
    )
config_editor(
    Section.MODELS,
    description="Each council role has its own gateway, pinned model and request options.",
)


def _deepseek_switch(active_id: int | None, roles: dict[str, Any]) -> None:
    """Draft every role on DeepSeek, test each role, then save the draft as a new version."""
    st.subheader("Switch every role to DeepSeek")
    st.caption(
        "Builds a models version with every role on the deepseek gateway. Configured roles keep their "
        "temperature, max_tokens and timeout_s; the values below apply to roles not configured yet. The "
        "DeepSeek key must be sealed on the API keys page first, and each role must pass its model test "
        "before the version can be saved."
    )
    current: dict[str, RoleModelConfig] = {}
    for role, payload in roles.items():
        try:
            current[role] = RoleModelConfig.model_validate(payload)
        except ValidationError:
            continue
    left, right = st.columns(2)
    model = left.selectbox("DeepSeek model", DEEPSEEK_MODELS)
    thinking: Thinking = right.selectbox(
        "Thinking", get_args(Thinking), help="Enabled thinking needs a larger max_tokens and timeout_s."
    )
    a, b, c = st.columns(3)
    temperature = a.number_input("temperature (new roles)", 0.0, 2.0, 0.2, 0.1)
    max_tokens = int(b.number_input("max_tokens (new roles)", 1, 393216, 2000, 100))
    timeout_s = c.number_input("timeout_s (new roles)", 1.0, 600.0, 60.0, 5.0)
    try:
        draft: ModelsSection = deepseek_profile(
            current,
            model=model,
            thinking=thinking,
            temperature=temperature,
            max_tokens=max_tokens,
            timeout_s=timeout_s,
        )
    except ValidationError as exc:
        st.error(str(exc))
        return
    payload = draft.model_dump(mode="json")
    with st.expander("Draft models version"):
        st.json(payload)
    if st.button("Request model tests for every role", key="deepseek-tests"):
        started: dict[str, str] = {}
        try:
            for role, config in payload["roles"].items():
                started[role] = client.start_model_test(role, config)
        except ApiError as exc:
            st.error(describe_error(exc))
        st.session_state[DEEPSEEK_TESTS_KEY] = started
    tests: dict[str, str] = st.session_state.get(DEEPSEEK_TESTS_KEY, {})
    if tests:
        rows: list[dict[str, Any]] = []
        for role, request_id in sorted(tests.items()):
            try:
                result = client.model_test_result(request_id)
            except ApiError as exc:
                rows.append({"role": role, "status": describe_error(exc)})
                continue
            rows.append(
                {
                    "role": role,
                    "status": result.status,
                    "schema_valid": result.schema_valid,
                    "cost_usd": result.cost_usd,
                    "error": result.error,
                }
            )
        st.dataframe(rows, width="stretch", hide_index=True)
        if st.button("Refresh model tests", key="deepseek-tests-refresh"):
            st.rerun()
    reason = st.text_input("Reason for the switch", key="deepseek-reason")
    if st.button("Save the draft as a new models version", type="primary", key="deepseek-save"):
        if not reason.strip():
            st.error("a reason is required")
            return
        try:
            saved = client.save_config(Section.MODELS.value, payload, reason.strip(), active_id)
        except ApiError as exc:
            st.error(describe_error(exc))
        else:
            st.session_state.pop(DEEPSEEK_TESTS_KEY, None)
            st.success(f"Saved models version {saved.id}")
            st.rerun()


try:
    active = client.get_config(Section.MODELS.value)
except ApiError as exc:
    st.error(describe_error(exc))
else:
    roles = active.payload.get("roles", {}) if active else {}
    _deepseek_switch(active.id if active else None, roles)
    st.subheader("Test a prospective role assignment")
    st.caption("Test the proposed model and provider options before saving a new configuration version.")
    role = st.selectbox("Role", get_args(LlmRole))
    sample = roles.get(role, {})
    draft = st.text_area(
        "Proposed role config (JSON)",
        value=json.dumps(sample, indent=2),
        height=250,
        key=f"model-draft-{role}",
    )
    if st.button("Request real model test", key="model-test"):
        try:
            candidate = RoleModelConfig.model_validate(json.loads(draft)).model_dump(mode="json")
            st.session_state["hdt-model-test-id"] = client.start_model_test(role, candidate)
        except (ValueError, ValidationError) as exc:
            st.error(str(exc))
        except ApiError as exc:
            st.error(describe_error(exc))
    request_id = st.session_state.get("hdt-model-test-id")
    if request_id:
        try:
            st.json(client.model_test_result(request_id).model_dump(mode="json"))
        except ApiError as exc:
            st.error(describe_error(exc))
        if st.button("Refresh test result"):
            st.rerun()
version_history(Section.MODELS)
