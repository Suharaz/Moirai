"""Risk config with immutable hard limits enforced by the API and risk runtime."""

import streamlit as st

from hdt.console.views._shared import config_editor, version_history
from hdt.settings.schemas import Section

st.warning(
    "Hard ceilings: leverage 3x, per-trade risk 0.5%, daily kill 2%, four positions, "
    "same-direction exposure 1.5%."
)
config_editor(Section.RISK, description="Fractions are decimals: 0.005 means 0.5% of equity.")
version_history(Section.RISK)
