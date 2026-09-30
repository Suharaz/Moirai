"""Versioned execution universe, banned symbols and slippage limits."""

from hdt.console.views._shared import config_editor, version_history
from hdt.settings.schemas import Section

config_editor(
    Section.BINANCE, description="Universe selection, exclusions and order-entry limits for Binance."
)
version_history(Section.BINANCE)
