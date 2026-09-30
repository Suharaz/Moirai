"""Positions, stops, order lifecycle, fills and exchange reconciliation."""

from __future__ import annotations

import streamlit as st

from hdt.console import components as ui
from hdt.console.readmodels import Available
from hdt.console.views._shared import heading, panel

heading("Positions", "Exposure, protective orders and actual exchange state.")
account = (
    st.segmented_control("Account", ["paper", "testnet", "live"], default="paper", key="positions-account")
    or "paper"
)
models = ui.read_models()
panel("Open positions and protective stops", models.open_positions(account))
left, right = st.columns(2)
with left:
    panel("Exchange orders", models.open_orders(account))
    panel("Recent fills", models.recent_fills(account))
with right:
    panel("Conditional stop and take-profit orders", models.algo_orders(account))
    panel("Hedge book", models.hedge_book(account))
panel("Last reconciliation", models.reconcile_status(account))
page = st.number_input("Closed trades page", min_value=1, value=1)
trades = models.closed_trades(account, limit=25, offset=(page - 1) * 25)
if isinstance(trades, Available):
    st.caption(f"Total closed trades: {trades.value[1]}")
    panel("Closed trades", Available(trades.value[0]))
else:
    panel("Closed trades", trades)
