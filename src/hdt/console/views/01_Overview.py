"""Account overview with explicit source availability and gate status."""

from __future__ import annotations

import streamlit as st

from hdt.console import components as ui
from hdt.console.readmodels import Available
from hdt.console.views._shared import heading, panel

heading("Overview", "Account performance, open exposure, system gates and alerts.")
account = (
    st.segmented_control("Account", ["paper", "testnet", "live"], default="paper", key="overview-account")
    or "paper"
)
models = ui.read_models()
summary = models.account_summary(account)
if isinstance(summary, Available) and summary.value is not None:
    info = summary.value
    ui.render(
        ui.kpis(
            [
                ui.Kpi("Equity", ui.fmt_usd(info.equity)),
                ui.Kpi(
                    "Today PnL",
                    ui.fmt_signed(info.today_pnl),
                    ui.fmt_pct(info.today_pnl_fraction, signed=True),
                ),
                ui.Kpi(
                    "30d change",
                    ui.fmt_signed(info.change_30d),
                    ui.fmt_pct(info.change_30d_fraction, signed=True),
                ),
                ui.Kpi("Drawdown", ui.fmt_pct(info.drawdown, signed=True)),
            ]
        )
    )
else:
    panel("Account balance", summary)
period = st.segmented_control(
    "Equity interval", [7, 30, 0], default=30, format_func=lambda n: "All" if n == 0 else f"{n} days"
)
equity = models.equity_series(account, period or None)
if isinstance(equity, Available) and equity.value:
    st.line_chart(
        [{"time": point.ts, "equity": point.equity} for point in equity.value], x="time", y="equity"
    )
else:
    panel("Equity history", equity)
left, right = st.columns(2)
with left:
    panel("Open positions", models.open_positions(account))
    panel("Gate progress", models.gates())
with right:
    panel("Recent alerts", models.open_alerts())
    panel("Trade statistics (30 days)", models.trade_stats(account))
