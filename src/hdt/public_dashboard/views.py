"""Pages of the public dashboard (mockup `visuals/console-mockup.html?app=public`).

Every page renders only what the published snapshot contains; nothing here can reach a trading store.
"""

from __future__ import annotations

import math
from collections.abc import Iterable, Mapping, Sequence
from datetime import UTC, date, datetime, timedelta
from typing import Any, Final

import altair as alt
import pandas as pd
import streamlit as st

from hdt.console.theme import AGENT_SERIES
from hdt.public_dashboard import ui
from hdt.public_dashboard.data import LoadedSnapshot

ACCOUNT_KEY: Final[str] = "hdt_public_account"
ACCOUNTS: Final[tuple[str, ...]] = ("testnet", "live")
PAGE_SIZE: Final[int] = 25
AGENT_LABELS: Final[dict[str, str]] = {
    "crowding": "Crowding",
    "technical": "Technical",
    "micro": "Microstructure",
    "fundamental": "Fundamental",
    "news": "News",
    "macro": "Macro",
    "pooled": "Pooled",
}
ROLE_LABELS: Final[dict[str, str]] = {
    **{k: v for k, v in AGENT_LABELS.items() if k != "pooled"},
    "news_extractor": "News extractor",
    "news_judge_a": "News judge A",
    "news_judge_b": "News judge B",
    "reflection": "Reflection",
}
PIPELINE_LABELS: Final[dict[str, str]] = {
    "council": "Council agents",
    "news": "News pipeline (extractor + 2 judges)",
    "reflection": "Reflection",
}
EXIT_REASONS: Final[dict[str, str]] = {
    "time_stop": "Time stop (12 h)",
    "council_exit": "Council EXIT (held re-evaluation)",
    "stop": "Stop hit",
    "stop_hit": "Stop hit",
    "tp1": "TP1 hit",
    "tp1_hit": "TP1 hit",
}
SOURCES: Final[tuple[str, ...]] = ("LTX", "MIGRATION", "HOLLOW_HYPE", "HELD")
OUTCOMES: Final[tuple[str, ...]] = ("LONG", "SHORT", "NO_TRADE", "HOLD", "EXIT")
# Forecast targets in plain words: the internal codes (RAW_12H, RESID_12H) mean nothing to a visitor.
TARGET_LABELS: Final[dict[str, str]] = {
    "RAW_12H": "Price up in 12 h",
    "RESID_12H": "Beats BTC in 12 h",
}
TARGET_HELP: Final[dict[str, str]] = {
    "RAW_12H": "Forecasts whether the coin's price is higher 12 hours after the event. Used for Leverage "
    "Migration and Hollow Hype events.",
    "RESID_12H": "Forecasts whether the coin beats BTC over the next 12 hours: its price move after "
    "removing the part that simply follows BTC (as if hedged against BTC). Used for LTX events.",
}
# Why the meeting stopped (`stop_reason`), as the Debate rounds KPI and the Debate tab say it.
STOP_TEXT: Final[dict[str, str]] = {
    "no_new_claims": "no agent had a new verified claim",
    "max_rounds": "round limit reached without consensus",
    "debate_disabled": "debate is turned off",
}
NEW_VERSION_FORECASTS: Final[int] = 50
DECISIONS_EMPTY: Final[str] = "No council decisions yet"
DECISIONS_EMPTY_DETAIL: Final[str] = (
    "Each council session appears here as soon as the council decides; the result is filled in once "
    "the forecast is scored, 12 hours after the event."
)
ORDER_LEGS: Final[dict[str, str]] = {
    "entry": "Entry",
    "entry_ioc": "Entry (IOC)",
    "exit": "Exit",
    "sl": "Stop",
    "tp1": "TP1",
    "trail": "Trailing stop",
    "hedge": "BTC hedge",
}
ORDER_STATUS_KIND: Final[dict[str, ui.Kind]] = {
    "FILLED": "pos",
    "FINISHED": "pos",
    "TRIGGERED": "pos",
    "PARTIALLY_FILLED": "info",
    "NEW": "info",
    "PENDING": "info",
    "TRIGGERING": "info",
    "UNKNOWN": "warn",
    "CANCELED": "mute",
    "EXPIRED": "mute",
}


def agent_label(agent: str) -> str:
    return AGENT_LABELS.get(agent, agent)


def exit_reason(value: str | None) -> str:
    return EXIT_REASONS.get(value or "", value or "-")


def target_label(target_type: str) -> str:
    return TARGET_LABELS.get(target_type, target_type)


def stop_reason_text(stop_reason: str | None) -> str:
    """`consensus_round_<n>` or one of `STOP_TEXT`; 1 round alone does not mean consensus."""
    reason = stop_reason or ""
    if reason == "consensus_round_1":
        return "consensus in the blind round"
    if reason.startswith("consensus_round_"):
        return f"consensus in round {reason.removeprefix('consensus_round_')}"
    return STOP_TEXT.get(reason, reason.replace("_", " ") or "-")


def source_tag(source: str) -> str:
    return ui.tag(source, "llm" if source == "HOLLOW_HYPE" else "" if source == "HELD" else "cmc")


def outcome_html(outcome: str, size: float | None) -> str:
    if outcome in ("LONG", "SHORT"):
        mult = f" x{size:.1f}" if size else ""
        return f"{ui.side_html(outcome)}{ui.esc(mult)}"
    return f'<span class="muted">{ui.esc(outcome)}</span>'


def outcome_tag(outcome: str, size: float | None) -> str:
    """Outcome as a colored tag (LONG green, SHORT red, NO_TRADE grey, HOLD blue, EXIT amber)."""
    mult = f" x{size:.1f}" if outcome in ("LONG", "SHORT") and size else ""
    return f'<span class="outcome-tag o-{ui.esc(outcome.lower())}">{ui.esc(outcome + mult)}</span>'


def result_badge(decision: Mapping[str, Any]) -> str:
    if decision.get("unscored"):
        return ui.badge("Not scored", "mute")
    hit = decision.get("hit")
    label = decision.get("label")
    suffix = f" ({label})" if label else ""
    if hit is True:
        return ui.badge(f"Correct{suffix}", "pos")
    if hit is False:
        return ui.badge(f"Wrong{suffix}", "neg")
    return ui.badge("Pending", "mute")


def decision_link(event_id: str | None) -> str:
    if not event_id:
        return '<span class="muted">-</span>'
    return f'<a href="decisions?event={ui.esc(event_id)}" target="_self" class="mono">{ui.esc(event_id)}</a>'


# --------------------------------------------------------------------------- shared account selection


def _accounts(snap: LoadedSnapshot) -> dict[str, Mapping[str, Any]]:
    return {str(a["account"]): a for a in snap.data.get("accounts") or []}


def pick_account(available: Iterable[str], requested: str | None) -> str | None:
    """The account to show: the requested one if the snapshot has it, else live, then testnet (the account
    closest to real money). Paper is never public, even if an older snapshot still carries it."""
    present = set(available)
    if requested in present and requested in ACCOUNTS:
        return requested
    return next((a for a in reversed(ACCOUNTS) if a in present), None)


def _remember_account(key: str) -> None:
    if st.session_state.get(key):
        st.session_state[ACCOUNT_KEY] = st.session_state[key]


def select_account(snap: LoadedSnapshot, key: str) -> Mapping[str, Any] | None:
    accounts = _accounts(snap)
    current = pick_account(accounts, st.session_state.get(ACCOUNT_KEY))
    if current is None:
        ui.empty(
            "No account data yet",
            "Equity, positions and trades appear with the first snapshot that includes an account. "
            "Snapshots are published every 60 seconds.",
        )
        return None
    options = [a for a in ACCOUNTS if a in accounts]
    if len(options) == 1:
        # One account: the page already names it, so a one-button switch would only add noise.
        return accounts[current]
    # Each page has its own switch; seed it from the shared choice so a page never shows an older one.
    st.session_state[key] = current
    choice = st.segmented_control(
        "Account",
        options,
        format_func=str.capitalize,
        key=key,
        on_change=_remember_account,
        args=(key,),
        label_visibility="collapsed",
    )
    return accounts[choice or current]


def _open_counts(account: Mapping[str, Any]) -> str:
    positions = [p for p in account["open_positions"] if not p["is_hedge_book"]]
    longs = sum(1 for p in positions if p["side"] == "LONG")
    shorts = len(positions) - longs
    parts = [f"{longs} long", f"{shorts} short"]
    if any(p["is_hedge_book"] for p in account["open_positions"]):
        parts.append("BTC hedge")
    return ", ".join(parts)


# --------------------------------------------------------------------------- charts


def equity_chart(points: Sequence[Mapping[str, Any]]) -> None:
    if len(points) < 2:
        ui.empty(
            "Equity history is still short",
            "The curve needs at least two points. It fills in as hourly equity snapshots accumulate.",
        )
        return
    frame = pd.DataFrame(
        {"time": [ui.parse_ts(p["t"]) for p in points], "equity": [float(p["equity"]) for p in points]}
    )
    frame["peak"] = frame["equity"].cummax()
    frame["drawdown"] = frame["equity"] / frame["peak"] - 1.0
    frame["floor"] = frame["equity"].min()
    p = ui.palette()
    base = alt.Chart(frame).encode(x=alt.X("time:T", title=None))
    glow = base.mark_area(
        line=False,
        color={
            "gradient": "linear",
            "stops": [{"color": p["area"], "offset": 0}, {"color": "transparent", "offset": 1}],
            "x1": 1,
            "x2": 1,
            "y1": 0,
            "y2": 1,
        },
    ).encode(y=alt.Y("equity:Q", title=None, stack=None), y2=alt.Y2("floor:Q"))
    area = base.mark_area(color=p["neg"], opacity=0.08).encode(y=alt.Y("equity:Q", title=None), y2="peak:Q")
    line = base.mark_line(color=p["line"], strokeWidth=2, strokeJoin="round").encode(
        y=alt.Y(
            "equity:Q",
            title=None,
            scale=alt.Scale(zero=False),
            axis=alt.Axis(format=",.0f", tickCount=5),
        ),
        tooltip=[alt.Tooltip("time:T", format="%Y-%m-%d %H:%M"), alt.Tooltip("equity:Q", format=",.1f")],
    )
    first, last = frame["equity"].iloc[0], frame["equity"].iloc[-1]
    worst = frame["drawdown"].min()
    worst_at = frame["time"].iloc[int(frame["drawdown"].idxmin())]
    description = (
        f"Equity moved from {ui.fmt_num(first, 0)} to {ui.fmt_num(last, 0)} over the period. "
        f"Largest drawdown {ui.fmt_pct(abs(worst))} around {ui.fmt_ts(worst_at)} UTC. "
        f"Currently {ui.fmt_pct(abs(frame['drawdown'].iloc[-1]))} below the peak."
    )
    ui.show_chart(glow + area + line, description=description)


# --------------------------------------------------------------------------- Overview


def _freshness(snap: LoadedSnapshot) -> str:
    age = f"{ui.fmt_duration(snap.age().total_seconds())} old"
    return ui.badge(f"Delayed, {age}", "warn") if snap.is_stale() else ui.badge(f"Fresh, {age}", "pos")


def equity_hero(snap: LoadedSnapshot, account: Mapping[str, Any]) -> str:
    change_html, change_cls = ui.pnl_html(account["change_30d"])
    change_pct = ui.fmt_pct(account["change_30d_fraction"], 2, signed=True)
    return ui.Hero(
        f"Equity, {ui.esc(str(account['account']).lower())} account",
        ui.esc(ui.fmt_num(account["equity"])),
        f'<span class="{change_cls}">{change_html} ({ui.esc(change_pct)})</span>'
        '<span class="muted">last 30 days</span>',
        (
            ("Snapshot", f"{ui.esc(ui.fmt_ts(snap.generated_at))} UTC"),
            ("Freshness", _freshness(snap)),
            ("Published", "every 60 s"),
        ),
    ).html()


def overview(snap: LoadedSnapshot) -> None:
    ui.render(
        ui.page_head(
            "Overview",
            "Live performance of the AI council trading Binance USD-M perpetuals. Updated every 60 seconds.",
        )
    )
    account = select_account(snap, "overview-account")
    costs = snap.data.get("costs")
    scoring = snap.data.get("scoring")
    llm_30d = float(costs["last_30d"]["cost_usd"]) if costs else None
    items: list[ui.Kpi] = []
    if account is not None:
        ui.render(equity_hero(snap, account))
        today_html, today_cls = ui.pnl_html(account["today_pnl"])
        items.append(
            ui.Kpi(
                "Today's PnL",
                today_html,
                ui.esc(ui.fmt_pct(account["today_pnl_fraction"], 2, signed=True)),
                today_cls,
            )
        )
        drawdown = account["drawdown_fraction"]
        items.append(
            ui.Kpi(
                "Drawdown",
                ui.esc(ui.fmt_pct(drawdown, 2, signed=True) if drawdown is not None else "-"),
                "From the equity peak",
            )
        )
        open_positions = [p for p in account["open_positions"] if not p["is_hedge_book"]]
        items.append(
            ui.Kpi("Open positions", ui.esc(str(len(open_positions))), ui.esc(_open_counts(account)))
        )
    if scoring:
        n = int(scoring["scored_30d"])
        rate = int(scoring["hits_30d"]) / n if n else None
        items.append(
            ui.Kpi(
                "Hit rate, 30 days",
                ui.esc(ui.fmt_pct(rate)),
                f"{ui.fmt_int(n)} scored forecasts, baseline 50%",
            )
        )
    if llm_30d is not None:
        net = (
            (float(account["change_30d"]) - llm_30d)
            if account is not None and account["change_30d"] is not None
            else None
        )
        items.append(
            ui.Kpi(
                "LLM cost, 30 days", ui.esc(ui.fmt_usd(llm_30d)), f"Net PnL after LLM {ui.signed_span(net)}"
            )
        )
    if items:
        ui.render(ui.kpis(items))
    left, right = st.columns([2, 1], gap="large")
    with left, ui.panel("equity"):
        st.subheader("Equity")
        if account is not None:
            st.caption("After fees and funding")
            window = st.segmented_control(
                "Range",
                ["7D", "30D", "All"],
                default="30D",
                key="overview-range",
                label_visibility="collapsed",
            )
            if window == "All":
                points = account["equity_daily"]
            else:
                days = 7 if window == "7D" else 30
                cutoff = snap.generated_at - timedelta(days=days)
                points = [
                    p for p in account["equity_hourly_30d"] if (ui.parse_ts(p["t"]) or cutoff) >= cutoff
                ]
            equity_chart(points)
    with right, ui.panel("gates"):
        st.subheader("Validation gates")
        st.caption("Go-live criteria, checked on simulated trading results")
        gates(snap.data.get("gates"))
    st.subheader("Recent decisions")
    st.caption("Every council session, published as soon as the council decides")
    decisions_table((snap.data.get("decisions") or [])[:5])
    ui.render(
        ui.callout(
            "Public read-only view, updated every 60 seconds: every council session, position, level and "
            "order is published as it happens. API keys, configuration and operations are never published.",
            icon_name="lock",
        )
    )


def gates(items: Sequence[Mapping[str, Any]] | None) -> None:
    if not items:
        ui.empty(
            "No gate progress yet",
            "Progress toward the go-live gates appears once it is published. Real money waits until the "
            "statistical shadow gate G4 passes.",
        )
        return
    blocks = []
    for gate in items:
        value, target = gate["value"], gate["target"]
        fraction = (float(value) / float(target)) if value is not None and target else None
        status = ""
        if gate["passed"] is True:
            status = ui.badge("Passed", "pos")
        elif gate["passed"] is False:
            status = ui.badge("Not passed", "neg")
        meter = ""
        if fraction is not None:
            meter = ui.Meter(fraction, "pos" if gate["met"] else "warn").html() + (
                f'<div class="meter-cap"><span>{ui.esc(ui.fmt_num(value, 0))} / '
                f"{ui.esc(ui.fmt_num(target, 0))}"
                f"</span><span>{ui.esc(ui.fmt_pct(min(fraction, 1.0), 0))}</span></div>"
            )
        checks = ui.check_list(
            (
                c["met"],
                ui.esc(f"{c['label']}: {ui.fmt_num(c['value'])}" if c["value"] is not None else c["label"]),
            )
            for c in gate["checks"]
        )
        blocks.append(
            f"<h3>{ui.esc(gate['gate'])}: {ui.esc(gate['label'] or '')} {status}</h3>{meter}{checks}"
        )
    ui.render(ui.card('<div class="divider"></div>'.join(blocks)))


def decisions_table(rows: Sequence[Mapping[str, Any]]) -> None:
    if not rows:
        ui.empty(DECISIONS_EMPTY, DECISIONS_EMPTY_DETAIL)
        return
    table = ui.Table(
        [
            ui.Column("Event"),
            ui.Column("Time (UTC)"),
            ui.Column("Coin"),
            ui.Column("Source"),
            ui.Column("Outcome"),
            ui.Column(
                "Pooled p",
                numeric=True,
                tip="The council's combined, calibrated probability (0 to 1) that the coin goes up over the "
                "next 12 h (for LTX events: that it beats BTC). Above 0.5 leans LONG, below 0.5 leans SHORT.",
            ),
            ui.Column(
                "D",
                numeric=True,
                tip="Disagreement between the agents, weighted by their trust. Lower means they agree more; "
                "0 means they all say the same.",
            ),
            ui.Column(
                "Rounds",
                numeric=True,
                tip="Rounds the council held. Round 1 is blind. The meeting stops early on consensus or when "
                "no agent has a new verified claim, so 1 round does not always mean consensus.",
            ),
            ui.Column(
                "Result after 12 h",
                tip="Whether the forecast was right, checked 12 h after the event. Pending: not yet 12 h or "
                "no price label yet. Not scored: a held-coin re-evaluation, a shadow-only run, or a repeat "
                "session for a coin already scored in the same 12 h window.",
            ),
        ],
        caption="Council decisions",
    )
    for d in rows:
        table.rows.append(
            [
                ui.Cell(decision_link(d["event_id"])),
                ui.fmt_ts_short(d["as_of"]),
                d["symbol"],
                ui.Cell(source_tag(d["source"])),
                ui.Cell(outcome_html(d["outcome"], d["manager_size"])),
                ui.fmt_num(d["p_pooled"], 3),
                ui.fmt_num(d["disagreement"], 2),
                str(d["rounds"]),
                ui.Cell(result_badge(d)),
            ]
        )
    ui.render(table.html())


# --------------------------------------------------------------------------- Positions


def positions(snap: LoadedSnapshot) -> None:
    ui.render(
        ui.page_head(
            "Positions",
            "Every trade the AI takes, live: open positions with their stop, take-profit and liquidation "
            "levels, closed trades and every order sent to the exchange.",
        )
    )
    account = select_account(snap, "positions-account")
    if account is None:
        return
    open_positions = account["open_positions"]
    stats = account["trade_stats_30d"] or {}
    upnl = sum(float(p["unrealized_pnl"] or 0) for p in open_positions)
    upnl_html, upnl_cls = ui.pnl_html(upnl)
    equity = account["equity"]
    closed = int(stats.get("closed") or 0)
    wins = int(stats.get("wins") or 0)
    realized_html, realized_cls = ui.pnl_html(stats.get("realized_pnl"))
    ui.render(
        ui.kpis(
            [
                ui.Kpi(
                    "Open positions",
                    ui.esc(str(sum(1 for p in open_positions if not p["is_hedge_book"]))),
                    ui.esc(_open_counts(account)),
                ),
                ui.Kpi(
                    "Unrealized PnL",
                    upnl_html,
                    f"{ui.esc(ui.fmt_pct(upnl / equity if equity else None, 2, signed=True))} of equity",
                    upnl_cls,
                ),
                ui.Kpi("Realized PnL, 30 days", realized_html, f"{closed} closed trades", realized_cls),
                ui.Kpi(
                    "Win rate, 30 days",
                    ui.esc(ui.fmt_pct(wins / closed if closed else None)),
                    f"{wins} of {closed}, average {ui.esc(ui.fmt_signed(stats.get('avg_r')))} R",
                ),
                ui.Kpi(
                    "Fees + funding, 30 days",
                    ui.esc(ui.fmt_signed(stats.get("fees_funding_usd"))),
                    "Included in realized PnL",
                ),
            ]
        )
    )
    open_tab, closed_tab, orders_tab = st.tabs(["Open positions", "Closed trades", "Orders"])
    with open_tab:
        open_positions_table(open_positions, snap.generated_at)
    with closed_tab:
        closed_trades_table(account["closed_trades"])
    with orders_tab:
        orders_table(account.get("orders") or [])


def open_positions_table(rows: Sequence[Mapping[str, Any]], now: datetime) -> None:
    if not rows:
        ui.empty(
            "No open positions",
            "The council holds no position right now. A new position appears with the next snapshot, "
            "within about 60 seconds of the fill.",
        )
        return
    table = ui.Table(
        [
            ui.Column("Symbol"),
            ui.Column("Side"),
            ui.Column("Size", numeric=True),
            ui.Column("Entry", numeric=True),
            ui.Column("Mark", numeric=True),
            ui.Column("uPnL", numeric=True),
            ui.Column("Stop", numeric=True),
            ui.Column("TP1", numeric=True),
            ui.Column("Liq.", numeric=True),
            ui.Column("Leverage"),
            ui.Column("Opened (UTC)"),
            ui.Column("Time stop"),
            ui.Column("Decision"),
        ],
        caption="Open positions",
    )
    for p in rows:
        hedge = bool(p["is_hedge_book"])
        margin = (p["margin_type"] or "").lower().replace("crossed", "cross")
        leverage = f"{p['leverage']}x" if p["leverage"] else "-"
        leverage += " hedge book" if hedge else (f" {margin}" if margin else "")
        time_stop = "follows book" if hedge else "-"
        if not hedge and p["time_stop_at"]:
            remaining = (ui.parse_ts(p["time_stop_at"]) or now) - now
            time_stop = f"{ui.fmt_duration(remaining.total_seconds())} left"
        table.rows.append(
            [
                p["symbol"],
                ui.Cell(ui.side_html(p["side"])),
                ui.fmt_num(p["qty"], 4),
                ui.fmt_num(p["entry_price"], 4),
                ui.fmt_num(p["mark_price"], 4),
                ui.Cell(ui.signed_span(p["unrealized_pnl"])),
                ui.fmt_num(p["stop_price"], 4),
                ui.fmt_num(p["tp1_price"], 4),
                ui.fmt_num(p["liquidation_price"], 4),
                leverage,
                "rolling" if hedge else ui.fmt_ts_short(p["opened_at"]),
                time_stop,
                ui.Cell('<span class="muted">Hedge book</span>' if hedge else decision_link(p["event_id"])),
            ]
        )
    ui.render(table.html())
    if any(p["is_hedge_book"] for p in rows):
        st.caption(
            "The BTC hedge book offsets the beta of altcoin positions; its size follows the open positions."
        )


def closed_trades_table(rows: Sequence[Mapping[str, Any]]) -> None:
    if not rows:
        ui.empty(
            "No closed trades yet",
            "Each closed trade appears here with its stop, take-profit, R multiple and fees once the "
            "position is closed.",
        )
        return
    pages = max(1, (len(rows) + PAGE_SIZE - 1) // PAGE_SIZE)
    page = int(st.number_input("Page", 1, pages, 1, key="closed-page")) if pages > 1 else 1
    chunk = rows[(page - 1) * PAGE_SIZE : page * PAGE_SIZE]
    table = ui.Table(
        [
            ui.Column("Closed (UTC)"),
            ui.Column("Symbol"),
            ui.Column("Side"),
            ui.Column("Entry", numeric=True),
            ui.Column("Exit", numeric=True),
            ui.Column("Stop", numeric=True),
            ui.Column("TP1", numeric=True),
            ui.Column("R", numeric=True),
            ui.Column("PnL", numeric=True),
            ui.Column("Fees + funding", numeric=True),
            ui.Column("Exit reason"),
            ui.Column("Decision"),
        ],
        caption="Closed trades",
    )
    for t in chunk:
        table.rows.append(
            [
                ui.fmt_ts_short(t["closed_at"]),
                t["symbol"],
                ui.Cell(ui.side_html(t["side"])),
                ui.fmt_num(t["entry_price"], 4),
                ui.fmt_num(t["exit_price"], 4),
                ui.fmt_num(t["stop_price"], 4),
                ui.fmt_num(t["tp1_price"], 4),
                ui.fmt_signed(t["r_multiple"]),
                ui.Cell(ui.signed_span(t["realized_pnl"])),
                ui.fmt_signed(t["fees_funding_usd"]),
                exit_reason(t["exit_reason"]),
                ui.Cell(decision_link(t["event_id"])),
            ]
        )
    ui.render(table.html())
    st.caption(f"{len(chunk)} of {len(rows)} closed trades, page {page} / {pages}")


def orders_table(rows: Sequence[Mapping[str, Any]]) -> None:
    if not rows:
        ui.empty(
            "No orders yet",
            "Every order the exchange accepts appears here: entries, exits, and the stop and take-profit "
            "orders that protect each position.",
        )
        return
    pages = max(1, (len(rows) + PAGE_SIZE - 1) // PAGE_SIZE)
    page = int(st.number_input("Page", 1, pages, 1, key="orders-page")) if pages > 1 else 1
    chunk = rows[(page - 1) * PAGE_SIZE : page * PAGE_SIZE]
    table = ui.Table([*ORDER_COLUMNS, ui.Column("Decision")], caption="Orders")
    for o in chunk:
        table.rows.append([*_order_cells(o), ui.Cell(decision_link(o["event_id"]))])
    ui.render(table.html())
    st.caption(
        f"{len(chunk)} of {len(rows)} orders, page {page} / {pages}. Stop and take-profit orders show "
        "their trigger price."
    )


ORDER_COLUMNS: Final[tuple[ui.Column, ...]] = (
    ui.Column("Sent (UTC)"),
    ui.Column("Symbol"),
    ui.Column("Order"),
    ui.Column("Side"),
    ui.Column("Type"),
    ui.Column("Price", numeric=True),
    ui.Column("Qty", numeric=True),
    ui.Column("Filled", numeric=True),
    ui.Column("Avg price", numeric=True),
    ui.Column("Status"),
)


def _order_cells(o: Mapping[str, Any]) -> list[Any]:
    leg = ORDER_LEGS.get(str(o["leg"]), str(o["leg"]))
    if o["reduce_only"]:
        leg += " (reduce-only)"
    status = str(o["status"])
    return [
        ui.fmt_ts_short(o["created_at"]),
        o["symbol"],
        leg,
        ui.Cell(ui.badge(o["side"], "pos" if o["side"] == "BUY" else "neg")),
        str(o["order_type"]).replace("_", " ").lower(),
        ui.fmt_num(o["price"], 4),
        ui.fmt_num(o["qty"], 4),
        ui.fmt_num(o["executed_qty"], 4),
        ui.fmt_num(o["avg_price"], 4),
        ui.Cell(ui.badge(status.replace("_", " ").lower(), ORDER_STATUS_KIND.get(status, "mute"))),
    ]


# --------------------------------------------------------------------------- Decisions


def decisions(snap: LoadedSnapshot) -> None:
    event_id = st.query_params.get("event")
    if event_id:
        decision_card(snap, str(event_id))
        return
    ui.render(
        ui.page_head(
            "Council decisions",
            "Each event is one council session, published as soon as the council decides. The result "
            "is filled in once the forecast is scored, 12 hours after the event. Re-evaluations of held "
            "coins (HELD) are not scored.",
        )
    )
    rows: list[Mapping[str, Any]] = list(snap.data.get("decisions") or [])
    c1, c2, c3, c4 = st.columns([2, 1, 1, 1])
    query = c1.text_input("Search coin or event", placeholder="e.g. SOL, EVT-0927").strip().upper()
    source = c2.selectbox("Source", ["All", *SOURCES])
    outcome = c3.selectbox("Outcome", ["All", *OUTCOMES])
    day: date | None = c4.date_input("Date (UTC)", value=None)
    filtered = [
        d
        for d in rows
        if (not query or query in str(d["symbol"]).upper() or query in str(d["event_id"]).upper())
        and (source == "All" or d["source"] == source)
        and (outcome == "All" or d["outcome"] == outcome)
        and (day is None or (ui.parse_ts(d["as_of"]) or datetime.min.replace(tzinfo=UTC)).date() == day)
    ]
    if not filtered:
        if rows:
            ui.empty("No matching events", "Try removing a filter.")
        else:
            ui.empty(DECISIONS_EMPTY, DECISIONS_EMPTY_DETAIL)
        return
    pages = max(1, (len(filtered) + PAGE_SIZE - 1) // PAGE_SIZE)
    page = int(st.number_input("Page", 1, pages, 1, key="decisions-page")) if pages > 1 else 1
    decisions_table(filtered[(page - 1) * PAGE_SIZE : page * PAGE_SIZE])
    st.caption(f"{len(filtered)} of {len(rows)} events, page {page} / {pages}")
    ids = [str(d["event_id"]) for d in filtered]
    chosen = st.selectbox("Open a decision card", ids, index=None, placeholder="Select an event")
    if chosen:
        st.query_params["event"] = chosen
        st.rerun()


def decision_card(snap: LoadedSnapshot, event_id: str) -> None:
    if st.button("Back to decisions", icon=":material/arrow_back:"):
        del st.query_params["event"]
        st.rerun()
    cards = {str(c["event_id"]): c for c in snap.data.get("decision_cards") or []}
    card = cards.get(event_id)
    if card is None:
        ui.render(
            ui.page_head("No council card for this event")
            + ui.callout(
                "This event has no council decision card: it is either older than the newest 1,000 "
                "sessions, or an order the system placed itself (a test lifecycle or the close of a "
                "position the council did not open).",
                "info",
            )
        )
        return
    ui.render(
        f'<div class="crumb">Decisions / <span class="mono">{ui.esc(card["event_id"])}</span></div>'
        + ui.page_head(card["symbol"], badge_html=outcome_tag(card["outcome"], card["manager_size"]))
        + f'<div class="row-actions">{source_tag(card["source"])}{ui.tag(target_label(card["target_type"]))}'
        f"{ui.tag(ui.fmt_ts(card['as_of']) + ' UTC')}</div>"
    )
    usage = card["usage"] or {}
    rounds = int(card["rounds"])
    stop_text = stop_reason_text(card["stop_reason"])
    ui.render(
        ui.kpis(
            [
                ui.Kpi(
                    "Pooled p", ui.esc(ui.fmt_num(card["p_pooled"], 3)), "log-pool after r and calibration"
                ),
                ui.Kpi(
                    "Disagreement D", ui.esc(ui.fmt_num(card["disagreement"], 2)), "weighted disagreement"
                ),
                ui.Kpi("Debate rounds", ui.esc(str(rounds)), ui.esc(stop_text)),
                ui.Kpi(
                    "LLM cost",
                    ui.esc(ui.fmt_usd(usage.get("cost_usd"))),
                    f"{ui.fmt_int(usage.get('calls'))} calls, {ui.fmt_int(usage.get('agents'))} agents",
                ),
                ui.Kpi("Result after 12 h", result_badge(card)),
            ]
        )
    )
    t1, t2, t3, t4, t5 = st.tabs(
        ["Round 1 (blind)", "Debate", "Manager & pooling", "Risk & orders", "Levels & outcome"]
    )
    with t1:
        forecast_table([f for f in card["forecasts"] if f["round"] == 1])
    with t2:
        later = [f for f in card["forecasts"] if f["round"] > 1]
        if rounds <= 1:
            ui.render(ui.callout(f"No debate: {stop_text}, so round 2 did not open.", "info"))
        else:
            forecast_table(later)
        claims_table(card["claims"])
    with t3:
        consensus = ui.check_list((c["passed"], ui.esc(c["text"])) for c in card["consensus"])
        manager = ui.check_list((c["passed"], ui.esc(c["text"])) for c in card["manager_rule"])
        timeline = "".join(
            f'<li class="t-{ui.esc(e["tone"])}"><span class="when">{ui.esc(ui.fmt_ts_short(e["at"]))}</span>'
            f"{ui.esc(e['text'])}</li>"
            for e in card["timeline"]
        )
        left, right = st.columns(2)
        with left:
            ui.render(ui.card(consensus + manager, title="Consensus and Manager rule"))
        with right:
            ui.render(ui.card(f'<ul class="timeline">{timeline}</ul>', title="Timeline (UTC)"))
    with t4:
        risk_and_orders(card)
    with t5:
        levels_and_outcome(card)


def forecast_table(rows: Sequence[Mapping[str, Any]]) -> None:
    if not rows:
        ui.empty("No forecasts recorded for this round")
        return
    table = ui.Table(
        [
            ui.Column("Agent"),
            ui.Column("Round", numeric=True),
            ui.Column("Model"),
            ui.Column("p_model", numeric=True),
            ui.Column("p_llm", numeric=True),
            ui.Column("p_used", numeric=True),
            ui.Column("Stance"),
            ui.Column("Candidate"),
            ui.Column("Weight", numeric=True),
            ui.Column("Commit"),
        ],
        caption="Agent forecasts",
    )
    for f in rows:
        commit = f["commit_sha256"] or ""
        table.rows.append(
            [
                agent_label(f["agent"]),
                str(f["round"]),
                f["model_slug"] or "-",
                ui.fmt_num(f["p_model"], 3),
                ui.fmt_num(f["p_llm"], 3),
                ui.fmt_num(f["p_used"], 3),
                ui.Cell(ui.badge("Abstain", "mute")) if f["abstain"] else (f["stance"] or "-"),
                f["candidate_id"] or "-",
                ui.fmt_num(f["weight_norm"], 2),
                ui.Cell(
                    f'<span class="mono">{ui.esc(commit[:4] + "..." + commit[-4:] if commit else "-")}</span>'
                ),
            ]
        )
    ui.render(table.html())


def claims_table(rows: Sequence[Mapping[str, Any]]) -> None:
    if not rows:
        return
    table = ui.Table(
        [
            ui.Column("Claim"),
            ui.Column("Type"),
            ui.Column("Content"),
            ui.Column("Verified"),
            ui.Column("Hard"),
        ],
        caption="Shared claims",
    )
    for k in rows:
        kind = str(k["kind"] or "-") + (f" - {k['tier']}" if k["tier"] else "")
        verified = (
            ui.badge("Verified", "pos")
            if k["verified"]
            else ui.badge(f"Rejected: {k['reject_reason']}" if k["reject_reason"] else "Not verified", "neg")
        )
        table.rows.append(
            [
                ui.Cell(f'<span class="mono">{ui.esc(k["shared_id"])}</span>'),
                kind,
                ui.Cell(ui.esc(k["statement"]), "wrap"),
                ui.Cell(verified),
                "Hard" if k["hard"] else "No",
            ]
        )
    ui.render(table.html())
    st.caption("Claims are anonymized before sharing.")


RISK_RESULT_KIND: Final[dict[str, ui.Kind]] = {
    "approved": "pos",
    "rejected": "neg",
    "hold": "info",
    "ignored": "mute",
}


def risk_and_orders(card: Mapping[str, Any]) -> None:
    """What Risk made of the decision on each account, then every order the event sent."""
    if card["risk"]:
        table = ui.Table(
            [
                ui.Column("Account"),
                ui.Column("Council"),
                ui.Column("Applied"),
                ui.Column("Risk result"),
                ui.Column("Reason"),
                ui.Column("Decided (UTC)"),
            ],
            caption="Risk review per account",
        )
        for r in card["risk"]:
            result = str(r["result"])
            table.rows.append(
                [
                    str(r["account"]).capitalize(),
                    r["council_action"],
                    r["applied_action"] or "-",
                    ui.Cell(ui.badge(result, RISK_RESULT_KIND.get(result, "mute"))),
                    str(r["reason"] or "-").replace("_", " "),
                    ui.fmt_ts_short(r["decided_at"]),
                ]
            )
        ui.render(table.html())
    else:
        # No guess from the outcome: a held NO_TRADE becomes a reviewed HOLD / EXIT, a shadow LONG never
        # reaches Risk.
        ui.render(ui.callout("No Risk review is recorded for this decision.", "info"))
    if card["orders"]:
        table = ui.Table([ui.Column("Account"), *ORDER_COLUMNS], caption="Orders of this event")
        for o in card["orders"]:
            table.rows.append([str(o["account"]).capitalize(), *_order_cells(o)])
        ui.render(table.html())
        st.caption("Rejected orders are left out. Stop and take-profit orders show their trigger price.")
    else:
        ui.render(ui.callout("No order was sent for this event.", "info"))


def levels_and_outcome(card: Mapping[str, Any]) -> None:
    trades = card["trades"]
    if not trades and not card["levels"]:
        ui.render(ui.callout("No position was opened for this event, so no levels apply.", "info"))
        return
    if card["levels"]:
        table = ui.Table(
            [
                ui.Column("Candidate"),
                ui.Column("Side"),
                ui.Column("Entry", numeric=True),
                ui.Column("Invalidation", numeric=True),
                ui.Column("TP1", numeric=True),
                ui.Column("R:R", numeric=True),
                ui.Column(""),
            ],
            chosen={i for i, lv in enumerate(card["levels"]) if lv["chosen"]},
            caption="Level candidates",
        )
        for lv in card["levels"]:
            table.rows.append(
                [
                    lv["candidate_id"],
                    ui.Cell(ui.side_html(lv["side"])),
                    ui.fmt_num(lv["entry"], 4),
                    ui.fmt_num(lv["invalidation"], 4),
                    ui.fmt_num(lv["tp1"], 4),
                    ui.fmt_num(lv["rr"], 2),
                    ui.Cell(ui.badge("Chosen", "info") if lv["chosen"] else ""),
                ]
            )
        ui.render(table.html())
    if not trades:
        ui.render(ui.callout("No position was opened for this event.", "info"))
    for t in trades:
        is_open = t["closed_at"] is None
        exit_value = (
            "Open"
            if is_open
            else ui.esc(f"{ui.fmt_num(t['exit_price'], 4)} - {ui.fmt_ts_short(t['closed_at'])}")
        )
        ui.render(
            ui.card(
                ui.kv(
                    [
                        ("Side", ui.side_html(t["side"])),
                        (
                            "Entry",
                            ui.esc(f"{ui.fmt_num(t['entry_price'], 4)} - {ui.fmt_ts_short(t['opened_at'])}"),
                        ),
                        ("Exit", exit_value),
                        ("Stop", ui.esc(ui.fmt_num(t["stop_price"], 4))),
                        ("TP1", ui.esc(ui.fmt_num(t["tp1_price"], 4))),
                        ("Exit reason", ui.esc("-" if is_open else exit_reason(t["exit_reason"]))),
                        ("R multiple", ui.esc(ui.fmt_signed(t["r_multiple"]))),
                        ("PnL (after fees + funding)", ui.signed_span(t["realized_pnl"])),
                    ]
                ),
                title=f"{t['symbol']} trade ({str(t['account']).capitalize()} account"
                + (", open)" if is_open else ")"),
            )
        )


# --------------------------------------------------------------------------- Agents


def agents(snap: LoadedSnapshot) -> None:
    ui.render(
        ui.page_head(
            "Agents & weights",
            "w is learned from blind round-1 forecasts (Hedge, sleeping experts). a is the trust in the LLM "
            "adjustment, r the trust in revisions. Weights are learned separately for each forecast target.",
        )
    )
    views = {str(v["target_type"]): v for v in snap.data.get("agents") or []}
    if not views:
        ui.empty(
            "No agent weights yet",
            "Weights, calibration and log-loss appear once the scorer has labelled forecasts, 12 hours "
            "after each event.",
        )
        return
    target = st.segmented_control(
        "Target",
        list(views),
        default=next(iter(views)),
        format_func=target_label,
        key="agents-target",
        label_visibility="collapsed",
    )
    chosen = target or next(iter(views))
    st.caption(TARGET_HELP.get(chosen, ""))
    view = views[chosen]
    table = ui.Table(
        [
            ui.Column("Agent"),
            ui.Column("Version"),
            ui.Column("Model"),
            ui.Column("w", numeric=True),
            ui.Column("a", numeric=True),
            ui.Column("r", numeric=True),
            ui.Column("Coverage", numeric=True),
            ui.Column("Forecasts", numeric=True),
            ui.Column("Log-loss 30d", numeric=True),
            ui.Column("Rejected claims", numeric=True),
            ui.Column("LLM cost 30d", numeric=True),
        ],
        caption="Agent weights",
    )
    for a in view["agents"]:
        version = a["version"]
        forecasts = a["forecasts"]
        version_html = ui.esc(f"v{version}" if version is not None else "-")
        if version and version > 1 and forecasts is not None and forecasts < NEW_VERSION_FORECASTS:
            version_html += " " + ui.badge(f"{forecasts}/{NEW_VERSION_FORECASTS}", "warn")
        w_html = ui.esc(ui.fmt_num(a["w"]))
        if a["w_capped"] is not None and a["w"] is not None and a["w_capped"] < a["w"]:
            w_html = ui.esc(f"{ui.fmt_num(a['w'])} to {ui.fmt_num(a['w_capped'])}")
        table.rows.append(
            [
                agent_label(a["agent"]),
                ui.Cell(version_html),
                a["model_slug"] or "-",
                ui.Cell(w_html),
                ui.fmt_num(a["a"]),
                ui.fmt_num(a["r"]),
                ui.fmt_pct(a["coverage"], 0),
                ui.fmt_int(forecasts),
                ui.fmt_num(a["log_loss_30d"], 3),
                ui.fmt_pct(a["rejected_claims_fraction"]),
                ui.fmt_usd(a["llm_cost_30d"]),
            ]
        )
    ui.render(table.html())
    st.caption(
        "Baseline: a forecast that always says 0.5 has a log-loss of 0.693. News pipeline costs (extractor, "
        "judges) are on the AI costs page."
    )
    left, right = st.columns(2, gap="large")
    with left, ui.panel("weights"):
        st.subheader("Weight w over time")
        weight_chart(view["weight_history"], view["version_changes"])
    with right, ui.panel("calibration"):
        st.subheader("Calibration")
        calibration(view["calibration"])


def weight_chart(points: Sequence[Mapping[str, Any]], changes: Sequence[Mapping[str, Any]]) -> None:
    if not points:
        ui.empty("No weight history yet", "The weight curve starts after the first scored forecasts.")
        return
    p = ui.palette()
    tokens = {
        "cmc": p["cmc"],
        "link": p["line"],
        "pos": p["pos"],
        "bnb": p["bnb"],
        "llm": p["llm"],
        "warn": p["warn"],
    }
    order = [agent_label(name) for name, _, _ in AGENT_SERIES]
    colors = [tokens[token] for _, token, _ in AGENT_SERIES]
    dashes = [list(d) if d else [1, 0] for _, _, d in AGENT_SERIES]
    frame = pd.DataFrame(
        {
            "day": [x["day"] for x in points],
            "agent": [agent_label(x["agent"]) for x in points],
            "w": [x["w"] for x in points],
        }
    )
    line = (
        alt.Chart(frame)
        .mark_line(strokeWidth=2)
        .encode(
            x=alt.X("day:T", title=None, axis=alt.Axis(format="%b %d", tickCount="day")),
            y=alt.Y("w:Q", title=None),
            color=alt.Color(
                "agent:N",
                scale=alt.Scale(domain=order, range=colors),
                title=None,
                legend=alt.Legend(columns=3, symbolType="stroke"),
            ),
            strokeDash=alt.StrokeDash(
                "agent:N",
                scale=alt.Scale(domain=order, range=dashes),
                title=None,
                legend=alt.Legend(columns=3, symbolType="stroke"),
            ),
            tooltip=["agent:N", alt.Tooltip("day:T", format="%Y-%m-%d"), alt.Tooltip("w:Q", format=".3f")],
        )
    )
    chart: alt.TopLevelMixin = line
    if changes:
        marks = pd.DataFrame(
            {
                "at": [c["at"] for c in changes],
                "label": [f"{agent_label(c['agent'])} v{c['version']}" for c in changes],
            }
        )
        rules = (
            alt.Chart(marks)
            .mark_rule(color=p["muted"], strokeDash=[3, 3])
            .encode(x="at:T", tooltip=["label:N"])
        )
        chart = line + rules
    last = frame.sort_values("day").groupby("agent").tail(1)
    summary = ", ".join(f"{r.agent} {r.w:.2f}" for r in last.itertuples())
    ui.show_chart(chart, description=f"60 days, latest weights: {summary}.")


def calibration(items: Sequence[Mapping[str, Any]]) -> None:
    if not items:
        ui.empty(
            "No calibration yet",
            "Calibration bins fill in as forecasts are scored, 12 hours after each event.",
        )
        return
    by_agent = {str(c["agent"]): c for c in items}
    names = sorted(by_agent, key=lambda n: (n != "pooled", n))
    chosen = st.selectbox("Agent", names, format_func=agent_label, key="calibration-agent")
    cal = by_agent[chosen]
    bins = [b for b in cal["bins"] if b["mean_p"] is not None and b["observed_rate"] is not None]
    p = ui.palette()
    if bins:
        frame = pd.DataFrame(bins)
        diagonal = (
            alt.Chart(pd.DataFrame({"x": [0.0, 1.0], "y": [0.0, 1.0]}))
            .mark_line(color=p["muted"], strokeDash=[4, 4])
            .encode(x=alt.X("x:Q", title="Forecast p"), y=alt.Y("y:Q", title="Observed rate"))
        )
        points = (
            alt.Chart(frame)
            .mark_line(point=alt.OverlayMarkDef(color=p["line"], filled=True, size=48), color=p["line"])
            .encode(
                x="mean_p:Q",
                y="observed_rate:Q",
                tooltip=[
                    alt.Tooltip("mean_p:Q", format=".2f"),
                    alt.Tooltip("observed_rate:Q", format=".2f"),
                    "n:Q",
                ],
            )
        )
        ui.show_chart(
            diagonal + points,
            height=240,
            description="Horizontal: forecast p; vertical: observed rate; the dashed diagonal is "
            "perfect calibration.",
        )
    badges = []
    if cal["spiegelhalter_z"] is not None:
        badges.append(
            ui.badge(
                f"Spiegelhalter Z {cal['spiegelhalter_z']:.2f}",
                "pos" if abs(cal["spiegelhalter_z"]) < 1.96 else "warn",
            )
        )
    if cal["ece"] is not None:
        badges.append(ui.badge(f"ECE {cal['ece'] * 100:.1f} pts", "mute"))
    if cal["n"] is not None:
        badges.append(ui.badge(f"N = {cal['n']}", "mute"))
    table = ui.Table(
        [
            ui.Column("Bin"),
            ui.Column("n", numeric=True),
            ui.Column("Mean p", numeric=True),
            ui.Column("Observed", numeric=True),
        ],
        caption="Calibration bins",
    )
    for b in cal["bins"]:
        table.rows.append(
            [
                f"{b['bin_lo']:.1f}-{b['bin_hi']:.1f}",
                ui.fmt_int(b["n"]),
                ui.fmt_num(b["mean_p"], 3),
                ui.fmt_num(b["observed_rate"], 3),
            ]
        )
    ui.render(f'<div class="row-actions">{"".join(badges)}</div>' + table.html())


# --------------------------------------------------------------------------- AI costs


def costs(snap: LoadedSnapshot) -> None:
    ui.render(
        ui.page_head(
            "AI costs",
            "What the council spends on LLM calls, taken from OpenRouter generation records "
            "(usage.cost), not estimates. Every model is called through OpenRouter.",
        )
    )
    data = snap.data.get("costs")
    if not data:
        ui.empty(
            "No LLM costs yet",
            "Costs come from OpenRouter generation records and appear after the first council call.",
        )
        return
    today, month = data["today"], data["last_30d"]
    per_event = month["council_cost_usd"] / month["council_events"] if month["council_events"] else None
    current = pick_account(_accounts(snap), st.session_state.get(ACCOUNT_KEY))
    account = _accounts(snap).get(current) if current else None
    trading = float(account["change_30d"]) if account and account["change_30d"] is not None else None
    net = trading - month["cost_usd"] if trading is not None else None
    net_html, net_cls = ui.pnl_html(net, arrow=False)
    ui.render(
        ui.kpis(
            [
                ui.Kpi(
                    "LLM cost today",
                    ui.esc(ui.fmt_usd(today["cost_usd"])),
                    f"{ui.fmt_int(today['calls'])} calls so far (UTC day)",
                ),
                ui.Kpi(
                    "LLM cost, 30 days",
                    ui.esc(ui.fmt_usd(month["cost_usd"])),
                    f"{ui.fmt_int(month['calls'])} calls",
                ),
                ui.Kpi(
                    "Cost per council event",
                    ui.esc(ui.fmt_usd(per_event)),
                    f"{ui.fmt_int(month['council_events'])} events in 30 days",
                ),
                ui.Kpi(
                    "Net PnL after LLM cost, 30 days",
                    net_html,
                    ui.esc(f"Trading {ui.fmt_signed(trading)} minus LLM {ui.fmt_usd(month['cost_usd'])}"),
                    net_cls,
                ),
            ]
        )
    )
    left, right = st.columns([2, 1], gap="large")
    with left, ui.panel("daily-cost"):
        st.subheader("Daily LLM cost, 30 days")
        st.caption("Last bar is today, still in progress")
        daily_cost_chart(data["daily"])
    with right, ui.panel("pipeline-cost"):
        st.subheader("Cost by pipeline, 30 days")
        st.caption("Share of the 30-day LLM spend")
        total = sum(float(x["cost_usd"]) for x in data["by_pipeline"]) or 0.0
        rows = []
        for item in data["by_pipeline"]:
            share = float(item["cost_usd"]) / total if total else 0.0
            rows.append(
                f"<div><b>{ui.esc(PIPELINE_LABELS.get(item['pipeline'], item['pipeline']))}</b>"
                f'<div class="meter-cap"><span>{ui.esc(ui.fmt_usd(item["cost_usd"]))}</span>'
                f"<span>{ui.esc(ui.fmt_pct(share))}</span></div>{ui.Meter(share).html()}</div>"
            )
        ui.render(
            f'<div class="rows">{"".join(rows)}</div>'
            if rows
            else ui.empty_html("No LLM calls in the last 30 days")
        )
    st.subheader("Cost by role, 30 days")
    role_table = ui.Table(
        [
            ui.Column("Role"),
            ui.Column("Model"),
            ui.Column("Calls", numeric=True),
            ui.Column("Input tokens", numeric=True),
            ui.Column("Output tokens", numeric=True),
            ui.Column("Cost", numeric=True),
            ui.Column("$ per call", numeric=True),
        ],
        caption="LLM cost by role",
    )
    for r in data["by_role"]:
        role_table.rows.append(
            _cost_row(ROLE_LABELS.get(r["role"], r["role"]), ", ".join(r["models"]) or "-", r)
        )
    if data["by_role"]:
        totals = {
            "calls": sum(r["calls"] for r in data["by_role"]),
            "prompt_tokens": sum(r["prompt_tokens"] for r in data["by_role"]),
            "completion_tokens": sum(r["completion_tokens"] for r in data["by_role"]),
            "cost_usd": sum(r["cost_usd"] for r in data["by_role"]),
        }
        role_table.rows.append(_cost_row("Total", "", totals))
    ui.render(role_table.html())
    st.subheader("Cost by model, 30 days")
    model_table = ui.Table(
        [
            ui.Column("Model"),
            ui.Column("Calls", numeric=True),
            ui.Column("Input tokens", numeric=True),
            ui.Column("Output tokens", numeric=True),
            ui.Column("Cost", numeric=True),
            ui.Column("$ per call", numeric=True),
        ],
        caption="LLM cost by model",
    )
    for m in data["by_model"]:
        model_table.rows.append(_cost_row(m["model_slug"], None, m))
    ui.render(model_table.html())


def _cost_row(label: str, models: str | None, r: Mapping[str, Any]) -> list[ui.CellLike]:
    calls = int(r["calls"])
    cells: list[ui.CellLike] = [label]
    if models is not None:
        cells.append(models)
    cells += [
        ui.fmt_int(calls),
        f"{r['prompt_tokens'] / 1e6:,.2f}M",
        f"{r['completion_tokens'] / 1e6:,.2f}M",
        ui.fmt_usd(r["cost_usd"]),
        ui.fmt_num(r["cost_usd"] / calls, 4) if calls else "-",
    ]
    return cells


def daily_cost_chart(days: Sequence[Mapping[str, Any]]) -> None:
    if not days:
        ui.empty("No LLM calls yet", "Daily cost bars appear after the first council call.")
        return
    p = ui.palette()
    frame = pd.DataFrame(days)
    frame["part"] = [
        "Today (in progress)" if i == len(frame) - 1 else "Complete day" for i in range(len(frame))
    ]
    peak = float(frame["cost_usd"].max() or 0.0)
    # Enough decimals that neighbouring ticks never print the same label (e.g. $0.001 twice).
    decimals = max(2, -math.floor(math.log10(peak / 4))) if peak > 0 else 2
    chart = (
        alt.Chart(frame)
        .mark_bar(cornerRadiusTopLeft=3, cornerRadiusTopRight=3)
        .encode(
            x=alt.X("day:T", title=None, axis=alt.Axis(format="%b %d", labelAngle=0, tickCount=6)),
            y=alt.Y("cost_usd:Q", title=None, axis=alt.Axis(format=f"$,.{decimals}f", tickCount=4)),
            color=alt.Color(
                "part:N",
                scale=alt.Scale(
                    domain=["Complete day", "Today (in progress)"], range=[p["line"], p["muted"]]
                ),
                title=None,
            ),
            tooltip=[
                alt.Tooltip("day:T", format="%Y-%m-%d"),
                alt.Tooltip("cost_usd:Q", format="$,.2f"),
                "calls:Q",
                "part:N",
            ],
        )
    )
    total = sum(float(d["cost_usd"]) for d in days)
    ui.show_chart(
        chart, description=f"{ui.fmt_usd(total)} over the last {len(days)} days (today still in progress)."
    )
