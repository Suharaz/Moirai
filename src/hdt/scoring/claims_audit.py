"""Wrong-claims audit (phase 08): per agent, the share of its claims the council verifier penalized over the
last `window_days` (7). Above `max_wrong_rate` (5%, on at least `min_claims` claims) the scorer opens a flag
in `claims_audit_flags` and raises a `claims_audit` warning; while the flag is open the agent's weight
ceiling is `flagged_ceiling` (0.20). A flag stays open until an operator reviews it:

    python -m hdt.scoring.claims_audit review --agent news --by <operator> --note "<why>"

(runs with the scorer DSN in `HDT_PG_DSN`). A claim counts once per event even when shared in several
rounds; wrong = the verifier penalized it (`decision_claims.penalty`: its `PENALIZED` reject reasons).
Claims rejected without a penalty (`repeated`, `instruction_like`, `packet_missing`) are not wrong claims.
After a review the agent's window restarts at the review time, so the reviewed claims cannot re-flag it.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timedelta

import sqlalchemy as sa
from sqlalchemy.orm import Session

from hdt.contracts.common import AgentName
from hdt.core.alerts import alert
from hdt.core.clock import utcnow
from hdt.core.config import ClaimsAuditParams
from hdt.db.models.decision import DecisionCardRow, DecisionClaimRow
from hdt.db.models.scoring import ClaimsAuditFlagRow
from hdt.db.session import make_engine, make_session_factory, transaction

SERVICE = "scorer"


@dataclass(frozen=True)
class ClaimRate:
    agent: str
    claims: int
    rejected: int

    @property
    def rate(self) -> float:
        return self.rejected / self.claims if self.claims else 0.0


def claim_rates(
    conn: sa.Connection, *, start: datetime, end: datetime, agent: str | None = None
) -> list[ClaimRate]:
    c, d = DecisionClaimRow, DecisionCardRow
    per_claim = (
        sa.select(
            c.source_agent.label("agent"),
            c.event_id,
            c.claim_sha256,
            sa.func.bool_or(c.penalty).label("rejected"),
        )
        .join(d, d.event_id == c.event_id)
        .where(d.as_of >= start, d.as_of < end, *(() if agent is None else (c.source_agent == agent,)))
        .group_by(c.source_agent, c.event_id, c.claim_sha256)
        .subquery()
    )
    rows = conn.execute(
        sa.select(
            per_claim.c.agent,
            sa.func.count().label("claims"),
            sa.func.count().filter(per_claim.c.rejected).label("rejected"),
        )
        .group_by(per_claim.c.agent)
        .order_by(per_claim.c.agent)
    ).all()
    return [ClaimRate(r.agent, int(r.claims), int(r.rejected)) for r in rows]


def audit(session: Session, params: ClaimsAuditParams, *, now: datetime) -> list[ClaimRate]:
    """Open a flag (and raise the warning) for each agent above the limit without an open flag. An agent's
    window starts at `max(now - window_days, its last review)`."""
    start = now - timedelta(days=params.window_days)
    conn = session.connection()
    f = ClaimsAuditFlagRow
    open_flags = set(conn.execute(sa.select(f.agent).where(f.reviewed_at.is_(None))).scalars())
    reviewed: dict[str, datetime] = {
        str(r.agent): r.last
        for r in conn.execute(
            sa.select(f.agent, sa.func.max(f.reviewed_at).label("last"))
            .where(f.reviewed_at.is_not(None))
            .group_by(f.agent)
        )
    }
    rates: list[tuple[datetime, ClaimRate]] = []
    for rate in claim_rates(conn, start=start, end=now):
        last = reviewed.get(rate.agent)
        if last is not None and last > start:
            rates.extend((last, r) for r in claim_rates(conn, start=last, end=now, agent=rate.agent))
        else:
            rates.append((start, rate))
    flagged: list[ClaimRate] = []
    for window_start, rate in rates:
        if rate.agent in open_flags or rate.claims < params.min_claims or rate.rate <= params.max_wrong_rate:
            continue
        session.add(
            ClaimsAuditFlagRow(
                agent=rate.agent,
                raised_at=now,
                window_start=window_start,
                window_end=now,
                claims=rate.claims,
                rejected=rate.rejected,
                rate=rate.rate,
            )
        )
        alert(
            session,
            kind="claims_audit",
            severity="warning",
            title=f"{rate.agent}: {rate.rejected} of {rate.claims} claims penalized since "
            f"{window_start:%Y-%m-%d %H:%M} UTC",
            detail=f"weight ceiling lowered to {params.flagged_ceiling:.2f} until reviewed",
            service=SERVICE,
            episode=rate.agent,
        )
        flagged.append(rate)
    session.flush()
    return flagged


def review(session: Session, agent: str, *, by: str, note: str, now: datetime) -> bool:
    """Close the agent's open flag; False when none is open."""
    result = session.execute(
        sa.update(ClaimsAuditFlagRow)
        .where(ClaimsAuditFlagRow.agent == AgentName(agent).value, ClaimsAuditFlagRow.reviewed_at.is_(None))
        .values(reviewed_at=now, reviewed_by=by, review_note=note)
    )
    return bool(result.rowcount)  # type: ignore[attr-defined]


def main() -> None:
    parser = argparse.ArgumentParser(description="Review a claims-audit flag (restores the agent's ceiling).")
    sub = parser.add_subparsers(dest="command", required=True)
    rv = sub.add_parser("review")
    rv.add_argument("--agent", required=True, choices=[a.value for a in AgentName])
    rv.add_argument("--by", required=True)
    rv.add_argument("--note", required=True)
    args = parser.parse_args()
    engine = make_engine(pool_size=1)
    try:
        with transaction(make_session_factory(engine)) as session:
            closed = review(session, args.agent, by=args.by, note=args.note, now=utcnow())
    finally:
        engine.dispose()
    print("flag reviewed" if closed else "no open flag for this agent")


if __name__ == "__main__":
    main()
