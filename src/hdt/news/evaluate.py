"""Measure the news pipeline against the human label set (`news_labels`, filled by the labeling app).

Phase 07 success criterion, on >= 300 labeled items:
- hollow precision >= 85%: of the items both judges classified RUMOR / KOL_MEME / CIRCULAR, the share a
  human also labeled one of those classes;
- hard catalyst veto recall >= 95%: of the items a human labeled EXPLOIT / DELIST / UNLOCK and hard, the
  share the pipeline caught with the human's class (agreed on that class, or a judge disagreement in
  which one judge said it and it is EXPLOIT / DELIST, which the veto scan turns into a soft veto);
- abstain rate <= 30%: of the labeled canonical items, the share without an agreed or no-event verdict
  (judge disagreement, failed quote check, failed or missing model output);
- leaked duplicates <= 5%: of the items a human labeled duplicate, the share ingestion did not mark as a
  duplicate.

Usage: `python -m hdt.news.evaluate [--labeler NAME] [--rule-version V]` (HDT_PG_DSN: a role that reads
`news_items`, `news_verdicts` and `news_labels`, e.g. hdt_news or hdt_console_ro). Exit code 0 when every
criterion passes, 1 otherwise, 2 when fewer than the minimum number of labels exist.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from typing import Final

import sqlalchemy as sa

from hdt.db.models.news import NewsItemRow, NewsLabelRow, NewsVerdictRow
from hdt.db.session import make_engine
from hdt.news.classes import DISAGREEMENT_VETO_CLASSES, VETO_CLASSES
from hdt.news.config import news_config

MIN_LABELS: Final[int] = 300
HOLLOW: Final[frozenset[str]] = frozenset({"RUMOR", "KOL_MEME", "CIRCULAR"})
TARGETS: Final[dict[str, float]] = {
    "hollow_precision": 0.85,
    "veto_recall": 0.95,
    "abstain_rate": 0.30,
    "leaked_duplicates": 0.05,
}


@dataclass(frozen=True)
class LabeledItem:
    item_id: str
    label_class: str
    label_hard: bool
    label_duplicate: bool
    marked_duplicate: bool
    status: str | None
    """Verdict status, None when the item has no verdict of the rule version."""
    event_class: str | None
    class_a: str | None
    class_b: str | None


@dataclass(frozen=True)
class Report:
    labeled: int
    hollow_precision: float | None
    hollow_n: int
    veto_recall: float | None
    veto_n: int
    abstain_rate: float | None
    abstain_n: int
    leaked_duplicates: float | None
    duplicates_n: int

    def passed(self) -> dict[str, bool | None]:
        return {
            "hollow_precision": None
            if self.hollow_precision is None
            else self.hollow_precision >= TARGETS["hollow_precision"],
            "veto_recall": None if self.veto_recall is None else self.veto_recall >= TARGETS["veto_recall"],
            "abstain_rate": None
            if self.abstain_rate is None
            else self.abstain_rate <= TARGETS["abstain_rate"],
            "leaked_duplicates": None
            if self.leaked_duplicates is None
            else self.leaked_duplicates <= TARGETS["leaked_duplicates"],
        }


def _share(hits: int, total: int) -> float | None:
    return round(hits / total, 4) if total else None


def evaluate(items: Sequence[LabeledItem]) -> Report:
    hollow = [i for i in items if i.status == "agreed" and i.event_class in HOLLOW]
    veto = [i for i in items if i.label_class in VETO_CLASSES and i.label_hard]
    caught = [
        i
        for i in veto
        if (i.status == "agreed" and i.event_class == i.label_class)
        or (
            i.status == "disagree"
            and i.label_class in DISAGREEMENT_VETO_CLASSES
            and i.label_class in {i.class_a, i.class_b}
        )
    ]
    canonical = [i for i in items if not i.marked_duplicate]
    abstained = [i for i in canonical if i.status not in ("agreed", "no_event")]
    duplicates = [i for i in items if i.label_duplicate]
    leaked = [i for i in duplicates if not i.marked_duplicate]
    return Report(
        labeled=len(items),
        hollow_precision=_share(sum(1 for i in hollow if i.label_class in HOLLOW), len(hollow)),
        hollow_n=len(hollow),
        veto_recall=_share(len(caught), len(veto)),
        veto_n=len(veto),
        abstain_rate=_share(len(abstained), len(canonical)),
        abstain_n=len(canonical),
        leaked_duplicates=_share(len(leaked), len(duplicates)),
        duplicates_n=len(duplicates),
    )


def load_items(engine: sa.Engine, rule_version: str, labeler: str | None) -> list[LabeledItem]:
    labels = NewsLabelRow.__table__
    items = NewsItemRow.__table__
    verdicts = NewsVerdictRow.__table__
    query = (
        sa.select(
            labels.c.item_id,
            labels.c.event_class,
            labels.c.hardness,
            labels.c.duplicate,
            items.c.duplicate_of,
            verdicts.c.status,
            verdicts.c.event_class.label("v_class"),
            verdicts.c.class_a,
            verdicts.c.class_b,
        )
        .join(items, items.c.item_id == labels.c.item_id)
        .outerjoin(
            verdicts,
            sa.and_(verdicts.c.item_id == labels.c.item_id, verdicts.c.rule_version == rule_version),
        )
        .order_by(labels.c.item_id, labels.c.labeled_at.desc())
    )
    if labeler is not None:
        query = query.where(labels.c.labeler == labeler)
    seen: set[str] = set()
    out: list[LabeledItem] = []
    with engine.connect() as conn:
        for r in conn.execute(query):
            if r.item_id in seen:
                continue  # one label per item: the newest (several labelers without --labeler)
            seen.add(r.item_id)
            out.append(
                LabeledItem(
                    item_id=r.item_id,
                    label_class=r.event_class,
                    label_hard=r.hardness == "hard",
                    label_duplicate=bool(r.duplicate),
                    marked_duplicate=r.duplicate_of is not None,
                    status=r.status,
                    event_class=r.v_class,
                    class_a=r.class_a,
                    class_b=r.class_b,
                )
            )
    return out


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("--labeler", default=None)
    parser.add_argument("--rule-version", default=news_config().rule_version)
    args = parser.parse_args(argv)
    engine = make_engine()
    try:
        items = load_items(engine, args.rule_version, args.labeler)
    finally:
        engine.dispose()
    report = evaluate(items)
    passed = report.passed()
    print(json.dumps({"report": asdict(report), "targets": TARGETS, "passed": passed}, indent=2))
    if report.labeled < MIN_LABELS:
        print(f"only {report.labeled} labeled items; the criterion needs {MIN_LABELS}", file=sys.stderr)
        return 2
    return 0 if all(v is True for v in passed.values()) else 1


if __name__ == "__main__":
    sys.exit(main())
