"""Quote check, judge disagreement and code tiering of the item pipeline (hdt.news.verdict)."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any

from pydantic import BaseModel

from hdt.agents.llm_types import Generation, LlmUnavailableError
from hdt.contracts.common import Tier
from hdt.contracts.forecast import LlmUsage
from hdt.core.config import static_config
from hdt.news.coins import CoinRef
from hdt.news.config import load_news_config
from hdt.news.extractor import ExtractorOutput
from hdt.news.judge import JudgeOutput
from hdt.news.store import StoredItem
from hdt.news.text import quote_found
from hdt.news.verdict import ItemJudge, OfficialEvidence, RoleConfigs, VerdictJob
from hdt.settings.schemas import RoleModelConfig
from hdt.tools.ports import Announcement

NOW = datetime(2026, 9, 1, 12, tzinfo=UTC)
CFG = load_news_config()
SOURCES = static_config().news_sources
COIN = CoinRef(5, "XYZ", "Xyz Protocol")
TEXT = "Binance will list Xyz Protocol (XYZ) on September 2. Trading opens at 08:00 UTC."


def test_quote_found_is_verbatim_modulo_typography() -> None:
    text = "The team said \u201cfunds are safe\u201d after the report \u2014 no drain found."
    assert quote_found('The team said "funds are safe" after the report - no drain found.', text)
    assert quote_found("  funds are safe\u201d after the   report ", text)
    assert not quote_found("The team said funds were drained.", text)
    assert not quote_found("funds", text)  # too short to prove anything


def item(url: str = "https://www.coindesk.com/x", kind: str = "rss") -> StoredItem:
    return StoredItem(
        item_id="rss:abc",
        source_key="coindesk",
        source_kind=kind,
        source_name="CoinDesk",
        url=url,
        domain=url.split("/")[2],
        tier=Tier.T1,
        title="Binance to list XYZ",
        summary=TEXT,
        content=None,
        published_at=NOW,
        ingested_at=NOW,
        coin_ids=(5,),
        duplicate_of=None,
    )


DEVELOPERS = {"news_extractor": "vendor-x", "news_judge_a": "vendor-a", "news_judge_b": "vendor-b"}


class FakeLlm:
    def __init__(
        self,
        extracted: ExtractorOutput,
        a: JudgeOutput,
        b: JudgeOutput,
        fail: bool = False,
        developers: dict[str, str] | None = None,
        returned: dict[str, str] | None = None,
    ) -> None:
        self.by_role: dict[str, BaseModel] = {
            "news_extractor": extracted,
            "news_judge_a": a,
            "news_judge_b": b,
        }
        self.fail = fail
        self.developers = developers or DEVELOPERS
        self.returned = returned or {}

    async def structured(self, *, role: str, schema: type[Any], **_: Any) -> Generation[Any]:
        if self.fail and role != "news_extractor":
            raise LlmUnavailableError("down")
        output = self.by_role[role]
        assert isinstance(output, schema)
        return Generation(
            output,
            "m/x",
            self.returned.get(role, f"{self.developers[role]}/x"),
            "p",
            f"g-{role}",
            LlmUsage(prompt_tokens=1, completion_tokens=1),
            "0" * 64,
            "0" * 64,
            False,
        )


class ClosedGate:
    def __init__(self, closed: str) -> None:
        self.closed = closed

    async def closed_reason(self, role: str, config: RoleModelConfig) -> str | None:
        return "model_self_test_failed" if role == self.closed else None


def extracted(quote: str = "Binance will list Xyz Protocol (XYZ) on September 2.") -> ExtractorOutput:
    return ExtractorOutput(
        has_event=True,
        event_class="LISTING",
        event_slug="binance_spot_listing",
        direction="up",
        quote=quote,
        event_time=None,
    )


def judged(
    cls: str = "LISTING", direction: str = "up", tier: str = "T0", event_time: str | None = None
) -> JudgeOutput:
    return JudgeOutput(
        event_class=cls,
        direction=direction,
        hardness=0.9,
        novelty=0.9,
        verified_tier=tier,
        event_time=event_time,
        confidence=0.8,  # type: ignore[arg-type]
    )


OPENROUTER_ROLE = RoleModelConfig(model="vendor/model", temperature=0.0, max_tokens=800, timeout_s=30)
DEEPSEEK_ROLE = RoleModelConfig(
    gateway="deepseek", model="deepseek-flash", temperature=0.0, max_tokens=800, timeout_s=30
)


def run(
    llm: FakeLlm,
    evidence: OfficialEvidence | None = None,
    it: StoredItem | None = None,
    health: ClosedGate | None = None,
    judges: tuple[RoleModelConfig, RoleModelConfig] = (OPENROUTER_ROLE, OPENROUTER_ROLE),
) -> dict[str, Any] | None:
    roles = RoleConfigs(OPENROUTER_ROLE, *judges)
    judge = ItemJudge(llm, roles, config=CFG, sources=SOURCES, health=health)
    job = VerdictJob(it or item(), (COIN,), evidence or OfficialEvidence(), NOW, "news")
    return asyncio.run(judge.verdict(job))


def test_hallucinated_quote_drops_evidence() -> None:
    row = run(
        FakeLlm(extracted("Binance confirmed a 500 million dollar investment in XYZ."), judged(), judged())
    )
    assert row is not None
    assert (row["status"], row["quote_ok"], row["quote"]) == ("quote_failed", False, None)


def test_judge_disagreement_on_class_or_direction() -> None:
    row = run(FakeLlm(extracted(), judged("LISTING"), judged("RUMOR")))
    assert row is not None
    assert row["status"] == "disagree"
    assert {row["class_a"], row["class_b"]} == {"LISTING", "RUMOR"}
    row = run(FakeLlm(extracted(), judged("LISTING", "up"), judged("LISTING", "none")))
    assert row is not None
    assert row["status"] == "disagree"


def test_judge_tier_ignored_unconfirmed_listing_not_t0() -> None:
    row = run(FakeLlm(extracted(), judged(tier="T0"), judged(tier="T0")))
    assert row is not None
    assert row["status"] == "agreed"
    assert (row["official"], row["tier"]) == (False, "T1")


def test_fake_official_page_is_demoted() -> None:
    fake = item("https://www.binance.com/en/square/post/123")
    row = run(FakeLlm(extracted(), judged(), judged()), it=fake)
    assert row is not None
    assert (row["official"], row["tier"]) == (False, "T2")


def test_recorded_announcement_confirms_listing() -> None:
    ann = Announcement(
        exchange="binance",
        title="Binance Will List Xyz Protocol (XYZ)",
        url="https://www.binance.com/en/support/announcement/abc",
        catalog="listing",
        published_at=NOW,
        recorded_at=NOW,
    )
    row = run(FakeLlm(extracted(), judged(), judged()), OfficialEvidence(announcements=(ann,)))
    assert row is not None
    assert (row["official"], row["tier"]) == (True, "T0")
    assert row["detail"]["official_coins"] == [5]


def test_unavailable_models_store_nothing() -> None:
    assert run(FakeLlm(extracted(), judged(), judged(), fail=True)) is None


def test_judges_served_by_one_developer_are_not_two_opinions() -> None:
    same = {"news_extractor": "vendor-x", "news_judge_a": "vendor-a", "news_judge_b": "vendor-a"}
    row = run(FakeLlm(extracted(), judged(), judged(), developers=same))
    assert row is not None
    assert (row["status"], row["detail"]["failed_step"]) == ("llm_failed", "judge_developer")


def test_judges_both_on_deepseek_are_accepted_as_two_opinions() -> None:
    """Owner decision 2026-09-28: the config accepts both judges on DeepSeek, so the verdict must too."""
    served = {"news_judge_a": "deepseek-flash", "news_judge_b": "deepseek-v4-flash"}
    row = run(
        FakeLlm(extracted(), judged(), judged(), returned=served), judges=(DEEPSEEK_ROLE, DEEPSEEK_ROLE)
    )
    assert row is not None
    assert row["status"] == "agreed"


def test_a_deepseek_judge_and_an_openrouter_judge_served_by_deepseek_are_refused() -> None:
    served = {"news_judge_a": "deepseek-flash", "news_judge_b": "deepseek/deepseek-chat"}
    row = run(
        FakeLlm(extracted(), judged(), judged(), returned=served), judges=(DEEPSEEK_ROLE, OPENROUTER_ROLE)
    )
    assert row is not None
    assert (row["status"], row["detail"]["failed_step"]) == ("llm_failed", "judge_developer")
    other = {"news_judge_a": "deepseek-flash", "news_judge_b": "vendor-b/x"}
    row = run(
        FakeLlm(extracted(), judged(), judged(), returned=other), judges=(DEEPSEEK_ROLE, OPENROUTER_ROLE)
    )
    assert row is not None
    assert row["status"] == "agreed"


def test_poisoned_event_times_are_ignored_not_raised() -> None:
    for poison in ("9999-12-31T23:59:59-01:00", "0001-01-01T00:00:00+01:00", "1" * 30, "2016-01-01"):
        row = run(FakeLlm(extracted(), judged(event_time=poison), judged(event_time=poison)))
        assert row is not None
        assert (row["status"], row["event_time"]) == ("agreed", None)
    row = run(FakeLlm(extracted(), judged(event_time="2026-09-02T08:00:00Z"), judged()))
    assert row is not None
    assert row["event_time"] == datetime(2026, 9, 2, 8, tzinfo=UTC)


def test_unexpected_failure_on_one_item_is_its_own_failed_row() -> None:
    def broken(_coin_id: int, _start: datetime, _end: datetime) -> None:
        raise ValueError("corrupt capture")

    exploit = extracted("Binance will list Xyz Protocol (XYZ) on September 2.")
    llm = FakeLlm(exploit, judged("EXPLOIT", "down"), judged("EXPLOIT", "down"))
    row = run(llm, OfficialEvidence(onchain=broken))
    assert row is not None
    assert (row["status"], row["detail"]["failed_step"]) == ("llm_failed", "internal")


def test_closed_role_judges_nothing() -> None:
    assert run(FakeLlm(extracted(), judged(), judged()), health=ClosedGate("news_judge_b")) is None
    assert run(FakeLlm(extracted(), judged(), judged()), health=ClosedGate("other")) is not None


def test_old_binance_announcement_does_not_confirm_a_new_rumor() -> None:
    old = Announcement(
        exchange="binance",
        title="Binance Will List Xyz Protocol (XYZ)",
        url="https://www.binance.com/en/support/announcement/old",
        catalog="listing",
        published_at=NOW - timedelta(days=10),
        recorded_at=NOW - timedelta(days=10),
    )
    quote = "Coinbase will list Xyz Protocol (XYZ) on September 2."
    evidence = OfficialEvidence(announcements=(old,))
    for summary in (quote, f"{quote} Binance listed it ten days ago."):
        coinbase = replace(item(), title="XYZ to list on Coinbase", summary=summary)
        row = run(FakeLlm(extracted(quote), judged(), judged()), evidence, it=coinbase)
        assert row is not None
        assert (row["status"], row["official"]) == ("agreed", False)
