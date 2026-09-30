"""Human label set of the news pipeline (phase 07): label ingested items in `news_labels`.

Run: `HDT_PG_DSN=<hdt_news DSN> HDT_LABELER=<name> streamlit run src/hdt/ops/labeling_app.py`
(the role needs SELECT on `news_items` and SELECT / INSERT / UPDATE on `news_labels`; hdt_news has
exactly that). Items are sampled deterministically (md5 order of the item id) from a chosen ingestion
window, duplicates included, so the label set also measures leaked duplicates. The labeler never sees the
pipeline's own verdict (no anchoring). Each label: event class, direction, hard / soft, duplicate, note.
Measure afterwards with `python -m hdt.news.evaluate`.
"""

from __future__ import annotations

import os
from datetime import UTC, date, datetime, time, timedelta
from typing import Any, Final

import sqlalchemy as sa
import streamlit as st
from sqlalchemy.dialects.postgresql import insert

from hdt.core.clock import utcnow
from hdt.db.models.news import DIRECTIONS, NewsItemRow, NewsLabelRow
from hdt.db.session import make_engine
from hdt.news.classes import EVENT_CLASSES

TARGET_LABELS: Final[int] = 300
ITEMS = NewsItemRow.__table__
LABELS: sa.Table = NewsLabelRow.__table__  # type: ignore[assignment]


@st.cache_resource
def engine() -> sa.Engine:
    return make_engine(pool_size=2)


def sample(conn: sa.Connection, start: datetime, end: datetime, size: int) -> list[Any]:
    return list(
        conn.execute(
            sa.select(ITEMS)
            .where(ITEMS.c.ingested_at >= start, ITEMS.c.ingested_at < end)
            .order_by(sa.func.md5(ITEMS.c.item_id))
            .limit(size)
        )
    )


def labeled_ids(conn: sa.Connection, labeler: str) -> set[str]:
    return set(conn.execute(sa.select(LABELS.c.item_id).where(LABELS.c.labeler == labeler)).scalars())


def save_label(conn: sa.Connection, values: dict[str, Any]) -> None:
    stmt = insert(LABELS).values(**values)
    conn.execute(
        stmt.on_conflict_do_update(
            index_elements=[LABELS.c.item_id, LABELS.c.labeler],
            set_={
                k: stmt.excluded[k]
                for k in ("labeled_at", "event_class", "direction", "hardness", "duplicate", "note")
            },
        )
    )


def main() -> None:
    st.set_page_config(page_title="News label set", layout="wide")
    st.title("News label set")
    labeler = st.sidebar.text_input("Labeler", value=os.environ.get("HDT_LABELER", "")).strip()
    today = utcnow().date()
    first: date = st.sidebar.date_input("Ingested from (UTC)", value=today - timedelta(days=14))
    last: date = st.sidebar.date_input("Ingested until (UTC, inclusive)", value=today)
    size = int(
        st.sidebar.number_input("Sample size", min_value=50, max_value=2000, value=TARGET_LABELS, step=50)
    )
    if not labeler:
        st.info("Enter your labeler name in the sidebar.")
        return
    start = datetime.combine(first, time.min, tzinfo=UTC)
    end = datetime.combine(last, time.min, tzinfo=UTC) + timedelta(days=1)
    with engine().connect() as conn:
        items = sample(conn, start, end, size)
        done = labeled_ids(conn, labeler)
    st.sidebar.metric("Labeled", f"{len(done & {i.item_id for i in items})} / {len(items)}")
    todo = [i for i in items if i.item_id not in done]
    if not items:
        st.warning("No ingested items in this window.")
        return
    if not todo:
        st.success("Every sampled item is labeled. Run `python -m hdt.news.evaluate` to measure.")
        return
    item = todo[0]
    st.subheader(item.title)
    st.caption(f"{item.source_name} | {item.domain} | published {item.published_at} | item {item.item_id}")
    st.markdown(f"[Original article]({item.url})")
    if item.summary:
        st.write(item.summary)
    if item.content:
        with st.expander("Stored text"):
            st.text(item.content)
    with st.form(f"label-{item.item_id}", clear_on_submit=True):
        event_class = st.selectbox("Event class", EVENT_CLASSES, index=EVENT_CLASSES.index("NO_EVENT"))
        direction = st.radio("Direction for the coin's price", DIRECTIONS, index=2, horizontal=True)
        hardness = st.radio("Evidence", ("hard", "soft"), index=1, horizontal=True)
        duplicate = st.checkbox("Duplicate of an item already seen (same story)")
        note = st.text_input("Note (optional)")
        if st.form_submit_button("Save and next"):
            with engine().begin() as conn:
                save_label(
                    conn,
                    {
                        "item_id": item.item_id,
                        "labeler": labeler,
                        "labeled_at": utcnow(),
                        "event_class": event_class,
                        "direction": direction,
                        "hardness": hardness,
                        "duplicate": duplicate,
                        "note": note.strip() or None,
                    },
                )
            st.rerun()


if __name__ == "__main__":
    main()
