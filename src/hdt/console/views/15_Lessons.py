"""Human review of shadow lessons after A/B evidence is recorded."""

from __future__ import annotations

import streamlit as st

from hdt.console import auth
from hdt.console import components as ui
from hdt.console.api import ApiError, describe_error
from hdt.console.readmodels import Available
from hdt.console.views._shared import heading, panel

heading("Lessons", "Only measured shadow lessons may be approved; retirement is auditable.")
board = ui.read_models().lessons()
if not isinstance(board, Available):
    panel("Lesson database", board)
    st.stop()
client = auth.get_client()
try:
    review_enabled = client.lessons_route_available()
except ApiError as exc:
    st.error(describe_error(exc))
    review_enabled = False
if not review_enabled:
    st.warning("Lesson review API is unavailable; read-only history remains visible.")
for title, group in (
    ("Awaiting human review", board.value.awaiting),
    ("Shadow evaluation", board.value.in_shadow),
    ("Active lessons", board.value.active),
):
    panel(title, Available(group))
    for lesson in group:
        with st.expander(f"{lesson.agent} | {lesson.title} | {lesson.lesson_id}"):
            st.write("When", lesson.when_text)
            st.write("Observation", lesson.observation)
            st.write("Adjustment", lesson.adjustment)
            st.write(
                {
                    "A/B sample": lesson.ab_n,
                    "log loss with": lesson.ab_logloss_with,
                    "log loss without": lesson.ab_logloss_without,
                    "CI": [lesson.ab_ci_low, lesson.ab_ci_high],
                }
            )
            if not review_enabled or not (lesson.awaiting_review or lesson.state == "active"):
                continue
            note = st.text_input("Decision rationale", key=f"lesson-note-{lesson.lesson_id}")
            action = "Approve" if lesson.awaiting_review else "Retire"
            if st.button(action, key=f"lesson-{action}-{lesson.lesson_id}", disabled=not note.strip()):
                try:
                    if action == "Approve":
                        client.approve_lesson(lesson.lesson_id, note.strip(), expected_state=lesson.state)
                    else:
                        client.retire_lesson(lesson.lesson_id, note.strip(), expected_state=lesson.state)
                except ApiError as exc:
                    st.error(describe_error(exc))
                else:
                    ui.notify(f"Lesson {lesson.lesson_id}: {action.lower()}d")
                    st.rerun()
