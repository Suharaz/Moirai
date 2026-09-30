"""Signed figures on the public dashboard: text, colour class and arrow follow the displayed value."""

from __future__ import annotations

import pytest

from hdt.public_dashboard import ui


@pytest.mark.parametrize(
    ("value", "text", "cls", "arrow"),
    [
        (0.0049, "+0.00", "", None),
        (-0.0049, "+0.00", "", None),
        (-0.0, "+0.00", "", None),
        (0.0051, "+0.01", "up", "up-a"),
        (-0.0051, "-0.01", "down", "down-a"),
        (-1234.5, "-1,234.50", "down", "down-a"),
    ],
)
def test_sign_class_and_arrow_agree_with_the_rounded_value(
    value: float, text: str, cls: str, arrow: str | None
) -> None:
    assert ui.fmt_signed(value) == text
    assert ui.signed_span(value) == f'<span class="num {cls}">{text}</span>'
    body, kpi_cls = ui.pnl_html(value)
    assert kpi_cls == cls
    assert body.endswith(text)
    assert (f"i-{arrow}" in body) if arrow else ("i-up-a" not in body and "i-down-a" not in body)


def test_missing_value_is_a_dash() -> None:
    assert ui.fmt_signed(None) == "-"
    assert ui.pnl_html(None) == ("-", "")
