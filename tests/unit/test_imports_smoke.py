"""Pinned-API smoke test: fails loudly if a dependency upgrade moves an API later phases rely on."""

from __future__ import annotations

import importlib.metadata as md


def test_pinned_major_versions() -> None:
    assert md.version("langgraph").startswith("1.2.")
    assert md.version("langgraph-checkpoint-postgres").startswith("3.1.")
    assert md.version("langgraph-checkpoint").startswith("4.")
    assert md.version("ta-lib").startswith("0.8.")
    assert md.version("langfuse").startswith("4.")


def test_langgraph_apis_used_by_the_council_exist() -> None:
    from langgraph.checkpoint.postgres import PostgresSaver
    from langgraph.graph import END, START, StateGraph
    from langgraph.store.postgres import PostgresStore
    from langgraph.types import Send

    assert all(obj is not None for obj in (PostgresSaver, PostgresStore, StateGraph, Send, START, END))


def test_talib_computes_indicators() -> None:
    import numpy as np
    import talib

    close = np.linspace(1.0, 2.0, 50)
    rsi = talib.RSI(close, timeperiod=14)
    assert rsi[-1] > 99.0  # monotonic rise -> RSI saturates
