"""Long-term agent memory (phase 04): bitemporal records on the LangGraph Store (Postgres).

Namespaces are `(agent, kind)` with kind `episodic` (decision episodes, their outcomes, known news events)
or `lessons` (the append-only lesson event chain). Every record carries `known_at`; every read is bounded
by an `as_of` and returns only records with `known_at < as_of`, so a replay never sees what was learned
later. No embeddings: duplicates are found by exact fingerprint and MinHash (`hdt.memory.dedupe`).
"""
