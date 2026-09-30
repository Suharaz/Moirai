"""Phase 11 go-live: the calibration / evaluation split, the one-shot final G4 evaluation and the scorer's
read grants for gate G4.

- `golive_eval_splits`: one row per (target type, generation), written by `hdt_scorer`, read by the console
  and the config-api live gate. The scorer may insert only `target_type`, `eval_start`, `decided_by` and
  `reason` (column-level grant) and can never update or delete a row. `created_at` is the server clock at the
  insert itself (`clock_timestamp()`, not the transaction start, so a held transaction cannot backdate it)
  and `eval_start >= created_at` is checked, so a split can never be set in the past, after reading any
  result. The trigger `golive_split_generation` assigns the next generation of the target type and refuses
  it while the previous generation has no final evaluation: a split is never moved, but once its final is
  recorded (passed or failed) a new split starts a new evaluation (the re-evaluation path after a `models`
  or `council` change).
- `golive_g4_finals`: the final G4 evaluation of each split (`python -m hdt.golive.final run`), one row per
  (target type, generation), inserted by `hdt_scorer` with the `gate_flags` row in the same transaction. A
  second final evaluation of the same split fails on the primary key, and the composite foreign key ties the
  row to that exact split (target type, generation, eval_start). A gate pass needs every target type to pass
  (`gate_passed` implies `passed`). `pins` holds the `models` / `council` config version ids, the agent pins
  and the static config digests of the window; the config-api live gate reads the newest final evaluation.
- The trigger `gate_flags_g4_final` accepts a `G4` gate flag only when it repeats the outcome of the newest
  final evaluation (same `evidence_ref`, `passed` equal to its `gate_passed`): a flag can never override a
  final, and the live gate does not read flags at all.
- `hdt_scorer` reads `orders` and `algo_orders` of the ledger (G4 counts independent entry orders of the
  paper account; paper vs live measures fill rate, entry price and stop slippage) and `equity_snapshots`
  (the whole-account equity curve of G4, unrealized PnL included). The scorer already reads `fills`,
  `positions` and `cash_flows` (0005), writes `gate_progress` (0010) and inserts `gate_flags` (0001).

Revision ID: 0011_golive
Revises: 0010_scoring
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision: str = "0011_golive"
down_revision: str | None = "0010_scoring"
branch_labels = None
depends_on = None

TS = sa.DateTime(timezone=True)
TARGETS = "target_type IN ('RAW_12H', 'RESID_12H')"
GRANTS = (
    "SELECT ON golive_eval_splits TO hdt_scorer, hdt_configapi",
    "INSERT (target_type, eval_start, decided_by, reason) ON golive_eval_splits TO hdt_scorer",
    "SELECT ON golive_eval_splits, golive_g4_finals TO hdt_console_ro",
    "SELECT ON golive_g4_finals TO hdt_configapi",
    "SELECT ON golive_g4_finals TO hdt_scorer",
    "INSERT (target_type, generation, eval_start, eval_end, passed, gate_passed, evidence_ref, result, pins) "
    "ON golive_g4_finals TO hdt_scorer",
    "SELECT ON orders, algo_orders, equity_snapshots TO hdt_scorer",
)
REVOKES = (
    "SELECT ON orders, algo_orders, equity_snapshots FROM hdt_scorer",
    "SELECT, INSERT ON golive_g4_finals FROM hdt_scorer",
    "SELECT ON golive_g4_finals FROM hdt_configapi",
    "SELECT ON golive_eval_splits, golive_g4_finals FROM hdt_console_ro",
    "SELECT, INSERT ON golive_eval_splits FROM hdt_scorer, hdt_configapi",
)
SPLIT_GENERATION = """
CREATE FUNCTION golive_split_generation() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    last_generation integer;
BEGIN
    SELECT max(generation) INTO last_generation FROM golive_eval_splits
        WHERE target_type = NEW.target_type;
    IF NEW.generation IS NULL THEN
        NEW.generation := coalesce(last_generation, 0) + 1;
    END IF;
    IF NEW.generation <> coalesce(last_generation, 0) + 1 THEN
        RAISE EXCEPTION 'golive_eval_splits: % takes generation %, not %', NEW.target_type,
            coalesce(last_generation, 0) + 1, NEW.generation USING ERRCODE = 'check_violation';
    END IF;
    IF last_generation IS NOT NULL AND NOT EXISTS (
        SELECT 1 FROM golive_g4_finals
        WHERE target_type = NEW.target_type AND generation = last_generation
    ) THEN
        RAISE EXCEPTION 'golive_eval_splits: split % of % has no final evaluation: a split is never moved',
            last_generation, NEW.target_type USING ERRCODE = 'check_violation';
    END IF;
    RETURN NEW;
END
$$
"""
G4_FLAG_FINAL = """
CREATE FUNCTION gate_flags_g4_final() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    run_rows integer;
    run_matching integer;
    run_at timestamptz;
BEGIN
    SELECT count(*), count(*) FILTER (WHERE gate_passed = NEW.passed), max(evaluated_at)
        INTO run_rows, run_matching, run_at
        FROM golive_g4_finals WHERE evidence_ref = NEW.evidence_ref;
    IF run_rows = 0 OR run_matching <> run_rows THEN
        RAISE EXCEPTION 'gate_flags: a G4 flag must repeat the outcome of its final evaluation (%)',
            NEW.evidence_ref USING ERRCODE = 'check_violation';
    END IF;
    IF EXISTS (
        SELECT 1 FROM golive_g4_finals WHERE evidence_ref <> NEW.evidence_ref AND evaluated_at > run_at
    ) THEN
        RAISE EXCEPTION 'gate_flags: % is not the newest final G4 evaluation', NEW.evidence_ref
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NEW;
END
$$
"""


def upgrade() -> None:
    op.create_table(
        "golive_eval_splits",
        sa.Column("target_type", sa.Text(), nullable=False),
        sa.Column("generation", sa.Integer(), nullable=False),
        sa.Column("eval_start", TS, nullable=False),
        sa.Column("decided_by", sa.Text(), nullable=False),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column("created_at", TS, server_default=sa.text("clock_timestamp()"), nullable=False),
        sa.CheckConstraint(TARGETS, name=op.f("ck_golive_eval_splits_target_type")),
        sa.CheckConstraint("generation >= 1", name=op.f("ck_golive_eval_splits_generation")),
        sa.CheckConstraint(
            "eval_start >= created_at", name=op.f("ck_golive_eval_splits_eval_start_not_past")
        ),
        sa.PrimaryKeyConstraint("target_type", "generation", name=op.f("pk_golive_eval_splits")),
        sa.UniqueConstraint(
            "target_type", "generation", "eval_start", name=op.f("uq_golive_eval_splits_target_type")
        ),
    )
    op.create_table(
        "golive_g4_finals",
        sa.Column("target_type", sa.Text(), nullable=False),
        sa.Column("generation", sa.Integer(), nullable=False),
        sa.Column("eval_start", TS, nullable=False),
        sa.Column("eval_end", TS, nullable=False),
        sa.Column("passed", sa.Boolean(), nullable=False),
        sa.Column("gate_passed", sa.Boolean(), nullable=False),
        sa.Column("evidence_ref", sa.Text(), nullable=False),
        sa.Column("result", JSONB, nullable=False),
        sa.Column("pins", JSONB(none_as_null=True), nullable=True),
        sa.Column("evaluated_at", TS, server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
        sa.CheckConstraint(TARGETS, name=op.f("ck_golive_g4_finals_target_type")),
        sa.CheckConstraint("eval_end > eval_start", name=op.f("ck_golive_g4_finals_window")),
        sa.CheckConstraint("passed OR NOT gate_passed", name=op.f("ck_golive_g4_finals_gate_needs_target")),
        sa.ForeignKeyConstraint(
            ["target_type", "generation", "eval_start"],
            [
                "golive_eval_splits.target_type",
                "golive_eval_splits.generation",
                "golive_eval_splits.eval_start",
            ],
            name=op.f("fk_golive_g4_finals_target_type_golive_eval_splits"),
        ),
        sa.PrimaryKeyConstraint("target_type", "generation", name=op.f("pk_golive_g4_finals")),
    )
    op.execute(SPLIT_GENERATION)
    op.execute(
        "CREATE TRIGGER golive_split_generation BEFORE INSERT ON golive_eval_splits "
        "FOR EACH ROW EXECUTE FUNCTION golive_split_generation()"
    )
    op.execute(G4_FLAG_FINAL)
    op.execute(
        "CREATE TRIGGER gate_flags_g4_final BEFORE INSERT ON gate_flags "
        "FOR EACH ROW WHEN (NEW.gate = 'G4') EXECUTE FUNCTION gate_flags_g4_final()"
    )
    for grant in GRANTS:
        op.execute(f"GRANT {grant}")


def downgrade() -> None:
    for revoke in REVOKES:
        op.execute(f"REVOKE {revoke}")
    op.execute("DROP TRIGGER gate_flags_g4_final ON gate_flags")
    op.execute("DROP FUNCTION gate_flags_g4_final()")
    op.drop_table("golive_g4_finals")
    op.execute("DROP TRIGGER golive_split_generation ON golive_eval_splits")
    op.execute("DROP FUNCTION golive_split_generation()")
    op.drop_table("golive_eval_splits")
