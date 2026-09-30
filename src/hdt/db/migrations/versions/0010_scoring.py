"""Phase 08 scoring and learning tables, the `lessons` read model and their grants.

- `scoring_labels`, `scored_forecasts`, `weight_history`, `calibration_bins`, `calibration_stats`,
  `scoring_params`, `stacking_models`, `claims_audit_flags`, `reflection_runs`: written by `hdt_scorer`
  (idempotent upserts from the full labeled history), read by the console; the public publisher and the
  Telegram bot read their published subset.
- `scoring_params`, `stacking_models` and `claims_audit_flags` are read by `hdt_council`
  (`hdt.scoring.params.PgParamsSource` applies open flags and new agent versions at load).
- `lesson_shadow_forecasts`: `hdt_council` inserts inside the decision transaction; the scorer reads it for
  the lesson A/B.
- `gate_progress`: created here with the scorer as writer; the G4 writer arrives in phase 11.
- `lessons`: a view over the append-only lesson events in `store` (`hdt.memory.lessons`), folded with the
  same transition rules as `fold_lessons`, so a config-api approval is visible without a scorer run.

Revision ID: 0010_scoring
Revises: 0009_council
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0010_scoring"
down_revision: str | None = "0009_council"
branch_labels = None
depends_on = None

TS = sa.DateTime(timezone=True)
JSONB = postgresql.JSONB(astext_type=sa.Text())
TARGET_TYPES = "target_type IN ('RAW_12H', 'RESID_12H')"
AGENTS = "'crowding', 'technical', 'micro', 'fundamental', 'news', 'macro'"
AGENT = f"agent IN ({AGENTS})"
AGENT_OR_POOLED = f"agent IN ({AGENTS}, 'pooled')"
REFLECTION_STATUSES = (
    "status IN ('proposed', 'skipped_no_wrong_agent', 'skipped_shadow_busy', 'skipped_duplicate', "
    "'invalid_output', 'invalid_template')"
)
SCORER_TABLES = (
    "scoring_labels",
    "scored_forecasts",
    "weight_history",
    "calibration_bins",
    "calibration_stats",
    "scoring_params",
    "stacking_models",
    "claims_audit_flags",
    "reflection_runs",
    "gate_progress",
)
TABLES = (*SCORER_TABLES, "lesson_shadow_forecasts")
GRANTS = (
    *(f"SELECT, INSERT, UPDATE ON {table} TO hdt_scorer" for table in SCORER_TABLES),
    "SELECT ON lesson_shadow_forecasts TO hdt_scorer",
    "SELECT, INSERT ON lesson_shadow_forecasts TO hdt_council",
    "SELECT ON scoring_params, stacking_models, claims_audit_flags TO hdt_council",
    f"SELECT ON {', '.join(TABLES)} TO hdt_console_ro",
    "SELECT ON scored_forecasts, weight_history, calibration_bins, calibration_stats, gate_progress "
    "TO hdt_publisher_ro",
    "SELECT ON weight_history, scored_forecasts TO hdt_telegram",
    "SELECT ON lessons TO hdt_scorer, hdt_configapi, hdt_console_ro",
)
REVOKES = (
    "SELECT ON lessons FROM hdt_scorer, hdt_configapi, hdt_console_ro",
    "SELECT ON weight_history, scored_forecasts FROM hdt_telegram",
    "SELECT ON scored_forecasts, weight_history, calibration_bins, calibration_stats, gate_progress "
    "FROM hdt_publisher_ro",
    f"SELECT ON {', '.join(TABLES)} FROM hdt_console_ro",  # noqa: S608 - constant table names
    "SELECT ON scoring_params, stacking_models, claims_audit_flags FROM hdt_council",
    "SELECT, INSERT ON lesson_shadow_forecasts FROM hdt_council",
    "SELECT ON lesson_shadow_forecasts FROM hdt_scorer",
    *(f"SELECT, INSERT, UPDATE ON {table} FROM hdt_scorer" for table in SCORER_TABLES),
)
# The fold of `hdt.memory.lessons.fold_lessons` in SQL. Record keys make each lesson hold at most one
# event per kind (`:proposed`, `:ab`, `:review`, `:retired`); events are ordered by (known_at_key, key).
# - ab_completed counts while the lesson is still in shadow (before any rejection or retirement);
# - approved counts after an improving A/B (n >= 50, with < without, ci_high < 0), before any retirement;
# - rejected counts before any retirement; retirement then counts only if no rejection came first.
LESSONS_VIEW = """
CREATE VIEW lessons AS
WITH ev AS (
    SELECT s.value->>'lesson_id' AS lesson_id,
           s.value->>'agent' AS agent,
           s.value->>'action' AS action,
           s.value->>'actor' AS actor,
           s.value->>'note' AS note,
           s.value->'template' AS template,
           s.value->'ab' AS ab,
           (s.value->>'known_at')::timestamptz AS known_at,
           s.value->>'known_at_key' AS kk,
           s.key AS key
    FROM store s
    WHERE s.prefix ~ '^(crowding|technical|micro|fundamental|news|macro)\\.lessons$'
      AND s.value->>'record_type' = 'lesson_event'
      AND split_part(s.prefix, '.', 1) = s.value->>'agent'
      AND s.key = (s.value->>'lesson_id') || ':' || CASE s.value->>'action'
          WHEN 'proposed' THEN 'proposed'
          WHEN 'ab_completed' THEN 'ab'
          WHEN 'retired' THEN 'retired'
          ELSE 'review' END
),
chain AS (
    SELECT p.lesson_id, p.agent, p.template, p.known_at AS proposed_at,
           a.ab, a.known_at AS ab_at,
           v.action AS review_action, v.actor AS review_actor, v.note AS review_text, v.known_at AS review_at,
           t.note AS retire_note, t.known_at AS retire_at,
           (v.action = 'rejected' AND (t.key IS NULL OR (v.kk, v.key) < (t.kk, t.key))) IS TRUE AS rejected,
           (a.key IS NOT NULL
            AND (t.key IS NULL OR (a.kk, a.key) < (t.kk, t.key))
            AND (v.key IS NULL OR v.action <> 'rejected' OR (a.kk, a.key) < (v.kk, v.key))) AS ab_ok,
           a.kk AS a_kk, a.key AS a_key, v.kk AS v_kk, v.key AS v_key, t.kk AS t_kk, t.key AS t_key
    FROM ev p
    LEFT JOIN ev a
        ON a.lesson_id = p.lesson_id AND a.action = 'ab_completed' AND (a.kk, a.key) > (p.kk, p.key)
    LEFT JOIN ev v ON v.lesson_id = p.lesson_id AND v.action IN ('approved', 'rejected')
         AND (v.kk, v.key) > (p.kk, p.key)
    LEFT JOIN ev t ON t.lesson_id = p.lesson_id AND t.action = 'retired' AND (t.kk, t.key) > (p.kk, p.key)
    WHERE p.action = 'proposed'
),
folded AS (
    SELECT c.*,
           (c.review_action = 'approved' AND c.ab_ok
            AND (c.ab->>'n')::int >= 50
            AND (c.ab->>'logloss_with')::float8 < (c.ab->>'logloss_without')::float8
            AND (c.ab->>'ci_high')::float8 < 0
            AND (c.a_kk, c.a_key) < (c.v_kk, c.v_key)
            AND (c.t_key IS NULL OR (c.v_kk, c.v_key) < (c.t_kk, c.t_key))) IS TRUE AS approved
    FROM chain c
)
SELECT f.lesson_id,
       f.agent,
       f.template->>'title' AS title,
       CASE WHEN f.rejected OR f.retire_at IS NOT NULL THEN 'retired'
            WHEN f.approved THEN 'active'
            ELSE 'shadow' END AS state,
       f.template->>'when_text' AS when_text,
       f.template->>'observation' AS observation,
       f.template->>'adjustment' AS adjustment,
       f.proposed_at AS created_at,
       f.proposed_at AS shadow_since,
       CASE WHEN f.ab_ok THEN f.ab_at END AS ab_completed_at,
       CASE WHEN f.ab_ok THEN (f.ab->>'n')::int END AS ab_n,
       CASE WHEN f.ab_ok THEN (f.ab->>'logloss_with')::float8 END AS ab_logloss_with,
       CASE WHEN f.ab_ok THEN (f.ab->>'logloss_without')::float8 END AS ab_logloss_without,
       CASE WHEN f.ab_ok THEN (f.ab->>'ci_low')::float8 END AS ab_ci_low,
       CASE WHEN f.ab_ok THEN (f.ab->>'ci_high')::float8 END AS ab_ci_high,
       CASE WHEN f.rejected OR f.approved THEN f.review_actor END AS decided_by,
       CASE WHEN f.rejected OR f.approved THEN f.review_at END AS decided_at,
       CASE WHEN f.rejected THEN f.review_text
            WHEN f.retire_at IS NOT NULL
                THEN coalesce(f.retire_note, CASE WHEN f.approved THEN f.review_text END)
            WHEN f.approved THEN f.review_text END AS review_note,
       CASE WHEN f.rejected THEN f.review_at ELSE f.retire_at END AS retired_at
FROM folded f
"""


def upgrade() -> None:
    op.create_table(
        "scoring_labels",
        sa.Column("event_id", sa.Text(), nullable=False),
        sa.Column("coin_id", sa.Integer(), nullable=False),
        sa.Column("symbol", sa.Text(), nullable=False),
        sa.Column("as_of", TS, nullable=False),
        sa.Column("target_type", sa.Text(), nullable=False),
        sa.Column("label_spec_version", sa.Text(), nullable=False),
        sa.Column("horizon_h", sa.Integer(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("y", sa.SmallInteger(), nullable=True),
        sa.Column("label_value", sa.Float(), nullable=True),
        sa.Column("coin_return", sa.Float(), nullable=True),
        sa.Column("btc_return", sa.Float(), nullable=True),
        sa.Column("beta_btc", sa.Float(), nullable=True),
        sa.Column("atr", sa.Float(), nullable=True),
        sa.Column("regime", sa.Text(), nullable=True),
        sa.Column("barrier", sa.SmallInteger(), nullable=True),
        sa.Column("barrier_y", sa.SmallInteger(), nullable=True),
        sa.Column("resolved_at", TS, nullable=False),
        sa.Column("outcomes_recorded_at", TS, nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.CheckConstraint(TARGET_TYPES, name=op.f("ck_scoring_labels_target_type")),
        sa.CheckConstraint(
            "status IN ('resolved', 'missing', 'error')", name=op.f("ck_scoring_labels_status")
        ),
        sa.CheckConstraint(
            "(status = 'resolved') = (y IS NOT NULL)", name=op.f("ck_scoring_labels_resolved_has_y")
        ),
        sa.CheckConstraint(
            "(status = 'error') = (error IS NOT NULL)", name=op.f("ck_scoring_labels_error_has_reason")
        ),
        sa.CheckConstraint("y IS NULL OR y IN (0, 1)", name=op.f("ck_scoring_labels_y")),
        sa.CheckConstraint(
            "barrier IS NULL OR barrier IN (-1, 0, 1)", name=op.f("ck_scoring_labels_barrier")
        ),
        sa.CheckConstraint(
            "barrier_y IS NULL OR barrier_y IN (0, 1)", name=op.f("ck_scoring_labels_barrier_y")
        ),
        sa.PrimaryKeyConstraint("event_id", name=op.f("pk_scoring_labels")),
    )
    op.create_index(
        "ix_scoring_labels_target",
        "scoring_labels",
        ["target_type", "label_spec_version", "as_of"],
        unique=False,
    )
    op.create_table(
        "scored_forecasts",
        sa.Column("event_id", sa.Text(), nullable=False),
        sa.Column("agent", sa.Text(), nullable=False),
        sa.Column("target_type", sa.Text(), nullable=False),
        sa.Column("label_spec_version", sa.Text(), nullable=False),
        sa.Column("coin_id", sa.Integer(), nullable=False),
        sa.Column("as_of", TS, nullable=False),
        sa.Column("agent_version", sa.Integer(), nullable=True),
        sa.Column("p", sa.Float(), nullable=False),
        sa.Column("p_model", sa.Float(), nullable=True),
        sa.Column("label", sa.Text(), nullable=False),
        sa.Column("y", sa.SmallInteger(), nullable=False),
        sa.Column("hit", sa.Boolean(), nullable=False),
        sa.Column("log_loss", sa.Float(), nullable=False),
        sa.Column("regime", sa.Text(), nullable=True),
        sa.Column("scored_at", TS, nullable=False),
        sa.CheckConstraint(TARGET_TYPES, name=op.f("ck_scored_forecasts_target_type")),
        sa.CheckConstraint(AGENT_OR_POOLED, name=op.f("ck_scored_forecasts_agent")),
        sa.CheckConstraint("label IN ('up', 'down')", name=op.f("ck_scored_forecasts_label")),
        sa.CheckConstraint("y IN (0, 1)", name=op.f("ck_scored_forecasts_y")),
        sa.CheckConstraint("p > 0 AND p < 1", name=op.f("ck_scored_forecasts_p")),
        sa.CheckConstraint("log_loss >= 0", name=op.f("ck_scored_forecasts_log_loss")),
        sa.PrimaryKeyConstraint("event_id", "agent", name=op.f("pk_scored_forecasts")),
    )
    op.create_index(
        "ix_scored_forecasts_target_scored_at", "scored_forecasts", ["target_type", "scored_at"], unique=False
    )
    op.create_table(
        "weight_history",
        sa.Column("target_type", sa.Text(), nullable=False),
        sa.Column("label_spec_version", sa.Text(), nullable=False),
        sa.Column("agent", sa.Text(), nullable=False),
        sa.Column("as_of", TS, nullable=False),
        sa.Column("agent_version", sa.Integer(), nullable=True),
        sa.Column("w", sa.Float(), nullable=False),
        sa.Column("w_capped", sa.Float(), nullable=False),
        sa.Column("a", sa.Float(), nullable=False),
        sa.Column("r", sa.Float(), nullable=False),
        sa.Column("coverage", sa.Float(), nullable=False),
        sa.Column("forecasts", sa.Integer(), nullable=False),
        sa.Column("events", sa.Integer(), nullable=False),
        sa.CheckConstraint(TARGET_TYPES, name=op.f("ck_weight_history_target_type")),
        sa.CheckConstraint(AGENT, name=op.f("ck_weight_history_agent")),
        sa.CheckConstraint(
            "w >= 0 AND w <= 1 AND w_capped >= 0 AND w_capped <= 1", name=op.f("ck_weight_history_w")
        ),
        sa.CheckConstraint("a >= 0 AND a <= 1 AND r >= 0 AND r <= 1", name=op.f("ck_weight_history_trust")),
        sa.CheckConstraint("coverage >= 0 AND coverage <= 1", name=op.f("ck_weight_history_coverage")),
        sa.PrimaryKeyConstraint(
            "target_type", "label_spec_version", "agent", "as_of", name=op.f("pk_weight_history")
        ),
    )
    op.create_index(
        "ix_weight_history_target_as_of", "weight_history", ["target_type", "as_of"], unique=False
    )
    op.create_table(
        "calibration_bins",
        sa.Column("target_type", sa.Text(), nullable=False),
        sa.Column("label_spec_version", sa.Text(), nullable=False),
        sa.Column("agent", sa.Text(), nullable=False),
        sa.Column("computed_at", TS, nullable=False),
        sa.Column("bin_index", sa.Integer(), nullable=False),
        sa.Column("bin_lo", sa.Float(), nullable=False),
        sa.Column("bin_hi", sa.Float(), nullable=False),
        sa.Column("n", sa.Integer(), nullable=False),
        sa.Column("mean_p", sa.Float(), nullable=False),
        sa.Column("observed_rate", sa.Float(), nullable=False),
        sa.CheckConstraint(TARGET_TYPES, name=op.f("ck_calibration_bins_target_type")),
        sa.CheckConstraint(AGENT_OR_POOLED, name=op.f("ck_calibration_bins_agent")),
        sa.CheckConstraint("n > 0", name=op.f("ck_calibration_bins_n")),
        sa.PrimaryKeyConstraint(
            "target_type",
            "label_spec_version",
            "agent",
            "computed_at",
            "bin_index",
            name=op.f("pk_calibration_bins"),
        ),
    )
    op.create_table(
        "calibration_stats",
        sa.Column("target_type", sa.Text(), nullable=False),
        sa.Column("label_spec_version", sa.Text(), nullable=False),
        sa.Column("agent", sa.Text(), nullable=False),
        sa.Column("computed_at", TS, nullable=False),
        sa.Column("spiegelhalter_z", sa.Float(), nullable=True),
        sa.Column("ece", sa.Float(), nullable=False),
        sa.Column("n", sa.Integer(), nullable=False),
        sa.CheckConstraint(TARGET_TYPES, name=op.f("ck_calibration_stats_target_type")),
        sa.CheckConstraint(AGENT_OR_POOLED, name=op.f("ck_calibration_stats_agent")),
        sa.PrimaryKeyConstraint(
            "target_type", "label_spec_version", "agent", "computed_at", name=op.f("pk_calibration_stats")
        ),
    )
    op.create_table(
        "scoring_params",
        sa.Column("target_type", sa.Text(), nullable=False),
        sa.Column("label_spec_version", sa.Text(), nullable=False),
        sa.Column("params_version", sa.Integer(), nullable=False),
        sa.Column("as_of", TS, nullable=False),
        sa.Column("payload", JSONB, nullable=False),
        sa.Column("payload_sha256", sa.Text(), nullable=False),
        sa.Column("created_at", TS, nullable=False),
        sa.CheckConstraint(TARGET_TYPES, name=op.f("ck_scoring_params_target_type")),
        sa.CheckConstraint("params_version >= 1", name=op.f("ck_scoring_params_params_version")),
        sa.PrimaryKeyConstraint(
            "target_type", "label_spec_version", "params_version", name=op.f("pk_scoring_params")
        ),
    )
    op.create_table(
        "stacking_models",
        sa.Column("target_type", sa.Text(), nullable=False),
        sa.Column("label_spec_version", sa.Text(), nullable=False),
        sa.Column("trained_through", TS, nullable=False),
        sa.Column("n", sa.Integer(), nullable=False),
        sa.Column("oos_n", sa.Integer(), nullable=False),
        sa.Column("oos_logloss_stack", sa.Float(), nullable=False),
        sa.Column("oos_logloss_pool", sa.Float(), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False),
        sa.Column("features", JSONB, nullable=False),
        sa.Column("model", sa.Text(), nullable=True),
        sa.Column("created_at", TS, nullable=False),
        sa.CheckConstraint(TARGET_TYPES, name=op.f("ck_stacking_models_target_type")),
        sa.CheckConstraint(
            "NOT enabled OR model IS NOT NULL", name=op.f("ck_stacking_models_enabled_has_model")
        ),
        sa.PrimaryKeyConstraint(
            "target_type", "label_spec_version", "trained_through", name=op.f("pk_stacking_models")
        ),
    )
    op.create_table(
        "claims_audit_flags",
        sa.Column("agent", sa.Text(), nullable=False),
        sa.Column("raised_at", TS, nullable=False),
        sa.Column("window_start", TS, nullable=False),
        sa.Column("window_end", TS, nullable=False),
        sa.Column("claims", sa.Integer(), nullable=False),
        sa.Column("rejected", sa.Integer(), nullable=False),
        sa.Column("rate", sa.Float(), nullable=False),
        sa.Column("reviewed_at", TS, nullable=True),
        sa.Column("reviewed_by", sa.Text(), nullable=True),
        sa.Column("review_note", sa.Text(), nullable=True),
        sa.CheckConstraint(AGENT, name=op.f("ck_claims_audit_flags_agent")),
        sa.CheckConstraint("rejected >= 0 AND claims >= rejected", name=op.f("ck_claims_audit_flags_counts")),
        sa.CheckConstraint(
            "(reviewed_at IS NULL) = (reviewed_by IS NULL)", name=op.f("ck_claims_audit_flags_review")
        ),
        sa.PrimaryKeyConstraint("agent", "raised_at", name=op.f("pk_claims_audit_flags")),
    )
    op.create_index(
        "uq_claims_audit_flags_open",
        "claims_audit_flags",
        ["agent"],
        unique=True,
        postgresql_where=sa.text("reviewed_at IS NULL"),
    )
    op.create_table(
        "reflection_runs",
        sa.Column("event_id", sa.Text(), nullable=False),
        sa.Column("agent", sa.Text(), nullable=True),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("lesson_id", sa.Text(), nullable=True),
        sa.Column("detail", sa.Text(), nullable=True),
        sa.Column("ran_at", TS, nullable=False),
        sa.CheckConstraint(REFLECTION_STATUSES, name=op.f("ck_reflection_runs_status")),
        sa.CheckConstraint(f"agent IS NULL OR {AGENT}", name=op.f("ck_reflection_runs_agent")),
        sa.CheckConstraint(
            "(status = 'proposed') = (lesson_id IS NOT NULL)", name=op.f("ck_reflection_runs_lesson")
        ),
        sa.PrimaryKeyConstraint("event_id", name=op.f("pk_reflection_runs")),
    )
    op.create_table(
        "gate_progress",
        sa.Column("gate", sa.Text(), nullable=False),
        sa.Column("check_key", sa.Text(), nullable=False),
        sa.Column("label", sa.Text(), nullable=False),
        sa.Column("value", sa.Float(), nullable=True),
        sa.Column("target", sa.Float(), nullable=True),
        sa.Column("met", sa.Boolean(), nullable=True),
        sa.Column("updated_at", TS, nullable=False),
        sa.PrimaryKeyConstraint("gate", "check_key", name=op.f("pk_gate_progress")),
    )
    op.create_table(
        "lesson_shadow_forecasts",
        sa.Column("event_id", sa.Text(), nullable=False),
        sa.Column("agent", sa.Text(), nullable=False),
        sa.Column("lesson_id", sa.Text(), nullable=False),
        sa.Column("forecast", JSONB, nullable=False),
        sa.Column("recorded_at", TS, server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
        sa.CheckConstraint(AGENT, name=op.f("ck_lesson_shadow_forecasts_agent")),
        sa.PrimaryKeyConstraint("event_id", "agent", "lesson_id", name=op.f("pk_lesson_shadow_forecasts")),
    )
    op.create_index(
        "ix_lesson_shadow_forecasts_lesson_id", "lesson_shadow_forecasts", ["lesson_id"], unique=False
    )
    op.execute(LESSONS_VIEW)
    for grant in GRANTS:
        op.execute(f"GRANT {grant}")


def downgrade() -> None:
    for revoke in REVOKES:
        op.execute(f"REVOKE {revoke}")
    op.execute("DROP VIEW lessons")
    op.drop_index("ix_lesson_shadow_forecasts_lesson_id", table_name="lesson_shadow_forecasts")
    op.drop_table("lesson_shadow_forecasts")
    op.drop_table("gate_progress")
    op.drop_table("reflection_runs")
    op.drop_index("uq_claims_audit_flags_open", table_name="claims_audit_flags")
    op.drop_table("claims_audit_flags")
    op.drop_table("stacking_models")
    op.drop_table("scoring_params")
    op.drop_table("calibration_stats")
    op.drop_table("calibration_bins")
    op.drop_index("ix_weight_history_target_as_of", table_name="weight_history")
    op.drop_table("weight_history")
    op.drop_index("ix_scored_forecasts_target_scored_at", table_name="scored_forecasts")
    op.drop_table("scored_forecasts")
    op.drop_index("ix_scoring_labels_target", table_name="scoring_labels")
    op.drop_table("scoring_labels")
