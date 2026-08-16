"""Migration domain Temporal workflow — durable orchestration of a tenant migration.

``MigrationWorkflow`` is the long-lived, human-gated state machine that moves one
agency off a legacy billing vendor and onto Adaptix. A migration runs for days to
weeks: profile the source export, propose and get a human to approve the field
mapping, dry-run it, backfill history in bulk, reconcile, wait for a named human
to approve cutover, promote, then hold a rollback window open.

Workflow ID convention: ``migration-{tenant_id}-{migration_id}``
  Built by :func:`migration_workflow_id`, which REFUSES anything that is not an
  opaque internal identifier. See its docstring — the workflow ID is written to
  Temporal history, indexes, and the Temporal UI, and is one of the easiest
  places to leak PHI by accident.

Task queues:
  ``migration``       — this workflow and all control-plane activities.
  ``migration-bulk``  — history backfill only, dispatched per-activity.
  Bulk work is pushed to its own queue so a multi-million-record backfill can
  never occupy the poll slots that cutover approvals and live revenue work need.

PHI safety
----------
* No PHI in the workflow ID (enforced, see :func:`migration_workflow_id`).
* No PHI in workflow state. Activity results are passed through
  :func:`_safe_summary`, which keeps only whitelisted scalar keys — counts,
  statuses, versions, internal ids. Anything else is dropped before it can reach
  workflow history, the ``status`` query, or a continue-as-new input.
* No PHI in the ``status`` query. It returns phase, counts, actor user ids and
  error codes only.
* No search attributes are set. Search attributes are indexed and browsable;
  this workflow has no need for any that would not be tenant or record data.
* No PHI in log lines. Only tenant_id, migration_id, phase and actor ids.

Money
-----
Any monetary value that reaches this workflow from reconciliation is an INTEGER
NUMBER OF CENTS. :func:`_safe_summary` only admits ``int`` for the ``*_cents``
keys, so a float total is dropped rather than silently rounded.

Activities
----------
Every migration activity currently raises a non-retryable
``MigrationActivityNotImplemented`` — the Adaptix Imports service that performs
the work does not exist yet. The orchestration here is real and tested; the
first step it schedules will fail loudly until Imports ships. See
``temporal_app/activities/migration_activities.py``.
"""

from __future__ import annotations

import asyncio
import re
from datetime import timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

with workflow.unsafe.imports_passed_through():
    from temporal_app.activities.migration_activities import (
        backfill_migration_history,
        build_field_mapping,
        profile_source_dataset,
        promote_migration_cutover,
        reconcile_migration,
        rollback_migration,
        run_migration_dry_run,
    )
    from temporal_app.config import MIGRATION_BULK_TASK_QUEUE
    from temporal_app.exceptions import ValidationError

# ---------------------------------------------------------------------------
# Timeouts
# ---------------------------------------------------------------------------

#: Control-plane activities: an API round trip to the Imports service.
_ACTIVITY_TIMEOUT = timedelta(minutes=10)

#: Bulk / whole-dataset activities. Long, but finite — an activity that has not
#: reported progress within its heartbeat timeout is retried rather than left
#: hanging for hours.
_LONG_ACTIVITY_TIMEOUT = timedelta(hours=2)

#: Heartbeat cadence expected of long activities. The real implementations must
#: call ``activity.heartbeat()`` at least this often so a worker restart resumes
#: from the last reported batch instead of restarting the dataset.
_HEARTBEAT_TIMEOUT = timedelta(minutes=2)

#: How long a prepared cutover waits for a named human. Long, because a cutover
#: is scheduled around an agency's business calendar — but FINITE, so a
#: forgotten migration ends as an explicit expiry record instead of a workflow
#: that lives forever holding a decision nobody knows is outstanding.
#: Expiry promotes nothing.
_CUTOVER_APPROVAL_TIMEOUT = timedelta(days=14)

#: How long the workflow stays open after promotion so a rollback can still be
#: requested against a live run. Once it elapses the migration is COMPLETED and
#: the workflow closes; a later reversal is a new, separately approved action.
_ROLLBACK_WINDOW = timedelta(days=7)

#: How long the workflow waits for the human mapping decisions it needs.
_MAPPING_DECISION_TIMEOUT = timedelta(days=14)

#: Bounded pause. A paused migration that nobody resumes is abandoned work, not
#: a permanent state; when this elapses the workflow fails visibly.
_MAX_PAUSE = timedelta(days=30)

# ---------------------------------------------------------------------------
# Retry policy
# ---------------------------------------------------------------------------

#: Migration activities touch a partner dataset and the Imports service. The
#: unimplemented-stub error is listed explicitly as non-retryable in addition to
#: being raised with ``non_retryable=True`` — the policy documents the intent at
#: the scheduling site, where an operator reads it.
_MIGRATION_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=2),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=10),
    maximum_attempts=8,
    non_retryable_error_types=[
        "ValidationError",
        "AuthorizationError",
        "MigrationActivityNotImplemented",
    ],
)

# ---------------------------------------------------------------------------
# Phases
# ---------------------------------------------------------------------------

PHASE_PROFILING = "profiling"
PHASE_AWAITING_MAPPING = "awaiting_mapping_decisions"
PHASE_DRY_RUN = "dry_run"
PHASE_BACKFILL = "history_backfill"
PHASE_RECONCILING = "reconciling"
PHASE_AWAITING_CUTOVER = "awaiting_cutover_approval"
PHASE_PROMOTING = "promoting"
PHASE_ROLLBACK_WINDOW = "rollback_window"
PHASE_ROLLING_BACK = "rolling_back"

# Terminal phases — the workflow has closed and no signal can reach it.
PHASE_COMPLETED = "completed"
PHASE_CUTOVER_REJECTED = "cutover_rejected"
PHASE_CUTOVER_EXPIRED = "cutover_expired"
PHASE_ROLLED_BACK = "rolled_back"

TERMINAL_PHASES: frozenset[str] = frozenset(
    {
        PHASE_COMPLETED,
        PHASE_CUTOVER_REJECTED,
        PHASE_CUTOVER_EXPIRED,
        PHASE_ROLLED_BACK,
    }
)

# Signal names, referenced by the legality table and by callers.
SIGNAL_PAUSE = "pause"
SIGNAL_RESUME = "resume"
SIGNAL_APPROVE_CUTOVER = "approve_cutover"
SIGNAL_REJECT_CUTOVER = "reject_cutover"
SIGNAL_REQUEST_ROLLBACK = "request_rollback"
SIGNAL_SUBMIT_MAPPING_DECISION = "submit_mapping_decision"

#: Decisions a human may record against a proposed field mapping.
MAPPING_DECISIONS: frozenset[str] = frozenset({"accept", "override", "skip"})

#: Error ``type`` raised when the migration cannot continue. Non-retryable:
#: every cause is a business-state condition, not a transient fault.
MIGRATION_HALTED_ERROR_TYPE = "MigrationHalted"

#: Most recent rejected signals retained for the status query. Bounded so a
#: signal storm cannot grow workflow state without limit.
_MAX_RETAINED_REJECTIONS = 20


# ---------------------------------------------------------------------------
# Workflow ID — the first place PHI leaks if nobody is watching
# ---------------------------------------------------------------------------

#: An internal identifier: opaque, URL-safe, and long enough that it is not a
#: human-typed label. Rejects '@', spaces, and '.', which is what stops an email
#: address or a patient name from becoming a workflow ID.
_INTERNAL_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{7,63}$")

#: A date anywhere in an identifier is refused outright. A date of birth is the
#: classic PHI leak into an "id", and no internal Adaptix identifier needs one.
_DATE_LIKE_RE = re.compile(r"\d{4}-\d{2}-\d{2}|\d{2}-\d{2}-\d{4}")


def _reject_non_internal_id(label: str, value: str) -> str:
    """Return ``value`` if it is an opaque internal id, else raise.

    The raised message names the FIELD and the rule, never the offending value —
    echoing a rejected value back would copy the suspected PHI into the error
    string, the logs, and the failure payload, which is the exact leak this
    function exists to prevent.
    """
    candidate = (value or "").strip()
    if not _INTERNAL_ID_RE.match(candidate):
        raise ValidationError(
            f"{label} is not a valid internal identifier. It must be 8-64 "
            "characters of letters, digits, '-' or '_' and start with a letter "
            "or digit. Names, emails and free text are refused because the "
            "workflow ID is written to Temporal history and is visible in the "
            "Temporal UI."
        )
    if _DATE_LIKE_RE.search(candidate):
        raise ValidationError(
            f"{label} contains a date. Dates are refused in identifiers that "
            "reach Temporal history — a date of birth must never appear in a "
            "workflow ID."
        )
    return candidate


def migration_workflow_id(tenant_id: str, migration_id: str) -> str:
    """Build the deterministic, PHI-free workflow ID for a tenant migration.

    Deterministic on purpose: the same tenant and migration always produce the
    same ID, so Temporal's own workflow-ID reuse policy is what prevents two
    concurrent migrations of the same dataset. A random or timestamped ID would
    let an operator start a second migration on top of a running one.

    Raises ``ValidationError`` (non-retryable) if either identifier is not an
    opaque internal id.
    """
    tenant = _reject_non_internal_id("tenant_id", tenant_id)
    migration = _reject_non_internal_id("migration_id", migration_id)
    return f"migration-{tenant}-{migration}"


# ---------------------------------------------------------------------------
# Result sanitising — nothing enters workflow state unchecked
# ---------------------------------------------------------------------------

#: Keys an activity result may contribute to workflow state, by activity. Every
#: one is a count, a status, a version or an internal id. Any key not listed is
#: dropped, so an Imports-service response that starts returning record content
#: cannot silently write PHI into Temporal history.
_ALLOWED_RESULT_KEYS: frozenset[str] = frozenset(
    {
        "status",
        "source_record_count",
        "mapped_record_count",
        "migrated_record_count",
        "unmapped_field_count",
        "error_count",
        "warning_count",
        "records_written",
        "mapping_version",
        "dry_run_id",
        "reconciliation_id",
        "promotion_id",
        "rollback_id",
        "next_cursor",
        "source_total_cents",
        "migrated_total_cents",
        "variance_cents",
    }
)

#: Keys that must be integer cents. A float or a formatted string is dropped
#: rather than accepted, because a rounded money value in a reconciliation
#: report is worse than a missing one.
_CENTS_KEYS: frozenset[str] = frozenset(
    {"source_total_cents", "migrated_total_cents", "variance_cents"}
)


def _safe_summary(result: Any) -> dict[str, Any]:
    """Reduce an activity result to whitelisted, non-PHI scalars.

    Fail-closed by omission: an unrecognised key, a nested structure, or a
    monetary value that is not an ``int`` is dropped. A non-mapping result
    yields an empty summary rather than an error, because losing a summary must
    never fail a migration that actually succeeded.
    """
    if not isinstance(result, dict):
        return {}

    summary: dict[str, Any] = {}
    for key, value in result.items():
        if key not in _ALLOWED_RESULT_KEYS:
            continue
        if key in _CENTS_KEYS:
            # bool is an int in Python; reject it explicitly.
            if isinstance(value, bool) or not isinstance(value, int):
                continue
            summary[key] = value
            continue
        if isinstance(value, (str, int, float, bool)) or value is None:
            summary[key] = value
    return summary


def _decision_keys(result: Any) -> list[str]:
    """Extract the field keys that need a human mapping decision.

    Field keys are schema metadata (``patient.last_name``), never values. Only
    strings matching a conservative schema-path shape are accepted so a
    malformed response cannot inject arbitrary text into workflow state.
    """
    if not isinstance(result, dict):
        return []
    raw = result.get("fields_requiring_decision")
    if not isinstance(raw, list):
        return []
    key_re = re.compile(r"^[A-Za-z0-9_.\[\]-]{1,120}$")
    return [item for item in raw if isinstance(item, str) and key_re.match(item)]


# ---------------------------------------------------------------------------
# Signal legality — the pure predicate the workflow and the tests both use
# ---------------------------------------------------------------------------


def _signal_denial(signal: str, state: dict[str, Any]) -> str | None:
    """Return why ``signal`` must be refused in ``state``, or ``None`` to accept.

    Pure and fail-closed: an unknown signal, an unreadable state, or an ordering
    the state machine does not define is DENIED. Ordering matters here because
    the signals drive protected actions — promoting a tenant's data to live and
    reverting it are not operations that may be triggered out of sequence by a
    stale operator screen or a retried API call.

    The rules, and why each exists:

    ``pause``   refused in a terminal phase (nothing left to pause) and when
                already paused (a second pause would let a single resume undo
                two operators' intent).
    ``resume``  refused unless actually paused.
    ``submit_mapping_decision``
                refused outside the mapping phase, for an unknown decision
                value, and for a field already decided — first decision wins, as
                in ``DenialResubmissionWorkflow``.
    ``approve_cutover`` / ``reject_cutover``
                refused unless the workflow is at the cutover gate, refused
                while paused (a decision recorded against a paused migration is
                ambiguous — resume first, then decide), and refused once either
                has been recorded. First decision wins: by the time a second
                arrives the promotion may already be running, and this workflow
                cannot un-promote by signal.
    ``request_rollback``
                refused before promotion — there is nothing to roll back, and
                the correct action on a not-yet-promoted migration is to reject
                the cutover. Refused once a rollback is already under way or
                done, and refused after the rollback window closed, because the
                workflow is then terminal and a reversal is a new approved
                action rather than a late signal.
    """
    if not isinstance(state, dict):
        return "workflow state is unreadable"

    phase = state.get("phase")
    paused = bool(state.get("paused"))
    terminal = phase in TERMINAL_PHASES

    if signal == SIGNAL_PAUSE:
        if terminal:
            return f"migration is already finished (phase={phase})"
        if paused:
            return "migration is already paused"
        return None

    if signal == SIGNAL_RESUME:
        if terminal:
            return f"migration is already finished (phase={phase})"
        if not paused:
            return "migration is not paused"
        return None

    if signal == SIGNAL_SUBMIT_MAPPING_DECISION:
        if phase != PHASE_AWAITING_MAPPING:
            return (
                "mapping decisions are only accepted while the migration is "
                f"awaiting them (phase={phase})"
            )
        decision = state.get("decision")
        if decision not in MAPPING_DECISIONS:
            return f"unknown mapping decision {decision!r}"
        field_key = state.get("field_key")
        if not isinstance(field_key, str) or not field_key.strip():
            return "mapping decision is missing its field key"
        if field_key in (state.get("decided_fields") or ()):
            return f"field {field_key!r} already has a decision"
        return None

    if signal in (SIGNAL_APPROVE_CUTOVER, SIGNAL_REJECT_CUTOVER):
        if phase != PHASE_AWAITING_CUTOVER:
            return (
                "cutover decisions are only accepted at the cutover gate "
                f"(phase={phase})"
            )
        if paused:
            return "migration is paused — resume it before deciding cutover"
        if state.get("cutover_decision") is not None:
            return "a cutover decision has already been recorded"
        return None

    if signal == SIGNAL_REQUEST_ROLLBACK:
        if phase in (PHASE_ROLLING_BACK, PHASE_ROLLED_BACK):
            return "a rollback is already under way or complete"
        if phase != PHASE_ROLLBACK_WINDOW:
            if terminal:
                return (
                    "the rollback window has closed — reverting a completed "
                    "migration is a new, separately approved action"
                )
            return f"nothing has been promoted yet (phase={phase})"
        if state.get("rollback_request") is not None:
            return "a rollback has already been requested"
        return None

    return f"unknown signal {signal!r}"


# ---------------------------------------------------------------------------
# Workflow
# ---------------------------------------------------------------------------


@workflow.defn
class MigrationWorkflow:
    """Durable, human-gated migration of one agency onto Adaptix.

    Sequence:
      1. Profile the legacy source dataset.
      2. Propose a field mapping and BLOCK for a human decision on every field
         that needs one.
      3. Dry-run the transformation. Nothing is written to live tenant state.
      4. Backfill history in batches on the ``migration-bulk`` queue.
      5. Reconcile source totals against migrated totals.
      6. BLOCK at the cutover gate until a named human approves or rejects.
      7. On approval only: promote to live.
      8. Hold a rollback window open, then complete.

    THE CUTOVER GATE FAILS CLOSED. Silence is not consent:
      - no decision inside the window -> ``cutover_expired``, nothing promoted
      - an explicit reject            -> ``cutover_rejected``, nothing promoted
    Only an explicit approval reaches step 7, and the approver's user id is
    carried into the promotion activity and the result so the audit trail names
    a person.

    Signals: ``pause``, ``resume``, ``submit_mapping_decision``,
    ``approve_cutover``, ``reject_cutover``, ``request_rollback``.
    Query: ``status`` — non-PHI progress for an operator surface.

    An illegally ordered signal does not fail the workflow (a raised exception
    in a signal handler would stall the workflow task and retry forever).
    It is refused, recorded with its reason, and exposed through ``status`` so
    the operator surface can show that the action was rejected and why.
    """

    def __init__(self) -> None:
        # Seeded here so signal handlers are safe from the first workflow task.
        # ``run`` overwrites these from a continue-as-new snapshot before its
        # first await, which is before any handler can observe them.
        self._tenant_id: str = ""
        self._migration_id: str = ""
        self._phase: str = PHASE_PROFILING
        self._paused: bool = False
        self._pause_actor: str | None = None
        self._mapping_required: list[str] = []
        self._mapping_decisions: dict[str, dict[str, Any]] = {}
        self._cutover_decision: dict[str, Any] | None = None
        self._rollback_request: dict[str, Any] | None = None
        self._steps: dict[str, Any] = {}
        self._backfill_batches: int = 0
        self._backfill_cursor: str = ""
        self._rejected_signals: list[dict[str, str]] = []
        self._rejected_signal_count: int = 0
        self._continued_runs: int = 0

    # -- signals ----------------------------------------------------------- #
    @workflow.signal
    def pause(self, actor_user_id: str, reason: str = "") -> None:
        """Hold the migration before its next step. In-flight activities finish."""
        if self._refuse(SIGNAL_PAUSE, actor_user_id):
            return
        self._paused = True
        self._pause_actor = actor_user_id
        workflow.logger.info(
            "MigrationWorkflow paused tenant_id=%s migration_id=%s by=%s phase=%s",
            self._tenant_id,
            self._migration_id,
            actor_user_id,
            self._phase,
        )

    @workflow.signal
    def resume(self, actor_user_id: str) -> None:
        """Release a pause and continue from the phase the migration stopped in."""
        if self._refuse(SIGNAL_RESUME, actor_user_id):
            return
        self._paused = False
        self._pause_actor = None
        workflow.logger.info(
            "MigrationWorkflow resumed tenant_id=%s migration_id=%s by=%s phase=%s",
            self._tenant_id,
            self._migration_id,
            actor_user_id,
            self._phase,
        )

    @workflow.signal
    def submit_mapping_decision(
        self,
        field_key: str,
        decision: str,
        actor_user_id: str,
        target_field: str = "",
    ) -> None:
        """Record one human decision on one proposed field mapping.

        ``field_key`` and ``target_field`` are schema paths, never values.
        First decision per field wins.
        """
        if self._refuse(
            SIGNAL_SUBMIT_MAPPING_DECISION,
            actor_user_id,
            field_key=field_key,
            decision=decision,
        ):
            return
        self._mapping_decisions[field_key] = {
            "decision": decision,
            "target_field": target_field,
            "actor_user_id": actor_user_id,
        }

    @workflow.signal
    def approve_cutover(self, actor_user_id: str, reason: str = "") -> None:
        """Approve promotion to live. Protected money/data action; names a person."""
        if self._refuse(SIGNAL_APPROVE_CUTOVER, actor_user_id):
            return
        self._cutover_decision = {
            "approved": True,
            "actor_user_id": actor_user_id,
            "reason": reason,
        }

    @workflow.signal
    def reject_cutover(self, actor_user_id: str, reason: str = "") -> None:
        """Refuse promotion. Nothing is promoted and the migration ends rejected."""
        if self._refuse(SIGNAL_REJECT_CUTOVER, actor_user_id):
            return
        self._cutover_decision = {
            "approved": False,
            "actor_user_id": actor_user_id,
            "reason": reason,
        }

    @workflow.signal
    def request_rollback(self, actor_user_id: str, reason: str = "") -> None:
        """Revert a promoted migration during the rollback window."""
        if self._refuse(SIGNAL_REQUEST_ROLLBACK, actor_user_id):
            return
        self._rollback_request = {
            "actor_user_id": actor_user_id,
            "reason": reason,
        }
        workflow.logger.info(
            "MigrationWorkflow rollback_requested tenant_id=%s migration_id=%s by=%s",
            self._tenant_id,
            self._migration_id,
            actor_user_id,
        )

    # -- query ------------------------------------------------------------- #
    @workflow.query
    def status(self) -> dict[str, Any]:
        """Non-PHI migration status for an operator surface.

        Carries phase, pause state, decision counts, actor user ids, whitelisted
        step counters, and the most recent refused signals with their reasons.
        It carries no record content, no field values, and no free text
        originating from the source dataset.
        """
        return {
            "tenant_id": self._tenant_id,
            "migration_id": self._migration_id,
            "phase": self._phase,
            "paused": self._paused,
            "paused_by": self._pause_actor,
            "mapping_fields_requiring_decision": len(self._mapping_required),
            "mapping_fields_decided": len(self._mapping_decisions),
            "awaiting_cutover_approval": (
                self._phase == PHASE_AWAITING_CUTOVER and self._cutover_decision is None
            ),
            "cutover_approved": (
                None
                if self._cutover_decision is None
                else bool(self._cutover_decision.get("approved"))
            ),
            "cutover_decided_by": (
                None
                if self._cutover_decision is None
                else self._cutover_decision.get("actor_user_id")
            ),
            "rollback_requested_by": (
                None
                if self._rollback_request is None
                else self._rollback_request.get("actor_user_id")
            ),
            "backfill_batches_completed": self._backfill_batches,
            "continued_runs": self._continued_runs,
            "rejected_signal_count": self._rejected_signal_count,
            "recent_rejected_signals": list(self._rejected_signals),
            "steps": dict(self._steps),
        }

    # -- run --------------------------------------------------------------- #
    @workflow.run
    async def run(
        self,
        tenant_id: str,
        migration_id: str,
        carried_state: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Drive the migration to a terminal phase.

        ``carried_state`` is the snapshot handed over by a previous run through
        continue-as-new; a fresh migration passes ``None``. It is applied before
        the first ``await`` so a signal delivered in this same workflow task
        cannot be overwritten by the restore.
        """
        self._tenant_id = tenant_id
        self._migration_id = migration_id
        if carried_state:
            self._restore(carried_state)

        workflow.logger.info(
            "MigrationWorkflow starting tenant_id=%s migration_id=%s phase=%s "
            "continued_runs=%d",
            tenant_id,
            migration_id,
            self._phase,
            self._continued_runs,
        )

        while self._phase not in TERMINAL_PHASES:
            await self._wait_while_paused()
            await self._advance()
            await self._maybe_continue_as_new()

        workflow.logger.info(
            "MigrationWorkflow finished tenant_id=%s migration_id=%s phase=%s",
            tenant_id,
            migration_id,
            self._phase,
        )
        return self._outcome()

    # -- phase machine ----------------------------------------------------- #
    async def _advance(self) -> None:
        """Execute exactly one phase transition."""
        phase = self._phase

        if phase == PHASE_PROFILING:
            result = await workflow.execute_activity(
                profile_source_dataset,
                args=[self._tenant_id, self._migration_id],
                start_to_close_timeout=_ACTIVITY_TIMEOUT,
                retry_policy=_MIGRATION_RETRY,
            )
            self._steps["profile"] = _safe_summary(result)
            self._phase = PHASE_AWAITING_MAPPING
            return

        if phase == PHASE_AWAITING_MAPPING:
            if not self._mapping_required:
                mapping = await workflow.execute_activity(
                    build_field_mapping,
                    args=[self._tenant_id, self._migration_id],
                    start_to_close_timeout=_ACTIVITY_TIMEOUT,
                    retry_policy=_MIGRATION_RETRY,
                )
                self._steps["mapping"] = _safe_summary(mapping)
                self._mapping_required = _decision_keys(mapping)

            if self._mapping_required:
                try:
                    await workflow.wait_condition(
                        self._all_mapping_decisions_in,
                        timeout=_MAPPING_DECISION_TIMEOUT,
                    )
                except asyncio.TimeoutError:
                    raise self._halt(
                        "the mapping review window elapsed with "
                        f"{len(self._mapping_required) - len(self._mapping_decisions)} "
                        "field decision(s) outstanding. Nothing was migrated."
                    ) from None
            self._phase = PHASE_DRY_RUN
            return

        if phase == PHASE_DRY_RUN:
            result = await workflow.execute_activity(
                run_migration_dry_run,
                args=[
                    self._tenant_id,
                    self._migration_id,
                    str(self._steps.get("mapping", {}).get("mapping_version", "")),
                ],
                start_to_close_timeout=_LONG_ACTIVITY_TIMEOUT,
                heartbeat_timeout=_HEARTBEAT_TIMEOUT,
                retry_policy=_MIGRATION_RETRY,
            )
            self._steps["dry_run"] = _safe_summary(result)
            self._phase = PHASE_BACKFILL
            return

        if phase == PHASE_BACKFILL:
            # One batch per pass through the loop. Returning to the caller after
            # each batch is what lets pause, continue-as-new and the rejection
            # bookkeeping run between batches instead of after the whole dataset.
            result = await workflow.execute_activity(
                backfill_migration_history,
                args=[self._tenant_id, self._migration_id, self._backfill_cursor],
                task_queue=MIGRATION_BULK_TASK_QUEUE,
                start_to_close_timeout=_LONG_ACTIVITY_TIMEOUT,
                heartbeat_timeout=_HEARTBEAT_TIMEOUT,
                retry_policy=_MIGRATION_RETRY,
            )
            summary = _safe_summary(result)
            self._backfill_batches += 1
            self._steps["backfill"] = {
                "batches_completed": self._backfill_batches,
                "last_batch": summary,
            }
            next_cursor = summary.get("next_cursor")
            if isinstance(next_cursor, str) and next_cursor:
                self._backfill_cursor = next_cursor
                return  # another batch
            self._backfill_cursor = ""
            self._phase = PHASE_RECONCILING
            return

        if phase == PHASE_RECONCILING:
            result = await workflow.execute_activity(
                reconcile_migration,
                args=[self._tenant_id, self._migration_id, "pre_cutover"],
                start_to_close_timeout=_LONG_ACTIVITY_TIMEOUT,
                heartbeat_timeout=_HEARTBEAT_TIMEOUT,
                retry_policy=_MIGRATION_RETRY,
            )
            self._steps["reconciliation"] = _safe_summary(result)
            self._phase = PHASE_AWAITING_CUTOVER
            return

        if phase == PHASE_AWAITING_CUTOVER:
            # Nothing below this line runs without an explicit human approval.
            try:
                await workflow.wait_condition(
                    lambda: self._cutover_decision is not None,
                    timeout=_CUTOVER_APPROVAL_TIMEOUT,
                )
            except asyncio.TimeoutError:
                workflow.logger.warning(
                    "MigrationWorkflow cutover_expired tenant_id=%s migration_id=%s "
                    "— nothing promoted",
                    self._tenant_id,
                    self._migration_id,
                )
                self._phase = PHASE_CUTOVER_EXPIRED
                return

            decision = self._cutover_decision or {}
            if not decision.get("approved"):
                workflow.logger.info(
                    "MigrationWorkflow cutover_rejected tenant_id=%s migration_id=%s "
                    "by=%s — nothing promoted",
                    self._tenant_id,
                    self._migration_id,
                    decision.get("actor_user_id"),
                )
                self._phase = PHASE_CUTOVER_REJECTED
                return

            self._phase = PHASE_PROMOTING
            return

        if phase == PHASE_PROMOTING:
            decision = self._cutover_decision or {}
            result = await workflow.execute_activity(
                promote_migration_cutover,
                args=[
                    self._tenant_id,
                    self._migration_id,
                    str(decision.get("actor_user_id", "")),
                ],
                start_to_close_timeout=_LONG_ACTIVITY_TIMEOUT,
                heartbeat_timeout=_HEARTBEAT_TIMEOUT,
                retry_policy=_MIGRATION_RETRY,
            )
            self._steps["promotion"] = _safe_summary(result)
            workflow.logger.info(
                "MigrationWorkflow promoted tenant_id=%s migration_id=%s "
                "approved_by=%s",
                self._tenant_id,
                self._migration_id,
                decision.get("actor_user_id"),
            )
            self._phase = PHASE_ROLLBACK_WINDOW
            return

        if phase == PHASE_ROLLBACK_WINDOW:
            try:
                await workflow.wait_condition(
                    lambda: self._rollback_request is not None,
                    timeout=_ROLLBACK_WINDOW,
                )
            except asyncio.TimeoutError:
                self._phase = PHASE_COMPLETED
                return
            self._phase = PHASE_ROLLING_BACK
            return

        if phase == PHASE_ROLLING_BACK:
            request = self._rollback_request or {}
            result = await workflow.execute_activity(
                rollback_migration,
                args=[
                    self._tenant_id,
                    self._migration_id,
                    str(request.get("actor_user_id", "")),
                ],
                start_to_close_timeout=_LONG_ACTIVITY_TIMEOUT,
                heartbeat_timeout=_HEARTBEAT_TIMEOUT,
                retry_policy=_MIGRATION_RETRY,
            )
            self._steps["rollback"] = _safe_summary(result)
            self._phase = PHASE_ROLLED_BACK
            return

        raise self._halt(f"unrecognised migration phase {phase!r}")

    # -- helpers ----------------------------------------------------------- #
    def _all_mapping_decisions_in(self) -> bool:
        return all(key in self._mapping_decisions for key in self._mapping_required)

    async def _wait_while_paused(self) -> None:
        """Block while an operator holds the migration, with a bounded ceiling."""
        if not self._paused:
            return
        try:
            await workflow.wait_condition(lambda: not self._paused, timeout=_MAX_PAUSE)
        except asyncio.TimeoutError:
            raise self._halt(
                "the migration stayed paused past its maximum hold. Nothing "
                "further was migrated or promoted."
            ) from None

    def _refuse(self, signal: str, actor_user_id: str, **extra: Any) -> bool:
        """Apply :func:`_signal_denial`; record and refuse when it denies.

        Returns True when the caller must stop. Never raises: an exception here
        would fail the workflow task and retry the signal forever.
        """
        state = self._signal_state()
        state.update(extra)
        state["decided_fields"] = tuple(self._mapping_decisions)
        reason = _signal_denial(signal, state)
        if reason is None:
            return False

        self._rejected_signal_count += 1
        self._rejected_signals.append(
            {
                "signal": signal,
                "reason": reason,
                "actor_user_id": actor_user_id,
                "phase": self._phase,
            }
        )
        del self._rejected_signals[:-_MAX_RETAINED_REJECTIONS]
        workflow.logger.warning(
            "MigrationWorkflow signal_refused tenant_id=%s migration_id=%s "
            "signal=%s by=%s phase=%s reason=%s",
            self._tenant_id,
            self._migration_id,
            signal,
            actor_user_id,
            self._phase,
            reason,
        )
        return True

    def _signal_state(self) -> dict[str, Any]:
        """The state slice :func:`_signal_denial` reads."""
        return {
            "phase": self._phase,
            "paused": self._paused,
            "cutover_decision": self._cutover_decision,
            "rollback_request": self._rollback_request,
        }

    def _halt(self, reason: str) -> ApplicationError:
        """Build the non-retryable error that ends a migration that cannot go on."""
        return ApplicationError(
            f"MigrationWorkflow halted for tenant {self._tenant_id} "
            f"(migration {self._migration_id}) in phase {self._phase}: {reason}",
            type=MIGRATION_HALTED_ERROR_TYPE,
            non_retryable=True,
        )

    def _snapshot(self) -> dict[str, Any]:
        """Serialise durable state for continue-as-new.

        Carries only what the next run needs to resume: phase, decisions, and
        whitelisted step summaries. It is itself a workflow input, so it is held
        to the same no-PHI rule as everything else here.
        """
        return {
            "phase": self._phase,
            "paused": self._paused,
            "pause_actor": self._pause_actor,
            "mapping_required": list(self._mapping_required),
            "mapping_decisions": dict(self._mapping_decisions),
            "cutover_decision": self._cutover_decision,
            "rollback_request": self._rollback_request,
            "steps": dict(self._steps),
            "backfill_batches": self._backfill_batches,
            "backfill_cursor": self._backfill_cursor,
            "rejected_signal_count": self._rejected_signal_count,
            "rejected_signals": list(self._rejected_signals),
            "continued_runs": self._continued_runs + 1,
        }

    def _restore(self, carried: dict[str, Any]) -> None:
        """Rehydrate state from a continue-as-new snapshot, defensively."""
        phase = carried.get("phase")
        if isinstance(phase, str) and phase:
            self._phase = phase
        self._paused = bool(carried.get("paused"))
        pause_actor = carried.get("pause_actor")
        self._pause_actor = pause_actor if isinstance(pause_actor, str) else None
        required = carried.get("mapping_required")
        self._mapping_required = (
            [k for k in required if isinstance(k, str)]
            if (isinstance(required, list))
            else []
        )
        decisions = carried.get("mapping_decisions")
        self._mapping_decisions = dict(decisions) if isinstance(decisions, dict) else {}
        cutover = carried.get("cutover_decision")
        self._cutover_decision = cutover if isinstance(cutover, dict) else None
        rollback = carried.get("rollback_request")
        self._rollback_request = rollback if isinstance(rollback, dict) else None
        steps = carried.get("steps")
        self._steps = dict(steps) if isinstance(steps, dict) else {}
        self._backfill_batches = _as_int(carried.get("backfill_batches"))
        cursor = carried.get("backfill_cursor")
        self._backfill_cursor = cursor if isinstance(cursor, str) else ""
        self._rejected_signal_count = _as_int(carried.get("rejected_signal_count"))
        rejected = carried.get("rejected_signals")
        self._rejected_signals = (
            [r for r in rejected if isinstance(r, dict)][-_MAX_RETAINED_REJECTIONS:]
            if isinstance(rejected, list)
            else []
        )
        self._continued_runs = _as_int(carried.get("continued_runs"))

    async def _maybe_continue_as_new(self) -> None:
        """Hand over to a fresh run before history grows unbounded.

        A migration backfills history in many batches, and each batch adds
        activity events to workflow history. Temporal tells us when that history
        is getting large; at that point the run hands its state to a new run,
        which starts with an empty history and identical durable state.

        Two guards, both required:
          * ``is_continue_as_new_suggested()`` — the SDK's own threshold, rather
            than a batch count guessed here.
          * ``all_handlers_finished()`` — never continue-as-new while a signal
            or query handler is mid-flight; that handler's work would be lost.

        Never continues while paused: the new run would immediately re-enter the
        pause wait, and an operator watching a paused migration should not see
        it change run id underneath them.
        """
        if self._paused or self._phase in TERMINAL_PHASES:
            return
        if not workflow.info().is_continue_as_new_suggested():
            return
        if not workflow.all_handlers_finished():
            return

        workflow.logger.info(
            "MigrationWorkflow continue_as_new tenant_id=%s migration_id=%s phase=%s "
            "batches=%d",
            self._tenant_id,
            self._migration_id,
            self._phase,
            self._backfill_batches,
        )
        workflow.continue_as_new(
            args=[self._tenant_id, self._migration_id, self._snapshot()]
        )

    def _outcome(self) -> dict[str, Any]:
        """Build a result that states plainly what was and was not done."""
        decision = self._cutover_decision or {}
        rollback = self._rollback_request or {}
        return {
            "outcome": self._phase,
            "tenant_id": self._tenant_id,
            "migration_id": self._migration_id,
            "promoted": self._phase in (PHASE_COMPLETED, PHASE_ROLLED_BACK),
            "rolled_back": self._phase == PHASE_ROLLED_BACK,
            "cutover_decided_by": decision.get("actor_user_id"),
            "cutover_reason": decision.get("reason", ""),
            "rollback_requested_by": rollback.get("actor_user_id"),
            "backfill_batches_completed": self._backfill_batches,
            "rejected_signal_count": self._rejected_signal_count,
            "steps": dict(self._steps),
        }


def _as_int(value: Any) -> int:
    """Coerce a snapshot counter to a non-negative int, defaulting to 0."""
    if isinstance(value, bool) or not isinstance(value, int):
        return 0
    return max(value, 0)
