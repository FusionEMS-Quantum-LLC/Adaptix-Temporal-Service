"""Tests for MigrationWorkflow — workflow ID safety, signal ordering, cutover gate.

WHAT IS REAL HERE, AND WHAT IS NOT
----------------------------------
Real: the workflow ID builder, the signal-legality predicate, the real
``MigrationWorkflow`` class, its real signal handlers, its real query, and its
real ``run`` body driving the real phase machine.

Not real, deliberately: the activity boundary and the Temporal waits are
substituted so the coroutine can run outside a worker. The migration activities
are not stand-ins for behaviour that exists — every one of them currently raises
``MigrationActivityNotImplemented`` because the Adaptix Imports service is not
built. Recording which activities the workflow SCHEDULES is the assertion: for a
protected action the question is not what the workflow returned but whether
``promote_migration_cutover`` was invoked at all.

What these tests prove:
  - ``migration_workflow_id`` is deterministic and REFUSES anything that is not
    an opaque internal id: emails, names, free text, dates (a DOB is the classic
    PHI leak into an "id"), and values with '@', '.' or spaces. The rejection
    message never echoes the rejected value.
  - Illegal signal ordering is rejected for every signal: resume without pause,
    double pause, mapping decisions outside the mapping phase, an unknown
    mapping decision value, a second decision on an already-decided field,
    cutover approval before the gate, cutover approval while paused, a second
    cutover decision after the first, rollback before promotion, and rollback
    after the window closed.
  - A refused signal changes NO workflow state, is counted, and is reported with
    its reason through the ``status`` query — it does not fail the workflow.
  - The cutover gate fails closed: no decision (expiry) and an explicit reject
    both end the migration without ``promote_migration_cutover`` ever running.
  - Only an explicit approval reaches promotion, and the approver's user id is
    carried into the activity and the result.
  - Bulk backfill is dispatched to the ``migration-bulk`` task queue, never the
    control-plane queue.
  - Long activities are scheduled with both a start-to-close timeout and a
    heartbeat timeout.
  - The continue-as-new guard fires only when the SDK suggests it AND all
    handlers are finished, and it carries durable state forward.
  - The ``status`` query returns no PHI-shaped free text.

What these tests do NOT prove:
  - Execution against a real Temporal server or history-replay determinism.
  - That the Imports service behaves correctly — it does not exist yet.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from temporalio import workflow
from temporalio.exceptions import ApplicationError

from temporal_app.config import MIGRATION_BULK_TASK_QUEUE, MIGRATION_TASK_QUEUE
from temporal_app.exceptions import ValidationError
from temporal_app.workflows.migration import (
    MAPPING_DECISIONS,
    PHASE_AWAITING_CUTOVER,
    PHASE_AWAITING_MAPPING,
    PHASE_COMPLETED,
    PHASE_CUTOVER_EXPIRED,
    PHASE_CUTOVER_REJECTED,
    PHASE_PROFILING,
    PHASE_PROMOTING,
    PHASE_ROLLBACK_WINDOW,
    PHASE_ROLLED_BACK,
    PHASE_ROLLING_BACK,
    SIGNAL_APPROVE_CUTOVER,
    SIGNAL_PAUSE,
    SIGNAL_REJECT_CUTOVER,
    SIGNAL_REQUEST_ROLLBACK,
    SIGNAL_RESUME,
    SIGNAL_SUBMIT_MAPPING_DECISION,
    MigrationWorkflow,
    _signal_denial,
    migration_workflow_id,
)

TENANT_ID = "11111111-2222-3333-4444-555555555555"
MIGRATION_ID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"


# ===========================================================================
# Workflow ID — no PHI
# ===========================================================================


def test_workflow_id_is_deterministic():
    """The same tenant and migration always produce the same workflow ID."""
    first = migration_workflow_id(TENANT_ID, MIGRATION_ID)
    second = migration_workflow_id(TENANT_ID, MIGRATION_ID)

    assert first == second
    assert first == f"migration-{TENANT_ID}-{MIGRATION_ID}"


def test_workflow_id_contains_no_phi_for_valid_internal_ids():
    """A well-formed ID is only 'migration-' plus two opaque internal ids."""
    workflow_id = migration_workflow_id(TENANT_ID, MIGRATION_ID)

    assert workflow_id.startswith("migration-")
    body = workflow_id[len("migration-") :]
    # Nothing but hex, digits and dashes survives — no names, emails or dates.
    assert set(body) <= set("0123456789abcdefABCDEF-")
    for forbidden in ("@", " ", ".", "/", "\\", ","):
        assert forbidden not in workflow_id


@pytest.mark.parametrize(
    ("tenant_id", "migration_id", "label"),
    [
        ("jane.doe@agency.test", MIGRATION_ID, "tenant is an email"),
        (TENANT_ID, "jane.doe@agency.test", "migration is an email"),
        ("Jane Doe", MIGRATION_ID, "tenant is a person name"),
        (TENANT_ID, "Jane Doe", "migration is a person name"),
        ("1985-03-02", MIGRATION_ID, "tenant is a date of birth"),
        (TENANT_ID, "1985-03-02", "migration is a date of birth"),
        (TENANT_ID, "03-02-1985", "migration is a US-format date of birth"),
        (TENANT_ID, "migration-2026-08-16-batch", "migration embeds a date"),
        ("", MIGRATION_ID, "tenant is empty"),
        (TENANT_ID, "", "migration is empty"),
        ("short", MIGRATION_ID, "tenant is too short to be an internal id"),
        (TENANT_ID, "MRN 4471/A", "migration is a record locator"),
        ("../../etc/passwd", MIGRATION_ID, "tenant is a path"),
        ("tenant id with spaces", MIGRATION_ID, "tenant has spaces"),
        ("-leading-dash-id-value", MIGRATION_ID, "tenant starts with a dash"),
    ],
)
def test_workflow_id_refuses_anything_that_is_not_an_internal_id(
    tenant_id: str, migration_id: str, label: str
):
    """PHI-shaped identifiers never become a workflow ID."""
    with pytest.raises(ValidationError):
        migration_workflow_id(tenant_id, migration_id)


def test_workflow_id_rejection_never_echoes_the_rejected_value():
    """Echoing a rejected value would copy suspected PHI into logs and history."""
    leaky = "jane.doe@agency.test"

    with pytest.raises(ValidationError) as exc:
        migration_workflow_id(leaky, MIGRATION_ID)

    assert leaky not in str(exc.value)
    assert "jane" not in str(exc.value).lower()


# ===========================================================================
# Signal legality predicate — illegal ordering is rejected
# ===========================================================================


def _state(**overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "phase": PHASE_AWAITING_CUTOVER,
        "paused": False,
        "cutover_decision": None,
        "rollback_request": None,
        "decided_fields": (),
    }
    base.update(overrides)
    return base


@pytest.mark.parametrize(
    ("signal", "state", "label"),
    [
        (SIGNAL_RESUME, _state(paused=False), "resume without a pause"),
        (SIGNAL_PAUSE, _state(paused=True), "pause while already paused"),
        (SIGNAL_PAUSE, _state(phase=PHASE_COMPLETED), "pause a finished migration"),
        (SIGNAL_RESUME, _state(phase=PHASE_ROLLED_BACK), "resume a finished migration"),
        (
            SIGNAL_SUBMIT_MAPPING_DECISION,
            _state(field_key="patient.last_name", decision="accept"),
            "mapping decision outside the mapping phase",
        ),
        (
            SIGNAL_SUBMIT_MAPPING_DECISION,
            _state(
                phase=PHASE_AWAITING_MAPPING,
                field_key="patient.last_name",
                decision="obliterate",
            ),
            "unknown mapping decision value",
        ),
        (
            SIGNAL_SUBMIT_MAPPING_DECISION,
            _state(phase=PHASE_AWAITING_MAPPING, field_key="", decision="accept"),
            "mapping decision with no field key",
        ),
        (
            SIGNAL_SUBMIT_MAPPING_DECISION,
            _state(
                phase=PHASE_AWAITING_MAPPING,
                field_key="patient.last_name",
                decision="accept",
                decided_fields=("patient.last_name",),
            ),
            "second decision on an already-decided field",
        ),
        (
            SIGNAL_APPROVE_CUTOVER,
            _state(phase=PHASE_PROFILING),
            "approve cutover before the gate",
        ),
        (
            SIGNAL_APPROVE_CUTOVER,
            _state(phase=PHASE_AWAITING_MAPPING),
            "approve cutover during mapping review",
        ),
        (
            SIGNAL_APPROVE_CUTOVER,
            _state(paused=True),
            "approve cutover while paused",
        ),
        (
            SIGNAL_REJECT_CUTOVER,
            _state(paused=True),
            "reject cutover while paused",
        ),
        (
            SIGNAL_APPROVE_CUTOVER,
            _state(cutover_decision={"approved": False, "actor_user_id": "u-1"}),
            "approve after a reject was recorded",
        ),
        (
            SIGNAL_REJECT_CUTOVER,
            _state(cutover_decision={"approved": True, "actor_user_id": "u-1"}),
            "reject after an approval was recorded",
        ),
        (
            SIGNAL_APPROVE_CUTOVER,
            _state(phase=PHASE_ROLLBACK_WINDOW),
            "approve cutover after promotion",
        ),
        (
            SIGNAL_REQUEST_ROLLBACK,
            _state(phase=PHASE_PROFILING),
            "rollback before anything was promoted",
        ),
        (
            SIGNAL_REQUEST_ROLLBACK,
            _state(phase=PHASE_AWAITING_CUTOVER),
            "rollback while still awaiting cutover approval",
        ),
        (
            SIGNAL_REQUEST_ROLLBACK,
            _state(phase=PHASE_COMPLETED),
            "rollback after the window closed",
        ),
        (
            SIGNAL_REQUEST_ROLLBACK,
            _state(phase=PHASE_ROLLING_BACK),
            "rollback while a rollback is already running",
        ),
        (
            SIGNAL_REQUEST_ROLLBACK,
            _state(
                phase=PHASE_ROLLBACK_WINDOW,
                rollback_request={"actor_user_id": "u-1"},
            ),
            "second rollback request",
        ),
        ("delete_everything", _state(), "unknown signal"),
        (SIGNAL_PAUSE, "not a mapping", "unreadable state"),
    ],
)
def test_signal_denial_rejects_illegal_ordering(signal: str, state: Any, label: str):
    """Every out-of-order signal is denied with a reason."""
    reason = _signal_denial(signal, state)

    assert reason is not None, f"must be denied: {label}"
    assert isinstance(reason, str) and reason, f"denial needs a reason: {label}"


@pytest.mark.parametrize(
    ("signal", "state", "label"),
    [
        (SIGNAL_PAUSE, _state(phase=PHASE_PROFILING), "pause a running migration"),
        (SIGNAL_RESUME, _state(phase=PHASE_PROFILING, paused=True), "resume a pause"),
        (
            SIGNAL_SUBMIT_MAPPING_DECISION,
            _state(
                phase=PHASE_AWAITING_MAPPING,
                field_key="patient.last_name",
                decision="accept",
            ),
            "first mapping decision at the mapping gate",
        ),
        (SIGNAL_APPROVE_CUTOVER, _state(), "first approval at the cutover gate"),
        (SIGNAL_REJECT_CUTOVER, _state(), "first rejection at the cutover gate"),
        (
            SIGNAL_REQUEST_ROLLBACK,
            _state(phase=PHASE_ROLLBACK_WINDOW),
            "rollback inside the window",
        ),
    ],
)
def test_signal_denial_allows_legal_ordering(signal: str, state: Any, label: str):
    """Legal signals are not blanket-denied."""
    assert _signal_denial(signal, state) is None, f"must be allowed: {label}"


@pytest.mark.parametrize("decision", sorted(MAPPING_DECISIONS))
def test_every_declared_mapping_decision_is_accepted(decision: str):
    """The allowed-decision set and the predicate agree."""
    state = _state(
        phase=PHASE_AWAITING_MAPPING, field_key="patient.dob", decision=decision
    )
    assert _signal_denial(SIGNAL_SUBMIT_MAPPING_DECISION, state) is None


# ===========================================================================
# Signal handlers on the real workflow instance
# ===========================================================================


def _instance(**state: Any) -> MigrationWorkflow:
    """A real workflow instance with its private state positioned for a test."""
    wf = MigrationWorkflow()
    wf._tenant_id = TENANT_ID
    wf._migration_id = MIGRATION_ID
    for key, value in state.items():
        setattr(wf, f"_{key}", value)
    return wf


def _with_workflow_logger():
    return patch.object(workflow, "logger", logging.getLogger("test.migration"))


def test_illegal_signal_changes_no_state_and_is_reported():
    """A refused signal is recorded, not applied, and never fails the workflow."""
    wf = _instance(phase=PHASE_PROFILING)

    with _with_workflow_logger():
        wf.resume("user-1")  # not paused

    assert wf._paused is False
    status = wf.status()
    assert status["rejected_signal_count"] == 1
    assert status["recent_rejected_signals"][0]["signal"] == SIGNAL_RESUME
    assert status["recent_rejected_signals"][0]["actor_user_id"] == "user-1"
    assert "not paused" in status["recent_rejected_signals"][0]["reason"]


def test_illegal_cutover_approval_before_the_gate_records_no_decision():
    """An early approval cannot arm the promotion step."""
    wf = _instance(phase=PHASE_PROFILING)

    with _with_workflow_logger():
        wf.approve_cutover("user-1")

    assert wf._cutover_decision is None
    assert wf.status()["cutover_approved"] is None
    assert wf.status()["rejected_signal_count"] == 1


def test_first_cutover_decision_wins():
    """A later signal cannot overturn a recorded decision."""
    wf = _instance(phase=PHASE_AWAITING_CUTOVER)

    with _with_workflow_logger():
        wf.reject_cutover("user-1", reason="totals do not reconcile")
        wf.approve_cutover("user-2", reason="override")

    assert wf._cutover_decision == {
        "approved": False,
        "actor_user_id": "user-1",
        "reason": "totals do not reconcile",
    }
    assert wf.status()["rejected_signal_count"] == 1


def test_first_mapping_decision_per_field_wins():
    """A second decision on the same field is refused."""
    wf = _instance(phase=PHASE_AWAITING_MAPPING)

    with _with_workflow_logger():
        wf.submit_mapping_decision("patient.last_name", "accept", "user-1")
        wf.submit_mapping_decision("patient.last_name", "skip", "user-2")

    assert wf._mapping_decisions["patient.last_name"]["decision"] == "accept"
    assert wf._mapping_decisions["patient.last_name"]["actor_user_id"] == "user-1"
    assert wf.status()["rejected_signal_count"] == 1


def test_rejected_signal_history_is_bounded():
    """A signal storm cannot grow workflow state without limit."""
    wf = _instance(phase=PHASE_PROFILING)

    with _with_workflow_logger():
        for i in range(100):
            wf.resume(f"user-{i}")

    assert wf.status()["rejected_signal_count"] == 100
    assert len(wf.status()["recent_rejected_signals"]) == 20


def test_status_query_carries_no_record_content():
    """The operator status surface exposes phase and counts, never payload data."""
    wf = _instance(
        phase=PHASE_AWAITING_CUTOVER,
        mapping_required=["patient.last_name", "encounter.disposition"],
        steps={"reconciliation": {"source_total_cents": 1234, "error_count": 0}},
    )

    with _with_workflow_logger():
        wf.approve_cutover("user-9", reason="reconciled clean")

    status = wf.status()

    assert status["phase"] == PHASE_AWAITING_CUTOVER
    assert status["mapping_fields_requiring_decision"] == 2
    assert status["cutover_decided_by"] == "user-9"
    # Counts and cents only — no field values, no free text from the dataset.
    assert status["steps"]["reconciliation"] == {
        "source_total_cents": 1234,
        "error_count": 0,
    }
    assert isinstance(status["steps"]["reconciliation"]["source_total_cents"], int)


# ===========================================================================
# Workflow body — activity boundary harness
# ===========================================================================


class _ContinuedAsNew(Exception):
    """Sentinel standing in for the SDK's continue-as-new control flow."""

    def __init__(self, args: list[Any]) -> None:
        super().__init__("continue_as_new")
        self.args_passed = args


class _Harness:
    """Runs the real workflow body with the activity and wait edges substituted.

    ``script`` is a list of callables applied one at a time whenever the
    workflow blocks on a condition that is not yet true — that is how a test
    delivers a signal at a specific gate. When the script is exhausted the wait
    times out, which is how a test expresses "nobody ever decided".
    """

    def __init__(
        self,
        results: dict[str, Any] | None = None,
        script: list[Any] | None = None,
        continue_as_new_suggested: bool = False,
    ) -> None:
        self.results = results or {}
        self.script = list(script or [])
        self.calls: list[str] = []
        self.call_kwargs: list[dict[str, Any]] = []
        self.call_args: list[Any] = []
        self._can_suggested = continue_as_new_suggested

    async def execute_activity(self, activity: Any, *args: Any, **kwargs: Any) -> Any:
        name = getattr(activity, "__name__", str(activity))
        self.calls.append(name)
        self.call_kwargs.append(kwargs)
        self.call_args.append(kwargs.get("args", args))
        return self.results.get(name, {})

    async def wait_condition(self, fn: Any, *, timeout: Any = None) -> None:
        if fn():
            return
        while self.script:
            self.script.pop(0)()
            if fn():
                return
        raise asyncio.TimeoutError

    def kwargs_for(self, activity_name: str) -> dict[str, Any]:
        index = self.calls.index(activity_name)
        return self.call_kwargs[index]

    def args_for(self, activity_name: str) -> Any:
        index = self.calls.index(activity_name)
        return self.call_args[index]


def _run(
    harness: _Harness,
    wf: MigrationWorkflow | None = None,
    carried_state: dict[str, Any] | None = None,
) -> tuple[MigrationWorkflow, Any, BaseException | None]:
    """Execute the real ``MigrationWorkflow.run`` against ``harness``."""
    wf = wf or MigrationWorkflow()

    info = MagicMock()
    info.is_continue_as_new_suggested.return_value = harness._can_suggested

    def _continue_as_new(*_a: Any, **kwargs: Any) -> None:
        raise _ContinuedAsNew(kwargs.get("args", []))

    result: Any = None
    raised: BaseException | None = None
    with (
        patch.object(workflow, "execute_activity", harness.execute_activity),
        patch.object(workflow, "wait_condition", harness.wait_condition),
        patch.object(workflow, "logger", logging.getLogger("test.migration")),
        patch.object(workflow, "info", lambda: info),
        patch.object(workflow, "all_handlers_finished", lambda: True),
        patch.object(workflow, "continue_as_new", _continue_as_new),
    ):
        try:
            result = asyncio.run(wf.run(TENANT_ID, MIGRATION_ID, carried_state))
        except BaseException as exc:  # noqa: BLE001 — the test asserts on it
            raised = exc

    return wf, result, raised


# ---------------------------------------------------------------------------
# The cutover gate fails closed
# ---------------------------------------------------------------------------


def test_no_decision_expires_the_cutover_and_promotes_nothing():
    """Silence is never consent: the window elapses and nothing is promoted."""
    harness = _Harness()

    wf, result, raised = _run(harness)

    assert raised is None
    assert result["outcome"] == PHASE_CUTOVER_EXPIRED
    assert result["promoted"] is False
    assert "promote_migration_cutover" not in harness.calls
    assert "rollback_migration" not in harness.calls


def test_explicit_rejection_promotes_nothing():
    """An explicit reject ends the migration without touching live tenant state."""
    holder: dict[str, MigrationWorkflow] = {}
    wf = MigrationWorkflow()
    holder["wf"] = wf
    harness = _Harness(
        script=[lambda: holder["wf"].reject_cutover("user-7", reason="variance")]
    )

    wf, result, raised = _run(harness, wf=wf)

    assert raised is None
    assert result["outcome"] == PHASE_CUTOVER_REJECTED
    assert result["promoted"] is False
    assert result["cutover_decided_by"] == "user-7"
    assert "promote_migration_cutover" not in harness.calls


def test_explicit_approval_promotes_and_names_the_approver():
    """Only an explicit approval reaches promotion, carrying the approver's id."""
    wf = MigrationWorkflow()
    harness = _Harness(script=[lambda: wf.approve_cutover("user-7", reason="clean")])

    wf, result, raised = _run(harness, wf=wf)

    assert raised is None
    assert "promote_migration_cutover" in harness.calls
    assert harness.args_for("promote_migration_cutover") == [
        TENANT_ID,
        MIGRATION_ID,
        "user-7",
    ]
    assert result["outcome"] == PHASE_COMPLETED
    assert result["promoted"] is True
    assert result["cutover_decided_by"] == "user-7"


def test_gate_runs_before_promotion_never_after():
    """Reconciliation and the human gate precede any promotion."""
    wf = MigrationWorkflow()
    harness = _Harness(script=[lambda: wf.approve_cutover("user-7")])

    wf, _result, _raised = _run(harness, wf=wf)

    assert harness.calls.index("reconcile_migration") < harness.calls.index(
        "promote_migration_cutover"
    )


def test_rollback_inside_the_window_runs_and_ends_rolled_back():
    """A rollback requested during the window reverts the promotion."""
    wf = MigrationWorkflow()
    harness = _Harness(
        script=[
            lambda: wf.approve_cutover("user-7"),
            lambda: wf.request_rollback("user-8", reason="agency aborted"),
        ]
    )

    wf, result, raised = _run(harness, wf=wf)

    assert raised is None
    assert "rollback_migration" in harness.calls
    assert harness.args_for("rollback_migration") == [
        TENANT_ID,
        MIGRATION_ID,
        "user-8",
    ]
    assert result["outcome"] == PHASE_ROLLED_BACK
    assert result["rolled_back"] is True
    assert result["rollback_requested_by"] == "user-8"


def test_mapping_review_that_nobody_completes_halts_without_migrating():
    """An abandoned mapping review halts non-retryably instead of proceeding."""
    harness = _Harness(
        results={
            "build_field_mapping": {
                "mapping_version": "v1",
                "fields_requiring_decision": ["patient.last_name"],
            }
        }
    )

    _wf, _result, raised = _run(harness)

    assert isinstance(raised, ApplicationError)
    assert raised.type == "MigrationHalted"
    assert raised.non_retryable is True
    assert "run_migration_dry_run" not in harness.calls
    assert "promote_migration_cutover" not in harness.calls


def test_mapping_decisions_release_the_gate():
    """Once every required field is decided the migration proceeds."""
    wf = MigrationWorkflow()
    harness = _Harness(
        results={
            "build_field_mapping": {
                "mapping_version": "v1",
                "fields_requiring_decision": ["patient.last_name", "unit.callsign"],
            }
        },
        script=[
            lambda: wf.submit_mapping_decision("patient.last_name", "accept", "u-1"),
            lambda: wf.submit_mapping_decision("unit.callsign", "skip", "u-1"),
            lambda: wf.approve_cutover("u-2"),
        ],
    )

    wf, result, raised = _run(harness, wf=wf)

    assert raised is None
    assert "run_migration_dry_run" in harness.calls
    assert result["outcome"] == PHASE_COMPLETED


# ---------------------------------------------------------------------------
# Queue separation and activity scheduling contracts
# ---------------------------------------------------------------------------


def test_bulk_backfill_is_dispatched_to_the_migration_bulk_queue():
    """Bulk work leaves the control-plane queue so it cannot starve it."""
    wf = MigrationWorkflow()
    harness = _Harness(script=[lambda: wf.approve_cutover("user-7")])

    _wf, _result, _raised = _run(harness, wf=wf)

    backfill_kwargs = harness.kwargs_for("backfill_migration_history")
    assert backfill_kwargs["task_queue"] == MIGRATION_BULK_TASK_QUEUE
    assert MIGRATION_BULK_TASK_QUEUE != MIGRATION_TASK_QUEUE


def test_control_plane_activities_are_not_pinned_to_another_queue():
    """Control-plane activities stay on the worker's own queue."""
    wf = MigrationWorkflow()
    harness = _Harness(script=[lambda: wf.approve_cutover("user-7")])

    _wf, _result, _raised = _run(harness, wf=wf)

    for name in (
        "profile_source_dataset",
        "build_field_mapping",
        "run_migration_dry_run",
        "reconcile_migration",
        "promote_migration_cutover",
    ):
        assert "task_queue" not in harness.kwargs_for(name), name


def test_every_activity_declares_a_start_to_close_timeout():
    """No migration activity is scheduled without a bound."""
    wf = MigrationWorkflow()
    harness = _Harness(script=[lambda: wf.approve_cutover("user-7")])

    _wf, _result, _raised = _run(harness, wf=wf)

    assert harness.call_kwargs, "no activities were scheduled"
    for name, kwargs in zip(harness.calls, harness.call_kwargs):
        assert kwargs.get("start_to_close_timeout") is not None, name
        assert kwargs.get("retry_policy") is not None, name


def test_long_running_activities_declare_a_heartbeat_timeout():
    """Long work must heartbeat so a worker restart resumes rather than restarts."""
    wf = MigrationWorkflow()
    harness = _Harness(
        script=[
            lambda: wf.approve_cutover("user-7"),
            lambda: wf.request_rollback("user-8"),
        ]
    )

    _wf, _result, _raised = _run(harness, wf=wf)

    for name in (
        "run_migration_dry_run",
        "backfill_migration_history",
        "reconcile_migration",
        "promote_migration_cutover",
        "rollback_migration",
    ):
        assert harness.kwargs_for(name).get("heartbeat_timeout") is not None, name


def test_backfill_follows_its_cursor_across_batches():
    """A cursor-driven backfill runs one batch per pass and then moves on."""
    cursors = [{"next_cursor": "batch-2"}, {"next_cursor": "batch-3"}, {}]
    wf = MigrationWorkflow()

    class _CursorHarness(_Harness):
        async def execute_activity(self, activity, *args, **kwargs):  # type: ignore[no-untyped-def]
            name = getattr(activity, "__name__", str(activity))
            if name == "backfill_migration_history":
                self.calls.append(name)
                self.call_kwargs.append(kwargs)
                self.call_args.append(kwargs.get("args", args))
                return cursors.pop(0)
            return await super().execute_activity(activity, *args, **kwargs)

    harness = _CursorHarness(script=[lambda: wf.approve_cutover("user-7")])
    wf, result, raised = _run(harness, wf=wf)

    assert raised is None
    assert harness.calls.count("backfill_migration_history") == 3
    assert result["backfill_batches_completed"] == 3


# ---------------------------------------------------------------------------
# Continue-as-new guard
# ---------------------------------------------------------------------------


def test_continue_as_new_carries_durable_state_forward():
    """When the SDK suggests it, the run hands its state to a fresh run."""
    harness = _Harness(continue_as_new_suggested=True)

    _wf, _result, raised = _run(harness)

    assert isinstance(raised, _ContinuedAsNew)
    tenant_id, migration_id, snapshot = raised.args_passed
    assert tenant_id == TENANT_ID
    assert migration_id == MIGRATION_ID
    assert snapshot["phase"] == PHASE_AWAITING_MAPPING
    assert snapshot["continued_runs"] == 1


def test_no_continue_as_new_when_the_sdk_does_not_suggest_it():
    """The guard does not fire on its own — it defers to the SDK's threshold."""
    harness = _Harness(continue_as_new_suggested=False)

    _wf, _result, raised = _run(harness)

    assert not isinstance(raised, _ContinuedAsNew)


def test_carried_state_resumes_the_migration_at_its_recorded_phase():
    """A continued run picks up where the previous run left off."""
    wf = MigrationWorkflow()
    harness = _Harness(script=[lambda: wf.approve_cutover("user-7")])
    carried = {
        "phase": PHASE_AWAITING_CUTOVER,
        "continued_runs": 3,
        "backfill_batches": 12,
        "steps": {"reconciliation": {"error_count": 0}},
    }

    wf, result, raised = _run(harness, wf=wf, carried_state=carried)

    assert raised is None
    # Profiling, mapping, dry-run and backfill are NOT re-run.
    assert "profile_source_dataset" not in harness.calls
    assert "backfill_migration_history" not in harness.calls
    assert "promote_migration_cutover" in harness.calls
    assert result["outcome"] == PHASE_COMPLETED
    assert result["backfill_batches_completed"] == 12


def _call_can_guard(
    wf: MigrationWorkflow,
    *,
    suggested: bool,
    handlers_finished: bool = True,
) -> BaseException | None:
    """Invoke the continue-as-new guard directly and return what it raised."""
    info = MagicMock()
    info.is_continue_as_new_suggested.return_value = suggested

    def _continue_as_new(*_a: Any, **kwargs: Any) -> None:
        raise _ContinuedAsNew(kwargs.get("args", []))

    with (
        patch.object(workflow, "logger", logging.getLogger("test.migration")),
        patch.object(workflow, "info", lambda: info),
        patch.object(workflow, "all_handlers_finished", lambda: handlers_finished),
        patch.object(workflow, "continue_as_new", _continue_as_new),
    ):
        try:
            asyncio.run(wf._maybe_continue_as_new())
        except BaseException as exc:  # noqa: BLE001 — the test asserts on it
            return exc
    return None


def test_continue_as_new_is_skipped_while_paused():
    """A paused migration does not change run id underneath the operator."""
    paused = _instance(phase=PHASE_AWAITING_CUTOVER, paused=True)
    running = _instance(phase=PHASE_AWAITING_CUTOVER, paused=False)

    assert _call_can_guard(paused, suggested=True) is None
    assert isinstance(_call_can_guard(running, suggested=True), _ContinuedAsNew)


def test_continue_as_new_is_skipped_while_a_handler_is_in_flight():
    """Handing over mid-handler would drop that handler's work."""
    wf = _instance(phase=PHASE_AWAITING_CUTOVER)

    assert _call_can_guard(wf, suggested=True, handlers_finished=False) is None
    assert isinstance(_call_can_guard(wf, suggested=True), _ContinuedAsNew)


def test_continue_as_new_is_skipped_in_a_terminal_phase():
    """A finished migration returns its result rather than continuing as new."""
    wf = _instance(phase=PHASE_COMPLETED)

    assert _call_can_guard(wf, suggested=True) is None


def test_promotion_phase_is_reached_only_through_the_gate():
    """PHASE_PROMOTING is never entered without a recorded approval."""
    wf = _instance(phase=PHASE_AWAITING_CUTOVER)

    with _with_workflow_logger():
        wf.request_rollback("user-1")

    assert wf._rollback_request is None
    assert wf._phase != PHASE_PROMOTING
    assert wf.status()["rejected_signal_count"] == 1
