"""Tests for the AgencyOnboardingWorkflow go-live readiness gate.

Regression coverage for the audit finding "readiness gate not enforced":
AgencyOnboardingWorkflow fetched the go-live readiness score but never gated on
it, so the workspace was unlocked/activated unconditionally even though the
documented contract requires ``overall_score >= 80``.

What these tests prove:
  - ``_readiness_gate_denial`` denies below-threshold, blocked, explicitly
    not-ready, missing, null, non-numeric, bool, and non-mapping snapshots
    (fail-closed), and allows only a real numeric score at or above 80.
  - Running the real ``AgencyOnboardingWorkflow.run`` body against a recording
    activity stub never reaches ``unlock_workspace`` (nor billing config, step
    completion, or the go-live notification) when the gate denies, and raises a
    non-retryable ``ApplicationError`` of type ``GoLiveReadinessNotMet``.
  - The same body DOES reach ``unlock_workspace`` when the score clears 80, so
    the gate is not a blanket denial.

What these tests do NOT prove:
  - Execution against a real Temporal server or history-replay determinism.
  - That the Core readiness endpoint returns the expected payload in production.

Test technique: ``workflow.execute_activity`` and ``workflow.logger`` are
patched on the ``temporalio.workflow`` module so the workflow coroutine can run
outside a Temporal worker. The workflow body under test is the real one — only
the activity boundary and the workflow-context logger are substituted.
"""

from __future__ import annotations

import logging
from typing import Any
from unittest.mock import patch

import pytest
from temporalio import workflow
from temporalio.exceptions import ApplicationError


# ---------------------------------------------------------------------------
# Activity-boundary harness
# ---------------------------------------------------------------------------


class _ActivityRecorder:
    """Records every activity the workflow schedules and returns canned results.

    ``execute_activity`` is called by the workflow either positionally
    (``activity, arg``) or with ``args=[...]``; both forms are recorded by the
    activity function's name.
    """

    def __init__(self, results: dict[str, Any]) -> None:
        self._results = results
        self.calls: list[str] = []

    async def execute_activity(
        self,
        activity: Any,
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        name = getattr(activity, "__name__", str(activity))
        self.calls.append(name)
        return self._results.get(name, {})


def _run_workflow(readiness: Any) -> tuple[_ActivityRecorder, BaseException | None]:
    """Run the real AgencyOnboardingWorkflow body with a stubbed activity edge.

    Returns the recorder (so callers can assert which activities ran) and the
    exception the workflow raised, or ``None`` when it completed.
    """
    import asyncio

    from temporal_app.workflows.onboarding_workflows import AgencyOnboardingWorkflow

    recorder = _ActivityRecorder(
        {
            "get_onboarding_case": {"case_id": "case-1111"},
            "provision_case": {"provisioned": True},
            "run_go_live_readiness_check": readiness,
            "configure_billing_provider_identity": {"configured": True},
            "complete_onboarding_step": {"status": "complete"},
            "unlock_workspace": {"is_locked": False},
            "send_go_live_notification": {"sent": True},
        }
    )

    raised: BaseException | None = None
    with (
        patch.object(workflow, "execute_activity", recorder.execute_activity),
        patch.object(workflow, "logger", logging.getLogger("test.onboarding")),
    ):
        try:
            asyncio.run(
                AgencyOnboardingWorkflow().run(
                    "tenant-aaaa",
                    "admin@agency.test",
                )
            )
        except BaseException as exc:  # noqa: BLE001 — the test asserts on it
            raised = exc

    return recorder, raised


# ---------------------------------------------------------------------------
# Gate predicate: denials (fail-closed)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("readiness", "label"),
    [
        ({"overall_score": 79, "overall_status": "ready"}, "one below threshold"),
        ({"overall_score": 0, "overall_status": "ready"}, "zero score"),
        ({"overall_score": -5, "overall_status": "ready"}, "negative score"),
        ({"overall_status": "ready"}, "overall_score absent"),
        ({"overall_score": None, "overall_status": "ready"}, "overall_score null"),
        ({"overall_score": "95", "overall_status": "ready"}, "score is a string"),
        ({"overall_score": True, "overall_status": "ready"}, "score is bool True"),
        (
            {
                "overall_score": 95,
                "overall_status": "blocked",
                "all_blockers": ["legal:x"],
            },
            "blocked status despite high score",
        ),
        (
            {"overall_score": 95, "go_live_ready": False},
            "explicitly not go_live_ready despite high score",
        ),
        ({}, "empty snapshot"),
        (None, "no snapshot at all"),
        ([], "list instead of mapping"),
        ("ready", "string instead of mapping"),
    ],
)
def test_readiness_gate_denies(readiness: Any, label: str):
    """The gate denies activation for every not-ready or unreadable snapshot."""
    from temporal_app.workflows.onboarding_workflows import _readiness_gate_denial

    reason = _readiness_gate_denial(readiness)
    assert reason is not None, f"gate must deny: {label}"
    assert isinstance(reason, str) and reason, f"denial needs a reason: {label}"


# ---------------------------------------------------------------------------
# Gate predicate: allows
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("readiness", "label"),
    [
        ({"overall_score": 80, "overall_status": "ready"}, "exactly at threshold"),
        ({"overall_score": 80.0, "overall_status": "ready"}, "float at threshold"),
        ({"overall_score": 100, "overall_status": "ready"}, "perfect score"),
        (
            {"overall_score": 92, "overall_status": "ready_with_warnings"},
            "ready_with_warnings is not blocking",
        ),
        (
            {"overall_score": 88, "overall_status": "ready", "go_live_ready": True},
            "tenant-scoped envelope carrying go_live_ready=True",
        ),
        ({"overall_score": 85}, "no overall_status key (case route may omit it)"),
    ],
)
def test_readiness_gate_allows(readiness: Any, label: str):
    """The gate allows activation only for a real score at or above 80."""
    from temporal_app.workflows.onboarding_workflows import _readiness_gate_denial

    assert _readiness_gate_denial(readiness) is None, f"gate must allow: {label}"


def test_threshold_matches_core_go_live_threshold():
    """The worker threshold is the documented Core GO_LIVE_THRESHOLD (80)."""
    from temporal_app.workflows.onboarding_workflows import (
        _GO_LIVE_READINESS_THRESHOLD,
    )

    assert _GO_LIVE_READINESS_THRESHOLD == 80


# ---------------------------------------------------------------------------
# Workflow body: the gate actually stops activation
# ---------------------------------------------------------------------------


def test_workflow_does_not_unlock_workspace_when_score_below_threshold():
    """A below-threshold score never reaches unlock_workspace or activation."""
    recorder, raised = _run_workflow({"overall_score": 42, "overall_status": "ready"})

    assert "run_go_live_readiness_check" in recorder.calls
    assert "unlock_workspace" not in recorder.calls
    assert "configure_billing_provider_identity" not in recorder.calls
    assert "complete_onboarding_step" not in recorder.calls
    assert "send_go_live_notification" not in recorder.calls

    assert isinstance(raised, ApplicationError)
    assert raised.type == "GoLiveReadinessNotMet"
    assert raised.non_retryable is True


def test_workflow_does_not_unlock_workspace_when_score_is_missing():
    """Fail-closed: an unreadable readiness snapshot blocks activation."""
    recorder, raised = _run_workflow({"overall_status": "ready"})

    assert "unlock_workspace" not in recorder.calls
    assert isinstance(raised, ApplicationError)
    assert raised.type == "GoLiveReadinessNotMet"


def test_workflow_does_not_unlock_workspace_when_status_blocked():
    """A blocked case is denied even when the weighted score clears 80."""
    recorder, raised = _run_workflow(
        {
            "overall_score": 96,
            "overall_status": "blocked",
            "all_blockers": ["legal:baa"],
        }
    )

    assert "unlock_workspace" not in recorder.calls
    assert isinstance(raised, ApplicationError)
    assert raised.type == "GoLiveReadinessNotMet"


def test_workflow_unlocks_workspace_when_readiness_passes():
    """A ready case still completes the full activation sequence."""
    recorder, raised = _run_workflow({"overall_score": 88, "overall_status": "ready"})

    assert raised is None, f"ready case must not raise: {raised!r}"
    assert "unlock_workspace" in recorder.calls
    assert "send_go_live_notification" in recorder.calls
    # The gate runs before activation, never after.
    assert recorder.calls.index("run_go_live_readiness_check") < recorder.calls.index(
        "unlock_workspace"
    )
