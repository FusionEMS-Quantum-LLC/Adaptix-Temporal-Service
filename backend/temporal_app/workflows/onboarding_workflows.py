"""Onboarding domain Temporal workflows.

Orchestrates the agency activation and workspace setup lifecycle. The
30-step go-live state machine is modeled as a durable Temporal workflow
so individual steps can fail, retry, and resume without losing progress.

Workflows registered here:
  - AgencyOnboardingWorkflow — complete agency activation sequence

PHI-safe: no patient data is processed in onboarding workflows. Only
tenant_id, agency_id, and agency_name are carried as workflow inputs.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

with workflow.unsafe.imports_passed_through():
    from temporal_app.activities.onboarding_activities import (
        complete_onboarding_step,
        configure_billing_provider_identity,
        get_onboarding_case,
        provision_case,
        run_go_live_readiness_check,
        send_go_live_notification,
        unlock_workspace,
    )
    from temporal_app.config import DEFAULT_RETRY_POLICY

_ACTIVITY_TIMEOUT = timedelta(minutes=5)
_LONG_ACTIVITY_TIMEOUT = timedelta(minutes=15)

# Onboarding-specific retry — fewer retries for user-state steps
_ONBOARDING_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=2),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=5),
    maximum_attempts=5,
    non_retryable_error_types=["ValidationError", "AuthorizationError"],
)

# --------------------------------------------------------------------------
# Go-Live readiness gate
# --------------------------------------------------------------------------

#: Minimum ``overall_score`` a Go-Live case must reach before the workspace may
#: be unlocked/activated. Mirrors ``GO_LIVE_THRESHOLD`` in Core
#: (``core_app/go_live/service.py``) and the documented AgencyOnboardingWorkflow
#: contract.
_GO_LIVE_READINESS_THRESHOLD = 80

#: ``overall_status`` value Core returns when at least one readiness category is
#: blocked (``core_app/onboarding/readiness_engine.py``). A blocked case can never
#: go live regardless of the weighted score.
_READINESS_STATUS_BLOCKED = "blocked"

#: Error ``type`` raised when the gate denies activation. Non-retryable: a low
#: readiness score is a business-state condition, not a transient failure.
_READINESS_GATE_ERROR_TYPE = "GoLiveReadinessNotMet"


def _readiness_gate_denial(readiness: Any) -> str | None:
    """Return why activation must be denied, or ``None`` when it may proceed.

    Fail-closed. Activation is permitted only when ALL of the following hold on
    the readiness snapshot returned by
    ``GET /api/v1/go-live/cases/{case_id}/readiness``:

      * the payload is a mapping,
      * ``overall_score`` is a real number >= ``_GO_LIVE_READINESS_THRESHOLD``,
      * ``overall_status`` is not ``"blocked"``,
      * ``go_live_ready`` is not explicitly ``False``.

    A missing, null, or non-numeric ``overall_score`` denies activation — the
    workflow must never unlock a workspace on an unreadable score. ``bool`` is
    rejected as a score so a stray ``True`` cannot clear the numeric threshold
    through Python's ``bool``-is-``int`` coercion.

    The returned reason carries no PHI — only score, status, and blocker codes.
    """
    if not isinstance(readiness, dict):
        return (
            "readiness check returned no usable snapshot "
            f"(type={type(readiness).__name__})"
        )

    score = readiness.get("overall_score")
    if isinstance(score, bool) or not isinstance(score, (int, float)):
        return f"overall_score is missing or non-numeric (value={score!r})"

    status = readiness.get("overall_status")
    if status == _READINESS_STATUS_BLOCKED:
        blockers = readiness.get("all_blockers") or []
        return f"overall_status={status!r} with blockers={list(blockers)[:10]!r}"

    if readiness.get("go_live_ready") is False:
        return f"go_live_ready is False (overall_score={score})"

    if score < _GO_LIVE_READINESS_THRESHOLD:
        return (
            f"overall_score={score} is below the required "
            f"{_GO_LIVE_READINESS_THRESHOLD}"
        )

    return None


@workflow.defn
class AgencyOnboardingWorkflow:
    """Orchestrate the complete agency activation sequence via the 31-step
    Go-Live Command Center state machine.

    Workflow ID convention: "agency-onboarding-{tenant_id}"
    Task queue: onboarding

    Input:
        tenant_id (str):    The Adaptix tenant UUID.
        admin_email (str):  Agency admin email for the go-live notification.
                            Not logged.

    Steps:
        1. Fetch the existing Go-Live case for the tenant.
        2. Confirm tenant provisioning (mark tenant_provisioned + workspace_created).
        3. Run go-live readiness check and ENFORCE the gate (score >= 80,
           status not "blocked") — the workflow fails here if the case is not
           ready, and nothing after this point runs.
        4. Configure billing provider identity task completion.
        5. Advance and complete key pipeline steps in sequence.
        6. Unlock the workspace (activates the agency).
        7. Send go-live notification email to the agency admin.

    Result:
        dict: Activation summary with step completion timestamps.

    Readiness gate:
        Steps 4-7 (including the workspace unlock that activates the agency)
        execute ONLY when ``_readiness_gate_denial`` clears the step-3 snapshot.
        Otherwise the workflow raises a non-retryable ``ApplicationError`` of
        type ``GoLiveReadinessNotMet`` and the workspace stays locked. The gate
        is fail-closed: an unreadable/missing score denies activation. Core's
        readiness engine scores only case/step/integration state that this
        workflow does not itself write in steps 4-7, so re-running after the
        blocking readiness items are resolved is the recovery path.

    The workflow is idempotent — re-running with the same tenant_id is safe.
    All Core Service state machine endpoints are guarded by idempotency.
    """

    @workflow.run
    async def run(
        self,
        tenant_id: str,
        admin_email: str,
    ) -> dict[str, Any]:
        workflow.logger.info(
            "AgencyOnboardingWorkflow starting tenant_id=%s",
            tenant_id,
        )

        results: dict[str, Any] = {"tenant_id": tenant_id}

        # Step 1: fetch the Go-Live case
        case: dict[str, Any] = await workflow.execute_activity(
            get_onboarding_case,
            tenant_id,
            start_to_close_timeout=_ACTIVITY_TIMEOUT,
            retry_policy=DEFAULT_RETRY_POLICY,
        )
        case_id: str = (case or {}).get("case_id", "")
        results["case_id"] = case_id
        if not case_id:
            raise RuntimeError(
                f"AgencyOnboardingWorkflow: no Go-Live case found for tenant {tenant_id}."
            )
        workflow.logger.info(
            "AgencyOnboardingWorkflow case_fetched tenant_id=%s case_id=%s",
            tenant_id,
            case_id,
        )

        # Step 2: provision the tenant + workspace for this case (founder-gated,
        # idempotent in Core).
        provisioned: dict[str, Any] = await workflow.execute_activity(
            provision_case,
            case_id,
            start_to_close_timeout=_ACTIVITY_TIMEOUT,
            retry_policy=_ONBOARDING_RETRY,
        )
        results["provisioned"] = provisioned
        workflow.logger.info(
            "AgencyOnboardingWorkflow tenant_provisioned tenant_id=%s",
            tenant_id,
        )

        # Step 3: run go-live readiness check (case-scoped)
        readiness: dict[str, Any] = await workflow.execute_activity(
            run_go_live_readiness_check,
            case_id,
            start_to_close_timeout=_ACTIVITY_TIMEOUT,
            retry_policy=DEFAULT_RETRY_POLICY,
        )
        results["readiness"] = readiness
        workflow.logger.info(
            "AgencyOnboardingWorkflow readiness_checked tenant_id=%s score=%s "
            "status=%s",
            tenant_id,
            (readiness or {}).get("overall_score"),
            (readiness or {}).get("overall_status"),
        )

        # Step 3b: ENFORCE the readiness gate. Fail closed — the workspace is
        # never unlocked/activated for a case that has not cleared go-live
        # readiness. Nothing below this point runs when the gate denies.
        denial_reason = _readiness_gate_denial(readiness)
        if denial_reason is not None:
            workflow.logger.error(
                "AgencyOnboardingWorkflow readiness_gate_denied tenant_id=%s "
                "case_id=%s reason=%s — workspace NOT unlocked",
                tenant_id,
                case_id,
                denial_reason,
            )
            raise ApplicationError(
                "AgencyOnboardingWorkflow: go-live readiness gate denied "
                f"activation for tenant {tenant_id} (case {case_id}): "
                f"{denial_reason}. Workspace was NOT unlocked. Resolve the "
                "blocking readiness items and re-run this workflow.",
                type=_READINESS_GATE_ERROR_TYPE,
                non_retryable=True,
            )

        results["readiness_gate"] = {
            "passed": True,
            "threshold": _GO_LIVE_READINESS_THRESHOLD,
            "reason": None,
        }
        workflow.logger.info(
            "AgencyOnboardingWorkflow readiness_gate_passed tenant_id=%s case_id=%s",
            tenant_id,
            case_id,
        )

        # Step 4: configure billing provider identity
        billing: dict[str, Any] = await workflow.execute_activity(
            configure_billing_provider_identity,
            tenant_id,
            start_to_close_timeout=_LONG_ACTIVITY_TIMEOUT,
            retry_policy=_ONBOARDING_RETRY,
        )
        results["billing"] = billing
        workflow.logger.info(
            "AgencyOnboardingWorkflow billing_configured tenant_id=%s",
            tenant_id,
        )

        # Step 5: complete key pipeline steps (Core completes a step directly;
        # there is no separate advance hop).
        for step_key in [
            "workspace_activated",
            "billing_configured",
            "first_user_invited",
        ]:
            await workflow.execute_activity(
                complete_onboarding_step,
                args=[case_id, step_key],
                start_to_close_timeout=_ACTIVITY_TIMEOUT,
                retry_policy=_ONBOARDING_RETRY,
            )
        workflow.logger.info(
            "AgencyOnboardingWorkflow pipeline_steps_completed tenant_id=%s",
            tenant_id,
        )

        # Step 6: unlock workspace
        unlock: dict[str, Any] = await workflow.execute_activity(
            unlock_workspace,
            tenant_id,
            start_to_close_timeout=_ACTIVITY_TIMEOUT,
            retry_policy=_ONBOARDING_RETRY,
        )
        results["workspace_unlocked"] = unlock
        workflow.logger.info(
            "AgencyOnboardingWorkflow workspace_unlocked tenant_id=%s is_locked=%s",
            tenant_id,
            unlock.get("is_locked"),
        )

        # Step 7: send go-live notification
        notification: dict[str, Any] = await workflow.execute_activity(
            send_go_live_notification,
            args=[tenant_id, admin_email],
            start_to_close_timeout=_ACTIVITY_TIMEOUT,
            retry_policy=DEFAULT_RETRY_POLICY,
        )
        results["notification"] = notification

        workflow.logger.info(
            "AgencyOnboardingWorkflow complete tenant_id=%s",
            tenant_id,
        )
        return results
