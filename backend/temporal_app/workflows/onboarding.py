"""Onboarding domain Temporal workflow definitions.

Workflows:
  AgencyOnboardingWorkflow    — orchestrates the full 31-step Go-Live pipeline.
  WorkspaceActivationWorkflow — runs the final activation sequence.

The 31-step pipeline is defined in:
  Adaptix-Core-Service/core/backend/core_app/onboarding/step_machine.py

This workflow drives the pipeline via the Core Service HTTP API. It does NOT
call the step machine directly — all state changes go through the authenticated
API to preserve service boundary isolation and ensure audit logging.

Orchestration strategy:
  AgencyOnboardingWorkflow orchestrates at the milestone level. It calls
  WorkspaceActivationWorkflow as a child workflow for the final activation
  sequence. Individual step completions are activity calls.

Tenant isolation:
  tenant_id is always passed explicitly. No workflow reads a tenant_id from
  shared state — each workflow execution is scoped to a single tenant.

Long-running workflow notes:
  Agency onboarding can span days or weeks (waiting for BAA, payer enrollment,
  migration, etc.). The workflow uses await workflow.sleep() for waiting periods
  and relies on external signals (from the API) to advance when human-gated
  steps are complete. The workflow execution timeout is set to 90 days.

  Temporal's workflow execution history is durable across server restarts,
  so a long wait does not lose state.
"""

from __future__ import annotations

import logging
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from temporal_app.activities.onboarding_activities import (
        advance_onboarding_step,
        complete_onboarding_step,
        configure_billing_provider_identity,
        get_onboarding_case,
        provision_tenant,
        run_go_live_readiness_check,
        send_go_live_notification,
        unlock_workspace,
    )
    from temporal_app.config import DEFAULT_RETRY_POLICY

logger = logging.getLogger(__name__)

# Poll interval for waiting on human-gated steps.
# The workflow will re-check readiness every POLL_INTERVAL_HOURS.
_POLL_INTERVAL_HOURS: int = 4

# Maximum wait cycles before failing the onboarding workflow.
# 90 days / 4 hours = 540 cycles.
_MAX_WAIT_CYCLES: int = 540


@workflow.defn
class WorkspaceActivationWorkflow:
    """Final workspace activation sequence for a newly onboarded agency.

    Input:  tenant_id (str)
    Output: dict — workspace state after unlock.

    Steps:
      1. Run Go-Live readiness check.
      2. If not ready: fail with the list of blocking items.
      3. Configure billing provider identity (confirm task complete).
      4. Unlock the workspace.
      5. Send the go-live notification email to the agency admin.

    This workflow is called as a child workflow by AgencyOnboardingWorkflow
    and can also be called directly by the founder to activate a tenant
    that has met all readiness criteria.
    """

    @workflow.run
    async def run(self, tenant_id: str, admin_email: str) -> dict:
        workflow.logger.info(
            "WorkspaceActivationWorkflow started tenant_id=%s", tenant_id
        )

        # Step 1: Run readiness check.
        readiness = await workflow.execute_activity(
            run_go_live_readiness_check,
            tenant_id,
            start_to_close_timeout=timedelta(minutes=3),
            retry_policy=DEFAULT_RETRY_POLICY,
        )

        if not readiness.get("go_live_ready"):
            blocking_items = readiness.get("blocking_items", [])
            workflow.logger.error(
                "WorkspaceActivationWorkflow tenant_id=%s not_ready blocking=%s",
                tenant_id,
                blocking_items,
            )
            raise RuntimeError(
                f"WorkspaceActivationWorkflow: tenant {tenant_id} is not ready for activation. "
                f"Blocking items: {blocking_items}. "
                f"Overall score: {readiness.get('overall_score')}/100 "
                f"(minimum required: 80)."
            )

        # Step 2: Configure billing provider identity.
        await workflow.execute_activity(
            configure_billing_provider_identity,
            tenant_id,
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=DEFAULT_RETRY_POLICY,
        )

        # Step 3: Unlock workspace.
        workspace = await workflow.execute_activity(
            unlock_workspace,
            tenant_id,
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=DEFAULT_RETRY_POLICY,
        )

        # Step 4: Send go-live notification.
        await workflow.execute_activity(
            send_go_live_notification,
            args=[tenant_id, admin_email],
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(seconds=2),
                maximum_attempts=3,
                # Email failure should not block activation success.
                # If this activity keeps failing after 3 attempts, the
                # operator will see the workflow history and can resend manually.
                non_retryable_error_types=["ValidationError", "AuthorizationError"],
            ),
        )

        workflow.logger.info(
            "WorkspaceActivationWorkflow completed tenant_id=%s is_locked=%s",
            tenant_id,
            workspace.get("is_locked"),
        )
        return workspace


@workflow.defn
class AgencyOnboardingWorkflow:
    """Orchestrate the full 31-step Go-Live pipeline for a new agency.

    Input:  tenant_id (str)
    Output: dict — final onboarding state summary.

    This workflow drives the milestone-level orchestration of the Go-Live
    pipeline. Individual tasks within each milestone are completed by agency
    admins and founders via the UI — this workflow monitors progress and
    drives automated steps.

    Pipeline milestones handled by this workflow:
      1. Tenant provisioning confirmation.
      2. Case state verification.
      3. Polling for human-gated milestones (BAA, payer setup, staff setup).
      4. Triggering workspace activation via WorkspaceActivationWorkflow.

    Steps that require human input (signing BAA, setting up payers, inviting
    staff) are human-gated: the workflow polls the Go-Live readiness score
    on a 4-hour interval. The founder/admin complete these steps in the UI;
    when the readiness score crosses the threshold, the workflow proceeds.

    The workflow execution timeout is 90 days to accommodate agencies that
    take time to complete their setup.

    Signals (future enhancement):
      This workflow is signal-ready — a "steps_updated" signal from the
      Core Service can interrupt the polling sleep and advance the workflow
      immediately when a step is completed. Signal handling is not
      implemented in this initial version to keep the implementation simple.
    """

    @workflow.run
    async def run(self, tenant_id: str) -> dict:
        workflow.logger.info("AgencyOnboardingWorkflow started tenant_id=%s", tenant_id)

        # Step 1: Confirm tenant provisioning.
        await workflow.execute_activity(
            provision_tenant,
            tenant_id,
            start_to_close_timeout=timedelta(minutes=3),
            retry_policy=DEFAULT_RETRY_POLICY,
        )

        # Step 2: Get the current Go-Live case.
        case = await workflow.execute_activity(
            get_onboarding_case,
            tenant_id,
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=DEFAULT_RETRY_POLICY,
        )

        case_id: str = case.get("case_id", "")
        workflow.logger.info(
            "AgencyOnboardingWorkflow case_id=%s tenant_id=%s",
            case_id,
            tenant_id,
        )

        # Step 3: Advance the lead_captured and agency_qualified steps to
        # in_progress. These are automatically advanced for programmatically
        # created cases — the workflow confirms the advance happened.
        for step_key in ("lead_captured", "agency_qualified"):
            await workflow.execute_activity(
                advance_onboarding_step,
                args=[case_id, step_key],
                start_to_close_timeout=timedelta(minutes=2),
                retry_policy=DEFAULT_RETRY_POLICY,
            )

        # Step 4: Poll for readiness. Human-gated steps (BAA, payer setup,
        # staff setup, RBAC, billing profile) are completed in the UI.
        # We poll every POLL_INTERVAL_HOURS until the score >= 80.
        admin_email: str = case.get("admin_email", "")

        for cycle in range(_MAX_WAIT_CYCLES):
            readiness = await workflow.execute_activity(
                run_go_live_readiness_check,
                tenant_id,
                start_to_close_timeout=timedelta(minutes=3),
                retry_policy=DEFAULT_RETRY_POLICY,
            )

            score: float = readiness.get("overall_score", 0)
            go_live_ready: bool = readiness.get("go_live_ready", False)

            workflow.logger.info(
                "AgencyOnboardingWorkflow poll cycle=%d tenant_id=%s score=%s ready=%s",
                cycle + 1,
                tenant_id,
                score,
                go_live_ready,
            )

            if go_live_ready:
                # Score crossed the threshold — proceed to activation.
                break

            if cycle < _MAX_WAIT_CYCLES - 1:
                await workflow.sleep(timedelta(hours=_POLL_INTERVAL_HOURS))
        else:
            # Exhausted all wait cycles without reaching readiness.
            raise RuntimeError(
                f"AgencyOnboardingWorkflow: tenant {tenant_id} did not reach "
                f"go-live readiness within "
                f"{_MAX_WAIT_CYCLES * _POLL_INTERVAL_HOURS} hours. "
                "Operator action required. Check the Go-Live Command Center."
            )

        # Step 5: Run workspace activation as a child workflow.
        activation_result: dict = await workflow.execute_child_workflow(
            WorkspaceActivationWorkflow,  # type: ignore[arg-type]
            args=[tenant_id, admin_email],
            id=f"workspace-activation-{tenant_id}",
            execution_timeout=timedelta(hours=1),
        )

        # Step 6: Mark the final pipeline steps as complete.
        for step_key in ("training_completed", "go_live_achieved"):
            try:
                await workflow.execute_activity(
                    complete_onboarding_step,
                    args=[case_id, step_key],
                    start_to_close_timeout=timedelta(minutes=2),
                    retry_policy=DEFAULT_RETRY_POLICY,
                )
            except Exception as exc:
                # These steps may already be complete (idempotent API);
                # log but do not fail the overall workflow.
                workflow.logger.warning(
                    "AgencyOnboardingWorkflow step_complete_failed step=%s error=%s",
                    step_key,
                    type(exc).__name__,
                )

        workflow.logger.info(
            "AgencyOnboardingWorkflow completed tenant_id=%s", tenant_id
        )
        return {
            "tenant_id": tenant_id,
            "case_id": case_id,
            "activation": activation_result,
        }
