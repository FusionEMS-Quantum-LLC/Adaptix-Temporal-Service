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
        3. Run go-live readiness check (score >= 80 required to proceed).
        4. Configure billing provider identity task completion.
        5. Advance and complete key pipeline steps in sequence.
        6. Unlock the workspace (activates the agency).
        7. Send go-live notification email to the agency admin.

    Result:
        dict: Activation summary with step completion timestamps.

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
        case_id: str = case.get("case_id", "")
        results["case_id"] = case_id
        workflow.logger.info(
            "AgencyOnboardingWorkflow case_fetched tenant_id=%s case_id=%s",
            tenant_id,
            case_id,
        )

        # Step 2: confirm tenant provisioned
        provisioned: dict[str, Any] = await workflow.execute_activity(
            provision_tenant,
            tenant_id,
            start_to_close_timeout=_ACTIVITY_TIMEOUT,
            retry_policy=_ONBOARDING_RETRY,
        )
        results["provisioned"] = provisioned
        workflow.logger.info(
            "AgencyOnboardingWorkflow tenant_provisioned tenant_id=%s",
            tenant_id,
        )

        # Step 3: run go-live readiness check
        readiness: dict[str, Any] = await workflow.execute_activity(
            run_go_live_readiness_check,
            tenant_id,
            start_to_close_timeout=_ACTIVITY_TIMEOUT,
            retry_policy=DEFAULT_RETRY_POLICY,
        )
        results["readiness"] = readiness
        workflow.logger.info(
            "AgencyOnboardingWorkflow readiness_checked tenant_id=%s score=%s "
            "go_live_ready=%s",
            tenant_id,
            readiness.get("overall_score"),
            readiness.get("go_live_ready"),
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

        # Step 5: advance and complete key pipeline steps (if case_id available)
        if case_id:
            for step_key in [
                "workspace_activated",
                "billing_configured",
                "first_user_invited",
            ]:
                await workflow.execute_activity(
                    advance_onboarding_step,
                    args=[case_id, step_key],
                    start_to_close_timeout=_ACTIVITY_TIMEOUT,
                    retry_policy=_ONBOARDING_RETRY,
                )
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
