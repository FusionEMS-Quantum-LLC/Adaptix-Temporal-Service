"""Onboarding domain activities for Temporal workflows.

Activities call the Adaptix Core Service onboarding and go-live endpoints
to orchestrate tenant provisioning and workspace activation.

Tenant isolation: every activity accepts tenant_id as an explicit parameter.
The Core Service routes derive tenant_id from the JWT and validate it against
any tenant_id in the request body — the service token used here must carry
the platform-level identity (not a user JWT). The ADAPTIX_SERVICE_TOKEN must
be provisioned with the founding/platform role to write onboarding state.

PHI-safe: patient data does not flow through onboarding activities.
Tenant configuration (name, slug, admin email) is treated as sensitive
and is not logged beyond tenant_id.

The 31-step Go-Live pipeline is defined in:
  Adaptix-Core-Service/core/backend/core_app/onboarding/step_machine.py

This worker drives the pipeline through the HTTP API, not by calling the
step machine directly. This preserves service boundary isolation and ensures
all state changes go through the authenticated API with audit logging.
"""

from __future__ import annotations

import logging
from typing import Any

import httpx
from temporalio import activity

from temporal_app.config import (
    ADAPTIX_API_BASE,
    ADAPTIX_SERVICE_TOKEN,
    ACTIVITY_HTTP_TIMEOUT_S,
)

logger = logging.getLogger(__name__)


def _auth_header() -> dict[str, str]:
    token = ADAPTIX_SERVICE_TOKEN
    if not token:
        raise RuntimeError(
            "ADAPTIX_SERVICE_TOKEN is not configured. "
            "This error is non-retryable — fix the deployment."
        )
    return {"Authorization": f"Bearer {token}"}


def _api_url(path: str) -> str:
    if not ADAPTIX_API_BASE:
        raise RuntimeError(
            "ADAPTIX_API_BASE is not configured. "
            "This error is non-retryable — fix the deployment."
        )
    return f"{ADAPTIX_API_BASE}{path}"


def _raise_for_non_retryable(exc: httpx.HTTPStatusError) -> None:
    status = exc.response.status_code
    if status in (400, 422):
        raise ValueError(
            f"ValidationError: Onboarding API returned {status}. "
            f"Response: {exc.response.text[:500]}"
        ) from exc
    if status in (401, 403):
        raise PermissionError(
            f"AuthorizationError: Onboarding API returned {status}. "
            "Check ADAPTIX_SERVICE_TOKEN."
        ) from exc
    raise exc


# ---------------------------------------------------------------------------
# Go-Live case management
# ---------------------------------------------------------------------------


@activity.defn
async def get_onboarding_case(tenant_id: str) -> dict[str, Any]:
    """Fetch the Go-Live Command Center case for a tenant.

    Calls: GET /api/v1/go-live/cases?tenant_id={tenant_id}

    Returns the active case record including current steps state,
    go-live score, and next actionable steps. Returns None if no
    case exists yet.
    """
    activity.heartbeat("fetching_onboarding_case")
    logger.info("onboarding_activity.get_onboarding_case tenant_id=%s", tenant_id)

    async with httpx.AsyncClient(timeout=ACTIVITY_HTTP_TIMEOUT_S) as client:
        try:
            resp = await client.get(
                _api_url("/api/v1/go-live/cases"),
                params={"tenant_id": tenant_id},
                headers=_auth_header(),
            )
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            logger.error(
                "onboarding_activity.get_onboarding_case tenant_id=%s status=%s",
                tenant_id,
                exc.response.status_code,
            )
            _raise_for_non_retryable(exc)

    return resp.json()


@activity.defn
async def advance_onboarding_step(
    case_id: str,
    step_key: str,
) -> dict[str, Any]:
    """Advance a Go-Live pipeline step to in_progress.

    Calls: POST /api/v1/go-live/cases/{case_id}/steps/{step_key}/advance

    The Core Service validates predecessor completion before advancing.
    Returns the updated step record.

    step_key: canonical step key from the STEP_CATALOG, e.g.
              "tenant_provisioned", "workspace_created", "admin_invited".
    """
    activity.heartbeat("advancing_onboarding_step")
    logger.info(
        "onboarding_activity.advance_step case_id=%s step_key=%s",
        case_id,
        step_key,
    )

    async with httpx.AsyncClient(timeout=ACTIVITY_HTTP_TIMEOUT_S) as client:
        try:
            resp = await client.post(
                _api_url(
                    f"/api/v1/go-live/cases/{case_id}/steps/{step_key}/advance"
                ),
                headers=_auth_header(),
            )
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            logger.error(
                "onboarding_activity.advance_step case_id=%s step_key=%s status=%s",
                case_id,
                step_key,
                exc.response.status_code,
            )
            _raise_for_non_retryable(exc)

    return resp.json()


@activity.defn
async def complete_onboarding_step(
    case_id: str,
    step_key: str,
) -> dict[str, Any]:
    """Mark a Go-Live pipeline step complete.

    Calls: POST /api/v1/go-live/cases/{case_id}/steps/{step_key}/complete

    Used by the AgencyOnboardingWorkflow to drive the 31-step pipeline
    through programmatic completion as each sub-workflow or activity confirms
    the underlying operation succeeded.

    The Core Service enforces:
      - Predecessor completion validation
      - Idempotency (re-completing a step is a no-op)
      - Audit logging via core_audit_logs
    """
    activity.heartbeat("completing_onboarding_step")
    logger.info(
        "onboarding_activity.complete_step case_id=%s step_key=%s",
        case_id,
        step_key,
    )

    async with httpx.AsyncClient(timeout=ACTIVITY_HTTP_TIMEOUT_S) as client:
        try:
            resp = await client.post(
                _api_url(
                    f"/api/v1/go-live/cases/{case_id}/steps/{step_key}/complete"
                ),
                headers=_auth_header(),
            )
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            logger.error(
                "onboarding_activity.complete_step case_id=%s step_key=%s status=%s",
                case_id,
                step_key,
                exc.response.status_code,
            )
            _raise_for_non_retryable(exc)

    return resp.json()


@activity.defn
async def provision_tenant(tenant_id: str) -> dict[str, Any]:
    """Confirm tenant provisioning completion in the Go-Live pipeline.

    Calls: POST /api/v1/onboarding/internal/confirm-provisioned

    This activity is called after the tenant row and workspace have already
    been created (which happens synchronously during signup). It marks the
    "tenant_provisioned" and "workspace_created" steps complete in the
    Go-Live pipeline and triggers the post-provisioning audit event.

    The Core Service tenant_provisioner module enforces:
      - Case must have reached baa_msa_complete
      - Tenant ID must already exist in core_tenants
      - Idempotency guard prevents double-provisioning
    """
    activity.heartbeat("confirming_tenant_provisioned")
    logger.info("onboarding_activity.provision_tenant tenant_id=%s", tenant_id)

    async with httpx.AsyncClient(timeout=ACTIVITY_HTTP_TIMEOUT_S) as client:
        try:
            resp = await client.post(
                _api_url("/api/v1/onboarding/internal/confirm-provisioned"),
                json={"tenant_id": tenant_id},
                headers=_auth_header(),
            )
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            logger.error(
                "onboarding_activity.provision_tenant tenant_id=%s status=%s",
                tenant_id,
                exc.response.status_code,
            )
            _raise_for_non_retryable(exc)

    return resp.json()


@activity.defn
async def run_go_live_readiness_check(tenant_id: str) -> dict[str, Any]:
    """Run the Go-Live readiness scoring engine for the tenant.

    Calls: GET /api/v1/workspace/go-live-readiness

    Returns the readiness snapshot including:
      - overall_score (0-100)
      - go_live_ready (bool, True iff score >= 80)
      - sub_scores breakdown
      - blocking_items list

    Used by AgencyOnboardingWorkflow to determine whether the tenant is
    ready for workspace activation.
    """
    activity.heartbeat("running_go_live_check")
    logger.info(
        "onboarding_activity.go_live_readiness_check tenant_id=%s", tenant_id
    )

    async with httpx.AsyncClient(timeout=ACTIVITY_HTTP_TIMEOUT_S) as client:
        try:
            resp = await client.get(
                _api_url("/api/v1/workspace/go-live-readiness"),
                headers={
                    **_auth_header(),
                    "X-Tenant-Id": tenant_id,
                },
            )
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            logger.error(
                "onboarding_activity.go_live_readiness_check tenant_id=%s status=%s",
                tenant_id,
                exc.response.status_code,
            )
            _raise_for_non_retryable(exc)

    result = resp.json()
    logger.info(
        "onboarding_activity.go_live_readiness_check tenant_id=%s "
        "score=%s go_live_ready=%s",
        tenant_id,
        result.get("overall_score"),
        result.get("go_live_ready"),
    )
    return result


@activity.defn
async def unlock_workspace(tenant_id: str) -> dict[str, Any]:
    """Unlock the tenant workspace to complete the activation sequence.

    Calls: POST /api/v1/core/internal/workspace/unlock

    This is the internal server-to-server endpoint protected by
    CORE_PROVISIONING_TOKEN. The Temporal service token must map to a
    system identity that satisfies the Core provisioning token check.

    Sets workspace_states.is_locked = False, sends the "Welcome to
    AdaptixCore" email, and fires the Cortex onboarding walkthrough trigger.

    Returns the workspace state record after unlock.
    """
    activity.heartbeat("unlocking_workspace")
    logger.info("onboarding_activity.unlock_workspace tenant_id=%s", tenant_id)

    async with httpx.AsyncClient(timeout=ACTIVITY_HTTP_TIMEOUT_S) as client:
        try:
            resp = await client.post(
                _api_url("/api/v1/core/internal/workspace/unlock"),
                json={
                    "tenant_id": tenant_id,
                    "unlock_authority": "onboarding_completion",
                },
                headers=_auth_header(),
            )
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            logger.error(
                "onboarding_activity.unlock_workspace tenant_id=%s status=%s",
                tenant_id,
                exc.response.status_code,
            )
            _raise_for_non_retryable(exc)

    result = resp.json()
    logger.info(
        "onboarding_activity.unlock_workspace tenant_id=%s is_locked=%s",
        tenant_id,
        result.get("is_locked"),
    )
    return result


@activity.defn
async def configure_billing_provider_identity(tenant_id: str) -> dict[str, Any]:
    """Configure the billing provider identity for a newly onboarded tenant.

    Calls: POST /api/v1/onboarding/tasks/configure_billing_provider_identity/complete

    This step marks the billing provider identity task complete in the
    onboarding state machine. The task itself requires that the agency admin
    has already filled in billing provider details in the billing settings UI.

    This activity should only be called after confirming that the billing
    profile, NPI, and clearinghouse credentials are present via the readiness
    check.
    """
    activity.heartbeat("configuring_billing_provider_identity")
    logger.info(
        "onboarding_activity.configure_billing_provider_identity tenant_id=%s",
        tenant_id,
    )

    async with httpx.AsyncClient(timeout=ACTIVITY_HTTP_TIMEOUT_S) as client:
        try:
            resp = await client.post(
                _api_url(
                    "/api/v1/onboarding/tasks/configure_billing_provider_identity/complete"
                ),
                json={"status": "complete"},
                headers={
                    **_auth_header(),
                    "X-Tenant-Id": tenant_id,
                },
            )
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            logger.error(
                "onboarding_activity.configure_billing_provider_identity "
                "tenant_id=%s status=%s",
                tenant_id,
                exc.response.status_code,
            )
            _raise_for_non_retryable(exc)

    return resp.json()


@activity.defn
async def send_go_live_notification(tenant_id: str, admin_email: str) -> dict[str, Any]:
    """Send the agency go-live notification email to the agency admin.

    Calls: POST /api/v1/notifications/email/send

    Sends the "Your agency is now live on AdaptixCore" email via SES.
    This is distinct from the welcome email sent by the workspace unlock —
    the go-live notification confirms operational readiness and includes
    next-steps guidance.

    PHI-safe: admin_email is not logged.
    """
    activity.heartbeat("sending_go_live_notification")
    logger.info(
        "onboarding_activity.send_go_live_notification tenant_id=%s",
        tenant_id,
    )

    async with httpx.AsyncClient(timeout=ACTIVITY_HTTP_TIMEOUT_S) as client:
        try:
            resp = await client.post(
                _api_url("/api/v1/notifications/email/send"),
                json={
                    "to": admin_email,
                    "subject": "Your agency is now live on AdaptixCore",
                    "template": "agency_go_live",
                    "context": {"tenant_id": tenant_id},
                },
                headers=_auth_header(),
            )
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            logger.error(
                "onboarding_activity.send_go_live_notification tenant_id=%s status=%s",
                tenant_id,
                exc.response.status_code,
            )
            _raise_for_non_retryable(exc)

    return resp.json()
