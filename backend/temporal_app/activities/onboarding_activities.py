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

import html
import logging
from typing import Any

import httpx
from temporalio import activity

from temporal_app.config import (
    ADAPTIX_API_BASE,
    ACTIVITY_HTTP_TIMEOUT_S,
)
from temporal_app.system_token_client import get_system_token_client

logger = logging.getLogger(__name__)

# Onboarding drives cross-tenant go-live operations (provision is founder-gated;
# complete/readiness accept founder OR the owning agency driver). Core's minter
# maps the logical "onboarding" scope to is_founder=True + ["founder"]
# (see core_app.auth.system_identity._SCOPE_ROLE_MAP).
_ONBOARDING_SCOPE: list[str] = ["onboarding"]


async def _auth_header() -> dict[str, str]:
    """Return the Authorization header carrying a minted founder system JWT.

    Mints (or reuses) a short-lived RS256 system JWT scoped to ``onboarding``
    (founder) for calls to Core go-live routes and Communications through the
    gateway (``ADAPTIX_API_BASE``). The worker never holds the RS256 private
    key. Raises ``SystemTokenError`` (non-retryable) on a misconfigured
    provisioning token or mint route.
    """
    return await get_system_token_client().auth_header(scope=_ONBOARDING_SCOPE)


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
                headers=await _auth_header(),
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
async def complete_onboarding_step(
    case_id: str,
    step_key: str,
    notes: str | None = None,
) -> dict[str, Any]:
    """Mark a Go-Live pipeline step complete.

    Calls: POST /api/v1/go-live/cases/{case_id}/steps/{step_key}/complete
    Body:  StepCompleteRequest{notes: str | None}.

    Used by the AgencyOnboardingWorkflow to drive the pipeline through
    programmatic completion as each activity confirms the underlying operation
    succeeded. The Core Service enforces predecessor validation, idempotency,
    and audit logging. Auth: founder OR the owning agency driver — the minted
    ``onboarding`` (founder) token satisfies this.
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
                _api_url(f"/api/v1/go-live/cases/{case_id}/steps/{step_key}/complete"),
                json={"notes": notes},
                headers=await _auth_header(),
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
async def provision_case(case_id: str) -> dict[str, Any]:
    """Provision the tenant + workspace for a Go-Live case.

    Calls: POST /api/v1/go-live/cases/{case_id}/provision (founder-gated).
    Body:  empty (the provisioner derives all data from the case).

    The Core provisioner creates / confirms the tenant row and workspace and
    advances the pipeline. The minted ``onboarding`` token carries founder
    authority, satisfying the route's ``require_founder`` gate.
    """
    activity.heartbeat("provisioning_case")
    logger.info("onboarding_activity.provision_case case_id=%s", case_id)

    async with httpx.AsyncClient(timeout=ACTIVITY_HTTP_TIMEOUT_S) as client:
        try:
            resp = await client.post(
                _api_url(f"/api/v1/go-live/cases/{case_id}/provision"),
                json={},
                headers=await _auth_header(),
            )
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            logger.error(
                "onboarding_activity.provision_case case_id=%s status=%s",
                case_id,
                exc.response.status_code,
            )
            _raise_for_non_retryable(exc)

    return resp.json()


@activity.defn
async def run_go_live_readiness_check(case_id: str) -> dict[str, Any]:
    """Score Go-Live readiness for a case.

    Calls: GET /api/v1/go-live/cases/{case_id}/readiness (case-scoped).

    Returns the readiness snapshot (overall_score, go_live_ready, sub-scores,
    blocking items). Auth: founder OR the owning agency driver — satisfied by
    the minted ``onboarding`` token.
    """
    activity.heartbeat("running_go_live_check")
    logger.info("onboarding_activity.go_live_readiness_check case_id=%s", case_id)

    async with httpx.AsyncClient(timeout=ACTIVITY_HTTP_TIMEOUT_S) as client:
        try:
            resp = await client.get(
                _api_url(f"/api/v1/go-live/cases/{case_id}/readiness"),
                headers=await _auth_header(),
            )
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            logger.error(
                "onboarding_activity.go_live_readiness_check case_id=%s status=%s",
                case_id,
                exc.response.status_code,
            )
            _raise_for_non_retryable(exc)

    result = resp.json()
    logger.info(
        "onboarding_activity.go_live_readiness_check case_id=%s "
        "score=%s go_live_ready=%s",
        case_id,
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
                headers=await _auth_header(),
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
                    **(await _auth_header()),
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

    Calls: POST /api/v1/communications/email/send (through the gateway).
    Body:  SendEmailRequest{to: [admin_email], subject, body_html, body_text}.

    Sends the "Your agency is now live on AdaptixCore" email. Distinct from the
    welcome email sent by the workspace unlock — this confirms operational
    readiness. The body is a fixed go-live confirmation (no template catalog).

    PHI-safe: admin_email is not logged.
    """
    activity.heartbeat("sending_go_live_notification")
    logger.info(
        "onboarding_activity.send_go_live_notification tenant_id=%s",
        tenant_id,
    )

    body_text = (
        "Your agency is now live on AdaptixCore. "
        "Sign in to your workspace to begin operations."
    )
    async with httpx.AsyncClient(timeout=ACTIVITY_HTTP_TIMEOUT_S) as client:
        try:
            resp = await client.post(
                _api_url("/api/v1/communications/email/send"),
                json={
                    "to": [admin_email],
                    "subject": "Your agency is now live on AdaptixCore",
                    "body_html": f"<p>{html.escape(body_text)}</p>",
                    "body_text": body_text,
                },
                headers=await _auth_header(),
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
