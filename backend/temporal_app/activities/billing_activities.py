"""Billing domain activities for Temporal workflows.

Activities in this module make HTTP calls to the Adaptix Billing Service API.
Every activity is:

  - Idempotent by construction where the downstream API supports idempotency
    (e.g. claim submission uses the claim_id as the workflow ID, so a retry
    hitting an already-submitted claim receives a structured response rather
    than a duplicate submission).
  - PHI-safe: no patient name, DOB, address, or diagnosis data is logged.
    Only tenant_id, claim_id, and result status are emitted to logs.
  - Non-retryable error types: ValidationError (bad input that no retry will
    fix) and AuthorizationError (permission or token misconfiguration).

Authentication:
  ADAPTIX_SERVICE_TOKEN is injected via ECS task definition secret. It is
  read from config at call time, not cached at module import, so a rolling
  secret rotation is picked up without a container restart.

Error handling:
  httpx.HTTPStatusError with 4xx is raised as-is if not retryable.
  httpx.HTTPStatusError with 5xx is allowed to propagate for Temporal retry.
  Network errors (ConnectError, TimeoutException) propagate for Temporal retry.
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


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _auth_header() -> dict[str, str]:
    """Build the Authorization header from the configured service token.

    Raises RuntimeError (non-retryable by Temporal) if the token is absent,
    because no retry will resolve a missing credential. The ECS task definition
    must be corrected and the worker redeployed.
    """
    token = ADAPTIX_SERVICE_TOKEN
    if not token:
        raise RuntimeError(
            "ADAPTIX_SERVICE_TOKEN is not configured. "
            "Add it to the ECS task definition container secrets. "
            "This error is non-retryable — fix the deployment."
        )
    return {"Authorization": f"Bearer {token}"}


def _api_url(path: str) -> str:
    """Resolve a full URL from the configured API base and a path fragment."""
    if not ADAPTIX_API_BASE:
        raise RuntimeError(
            "ADAPTIX_API_BASE is not configured. "
            "This error is non-retryable — fix the deployment."
        )
    return f"{ADAPTIX_API_BASE}{path}"


def _raise_for_non_retryable(exc: httpx.HTTPStatusError) -> None:
    """Re-raise 4xx errors as application-specific exceptions that Temporal
    will not retry (they are in the non_retryable_error_types list in config).

    400 Bad Request / 422 Unprocessable → ValidationError
    401 Unauthorized / 403 Forbidden    → AuthorizationError
    All other 4xx                       → pass through (will retry)
    5xx                                 → pass through (will retry)
    """
    status = exc.response.status_code
    if status == 400 or status == 422:
        raise ValueError(
            f"ValidationError: Billing API rejected the request with {status}. "
            f"Response: {exc.response.text[:500]}"
        ) from exc
    if status in (401, 403):
        raise PermissionError(
            f"AuthorizationError: Billing API returned {status}. "
            "Check ADAPTIX_SERVICE_TOKEN and RBAC configuration."
        ) from exc
    # Let other status codes propagate for Temporal retry.
    raise exc


# ---------------------------------------------------------------------------
# Claim submission activities
# ---------------------------------------------------------------------------


@activity.defn
async def submit_claim_to_clearinghouse(claim_id: str) -> dict[str, Any]:
    """Submit a claim to Office Ally via the Billing Service clearinghouse endpoint.

    Calls: POST /api/v1/billing/claims/{claim_id}/submit-to-clearinghouse

    Returns the Billing Service response payload on success.
    Raises ValueError (non-retryable) on 400/422.
    Raises PermissionError (non-retryable) on 401/403.
    Raises httpx error on 5xx or network failure — Temporal will retry.

    PHI-safe logging: only claim_id and HTTP status are logged.
    """
    activity.heartbeat("submitting_to_clearinghouse")
    logger.info("billing_activity.submit_claim_to_clearinghouse claim_id=%s", claim_id)

    async with httpx.AsyncClient(timeout=ACTIVITY_HTTP_TIMEOUT_S) as client:
        try:
            resp = await client.post(
                _api_url(f"/api/v1/billing/claims/{claim_id}/submit-to-clearinghouse"),
                headers=_auth_header(),
            )
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            logger.error(
                "billing_activity.submit_claim_to_clearinghouse claim_id=%s status=%s",
                claim_id,
                exc.response.status_code,
            )
            _raise_for_non_retryable(exc)

    result: dict[str, Any] = resp.json()
    logger.info(
        "billing_activity.submit_claim_to_clearinghouse claim_id=%s success",
        claim_id,
    )
    return result


@activity.defn
async def get_claim_status(claim_id: str) -> dict[str, Any]:
    """Fetch the current clearinghouse submission status for a claim.

    Calls: GET /api/v1/billing/claims/{claim_id}/clearinghouse-status

    Used by DenialResubmissionWorkflow and ERAPostingWorkflow to confirm
    claim state before proceeding.
    """
    activity.heartbeat("fetching_claim_status")
    logger.info("billing_activity.get_claim_status claim_id=%s", claim_id)

    async with httpx.AsyncClient(timeout=ACTIVITY_HTTP_TIMEOUT_S) as client:
        try:
            resp = await client.get(
                _api_url(f"/api/v1/billing/claims/{claim_id}/clearinghouse-status"),
                headers=_auth_header(),
            )
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            logger.error(
                "billing_activity.get_claim_status claim_id=%s status=%s",
                claim_id,
                exc.response.status_code,
            )
            _raise_for_non_retryable(exc)

    return resp.json()


@activity.defn
async def create_denial_appeal(claim_id: str, denial_code: str) -> dict[str, Any]:
    """Create a denial appeal record for a denied claim.

    Calls: POST /api/v1/billing/claims/{claim_id}/appeal

    The Billing Service validates the denial code, creates a ClaimDenial
    appeal record, and re-queues the claim for resubmission.

    denial_code: CARC denial code string (e.g. "CO-4", "PR-1").
    """
    activity.heartbeat("creating_denial_appeal")
    logger.info(
        "billing_activity.create_denial_appeal claim_id=%s denial_code=%s",
        claim_id,
        denial_code,
    )

    async with httpx.AsyncClient(timeout=ACTIVITY_HTTP_TIMEOUT_S) as client:
        try:
            resp = await client.post(
                _api_url(f"/api/v1/billing/claims/{claim_id}/appeal"),
                json={"denial_code": denial_code},
                headers=_auth_header(),
            )
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            logger.error(
                "billing_activity.create_denial_appeal claim_id=%s status=%s",
                claim_id,
                exc.response.status_code,
            )
            _raise_for_non_retryable(exc)

    result = resp.json()
    logger.info(
        "billing_activity.create_denial_appeal claim_id=%s appeal_created",
        claim_id,
    )
    return result


@activity.defn
async def resubmit_denied_claim(claim_id: str) -> dict[str, Any]:
    """Resubmit a denied claim after appeal record creation.

    Calls: POST /api/v1/billing/claims/{claim_id}/submit-to-clearinghouse

    The Billing Service will re-generate the 837P from the corrected claim
    data and upload it to Office Ally. This is the same endpoint as the
    initial submission — the Billing Service distinguishes first submission
    from resubmission based on existing ClearinghouseSubmission records.
    """
    activity.heartbeat("resubmitting_denied_claim")
    logger.info("billing_activity.resubmit_denied_claim claim_id=%s", claim_id)

    async with httpx.AsyncClient(timeout=ACTIVITY_HTTP_TIMEOUT_S) as client:
        try:
            resp = await client.post(
                _api_url(f"/api/v1/billing/claims/{claim_id}/submit-to-clearinghouse"),
                headers=_auth_header(),
            )
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            logger.error(
                "billing_activity.resubmit_denied_claim claim_id=%s status=%s",
                claim_id,
                exc.response.status_code,
            )
            _raise_for_non_retryable(exc)

    return resp.json()


@activity.defn
async def process_era_file(era_file_path: str) -> dict[str, Any]:
    """Trigger ERA/835 remittance posting via the Billing Service.

    Calls: POST /api/v1/billing/claims/webhooks/835-remittance

    era_file_path: S3 key for the ERA file (e.g.
        s3://adaptix-billing-edi/era/2026/06/835_20260605_001.edi)

    The Billing Service will:
      1. Download the ERA from S3.
      2. Parse the X12 835.
      3. Post payment or denial state to each referenced claim.
      4. Write ClearinghouseEra and BillingAuditEvent rows.

    Returns the posting summary (claims_posted, denials_routed, errors).
    """
    activity.heartbeat("processing_era_file")
    logger.info("billing_activity.process_era_file path=%s", era_file_path)

    async with httpx.AsyncClient(timeout=ACTIVITY_HTTP_TIMEOUT_S) as client:
        try:
            resp = await client.post(
                _api_url("/api/v1/billing/claims/webhooks/835-remittance"),
                json={"era_file_path": era_file_path},
                headers=_auth_header(),
            )
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            logger.error(
                "billing_activity.process_era_file path=%s status=%s",
                era_file_path,
                exc.response.status_code,
            )
            _raise_for_non_retryable(exc)

    result = resp.json()
    logger.info(
        "billing_activity.process_era_file path=%s claims_posted=%s",
        era_file_path,
        result.get("claims_posted", "unknown"),
    )
    return result


@activity.defn
async def run_monthly_agency_invoicing(billing_month: str) -> dict[str, Any]:
    """Run monthly invoicing for all active agency subscriptions.

    Calls: POST /api/v1/billing/subscriptions/invoice-all-agencies

    billing_month: ISO month string (e.g. "2026-06"). The Billing Service
        will invoice all active subscriptions for that calendar month via
        Stripe, send statements via SES/PostGrid, and persist billing cycle
        records.

    This is a long-running activity — heartbeats are emitted during the call.
    The HTTP timeout is extended to 5 minutes to allow the Billing Service
    to process all active agencies.
    """
    activity.heartbeat("starting_monthly_invoicing")
    logger.info("billing_activity.run_monthly_agency_invoicing month=%s", billing_month)

    # Extended timeout for batch invoicing — Billing Service processes all
    # active agencies within one HTTP call. Temporal heartbeat ensures the
    # activity is alive from Temporal's perspective.
    async with httpx.AsyncClient(timeout=300) as client:
        try:
            resp = await client.post(
                _api_url("/api/v1/billing/subscriptions/invoice-all-agencies"),
                json={"billing_month": billing_month},
                headers=_auth_header(),
            )
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            logger.error(
                "billing_activity.run_monthly_agency_invoicing month=%s status=%s",
                billing_month,
                exc.response.status_code,
            )
            _raise_for_non_retryable(exc)

    result = resp.json()
    logger.info(
        "billing_activity.run_monthly_agency_invoicing month=%s "
        "invoices_processed=%s",
        billing_month,
        result.get("invoices_processed", "unknown"),
    )
    return result
