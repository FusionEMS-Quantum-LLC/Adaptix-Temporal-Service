"""Document domain activities for Temporal workflows.

Activities call:
  - Adaptix Documents Service for PDF generation
  - Adaptix Core Service for TrustSign document lifecycle
  - Adaptix Billing Service for PostGrid mail delivery

PHI-safe: document content is never logged. Only document_id, document_type,
output_key (S3 path), and operation status are emitted.

TrustSign is Adaptix-native. No external e-signature provider is called or
referenced. Any attempt to route a document to an external provider must be
treated as a production integrity violation.
"""

from __future__ import annotations

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

# Core's minter maps the logical "documents" scope to ["agency_admin"]
# (see core_app.auth.system_identity._SCOPE_ROLE_MAP). Document/TrustSign
# routes are tenant-scoped; the minted token carries the system tenant +
# agency_admin role and authenticates through the gateway.
_DOCUMENTS_SCOPE: list[str] = ["documents"]


async def _auth_header() -> dict[str, str]:
    """Return the Authorization header carrying a minted ``documents`` JWT.

    Mints (or reuses) a short-lived RS256 system JWT scoped to ``documents``
    for calls to the Documents/Billing services through the gateway
    (``ADAPTIX_API_BASE``). The worker never holds the RS256 private key.
    Raises ``SystemTokenError`` (non-retryable) on a misconfigured provisioning
    token or mint route.
    """
    return await get_system_token_client().auth_header(scope=_DOCUMENTS_SCOPE)


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
            f"ValidationError: Documents API returned {status}. "
            f"Response: {exc.response.text[:500]}"
        ) from exc
    if status in (401, 403):
        raise PermissionError(
            f"AuthorizationError: Documents API returned {status}. "
            "Check ADAPTIX_SERVICE_TOKEN."
        ) from exc
    raise exc


# ---------------------------------------------------------------------------
# PDF generation
# ---------------------------------------------------------------------------


@activity.defn
async def generate_pdf_document(
    document_type: str,
    context: dict[str, Any],
    output_key: str,
) -> dict[str, Any]:
    """Generate a PDF document via the Documents Service.

    Calls: POST /api/v1/documents/generate

    document_type: Template key (e.g. "billing_statement", "cms1500",
                   "consent_form", "pcs_certificate").
    context:       Rendering context — must not include raw PHI text.
                   Use entity IDs; the Documents Service resolves display values.
    output_key:    S3 key where the generated PDF will be stored
                   (e.g. "documents/billing/2026/06/statement-{statement_id}.pdf").

    Returns:
      {"document_id": "...", "s3_key": "...", "size_bytes": ...}

    The Documents Service persists a document record, generates the PDF via
    ReportLab, uploads to S3, and returns the document ID and storage key.
    """
    activity.heartbeat("generating_pdf")
    logger.info(
        "document_activity.generate_pdf document_type=%s output_key=%s",
        document_type,
        output_key,
    )

    async with httpx.AsyncClient(timeout=ACTIVITY_HTTP_TIMEOUT_S) as client:
        try:
            resp = await client.post(
                _api_url("/api/v1/documents/generate"),
                json={
                    "document_type": document_type,
                    "context": context,
                    "output_key": output_key,
                },
                headers=await _auth_header(),
            )
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            logger.error(
                "document_activity.generate_pdf document_type=%s status=%s",
                document_type,
                exc.response.status_code,
            )
            _raise_for_non_retryable(exc)

    result = resp.json()
    logger.info(
        "document_activity.generate_pdf document_type=%s document_id=%s s3_key=%s",
        document_type,
        result.get("document_id"),
        result.get("s3_key"),
    )
    return result


# ---------------------------------------------------------------------------
# TrustSign activities
# ---------------------------------------------------------------------------


@activity.defn
async def initiate_trustsign_envelope(
    document_id: str,
    recipient_email: str,
) -> dict[str, Any]:
    """Create a TrustSign envelope and send the signature invitation.

    Calls: POST /api/v1/documents/trustsign/envelopes

    document_id:     The Adaptix document ID of the prepared PDF.
    recipient_email: The signer's email address. Used ONLY for the
                     SES invitation delivery; not stored in logs.

    The Core/Documents Service will:
      1. Create a TrustSign envelope record.
      2. Generate a token-gated signing URL (/transportlink/sign/{token}).
      3. Send the SES invitation email to the recipient.
      4. Persist the TrustSign audit record.

    Returns:
      {"envelope_id": "...", "signing_url": "...", "status": "pending"}
    """
    activity.heartbeat("initiating_trustsign_envelope")
    logger.info(
        "document_activity.initiate_trustsign document_id=%s",
        document_id,
    )

    async with httpx.AsyncClient(timeout=ACTIVITY_HTTP_TIMEOUT_S) as client:
        try:
            resp = await client.post(
                _api_url("/api/v1/documents/trustsign/envelopes"),
                json={
                    "document_id": document_id,
                    "signers": [{"email": recipient_email}],
                },
                headers=await _auth_header(),
            )
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            logger.error(
                "document_activity.initiate_trustsign document_id=%s status=%s",
                document_id,
                exc.response.status_code,
            )
            _raise_for_non_retryable(exc)

    result = resp.json()
    logger.info(
        "document_activity.initiate_trustsign document_id=%s envelope_id=%s",
        document_id,
        result.get("envelope_id"),
    )
    return result


@activity.defn
async def poll_trustsign_status(envelope_id: str) -> dict[str, Any]:
    """Poll TrustSign envelope signature status.

    Calls: GET /api/v1/documents/trustsign/envelopes/{envelope_id}

    Returns the current envelope status record:
      {"envelope_id": "...", "status": "pending|signed|declined|expired",
       "signed_at": "...", "signer_ip": "..."}

    Called by TrustSignWorkflow on a polling loop until status is terminal
    (signed, declined, or expired). The workflow uses a timer activity with
    WORKFLOW_EXECUTION_TIMEOUT to enforce the signing deadline.
    """
    activity.heartbeat("polling_trustsign_status")
    logger.info(
        "document_activity.poll_trustsign_status envelope_id=%s",
        envelope_id,
    )

    async with httpx.AsyncClient(timeout=ACTIVITY_HTTP_TIMEOUT_S) as client:
        try:
            resp = await client.get(
                _api_url(f"/api/v1/documents/trustsign/envelopes/{envelope_id}"),
                headers=await _auth_header(),
            )
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            logger.error(
                "document_activity.poll_trustsign_status envelope_id=%s status=%s",
                envelope_id,
                exc.response.status_code,
            )
            _raise_for_non_retryable(exc)

    return resp.json()


@activity.defn
async def finalize_trustsign_envelope(envelope_id: str) -> dict[str, Any]:
    """Finalize a completed TrustSign envelope.

    Calls: POST /api/v1/documents/trustsign/envelopes/{envelope_id}/finalize

    Triggers:
      1. Download and store the signed PDF to S3.
      2. Advance the document record to "signed" status.
      3. Write the TrustSign completion audit event.
      4. Trigger downstream notifications (patient portal + billing).

    Called only after poll_trustsign_status returns status="signed".
    """
    activity.heartbeat("finalizing_trustsign_envelope")
    logger.info(
        "document_activity.finalize_trustsign envelope_id=%s",
        envelope_id,
    )

    async with httpx.AsyncClient(timeout=ACTIVITY_HTTP_TIMEOUT_S) as client:
        try:
            resp = await client.post(
                _api_url(
                    f"/api/v1/documents/trustsign/envelopes/{envelope_id}/finalize"
                ),
                headers=await _auth_header(),
            )
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            logger.error(
                "document_activity.finalize_trustsign envelope_id=%s status=%s",
                envelope_id,
                exc.response.status_code,
            )
            _raise_for_non_retryable(exc)

    result = resp.json()
    logger.info(
        "document_activity.finalize_trustsign envelope_id=%s signed_doc_key=%s",
        envelope_id,
        result.get("signed_doc_key"),
    )
    return result


# ---------------------------------------------------------------------------
# PostGrid mail activity
# ---------------------------------------------------------------------------


@activity.defn
async def send_statement_via_postgrid(statement_id: str) -> dict[str, Any]:
    """Submit a patient statement to PostGrid for physical mail delivery.

    Calls: POST /api/v1/billing/statements/{statement_id}/send-mail

    The Billing Service:
      1. Resolves the statement PDF from S3.
      2. Submits a letter job to the PostGrid API.
      3. Persists the PostGrid letter ID and estimated delivery date.
      4. Writes a billing audit event.

    Returns:
      {"postgrid_letter_id": "...", "estimated_delivery": "...",
       "status": "queued"}
    """
    activity.heartbeat("sending_via_postgrid")
    logger.info(
        "document_activity.send_via_postgrid statement_id=%s",
        statement_id,
    )

    async with httpx.AsyncClient(timeout=ACTIVITY_HTTP_TIMEOUT_S) as client:
        try:
            resp = await client.post(
                _api_url(f"/api/v1/billing/statements/{statement_id}/send-mail"),
                headers=await _auth_header(),
            )
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            logger.error(
                "document_activity.send_via_postgrid statement_id=%s status=%s",
                statement_id,
                exc.response.status_code,
            )
            _raise_for_non_retryable(exc)

    result = resp.json()
    logger.info(
        "document_activity.send_via_postgrid statement_id=%s postgrid_letter_id=%s",
        statement_id,
        result.get("postgrid_letter_id"),
    )
    return result
