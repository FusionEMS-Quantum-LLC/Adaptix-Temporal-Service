"""Notification domain activities for Temporal workflows.

Activities in this module send email and SMS via the Adaptix Communications
Service (``/api/v1/communications/email/send`` and ``/sms/send``) and drive
batch statement delivery via the Billing Service — all through the gateway
(``ADAPTIX_API_BASE``), authenticated with a minted ``notifications``-scoped
system JWT (no static service token).

Platform policy enforced here:
  SMS (SendSMSActivity) is ONLY permitted for billing AR notifications
  (statement reminders, payment due, plan installments, late notices). Any
  caller passing a non-billing purpose will receive a ValidationError and
  the workflow will not retry. This mirrors the Telnyx allowlist enforcement
  in Adaptix-Communications-Service and the SMS-only-for-billing policy
  documented in platform memory.

PHI-safe logging: recipient addresses are never logged. Only tenant_id,
notification type, and result status are emitted.
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

# Role scope the Communications routes require. Core's minter maps the logical
# "notifications" scope to the role(s) the Communications email/sms send routes
# accept (see core_app.auth.system_identity._SCOPE_ROLE_MAP).
_NOTIFICATIONS_SCOPE: list[str] = ["notifications"]

# Allowed SMS notification categories per platform policy.
# Any value not in this set raises a ValidationError (non-retryable).
_ALLOWED_SMS_CATEGORIES: frozenset[str] = frozenset(
    {
        "billing_statement_reminder",
        "billing_payment_due",
        "billing_plan_installment",
        "billing_late_notice",
    }
)


async def _auth_header() -> dict[str, str]:
    """Return the Authorization header carrying a minted system JWT.

    Mints (or reuses a cached) short-lived RS256 system JWT scoped to
    ``notifications`` and returns it as a Bearer header for calls to the
    Communications Service through ``ADAPTIX_API_BASE`` (the gateway). The
    worker never holds the RS256 private key; it exchanges its
    ``CORE_PROVISIONING_TOKEN`` for this short-lived token at the Core mint
    route. Raises ``SystemTokenError`` (non-retryable) on a misconfigured
    provisioning token or mint route.
    """
    return await get_system_token_client().auth_header(scope=_NOTIFICATIONS_SCOPE)


def _context_body(context: dict[str, Any]) -> tuple[str, str]:
    """Derive (body_html, body_text) from an activity-provided context.

    The Communications ``/email/send`` route accepts pre-rendered
    ``body_html`` / ``body_text`` rather than a template name. The worker does
    NOT own a template catalog, so the body is taken from the context the
    caller supplied:

    * ``body_html`` / ``body_text`` keys are used directly when present.
    * otherwise a single ``body`` / ``message`` value is used as plain text and
      HTML-escaped into a minimal HTML body.

    No content is fabricated — an empty context yields an empty body, which the
    Communications route validates.
    """
    body_html = str(context.get("body_html") or "").strip()
    body_text = str(
        context.get("body_text") or context.get("body") or context.get("message") or ""
    ).strip()
    if not body_html and body_text:
        body_html = f"<p>{html.escape(body_text)}</p>"
    return body_html, body_text


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
            f"ValidationError: Notification API rejected the request with {status}. "
            f"Response: {exc.response.text[:500]}"
        ) from exc
    if status in (401, 403):
        raise PermissionError(
            f"AuthorizationError: Notification API returned {status}. "
            "The minted system JWT was rejected — check that the system "
            "principal seed row is ACTIVE and that the Core minter maps the "
            "'notifications' scope to a role the Communications routes accept."
        ) from exc
    raise exc


# ---------------------------------------------------------------------------
# Email activity
# ---------------------------------------------------------------------------


@activity.defn
async def send_email_notification(
    to: str,
    subject: str,
    template: str,
    context: dict[str, Any],
) -> dict[str, Any]:
    """Send a transactional email through the Communications Service.

    Calls: POST /api/v1/communications/email/send (through the gateway).
    Body: SendEmailRequest{to: [<addr>], subject, body_html, body_text}.

    to:       Recipient email address. Not logged (PHI-safe). Wrapped into the
              Communications route's ``to`` list.
    subject:  Email subject line.
    template: Retained for backward compatibility with existing workflow
              callers. The Communications route renders no templates; the body
              is taken from ``context`` (body_html / body_text / body / message).
              The worker does not own a template catalog.
    context:  Rendering context. ``body_html`` / ``body_text`` / ``body`` /
              ``message`` supply the email body. Must not include raw PHI.

    Returns the Communications delivery record on success.
    """
    activity.heartbeat("sending_email")
    logger.info(
        "notification_activity.send_email template=%s",
        template,
    )

    body_html, body_text = _context_body(context)
    async with httpx.AsyncClient(timeout=ACTIVITY_HTTP_TIMEOUT_S) as client:
        try:
            resp = await client.post(
                _api_url("/api/v1/communications/email/send"),
                json={
                    "to": [to],
                    "subject": subject,
                    "body_html": body_html,
                    "body_text": body_text,
                },
                headers=await _auth_header(),
            )
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            logger.error(
                "notification_activity.send_email template=%s status=%s",
                template,
                exc.response.status_code,
            )
            _raise_for_non_retryable(exc)

    result = resp.json()
    logger.info(
        "notification_activity.send_email template=%s delivery_id=%s",
        template,
        result.get("delivery_id") or result.get("id"),
    )
    return result


# ---------------------------------------------------------------------------
# SMS activity — billing AR only
# ---------------------------------------------------------------------------


@activity.defn
async def send_sms_notification(
    to: str,
    message: str,
    notification_category: str,
) -> dict[str, Any]:
    """Send an SMS via Telnyx through the Core Service.

    PLATFORM POLICY: SMS is ONLY for billing AR notifications.
    notification_category must be one of:
      - billing_statement_reminder
      - billing_payment_due
      - billing_plan_installment
      - billing_late_notice

    Passing any other category raises a ValidationError immediately without
    making any HTTP call. This is a non-retryable activity error.

    Calls: POST /api/v1/communications/sms/send (through the gateway).
    Body: SendRequest{to_number, body}.

    PHI-safe: recipient phone number is not logged.
    """
    # Enforce SMS allowlist before any network call.
    if notification_category not in _ALLOWED_SMS_CATEGORIES:
        raise ValueError(
            f"ValidationError: SMS category '{notification_category}' is not permitted. "
            f"SMS is reserved for billing AR only. "
            f"Allowed categories: {sorted(_ALLOWED_SMS_CATEGORIES)}"
        )

    activity.heartbeat("sending_sms")
    logger.info(
        "notification_activity.send_sms category=%s",
        notification_category,
    )

    async with httpx.AsyncClient(timeout=ACTIVITY_HTTP_TIMEOUT_S) as client:
        try:
            resp = await client.post(
                _api_url("/api/v1/communications/sms/send"),
                json={
                    "to_number": to,
                    "body": message,
                },
                headers=await _auth_header(),
            )
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            logger.error(
                "notification_activity.send_sms category=%s status=%s",
                notification_category,
                exc.response.status_code,
            )
            _raise_for_non_retryable(exc)

    result = resp.json()
    logger.info(
        "notification_activity.send_sms category=%s message_sid=%s",
        notification_category,
        result.get("message_sid") or result.get("id"),
    )
    return result


# ---------------------------------------------------------------------------
# Batch statement delivery
# ---------------------------------------------------------------------------


@activity.defn
async def list_agency_statement_recipients(
    agency_id: str,
    month: str,
) -> list[dict[str, Any]]:
    """Retrieve the list of patients who need statements for a given month.

    Calls: GET /api/v1/billing/statements/recipients?agency_id=...&month=...

    Returns a list of recipient records each containing:
      - statement_id
      - delivery_method (email | mail | both)
      - email (present when delivery_method includes email)
      - mailing_address (present when delivery_method includes mail)

    PHI-safe: patient identifiers in the response are pseudonymous
    statement_id values. The Billing Service owns the resolution to real
    patient records.
    """
    activity.heartbeat("listing_statement_recipients")
    logger.info(
        "notification_activity.list_recipients agency_id=%s month=%s",
        agency_id,
        month,
    )

    async with httpx.AsyncClient(timeout=ACTIVITY_HTTP_TIMEOUT_S) as client:
        try:
            resp = await client.get(
                _api_url("/api/v1/billing/statements/recipients"),
                params={"agency_id": agency_id, "month": month},
                headers=await _auth_header(),
            )
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            logger.error(
                "notification_activity.list_recipients agency_id=%s status=%s",
                agency_id,
                exc.response.status_code,
            )
            _raise_for_non_retryable(exc)

    result = resp.json()
    recipients: list[dict[str, Any]] = result.get("recipients", [])
    logger.info(
        "notification_activity.list_recipients agency_id=%s month=%s count=%d",
        agency_id,
        month,
        len(recipients),
    )
    return recipients


@activity.defn
async def send_statement_email(statement_id: str, to: str) -> dict[str, Any]:
    """Send a single billing statement via email.

    Calls: POST /api/v1/billing/statements/{statement_id}/send-email

    The Billing Service resolves the statement PDF, renders it, and delivers
    it via SES. Returns delivery record on success.

    PHI-safe: recipient address is not logged.
    """
    activity.heartbeat("sending_statement_email")
    logger.info(
        "notification_activity.send_statement_email statement_id=%s",
        statement_id,
    )

    async with httpx.AsyncClient(timeout=ACTIVITY_HTTP_TIMEOUT_S) as client:
        try:
            resp = await client.post(
                _api_url(f"/api/v1/billing/statements/{statement_id}/send-email"),
                json={"to": to},
                headers=await _auth_header(),
            )
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            logger.error(
                "notification_activity.send_statement_email statement_id=%s status=%s",
                statement_id,
                exc.response.status_code,
            )
            _raise_for_non_retryable(exc)

    return resp.json()


@activity.defn
async def queue_statement_for_mail(statement_id: str) -> dict[str, Any]:
    """Queue a patient statement for physical mail via PostGrid.

    Calls: POST /api/v1/billing/statements/{statement_id}/send-mail

    The Billing Service generates a PostGrid letter request, submits it,
    and persists the delivery tracking record. Returns the PostGrid letter
    ID and estimated delivery window.

    Used by SendBatchStatementsWorkflow for patients with mail-only or
    mail+email delivery preference.
    """
    activity.heartbeat("queueing_statement_for_mail")
    logger.info(
        "notification_activity.queue_statement_for_mail statement_id=%s",
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
                "notification_activity.queue_statement_for_mail statement_id=%s status=%s",
                statement_id,
                exc.response.status_code,
            )
            _raise_for_non_retryable(exc)

    result = resp.json()
    logger.info(
        "notification_activity.queue_statement_for_mail statement_id=%s "
        "postgrid_letter_id=%s",
        statement_id,
        result.get("postgrid_letter_id"),
    )
    return result
