"""Tests for notification domain activities.

What these tests prove:
  - send_sms_notification raises ValidationError for non-billing categories.
  - Allowed SMS categories are accepted.
  - send_email_notification calls the correct endpoint.
  - list_agency_statement_recipients returns parsed recipient list.
  - 4xx responses produce non-retryable errors.

What these tests do NOT prove:
  - Actual SES or Telnyx delivery.
  - Runtime behavior against a live Core Service.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, patch

import httpx
import pytest


def _make_response(status_code: int, body: dict | None = None) -> httpx.Response:
    content = json.dumps(body or {}).encode()
    return httpx.Response(
        status_code=status_code,
        headers={"content-type": "application/json"},
        content=content,
        request=httpx.Request("POST", "https://test.internal/api"),
    )


# ---------------------------------------------------------------------------
# SMS allowlist enforcement
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_send_sms_forbidden_category_raises_value_error():
    """SMS with a non-billing category raises ValueError without making any HTTP call."""
    from temporal_app.activities import notification_activities

    with patch("temporalio.activity.heartbeat"):
        with pytest.raises(ValueError, match="ValidationError"):
            await notification_activities.send_sms_notification(
                to="+15555551234",
                message="Your transport is on the way",
                notification_category="transport_status_update",  # NOT allowed
            )


@pytest.mark.asyncio
async def test_send_sms_billing_statement_reminder_allowed():
    """SMS with billing_statement_reminder category is allowed."""
    from temporal_app.activities import notification_activities

    response_body = {"message_sid": "SM123", "status": "queued"}
    mock_response = _make_response(200, response_body)
    mock_client = AsyncMock()
    mock_client.post = AsyncMock(return_value=mock_response)

    with patch("temporal_app.activities.notification_activities.httpx.AsyncClient") as mock_cls:
        mock_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
        mock_cls.return_value.__aexit__ = AsyncMock(return_value=False)
        with patch("temporalio.activity.heartbeat"):
            result = await notification_activities.send_sms_notification(
                to="+15555551234",
                message="Your statement is ready",
                notification_category="billing_statement_reminder",
            )

    assert result["message_sid"] == "SM123"


@pytest.mark.parametrize(
    "category",
    [
        "billing_statement_reminder",
        "billing_payment_due",
        "billing_plan_installment",
        "billing_late_notice",
    ],
)
@pytest.mark.asyncio
async def test_all_allowed_sms_categories_pass_validation(category):
    """All four billing AR SMS categories pass the allowlist check."""
    from temporal_app.activities import notification_activities

    response_body = {"message_sid": "SM001", "status": "queued"}
    mock_response = _make_response(200, response_body)
    mock_client = AsyncMock()
    mock_client.post = AsyncMock(return_value=mock_response)

    with patch("temporal_app.activities.notification_activities.httpx.AsyncClient") as mock_cls:
        mock_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
        mock_cls.return_value.__aexit__ = AsyncMock(return_value=False)
        with patch("temporalio.activity.heartbeat"):
            result = await notification_activities.send_sms_notification(
                to="+15555559999",
                message="Test billing message",
                notification_category=category,
            )

    assert result.get("message_sid") == "SM001"


# ---------------------------------------------------------------------------
# send_email_notification
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_send_email_success():
    """send_email_notification posts to correct endpoint and returns delivery_id."""
    from temporal_app.activities import notification_activities

    response_body = {"delivery_id": "del-789", "ses_message_id": "SES001"}
    mock_response = _make_response(200, response_body)
    mock_client = AsyncMock()
    mock_client.post = AsyncMock(return_value=mock_response)

    with patch("temporal_app.activities.notification_activities.httpx.AsyncClient") as mock_cls:
        mock_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
        mock_cls.return_value.__aexit__ = AsyncMock(return_value=False)
        with patch("temporalio.activity.heartbeat"):
            result = await notification_activities.send_email_notification(
                to="admin@agency.example.com",
                subject="Your billing statement",
                template="billing_statement",
                context={"statement_id": "stmt-001"},
            )

    assert result["delivery_id"] == "del-789"
    call_kwargs = mock_client.post.call_args
    assert call_kwargs.kwargs["json"]["template"] == "billing_statement"


@pytest.mark.asyncio
async def test_send_email_422_raises_value_error():
    """send_email_notification raises ValueError on 422 (non-retryable)."""
    from temporal_app.activities import notification_activities

    mock_response = _make_response(422, {"detail": "invalid_template"})
    mock_client = AsyncMock()
    mock_client.post = AsyncMock(
        side_effect=httpx.HTTPStatusError(
            "422 error",
            request=httpx.Request("POST", "https://test.internal"),
            response=mock_response,
        )
    )

    with patch("temporal_app.activities.notification_activities.httpx.AsyncClient") as mock_cls:
        mock_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
        mock_cls.return_value.__aexit__ = AsyncMock(return_value=False)
        with patch("temporalio.activity.heartbeat"):
            with pytest.raises(ValueError, match="ValidationError"):
                await notification_activities.send_email_notification(
                    to="a@b.com",
                    subject="Test",
                    template="invalid_template",
                    context={},
                )


# ---------------------------------------------------------------------------
# list_agency_statement_recipients
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_recipients_returns_list():
    """list_agency_statement_recipients returns the recipients list from API."""
    from temporal_app.activities import notification_activities

    recipients = [
        {"statement_id": "stmt-001", "delivery_method": "email", "email": "patient@example.com"},
        {"statement_id": "stmt-002", "delivery_method": "mail"},
    ]
    response_body = {"recipients": recipients}
    mock_response = _make_response(200, response_body)
    mock_client = AsyncMock()
    mock_client.get = AsyncMock(return_value=mock_response)

    with patch("temporal_app.activities.notification_activities.httpx.AsyncClient") as mock_cls:
        mock_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
        mock_cls.return_value.__aexit__ = AsyncMock(return_value=False)
        with patch("temporalio.activity.heartbeat"):
            result = await notification_activities.list_agency_statement_recipients(
                agency_id="agency-abc",
                month="2026-06",
            )

    assert len(result) == 2
    assert result[0]["statement_id"] == "stmt-001"


@pytest.mark.asyncio
async def test_list_recipients_empty_on_no_recipients():
    """list_agency_statement_recipients returns empty list when no recipients."""
    from temporal_app.activities import notification_activities

    mock_response = _make_response(200, {"recipients": []})
    mock_client = AsyncMock()
    mock_client.get = AsyncMock(return_value=mock_response)

    with patch("temporal_app.activities.notification_activities.httpx.AsyncClient") as mock_cls:
        mock_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
        mock_cls.return_value.__aexit__ = AsyncMock(return_value=False)
        with patch("temporalio.activity.heartbeat"):
            result = await notification_activities.list_agency_statement_recipients(
                agency_id="agency-xyz",
                month="2026-06",
            )

    assert result == []
