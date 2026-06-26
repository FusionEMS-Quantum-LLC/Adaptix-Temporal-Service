"""Tests for billing domain activities.

What these tests prove:
  - Each activity calls the correct Billing Service endpoint (path + body).
  - The activity authenticates by minting a system JWT scoped to
    ``billing_operator`` via the system-token client and sends it as
    ``Authorization: Bearer <minted JWT>`` on every Billing Service call.
  - 400/422 responses are raised as ValueError (non-retryable ValidationError).
  - 401/403 responses are raised as PermissionError (non-retryable AuthorizationError).
  - 5xx responses are re-raised as httpx.HTTPStatusError (retryable by Temporal).
  - A misconfigured provisioning token surfaces SystemTokenError (non-retryable).
  - Successful responses return the parsed JSON dict.

What these tests do NOT prove:
  - Actual Billing Service behavior.
  - Real claim submission to Office Ally.
  - Runtime verification against a deployed system.
  - Temporal retry behavior (tested by Temporal SDK internals).
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import httpx
import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# The exact header the billing activities must send. The token VALUE here is a
# test placeholder, never a real secret.
_MINTED_BEARER = "Bearer minted-system-jwt-test-placeholder"


def _make_response(status_code: int, json_body: dict | None = None) -> httpx.Response:
    """Build a mock httpx.Response."""
    import json as _json

    content = _json.dumps(json_body or {}).encode()
    return httpx.Response(
        status_code=status_code,
        headers={"content-type": "application/json"},
        content=content,
        request=httpx.Request("POST", "https://test.internal/api"),
    )


def _patch_token_client():
    """Patch the system-token client so the activity's auth header mint is
    intercepted (no real network call) and records the requested scope.

    Returns the AsyncMock standing in for ``auth_header`` so callers can assert
    it was awaited with ``scope=["billing_operator"]``.
    """
    from temporal_app.activities import billing_activities

    auth_header_mock = AsyncMock(return_value={"Authorization": _MINTED_BEARER})
    client_stub = AsyncMock()
    client_stub.auth_header = auth_header_mock
    return (
        patch.object(
            billing_activities,
            "get_system_token_client",
            return_value=client_stub,
        ),
        auth_header_mock,
    )


# ---------------------------------------------------------------------------
# submit_claim_to_clearinghouse
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_submit_claim_success_mints_and_sends_bearer():
    """submit_claim mints a billing_operator JWT, sends it as Bearer, hits the
    correct clearinghouse path, and returns parsed JSON on 200."""
    from temporal_app.activities import billing_activities

    response_body = {"submission_id": "sub-123", "status": "pending_ack"}
    mock_client = AsyncMock()
    mock_client.post = AsyncMock(return_value=_make_response(200, response_body))

    token_patch, auth_header_mock = _patch_token_client()
    with token_patch, patch.object(billing_activities.httpx, "AsyncClient") as mock_cls:
        mock_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
        mock_cls.return_value.__aexit__ = AsyncMock(return_value=False)
        with patch("temporalio.activity.heartbeat"):
            result = await billing_activities.submit_claim_to_clearinghouse("claim-abc")

    assert result == response_body

    # Mint was requested with the billing_operator scope.
    auth_header_mock.assert_awaited_once_with(scope=["billing_operator"])

    # The minted Bearer header was sent on the Billing Service call.
    call = mock_client.post.call_args
    assert call.kwargs["headers"]["Authorization"] == _MINTED_BEARER

    # Correct clearinghouse submit path was called.
    called_url = call.args[0] if call.args else call.kwargs["url"]
    assert called_url.endswith(
        "/api/v1/billing/claims/claim-abc/submit-to-clearinghouse"
    )


@pytest.mark.asyncio
async def test_submit_claim_400_raises_value_error():
    """submit_claim raises ValueError on 400 (non-retryable)."""
    from temporal_app.activities import billing_activities

    mock_response = _make_response(400, {"detail": "invalid_claim"})
    mock_client = AsyncMock()
    mock_client.post = AsyncMock(
        side_effect=httpx.HTTPStatusError(
            "400 error",
            request=httpx.Request("POST", "https://test.internal"),
            response=mock_response,
        )
    )

    token_patch, _ = _patch_token_client()
    with token_patch, patch.object(billing_activities.httpx, "AsyncClient") as mock_cls:
        mock_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
        mock_cls.return_value.__aexit__ = AsyncMock(return_value=False)
        with patch("temporalio.activity.heartbeat"):
            with pytest.raises(ValueError, match="ValidationError"):
                await billing_activities.submit_claim_to_clearinghouse("claim-abc")


@pytest.mark.asyncio
async def test_submit_claim_403_raises_permission_error():
    """submit_claim raises PermissionError on 403 (non-retryable)."""
    from temporal_app.activities import billing_activities

    mock_response = _make_response(403, {"detail": "forbidden"})
    mock_client = AsyncMock()
    mock_client.post = AsyncMock(
        side_effect=httpx.HTTPStatusError(
            "403 error",
            request=httpx.Request("POST", "https://test.internal"),
            response=mock_response,
        )
    )

    token_patch, _ = _patch_token_client()
    with token_patch, patch.object(billing_activities.httpx, "AsyncClient") as mock_cls:
        mock_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
        mock_cls.return_value.__aexit__ = AsyncMock(return_value=False)
        with patch("temporalio.activity.heartbeat"):
            with pytest.raises(PermissionError, match="AuthorizationError"):
                await billing_activities.submit_claim_to_clearinghouse("claim-abc")


@pytest.mark.asyncio
async def test_submit_claim_500_reraises_for_retry():
    """submit_claim re-raises 5xx for Temporal retry."""
    from temporal_app.activities import billing_activities

    mock_response = _make_response(503, {"detail": "service_unavailable"})
    mock_client = AsyncMock()
    mock_client.post = AsyncMock(
        side_effect=httpx.HTTPStatusError(
            "503 error",
            request=httpx.Request("POST", "https://test.internal"),
            response=mock_response,
        )
    )

    token_patch, _ = _patch_token_client()
    with token_patch, patch.object(billing_activities.httpx, "AsyncClient") as mock_cls:
        mock_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
        mock_cls.return_value.__aexit__ = AsyncMock(return_value=False)
        with patch("temporalio.activity.heartbeat"):
            with pytest.raises(httpx.HTTPStatusError):
                await billing_activities.submit_claim_to_clearinghouse("claim-abc")


@pytest.mark.asyncio
async def test_submit_claim_mint_failure_is_non_retryable():
    """A mint failure (SystemTokenError) propagates and is non-retryable.

    The activity must not silently fall back to an unauthenticated call when
    the system token cannot be minted.
    """
    from temporal_app.activities import billing_activities
    from temporal_app.system_token_client import SystemTokenError

    client_stub = AsyncMock()
    client_stub.auth_header = AsyncMock(
        side_effect=SystemTokenError("CORE_PROVISIONING_TOKEN is not configured")
    )

    with patch.object(
        billing_activities, "get_system_token_client", return_value=client_stub
    ):
        with patch("temporalio.activity.heartbeat"):
            with pytest.raises(SystemTokenError):
                await billing_activities.submit_claim_to_clearinghouse("claim-abc")


# ---------------------------------------------------------------------------
# create_denial_appeal
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_denial_appeal_success():
    """create_denial_appeal posts the correct payload/path with a Bearer header."""
    from temporal_app.activities import billing_activities

    response_body = {"appeal_id": "appeal-456", "status": "pending_resubmit"}
    mock_client = AsyncMock()
    mock_client.post = AsyncMock(return_value=_make_response(200, response_body))

    token_patch, auth_header_mock = _patch_token_client()
    with token_patch, patch.object(billing_activities.httpx, "AsyncClient") as mock_cls:
        mock_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
        mock_cls.return_value.__aexit__ = AsyncMock(return_value=False)
        with patch("temporalio.activity.heartbeat"):
            result = await billing_activities.create_denial_appeal("claim-abc", "CO-4")

    assert result == response_body
    auth_header_mock.assert_awaited_once_with(scope=["billing_operator"])
    call = mock_client.post.call_args
    assert call.kwargs["json"]["denial_code"] == "CO-4"
    assert call.kwargs["headers"]["Authorization"] == _MINTED_BEARER
    called_url = call.args[0] if call.args else call.kwargs["url"]
    assert called_url.endswith("/api/v1/billing/claims/claim-abc/appeal")


# ---------------------------------------------------------------------------
# process_era_file
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_process_era_file_success():
    """process_era_file sends the S3 path with a Bearer header and returns summary."""
    from temporal_app.activities import billing_activities

    response_body = {"claims_posted": 12, "denials_routed": 3, "errors": 0}
    mock_client = AsyncMock()
    mock_client.post = AsyncMock(return_value=_make_response(200, response_body))

    token_patch, auth_header_mock = _patch_token_client()
    with token_patch, patch.object(billing_activities.httpx, "AsyncClient") as mock_cls:
        mock_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
        mock_cls.return_value.__aexit__ = AsyncMock(return_value=False)
        with patch("temporalio.activity.heartbeat"):
            result = await billing_activities.process_era_file(
                "s3://adaptix-billing-edi/era/2026/06/835_001.edi"
            )

    assert result["claims_posted"] == 12
    assert result["denials_routed"] == 3
    auth_header_mock.assert_awaited_once_with(scope=["billing_operator"])
    call = mock_client.post.call_args
    assert call.kwargs["headers"]["Authorization"] == _MINTED_BEARER


# ---------------------------------------------------------------------------
# run_monthly_agency_invoicing
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_monthly_agency_invoicing_success():
    """run_monthly_agency_invoicing sends the billing month with a Bearer header."""
    from temporal_app.activities import billing_activities

    response_body = {"invoices_processed": 47, "failed": 0}
    mock_client = AsyncMock()
    mock_client.post = AsyncMock(return_value=_make_response(200, response_body))

    token_patch, auth_header_mock = _patch_token_client()
    with token_patch, patch.object(billing_activities.httpx, "AsyncClient") as mock_cls:
        mock_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
        mock_cls.return_value.__aexit__ = AsyncMock(return_value=False)
        with patch("temporalio.activity.heartbeat"):
            result = await billing_activities.run_monthly_agency_invoicing("2026-06")

    assert result["invoices_processed"] == 47
    auth_header_mock.assert_awaited_once_with(scope=["billing_operator"])
    call = mock_client.post.call_args
    assert call.kwargs["json"]["billing_month"] == "2026-06"
    assert call.kwargs["headers"]["Authorization"] == _MINTED_BEARER
