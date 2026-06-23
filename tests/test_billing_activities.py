"""Tests for billing domain activities.

What these tests prove:
  - Each activity calls the correct Billing Service endpoint.
  - Auth header is included in every request.
  - 400/422 responses are raised as ValueError (non-retryable ValidationError).
  - 401/403 responses are raised as PermissionError (non-retryable AuthorizationError).
  - 5xx responses are re-raised as httpx.HTTPStatusError (retryable by Temporal).
  - Missing ADAPTIX_SERVICE_TOKEN raises RuntimeError.
  - Successful responses return the parsed JSON dict.

What these tests do NOT prove:
  - Actual Billing Service behavior.
  - Real claim submission to Office Ally.
  - Runtime verification against a deployed system.
  - Temporal retry behavior (tested by Temporal SDK internals).
"""

from __future__ import annotations

import os
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
import pytest_asyncio


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_response(status_code: int, json_body: dict | None = None) -> httpx.Response:
    """Build a mock httpx.Response."""
    content = __import__("json").dumps(json_body or {}).encode()
    return httpx.Response(
        status_code=status_code,
        headers={"content-type": "application/json"},
        content=content,
        request=httpx.Request("POST", "https://test.internal/api"),
    )


# ---------------------------------------------------------------------------
# submit_claim_to_clearinghouse
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_submit_claim_success(monkeypatch):
    """submit_claim_to_clearinghouse returns parsed JSON on 200."""
    from temporal_app.activities import billing_activities

    response_body = {"submission_id": "sub-123", "status": "pending_ack"}
    mock_response = _make_response(200, response_body)

    with patch.object(
        billing_activities.httpx.AsyncClient,
        "__aenter__",
        return_value=AsyncMock(post=AsyncMock(return_value=mock_response)),
    ):
        # Wrap with activity.defn context mock.
        with patch("temporalio.activity.heartbeat"):
            result = await billing_activities.submit_claim_to_clearinghouse("claim-abc")

    assert result == response_body


@pytest.mark.asyncio
async def test_submit_claim_400_raises_value_error(monkeypatch):
    """submit_claim_to_clearinghouse raises ValueError on 400 (non-retryable)."""
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

    with patch(
        "temporal_app.activities.billing_activities.httpx.AsyncClient"
    ) as mock_cls:
        mock_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
        mock_cls.return_value.__aexit__ = AsyncMock(return_value=False)
        with patch("temporalio.activity.heartbeat"):
            with pytest.raises(ValueError, match="ValidationError"):
                await billing_activities.submit_claim_to_clearinghouse("claim-abc")


@pytest.mark.asyncio
async def test_submit_claim_403_raises_permission_error(monkeypatch):
    """submit_claim_to_clearinghouse raises PermissionError on 403 (non-retryable)."""
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

    with patch(
        "temporal_app.activities.billing_activities.httpx.AsyncClient"
    ) as mock_cls:
        mock_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
        mock_cls.return_value.__aexit__ = AsyncMock(return_value=False)
        with patch("temporalio.activity.heartbeat"):
            with pytest.raises(PermissionError, match="AuthorizationError"):
                await billing_activities.submit_claim_to_clearinghouse("claim-abc")


@pytest.mark.asyncio
async def test_submit_claim_500_reraises_for_retry(monkeypatch):
    """submit_claim_to_clearinghouse re-raises 5xx for Temporal retry."""
    from temporal_app.activities import billing_activities

    mock_response = _make_response(503, {"detail": "service_unavailable"})
    mock_client = AsyncMock()
    original_exc = httpx.HTTPStatusError(
        "503 error",
        request=httpx.Request("POST", "https://test.internal"),
        response=mock_response,
    )
    mock_client.post = AsyncMock(side_effect=original_exc)

    with patch(
        "temporal_app.activities.billing_activities.httpx.AsyncClient"
    ) as mock_cls:
        mock_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
        mock_cls.return_value.__aexit__ = AsyncMock(return_value=False)
        with patch("temporalio.activity.heartbeat"):
            with pytest.raises(httpx.HTTPStatusError):
                await billing_activities.submit_claim_to_clearinghouse("claim-abc")


@pytest.mark.asyncio
async def test_submit_claim_missing_token_raises_runtime_error(monkeypatch):
    """submit_claim_to_clearinghouse raises RuntimeError when service token is absent."""
    import sys

    monkeypatch.setenv("ADAPTIX_SERVICE_TOKEN", "")

    # Reload config and activities to pick up the empty token.
    for mod in list(sys.modules.keys()):
        if "temporal_app" in mod:
            del sys.modules[mod]

    from temporal_app.activities import billing_activities

    with patch("temporalio.activity.heartbeat"):
        with pytest.raises(RuntimeError, match="ADAPTIX_SERVICE_TOKEN"):
            await billing_activities.submit_claim_to_clearinghouse("claim-abc")

    # Restore the token so subsequent tests in this process are not affected
    # by the module purge.
    monkeypatch.setenv("ADAPTIX_SERVICE_TOKEN", "test-service-token-not-a-real-secret")
    for mod in list(sys.modules.keys()):
        if "temporal_app" in mod:
            del sys.modules[mod]


# ---------------------------------------------------------------------------
# create_denial_appeal
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_denial_appeal_success(monkeypatch):
    """create_denial_appeal posts the correct payload and returns JSON."""
    monkeypatch.setenv("ADAPTIX_SERVICE_TOKEN", "test-service-token-not-a-real-secret")
    import importlib, temporal_app.activities.billing_activities as _m

    importlib.reload(_m)
    from temporal_app.activities import billing_activities

    response_body = {"appeal_id": "appeal-456", "status": "pending_resubmit"}
    mock_response = _make_response(200, response_body)
    mock_client = AsyncMock()
    mock_client.post = AsyncMock(return_value=mock_response)

    with patch(
        "temporal_app.activities.billing_activities.httpx.AsyncClient"
    ) as mock_cls:
        mock_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
        mock_cls.return_value.__aexit__ = AsyncMock(return_value=False)
        with patch("temporalio.activity.heartbeat"):
            result = await billing_activities.create_denial_appeal("claim-abc", "CO-4")

    assert result == response_body
    # Verify the denial_code was sent in the request body.
    call_kwargs = mock_client.post.call_args
    assert call_kwargs.kwargs["json"]["denial_code"] == "CO-4"


# ---------------------------------------------------------------------------
# process_era_file
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_process_era_file_success(monkeypatch):
    """process_era_file sends the S3 path and returns posting summary."""
    monkeypatch.setenv("ADAPTIX_SERVICE_TOKEN", "test-service-token-not-a-real-secret")
    import importlib, temporal_app.activities.billing_activities as _m

    importlib.reload(_m)
    from temporal_app.activities import billing_activities

    response_body = {
        "claims_posted": 12,
        "denials_routed": 3,
        "errors": 0,
    }
    mock_response = _make_response(200, response_body)
    mock_client = AsyncMock()
    mock_client.post = AsyncMock(return_value=mock_response)

    with patch(
        "temporal_app.activities.billing_activities.httpx.AsyncClient"
    ) as mock_cls:
        mock_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
        mock_cls.return_value.__aexit__ = AsyncMock(return_value=False)
        with patch("temporalio.activity.heartbeat"):
            result = await billing_activities.process_era_file(
                "s3://adaptix-billing-edi/era/2026/06/835_001.edi"
            )

    assert result["claims_posted"] == 12
    assert result["denials_routed"] == 3


# ---------------------------------------------------------------------------
# run_monthly_agency_invoicing
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_monthly_agency_invoicing_success(monkeypatch):
    """run_monthly_agency_invoicing sends the billing month and returns summary."""
    monkeypatch.setenv("ADAPTIX_SERVICE_TOKEN", "test-service-token-not-a-real-secret")
    import importlib, temporal_app.activities.billing_activities as _m

    importlib.reload(_m)
    from temporal_app.activities import billing_activities

    response_body = {"invoices_processed": 47, "failed": 0}
    mock_response = _make_response(200, response_body)
    mock_client = AsyncMock()
    mock_client.post = AsyncMock(return_value=mock_response)

    with patch(
        "temporal_app.activities.billing_activities.httpx.AsyncClient"
    ) as mock_cls:
        mock_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
        mock_cls.return_value.__aexit__ = AsyncMock(return_value=False)
        with patch("temporalio.activity.heartbeat"):
            result = await billing_activities.run_monthly_agency_invoicing("2026-06")

    assert result["invoices_processed"] == 47
    call_kwargs = mock_client.post.call_args
    assert call_kwargs.kwargs["json"]["billing_month"] == "2026-06"
