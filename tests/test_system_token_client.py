"""Tests for the Temporal worker system-token client.

What these tests prove:
  - The client requests a token from Core's mint route with the
    CORE_PROVISIONING_TOKEN as a Bearer header (Cloud Map direct hop).
  - The minted token is cached and reused until near expiry.
  - The token is refreshed before it expires.
  - auth_header() returns Authorization: Bearer <system JWT> for ADAPTIX_API_BASE.
  - CORE_PROVISIONING_TOKEN is never written to logs; the system JWT is never logged.
  - Missing CORE_SERVICE_URL / CORE_PROVISIONING_TOKEN -> non-retryable error.
  - 401/403 from the mint route -> non-retryable SystemTokenError.

What these tests do NOT prove:
  - Live behavior against a deployed Core service.
  - Gateway acceptance of the minted token at runtime.
"""

from __future__ import annotations

import httpx
import pytest

from temporal_app.system_token_client import (
    SystemTokenClient,
    SystemTokenError,
    get_system_token_client,
    reset_system_token_client,
)

_PROV_TOKEN = "test-core-provisioning-token-not-a-real-secret"
_CORE_URL = "http://core.test.adaptix.internal:8000"
_MINT_URL = f"{_CORE_URL}/api/v1/core/internal/system-token"
_SYSTEM_JWT = "eyTESTHEADER.eyTESTPAYLOAD.TESTSIGNATURE"


class _MintRecorder:
    """Records mint calls and returns a configurable response."""

    def __init__(self, token: str = _SYSTEM_JWT, expires_in: int = 300, status: int = 200) -> None:
        self.calls: list[httpx.Request] = []
        self.token = token
        self.expires_in = expires_in
        self.status = status

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.calls.append(request)
        if self.status != 200:
            return httpx.Response(self.status, json={"detail": "denied"})
        return httpx.Response(200, json={"token": self.token, "expires_in": self.expires_in})


@pytest.fixture(autouse=True)
def _reset_singleton():
    reset_system_token_client()
    yield
    reset_system_token_client()


@pytest.fixture()
def patch_async_client(monkeypatch):
    """Route all httpx.AsyncClient traffic through a per-test MockTransport."""

    def _install(recorder: _MintRecorder) -> None:
        transport = httpx.MockTransport(recorder.handler)
        orig_init = httpx.AsyncClient.__init__

        def patched_init(self, *a, **kw):  # type: ignore[no-untyped-def]
            kw.pop("transport", None)
            orig_init(self, *a, transport=transport, **kw)

        monkeypatch.setattr(httpx.AsyncClient, "__init__", patched_init)

    return _install


@pytest.mark.asyncio
async def test_mint_sends_provisioning_token_bearer(patch_async_client) -> None:
    rec = _MintRecorder()
    patch_async_client(rec)
    client = SystemTokenClient(core_service_url=_CORE_URL, provisioning_token=_PROV_TOKEN)

    token = await client.get_token()
    assert token == _SYSTEM_JWT
    assert len(rec.calls) == 1
    req = rec.calls[0]
    assert str(req.url) == _MINT_URL
    assert req.headers["Authorization"] == f"Bearer {_PROV_TOKEN}"


@pytest.mark.asyncio
async def test_token_is_cached_and_reused(patch_async_client) -> None:
    rec = _MintRecorder(expires_in=300)
    patch_async_client(rec)
    client = SystemTokenClient(core_service_url=_CORE_URL, provisioning_token=_PROV_TOKEN)

    t1 = await client.get_token()
    t2 = await client.get_token()
    assert t1 == t2 == _SYSTEM_JWT
    # Only one mint call — the second read served from cache.
    assert len(rec.calls) == 1


@pytest.mark.asyncio
async def test_token_refreshes_before_expiry(patch_async_client, monkeypatch) -> None:
    # Deterministic clock: mint at t=1000 with TTL=300, skew=30 ->
    # refresh_after = 1000 + (300 - 30) = 1270. At t=1271 the cached token is
    # stale and a second mint occurs.
    rec = _MintRecorder(expires_in=300)
    patch_async_client(rec)

    fake = {"now": 1000.0}
    monkeypatch.setattr(
        "temporal_app.system_token_client.time.monotonic",
        lambda: fake["now"],
    )
    client = SystemTokenClient(
        core_service_url=_CORE_URL,
        provisioning_token=_PROV_TOKEN,
        refresh_skew_s=30,
    )
    await client.get_token()
    assert len(rec.calls) == 1

    # Advance clock past refresh_after — next read must re-mint.
    fake["now"] = 1271.0
    await client.get_token()
    assert len(rec.calls) == 2

    # Still within the new token's window — no extra mint.
    fake["now"] = 1300.0
    await client.get_token()
    assert len(rec.calls) == 2


@pytest.mark.asyncio
async def test_force_refresh_remints(patch_async_client) -> None:
    rec = _MintRecorder(expires_in=300)
    patch_async_client(rec)
    client = SystemTokenClient(core_service_url=_CORE_URL, provisioning_token=_PROV_TOKEN)
    await client.get_token()
    await client.get_token(force_refresh=True)
    assert len(rec.calls) == 2


@pytest.mark.asyncio
async def test_auth_header_is_bearer_system_jwt(patch_async_client) -> None:
    rec = _MintRecorder()
    patch_async_client(rec)
    client = SystemTokenClient(core_service_url=_CORE_URL, provisioning_token=_PROV_TOKEN)
    header = await client.auth_header()
    assert header == {"Authorization": f"Bearer {_SYSTEM_JWT}"}


@pytest.mark.asyncio
async def test_401_is_non_retryable_error(patch_async_client) -> None:
    rec = _MintRecorder(status=401)
    patch_async_client(rec)
    client = SystemTokenClient(core_service_url=_CORE_URL, provisioning_token=_PROV_TOKEN)
    with pytest.raises(SystemTokenError):
        await client.get_token()


@pytest.mark.asyncio
async def test_403_is_non_retryable_error(patch_async_client) -> None:
    rec = _MintRecorder(status=403)
    patch_async_client(rec)
    client = SystemTokenClient(core_service_url=_CORE_URL, provisioning_token=_PROV_TOKEN)
    with pytest.raises(SystemTokenError):
        await client.get_token()


@pytest.mark.asyncio
async def test_missing_core_service_url_raises() -> None:
    client = SystemTokenClient(core_service_url="", provisioning_token=_PROV_TOKEN)
    with pytest.raises(SystemTokenError):
        await client.get_token()


@pytest.mark.asyncio
async def test_missing_provisioning_token_raises() -> None:
    client = SystemTokenClient(core_service_url=_CORE_URL, provisioning_token=None)
    with pytest.raises(SystemTokenError):
        await client.get_token()


@pytest.mark.asyncio
async def test_missing_token_in_response_raises(patch_async_client) -> None:
    rec = _MintRecorder(token="")  # empty token
    patch_async_client(rec)
    client = SystemTokenClient(core_service_url=_CORE_URL, provisioning_token=_PROV_TOKEN)
    with pytest.raises(SystemTokenError):
        await client.get_token()


@pytest.mark.asyncio
async def test_provisioning_token_and_jwt_not_logged(patch_async_client, caplog) -> None:
    rec = _MintRecorder()
    patch_async_client(rec)
    client = SystemTokenClient(core_service_url=_CORE_URL, provisioning_token=_PROV_TOKEN)
    with caplog.at_level("INFO"):
        token = await client.get_token()
    for record in caplog.records:
        msg = record.getMessage()
        assert _PROV_TOKEN not in msg, "provisioning token leaked to logs"
        assert token not in msg, "system JWT leaked to logs"


def test_singleton_is_stable() -> None:
    a = get_system_token_client()
    b = get_system_token_client()
    assert a is b
    reset_system_token_client()
    c = get_system_token_client()
    assert c is not a


@pytest.mark.asyncio
async def test_network_error_is_non_retryable(monkeypatch) -> None:
    def boom_handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused", request=request)

    transport = httpx.MockTransport(boom_handler)
    orig_init = httpx.AsyncClient.__init__

    def patched_init(self, *a, **kw):  # type: ignore[no-untyped-def]
        kw.pop("transport", None)
        orig_init(self, *a, transport=transport, **kw)

    monkeypatch.setattr(httpx.AsyncClient, "__init__", patched_init)
    client = SystemTokenClient(core_service_url=_CORE_URL, provisioning_token=_PROV_TOKEN)
    with pytest.raises(SystemTokenError):
        await client.get_token()
