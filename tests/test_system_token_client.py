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

  - The client asserts its own caller identity (defaulting to config.TASK_QUEUE)
    on every mint request, so Core can bind authority to who is asking
    (companion to the Adaptix-Core-Service caller-scoped authorization fix,
    2026-08-20).
  - A 403 detail from Core (e.g. "caller not permitted to request scope") is
    surfaced in the raised error instead of a generic guess.

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

    def __init__(
        self, token: str = _SYSTEM_JWT, expires_in: int = 300, status: int = 200
    ) -> None:
        self.calls: list[httpx.Request] = []
        self.token = token
        self.expires_in = expires_in
        self.status = status

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.calls.append(request)
        if self.status != 200:
            return httpx.Response(self.status, json={"detail": "denied"})
        return httpx.Response(
            200, json={"token": self.token, "expires_in": self.expires_in}
        )


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
    client = SystemTokenClient(
        core_service_url=_CORE_URL, provisioning_token=_PROV_TOKEN
    )

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
    client = SystemTokenClient(
        core_service_url=_CORE_URL, provisioning_token=_PROV_TOKEN
    )

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
    client = SystemTokenClient(
        core_service_url=_CORE_URL, provisioning_token=_PROV_TOKEN
    )
    await client.get_token()
    await client.get_token(force_refresh=True)
    assert len(rec.calls) == 2


@pytest.mark.asyncio
async def test_auth_header_is_bearer_system_jwt(patch_async_client) -> None:
    rec = _MintRecorder()
    patch_async_client(rec)
    client = SystemTokenClient(
        core_service_url=_CORE_URL, provisioning_token=_PROV_TOKEN
    )
    header = await client.auth_header()
    assert header == {"Authorization": f"Bearer {_SYSTEM_JWT}"}


@pytest.mark.asyncio
async def test_401_is_non_retryable_error(patch_async_client) -> None:
    rec = _MintRecorder(status=401)
    patch_async_client(rec)
    client = SystemTokenClient(
        core_service_url=_CORE_URL, provisioning_token=_PROV_TOKEN
    )
    with pytest.raises(SystemTokenError):
        await client.get_token()


@pytest.mark.asyncio
async def test_403_is_non_retryable_error(patch_async_client) -> None:
    rec = _MintRecorder(status=403)
    patch_async_client(rec)
    client = SystemTokenClient(
        core_service_url=_CORE_URL, provisioning_token=_PROV_TOKEN
    )
    with pytest.raises(SystemTokenError):
        await client.get_token()


@pytest.mark.asyncio
async def test_403_surfaces_cores_own_rejection_detail(patch_async_client) -> None:
    """Companion to the Core-Service caller-scoped authorization fix: a 403
    can now mean Core's resolve_scope rejected this caller/scope combination,
    not only a bad CORE_PROVISIONING_TOKEN. The real reason must be visible
    in the raised error, not papered over with a generic guess."""

    class _DeniedRecorder(_MintRecorder):
        def handler(self, request: httpx.Request) -> httpx.Response:
            self.calls.append(request)
            return httpx.Response(
                403,
                json={"detail": "Caller 'billing' is not permitted to request scope 'onboarding'."},
            )

    rec = _DeniedRecorder()
    patch_async_client(rec)
    client = SystemTokenClient(
        core_service_url=_CORE_URL, provisioning_token=_PROV_TOKEN
    )
    with pytest.raises(SystemTokenError, match="not permitted to request scope 'onboarding'"):
        await client.get_token(scope=["onboarding"])


@pytest.mark.asyncio
async def test_403_with_unparsable_body_falls_back_to_generic_message(
    patch_async_client,
) -> None:
    """A malformed/non-JSON 403 body must not crash the client — it falls
    back to the generic message rather than raising from inside the error
    handler itself."""

    class _BrokenBodyRecorder(_MintRecorder):
        def handler(self, request: httpx.Request) -> httpx.Response:
            self.calls.append(request)
            return httpx.Response(403, content=b"not json")

    rec = _BrokenBodyRecorder()
    patch_async_client(rec)
    client = SystemTokenClient(
        core_service_url=_CORE_URL, provisioning_token=_PROV_TOKEN
    )
    with pytest.raises(SystemTokenError, match="check CORE_PROVISIONING_TOKEN"):
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
    client = SystemTokenClient(
        core_service_url=_CORE_URL, provisioning_token=_PROV_TOKEN
    )
    with pytest.raises(SystemTokenError):
        await client.get_token()


@pytest.mark.asyncio
async def test_provisioning_token_and_jwt_not_logged(
    patch_async_client, caplog
) -> None:
    rec = _MintRecorder()
    patch_async_client(rec)
    client = SystemTokenClient(
        core_service_url=_CORE_URL, provisioning_token=_PROV_TOKEN
    )
    with caplog.at_level("INFO"):
        token = await client.get_token()
    for record in caplog.records:
        msg = record.getMessage()
        assert _PROV_TOKEN not in msg, "provisioning token leaked to logs"
        assert token not in msg, "system JWT leaked to logs"


@pytest.mark.asyncio
async def test_default_scope_sends_only_caller_in_mint_body(patch_async_client) -> None:
    """With no scope requested, the mint body carries no 'scope' key (Core
    defaults to ["system"]) but DOES carry this worker's own 'caller'
    identity — sent unconditionally so Core's audit trail can attribute
    every mint, even the unprivileged default one, to the requesting worker.
    tests/conftest.py sets TASK_QUEUE=billing for this suite, so that is the
    caller value the client defaults to (SystemTokenClient(caller=...) is not
    passed here, exactly like a real worker never overrides it)."""
    import json as _json

    rec = _MintRecorder()
    patch_async_client(rec)
    client = SystemTokenClient(
        core_service_url=_CORE_URL, provisioning_token=_PROV_TOKEN
    )

    await client.get_token()
    body = _json.loads(rec.calls[0].content.decode() or "{}")
    assert body == {"caller": "billing"}


@pytest.mark.asyncio
async def test_scope_and_caller_are_sent_in_mint_body(patch_async_client) -> None:
    """A requested scope is forwarded to Core as {"scope": [...]}, alongside
    this worker's own caller identity — the contract the Billing worker
    relies on to obtain a billing_operator-scoped token that Core will
    actually grant (Core now requires a recognised caller for some scopes
    and confines an identified caller to its own domain for all of them).
    """
    import json as _json

    rec = _MintRecorder()
    patch_async_client(rec)
    client = SystemTokenClient(
        core_service_url=_CORE_URL, provisioning_token=_PROV_TOKEN
    )

    header = await client.auth_header(scope=["billing_operator"])
    assert header == {"Authorization": f"Bearer {_SYSTEM_JWT}"}
    body = _json.loads(rec.calls[0].content.decode() or "{}")
    assert body == {"scope": ["billing_operator"], "caller": "billing"}


@pytest.mark.asyncio
async def test_caller_defaults_from_task_queue_config(patch_async_client, monkeypatch) -> None:
    """Explicit proof the caller identity is sourced from config.TASK_QUEUE
    (already required + validated against VALID_TASK_QUEUES at worker
    startup), not hardcoded or guessed — an onboarding-worker process must
    assert 'onboarding', not 'billing'.

    Patches TASK_QUEUE via temporal_app.system_token_client's OWN bound
    `config` reference (module.config, not a fresh `from temporal_app import
    config`) — test_config.py's _reload_config() helper replaces
    sys.modules["temporal_app.config"] with a brand new module object via
    del+reimport, which orphans any `from temporal_app import config`
    binding other modules already hold (system_token_client's included).
    Going through system_token_client's own reference is what
    SystemTokenClient.__init__ actually reads, so this stays correct
    regardless of suite ordering / whether test_config.py ran first.
    """
    import json as _json

    from temporal_app import system_token_client as stc_module

    monkeypatch.setattr(stc_module.config, "TASK_QUEUE", "onboarding")
    rec = _MintRecorder()
    patch_async_client(rec)
    client = SystemTokenClient(
        core_service_url=_CORE_URL, provisioning_token=_PROV_TOKEN
    )
    await client.get_token(scope=["onboarding"])

    body = _json.loads(rec.calls[0].content.decode() or "{}")
    assert body == {"scope": ["onboarding"], "caller": "onboarding"}


@pytest.mark.asyncio
async def test_explicit_caller_overrides_task_queue_default(patch_async_client) -> None:
    """An explicit caller=... constructor argument wins over config.TASK_QUEUE
    (mirrors how core_service_url/provisioning_token already override their
    config defaults) — used by tests and any future explicit-identity caller."""
    import json as _json

    rec = _MintRecorder()
    patch_async_client(rec)
    # conftest.py sets TASK_QUEUE=billing; explicit caller must still win.
    client = SystemTokenClient(
        core_service_url=_CORE_URL,
        provisioning_token=_PROV_TOKEN,
        caller="documents",
    )
    await client.get_token()
    body = _json.loads(rec.calls[0].content.decode() or "{}")
    assert body == {"caller": "documents"}


@pytest.mark.asyncio
async def test_caller_omitted_entirely_when_explicitly_blank(patch_async_client) -> None:
    """An explicitly blank caller (e.g. a queue name that resolved empty) is
    omitted from the body rather than sent as an empty string — Core treats
    caller="" the same as an unrecognised caller (reject), so a genuinely
    unset identity must look like "no caller asserted", matching the
    pre-existing back-compat behaviour for scope-only callers."""
    import json as _json

    rec = _MintRecorder()
    patch_async_client(rec)
    client = SystemTokenClient(
        core_service_url=_CORE_URL,
        provisioning_token=_PROV_TOKEN,
        caller="",
    )
    await client.get_token()
    body = _json.loads(rec.calls[0].content.decode() or "{}")
    assert body == {}
    assert "caller" not in body


@pytest.mark.asyncio
async def test_caller_sent_on_every_call_not_just_first(patch_async_client) -> None:
    """Caller is a fixed per-process identity (not scope-keyed), so it must
    appear on every mint call this client instance makes, regardless of which
    distinct scope triggered that particular mint."""
    import json as _json

    rec = _MintRecorder()
    patch_async_client(rec)
    client = SystemTokenClient(
        core_service_url=_CORE_URL, provisioning_token=_PROV_TOKEN
    )
    await client.get_token()  # default scope -> mint #1
    await client.get_token(scope=["billing_operator"])  # distinct scope -> mint #2
    assert len(rec.calls) == 2
    for call in rec.calls:
        body = _json.loads(call.content.decode() or "{}")
        assert body.get("caller") == "billing"


@pytest.mark.asyncio
async def test_distinct_scopes_are_cached_separately(patch_async_client) -> None:
    """Default and role-scoped tokens occupy distinct cache slots.

    Requesting the default token then a billing_operator token must mint twice
    (two distinct credentials), and re-requesting either serves from cache.
    """
    rec = _MintRecorder()
    patch_async_client(rec)
    client = SystemTokenClient(
        core_service_url=_CORE_URL, provisioning_token=_PROV_TOKEN
    )

    await client.get_token()  # default scope -> mint #1
    await client.get_token(scope=["billing_operator"])  # billing scope -> mint #2
    await client.get_token()  # default cached
    await client.get_token(scope=["billing_operator"])  # billing cached
    assert len(rec.calls) == 2


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
    client = SystemTokenClient(
        core_service_url=_CORE_URL, provisioning_token=_PROV_TOKEN
    )
    with pytest.raises(SystemTokenError):
        await client.get_token()
