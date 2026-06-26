"""System-token client for Temporal workers.

Temporal worker activation Phase 1 (auth foundation), Temporal-Service side.

Workers hold only ``CORE_PROVISIONING_TOKEN`` (an ECS secret), never the RS256
private key. This client exchanges that provisioning token — via Core's internal
token-mint route — for a short-lived RS256 **system JWT**, caches it, refreshes
it before expiry, and exposes it as an ``Authorization: Bearer <system JWT>``
header for downstream calls to ``ADAPTIX_API_BASE`` (the gateway/internal ALB).

Flow
----
1. ``POST {CORE_SERVICE_URL}/api/v1/core/internal/system-token``
   with ``Authorization: Bearer {CORE_PROVISIONING_TOKEN}`` (Cloud Map direct
   hop — this is the only direct-to-Core call; everything else goes through the
   gateway).
2. Cache the returned ``{token, expires_in}``.
3. Re-use the cached token until ``exp - SYSTEM_TOKEN_REFRESH_SKEW_S``, then
   re-mint.

This module does **not** re-point any activities — that is Phase 2. It only adds
the client + caching so the wiring is ready.

Security
--------
* ``CORE_PROVISIONING_TOKEN`` is never logged.
* The minted system JWT value is never logged.
* This client is async-safe: concurrent callers share one in-flight mint via an
  asyncio lock so the provisioning token is not stamped repeatedly under load.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass

import httpx

from temporal_app import config

logger = logging.getLogger(__name__)

_MINT_PATH = "/api/v1/core/internal/system-token"


class SystemTokenError(RuntimeError):
    """Raised when a system token cannot be obtained.

    Treated as non-retryable at the activity layer: a missing/invalid
    provisioning token or an unreachable Core mint route will not resolve by
    retrying the same call — the deployment must be corrected.
    """


@dataclass
class _CachedToken:
    token: str
    # Monotonic deadline (time.monotonic seconds) after which the token must be
    # refreshed. Computed as mint_time + expires_in - refresh_skew.
    refresh_after: float


class SystemTokenClient:
    """Caches and refreshes a short-lived system JWT minted by Core."""

    def __init__(
        self,
        *,
        core_service_url: str | None = None,
        provisioning_token: str | None = None,
        refresh_skew_s: int | None = None,
        default_ttl_s: int | None = None,
        timeout_s: float | None = None,
    ) -> None:
        self._core_service_url = (
            core_service_url
            if core_service_url is not None
            else config.CORE_SERVICE_URL
        ).rstrip("/")
        self._provisioning_token = (
            provisioning_token
            if provisioning_token is not None
            else config.CORE_PROVISIONING_TOKEN
        )
        self._refresh_skew_s = (
            refresh_skew_s
            if refresh_skew_s is not None
            else config.SYSTEM_TOKEN_REFRESH_SKEW_S
        )
        self._default_ttl_s = (
            default_ttl_s
            if default_ttl_s is not None
            else config.SYSTEM_TOKEN_DEFAULT_TTL_S
        )
        self._timeout_s = (
            timeout_s if timeout_s is not None else config.SYSTEM_TOKEN_MINT_TIMEOUT_S
        )
        self._cached: _CachedToken | None = None
        self._lock = asyncio.Lock()

    # -- public API -------------------------------------------------------- #
    async def get_token(self, *, force_refresh: bool = False) -> str:
        """Return a valid system JWT, minting/refreshing as needed."""
        if not force_refresh and self._is_fresh():
            assert self._cached is not None
            return self._cached.token

        async with self._lock:
            # Re-check under the lock — another caller may have refreshed while
            # we were waiting.
            if not force_refresh and self._is_fresh():
                assert self._cached is not None
                return self._cached.token
            return await self._mint_locked()

    async def auth_header(self, *, force_refresh: bool = False) -> dict[str, str]:
        """Return the ``Authorization`` header for ADAPTIX_API_BASE calls."""
        token = await self.get_token(force_refresh=force_refresh)
        return {"Authorization": f"Bearer {token}"}

    # -- internals --------------------------------------------------------- #
    def _is_fresh(self) -> bool:
        return (
            self._cached is not None and time.monotonic() < self._cached.refresh_after
        )

    async def _mint_locked(self) -> str:
        if not self._core_service_url:
            raise SystemTokenError(
                "CORE_SERVICE_URL is not configured — cannot mint a system token. "
                "This error is non-retryable; fix the deployment."
            )
        if not self._provisioning_token:
            raise SystemTokenError(
                "CORE_PROVISIONING_TOKEN is not configured — cannot mint a system "
                "token. This error is non-retryable; fix the deployment."
            )

        url = f"{self._core_service_url}{_MINT_PATH}"
        headers = {"Authorization": f"Bearer {self._provisioning_token}"}
        mint_started = time.monotonic()
        try:
            async with httpx.AsyncClient(timeout=self._timeout_s) as client:
                resp = await client.post(url, headers=headers, json={})
        except httpx.HTTPError as exc:
            # Network/connect/timeout — surface without leaking the token.
            raise SystemTokenError(
                f"system-token mint request failed: {type(exc).__name__}"
            ) from exc

        if resp.status_code in (401, 403):
            raise SystemTokenError(
                f"system-token mint rejected with {resp.status_code} — check "
                "CORE_PROVISIONING_TOKEN. This error is non-retryable."
            )
        if resp.status_code >= 400:
            raise SystemTokenError(
                f"system-token mint returned HTTP {resp.status_code}"
            )

        try:
            body = resp.json()
        except ValueError as exc:
            raise SystemTokenError(
                "system-token mint returned a non-JSON body"
            ) from exc

        token = body.get("token")
        if not token or not isinstance(token, str):
            raise SystemTokenError("system-token mint response missing 'token'")

        expires_in = body.get("expires_in")
        ttl = (
            int(expires_in)
            if isinstance(expires_in, int) and expires_in > 0
            else self._default_ttl_s
        )

        # Refresh slightly before expiry. Clamp so refresh_after is always in the
        # future even for very short TTLs.
        skew = min(self._refresh_skew_s, max(ttl - 1, 0))
        self._cached = _CachedToken(
            token=token, refresh_after=mint_started + (ttl - skew)
        )

        logger.info(
            "system_token_client: minted system token (ttl=%ds, refresh_skew=%ds); token value not logged",
            ttl,
            skew,
        )
        return token


# Process-wide singleton. Activities (in Phase 2) obtain the auth header via
# ``await get_system_token_client().auth_header()``.
_CLIENT: SystemTokenClient | None = None


def get_system_token_client() -> SystemTokenClient:
    """Return the process-wide system-token client, constructing it on first use."""
    global _CLIENT
    if _CLIENT is None:
        _CLIENT = SystemTokenClient()
    return _CLIENT


def reset_system_token_client() -> None:
    """Reset the singleton (test helper)."""
    global _CLIENT
    _CLIENT = None
