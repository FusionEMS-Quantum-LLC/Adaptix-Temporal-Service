"""Temporal worker configuration for the Adaptix platform.

All configuration is sourced from environment variables — no defaults
carry sensitive values or production-specific URLs.

Environment variables:
    TEMPORAL_HOST         — Temporal server host:port (e.g. temporal.internal:7233).
                            Required. Worker refuses to start without this.
    TEMPORAL_NAMESPACE    — Temporal namespace. Defaults to "adaptix".
    TASK_QUEUE            — Which task queue this worker handles.
                            One of: billing | notifications | documents | onboarding.
                            Required. Worker refuses to start without a recognised value.
    ADAPTIX_API_BASE      — Base URL for internal Adaptix service API calls.
                            Required. All activities fail if absent.
    CORE_PROVISIONING_TOKEN — Bearer token presented to Core's internal
                            token-mint route to obtain a short-lived system JWT.
                            Required. Sourced from AWS Secrets Manager
                            (adaptix/production/core/service-token) via the ECS
                            task definition. Workers hold ONLY this token, never
                            the RS256 private key.
    CORE_SERVICE_URL      — Cloud Map direct-hop base for the Core token-mint
                            call (e.g. http://core.adaptix.internal:8000).
                            Required for the system-token client.
    ADAPTIX_SERVICE_TOKEN — DEPRECATED legacy alias. Phase 1 keeps it as a
                            fallback for CORE_PROVISIONING_TOKEN so existing
                            activities continue to work until Phase 2 re-points
                            them to the system-token client. Prefer
                            CORE_PROVISIONING_TOKEN.
    AWS_REGION            — AWS region for boto3 calls. Defaults to us-east-1.
    WORKER_MAX_CONCURRENT_WORKFLOW_TASKS
                          — Max concurrent workflow task poll slots. Default 40.
    WORKER_MAX_CONCURRENT_ACTIVITY_TASKS
                          — Max concurrent activity task poll slots. Default 100.
"""

from __future__ import annotations

import os
from datetime import timedelta

from temporalio.common import RetryPolicy

# ---------------------------------------------------------------------------
# Temporal connection
# ---------------------------------------------------------------------------

TEMPORAL_HOST: str = os.environ.get("TEMPORAL_HOST", "")
TEMPORAL_NAMESPACE: str = os.environ.get("TEMPORAL_NAMESPACE", "adaptix")

# ---------------------------------------------------------------------------
# Task queue routing
# ---------------------------------------------------------------------------

TASK_QUEUE: str = os.environ.get("TASK_QUEUE", "")

VALID_TASK_QUEUES: frozenset[str] = frozenset(
    {"billing", "notifications", "documents", "onboarding"}
)

# ---------------------------------------------------------------------------
# Inter-service API
# ---------------------------------------------------------------------------

ADAPTIX_API_BASE: str = os.environ.get("ADAPTIX_API_BASE", "").rstrip("/")

# Core token-mint configuration (Temporal worker activation Phase 1).
# CORE_PROVISIONING_TOKEN is the preferred credential; ADAPTIX_SERVICE_TOKEN is
# kept as a legacy fallback so Phase-1 does not break activities that have not
# yet been re-pointed to the system-token client (that re-point is Phase 2).
CORE_PROVISIONING_TOKEN: str | None = (
    os.environ.get("CORE_PROVISIONING_TOKEN")
    or os.environ.get("ADAPTIX_SERVICE_TOKEN")
    or None
)
CORE_SERVICE_URL: str = os.environ.get("CORE_SERVICE_URL", "").rstrip("/")

# Backward-compatible alias. Existing activity modules import
# ADAPTIX_SERVICE_TOKEN directly; resolve it to the provisioning token so they
# keep authenticating during Phase 1. Phase 2 replaces these direct uses with
# the system-token client.
ADAPTIX_SERVICE_TOKEN: str | None = CORE_PROVISIONING_TOKEN

# System-token client tuning.
# Refresh the cached system JWT this many seconds BEFORE its exp so an in-flight
# request never carries an about-to-expire token.
SYSTEM_TOKEN_REFRESH_SKEW_S: int = int(
    os.environ.get("SYSTEM_TOKEN_REFRESH_SKEW_S", "30")
)
# Default lifetime assumed when Core does not return expires_in (it does today).
SYSTEM_TOKEN_DEFAULT_TTL_S: int = int(
    os.environ.get("SYSTEM_TOKEN_DEFAULT_TTL_S", "300")
)
# Timeout for the Core token-mint HTTP call.
SYSTEM_TOKEN_MINT_TIMEOUT_S: float = float(
    os.environ.get("SYSTEM_TOKEN_MINT_TIMEOUT_S", "10")
)

# ---------------------------------------------------------------------------
# AWS
# ---------------------------------------------------------------------------

AWS_REGION: str = os.environ.get("AWS_REGION", "us-east-1")

# ---------------------------------------------------------------------------
# Worker concurrency
# ---------------------------------------------------------------------------

WORKER_MAX_CONCURRENT_WORKFLOW_TASKS: int = int(
    os.environ.get("WORKER_MAX_CONCURRENT_WORKFLOW_TASKS", "40")
)
WORKER_MAX_CONCURRENT_ACTIVITY_TASKS: int = int(
    os.environ.get("WORKER_MAX_CONCURRENT_ACTIVITY_TASKS", "100")
)

# ---------------------------------------------------------------------------
# HTTP client settings
# ---------------------------------------------------------------------------

ACTIVITY_HTTP_TIMEOUT_S: float = float(os.environ.get("ACTIVITY_HTTP_TIMEOUT_S", "30"))

# ---------------------------------------------------------------------------
# Retry policy — applied to all Adaptix workflow activities unless overridden.
#
# Behaviour:
#   - 1 s initial backoff, doubles on each attempt, caps at 10 min.
#   - Maximum 10 total attempts (1 original + 9 retries).
#   - ValidationError and AuthorizationError are non-retryable — these
#     indicate a programming error or a permission misconfiguration that
#     retries cannot resolve. The workflow is expected to fail fast and
#     surface the issue to the operator rather than burning retry budget.
# ---------------------------------------------------------------------------

DEFAULT_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=10),
    maximum_attempts=10,
    non_retryable_error_types=["ValidationError", "AuthorizationError"],
)

# Short retry policy for activities that call external vendors where
# the window for transient errors is narrower.
EXTERNAL_VENDOR_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=2),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=5),
    maximum_attempts=5,
    non_retryable_error_types=["ValidationError", "AuthorizationError"],
)

# ---------------------------------------------------------------------------
# Startup validation
# ---------------------------------------------------------------------------


def validate_config() -> list[str]:
    """Return a list of missing/invalid configuration values.

    Called at worker startup — an empty list means all required values are
    present. A non-empty list is logged as a fatal error and the worker exits.
    """
    errors: list[str] = []

    if not TEMPORAL_HOST:
        errors.append("TEMPORAL_HOST is required (e.g. temporal.internal:7233)")

    if not TASK_QUEUE:
        errors.append(
            "TASK_QUEUE is required (billing|notifications|documents|onboarding)"
        )
    elif TASK_QUEUE not in VALID_TASK_QUEUES:
        errors.append(
            f"TASK_QUEUE '{TASK_QUEUE}' is not recognised. "
            f"Valid values: {sorted(VALID_TASK_QUEUES)}"
        )

    if not ADAPTIX_API_BASE:
        errors.append("ADAPTIX_API_BASE is required (e.g. https://api.adaptixcore.com)")

    if not CORE_PROVISIONING_TOKEN:
        errors.append(
            "CORE_PROVISIONING_TOKEN is required for inter-service API authentication "
            "(legacy ADAPTIX_SERVICE_TOKEN accepted as a fallback). "
            "Set this via ECS task definition secret (AWS Secrets Manager, "
            "adaptix/production/core/service-token)."
        )

    if not CORE_SERVICE_URL:
        errors.append(
            "CORE_SERVICE_URL is required for the Core system-token mint call "
            "(e.g. http://core.adaptix.internal:8000)."
        )

    return errors
