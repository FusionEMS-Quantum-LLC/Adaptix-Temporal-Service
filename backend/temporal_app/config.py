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
    ADAPTIX_SERVICE_TOKEN — Bearer token for inter-service API authentication.
                            Required. All activities fail if absent.
                            Sourced from AWS Secrets Manager via ECS task definition.
    AWS_REGION            — AWS region for boto3 calls. Defaults to us-east-1.
    WORKER_MAX_CONCURRENT_WORKFLOW_TASKS
                          — Max concurrent workflow task poll slots. Default 40.
    WORKER_MAX_CONCURRENT_ACTIVITY_TASKS
                          — Max concurrent activity task poll slots. Default 100.
"""

from __future__ import annotations

import os
from datetime import timedelta
from typing import Optional

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
ADAPTIX_SERVICE_TOKEN: str | None = os.environ.get("ADAPTIX_SERVICE_TOKEN") or None

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

ACTIVITY_HTTP_TIMEOUT_S: float = float(
    os.environ.get("ACTIVITY_HTTP_TIMEOUT_S", "30")
)

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
        errors.append("TASK_QUEUE is required (billing|notifications|documents|onboarding)")
    elif TASK_QUEUE not in VALID_TASK_QUEUES:
        errors.append(
            f"TASK_QUEUE '{TASK_QUEUE}' is not recognised. "
            f"Valid values: {sorted(VALID_TASK_QUEUES)}"
        )

    if not ADAPTIX_API_BASE:
        errors.append(
            "ADAPTIX_API_BASE is required "
            "(e.g. https://api.adaptixcore.com)"
        )

    if not ADAPTIX_SERVICE_TOKEN:
        errors.append(
            "ADAPTIX_SERVICE_TOKEN is required for inter-service API authentication. "
            "Set this via ECS task definition secret (AWS Secrets Manager)."
        )

    return errors
