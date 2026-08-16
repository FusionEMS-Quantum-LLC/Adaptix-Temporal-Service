"""Temporal worker configuration for the Adaptix platform.

All configuration is sourced from environment variables — no defaults
carry sensitive values or production-specific URLs.

Environment variables:
    TEMPORAL_HOST         — Temporal server host:port (e.g. temporal.internal:7233).
                            Required. Worker refuses to start without this.
    TEMPORAL_NAMESPACE    — Temporal namespace. Defaults to "adaptix".
    TASK_QUEUE            — Which task queue this worker handles.
                            One of: billing | notifications | documents |
                            onboarding | migration | migration-bulk.
                            Required. Worker refuses to start without a recognised value.
    ENVIRONMENT           — Deployment environment name ("production" in AWS,
                            set by the ECS task definition). Used to refuse the
                            plaintext payload-codec flag in production.
    TEMPORAL_PAYLOAD_CODEC_KEY
                          — Payload encryption keyring. Required in every
                            environment that does not set the local plaintext
                            flag. Sourced from AWS Secrets Manager via the ECS
                            task definition, exactly like CORE_PROVISIONING_TOKEN.
                            Shape: {"primary_key_id": "...",
                                    "keys": {"...": "<base64 32 bytes>"}}
                            (a bare base64 32-byte key is accepted for local dev).
    TEMPORAL_PAYLOAD_CODEC_PLAINTEXT_LOCAL
                          — Explicit local/test escape hatch that disables
                            payload encryption. Defaults OFF and is REFUSED when
                            ENVIRONMENT names a production environment.
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

# Domain queues. Each is served by its own ECS worker service so one domain's
# backlog can never consume another domain's poll slots.
BILLING_TASK_QUEUE: str = "billing"
NOTIFICATIONS_TASK_QUEUE: str = "notifications"
DOCUMENTS_TASK_QUEUE: str = "documents"
ONBOARDING_TASK_QUEUE: str = "onboarding"

# Migration control plane: profiling, mapping decisions, dry runs,
# reconciliation, cutover approval, rollback. Low volume, latency-sensitive,
# human-in-the-loop.
MIGRATION_TASK_QUEUE: str = "migration"

# Migration bulk plane: history backfill and high-volume promotion.
#
# WHY THIS IS A SEPARATE QUEUE — do not merge it back into `migration`.
# A single agency's history backfill is millions of records. Temporal dispatches
# activity tasks to whichever worker polls the queue, so bulk and control-plane
# work sharing one queue means a backfill saturates every activity slot and the
# control plane (and, on a shared worker, live revenue work) waits behind it.
# Separate queues mean separate ECS services, separate concurrency budgets, and
# separate scaling: bulk can be throttled or stopped outright without pausing a
# cutover approval or a claim submission.
MIGRATION_BULK_TASK_QUEUE: str = "migration-bulk"

VALID_TASK_QUEUES: frozenset[str] = frozenset(
    {
        BILLING_TASK_QUEUE,
        NOTIFICATIONS_TASK_QUEUE,
        DOCUMENTS_TASK_QUEUE,
        ONBOARDING_TASK_QUEUE,
        MIGRATION_TASK_QUEUE,
        MIGRATION_BULK_TASK_QUEUE,
    }
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
# Deployment environment
# ---------------------------------------------------------------------------

#: Environment variable names, shared with ``temporal_app.codec`` so the
#: startup validator below and the codec itself can never disagree about which
#: variable they are reading.
ENVIRONMENT_ENV: str = "ENVIRONMENT"
PAYLOAD_CODEC_KEY_ENV: str = "TEMPORAL_PAYLOAD_CODEC_KEY"
PAYLOAD_CODEC_PLAINTEXT_ENV: str = "TEMPORAL_PAYLOAD_CODEC_PLAINTEXT_LOCAL"

#: Values of ENVIRONMENT that mean "real customer data lives here". Matched
#: case-insensitively. Anything in this set forbids unencrypted payloads.
PRODUCTION_ENVIRONMENTS: frozenset[str] = frozenset({"production", "prod"})

ENVIRONMENT: str = os.environ.get(ENVIRONMENT_ENV, "")


def is_production_environment(value: str | None) -> bool:
    """Return True when ``value`` names a production environment.

    Pure and case/whitespace tolerant so the same rule applies to the module
    constant, to a value read live by the codec, and to tests.
    """
    return (value or "").strip().lower() in PRODUCTION_ENVIRONMENTS


def env_flag_is_true(value: str | None) -> bool:
    """Interpret an environment variable as a boolean flag, defaulting to False.

    Only an explicit affirmative enables a flag. Anything else — unset, empty,
    "0", "false", "off", or a typo — is False, so a safety flag can never be
    switched on by accident.
    """
    return (value or "").strip().lower() in {"1", "true", "yes", "on"}


# ---------------------------------------------------------------------------
# Temporal payload encryption (see temporal_app/codec.py)
# ---------------------------------------------------------------------------

TEMPORAL_PAYLOAD_CODEC_KEY: str = os.environ.get(PAYLOAD_CODEC_KEY_ENV, "")
TEMPORAL_PAYLOAD_CODEC_PLAINTEXT_LOCAL: bool = env_flag_is_true(
    os.environ.get(PAYLOAD_CODEC_PLAINTEXT_ENV)
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
            "TASK_QUEUE is required "
            "(billing|notifications|documents|onboarding|migration|migration-bulk)"
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

    # Payload encryption. Fail closed: a worker with no key does not start,
    # because the alternative is writing plaintext payloads into Temporal
    # history on the shared Temporal RDS instance.
    if TEMPORAL_PAYLOAD_CODEC_PLAINTEXT_LOCAL and is_production_environment(
        ENVIRONMENT
    ):
        errors.append(
            f"{PAYLOAD_CODEC_PLAINTEXT_ENV} must never be set when "
            f"{ENVIRONMENT_ENV}='{ENVIRONMENT}'. Unencrypted Temporal payloads "
            "are not permitted in production."
        )
    elif not TEMPORAL_PAYLOAD_CODEC_KEY.strip() and not (
        TEMPORAL_PAYLOAD_CODEC_PLAINTEXT_LOCAL
    ):
        errors.append(
            f"{PAYLOAD_CODEC_KEY_ENV} is required so Temporal payloads are "
            "encrypted before they reach workflow history. Set it via ECS task "
            "definition secret (AWS Secrets Manager). For local development "
            f"only, set {PAYLOAD_CODEC_PLAINTEXT_ENV}=true."
        )

    return errors
