"""Tests for temporal_app.config — startup validation logic.

What these tests prove:
  - validate_config() correctly identifies missing required variables.
  - validate_config() rejects unknown TASK_QUEUE values.
  - validate_config() returns an empty list when all required values are present.
  - Valid task queue values are accepted, including the two migration queues.
  - The migration control-plane and bulk queues are registered and SEPARATE, so
    bulk backfill cannot occupy the slots live revenue work needs.
  - validate_config() fails closed on payload encryption: no key is a startup
    error, and the local plaintext flag is refused in a production ENVIRONMENT.

What these tests do NOT prove:
  - Runtime behavior against a real Temporal server.
  - Production environment variable values.
  - Worker connection success.
"""

from __future__ import annotations

import sys

import pytest


def _reload_config(monkeypatch, overrides: dict) -> object:
    """Reload config module with overridden environment variables.

    Removes the cached module so fresh import picks up the patched env.
    """
    for key, value in overrides.items():
        if value is None:
            monkeypatch.delenv(key, raising=False)
        else:
            monkeypatch.setenv(key, value)

    # Remove from sys.modules to force a clean re-import.
    for mod_name in list(sys.modules.keys()):
        if "temporal_app.config" in mod_name:
            del sys.modules[mod_name]

    import temporal_app.config as cfg

    return cfg


def test_validate_config_all_present(monkeypatch):
    """All required variables present returns empty error list."""
    cfg = _reload_config(
        monkeypatch,
        {
            "TEMPORAL_HOST": "temporal.internal:7233",
            "TASK_QUEUE": "billing",
            "ADAPTIX_API_BASE": "https://api.adaptixcore.internal",
            "CORE_SERVICE_URL": "http://core.adaptix.internal:8000",
            "CORE_PROVISIONING_TOKEN": "token-abc",
        },
    )
    errors = cfg.validate_config()
    assert errors == [], f"Expected no errors, got: {errors}"


def test_validate_config_missing_temporal_host(monkeypatch):
    """Missing TEMPORAL_HOST produces an error."""
    cfg = _reload_config(
        monkeypatch,
        {
            "TEMPORAL_HOST": "",
            "TASK_QUEUE": "billing",
            "ADAPTIX_API_BASE": "https://api.adaptixcore.internal",
            "ADAPTIX_SERVICE_TOKEN": "token-abc",
        },
    )
    errors = cfg.validate_config()
    assert any("TEMPORAL_HOST" in e for e in errors)


def test_validate_config_missing_task_queue(monkeypatch):
    """Missing TASK_QUEUE produces an error."""
    cfg = _reload_config(
        monkeypatch,
        {
            "TEMPORAL_HOST": "temporal.internal:7233",
            "TASK_QUEUE": "",
            "ADAPTIX_API_BASE": "https://api.adaptixcore.internal",
            "ADAPTIX_SERVICE_TOKEN": "token-abc",
        },
    )
    errors = cfg.validate_config()
    assert any("TASK_QUEUE" in e for e in errors)


def test_validate_config_invalid_task_queue(monkeypatch):
    """An unrecognised TASK_QUEUE value produces an error."""
    cfg = _reload_config(
        monkeypatch,
        {
            "TEMPORAL_HOST": "temporal.internal:7233",
            "TASK_QUEUE": "not-a-real-queue",
            "ADAPTIX_API_BASE": "https://api.adaptixcore.internal",
            "ADAPTIX_SERVICE_TOKEN": "token-abc",
        },
    )
    errors = cfg.validate_config()
    assert any("not-a-real-queue" in e for e in errors)


def test_validate_config_missing_api_base(monkeypatch):
    """Missing ADAPTIX_API_BASE produces an error."""
    cfg = _reload_config(
        monkeypatch,
        {
            "TEMPORAL_HOST": "temporal.internal:7233",
            "TASK_QUEUE": "billing",
            "ADAPTIX_API_BASE": "",
            "ADAPTIX_SERVICE_TOKEN": "token-abc",
        },
    )
    errors = cfg.validate_config()
    assert any("ADAPTIX_API_BASE" in e for e in errors)


def test_validate_config_missing_provisioning_token(monkeypatch):
    """Missing CORE_PROVISIONING_TOKEN (and legacy fallback) produces an error."""
    cfg = _reload_config(
        monkeypatch,
        {
            "TEMPORAL_HOST": "temporal.internal:7233",
            "TASK_QUEUE": "billing",
            "ADAPTIX_API_BASE": "https://api.adaptixcore.internal",
            "CORE_SERVICE_URL": "http://core.adaptix.internal:8000",
            "CORE_PROVISIONING_TOKEN": "",
            "ADAPTIX_SERVICE_TOKEN": "",
        },
    )
    errors = cfg.validate_config()
    assert any("CORE_PROVISIONING_TOKEN" in e for e in errors)


def test_legacy_service_token_satisfies_provisioning_token(monkeypatch):
    """ADAPTIX_SERVICE_TOKEN still satisfies the provisioning-token requirement."""
    cfg = _reload_config(
        monkeypatch,
        {
            "TEMPORAL_HOST": "temporal.internal:7233",
            "TASK_QUEUE": "billing",
            "ADAPTIX_API_BASE": "https://api.adaptixcore.internal",
            "CORE_SERVICE_URL": "http://core.adaptix.internal:8000",
            "CORE_PROVISIONING_TOKEN": "",
            "ADAPTIX_SERVICE_TOKEN": "legacy-token",
        },
    )
    errors = cfg.validate_config()
    assert not any("CORE_PROVISIONING_TOKEN" in e for e in errors)


def test_validate_config_missing_core_service_url(monkeypatch):
    """Missing CORE_SERVICE_URL produces an error."""
    cfg = _reload_config(
        monkeypatch,
        {
            "TEMPORAL_HOST": "temporal.internal:7233",
            "TASK_QUEUE": "billing",
            "ADAPTIX_API_BASE": "https://api.adaptixcore.internal",
            "CORE_SERVICE_URL": "",
            "CORE_PROVISIONING_TOKEN": "token-abc",
        },
    )
    errors = cfg.validate_config()
    assert any("CORE_SERVICE_URL" in e for e in errors)


def test_valid_task_queues_accepted(monkeypatch):
    """All six valid task queue values produce no task-queue error."""
    valid_queues = [
        "billing",
        "notifications",
        "documents",
        "onboarding",
        "migration",
        "migration-bulk",
    ]
    for queue in valid_queues:
        cfg = _reload_config(
            monkeypatch,
            {
                "TEMPORAL_HOST": "temporal.internal:7233",
                "TASK_QUEUE": queue,
                "ADAPTIX_API_BASE": "https://api.adaptixcore.internal",
                "CORE_SERVICE_URL": "http://core.adaptix.internal:8000",
                "CORE_PROVISIONING_TOKEN": "token-abc",
            },
        )
        errors = cfg.validate_config()
        queue_errors = [e for e in errors if queue in e]
        assert not queue_errors, (
            f"Queue '{queue}' should be valid but produced errors: {queue_errors}"
        )


# ---------------------------------------------------------------------------
# Migration task queues
# ---------------------------------------------------------------------------


def test_migration_queues_are_registered():
    """Both migration queues are recognised task queues."""
    import temporal_app.config as cfg

    assert cfg.MIGRATION_TASK_QUEUE in cfg.VALID_TASK_QUEUES
    assert cfg.MIGRATION_BULK_TASK_QUEUE in cfg.VALID_TASK_QUEUES


def test_migration_control_and_bulk_queues_are_separate():
    """Bulk backfill must not share a queue with the control plane.

    Sharing one queue would let a multi-million-record backfill occupy every
    activity slot, so cutover approvals — and, on a shared worker, live billing
    work — would wait behind it.
    """
    import temporal_app.config as cfg

    assert cfg.MIGRATION_TASK_QUEUE == "migration"
    assert cfg.MIGRATION_BULK_TASK_QUEUE == "migration-bulk"
    assert cfg.MIGRATION_TASK_QUEUE != cfg.MIGRATION_BULK_TASK_QUEUE


def test_existing_queues_are_unchanged():
    """Adding migration queues did not disturb the four production queues."""
    import temporal_app.config as cfg

    assert {
        "billing",
        "notifications",
        "documents",
        "onboarding",
    } <= cfg.VALID_TASK_QUEUES
    assert len(cfg.VALID_TASK_QUEUES) == 6


def test_invalid_task_queue_message_lists_the_migration_queues(monkeypatch):
    """An operator who mistypes a queue is told the migration queues exist."""
    cfg = _reload_config(
        monkeypatch,
        {
            "TEMPORAL_HOST": "temporal.internal:7233",
            "TASK_QUEUE": "",
            "ADAPTIX_API_BASE": "https://api.adaptixcore.internal",
            "CORE_SERVICE_URL": "http://core.adaptix.internal:8000",
            "CORE_PROVISIONING_TOKEN": "token-abc",
        },
    )
    errors = cfg.validate_config()
    assert any("migration-bulk" in e for e in errors)


# ---------------------------------------------------------------------------
# Payload encryption startup gate
# ---------------------------------------------------------------------------

_BASE_ENV = {
    "TEMPORAL_HOST": "temporal.internal:7233",
    "TASK_QUEUE": "billing",
    "ADAPTIX_API_BASE": "https://api.adaptixcore.internal",
    "CORE_SERVICE_URL": "http://core.adaptix.internal:8000",
    "CORE_PROVISIONING_TOKEN": "token-abc",
}

# Fixed NON-SECRET test key: base64 of 32 ASCII bytes that say what they are.
_TEST_CODEC_KEY = "YWRhcHRpeC10ZXN0LWtleS1ub3QtYS1zZWNyZXQhISE="


def test_missing_payload_codec_key_is_a_startup_error(monkeypatch):
    """No key means the worker does not start — it never writes plaintext."""
    cfg = _reload_config(
        monkeypatch,
        {
            **_BASE_ENV,
            "TEMPORAL_PAYLOAD_CODEC_KEY": "",
            "TEMPORAL_PAYLOAD_CODEC_PLAINTEXT_LOCAL": "",
            "ENVIRONMENT": "production",
        },
    )
    errors = cfg.validate_config()
    assert any("TEMPORAL_PAYLOAD_CODEC_KEY" in e for e in errors)


def test_payload_codec_key_present_produces_no_error(monkeypatch):
    """A configured key satisfies the gate."""
    cfg = _reload_config(
        monkeypatch,
        {
            **_BASE_ENV,
            "TEMPORAL_PAYLOAD_CODEC_KEY": _TEST_CODEC_KEY,
            "TEMPORAL_PAYLOAD_CODEC_PLAINTEXT_LOCAL": "",
            "ENVIRONMENT": "production",
        },
    )
    assert cfg.validate_config() == []


def test_plaintext_flag_is_refused_in_production(monkeypatch):
    """The local escape hatch cannot be switched on in production."""
    cfg = _reload_config(
        monkeypatch,
        {
            **_BASE_ENV,
            "TEMPORAL_PAYLOAD_CODEC_KEY": "",
            "TEMPORAL_PAYLOAD_CODEC_PLAINTEXT_LOCAL": "true",
            "ENVIRONMENT": "production",
        },
    )
    errors = cfg.validate_config()
    assert any("TEMPORAL_PAYLOAD_CODEC_PLAINTEXT_LOCAL" in e for e in errors)


def test_plaintext_flag_allowed_without_a_key_outside_production(monkeypatch):
    """Local development may run without a key when it says so explicitly."""
    cfg = _reload_config(
        monkeypatch,
        {
            **_BASE_ENV,
            "TEMPORAL_PAYLOAD_CODEC_KEY": "",
            "TEMPORAL_PAYLOAD_CODEC_PLAINTEXT_LOCAL": "true",
            "ENVIRONMENT": "local",
        },
    )
    errors = cfg.validate_config()
    assert not any("TEMPORAL_PAYLOAD_CODEC" in e for e in errors)


@pytest.mark.parametrize("environment", ["production", "PRODUCTION", "prod", " Prod "])
def test_is_production_environment_matches_production_names(environment):
    """Production detection is case and whitespace tolerant."""
    import temporal_app.config as cfg

    assert cfg.is_production_environment(environment) is True


@pytest.mark.parametrize("environment", ["", None, "local", "staging", "dev", "test"])
def test_is_production_environment_rejects_everything_else(environment):
    """Only the named production environments count as production."""
    import temporal_app.config as cfg

    assert cfg.is_production_environment(environment) is False


@pytest.mark.parametrize("value", ["1", "true", "TRUE", " yes ", "on"])
def test_env_flag_is_true_for_affirmatives(value):
    """An explicit affirmative enables a flag."""
    import temporal_app.config as cfg

    assert cfg.env_flag_is_true(value) is True


@pytest.mark.parametrize("value", [None, "", " ", "0", "false", "off", "no", "maybe"])
def test_env_flag_defaults_to_off(value):
    """A safety flag is never switched on by accident."""
    import temporal_app.config as cfg

    assert cfg.env_flag_is_true(value) is False
