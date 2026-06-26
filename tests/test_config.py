"""Tests for temporal_app.config — startup validation logic.

What these tests prove:
  - validate_config() correctly identifies missing required variables.
  - validate_config() rejects unknown TASK_QUEUE values.
  - validate_config() returns an empty list when all required values are present.
  - Valid task queue values are accepted.

What these tests do NOT prove:
  - Runtime behavior against a real Temporal server.
  - Production environment variable values.
  - Worker connection success.
"""

from __future__ import annotations

import importlib
import os
import sys


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
    """All four valid task queue values produce no task-queue error."""
    valid_queues = ["billing", "notifications", "documents", "onboarding"]
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
