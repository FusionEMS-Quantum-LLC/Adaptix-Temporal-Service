"""Pytest configuration and shared fixtures for Adaptix Temporal worker tests.

Sets required environment variables before any module imports so config.py
does not fail at collection time.

All tests that make HTTP calls use patched httpx.AsyncClient — no live
Adaptix API calls are made during unit testing.
"""

import os
import sys
from pathlib import Path

# Ensure the backend package is importable without installation.
BACKEND_ROOT = Path(__file__).resolve().parent.parent / "backend"
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

# Set required environment variables before any module is imported.
# These are test-safe values — no real credentials or production URLs.
os.environ.setdefault("TEMPORAL_HOST", "localhost:7233")
os.environ.setdefault("TEMPORAL_NAMESPACE", "adaptix-test")
os.environ.setdefault("TASK_QUEUE", "billing")
os.environ.setdefault("ADAPTIX_API_BASE", "https://api.test.adaptixcore.internal")
os.environ.setdefault("ADAPTIX_SERVICE_TOKEN", "test-service-token-not-a-real-secret")
os.environ.setdefault(
    "CORE_PROVISIONING_TOKEN", "test-core-provisioning-token-not-a-real-secret"
)
os.environ.setdefault("CORE_SERVICE_URL", "http://core.test.adaptix.internal:8000")

# Payload codec key for the test suite. Base64 of 32 bytes of the ASCII text
# below — a fixed, published, deliberately obvious NON-SECRET so that a leaked
# test key is worthless and nobody mistakes it for production key material. The
# real key is provisioned in AWS Secrets Manager and injected by ECS.
#
# Tests exercise the fail-closed and plaintext-mode paths by overriding these
# with monkeypatch; this default only keeps the ordinary suite importable.
os.environ.setdefault(
    "TEMPORAL_PAYLOAD_CODEC_KEY",
    "YWRhcHRpeC10ZXN0LWtleS1ub3QtYS1zZWNyZXQhISE=",
)
