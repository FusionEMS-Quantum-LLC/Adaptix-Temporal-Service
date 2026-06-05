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
