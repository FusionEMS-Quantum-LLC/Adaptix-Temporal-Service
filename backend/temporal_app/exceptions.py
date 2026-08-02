"""Application-specific activity exceptions for the Adaptix Temporal worker.

Temporal converts a non-``ApplicationError`` exception raised inside an activity
into an ``ApplicationError`` whose ``type`` is ``type(exc).__name__`` (the
concrete class name). The retry policies in ``config.py`` list
``non_retryable_error_types=["ValidationError", "AuthorizationError"]`` and the
Temporal server matches those strings against that ``type``.

Raising the CPython builtins ``ValueError``/``PermissionError`` produced a
``type`` of ``"ValueError"``/``"PermissionError"``, which never matched the
declared non-retryable names — so 4xx failures the activities intend to be
fail-fast were classified retryable and hammered up to ``maximum_attempts``.

These classes give the raised exception the exact class name the retry policies
declare, so the classification triggers as documented. They subclass the
builtins they replace so existing ``except``/test semantics that expect a
``ValueError``/``PermissionError`` continue to hold.
"""

from __future__ import annotations


class ValidationError(ValueError):
    """Raised for 400/422 responses — bad input no retry will fix.

    Non-retryable: its class name matches ``non_retryable_error_types`` so
    Temporal fails the activity fast instead of retrying.
    """


class AuthorizationError(PermissionError):
    """Raised for 401/403 responses — a permission or token misconfiguration.

    Non-retryable: its class name matches ``non_retryable_error_types`` so
    Temporal fails the activity fast instead of retrying.
    """
