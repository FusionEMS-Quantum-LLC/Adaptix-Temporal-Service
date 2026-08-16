"""Tests for migration domain activities — they must refuse, never fake success.

The Adaptix Imports service that performs migration work does not exist yet.
Every activity in ``temporal_app.activities.migration_activities`` is therefore a
declared-but-unimplemented step. The danger a stub creates is not that it fails —
it is that it SUCCEEDS: a stub returning ``{"status": "ok"}`` would show an
operator a green cutover for an agency whose data was never moved, and that
operator would switch off the legacy system.

What these tests prove:
  - Every migration activity raises, and none of them returns a value.
  - Each raises an ``ApplicationError`` typed ``MigrationActivityNotImplemented``
    and marked ``non_retryable`` — so Temporal fails it fast instead of burning
    the retry budget on a missing implementation.
  - The failure message names the activity and the owning service, so an
    operator who hits one knows what is missing.
  - No failure message leaks the tenant or migration identifier it was called
    with.
  - The workflow's retry policy lists the same error type, so the two agree.

What these tests do NOT prove:
  - Anything about the real implementations — there are none yet.
  - That the Imports service exists or behaves correctly.
"""

from __future__ import annotations

import inspect

import pytest
from temporalio.exceptions import ApplicationError

from temporal_app.activities import migration_activities
from temporal_app.activities.migration_activities import (
    NOT_IMPLEMENTED_ERROR_TYPE,
    backfill_migration_history,
    build_field_mapping,
    profile_source_dataset,
    promote_migration_cutover,
    reconcile_migration,
    rollback_migration,
    run_migration_dry_run,
)

TENANT_ID = "11111111-2222-3333-4444-555555555555"
MIGRATION_ID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"

# Every activity paired with the arguments the workflow calls it with.
_ACTIVITY_CALLS = [
    (profile_source_dataset, (TENANT_ID, MIGRATION_ID)),
    (build_field_mapping, (TENANT_ID, MIGRATION_ID)),
    (run_migration_dry_run, (TENANT_ID, MIGRATION_ID, "v1")),
    (reconcile_migration, (TENANT_ID, MIGRATION_ID, "pre_cutover")),
    (promote_migration_cutover, (TENANT_ID, MIGRATION_ID, "user-7")),
    (rollback_migration, (TENANT_ID, MIGRATION_ID, "user-8")),
    (backfill_migration_history, (TENANT_ID, MIGRATION_ID, "cursor-1")),
]

_ACTIVITY_IDS = [fn.__name__ for fn, _ in _ACTIVITY_CALLS]


@pytest.mark.parametrize(("activity_fn", "args"), _ACTIVITY_CALLS, ids=_ACTIVITY_IDS)
@pytest.mark.asyncio
async def test_activity_refuses_instead_of_returning_a_fake_result(activity_fn, args):
    """An unimplemented migration step raises; it never reports success."""
    with pytest.raises(ApplicationError) as exc:
        await activity_fn(*args)

    assert exc.value.type == NOT_IMPLEMENTED_ERROR_TYPE
    assert exc.value.non_retryable is True


@pytest.mark.parametrize(("activity_fn", "args"), _ACTIVITY_CALLS, ids=_ACTIVITY_IDS)
@pytest.mark.asyncio
async def test_failure_names_the_activity_and_the_owning_service(activity_fn, args):
    """An operator who hits the failure learns what is missing."""
    with pytest.raises(ApplicationError) as exc:
        await activity_fn(*args)

    message = str(exc.value)
    assert activity_fn.__name__ in message
    assert "Imports service" in message


@pytest.mark.parametrize(("activity_fn", "args"), _ACTIVITY_CALLS, ids=_ACTIVITY_IDS)
@pytest.mark.asyncio
async def test_failure_message_carries_no_identifiers(activity_fn, args):
    """Error strings reach logs and failure payloads — keep identifiers out."""
    with pytest.raises(ApplicationError) as exc:
        await activity_fn(*args)

    message = str(exc.value)
    assert TENANT_ID not in message
    assert MIGRATION_ID not in message


@pytest.mark.parametrize(("activity_fn", "args"), _ACTIVITY_CALLS, ids=_ACTIVITY_IDS)
def test_activity_is_registered_as_a_temporal_activity(activity_fn, args):
    """Each stub is a real @activity.defn, so replacing its body is the only change."""
    assert getattr(activity_fn, "__temporal_activity_definition", None) is not None
    assert inspect.iscoroutinefunction(activity_fn)


def test_no_migration_activity_can_return_without_raising():
    """No implementation has quietly appeared with a hardcoded success path."""
    for activity_fn, _args in _ACTIVITY_CALLS:
        source = inspect.getsource(activity_fn)
        assert "_not_implemented" in source, activity_fn.__name__
        assert "return {" not in source, activity_fn.__name__


def test_workflow_retry_policy_agrees_with_the_error_type():
    """The scheduling site declares the same error type the activities raise."""
    from temporal_app.workflows.migration import _MIGRATION_RETRY

    assert NOT_IMPLEMENTED_ERROR_TYPE in (
        _MIGRATION_RETRY.non_retryable_error_types or []
    )


def test_module_states_plainly_that_it_is_not_implemented():
    """The module docstring must not read as if migration works today."""
    doc = migration_activities.__doc__ or ""

    assert "NOT IMPLEMENTED" in doc.upper()
    assert "Imports service" in doc
