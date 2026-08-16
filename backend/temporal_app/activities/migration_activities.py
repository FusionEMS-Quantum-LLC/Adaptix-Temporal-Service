"""Migration domain activities for Temporal workflows.

STATUS: DECLARED, NOT IMPLEMENTED. READ THIS BEFORE USING ANYTHING HERE.

Every activity in this module raises a non-retryable ``ApplicationError`` of
type ``MigrationActivityNotImplemented`` on its first line. None of them returns
a result, fabricated or otherwise.

That is deliberate and is the whole point of the module. ``MigrationWorkflow``
is a real durable state machine — its signals, its approval gate, its rollback
path and its queue separation are implemented and tested — but the work those
steps perform (reading a legacy billing vendor's export, profiling it, mapping
its fields onto the Adaptix schema, dry-running, reconciling, promoting, rolling
back) belongs to the Adaptix Imports service, which does not exist yet.

A stub that returned ``{"status": "ok"}`` would make the workflow appear to
complete a migration it never performed: an operator would see a green cutover
for an agency whose data was never moved, and would switch off the legacy
system. A hard non-retryable failure is the only safe placeholder. The workflow
fails loudly and visibly at the first unimplemented step, which is exactly the
signal "this path is not ready" should produce.

Replacing a stub means replacing its whole body with a real call to the Imports
service through ``ADAPTIX_API_BASE``, authenticated with a minted system JWT
(see ``temporal_app/system_token_client.py`` and the pattern in
``onboarding_activities.py``). Delete the raise; do not weaken it.

PHI safety
----------
Migration payloads are the highest-PHI data in the platform: they are entire
patient and encounter histories. NOTHING that flows through these activities may
be returned to the workflow beyond identifiers and counts, because a workflow
return value is written into Temporal history. Results must carry internal ids,
counts, status codes and error codes only. Record locators stay inside the
Imports service, addressed by ``migration_id`` and batch cursor.

Money
-----
Any monetary value in a migration result (an AR balance being reconciled, a
posted payment total) is an INTEGER NUMBER OF CENTS. Floats and formatted
currency strings are prohibited in these payloads.
"""

from __future__ import annotations

import logging

from temporalio import activity
from temporalio.exceptions import ApplicationError

logger = logging.getLogger(__name__)

#: Error ``type`` every unimplemented migration activity raises.
#:
#: Non-retryable by construction: a missing implementation is not a transient
#: fault, and retrying it ten times only delays the failure and burns the
#: workflow's retry budget. The name is matched by operators and dashboards, so
#: it must stay stable.
NOT_IMPLEMENTED_ERROR_TYPE = "MigrationActivityNotImplemented"

#: Owner of the real implementations, named in every failure so an operator who
#: hits one knows what is missing rather than filing a bug against Temporal.
_OWNING_SERVICE = "Adaptix Imports service"


def _not_implemented(activity_name: str, work: str) -> ApplicationError:
    """Build the non-retryable error an unimplemented migration step raises.

    The message carries only the activity name and a description of the missing
    capability — never tenant data, never migration record content.
    """
    return ApplicationError(
        f"{activity_name} is declared but not implemented. {work} is owned by "
        f"the {_OWNING_SERVICE}, which is not built yet. This activity refuses "
        "to return a result rather than report success for work it did not do.",
        type=NOT_IMPLEMENTED_ERROR_TYPE,
        non_retryable=True,
    )


# ---------------------------------------------------------------------------
# Control plane — task queue: migration
# ---------------------------------------------------------------------------


@activity.defn
async def profile_source_dataset(
    tenant_id: str, migration_id: str
) -> dict[str, object]:
    """Profile the legacy vendor export: entity counts, field coverage, anomalies.

    Will call the Imports service to inspect the uploaded source dataset and
    return COUNTS AND FIELD NAMES ONLY — never sample rows, which would place
    patient data in workflow history.
    """
    raise _not_implemented(
        "profile_source_dataset",
        "source dataset profiling (entity counts, field coverage, anomaly scan)",
    )


@activity.defn
async def build_field_mapping(tenant_id: str, migration_id: str) -> dict[str, object]:
    """Propose a source-field -> Adaptix-field mapping for human review.

    Returns the proposed mapping plus the list of field keys that need a human
    decision. Field KEYS are schema metadata, not patient data.
    """
    raise _not_implemented(
        "build_field_mapping",
        "source-to-Adaptix field mapping proposal",
    )


@activity.defn
async def run_migration_dry_run(
    tenant_id: str, migration_id: str, mapping_version: str
) -> dict[str, object]:
    """Transform the full source dataset against the approved mapping, writing nothing.

    Long-running: the workflow schedules it with a heartbeat timeout, so the
    real implementation must call ``activity.heartbeat()`` as it advances
    through batches to stay resumable after a worker restart.
    """
    raise _not_implemented(
        "run_migration_dry_run",
        "non-destructive dry-run transformation of the full source dataset",
    )


@activity.defn
async def reconcile_migration(
    tenant_id: str, migration_id: str, phase: str
) -> dict[str, object]:
    """Compare source totals against migrated totals and report the differences.

    The reconciliation report is the evidence a cutover approval rests on.
    Returns counts and integer-cent totals only.
    """
    raise _not_implemented(
        "reconcile_migration",
        "source-vs-target reconciliation (record counts and integer-cent totals)",
    )


@activity.defn
async def promote_migration_cutover(
    tenant_id: str, migration_id: str, approved_by_user_id: str
) -> dict[str, object]:
    """Promote migrated data to live and mark the tenant cut over.

    Protected action. The workflow reaches this only after an explicit named
    human approval; the approver's user id is carried in so the Imports service
    writes it to the audit trail.
    """
    raise _not_implemented(
        "promote_migration_cutover",
        "promotion of migrated data to live tenant state",
    )


@activity.defn
async def rollback_migration(
    tenant_id: str, migration_id: str, requested_by_user_id: str
) -> dict[str, object]:
    """Revert a migration, returning the tenant to its pre-cutover state.

    Protected action, requested by a named human. Long-running: scheduled with
    a heartbeat timeout.
    """
    raise _not_implemented(
        "rollback_migration",
        "rollback of a promoted migration to pre-cutover tenant state",
    )


# ---------------------------------------------------------------------------
# Bulk plane — task queue: migration-bulk
# ---------------------------------------------------------------------------


@activity.defn
async def backfill_migration_history(
    tenant_id: str, migration_id: str, batch_cursor: str
) -> dict[str, object]:
    """Backfill one batch of historical records for a tenant.

    Runs on the ``migration-bulk`` queue, never on ``migration``. It is the
    high-volume step: an agency's history is millions of records, and it must
    not compete for poll slots with cutover approvals or live billing work.

    Batched and cursor-driven on purpose. Returns the next cursor so the
    workflow can drive the backfill in bounded steps, keeping each activity
    short enough to heartbeat and resume, and keeping history small enough to
    continue-as-new.
    """
    raise _not_implemented(
        "backfill_migration_history",
        "bulk historical record backfill",
    )
