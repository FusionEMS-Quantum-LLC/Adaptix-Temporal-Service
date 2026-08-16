"""Adaptix Temporal migration CONTROL-PLANE worker entrypoint.

ECS task CMD override:
    python -m workers.migration_worker

Environment variables required:
    TEMPORAL_HOST         — Temporal server host:port
    TEMPORAL_NAMESPACE    — Defaults to "adaptix"
    TEMPORAL_PAYLOAD_CODEC_KEY
                          — AES-256 payload encryption keyring (AWS Secrets
                            Manager via the ECS task definition). Required:
                            the worker refuses to start without it so no
                            plaintext payload can reach workflow history.
                            Migration payloads are the highest-PHI data on the
                            platform, so this matters most here.
    TASK_QUEUE            — Must be "migration" (validated at startup)
    ADAPTIX_API_BASE      — Internal API base URL
    ADAPTIX_SERVICE_TOKEN — Bearer token for inter-service authentication

This worker registers:
  Workflows:
    - MigrationWorkflow
  Activities:
    - profile_source_dataset
    - build_field_mapping
    - run_migration_dry_run
    - reconcile_migration
    - promote_migration_cutover
    - rollback_migration

It deliberately does NOT register backfill_migration_history. That activity is
bulk work and belongs to the separate `migration-bulk` queue
(workers/migration_bulk_worker.py), so a multi-million-record backfill can never
occupy the slots a cutover approval or a live billing workflow needs.

The activities above currently raise a non-retryable
MigrationActivityNotImplemented — the Adaptix Imports service that performs the
work is not built yet. This worker is real and will run; the first step a
migration schedules fails loudly until Imports ships.
"""

from __future__ import annotations

import asyncio
import logging
import sys

from temporalio.worker import Worker

from temporal_app.activities.migration_activities import (
    build_field_mapping,
    profile_source_dataset,
    promote_migration_cutover,
    reconcile_migration,
    rollback_migration,
    run_migration_dry_run,
)
from temporal_app.client import connect_temporal_client
from temporal_app.config import (
    MIGRATION_TASK_QUEUE,
    TEMPORAL_HOST,
    TEMPORAL_NAMESPACE,
    WORKER_MAX_CONCURRENT_ACTIVITY_TASKS,
    WORKER_MAX_CONCURRENT_WORKFLOW_TASKS,
    validate_config,
)
from temporal_app.workflows.migration import MigrationWorkflow

TASK_QUEUE = MIGRATION_TASK_QUEUE

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
logger = logging.getLogger(__name__)


async def main() -> None:
    """Start the migration control-plane worker and block until shutdown."""
    errors = validate_config()
    if errors:
        for err in errors:
            logger.critical("CONFIG_ERROR: %s", err)
        sys.exit(1)

    logger.info(
        "migration_worker.starting host=%s namespace=%s task_queue=%s",
        TEMPORAL_HOST,
        TEMPORAL_NAMESPACE,
        TASK_QUEUE,
    )

    # Connects through the shared helper so the encrypting payload codec is
    # applied. Raises before connecting when no payload key is configured.
    client = await connect_temporal_client()

    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[
            MigrationWorkflow,
        ],
        activities=[
            profile_source_dataset,
            build_field_mapping,
            run_migration_dry_run,
            reconcile_migration,
            promote_migration_cutover,
            rollback_migration,
        ],
        max_concurrent_workflow_tasks=WORKER_MAX_CONCURRENT_WORKFLOW_TASKS,
        max_concurrent_activities=WORKER_MAX_CONCURRENT_ACTIVITY_TASKS,
    )

    logger.info("migration_worker.running task_queue=%s", TASK_QUEUE)
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
