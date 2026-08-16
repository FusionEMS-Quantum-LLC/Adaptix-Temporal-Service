"""Adaptix Temporal migration BULK worker entrypoint.

ECS task CMD override:
    python -m workers.migration_bulk_worker

Environment variables required:
    TEMPORAL_HOST         — Temporal server host:port
    TEMPORAL_NAMESPACE    — Defaults to "adaptix"
    TEMPORAL_PAYLOAD_CODEC_KEY
                          — AES-256 payload encryption keyring (AWS Secrets
                            Manager via the ECS task definition). Required:
                            the worker refuses to start without it so no
                            plaintext payload can reach workflow history.
    TASK_QUEUE            — Must be "migration-bulk" (validated at startup)
    ADAPTIX_API_BASE      — Internal API base URL
    ADAPTIX_SERVICE_TOKEN — Bearer token for inter-service authentication

WHY THIS IS ITS OWN PROCESS AND ITS OWN QUEUE
---------------------------------------------
Backfilling an agency's history is millions of records and can run for hours.
Temporal hands activity tasks to whichever worker polls the queue, so if bulk
backfill shared the `migration` queue it would occupy every activity slot and
the control plane — pause, mapping decisions, dry runs, reconciliation, cutover
approval, rollback — would queue behind it. Worse, on a shared worker it would
compete with live revenue work.

Separate queue, separate ECS service, separate concurrency budget. Bulk can be
scaled down, throttled, or stopped entirely without touching a cutover approval
or a claim submission. That is the contract requirement that bulk work must
never starve live revenue work, expressed in the only place it can actually be
enforced.

This worker registers:
  Workflows:  none. MigrationWorkflow runs on the `migration` queue and
              dispatches backfill activities here by task queue. A bulk worker
              that ran workflows would defeat the separation.
  Activities:
    - backfill_migration_history

The activity currently raises a non-retryable MigrationActivityNotImplemented —
the Adaptix Imports service that performs the backfill is not built yet.
"""

from __future__ import annotations

import asyncio
import logging
import sys

from temporalio.worker import Worker

from temporal_app.activities.migration_activities import (
    backfill_migration_history,
)
from temporal_app.client import connect_temporal_client
from temporal_app.config import (
    MIGRATION_BULK_TASK_QUEUE,
    TEMPORAL_HOST,
    TEMPORAL_NAMESPACE,
    WORKER_MAX_CONCURRENT_ACTIVITY_TASKS,
    WORKER_MAX_CONCURRENT_WORKFLOW_TASKS,
    validate_config,
)

TASK_QUEUE = MIGRATION_BULK_TASK_QUEUE

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
logger = logging.getLogger(__name__)


async def main() -> None:
    """Start the migration bulk worker and block until shutdown."""
    errors = validate_config()
    if errors:
        for err in errors:
            logger.critical("CONFIG_ERROR: %s", err)
        sys.exit(1)

    logger.info(
        "migration_bulk_worker.starting host=%s namespace=%s task_queue=%s",
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
        workflows=[],
        activities=[
            backfill_migration_history,
        ],
        max_concurrent_workflow_tasks=WORKER_MAX_CONCURRENT_WORKFLOW_TASKS,
        max_concurrent_activities=WORKER_MAX_CONCURRENT_ACTIVITY_TASKS,
    )

    logger.info("migration_bulk_worker.running task_queue=%s", TASK_QUEUE)
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
