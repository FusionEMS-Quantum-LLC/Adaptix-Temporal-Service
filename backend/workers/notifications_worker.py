"""Adaptix Temporal notifications worker entrypoint.

ECS task CMD override:
    python -m workers.notifications_worker

Environment variables required:
    TEMPORAL_HOST         — Temporal server host:port
    TEMPORAL_NAMESPACE    — Defaults to "adaptix"
    TASK_QUEUE            — Must be "notifications" (validated at startup)
    ADAPTIX_API_BASE      — Internal API base URL
    ADAPTIX_SERVICE_TOKEN — Bearer token for inter-service authentication

This worker registers:
  Workflows:
    - SendBatchStatementsWorkflow
  Activities:
    - send_email_notification
    - send_sms_notification
    - list_agency_statement_recipients
    - send_statement_email
    - queue_statement_for_mail

Platform policy enforced: SMS activities are only permitted for billing AR
notification categories. The activity itself enforces the allowlist.
"""

from __future__ import annotations

import asyncio
import logging
import sys

from temporalio.client import Client
from temporalio.worker import Worker

from temporal_app.activities.notification_activities import (
    list_agency_statement_recipients,
    queue_statement_for_mail,
    send_email_notification,
    send_sms_notification,
    send_statement_email,
)
from temporal_app.config import (
    TEMPORAL_HOST,
    TEMPORAL_NAMESPACE,
    WORKER_MAX_CONCURRENT_ACTIVITY_TASKS,
    WORKER_MAX_CONCURRENT_WORKFLOW_TASKS,
    validate_config,
)
from temporal_app.workflows.notification_workflows import SendBatchStatementsWorkflow

TASK_QUEUE = "notifications"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
logger = logging.getLogger(__name__)


async def main() -> None:
    """Start the notifications Temporal worker and block until shutdown signal."""
    errors = validate_config()
    if errors:
        for err in errors:
            logger.critical("CONFIG_ERROR: %s", err)
        sys.exit(1)

    logger.info(
        "notifications_worker.starting host=%s namespace=%s task_queue=%s",
        TEMPORAL_HOST,
        TEMPORAL_NAMESPACE,
        TASK_QUEUE,
    )

    client = await Client.connect(
        TEMPORAL_HOST,
        namespace=TEMPORAL_NAMESPACE,
    )

    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[
            SendBatchStatementsWorkflow,
        ],
        activities=[
            send_email_notification,
            send_sms_notification,
            list_agency_statement_recipients,
            send_statement_email,
            queue_statement_for_mail,
        ],
        max_concurrent_workflow_tasks=WORKER_MAX_CONCURRENT_WORKFLOW_TASKS,
        max_concurrent_activities=WORKER_MAX_CONCURRENT_ACTIVITY_TASKS,
    )

    logger.info("notifications_worker.running task_queue=%s", TASK_QUEUE)
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
