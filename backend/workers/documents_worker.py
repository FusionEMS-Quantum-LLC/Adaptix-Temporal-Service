"""Adaptix Temporal documents worker entrypoint.

ECS task CMD override:
    python -m workers.documents_worker

Environment variables required:
    TEMPORAL_HOST         — Temporal server host:port
    TEMPORAL_NAMESPACE    — Defaults to "adaptix"
    TASK_QUEUE            — Must be "documents" (validated at startup)
    ADAPTIX_API_BASE      — Internal API base URL
    ADAPTIX_SERVICE_TOKEN — Bearer token for inter-service authentication

This worker registers:
  Workflows:
    - GeneratePDFWorkflow
    - TrustSignWorkflow
    - PostGridDeliveryWorkflow
  Activities:
    - generate_pdf_document
    - initiate_trustsign_envelope
    - poll_trustsign_status
    - finalize_trustsign_envelope
    - send_statement_via_postgrid

TrustSign is Adaptix-native. No external e-signature provider is registered
or called by this worker. Any attempt to route signatures externally is a
production integrity violation.
"""

from __future__ import annotations

import asyncio
import logging
import sys

from temporalio.client import Client
from temporalio.worker import Worker

from temporal_app.activities.document_activities import (
    finalize_trustsign_envelope,
    generate_pdf_document,
    initiate_trustsign_envelope,
    poll_trustsign_status,
    send_statement_via_postgrid,
)
from temporal_app.config import (
    TEMPORAL_HOST,
    TEMPORAL_NAMESPACE,
    WORKER_MAX_CONCURRENT_ACTIVITY_TASKS,
    WORKER_MAX_CONCURRENT_WORKFLOW_TASKS,
    validate_config,
)
from temporal_app.workflows.document_workflows import (
    GeneratePDFWorkflow,
    PostGridDeliveryWorkflow,
    TrustSignWorkflow,
)

TASK_QUEUE = "documents"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
logger = logging.getLogger(__name__)


async def main() -> None:
    """Start the documents Temporal worker and block until shutdown signal."""
    errors = validate_config()
    if errors:
        for err in errors:
            logger.critical("CONFIG_ERROR: %s", err)
        sys.exit(1)

    logger.info(
        "documents_worker.starting host=%s namespace=%s task_queue=%s",
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
            GeneratePDFWorkflow,
            TrustSignWorkflow,
            PostGridDeliveryWorkflow,
        ],
        activities=[
            generate_pdf_document,
            initiate_trustsign_envelope,
            poll_trustsign_status,
            finalize_trustsign_envelope,
            send_statement_via_postgrid,
        ],
        max_concurrent_workflow_tasks=WORKER_MAX_CONCURRENT_WORKFLOW_TASKS,
        max_concurrent_activities=WORKER_MAX_CONCURRENT_ACTIVITY_TASKS,
    )

    logger.info("documents_worker.running task_queue=%s", TASK_QUEUE)
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
