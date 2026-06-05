"""Adaptix Temporal billing worker entrypoint.

ECS task CMD override:
    python -m workers.billing_worker

Environment variables required:
    TEMPORAL_HOST         — Temporal server host:port (e.g. host.internal:7233)
    TEMPORAL_NAMESPACE    — Defaults to "adaptix"
    TASK_QUEUE            — Must be "billing" (validated at startup)
    ADAPTIX_API_BASE      — Internal API base URL
    ADAPTIX_SERVICE_TOKEN — Bearer token for inter-service authentication
    DATABASE_URL          — Not used directly but validated as present
    AWS_DEFAULT_REGION    — AWS region for boto3

This worker registers:
  Workflows:
    - ClaimSubmissionWorkflow
    - DenialResubmissionWorkflow
    - ERAPostingWorkflow
    - MonthlyAgencyInvoicingWorkflow
  Activities:
    - submit_claim_to_clearinghouse
    - get_claim_status
    - create_denial_appeal
    - resubmit_denied_claim
    - process_era_file
    - run_monthly_agency_invoicing
"""

from __future__ import annotations

import asyncio
import logging
import sys

from temporalio.client import Client
from temporalio.worker import Worker

from temporal_app.activities.billing_activities import (
    create_denial_appeal,
    get_claim_status,
    process_era_file,
    resubmit_denied_claim,
    run_monthly_agency_invoicing,
    submit_claim_to_clearinghouse,
)
from temporal_app.config import (
    TEMPORAL_HOST,
    TEMPORAL_NAMESPACE,
    WORKER_MAX_CONCURRENT_ACTIVITY_TASKS,
    WORKER_MAX_CONCURRENT_WORKFLOW_TASKS,
    validate_config,
)
from temporal_app.workflows.billing_workflows import (
    ClaimSubmissionWorkflow,
    DenialResubmissionWorkflow,
    ERAPostingWorkflow,
    MonthlyAgencyInvoicingWorkflow,
)

TASK_QUEUE = "billing"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
logger = logging.getLogger(__name__)


async def main() -> None:
    """Start the billing Temporal worker and block until shutdown signal."""
    errors = validate_config()
    if errors:
        for err in errors:
            logger.critical("CONFIG_ERROR: %s", err)
        sys.exit(1)

    logger.info(
        "billing_worker.starting host=%s namespace=%s task_queue=%s",
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
            ClaimSubmissionWorkflow,
            DenialResubmissionWorkflow,
            ERAPostingWorkflow,
            MonthlyAgencyInvoicingWorkflow,
        ],
        activities=[
            submit_claim_to_clearinghouse,
            get_claim_status,
            create_denial_appeal,
            resubmit_denied_claim,
            process_era_file,
            run_monthly_agency_invoicing,
        ],
        max_concurrent_workflow_tasks=WORKER_MAX_CONCURRENT_WORKFLOW_TASKS,
        max_concurrent_activities=WORKER_MAX_CONCURRENT_ACTIVITY_TASKS,
    )

    logger.info("billing_worker.running task_queue=%s", TASK_QUEUE)
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
