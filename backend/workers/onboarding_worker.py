"""Adaptix Temporal onboarding worker entrypoint.

ECS task CMD override:
    python -m workers.onboarding_worker

Environment variables required:
    TEMPORAL_HOST         — Temporal server host:port
    TEMPORAL_NAMESPACE    — Defaults to "adaptix"
    TASK_QUEUE            — Must be "onboarding" (validated at startup)
    ADAPTIX_API_BASE      — Internal API base URL
    ADAPTIX_SERVICE_TOKEN — Bearer token for inter-service authentication

This worker registers:
  Workflows:
    - AgencyOnboardingWorkflow
  Activities:
    - get_onboarding_case
    - advance_onboarding_step
    - complete_onboarding_step
    - provision_tenant
    - run_go_live_readiness_check
    - configure_billing_provider_identity
    - unlock_workspace
    - send_go_live_notification
"""

from __future__ import annotations

import asyncio
import logging
import sys

from temporalio.client import Client
from temporalio.worker import Worker

from temporal_app.activities.onboarding_activities import (
    advance_onboarding_step,
    complete_onboarding_step,
    configure_billing_provider_identity,
    get_onboarding_case,
    provision_tenant,
    run_go_live_readiness_check,
    send_go_live_notification,
    unlock_workspace,
)
from temporal_app.config import (
    TEMPORAL_HOST,
    TEMPORAL_NAMESPACE,
    WORKER_MAX_CONCURRENT_ACTIVITY_TASKS,
    WORKER_MAX_CONCURRENT_WORKFLOW_TASKS,
    validate_config,
)
from temporal_app.workflows.onboarding_workflows import AgencyOnboardingWorkflow

TASK_QUEUE = "onboarding"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
logger = logging.getLogger(__name__)


async def main() -> None:
    """Start the onboarding Temporal worker and block until shutdown signal."""
    errors = validate_config()
    if errors:
        for err in errors:
            logger.critical("CONFIG_ERROR: %s", err)
        sys.exit(1)

    logger.info(
        "onboarding_worker.starting host=%s namespace=%s task_queue=%s",
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
            AgencyOnboardingWorkflow,
        ],
        activities=[
            get_onboarding_case,
            advance_onboarding_step,
            complete_onboarding_step,
            provision_tenant,
            run_go_live_readiness_check,
            configure_billing_provider_identity,
            unlock_workspace,
            send_go_live_notification,
        ],
        max_concurrent_workflow_tasks=WORKER_MAX_CONCURRENT_WORKFLOW_TASKS,
        max_concurrent_activities=WORKER_MAX_CONCURRENT_ACTIVITY_TASKS,
    )

    logger.info("onboarding_worker.running task_queue=%s", TASK_QUEUE)
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
