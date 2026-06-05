"""Adaptix Temporal worker entrypoint.

Reads TASK_QUEUE from the environment to determine which domain this worker
instance handles. Each ECS task definition runs a single domain worker.

Domain routing:
  TASK_QUEUE=billing       -- billing workflows + billing activities
  TASK_QUEUE=notifications -- notifications workflows + notification activities
  TASK_QUEUE=documents     -- document workflows + document activities
  TASK_QUEUE=onboarding    -- onboarding workflows + onboarding activities

Startup:
  1. Validate required configuration (TEMPORAL_HOST, TASK_QUEUE,
     ADAPTIX_API_BASE, ADAPTIX_SERVICE_TOKEN). Exit 1 if any are missing.
  2. Connect to Temporal server at TEMPORAL_HOST, namespace TEMPORAL_NAMESPACE.
  3. Construct Worker with the domain-appropriate workflow and activity lists.
  4. Run the worker and block until SIGINT/SIGTERM.

Error handling:
  Connection failures to the Temporal server are retried by the temporalio
  client with exponential backoff. If the server is unreachable at startup
  after the connect call, the process exits with a non-zero code so that
  ECS marks the task unhealthy and triggers a replacement.

Logging:
  Structured JSON logging is configured at startup. The LOG_LEVEL environment
  variable controls verbosity (default INFO). Sensitive values (tokens,
  database URLs) are never logged.
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys

from temporalio.client import Client
from temporalio.worker import Worker

from temporal_app.config import (
    TASK_QUEUE,
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
from temporal_app.workflows.document_workflows import (
    GeneratePDFWorkflow,
    PostGridDeliveryWorkflow,
    TrustSignWorkflow,
)
from temporal_app.workflows.notification_workflows import (
    SendBatchStatementsWorkflow,
)
from temporal_app.workflows.onboarding_workflows import (
    AgencyOnboardingWorkflow,
)
from temporal_app.activities.billing_activities import (
    create_denial_appeal,
    get_claim_status,
    process_era_file,
    resubmit_denied_claim,
    run_monthly_agency_invoicing,
    submit_claim_to_clearinghouse,
)
from temporal_app.activities.notification_activities import (
    list_agency_statement_recipients,
    queue_statement_for_mail,
    send_email_notification,
    send_sms_notification,
    send_statement_email,
)
from temporal_app.activities.document_activities import (
    finalize_trustsign_envelope,
    generate_pdf_document,
    initiate_trustsign_envelope,
    poll_trustsign_status,
    send_statement_via_postgrid,
)
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


def _configure_logging() -> None:
    """Configure structured logging.

    JSON-formatted output at the configured log level. Uses the root logger
    so all temporal SDK logs are captured.
    """
    log_level_str = os.environ.get("LOG_LEVEL", "INFO").upper()
    log_level = getattr(logging, log_level_str, logging.INFO)

    # Minimal structured format compatible with AWS CloudWatch Logs Insights.
    logging.basicConfig(
        level=log_level,
        format='{"timestamp": "%(asctime)s", "level": "%(levelname)s", '
               '"logger": "%(name)s", "message": "%(message)s"}',
        datefmt="%Y-%m-%dT%H:%M:%S",
        stream=sys.stdout,
    )


def _build_worker(client: Client, task_queue: str) -> Worker:
    """Construct a Worker for the specified task queue.

    Each task queue gets the exact set of workflows and activities it needs --
    no domain bleeds into another worker's responsibility.

    Raises ValueError if task_queue is not a recognised domain value.
    """
    if task_queue == "billing":
        return Worker(
            client,
            task_queue=task_queue,
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

    if task_queue == "notifications":
        return Worker(
            client,
            task_queue=task_queue,
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

    if task_queue == "documents":
        return Worker(
            client,
            task_queue=task_queue,
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

    if task_queue == "onboarding":
        return Worker(
            client,
            task_queue=task_queue,
            workflows=[
                AgencyOnboardingWorkflow,
            ],
            activities=[
                get_onboarding_case,
                advance_onboarding_step,
                complete_onboarding_step,
                provision_tenant,
                run_go_live_readiness_check,
                unlock_workspace,
                configure_billing_provider_identity,
                send_go_live_notification,
            ],
            max_concurrent_workflow_tasks=WORKER_MAX_CONCURRENT_WORKFLOW_TASKS,
            max_concurrent_activities=WORKER_MAX_CONCURRENT_ACTIVITY_TASKS,
        )

    raise ValueError(
        f"Unrecognised TASK_QUEUE value '{task_queue}'. "
        "Valid values: billing | notifications | documents | onboarding"
    )


async def main() -> None:
    """Start the Temporal worker for the configured domain."""
    _configure_logging()
    logger = logging.getLogger(__name__)

    # Validate configuration before attempting connection.
    config_errors = validate_config()
    if config_errors:
        for error in config_errors:
            logger.error("temporal_worker.config_error error=%s", error)
        logger.error(
            "temporal_worker.startup_failed reason=configuration_invalid "
            "error_count=%d",
            len(config_errors),
        )
        sys.exit(1)

    logger.info(
        "temporal_worker.connecting host=%s namespace=%s task_queue=%s",
        TEMPORAL_HOST,
        TEMPORAL_NAMESPACE,
        TASK_QUEUE,
    )

    try:
        client = await Client.connect(
            TEMPORAL_HOST,
            namespace=TEMPORAL_NAMESPACE,
        )
    except Exception as exc:
        logger.error(
            "temporal_worker.connect_failed host=%s error=%s",
            TEMPORAL_HOST,
            type(exc).__name__,
        )
        sys.exit(1)

    logger.info("temporal_worker.connected task_queue=%s", TASK_QUEUE)

    try:
        worker = _build_worker(client, TASK_QUEUE)
    except ValueError as exc:
        logger.error("temporal_worker.build_failed error=%s", exc)
        sys.exit(1)

    # Graceful shutdown on SIGTERM (ECS stop task).
    shutdown_event = asyncio.Event()

    def _handle_signal() -> None:
        logger.info(
            "temporal_worker.shutdown_signal_received task_queue=%s", TASK_QUEUE
        )
        shutdown_event.set()

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _handle_signal)

    logger.info("temporal_worker.running task_queue=%s", TASK_QUEUE)

    # Run the worker until shutdown signal.
    worker_task = asyncio.ensure_future(worker.run())
    await shutdown_event.wait()

    logger.info("temporal_worker.stopping task_queue=%s", TASK_QUEUE)
    worker_task.cancel()
    try:
        await worker_task
    except asyncio.CancelledError:
        pass

    logger.info("temporal_worker.stopped task_queue=%s", TASK_QUEUE)


if __name__ == "__main__":
    asyncio.run(main())
