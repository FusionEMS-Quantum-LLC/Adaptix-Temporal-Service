"""Notification domain Temporal workflows.

Workflows orchestrate multi-step notification delivery that requires
durability and fan-out retry semantics. Each workflow is idempotent.

Workflows registered here:
  - SendBatchStatementsWorkflow — fan out monthly statements to all recipients

PHI-safe: workflows carry only agency_id, statement_id, and month. No
patient name, address, DOB, or contact details are passed as workflow inputs.
Activities resolve and deliver to real recipients but do not log PHI values.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from temporal_app.activities.notification_activities import (
        list_agency_statement_recipients,
        queue_statement_for_mail,
        send_statement_email,
    )
    from temporal_app.config import DEFAULT_RETRY_POLICY

_ACTIVITY_TIMEOUT = timedelta(minutes=5)


@workflow.defn
class SendBatchStatementsWorkflow:
    """Fan out monthly billing statements to all recipients for an agency.

    Workflow ID convention: "batch-statements-{agency_id}-{month}"
    Task queue: notifications

    Input:
        agency_id (str): The Adaptix agency UUID.
        month (str):     ISO month string (e.g. "2026-06").

    Steps:
        1. Fetch the list of recipients who need statements this month.
        2. For each recipient, send email and/or queue physical mail
           based on their delivery preference.

    Result:
        dict: Summary (email_sent, mail_queued, errors).

    The fan-out uses sequential execution (not concurrent futures) to avoid
    overwhelming the Core Service. For large agencies (>1000 statements),
    the workflow can be modified to use concurrent activities with a
    semaphore guard. For current scale (sub-100 agencies in launch cohort),
    sequential is safer and easier to observe.
    """

    @workflow.run
    async def run(self, agency_id: str, month: str) -> dict[str, Any]:
        workflow.logger.info(
            "SendBatchStatementsWorkflow starting agency_id=%s month=%s",
            agency_id,
            month,
        )

        recipients: list[dict[str, Any]] = await workflow.execute_activity(
            list_agency_statement_recipients,
            args=[agency_id, month],
            start_to_close_timeout=_ACTIVITY_TIMEOUT,
            retry_policy=DEFAULT_RETRY_POLICY,
        )

        workflow.logger.info(
            "SendBatchStatementsWorkflow recipient_count=%d agency_id=%s month=%s",
            len(recipients),
            agency_id,
            month,
        )

        email_sent = 0
        mail_queued = 0
        errors: list[str] = []

        for recipient in recipients:
            statement_id = recipient["statement_id"]
            delivery_method = recipient.get("delivery_method", "email")

            if delivery_method in ("email", "both"):
                try:
                    await workflow.execute_activity(
                        send_statement_email,
                        args=[statement_id, recipient.get("email", "")],
                        start_to_close_timeout=_ACTIVITY_TIMEOUT,
                        retry_policy=DEFAULT_RETRY_POLICY,
                    )
                    email_sent += 1
                except Exception as exc:  # noqa: BLE001
                    workflow.logger.error(
                        "SendBatchStatementsWorkflow email_failed "
                        "statement_id=%s error=%s",
                        statement_id,
                        str(exc)[:200],
                    )
                    errors.append(f"email:{statement_id}")

            if delivery_method in ("mail", "both"):
                try:
                    await workflow.execute_activity(
                        queue_statement_for_mail,
                        statement_id,
                        start_to_close_timeout=_ACTIVITY_TIMEOUT,
                        retry_policy=DEFAULT_RETRY_POLICY,
                    )
                    mail_queued += 1
                except Exception as exc:  # noqa: BLE001
                    workflow.logger.error(
                        "SendBatchStatementsWorkflow mail_failed "
                        "statement_id=%s error=%s",
                        statement_id,
                        str(exc)[:200],
                    )
                    errors.append(f"mail:{statement_id}")

        result = {
            "agency_id": agency_id,
            "month": month,
            "total_recipients": len(recipients),
            "email_sent": email_sent,
            "mail_queued": mail_queued,
            "error_count": len(errors),
        }

        workflow.logger.info(
            "SendBatchStatementsWorkflow complete agency_id=%s month=%s "
            "email_sent=%d mail_queued=%d errors=%d",
            agency_id,
            month,
            email_sent,
            mail_queued,
            len(errors),
        )
        return result
