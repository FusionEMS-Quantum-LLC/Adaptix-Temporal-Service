"""Notification domain Temporal workflow definitions.

Workflows:
  SendEmailWorkflow           — sends a single transactional email.
  SendSMSWorkflow             — sends a billing AR SMS (billing AR only).
  SendBatchStatementsWorkflow — delivers monthly statements for all agency patients.

SMS policy:
  SendSMSWorkflow enforces the Adaptix platform rule: SMS is ONLY for billing
  AR notifications. The notification_category parameter must be one of the
  allowed billing categories. This is enforced in the activity layer and the
  workflow will fail with a non-retryable ValidationError if violated.

Batch statement delivery:
  SendBatchStatementsWorkflow pages through all recipients for an agency+month,
  dispatching email and/or mail per the patient's delivery preference.
  Each individual dispatch is a separate activity call so that a single
  delivery failure does not abort the entire batch.
"""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from temporal_app.activities.notification_activities import (
        list_agency_statement_recipients,
        queue_statement_for_mail,
        send_email_notification,
        send_sms_notification,
        send_statement_email,
    )
    from temporal_app.config import DEFAULT_RETRY_POLICY

logger = logging.getLogger(__name__)


@workflow.defn
class SendEmailWorkflow:
    """Send a single transactional email via SES.

    Input:
      to:       str  — recipient address
      subject:  str  — email subject
      template: str  — Core Service template key
      context:  dict — template rendering context (no raw PHI)

    Output: dict — delivery record from Core Service.

    Idempotency: email sends are not idempotent at the SES level (a second
    call will send a second email). Callers must ensure the workflow ID is
    unique per intended send event — use a deterministic ID such as
    "email-{template}-{target_entity_id}-{date}" to prevent duplicate sends
    from workflow retries.
    """

    @workflow.run
    async def run(
        self,
        to: str,
        subject: str,
        template: str,
        context: dict[str, Any],
    ) -> dict:
        workflow.logger.info(
            "SendEmailWorkflow started template=%s", template
        )

        result = await workflow.execute_activity(
            send_email_notification,
            args=[to, subject, template, context],
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=DEFAULT_RETRY_POLICY,
        )

        workflow.logger.info(
            "SendEmailWorkflow completed template=%s delivery_id=%s",
            template,
            result.get("delivery_id"),
        )
        return result


@workflow.defn
class SendSMSWorkflow:
    """Send a billing AR SMS notification via Telnyx.

    Input:
      to:                    str — recipient phone number (E.164 format)
      message:               str — SMS body text
      notification_category: str — must be a billing AR category

    Allowed notification_category values (enforced at activity layer):
      billing_statement_reminder
      billing_payment_due
      billing_plan_installment
      billing_late_notice

    Any other category will cause a non-retryable ValidationError.
    The workflow will fail immediately without retrying.
    """

    @workflow.run
    async def run(
        self,
        to: str,
        message: str,
        notification_category: str,
    ) -> dict:
        workflow.logger.info(
            "SendSMSWorkflow started category=%s", notification_category
        )

        result = await workflow.execute_activity(
            send_sms_notification,
            args=[to, message, notification_category],
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=DEFAULT_RETRY_POLICY,
        )

        workflow.logger.info(
            "SendSMSWorkflow completed category=%s",
            notification_category,
        )
        return result


@workflow.defn
class SendBatchStatementsWorkflow:
    """Deliver monthly billing statements for all patients in an agency.

    Input:
      agency_id: str — agency (tenant) UUID
      month:     str — ISO month string, e.g. "2026-06"

    Output: dict — summary of deliveries attempted and results.

    Execution model:
      1. Fetch the full recipient list from the Billing Service.
      2. For each recipient, dispatch statement(s) based on delivery preference:
         - email: send via SES
         - mail:  queue via DocuPost
         - both:  send via SES AND queue via DocuPost
      3. Collect results; failures are recorded but do not abort the batch.

    Failure handling:
      Individual delivery failures (e.g. a single bad email address) are
      caught and recorded in the results summary. The workflow continues
      processing remaining recipients. The caller can inspect "failed" in
      the result to identify failed deliveries for manual follow-up.

    Idempotency:
      Re-running this workflow for the same agency_id + month will re-attempt
      delivery for all recipients. The Billing Service statement endpoint is
      idempotent on statement_id so re-running does not generate duplicate
      DocuPost letters (DocuPost deduplication is enforced by the Billing
      Service, not here).
    """

    @workflow.run
    async def run(self, agency_id: str, month: str) -> dict:
        workflow.logger.info(
            "SendBatchStatementsWorkflow started agency_id=%s month=%s",
            agency_id,
            month,
        )

        # Step 1: Get recipient list.
        recipients: list[dict[str, Any]] = await workflow.execute_activity(
            list_agency_statement_recipients,
            args=[agency_id, month],
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=DEFAULT_RETRY_POLICY,
        )

        workflow.logger.info(
            "SendBatchStatementsWorkflow agency_id=%s month=%s recipients=%d",
            agency_id,
            month,
            len(recipients),
        )

        # Step 2: Dispatch per recipient.
        results: dict[str, Any] = {
            "agency_id": agency_id,
            "month": month,
            "total": len(recipients),
            "emailed": 0,
            "mailed": 0,
            "failed": [],
        }

        for recipient in recipients:
            statement_id: str = recipient.get("statement_id", "")
            delivery_method: str = recipient.get("delivery_method", "email")
            email: str | None = recipient.get("email")
            # mailing_address is resolved server-side by the Billing/Documents
            # Service — we do not handle it in the workflow.

            # Email delivery.
            if delivery_method in ("email", "both") and email:
                try:
                    await workflow.execute_activity(
                        send_statement_email,
                        args=[statement_id, email],
                        start_to_close_timeout=timedelta(minutes=2),
                        retry_policy=RetryPolicy(
                            initial_interval=timedelta(seconds=2),
                            maximum_attempts=3,
                            non_retryable_error_types=[
                                "ValidationError",
                                "AuthorizationError",
                            ],
                        ),
                    )
                    results["emailed"] += 1
                except Exception as exc:
                    workflow.logger.error(
                        "SendBatchStatementsWorkflow email_failed "
                        "statement_id=%s error=%s",
                        statement_id,
                        type(exc).__name__,
                    )
                    results["failed"].append(
                        {
                            "statement_id": statement_id,
                            "method": "email",
                            "error": type(exc).__name__,
                        }
                    )

            # Physical mail delivery.
            if delivery_method in ("mail", "both"):
                try:
                    await workflow.execute_activity(
                        queue_statement_for_mail,
                        statement_id,
                        start_to_close_timeout=timedelta(minutes=2),
                        retry_policy=RetryPolicy(
                            initial_interval=timedelta(seconds=2),
                            maximum_attempts=3,
                            non_retryable_error_types=[
                                "ValidationError",
                                "AuthorizationError",
                            ],
                        ),
                    )
                    results["mailed"] += 1
                except Exception as exc:
                    workflow.logger.error(
                        "SendBatchStatementsWorkflow mail_failed "
                        "statement_id=%s error=%s",
                        statement_id,
                        type(exc).__name__,
                    )
                    results["failed"].append(
                        {
                            "statement_id": statement_id,
                            "method": "mail",
                            "error": type(exc).__name__,
                        }
                    )

        workflow.logger.info(
            "SendBatchStatementsWorkflow completed agency_id=%s month=%s "
            "emailed=%d mailed=%d failed=%d",
            agency_id,
            month,
            results["emailed"],
            results["mailed"],
            len(results["failed"]),
        )
        return results
