"""Document domain Temporal workflow definitions.

Workflows:
  GeneratePDFWorkflow   — generates a PDF document via the Documents Service.
  TrustSignWorkflow     — manages the full TrustSign signing lifecycle.
  PostGridMailWorkflow  — submits a patient statement for physical mail.

TrustSign is Adaptix-native. No external e-signature provider is called,
referenced, or fallback-enabled. Any change to this workflow that introduces
an external signing provider is a production integrity violation.

TrustSignWorkflow implements a polling loop to wait for the signer to act.
The maximum wait time is controlled by TRUSTSIGN_SIGNING_DEADLINE_HOURS
(default 72 hours). After the deadline the workflow fails with a clear error
indicating the envelope expired, which the caller can handle by re-sending
the invitation.
"""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import Any

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from temporal_app.activities.document_activities import (
        finalize_trustsign_envelope,
        generate_pdf_document,
        initiate_trustsign_envelope,
        poll_trustsign_status,
        send_statement_via_postgrid,
    )
    from temporal_app.config import DEFAULT_RETRY_POLICY

logger = logging.getLogger(__name__)

# Maximum number of poll iterations before the workflow abandons waiting
# for a signature. Each iteration sleeps for POLL_INTERVAL_MINUTES.
_TRUSTSIGN_MAX_POLL_ATTEMPTS: int = 144  # 144 * 30min = 72 hours
_TRUSTSIGN_POLL_INTERVAL_MINUTES: int = 30


@workflow.defn
class GeneratePDFWorkflow:
    """Generate a PDF document via the Documents Service and store it in S3.

    Input:
      document_type: str  — template key (e.g. "billing_statement", "cms1500")
      context:       dict — rendering context (entity IDs, not raw PHI)
      output_key:    str  — S3 key for the generated PDF

    Output: dict — {"document_id": "...", "s3_key": "...", "size_bytes": ...}

    Idempotency: output_key should be deterministic (e.g. include the
    entity ID and date) so a retry of this workflow overwrites the same
    S3 object rather than creating a new one.
    """

    @workflow.run
    async def run(
        self,
        document_type: str,
        context: dict[str, Any],
        output_key: str,
    ) -> dict:
        workflow.logger.info(
            "GeneratePDFWorkflow started document_type=%s output_key=%s",
            document_type,
            output_key,
        )

        result = await workflow.execute_activity(
            generate_pdf_document,
            args=[document_type, context, output_key],
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=DEFAULT_RETRY_POLICY,
        )

        workflow.logger.info(
            "GeneratePDFWorkflow completed document_type=%s document_id=%s",
            document_type,
            result.get("document_id"),
        )
        return result


@workflow.defn
class TrustSignWorkflow:
    """Manage the full TrustSign signing lifecycle for a document.

    Input:
      document_id:      str — Adaptix document UUID
      recipient_email:  str — signer email address

    Output: dict — final envelope state record.

    Lifecycle:
      1. Initiate the TrustSign envelope and send the signing invitation.
      2. Poll envelope status on a 30-minute interval for up to 72 hours.
      3. On terminal status:
         - "signed":   finalize the envelope (store signed PDF, audit, notify).
         - "declined": fail the workflow with a clear error.
         - "expired":  fail the workflow with a clear error.
      4. Return the final envelope state.

    On failure the caller is responsible for:
      - Notifying the operator or re-sending the invitation.
      - The original document remains in S3; only the signing record changes.

    TrustSign is Adaptix-native. No external signing provider is involved.
    """

    @workflow.run
    async def run(self, document_id: str, recipient_email: str) -> dict:
        workflow.logger.info("TrustSignWorkflow started document_id=%s", document_id)

        # Step 1: Create envelope and send invitation.
        envelope = await workflow.execute_activity(
            initiate_trustsign_envelope,
            args=[document_id, recipient_email],
            start_to_close_timeout=timedelta(minutes=3),
            retry_policy=DEFAULT_RETRY_POLICY,
        )

        envelope_id: str = envelope.get("envelope_id", "")
        workflow.logger.info(
            "TrustSignWorkflow envelope_created document_id=%s envelope_id=%s",
            document_id,
            envelope_id,
        )

        # Step 2: Poll for signature completion.
        for attempt in range(_TRUSTSIGN_MAX_POLL_ATTEMPTS):
            # Wait before polling (except on first iteration to check immediately
            # in case the signer already completed before we start polling).
            if attempt > 0:
                await workflow.sleep(
                    timedelta(minutes=_TRUSTSIGN_POLL_INTERVAL_MINUTES)
                )

            status_record = await workflow.execute_activity(
                poll_trustsign_status,
                envelope_id,
                start_to_close_timeout=timedelta(minutes=2),
                retry_policy=DEFAULT_RETRY_POLICY,
            )

            current_status: str = status_record.get("status", "pending")
            workflow.logger.info(
                "TrustSignWorkflow poll attempt=%d envelope_id=%s status=%s",
                attempt + 1,
                envelope_id,
                current_status,
            )

            if current_status == "signed":
                # Step 3a: Finalize the signed envelope.
                final_result = await workflow.execute_activity(
                    finalize_trustsign_envelope,
                    envelope_id,
                    start_to_close_timeout=timedelta(minutes=5),
                    retry_policy=DEFAULT_RETRY_POLICY,
                )
                workflow.logger.info(
                    "TrustSignWorkflow completed envelope_id=%s signed",
                    envelope_id,
                )
                return final_result

            if current_status == "declined":
                raise RuntimeError(
                    f"TrustSignWorkflow: envelope {envelope_id} was declined by the signer. "
                    "Operator must review and re-send if appropriate."
                )

            if current_status == "expired":
                raise RuntimeError(
                    f"TrustSignWorkflow: envelope {envelope_id} expired before the signer acted. "
                    "Re-send the invitation via TrustSignWorkflow with the same document_id."
                )

            # Continue polling for pending status.

        # Max poll attempts reached without a terminal status.
        raise RuntimeError(
            f"TrustSignWorkflow: envelope {envelope_id} did not reach a terminal status "
            f"within {_TRUSTSIGN_MAX_POLL_ATTEMPTS * _TRUSTSIGN_POLL_INTERVAL_MINUTES} minutes. "
            "The envelope may have an extended signing window. Check the TrustSign admin panel."
        )


@workflow.defn
class PostGridMailWorkflow:
    """Submit a patient statement for physical mail delivery via PostGrid.

    Input:  statement_id (str) — Adaptix statement UUID
    Output: dict — PostGrid delivery record.

    The Billing Service handles PostGrid API interaction, letter generation
    from the statement PDF, and persistence of the delivery tracking record.
    This workflow is a thin orchestrator that ensures the activity is called
    with the correct retry policy.

    Idempotency: The Billing Service is responsible for deduplication at
    the PostGrid level. Re-running this workflow with the same statement_id
    is safe — the Billing Service checks for an existing PostGrid letter
    record before submitting a new request.
    """

    @workflow.run
    async def run(self, statement_id: str) -> dict:
        workflow.logger.info(
            "PostGridMailWorkflow started statement_id=%s", statement_id
        )

        result = await workflow.execute_activity(
            send_statement_via_postgrid,
            statement_id,
            start_to_close_timeout=timedelta(minutes=3),
            retry_policy=DEFAULT_RETRY_POLICY,
        )

        workflow.logger.info(
            "PostGridMailWorkflow completed statement_id=%s postgrid_letter_id=%s",
            statement_id,
            result.get("postgrid_letter_id"),
        )
        return result
