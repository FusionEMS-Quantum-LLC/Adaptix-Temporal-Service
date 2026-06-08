"""Document domain Temporal workflows.

Workflows orchestrate multi-step document lifecycle operations:
  - PDF generation
  - TrustSign envelope lifecycle (create → poll → finalize)
  - DocuPost physical mail delivery

TrustSign is Adaptix-native. No external e-signature provider is referenced
anywhere in this module. Attempting to route signatures externally is a
production integrity violation.

Workflows registered here:
  - GeneratePDFWorkflow       — generate and store a PDF document
  - TrustSignWorkflow         — complete a document signing lifecycle
  - DocuPostDeliveryWorkflow  — send a document via physical mail (DocuPost)
"""

from __future__ import annotations

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

_ACTIVITY_TIMEOUT = timedelta(minutes=5)
# TrustSign polling timeout — give signers up to 7 days.
_TRUSTSIGN_POLL_INTERVAL = timedelta(minutes=30)
_TRUSTSIGN_MAX_POLLS = 336  # 7 days at 30-minute intervals


@workflow.defn
class GeneratePDFWorkflow:
    """Generate a PDF document and store it in S3.

    Workflow ID convention: "generate-pdf-{document_type}-{context_hash}"
    Task queue: documents

    Input:
        document_type (str): Template key (e.g. "billing_statement", "cms1500").
        context (dict):      Rendering context (entity IDs only, no raw PHI).
        output_key (str):    S3 key for the output file.

    Result:
        dict: {"document_id": "...", "s3_key": "...", "size_bytes": ...}
    """

    @workflow.run
    async def run(
        self,
        document_type: str,
        context: dict[str, Any],
        output_key: str,
    ) -> dict[str, Any]:
        workflow.logger.info(
            "GeneratePDFWorkflow starting document_type=%s output_key=%s",
            document_type,
            output_key,
        )

        result: dict[str, Any] = await workflow.execute_activity(
            generate_pdf_document,
            args=[document_type, context, output_key],
            start_to_close_timeout=_ACTIVITY_TIMEOUT,
            retry_policy=DEFAULT_RETRY_POLICY,
        )

        workflow.logger.info(
            "GeneratePDFWorkflow complete document_type=%s document_id=%s",
            document_type,
            result.get("document_id"),
        )
        return result


@workflow.defn
class TrustSignWorkflow:
    """Orchestrate the complete TrustSign document signing lifecycle.

    Workflow ID convention: "trustsign-{document_id}"
    Task queue: documents

    Input:
        document_id (str):     Adaptix document UUID of the prepared PDF.
        recipient_email (str): Signer's email address for the SES invitation.
                               Not logged by the workflow.

    Steps:
        1. Initiate TrustSign envelope — generates token, sends SES invitation.
        2. Poll envelope status at 30-minute intervals for up to 7 days.
        3. On signed: finalize envelope (download signed PDF, write audit log).
        4. On declined/expired: workflow completes with final status.

    Result:
        dict: Final envelope status record from the TrustSign service.

    TrustSign is Adaptix-native. This workflow never calls external
    e-signature providers. Any such call is a production integrity violation.
    """

    @workflow.run
    async def run(
        self,
        document_id: str,
        recipient_email: str,
    ) -> dict[str, Any]:
        workflow.logger.info(
            "TrustSignWorkflow starting document_id=%s",
            document_id,
        )

        # Step 1: initiate
        envelope: dict[str, Any] = await workflow.execute_activity(
            initiate_trustsign_envelope,
            args=[document_id, recipient_email],
            start_to_close_timeout=_ACTIVITY_TIMEOUT,
            retry_policy=DEFAULT_RETRY_POLICY,
        )
        envelope_id: str = envelope["envelope_id"]

        workflow.logger.info(
            "TrustSignWorkflow envelope_initiated document_id=%s envelope_id=%s",
            document_id,
            envelope_id,
        )

        # Step 2: poll until terminal status or max polls exceeded
        polls = 0
        status_record: dict[str, Any] = {}
        while polls < _TRUSTSIGN_MAX_POLLS:
            await workflow.sleep(_TRUSTSIGN_POLL_INTERVAL)
            polls += 1

            status_record = await workflow.execute_activity(
                poll_trustsign_status,
                envelope_id,
                start_to_close_timeout=_ACTIVITY_TIMEOUT,
                retry_policy=DEFAULT_RETRY_POLICY,
            )
            current_status = status_record.get("status", "")

            workflow.logger.info(
                "TrustSignWorkflow poll document_id=%s envelope_id=%s "
                "poll=%d status=%s",
                document_id,
                envelope_id,
                polls,
                current_status,
            )

            if current_status == "signed":
                # Step 3: finalize
                finalized: dict[str, Any] = await workflow.execute_activity(
                    finalize_trustsign_envelope,
                    envelope_id,
                    start_to_close_timeout=_ACTIVITY_TIMEOUT,
                    retry_policy=DEFAULT_RETRY_POLICY,
                )
                workflow.logger.info(
                    "TrustSignWorkflow finalized document_id=%s envelope_id=%s "
                    "signed_doc_key=%s",
                    document_id,
                    envelope_id,
                    finalized.get("signed_doc_key"),
                )
                return finalized

            if current_status in ("declined", "expired"):
                workflow.logger.warning(
                    "TrustSignWorkflow terminal_without_signature document_id=%s "
                    "envelope_id=%s status=%s",
                    document_id,
                    envelope_id,
                    current_status,
                )
                return status_record

        # Max polls exceeded — envelope is effectively expired
        workflow.logger.warning(
            "TrustSignWorkflow max_polls_exceeded document_id=%s envelope_id=%s",
            document_id,
            envelope_id,
        )
        return {**status_record, "status": "workflow_timeout"}


@workflow.defn
class DocuPostDeliveryWorkflow:
    """Deliver a billing statement via DocuPost physical mail.

    Workflow ID convention: "docupost-delivery-{statement_id}"
    Task queue: documents

    Input:
        statement_id (str): Adaptix statement UUID.

    Result:
        dict: DocuPost delivery record (postgrid_letter_id key retained for
        backwards compatibility — value is now the DocuPost submission id).
    """

    @workflow.run
    async def run(self, statement_id: str) -> dict[str, Any]:
        workflow.logger.info(
            "DocuPostDeliveryWorkflow starting statement_id=%s", statement_id
        )

        result: dict[str, Any] = await workflow.execute_activity(
            send_statement_via_postgrid,
            statement_id,
            start_to_close_timeout=_ACTIVITY_TIMEOUT,
            retry_policy=DEFAULT_RETRY_POLICY,
        )

        workflow.logger.info(
            "DocuPostDeliveryWorkflow complete statement_id=%s "
            "submission_id=%s",
            statement_id,
            result.get("postgrid_letter_id"),
        )
        return result


# Alias so any code still referencing PostGridDeliveryWorkflow continues to work.
PostGridDeliveryWorkflow = DocuPostDeliveryWorkflow
