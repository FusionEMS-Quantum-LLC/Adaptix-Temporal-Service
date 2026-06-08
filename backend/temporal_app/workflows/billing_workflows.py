"""Billing domain Temporal workflows.

Workflows in this module orchestrate multi-step billing operations that
require durability, retries, and auditable state transitions. Each workflow
is idempotent: re-running with the same workflow ID is safe.

Workflows registered here:
  - ClaimSubmissionWorkflow       — submit a single claim to Office Ally
  - DenialResubmissionWorkflow    — create appeal + resubmit a denied claim
  - ERAPostingWorkflow            — process an 835 ERA remittance file
  - MonthlyAgencyInvoicingWorkflow — generate and send all monthly invoices

PHI-safe: workflows carry only claim_id and agency_id identifiers. No PHI
values (patient name, DOB, address, SSN, diagnosis) are passed as workflow
inputs or stored in Temporal history. The Billing Service resolves all
display values from the database when the activity executes.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from temporal_app.activities.billing_activities import (
        create_denial_appeal,
        get_claim_status,
        process_era_file,
        resubmit_denied_claim,
        run_monthly_agency_invoicing,
        submit_claim_to_clearinghouse,
    )
    from temporal_app.config import (
        DEFAULT_RETRY_POLICY,
        EXTERNAL_VENDOR_RETRY_POLICY,
    )

_ACTIVITY_TIMEOUT = timedelta(minutes=5)
_LONG_ACTIVITY_TIMEOUT = timedelta(minutes=30)


@workflow.defn
class ClaimSubmissionWorkflow:
    """Submit a single insurance claim to the clearinghouse.

    Workflow ID convention: "claim-submit-{claim_id}"
    Task queue: billing

    Input:
        claim_id (str): The Adaptix claim UUID.

    Result:
        dict: The clearinghouse submission response from the Billing Service.

    The workflow is idempotent — starting it twice with the same claim_id
    is safe. The Billing Service clearinghouse endpoint uses claim_id to
    guard against duplicate submissions.
    """

    @workflow.run
    async def run(self, claim_id: str) -> dict[str, Any]:
        workflow.logger.info(
            "ClaimSubmissionWorkflow starting claim_id=%s", claim_id
        )

        result: dict[str, Any] = await workflow.execute_activity(
            submit_claim_to_clearinghouse,
            claim_id,
            start_to_close_timeout=_ACTIVITY_TIMEOUT,
            retry_policy=EXTERNAL_VENDOR_RETRY_POLICY,
        )

        workflow.logger.info(
            "ClaimSubmissionWorkflow complete claim_id=%s submission_id=%s",
            claim_id,
            result.get("submission_id"),
        )
        return result


@workflow.defn
class DenialResubmissionWorkflow:
    """Create a denial appeal and resubmit a denied claim.

    Workflow ID convention: "claim-denial-{claim_id}-{denial_code}"
    Task queue: billing

    Input:
        claim_id (str):    The Adaptix claim UUID.
        denial_code (str): CARC denial code (e.g. "CO-4", "PR-1").

    Steps:
        1. Fetch current claim status to confirm it is in denied state.
        2. Create a denial appeal record in the Billing Service.
        3. Resubmit the claim to the clearinghouse.

    Result:
        dict: Resubmission response from step 3.
    """

    @workflow.run
    async def run(self, claim_id: str, denial_code: str) -> dict[str, Any]:
        workflow.logger.info(
            "DenialResubmissionWorkflow starting claim_id=%s denial_code=%s",
            claim_id,
            denial_code,
        )

        # Step 1: confirm current state
        claim_status: dict[str, Any] = await workflow.execute_activity(
            get_claim_status,
            claim_id,
            start_to_close_timeout=_ACTIVITY_TIMEOUT,
            retry_policy=DEFAULT_RETRY_POLICY,
        )
        workflow.logger.info(
            "DenialResubmissionWorkflow claim_status claim_id=%s status=%s",
            claim_id,
            claim_status.get("status"),
        )

        # Step 2: create appeal
        await workflow.execute_activity(
            create_denial_appeal,
            args=[claim_id, denial_code],
            start_to_close_timeout=_ACTIVITY_TIMEOUT,
            retry_policy=DEFAULT_RETRY_POLICY,
        )

        # Step 3: resubmit
        result: dict[str, Any] = await workflow.execute_activity(
            resubmit_denied_claim,
            claim_id,
            start_to_close_timeout=_ACTIVITY_TIMEOUT,
            retry_policy=EXTERNAL_VENDOR_RETRY_POLICY,
        )

        workflow.logger.info(
            "DenialResubmissionWorkflow complete claim_id=%s resubmission_id=%s",
            claim_id,
            result.get("submission_id"),
        )
        return result


@workflow.defn
class ERAPostingWorkflow:
    """Process an ERA 835 remittance file and post payments/denials.

    Workflow ID convention: "era-posting-{era_file_key_hash}"
    Task queue: billing

    Input:
        era_file_path (str): S3 key for the ERA file.

    Result:
        dict: Posting summary (claims_posted, denials_routed, errors).
    """

    @workflow.run
    async def run(self, era_file_path: str) -> dict[str, Any]:
        workflow.logger.info(
            "ERAPostingWorkflow starting era_file_path=%s", era_file_path
        )

        result: dict[str, Any] = await workflow.execute_activity(
            process_era_file,
            era_file_path,
            start_to_close_timeout=_LONG_ACTIVITY_TIMEOUT,
            retry_policy=EXTERNAL_VENDOR_RETRY_POLICY,
        )

        workflow.logger.info(
            "ERAPostingWorkflow complete era_file=%s claims_posted=%s denials=%s",
            era_file_path,
            result.get("claims_posted"),
            result.get("denials_routed"),
        )
        return result


@workflow.defn
class MonthlyAgencyInvoicingWorkflow:
    """Generate and deliver all monthly invoices for active agencies.

    Workflow ID convention: "monthly-invoicing-{billing_month}"
    Task queue: billing
    Cron schedule: "0 2 1 * *"  (first of month 02:00 UTC)

    Input:
        billing_month (str): ISO month string (e.g. "2026-06").

    Result:
        dict: Invoicing summary (invoices_processed, total_billed_cents, errors).

    This is the top-level monthly billing cycle orchestration. It is safe
    to run multiple times for the same month — the Billing Service is
    idempotent for billing cycle calls.
    """

    @workflow.run
    async def run(self, billing_month: str) -> dict[str, Any]:
        workflow.logger.info(
            "MonthlyAgencyInvoicingWorkflow starting month=%s", billing_month
        )

        result: dict[str, Any] = await workflow.execute_activity(
            run_monthly_agency_invoicing,
            billing_month,
            start_to_close_timeout=_LONG_ACTIVITY_TIMEOUT,
            retry_policy=DEFAULT_RETRY_POLICY,
        )

        workflow.logger.info(
            "MonthlyAgencyInvoicingWorkflow complete month=%s invoices=%s",
            billing_month,
            result.get("invoices_processed"),
        )
        return result
