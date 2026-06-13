"""Billing domain Temporal workflow definitions.

Workflows:
  ClaimSubmissionWorkflow        — submits a claim to Office Ally clearinghouse.
  DenialResubmissionWorkflow     — resubmits correctable denied claims.
  ERAPostingWorkflow             — posts ERA/835 remittance to claims.
  MonthlyInvoicingWorkflow       — runs monthly agency invoicing via Stripe.

Workflow determinism rules:
  - No I/O inside workflow code. All I/O goes through activity calls.
  - No random, datetime.now(), uuid4(), or other non-deterministic calls
    inside workflow functions. Use workflow.now() for current time.
  - Activity calls use the DEFAULT_RETRY_POLICY from config unless
    explicitly overridden (e.g. longer timeout for batch invoicing).

Workflow IDs:
  Callers must use the canonical workflow ID formats to guarantee idempotency:
    claim-submit-{claim_id}
    claim-denial-resubmit-{claim_id}-{denial_code}
    era-posting-{safe_file_key}
    monthly-invoicing-{billing_month}
  These IDs must be used consistently by the temporal_client modules in
  Billing Service and Core Service.
"""

from __future__ import annotations

import logging
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    # Activities are imported inside the unsafe context because they use
    # asyncio and httpx which are not determinism-safe in the workflow sandbox.
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

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# ClaimSubmissionWorkflow
# ---------------------------------------------------------------------------


@workflow.defn
class ClaimSubmissionWorkflow:
    """Submit a single claim to the Office Ally clearinghouse.

    Input:  claim_id (str) — the Adaptix Claim UUID.
    Output: dict — the Billing Service response payload.

    Idempotency: The Billing Service submit-to-clearinghouse endpoint is
    idempotent on claim_id. A retry of this workflow with the same claim_id
    will detect the existing ClearinghouseSubmission row and return its
    current state without re-uploading the 837P.

    Execution timeout: 30 minutes. A claim submission should complete well
    within this window under normal Office Ally connectivity.
    """

    @workflow.run
    async def run(self, claim_id: str) -> dict:
        workflow.logger.info("ClaimSubmissionWorkflow started claim_id=%s", claim_id)

        result = await workflow.execute_activity(
            submit_claim_to_clearinghouse,
            claim_id,
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=EXTERNAL_VENDOR_RETRY_POLICY,
        )

        workflow.logger.info("ClaimSubmissionWorkflow completed claim_id=%s", claim_id)
        return result


# ---------------------------------------------------------------------------
# DenialResubmissionWorkflow
# ---------------------------------------------------------------------------


@workflow.defn
class DenialResubmissionWorkflow:
    """Resubmit a correctable denied claim.

    Input:  claim_id (str), denial_code (str) — CARC denial code.
    Output: dict — the resubmission response payload.

    Steps:
      1. Verify current claim status is "denied".
      2. Create an appeal record with the denial code.
      3. Resubmit to clearinghouse.

    Non-correctable denials (those not mapped to deterministic correction
    strategies in the Billing Service auto-resubmit logic) will surface as
    a ValidationError from the create_denial_appeal activity, which is
    non-retryable. The workflow fails fast in that case, signalling to the
    operator that human intervention is required.
    """

    @workflow.run
    async def run(self, claim_id: str, denial_code: str) -> dict:
        workflow.logger.info(
            "DenialResubmissionWorkflow started claim_id=%s denial_code=%s",
            claim_id,
            denial_code,
        )

        # Step 1: Confirm current claim state.
        claim_status = await workflow.execute_activity(
            get_claim_status,
            claim_id,
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=DEFAULT_RETRY_POLICY,
        )

        if claim_status.get("status") not in ("denied", "appealing"):
            workflow.logger.warning(
                "DenialResubmissionWorkflow claim_id=%s unexpected_status=%s "
                "— skipping resubmission",
                claim_id,
                claim_status.get("status"),
            )
            return {
                "skipped": True,
                "reason": "claim_not_in_denied_state",
                "current_status": claim_status.get("status"),
            }

        # Step 2: Create the denial appeal record.
        appeal_result = await workflow.execute_activity(
            create_denial_appeal,
            args=[claim_id, denial_code],
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=DEFAULT_RETRY_POLICY,
        )

        workflow.logger.info(
            "DenialResubmissionWorkflow appeal_created claim_id=%s appeal_id=%s",
            claim_id,
            appeal_result.get("appeal_id"),
        )

        # Step 3: Resubmit to clearinghouse.
        resubmit_result = await workflow.execute_activity(
            resubmit_denied_claim,
            claim_id,
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=EXTERNAL_VENDOR_RETRY_POLICY,
        )

        workflow.logger.info(
            "DenialResubmissionWorkflow completed claim_id=%s", claim_id
        )
        return {
            "appeal": appeal_result,
            "resubmission": resubmit_result,
        }


# ---------------------------------------------------------------------------
# ERAPostingWorkflow
# ---------------------------------------------------------------------------


@workflow.defn
class ERAPostingWorkflow:
    """Post an ERA/835 remittance file to the corresponding claims.

    Input:  era_file_path (str) — S3 key for the ERA file.
    Output: dict — posting summary (claims_posted, denials_routed, errors).

    The Billing Service ERA processor:
      1. Downloads the ERA from S3.
      2. Parses the X12 835 transaction set.
      3. Posts payment or denial state to each referenced claim.
      4. Routes denials to the auto-resubmit queue.
      5. Writes ClearinghouseEra and BillingAuditEvent rows.

    ERA processing is idempotent: the Billing Service uses audit table
    anchoring to skip ERA files that have already been processed. A retry
    of this workflow with the same era_file_path is safe.
    """

    @workflow.run
    async def run(self, era_file_path: str) -> dict:
        workflow.logger.info(
            "ERAPostingWorkflow started era_file_path=%s", era_file_path
        )

        result = await workflow.execute_activity(
            process_era_file,
            era_file_path,
            start_to_close_timeout=timedelta(minutes=10),
            retry_policy=DEFAULT_RETRY_POLICY,
        )

        workflow.logger.info(
            "ERAPostingWorkflow completed era_file_path=%s claims_posted=%s",
            era_file_path,
            result.get("claims_posted"),
        )
        return result


# ---------------------------------------------------------------------------
# MonthlyInvoicingWorkflow
# ---------------------------------------------------------------------------


@workflow.defn
class MonthlyInvoicingWorkflow:
    """Run monthly agency invoicing for all active subscriptions.

    Input:  billing_month (str) — ISO month string, e.g. "2026-06".
    Output: dict — invoicing summary.

    This workflow triggers a potentially long-running operation in the
    Billing Service that:
      1. Queries all active TenantSubscription rows.
      2. Creates billing cycle records for each.
      3. Charges each via Stripe.
      4. Sends billing statements via SES and/or PostGrid.
      5. Persists billing audit events.

    The Billing Service endpoint is idempotent on billing_month — a second
    call for the same month returns the existing cycle records without
    double-charging.

    Execution timeout: 60 minutes. Monthly invoicing covers all active
    agencies and can take significant time under high agency volume.
    """

    @workflow.run
    async def run(self, billing_month: str) -> dict:
        workflow.logger.info(
            "MonthlyInvoicingWorkflow started billing_month=%s", billing_month
        )

        # Extended start-to-close timeout for the invoicing activity because
        # it processes all active agencies in a single HTTP call.
        result = await workflow.execute_activity(
            run_monthly_agency_invoicing,
            billing_month,
            start_to_close_timeout=timedelta(minutes=55),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(seconds=10),
                backoff_coefficient=2.0,
                maximum_interval=timedelta(minutes=10),
                maximum_attempts=3,
                # Invoicing errors that indicate bad data or auth misconfiguration
                # must not retry.
                non_retryable_error_types=["ValidationError", "AuthorizationError"],
            ),
        )

        workflow.logger.info(
            "MonthlyInvoicingWorkflow completed billing_month=%s invoices_processed=%s",
            billing_month,
            result.get("invoices_processed"),
        )
        return result
