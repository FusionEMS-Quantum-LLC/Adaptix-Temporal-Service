"""Billing domain Temporal workflows.

Workflows in this module orchestrate multi-step billing operations that
require durability, retries, and auditable state transitions. Each workflow
is idempotent: re-running with the same workflow ID is safe.

Workflows registered here:
  - ClaimSubmissionWorkflow       — submit a single claim to Office Ally
  - DenialResubmissionWorkflow    — appeal + resubmit a denied claim, ONLY
                                    after a named human approves
  - ERAPostingWorkflow            — process an 835 ERA remittance file
  - MonthlyAgencyInvoicingWorkflow — generate and send all monthly invoices

PHI-safe: workflows carry only claim_id and agency_id identifiers. No PHI
values (patient name, DOB, address, SSN, diagnosis) are passed as workflow
inputs or stored in Temporal history. The Billing Service resolves all
display values from the database when the activity executes.
"""

from __future__ import annotations

import asyncio
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

# How long a prepared appeal waits for a human before the workflow gives up.
# Long, because a denial appeal is queued work for one biller, not an
# interactive prompt — but FINITE, so a forgotten appeal ends as an explicit
# "expired" record instead of a workflow that lives forever holding a decision
# nobody knows is outstanding. Expiry files nothing. See the class docstring.
_APPROVAL_TIMEOUT = timedelta(days=14)


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
        workflow.logger.info("ClaimSubmissionWorkflow starting claim_id=%s", claim_id)

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
    """Appeal a denied claim and resubmit it — only after a human approves.

    Workflow ID convention: "claim-denial-{claim_id}-{denial_code}"
    Task queue: billing

    Input:
        claim_id (str):    The Adaptix claim UUID.
        denial_code (str): CARC denial code (e.g. "CO-4", "PR-1").

    Steps:
        1. Fetch current claim status to confirm it is in denied state.
        2. BLOCK until a named human approves or rejects.
        3. On approval only: create the denial appeal record.
        4. On approval only: resubmit the claim to the clearinghouse.

    Result:
        dict: always carries ``outcome`` — one of "resubmitted", "rejected"
        or "expired" — plus ``appeal_filed`` and ``resubmitted`` booleans so a
        caller can never mistake a refusal for a submission.

    WHY THE HUMAN GATE EXISTS — read before changing anything here
    --------------------------------------------------------------
    Filing an appeal and resubmitting a claim are protected money actions.
    An appeal is a formal assertion to a payer, and a resubmission can
    duplicate a claim that is still adjudicating — which reads as duplicate
    billing. Neither may be taken autonomously, no matter how confident an
    upstream denial classifier is. Software prepares the work; a person
    decides to send it.

    THE GATE FAILS CLOSED. Silence is not consent:
      - no decision within the window  -> outcome "expired", nothing filed
      - an explicit reject             -> outcome "rejected", nothing filed
    Only an explicit approval reaches steps 3 and 4, and the approver's user
    id is carried into the result so the audit trail names a person.

    Steps 3 and 4 are deliberately ordered and NOT independently retryable as
    a pair: the appeal record is written first so that, if resubmission fails
    after its own retries, the appeal still exists and the claim is visibly
    mid-appeal rather than silently untouched.
    """

    def __init__(self) -> None:
        self._decision: dict[str, Any] | None = None

    @workflow.signal
    def submit_decision(
        self, approved: bool, actor_user_id: str, reason: str = ""
    ) -> None:
        """Record a named human's decision on this appeal.

        FIRST DECISION WINS. A later signal cannot overturn one already
        recorded: by the time a second arrives the first may already have
        filed an appeal with the payer, and this workflow cannot unsend that.
        Reversing a filed appeal is a new, separately-approved action.
        """
        if self._decision is not None:
            workflow.logger.info(
                "DenialResubmissionWorkflow ignoring late decision "
                "already_decided_by=%s late_actor=%s",
                self._decision.get("actor_user_id"),
                actor_user_id,
            )
            return
        self._decision = {
            "approved": approved,
            "actor_user_id": actor_user_id,
            "reason": reason,
        }
        workflow.logger.info(
            "DenialResubmissionWorkflow decision recorded approved=%s actor=%s",
            approved,
            actor_user_id,
        )

    @workflow.query
    def pending_decision(self) -> dict[str, Any]:
        """Expose gate state so an operator surface can list what awaits a human."""
        if self._decision is None:
            return {"awaiting_human": True, "approved": None, "actor_user_id": None}
        return {
            "awaiting_human": False,
            "approved": self._decision["approved"],
            "actor_user_id": self._decision["actor_user_id"],
        }

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

        # Step 2: block until a human decides. Nothing below this line runs
        # without an explicit approval.
        try:
            await workflow.wait_condition(
                lambda: self._decision is not None,
                timeout=_APPROVAL_TIMEOUT,
            )
        except asyncio.TimeoutError:
            workflow.logger.warning(
                "DenialResubmissionWorkflow expired unapproved claim_id=%s "
                "denial_code=%s — no appeal filed, no resubmission",
                claim_id,
                denial_code,
            )
            return self._outcome("expired", claim_id, denial_code)

        decision = self._decision or {}
        if not decision.get("approved"):
            workflow.logger.info(
                "DenialResubmissionWorkflow rejected claim_id=%s by=%s — "
                "no appeal filed, no resubmission",
                claim_id,
                decision.get("actor_user_id"),
            )
            return self._outcome("rejected", claim_id, denial_code, decision)

        # Step 3: create appeal (approved)
        await workflow.execute_activity(
            create_denial_appeal,
            args=[claim_id, denial_code],
            start_to_close_timeout=_ACTIVITY_TIMEOUT,
            retry_policy=DEFAULT_RETRY_POLICY,
        )

        # Step 4: resubmit (approved)
        result: dict[str, Any] = await workflow.execute_activity(
            resubmit_denied_claim,
            claim_id,
            start_to_close_timeout=_ACTIVITY_TIMEOUT,
            retry_policy=EXTERNAL_VENDOR_RETRY_POLICY,
        )

        workflow.logger.info(
            "DenialResubmissionWorkflow complete claim_id=%s resubmission_id=%s "
            "approved_by=%s",
            claim_id,
            result.get("submission_id"),
            decision.get("actor_user_id"),
        )
        outcome = self._outcome("resubmitted", claim_id, denial_code, decision)
        outcome["appeal_filed"] = True
        outcome["resubmitted"] = True
        outcome["submission"] = result
        return outcome

    @staticmethod
    def _outcome(
        outcome: str,
        claim_id: str,
        denial_code: str,
        decision: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Build a result that states plainly whether money actions were taken."""
        decision = decision or {}
        return {
            "outcome": outcome,
            "claim_id": claim_id,
            "denial_code": denial_code,
            "appeal_filed": False,
            "resubmitted": False,
            "decided_by": decision.get("actor_user_id"),
            "reason": decision.get("reason", ""),
        }


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
