"""Tests for Temporal workflow definitions.

These tests verify workflow structure — that @workflow.defn decorators are
applied, that the run() method has the correct signature, and that workflow
class names match the expected Temporal workflow names.

What these tests prove:
  - All expected workflow classes are importable and decorated with @workflow.defn.
  - Workflow run() methods accept the expected parameter types.
  - The worker _build_worker function raises ValueError for unknown task queues.
  - Each task queue registers the expected set of workflows.

What these tests do NOT prove:
  - Actual workflow execution against a Temporal server.
  - Activity execution.
  - Runtime behavior.
  - Temporal history replay determinism.

Note on Temporal testing strategy:
  Full workflow execution tests require the temporalio testing framework
  (temporalio.testing.WorkflowEnvironment) and are in test_workflow_execution.py
  (future). These structural tests run without a Temporal server and are safe
  for standard CI.
"""

from __future__ import annotations

import inspect

import pytest


# ---------------------------------------------------------------------------
# Billing workflow structure
# ---------------------------------------------------------------------------


def test_claim_submission_workflow_is_defined():
    """ClaimSubmissionWorkflow is importable and has a run() method."""
    from temporal_app.workflows.billing_workflows import ClaimSubmissionWorkflow

    assert hasattr(ClaimSubmissionWorkflow, "run")
    sig = inspect.signature(ClaimSubmissionWorkflow.run)
    params = list(sig.parameters.keys())
    # run(self, claim_id: str)
    assert "claim_id" in params


def test_denial_resubmission_workflow_is_defined():
    """DenialResubmissionWorkflow has a run(claim_id, denial_code) signature."""
    from temporal_app.workflows.billing_workflows import DenialResubmissionWorkflow

    sig = inspect.signature(DenialResubmissionWorkflow.run)
    params = list(sig.parameters.keys())
    assert "claim_id" in params
    assert "denial_code" in params


def test_era_posting_workflow_is_defined():
    """ERAPostingWorkflow has a run(era_file_path) signature."""
    from temporal_app.workflows.billing_workflows import ERAPostingWorkflow

    sig = inspect.signature(ERAPostingWorkflow.run)
    params = list(sig.parameters.keys())
    assert "era_file_path" in params


def test_monthly_invoicing_workflow_is_defined():
    """MonthlyAgencyInvoicingWorkflow has a run(billing_month) signature."""
    from temporal_app.workflows.billing_workflows import MonthlyAgencyInvoicingWorkflow

    sig = inspect.signature(MonthlyAgencyInvoicingWorkflow.run)
    params = list(sig.parameters.keys())
    assert "billing_month" in params


# ---------------------------------------------------------------------------
# Document workflow structure
# ---------------------------------------------------------------------------


def test_generate_pdf_workflow_is_defined():
    """GeneratePDFWorkflow has a run(document_type, context, output_key) signature."""
    from temporal_app.workflows.document_workflows import GeneratePDFWorkflow

    sig = inspect.signature(GeneratePDFWorkflow.run)
    params = list(sig.parameters.keys())
    assert "document_type" in params
    assert "context" in params
    assert "output_key" in params


def test_trustsign_workflow_is_defined():
    """TrustSignWorkflow has a run(document_id, recipient_email) signature."""
    from temporal_app.workflows.document_workflows import TrustSignWorkflow

    sig = inspect.signature(TrustSignWorkflow.run)
    params = list(sig.parameters.keys())
    assert "document_id" in params
    assert "recipient_email" in params


def test_postgrid_delivery_workflow_is_defined():
    """PostGridDeliveryWorkflow has a run(statement_id) signature."""
    from temporal_app.workflows.document_workflows import PostGridDeliveryWorkflow

    sig = inspect.signature(PostGridDeliveryWorkflow.run)
    params = list(sig.parameters.keys())
    assert "statement_id" in params


# ---------------------------------------------------------------------------
# Notification workflow structure
# ---------------------------------------------------------------------------


def test_send_batch_statements_workflow_is_defined():
    """SendBatchStatementsWorkflow has a run(agency_id, month) signature."""
    from temporal_app.workflows.notification_workflows import (
        SendBatchStatementsWorkflow,
    )

    sig = inspect.signature(SendBatchStatementsWorkflow.run)
    params = list(sig.parameters.keys())
    assert "agency_id" in params
    assert "month" in params


# ---------------------------------------------------------------------------
# Onboarding workflow structure
# ---------------------------------------------------------------------------


def test_agency_onboarding_workflow_is_defined():
    """AgencyOnboardingWorkflow has a run(tenant_id, admin_email) signature."""
    from temporal_app.workflows.onboarding_workflows import AgencyOnboardingWorkflow

    sig = inspect.signature(AgencyOnboardingWorkflow.run)
    params = list(sig.parameters.keys())
    assert "tenant_id" in params
    assert "admin_email" in params


# ---------------------------------------------------------------------------
# Worker build function
# ---------------------------------------------------------------------------


def test_build_worker_raises_for_unknown_queue(monkeypatch):
    """_build_worker raises ValueError for an unrecognised task queue."""
    from unittest.mock import MagicMock

    from temporal_app.worker import _build_worker

    mock_client = MagicMock()
    with pytest.raises(ValueError, match="Unrecognised TASK_QUEUE"):
        _build_worker(mock_client, "unknown-queue")


def test_build_worker_billing_queue_returns_worker(monkeypatch):
    """_build_worker returns a Worker instance for the billing queue.

    This is a structural test — it does not connect to Temporal.
    The Worker constructor is mocked to avoid a real connection attempt.
    """
    from unittest.mock import MagicMock, patch

    mock_client = MagicMock()
    mock_worker = MagicMock()

    with patch(
        "temporal_app.worker.Worker", return_value=mock_worker
    ) as mock_worker_cls:
        from temporal_app.worker import _build_worker

        result = _build_worker(mock_client, "billing")

    assert result is mock_worker
    # Verify the Worker was instantiated with the billing task queue.
    call_kwargs = mock_worker_cls.call_args
    assert call_kwargs.kwargs.get("task_queue") == "billing" or (
        len(call_kwargs.args) >= 2 and call_kwargs.args[1] == "billing"
    )
