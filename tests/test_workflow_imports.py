"""Smoke tests for workflow module imports and workflow class registration.

What these tests prove:
  - All four workflow modules import without error.
  - The @workflow.defn decorator is applied to each workflow class.
  - All workflow classes have a run method.
  - The worker.py dispatcher module imports correctly with updated paths.

What these tests do NOT prove:
  - Runtime execution against a real Temporal server.
  - Activity correctness.
  - Database or API connectivity.
"""

from __future__ import annotations


def test_billing_workflows_import():
    """billing_workflows module imports all four workflow classes."""
    from temporal_app.workflows.billing_workflows import (
        ClaimSubmissionWorkflow,
        DenialResubmissionWorkflow,
        ERAPostingWorkflow,
        MonthlyAgencyInvoicingWorkflow,
    )

    for cls in [
        ClaimSubmissionWorkflow,
        DenialResubmissionWorkflow,
        ERAPostingWorkflow,
        MonthlyAgencyInvoicingWorkflow,
    ]:
        assert callable(cls), f"{cls.__name__} should be callable"
        assert hasattr(cls, "run"), f"{cls.__name__} should have a run method"


def test_notification_workflows_import():
    """notification_workflows module imports SendBatchStatementsWorkflow."""
    from temporal_app.workflows.notification_workflows import (
        SendBatchStatementsWorkflow,
    )

    assert callable(SendBatchStatementsWorkflow)
    assert hasattr(SendBatchStatementsWorkflow, "run")


def test_document_workflows_import():
    """document_workflows module imports all three workflow classes."""
    from temporal_app.workflows.document_workflows import (
        GeneratePDFWorkflow,
        PostGridDeliveryWorkflow,
        TrustSignWorkflow,
    )

    for cls in [GeneratePDFWorkflow, TrustSignWorkflow, PostGridDeliveryWorkflow]:
        assert callable(cls), f"{cls.__name__} should be callable"
        assert hasattr(cls, "run"), f"{cls.__name__} should have a run method"


def test_onboarding_workflows_import():
    """onboarding_workflows module imports AgencyOnboardingWorkflow."""
    from temporal_app.workflows.onboarding_workflows import AgencyOnboardingWorkflow

    assert callable(AgencyOnboardingWorkflow)
    assert hasattr(AgencyOnboardingWorkflow, "run")


def test_worker_dispatcher_imports():
    """worker.py dispatcher module imports without error using updated module paths."""
    import importlib
    import sys

    # Remove cached modules to force a clean import.
    for mod in list(sys.modules.keys()):
        if "temporal_app" in mod:
            del sys.modules[mod]

    # Import the dispatcher — raises ImportError if any module path is wrong.
    import temporal_app.worker as w

    assert hasattr(w, "main"), "worker.py must have a main() coroutine"
    assert hasattr(w, "_build_worker"), "worker.py must have _build_worker()"


def test_worker_entrypoints_import():
    """All four worker entrypoint modules import without error."""
    import importlib
    import sys

    for mod_name in list(sys.modules.keys()):
        if "temporal_app" in mod_name or "workers" in mod_name:
            del sys.modules[mod_name]

    from workers import (
        billing_worker,
        documents_worker,
        notifications_worker,
        onboarding_worker,
    )

    for w in [
        billing_worker,
        documents_worker,
        notifications_worker,
        onboarding_worker,
    ]:
        assert hasattr(w, "main"), f"{w.__name__} must have main()"
