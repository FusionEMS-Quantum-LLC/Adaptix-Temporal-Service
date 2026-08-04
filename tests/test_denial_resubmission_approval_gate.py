"""Execution tests for the DenialResubmissionWorkflow human-approval gate.

WHAT IS REAL HERE, AND THE ONE THING THAT IS NOT
------------------------------------------------
Real: a real Temporal server, the real DenialResubmissionWorkflow class, real
workflow execution, real signals, real queries, and a real durable timer for
the approval window. Nothing about the gate itself is simulated.

Not real, deliberately: the three billing activities are replaced by call
RECORDERS. They are not stand-ins for behaviour the product lacks — the real
activities exist and are tested in test_billing_activities.py. They are
replaced because running them here would file a genuine appeal with a payer
and resubmit a genuine claim. A test that proves "we must never contact the
payer without approval" cannot contact the payer to prove it.

Recording the calls IS the assertion. For a protected money action the
question is not what the workflow returned — a return value can be wrong —
but whether create_denial_appeal and resubmit_denied_claim were INVOKED AT
ALL. Every refusal test asserts they were not.

What these tests prove:
  - Without a decision the workflow blocks: neither create_denial_appeal nor
    resubmit_denied_claim is ever invoked.
  - An explicit reject files nothing and resubmits nothing.
  - Letting the approval window elapse files nothing and resubmits nothing —
    silence is never consent.
  - Only an explicit approval reaches the appeal and the resubmission, and the
    approving user id is carried into the result.
  - A late second decision cannot overturn the first.
  - The pending_decision query reports gate state for an operator surface.

What these tests do NOT prove:
  - That the real Billing Service activities behave correctly (they are
    replaced here). See test_billing_activities.py for those.
  - That any caller starts this workflow correctly.
  - Production Temporal configuration, task-queue routing, or worker health.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import timedelta
from typing import Any

import pytest
from temporalio import activity
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from temporal_app.workflows.billing_workflows import DenialResubmissionWorkflow

TASK_QUEUE = "billing-test"

# Records every protected call the workflow makes. A test that expects a
# refusal asserts these stay empty.
CALLS: dict[str, list[Any]] = {"status": [], "appeal": [], "resubmit": []}


@activity.defn(name="get_claim_status")
async def record_get_claim_status(claim_id: str) -> dict[str, Any]:
    CALLS["status"].append(claim_id)
    return {"claim_id": claim_id, "status": "denied"}


@activity.defn(name="create_denial_appeal")
async def record_create_denial_appeal(
    claim_id: str, denial_code: str
) -> dict[str, Any]:
    CALLS["appeal"].append((claim_id, denial_code))
    return {"appeal_id": "appeal-1"}


@activity.defn(name="resubmit_denied_claim")
async def record_resubmit_denied_claim(claim_id: str) -> dict[str, Any]:
    CALLS["resubmit"].append(claim_id)
    return {"submission_id": "sub-1"}


@pytest.fixture(autouse=True)
def _reset_calls():
    for key in CALLS:
        CALLS[key].clear()
    yield


def _wf_id() -> str:
    return f"claim-denial-{uuid.uuid4()}"


async def _start(env: WorkflowEnvironment, worker_ctx):
    """Start the workflow and return its handle."""
    return await env.client.start_workflow(
        DenialResubmissionWorkflow.run,
        args=["claim-123", "CO-4"],
        id=_wf_id(),
        task_queue=TASK_QUEUE,
    )


def _worker(env: WorkflowEnvironment) -> Worker:
    return Worker(
        env.client,
        task_queue=TASK_QUEUE,
        workflows=[DenialResubmissionWorkflow],
        activities=[
            record_get_claim_status,
            record_create_denial_appeal,
            record_resubmit_denied_claim,
        ],
    )


@pytest.mark.asyncio
async def test_blocks_and_files_nothing_until_a_human_decides():
    """The gate holds: no decision means no appeal and no resubmission."""
    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with _worker(env):
            handle = await _start(env, None)

            # Let the workflow run up to the gate. Step 1 completes; the two
            # protected steps must not.
            await asyncio.sleep(0)
            for _ in range(20):
                if CALLS["status"]:
                    break
                await asyncio.sleep(0.1)

            state = await handle.query(DenialResubmissionWorkflow.pending_decision)

            assert state["awaiting_human"] is True
            assert CALLS["appeal"] == [], "appeal was filed with no human approval"
            assert CALLS["resubmit"] == [], (
                "claim was resubmitted with no human approval"
            )

            await handle.signal(
                DenialResubmissionWorkflow.submit_decision,
                args=[False, "biller-1", "stop"],
            )
            await handle.result()


@pytest.mark.asyncio
async def test_reject_files_nothing_and_resubmits_nothing():
    """An explicit refusal must not touch the payer."""
    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with _worker(env):
            handle = await _start(env, None)
            await handle.signal(
                DenialResubmissionWorkflow.submit_decision,
                args=[False, "biller-1", "not appealable"],
            )
            result = await handle.result()

    assert result["outcome"] == "rejected"
    assert result["appeal_filed"] is False
    assert result["resubmitted"] is False
    assert result["decided_by"] == "biller-1"
    assert result["reason"] == "not appealable"
    assert CALLS["appeal"] == []
    assert CALLS["resubmit"] == []


@pytest.mark.asyncio
async def test_silence_expires_and_is_never_treated_as_consent():
    """Letting the window elapse files nothing. The gate fails closed."""
    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with _worker(env):
            handle = await _start(env, None)
            # Skip past the 14-day approval window without sending a decision.
            await env.sleep(timedelta(days=15))
            result = await handle.result()

    assert result["outcome"] == "expired"
    assert result["appeal_filed"] is False
    assert result["resubmitted"] is False
    assert result["decided_by"] is None
    assert CALLS["appeal"] == [], "an unapproved appeal was filed after timeout"
    assert CALLS["resubmit"] == [], "an unapproved claim was resubmitted after timeout"


@pytest.mark.asyncio
async def test_approval_is_the_only_path_to_appeal_and_resubmit():
    """Only an explicit approval reaches the two protected activities."""
    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with _worker(env):
            handle = await _start(env, None)
            await handle.signal(
                DenialResubmissionWorkflow.submit_decision,
                args=[True, "biller-7", "documentation attached"],
            )
            result = await handle.result()

    assert result["outcome"] == "resubmitted"
    assert result["appeal_filed"] is True
    assert result["resubmitted"] is True
    assert result["decided_by"] == "biller-7"
    assert result["submission"]["submission_id"] == "sub-1"
    assert CALLS["appeal"] == [("claim-123", "CO-4")]
    assert CALLS["resubmit"] == ["claim-123"]


@pytest.mark.asyncio
async def test_a_late_decision_cannot_overturn_the_first():
    """First decision wins — a filed appeal cannot be unsent by a later signal."""
    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with _worker(env):
            handle = await _start(env, None)
            await handle.signal(
                DenialResubmissionWorkflow.submit_decision,
                args=[False, "biller-1", "no"],
            )
            await handle.signal(
                DenialResubmissionWorkflow.submit_decision,
                args=[True, "biller-2", "yes"],
            )
            result = await handle.result()

    assert result["outcome"] == "rejected"
    assert result["decided_by"] == "biller-1"
    assert CALLS["appeal"] == []
    assert CALLS["resubmit"] == []
