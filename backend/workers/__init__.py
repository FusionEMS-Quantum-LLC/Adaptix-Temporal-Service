"""Adaptix Temporal worker entrypoints.

Each module in this package is the main entry point for one ECS worker task.
Worker tasks are launched by overriding the CMD in the ECS task definition:

    CMD: ["python", "-m", "workers.billing_worker"]
    CMD: ["python", "-m", "workers.notifications_worker"]
    CMD: ["python", "-m", "workers.documents_worker"]
    CMD: ["python", "-m", "workers.onboarding_worker"]

Each worker connects to the Temporal server at TEMPORAL_HOST, registers its
workflow and activity classes, and begins polling the named TASK_QUEUE.
"""
