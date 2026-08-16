"""Adaptix Temporal worker entrypoints.

Each module in this package is the main entry point for one ECS worker task.
Worker tasks are launched by overriding the CMD in the ECS task definition:

    CMD: ["python", "-m", "workers.billing_worker"]
    CMD: ["python", "-m", "workers.notifications_worker"]
    CMD: ["python", "-m", "workers.documents_worker"]
    CMD: ["python", "-m", "workers.onboarding_worker"]
    CMD: ["python", "-m", "workers.migration_worker"]
    CMD: ["python", "-m", "workers.migration_bulk_worker"]

Each worker connects to the Temporal server through
``temporal_app.client.connect_temporal_client`` — never ``Client.connect``
directly — so every worker picks up the encrypting payload codec. A worker that
cannot encrypt payloads fails before it registers as a poller. It then
registers its workflow and activity classes and begins polling the named
TASK_QUEUE.
"""
