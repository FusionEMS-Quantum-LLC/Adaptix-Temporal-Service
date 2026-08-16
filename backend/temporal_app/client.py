"""The single way an Adaptix worker connects to the Temporal server.

Every worker entrypoint MUST connect through :func:`connect_temporal_client`.

WHY THIS MODULE EXISTS
----------------------
The payload codec only protects the payloads that pass through the client that
carries it. This repository has six places that open a Temporal connection: the
multiplexed ``temporal_app/worker.py`` and one standalone module per queue in
``workers/``. Production runs the standalone modules — the ECS task definitions
override the container command with ``python -m workers.{domain}_worker`` — so a
codec wired into ``worker.py`` alone would encrypt nothing that customers touch.

Routing every connection through one function makes that impossible to get
wrong: a new worker module cannot connect without picking up encryption, and
connection concerns (data converter today, TLS when the server offers it) are
changed in exactly one place.

TLS — the honest current state
------------------------------
The Adaptix Temporal server is self-hosted on ECS Fargate from the
``temporalio/auto-setup`` image, which serves plain gRPC on 7233 with no TLS.
Worker traffic to it stays inside the VPC's private subnets. This function
therefore does not pass ``tls=`` — turning it on client-side against a server
that does not offer it fails every connection. Enabling TLS is an
infrastructure change (server certificates first, then this call site), not an
application flag, and it is tracked as such rather than pretended here.
"""

from __future__ import annotations

import logging

from temporalio.client import Client

from temporal_app.codec import build_data_converter
from temporal_app.config import TEMPORAL_HOST, TEMPORAL_NAMESPACE

logger = logging.getLogger(__name__)


async def connect_temporal_client(
    *,
    host: str | None = None,
    namespace: str | None = None,
) -> Client:
    """Connect to the Temporal server with the encrypting data converter applied.

    Raises ``PayloadCodecError`` before any connection attempt when payload
    encryption is not configured — the worker must not reach a task queue it
    cannot safely write history for.
    """
    target_host = host if host is not None else TEMPORAL_HOST
    target_namespace = namespace if namespace is not None else TEMPORAL_NAMESPACE

    # Built first, deliberately. A missing/malformed key must fail the process
    # before it registers as a poller on a live task queue.
    data_converter = build_data_converter()

    client = await Client.connect(
        target_host,
        namespace=target_namespace,
        data_converter=data_converter,
    )
    logger.info(
        "temporal_client.connected namespace=%s payload_encryption=%s",
        target_namespace,
        getattr(data_converter.payload_codec, "is_encrypting", False),
    )
    return client
