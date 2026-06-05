#!/bin/sh
set -eu

TASK_QUEUE="${TASK_QUEUE:-}"
LOG_LEVEL="${LOG_LEVEL:-INFO}"

if [ -z "${TASK_QUEUE}" ]; then
    echo '{"level":"ERROR","message":"TASK_QUEUE is required. Set it to billing|notifications|documents|onboarding in the ECS task definition."}'
    exit 1
fi

if [ -z "${TEMPORAL_HOST:-}" ]; then
    echo '{"level":"ERROR","message":"TEMPORAL_HOST is required. Set it to the Temporal server host:port in the ECS task definition."}'
    exit 1
fi

if [ -z "${ADAPTIX_API_BASE:-}" ]; then
    echo '{"level":"ERROR","message":"ADAPTIX_API_BASE is required. Set it to the Adaptix API base URL in the ECS task definition."}'
    exit 1
fi

if [ -z "${ADAPTIX_SERVICE_TOKEN:-}" ]; then
    echo '{"level":"ERROR","message":"ADAPTIX_SERVICE_TOKEN is required. Set it via AWS Secrets Manager in the ECS task definition."}'
    exit 1
fi

echo "{\"level\":\"INFO\",\"message\":\"Starting Adaptix Temporal worker\",\"task_queue\":\"${TASK_QUEUE}\"}"

exec python -m temporal_app.worker
