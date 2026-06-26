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

# If the container was given an explicit command (e.g. the ECS task definition
# sets command = ["python", "-m", "workers.onboarding_worker"]), run that exact
# command after the env-validation gate above. Docker `command` / ECS
# `command` override the image CMD; this wrapper is the ENTRYPOINT, so the
# override arrives here as positional args. Falling back to the multiplexed
# temporal_app.worker (which routes on TASK_QUEUE) preserves standalone runs
# and any deployment that does not override the command.
if [ "$#" -gt 0 ]; then
    exec "$@"
fi

exec python -m temporal_app.worker
