FROM public.ecr.aws/docker/library/python:3.12-slim@sha256:090ba77e2958f6af52a5341f788b50b032dd4ca28377d2893dcf1ecbdfdfe203

WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app \
    PIP_NO_CACHE_DIR=1

# Security: apply latest Debian security patches before installing dependencies.
RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Patch bundled pip/setuptools/wheel for known CVEs before installing project.
RUN pip install --no-cache-dir --upgrade \
        "pip>=24.2" \
        "setuptools>=78.1.1" \
        "wheel>=0.44.0"

# Copy pyproject.toml first for layer caching — dependencies are re-installed
# only when pyproject.toml changes.
COPY pyproject.toml ./
COPY requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir --no-build-isolation .

# Copy application source after dependencies to maximise layer cache hits.
COPY . .

ARG GIT_COMMIT_SHA=unknown
ARG BUILD_TIME=unknown
ENV GIT_COMMIT_SHA=${GIT_COMMIT_SHA} \
    BUILD_TIME=${BUILD_TIME}

# Run as non-root user for runtime security.
RUN groupadd --gid 1001 appgroup && \
    useradd --uid 1001 --gid appgroup --shell /bin/bash --create-home appuser && \
    chown -R appuser:appgroup /app
USER appuser

# Temporal workers do not expose an HTTP port — they connect outbound to
# the Temporal server only. No EXPOSE directive is needed.
# ECS health checks should use the temporal worker liveness probe (future
# HTTP healthz endpoint) or ECS task status.

# entrypoint.sh validates required env vars, then exec's the container command.
# Each ECS worker task overrides CMD with its own entrypoint, e.g.
#   command = ["python", "-m", "workers.onboarding_worker"]
# When no command is supplied, the default CMD below runs the multiplexed
# worker which routes on the TASK_QUEUE env var.
ENTRYPOINT ["sh", "/app/entrypoint.sh"]
CMD ["python", "-m", "temporal_app.worker"]
