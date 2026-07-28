# Single image backing BOTH the API and the orchestrator containers.
# The differing behavior comes from the compose `command`, not the image.
FROM python:3.12-slim

# uv provides fast, reproducible dependency resolution.
COPY --from=ghcr.io/astral-sh/uv:0.5.11 /uv /uvx /bin/

WORKDIR /app
ENV PYTHONPATH=/app/src \
    PYTHONUNBUFFERED=1 \
    UV_LINK_MODE=copy

# Install dependencies first (better layer caching). --no-install-project keeps
# the source out of this layer so code edits don't reinstall the world.
COPY pyproject.toml README.md ./
RUN uv sync --no-dev --no-install-project

# Copy application source and bundled mock data.
COPY src ./src
COPY data ./data

# Put the virtualenv on PATH so `uvicorn`/`python` resolve to it.
ENV PATH="/app/.venv/bin:$PATH"