# One image for the simulator, the ingest service and the dashboard.
FROM ghcr.io/astral-sh/uv:0.11.26 AS uv

FROM python:3.12-slim-bookworm

COPY --from=uv /uv /usr/local/bin/uv

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    UV_PROJECT_ENVIRONMENT=/app/.venv \
    PYTHONUNBUFFERED=1 \
    PATH=/app/.venv/bin:$PATH

WORKDIR /app

# Dependencies first so code changes do not invalidate this layer.
COPY pyproject.toml uv.lock README.md LICENSE ./
RUN uv sync --frozen --no-dev --no-install-project

COPY src ./src
RUN uv sync --frozen --no-dev

RUN useradd --system --uid 10001 app
USER app

CMD ["python", "-m", "sparkplug_pipeline.ingest"]
