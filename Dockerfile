FROM python:3.12-slim

# uv, pinned to the version that generated uv.lock.
COPY --from=ghcr.io/astral-sh/uv:0.12.23 /uv /uvx /bin/

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    UV_PROJECT_ENVIRONMENT=/srv/.venv \
    PORT=8000
ENV PATH="/srv/.venv/bin:${PATH}"

WORKDIR /srv

# Dependencies first so this layer is cached until the lockfile changes. --locked
# fails the build if uv.lock is out of date with pyproject.toml, so the image always
# gets exactly the versions that were tested.
COPY pyproject.toml uv.lock .python-version ./
RUN uv sync --locked --no-dev --no-install-project --no-cache

COPY app ./app

RUN useradd --create-home --uid 10001 appuser
USER appuser

EXPOSE 8000

# Liveness only: the container is healthy if the process serves HTTP. Database
# reachability is a readiness concern (GET /readyz), not a reason to restart us.
HEALTHCHECK --interval=10s --timeout=3s --start-period=30s --retries=3 \
  CMD python -c "import os, urllib.request; urllib.request.urlopen('http://127.0.0.1:%s/livez' % os.environ.get('PORT', '8000'), timeout=2)"

# One worker on purpose: the database is the bottleneck, asyncio handles the
# concurrency, and a single process keeps the Prometheus counters and the /logs
# ring buffer coherent. Scale out with more instances, not more workers.
CMD ["sh", "-c", "exec uvicorn app.main:app --host 0.0.0.0 --port ${PORT} --no-access-log --backlog 4096 --timeout-keep-alive 75"]
