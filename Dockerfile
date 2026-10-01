FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PORT=8000

WORKDIR /srv

COPY requirements.txt .
RUN pip install -r requirements.txt

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
