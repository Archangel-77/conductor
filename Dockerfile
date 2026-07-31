# ─────────────────────────────────────────────────────────────────────────
# Conductor — Worker Image
# ─────────────────────────────────────────────────────────────────────────
# Build:
#   docker build -t conductor:0.1.0 .
#
# Run (worker polls PostgreSQL; see docker-compose.yml for a full stack):
#   docker run --rm -e DATABASE_URL=postgresql://... conductor:0.1.0
#
# Handlers: mount a module and pass it via CONDUCTOR_HANDLERS_MODULE or
#   `conductor worker --handlers myapp.handlers`.
# ─────────────────────────────────────────────────────────────────────────

FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Copy only what pip install needs, then the package itself.
COPY pyproject.toml setup.py README.md LICENSE ./
COPY conductor ./conductor

RUN pip install --no-cache-dir . \
    && rm -rf /root/.cache/pip

# Run as an unprivileged user.
RUN useradd --create-home --shell /usr/sbin/nologin conductor \
    && chown -R conductor:conductor /app
USER conductor

# Metrics/health HTTP server (Prometheus /metrics + JSON /health).
EXPOSE 8000

# Liveness/readiness against the health endpoint (python has urllib; slim has no curl).
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD ["python", "-c", \
         "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')"]

ENTRYPOINT ["conductor"]
CMD ["worker"]
