# Deployment

Conductor ships as a plain Python package plus an optional `conductor` CLI.
This page covers running it with Docker, Docker Compose, Kubernetes, and
systemd. All artifacts referenced here are committed in the repository and
can be validated locally without Docker via `scripts/validate_deploy.py`.

## Docker

A production-ready `Dockerfile` is included:

```bash
docker build -t conductor:0.1.0 .
docker run --rm \
  -e DATABASE_URL=postgresql://user:pass@pg-host:5432/conductor \
  -p 8000:8000 \
  conductor:0.1.0
```

The image:

- uses `python:3.11-slim` and runs as a non-root `conductor` user;
- installs the package and its `conductor` console script;
- exposes port `8000` and a `HEALTHCHECK` against `/health`;
- runs `conductor worker` (reads env vars / `.env`).

Add task handlers by mounting a module and pointing the worker at it:

```bash
docker run --rm \
  -v "$PWD/myapp:/app/handlers" \
  -e DATABASE_URL=postgresql://... \
  -e CONDUCTOR_HANDLERS_MODULE=handlers.myapp \
  conductor:0.1.0
```

## Docker Compose

### Development

`docker-compose.yml` runs PostgreSQL + a worker:

```bash
docker compose up -d --build
```

- Worker: `http://localhost:8000/health` and `/metrics`.
- Config is set via `environment:` (see [Configuration](configuration.md)).

### Production

`docker-compose.prod.yml` adds worker replicas, resource limits, log
rotation, and a nightly `pg_dump` backup sidecar:

```bash
export POSTGRES_PASSWORD='<strong-password>'
docker compose -f docker-compose.prod.yml up -d --build --scale worker=3
```

> `deploy.replicas` is honoured under Docker Swarm; under plain Compose use
> `--scale worker=N` (as above).

Validate both files:

```bash
docker compose config --quiet
docker compose -f docker-compose.prod.yml config --quiet
python scripts/validate_deploy.py   # static checks, no Docker needed
```

## Kubernetes

A complete manifest is in `examples/kubernetes.yaml`:

```bash
kubectl apply -f examples/kubernetes.yaml
```

It defines a `ConfigMap` (non-secret config), a `Secret` (DATABASE_URL),
a `Deployment` with 3 replicas plus liveness/readiness probes on `/health`,
and a `ClusterIP` `Service` on port 8000. PostgreSQL is assumed to be
external/managed — set `DATABASE_URL` in the Secret accordingly.

## Systemd

A unit file is provided at `examples/conductor-worker.service`:

```bash
sudo cp examples/conductor-worker.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now conductor-worker
```

It runs `conductor worker` under a dedicated `conductor` user with
`EnvironmentFile` pointing at your `.env` file.

## Observability

The worker serves Prometheus metrics at `/metrics` and a JSON health check
at `/health` on `METRICS_PORT` (default 8000). Scrape it with Prometheus and
visualize with the bundled Grafana dashboard (`docs/grafana/`):

```yaml
# prometheus.yml
scrape_configs:
  - job_name: "conductor"
    metrics_path: /metrics
    static_configs:
      - targets: ["<worker-host>:8000"]
```

See `docs/grafana/README.md` for dashboard import instructions.
