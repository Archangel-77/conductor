# Conductor Documentation

Conductor is a lightweight, async task queue for Python that runs on the
database you already have — no Redis, no message broker. PostgreSQL is the
reference backend; **MySQL/MariaDB** and **SQLite** are supported too, and all
three run the same parity-tested core flows.

## Getting Started

- [Installation](installation.md) — install, database setup, worker startup
- [Configuration](configuration.md) — environment variables & options
- [API Reference](api-reference.md) — TaskQueue, Worker, DeadLetterQueue, models

## Operating

- [Deployment](deployment.md) — Docker, Docker Compose, Kubernetes, systemd
- [Troubleshooting](troubleshooting.md) — common issues & debugging
- [Grafana Dashboard](grafana/README.md) — metrics visualization

## Repository

- [README](../README.md) — overview, quick start, comparison
- [Contributing](../CONTRIBUTING.md) — development setup, tests, code style
- [Code of Conduct](../CODE_OF_CONDUCT.md) — community standards
- [Security](../SECURITY.md) — reporting vulnerabilities
- [License](../LICENSE) — MIT
