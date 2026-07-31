# Security Policy

Thanks for helping keep Conductor and its users safe. This document describes
how to report a security vulnerability and what to expect.

## Supported Versions

Security fixes are applied to the latest release and, where practical, to the
most recent minor of the previous release.

| Version | Supported          |
| ------- | ------------------ |
| 0.1.x   | ✅ (current)       |
| < 0.1   | ❌                 |

## Reporting a Vulnerability

**Please do not open a public issue for security vulnerabilities.** Instead,
use GitHub's private vulnerability reporting:

1. Go to <https://github.com/Archangel-77/Conductor/security/advisories/new>.
2. Describe the vulnerability, including:
   - The affected version(s)
   - The component / entry point
   - A minimal reproduction (steps or a small script)
   - Impact and any suggested mitigation, if known

You can also reach the maintainers directly via the email listed on the project
homepage if you prefer.

### What happens next

- **Acknowledgment** — we'll confirm receipt within **48 hours**.
- **Triage** — we'll assess severity and impact within **5 business days**.
- **Fix & disclosure** — we'll work on a fix, prepare a patched release, and
  publish a security advisory on GitHub. We coordinate disclosure timing so
  users can upgrade before details are widely shared.

We ask that you give us a reasonable window to fix and release before publicly
disclosing the issue.

## Security Best Practices for Users

- Keep the package up to date; subscribe to GitHub release notifications.
- Never commit secrets (database credentials, API tokens) to repositories or
  image layers — use environment variables or a secret manager.
- Run the worker with a dedicated, least-privilege PostgreSQL role rather than a
  superuser.
- If you expose `/metrics` or `/health`, place them behind your network/firewall
  or a reverse proxy — the exporter itself performs no authentication.
- Check the license and dependencies of your own deployment, and re-verify
  supply-chain metadata when installing (`pip` hashes, pinned versions).

## Vulnerability Reporting SLA

- Confirmation of receipt: within **48 hours**
- Severity assessment: within **5 business days**
- Patched release for critical/high issues: as soon as a fix is verified
