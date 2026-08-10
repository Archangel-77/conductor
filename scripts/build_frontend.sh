#!/usr/bin/env bash
# Build the Conductor web dashboard frontend.
#
# Regenerates the committed `conductor/web/dist` bundle that is shipped
# inside the Python package (see `[tool.setuptools.package-data]`). Run this
# whenever the React sources in `conductor/web/src` change.
set -euo pipefail

cd "$(dirname "$0")/../conductor/web"
npm ci
npm run build
echo "Frontend built into conductor/web/dist."
