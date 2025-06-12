#!/usr/bin/env bash
set -euo pipefail

pip install --upgrade uv

uv sync --dev --python 3.12
uv run pre-commit run test-query-constructor -a
uv run pre-commit run test-actions -a
uv run pre-commit run test-managed-table -a
uv run pre-commit run test-scripts -a
uv run pre-commit run test-configs -a
uv run pre-commit run test-pipelines -a
