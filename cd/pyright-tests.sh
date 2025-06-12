#!/usr/bin/env bash
set -euo pipefail

pip install --upgrade uv
uv sync --dev --python 3.12
uv run pre-commit run ruff -a
