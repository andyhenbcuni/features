#!/usr/bin/env bash
set -euo pipefail

pip install ruff==0.4.1 && ruff format src/ tests/
