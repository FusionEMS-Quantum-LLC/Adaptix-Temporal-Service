#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${repo_root}"

python3 scripts/generate_tool_inventory.py
python3 scripts/verify_toolchain.py

if [[ -f package-lock.json ]]; then
  npm ci
  npm run typecheck --if-present
  npm run lint --if-present
  npm test --if-present
  npm run build --if-present
fi

if [[ -f pyproject.toml && -f uv.lock ]]; then
  uv sync --frozen --all-groups
  uv run ruff format --check .
  uv run ruff check .
  uv run mypy .
  uv run pytest
fi

if find . -name '*.tf' -not -path '*/.terraform/*' -print -quit | grep -q .; then
  terraform fmt -check -recursive
fi

if [[ -x ./gradlew ]]; then
  ./gradlew --no-daemon check
fi
