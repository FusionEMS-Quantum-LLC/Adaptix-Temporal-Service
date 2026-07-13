#!/usr/bin/env bash
###############################################################################
# AdaptixCore canonical CI script
#
# ONE source of truth for what a passing CI run means. Called BOTH:
#   - locally by .githooks/pre-push (blocks push on failure)
#   - remotely by Adaptix-Governance/standard-ci.yml on CodeBuild runners
#
# That guarantees local == remote, byte-identical. Zero drift.
#
# Usage:
#   scripts/local-ci.sh <runtime> [mode]
#       runtime: python | node | mixed
#       mode:    quick | pr | full | deploy   (default: pr)
#
# Tiered validation (Adaptix standard):
#   quick  — fast local/pre-push gate. Deterministic, < ~60-90s. Runs:
#              format check, lint, secrets scan on the pushed diff,
#              targeted/fast typecheck if cheap. NO full test suite, NO heavy
#              repo-wide mypy, NO integration/db/network/e2e tests, NO builds.
#   pr     — full PR validation (authoritative). lint, format, typecheck,
#              unit tests, secrets, dependency/security audit. (default)
#   full   — main/release validation. Same as pr today; reserved for adding
#              contract/build verification without touching pre-push speed.
#   deploy — deployment/runtime validation. Reserved (image build, migration
#              check, deploy validation, health/smoke) — runs in AWS Pipeline.
#
# Pre-push calls `quick`. CodeBuild PR calls `pr`. CodeBuild main calls `full`.
# AWS Pipeline calls `deploy`. Coverage is NOT removed — expensive validation
# moves from pre-push to CodeBuild/Pipeline, which remain the authoritative,
# fail-closed gates.
#
# Optional environment:
#   ADAPTIX_CI_SKIP=<comma-list>   skip named checks (lint,format,typecheck,test,secrets,deps)
#   ADAPTIX_CI_BACKEND=<path>      python backend dir (default: backend/, falls back to .)
#   ADAPTIX_CI_FAST=1              skip slow tests (pytest -k 'not slow', npm test -- --bail)
#
# Exit codes:
#   0   all checks passed
#   1   one or more checks failed
#   2   misuse (bad runtime/mode arg, missing tools)
#
# Output: each check prints `[CHECK <name>] <result>` so logs are greppable
# both locally and in CodeBuild.
###############################################################################
set -uo pipefail

readonly RUNTIME="${1:-}"
readonly MODE="${2:-pr}"
readonly REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"

if [[ -z "$RUNTIME" ]]; then
  echo "usage: local-ci.sh <python|node|mixed> [quick|pr|full|deploy]" >&2
  exit 2
fi

case "$MODE" in
  quick|pr|full|deploy) ;;
  *)
    echo "unknown mode: $MODE" >&2
    echo "valid: quick | pr | full | deploy" >&2
    exit 2
    ;;
esac

# In quick mode, the test + dependency-audit checks are expensive and belong in
# CodeBuild; mark them skipped so quick stays fast and deterministic.
QUICK_SKIP="test,deps"
if [[ "$MODE" == "quick" ]]; then
  if [[ -n "${ADAPTIX_CI_SKIP:-}" ]]; then
    ADAPTIX_CI_SKIP="${ADAPTIX_CI_SKIP},${QUICK_SKIP}"
  else
    ADAPTIX_CI_SKIP="${QUICK_SKIP}"
  fi
fi

# ─── helpers ────────────────────────────────────────────────────────────────

FAILED_CHECKS=()
PASSED_CHECKS=()
SKIPPED_CHECKS=()

is_skipped() {
  local check="$1"
  [[ ",${ADAPTIX_CI_SKIP:-}," == *",${check},"* ]]
}

run_check() {
  local name="$1"
  shift
  if is_skipped "$name"; then
    echo "[CHECK ${name}] SKIPPED (ADAPTIX_CI_SKIP)"
    SKIPPED_CHECKS+=("$name")
    return 0
  fi
  echo "[CHECK ${name}] running: $*"
  if "$@"; then
    echo "[CHECK ${name}] PASS"
    PASSED_CHECKS+=("$name")
    return 0
  else
    local rc=$?
    echo "[CHECK ${name}] FAIL (exit $rc)"
    FAILED_CHECKS+=("$name")
    return 1
  fi
}

# Find first existing dir from a list
first_existing_dir() {
  for d in "$@"; do
    if [[ -d "$REPO_ROOT/$d" ]]; then
      echo "$d"
      return 0
    fi
  done
  echo "."
}

# ─── runtime: python ────────────────────────────────────────────────────────

run_python() {
  local backend="${ADAPTIX_CI_BACKEND:-$(first_existing_dir backend core/backend src .)}"
  echo "[python] backend dir: $backend"

  if ! command -v ruff >/dev/null 2>&1; then
    echo "[CHECK lint] tool missing: ruff (install: pip install ruff)" >&2
    FAILED_CHECKS+=("lint")
  else
    run_check lint    bash -c "cd '$REPO_ROOT/$backend' && ruff check ."
    run_check format  bash -c "cd '$REPO_ROOT/$backend' && ruff format --check ."
  fi

  if command -v mypy >/dev/null 2>&1; then
    if [[ -f "$REPO_ROOT/$backend/mypy.ini" || -f "$REPO_ROOT/$backend/pyproject.toml" ]]; then
      if [[ "$MODE" == "quick" ]]; then
        # Fast typecheck: only the changed .py files under the backend dir.
        # Full repo-wide mypy is authoritative and runs in CodeBuild (pr/full).
        local diff_base
        diff_base="$(git -C "$REPO_ROOT" rev-parse --abbrev-ref --symbolic-full-name '@{push}' 2>/dev/null && echo '@{push}' || echo 'HEAD~1')"
        local -a changed_py=()
        mapfile -t changed_py < <(
          git -C "$REPO_ROOT" diff --name-only --diff-filter=ACMR "$diff_base" HEAD 2>/dev/null \
            | grep -E '\.py$' \
            | sed -n "s#^${backend}/##p"
        )
        if (( ${#changed_py[@]} > 0 )); then
          run_check typecheck bash -c "cd '$REPO_ROOT/$backend' && mypy --ignore-missing-imports --follow-imports=silent \"\$@\"" _ "${changed_py[@]}"
        else
          echo "[CHECK typecheck] SKIPPED (quick: no changed .py files under $backend)"
          SKIPPED_CHECKS+=("typecheck")
        fi
      else
        run_check typecheck bash -c "cd '$REPO_ROOT/$backend' && mypy --ignore-missing-imports ."
      fi
    fi
  fi

  if command -v pytest >/dev/null 2>&1; then
    local pytest_args="-q --no-header --tb=short -x"
    if [[ "${ADAPTIX_CI_FAST:-}" == "1" ]]; then
      pytest_args="$pytest_args -k 'not slow and not integration and not e2e'"
    fi
    run_check test bash -c "cd '$REPO_ROOT/$backend' && pytest $pytest_args"
  else
    echo "[CHECK test] SKIPPED (pytest not installed)"
    SKIPPED_CHECKS+=("test")
  fi

  if command -v pip-audit >/dev/null 2>&1; then
    # pip-audit is REPORT-ONLY, matching the canonical CI security-scan buildspec.
    # Two reasons it must not block locally: (1) --strict + --skip-editable are
    # mutually defeating (--strict fails *because* --skip-editable skipped a
    # private pkg like adaptix-contracts, not on PyPI); (2) it audits the shared
    # dev environment, which carries many editable private service packages
    # (adaptix-*-service) that cannot be audited. Per founder CI philosophy,
    # transitive dependency CVEs are outside a single PR's control and gating on
    # them makes zero-failures impossible. Findings are printed for visibility;
    # secrets (gitleaks) + app-code security (bandit) remain blocking elsewhere.
    run_check deps bash -c "cd '$REPO_ROOT/$backend' && { pip-audit --skip-editable || echo 'WARN: pip-audit findings (report-only — matches CI security-scan)'; }"
  fi
}

# ─── runtime: node ──────────────────────────────────────────────────────────

run_node() {
  if [[ ! -f "$REPO_ROOT/package.json" ]]; then
    echo "[node] no package.json at repo root, looking in backend/"
    if [[ -f "$REPO_ROOT/backend/package.json" ]]; then
      cd "$REPO_ROOT/backend"
    else
      echo "[node] no package.json found, nothing to check"
      return 0
    fi
  fi

  if [[ ! -d "node_modules" ]]; then
    run_check install npm ci --no-audit --no-fund
  fi

  # Detect available scripts.
  # Lint policy:
  #   pr/full (CodeBuild, authoritative): zero-warnings — `npm run lint -- --max-warnings=0`
  #     (canon 2026-06-09). Errors AND warnings block.
  #   quick (pre-push, fast local gate): `npm run lint` — fails on errors only,
  #     allowing the repo's own eslint.config intentional `warn`-level debt
  #     (e.g. react-hooks@7.x set-state-in-effect, which the founder explicitly
  #     set to warn-not-block with a phased cleanup plan). This keeps local
  #     pre-push from being STRICTER than the authoritative CodeBuild gate
  #     (which runs plain `npm run lint`) and unblocks pushes that CodeBuild
  #     would accept. New lint ERRORS still block pre-push.
  if npm run | grep -qE "^\s*lint\b"; then
    if [[ "$MODE" == "quick" ]]; then
      run_check lint npm run lint
    else
      run_check lint bash -c "npm run lint -- --max-warnings=0"
    fi
  fi
  if npm run | grep -qE "^\s*format:check\b|^\s*prettier"; then
    run_check format npm run format:check 2>/dev/null || npm run prettier -- --check
  fi
  if npm run | grep -qE "^\s*typecheck\b|^\s*tsc\b"; then
    run_check typecheck npm run typecheck
  elif command -v npx >/dev/null 2>&1 && [[ -f "tsconfig.json" ]]; then
    run_check typecheck npx --no -- tsc --noEmit
  fi
  if npm run | grep -qE "^\s*test\b"; then
    if [[ "${ADAPTIX_CI_FAST:-}" == "1" ]]; then
      run_check test bash -c "CI=true npm test -- --bail"
    else
      run_check test bash -c "CI=true npm test"
    fi
  fi

  if command -v npm >/dev/null 2>&1; then
    # REPORT-ONLY (matches python pip-audit + CI security-scan philosophy):
    # transitive dependency CVEs are outside a single PR's control; gating
    # pushes on them makes zero-failures impossible. Findings printed, not blocking.
    run_check deps bash -c "{ npm audit --audit-level=high || echo 'WARN: npm audit findings (report-only — matches CI security-scan)'; }"
  fi
}

# ─── runtime: mixed (python + node) ─────────────────────────────────────────

run_mixed() {
  run_python
  run_node
}

# ─── secrets (every runtime) ────────────────────────────────────────────────

run_secrets() {
  if command -v gitleaks >/dev/null 2>&1; then
    if [[ "$MODE" == "quick" ]]; then
      # Pre-push runs after commit, so --staged finds nothing. Scan the commits
      # being pushed (@{push}..HEAD), falling back to the last commit.
      local range
      range="$(git -C "$REPO_ROOT" rev-parse --abbrev-ref --symbolic-full-name @{push} >/dev/null 2>&1 \
               && echo '@{push}..HEAD' || echo 'HEAD~1..HEAD')"
      run_check secrets bash -c "cd '$REPO_ROOT' && gitleaks detect --no-banner --redact --log-opts='$range'"
    else
      run_check secrets gitleaks protect --no-banner --redact --staged
    fi
  else
    echo "[CHECK secrets] SKIPPED (gitleaks not installed — recommend: brew install gitleaks or go install github.com/zricethezav/gitleaks/v8@latest)"
    SKIPPED_CHECKS+=("secrets")
  fi
}

# ─── main ───────────────────────────────────────────────────────────────────

echo "==============================================================="
echo "AdaptixCore canonical CI"
echo "  repo:    $REPO_ROOT"
echo "  runtime: $RUNTIME"
echo "  mode:    $MODE"
if [[ "$MODE" == "quick" ]]; then
  echo "           (fast pre-push gate — full validation runs in CodeBuild/Pipeline)"
fi
echo "  skip:    ${ADAPTIX_CI_SKIP:-<none>}"
echo "  fast:    ${ADAPTIX_CI_FAST:-0}"
echo "==============================================================="

case "$RUNTIME" in
  python) run_python ;;
  node)   run_node   ;;
  mixed)  run_mixed  ;;
  *)
    echo "unknown runtime: $RUNTIME" >&2
    echo "valid: python | node | mixed" >&2
    exit 2
    ;;
esac

run_secrets

echo "==============================================================="
echo "Summary"
echo "  passed:  ${#PASSED_CHECKS[@]}  (${PASSED_CHECKS[*]:-})"
echo "  skipped: ${#SKIPPED_CHECKS[@]} (${SKIPPED_CHECKS[*]:-})"
echo "  failed:  ${#FAILED_CHECKS[@]}  (${FAILED_CHECKS[*]:-})"
echo "==============================================================="

if (( ${#FAILED_CHECKS[@]} > 0 )); then
  echo ""
  echo "PUSH BLOCKED: ${#FAILED_CHECKS[@]} check(s) failed: ${FAILED_CHECKS[*]}"
  echo ""
  echo "Fix the failures above and try again. To skip a specific check"
  echo "(NOT recommended for security/deps), set ADAPTIX_CI_SKIP=name1,name2"
  exit 1
fi

echo ""
echo "All checks passed. Safe to push."
exit 0
