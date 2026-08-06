#!/usr/bin/env python3
"""Fail CI when a repository uses an unapproved dependency or unlocked toolchain."""

from __future__ import annotations

import json
import re
import sys
import tomllib
from pathlib import Path
from typing import Any, Iterable

ROOT = Path(__file__).resolve().parents[1]
POLICY_PATH = ROOT / "governance" / "approved-tools.json"

SKIP_PARTS = {
    ".git",
    ".next",
    ".terraform",
    ".venv",
    "build",
    "dist",
    "node_modules",
    "target",
    "vendor",
}

DEPENDENCY_SECTIONS = (
    "dependencies",
    "devDependencies",
    "optionalDependencies",
    "peerDependencies",
)


def fail(message: str) -> None:
    print(f"TOOLCHAIN ERROR: {message}", file=sys.stderr)


def should_skip(path: Path) -> bool:
    return any(part in SKIP_PARTS for part in path.parts)


def iter_files(name: str) -> Iterable[Path]:
    for path in ROOT.rglob(name):
        if not should_skip(path):
            yield path


def normalized_python_name(requirement: str) -> str:
    value = requirement.strip()

    if " @ " in value:
        value = value.split(" @ ", 1)[0]

    value = value.split(";", 1)[0]
    value = value.split("[", 1)[0]
    value = re.split(r"[<>=!~\s]", value, maxsplit=1)[0]

    return value.strip().lower().replace("_", "-")


def collect_allowed(section: dict[str, Any]) -> set[str]:
    allowed: set[str] = set()

    for key, value in section.items():
        if key == "forbidden":
            continue

        if isinstance(value, list):
            allowed.update(str(item).lower() for item in value)

    return allowed


def exact_npm_spec(spec: str) -> bool:
    value = spec.strip()

    allowed_nonregistry_prefixes = (
        "file:",
        "git+",
        "github:",
        "link:",
        "workspace:",
    )

    if value.startswith(allowed_nonregistry_prefixes):
        return True

    if value in {"*", "latest", "next"}:
        return False

    if value.startswith(("^", "~", ">", "<", "=")):
        return False

    if "||" in value or " - " in value:
        return False

    return bool(re.fullmatch(r"\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?", value))


def check_npm(policy: dict[str, Any], violations: list[str]) -> None:
    allowed = collect_allowed(policy)
    forbidden = {str(item).lower() for item in policy.get("forbidden", [])}

    for package_path in iter_files("package.json"):
        data = json.loads(package_path.read_text(encoding="utf-8"))

        lock_path = package_path.with_name("package-lock.json")
        if not lock_path.exists():
            violations.append(f"{package_path}: package-lock.json is missing")

        for section_name in DEPENDENCY_SECTIONS:
            dependencies = data.get(section_name, {})

            if not isinstance(dependencies, dict):
                continue

            for package_name, spec in dependencies.items():
                normalized = package_name.lower()

                if normalized in forbidden:
                    violations.append(
                        f"{package_path}: forbidden npm dependency {package_name}"
                    )
                    continue

                if normalized not in allowed:
                    violations.append(
                        f"{package_path}: unapproved npm dependency {package_name}"
                    )

                if not exact_npm_spec(str(spec)):
                    violations.append(
                        f"{package_path}: direct dependency {package_name} "
                        f"must use an exact approved version, found {spec!r}"
                    )


def nearest_uv_lock(pyproject_path: Path) -> Path | None:
    current = pyproject_path.parent

    while current == ROOT or ROOT in current.parents:
        candidate = current / "uv.lock"
        if candidate.exists():
            return candidate

        if current == ROOT:
            break

        current = current.parent

    return None


def check_python(policy: dict[str, Any], violations: list[str]) -> None:
    allowed = collect_allowed(policy)
    forbidden = {
        normalized_python_name(str(item))
        for item in policy.get("forbidden", [])
    }

    for pyproject_path in iter_files("pyproject.toml"):
        data = tomllib.loads(pyproject_path.read_text(encoding="utf-8"))
        project = data.get("project", {})

        if not isinstance(project, dict):
            continue

        runtime_requirements = project.get("dependencies", [])
        optional_requirements = project.get("optional-dependencies", {})

        all_requirements: list[str] = []

        if isinstance(runtime_requirements, list):
            all_requirements.extend(str(item) for item in runtime_requirements)

        if isinstance(optional_requirements, dict):
            for values in optional_requirements.values():
                if isinstance(values, list):
                    all_requirements.extend(str(item) for item in values)

        for requirement in all_requirements:
            package_name = normalized_python_name(requirement)

            if package_name in forbidden:
                violations.append(
                    f"{pyproject_path}: forbidden Python dependency {package_name}"
                )
                continue

            if package_name not in allowed:
                violations.append(
                    f"{pyproject_path}: unapproved Python dependency {package_name}"
                )

        if all_requirements and nearest_uv_lock(pyproject_path) is None:
            violations.append(
                f"{pyproject_path}: uv.lock is missing from the project hierarchy"
            )


def check_terraform(violations: list[str]) -> None:
    terraform_files = list(iter_files("*.tf"))

    if not terraform_files:
        return

    lockfiles = list(iter_files(".terraform.lock.hcl"))

    if not lockfiles:
        violations.append(
            "Terraform files exist but no .terraform.lock.hcl was found"
        )


def check_gradle(violations: list[str]) -> None:
    gradle_files = [
        *iter_files("build.gradle"),
        *iter_files("build.gradle.kts"),
    ]

    if not gradle_files:
        return

    wrappers = list(iter_files("gradle-wrapper.properties"))

    if not wrappers:
        violations.append(
            "Gradle project exists but gradle-wrapper.properties is missing"
        )


def main() -> int:
    if not POLICY_PATH.exists():
        fail(f"policy file not found: {POLICY_PATH}")
        return 2

    policy = json.loads(POLICY_PATH.read_text(encoding="utf-8"))
    violations: list[str] = []

    check_npm(policy["npm"], violations)
    check_python(policy["python"], violations)
    check_terraform(violations)
    check_gradle(violations)

    if violations:
        print("\n".join(f"- {item}" for item in sorted(set(violations))))
        print(f"\n{len(set(violations))} toolchain violation(s) found.")
        return 1

    print("Toolchain policy passed with zero violations.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
