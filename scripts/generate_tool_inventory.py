#!/usr/bin/env python3
"""Generate an evidence-only dependency inventory without approving anything."""

from __future__ import annotations

import json
import tomllib
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "reports" / "toolchain-inventory.json"

SKIP = {
    ".git",
    ".next",
    ".terraform",
    ".venv",
    "build",
    "dist",
    "node_modules",
    "target",
}


def skipped(path: Path) -> bool:
    return any(part in SKIP for part in path.parts)


def relative(path: Path) -> str:
    return str(path.relative_to(ROOT))


def read_package_json(path: Path) -> dict[str, Any]:
    data = json.loads(path.read_text(encoding="utf-8"))

    return {
        "path": relative(path),
        "name": data.get("name"),
        "version": data.get("version"),
        "packageManager": data.get("packageManager"),
        "engines": data.get("engines", {}),
        "dependencies": data.get("dependencies", {}),
        "devDependencies": data.get("devDependencies", {}),
        "optionalDependencies": data.get("optionalDependencies", {}),
    }


def read_pyproject(path: Path) -> dict[str, Any]:
    data = tomllib.loads(path.read_text(encoding="utf-8"))
    project = data.get("project", {})

    return {
        "path": relative(path),
        "name": project.get("name"),
        "version": project.get("version"),
        "requiresPython": project.get("requires-python"),
        "dependencies": project.get("dependencies", []),
        "optionalDependencies": project.get("optional-dependencies", {}),
    }


def main() -> None:
    npm = [
        read_package_json(path)
        for path in ROOT.rglob("package.json")
        if not skipped(path)
    ]

    python = [
        read_pyproject(path)
        for path in ROOT.rglob("pyproject.toml")
        if not skipped(path)
    ]

    report = {
        "npmProjects": sorted(npm, key=lambda item: item["path"]),
        "pythonProjects": sorted(python, key=lambda item: item["path"]),
        "terraformLocks": sorted(
            relative(path)
            for path in ROOT.rglob(".terraform.lock.hcl")
            if not skipped(path)
        ),
        "gradleWrappers": sorted(
            relative(path)
            for path in ROOT.rglob("gradle-wrapper.properties")
            if not skipped(path)
        ),
    }

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    print(f"Wrote {OUTPUT}")


if __name__ == "__main__":
    main()
