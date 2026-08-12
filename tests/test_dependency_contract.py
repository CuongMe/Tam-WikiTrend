from __future__ import annotations

import tomllib
from pathlib import Path


def test_direct_dependency_lock_matches_pyproject() -> None:
    root = Path(__file__).resolve().parents[1]
    project = tomllib.loads((root / "pyproject.toml").read_text(encoding="utf-8"))
    runtime_declared = set(project["project"]["dependencies"])
    development_declared = set(project["project"]["optional-dependencies"]["dev"])
    runtime_locked = {
        line.strip()
        for line in (root / "requirements.lock").read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.startswith("#")
    }
    development_lines = {
        line.strip()
        for line in (root / "requirements-dev.lock")
        .read_text(encoding="utf-8")
        .splitlines()
        if line.strip() and not line.startswith("#")
    }

    assert runtime_locked == runtime_declared
    assert development_lines == {"-r requirements.lock", *development_declared}
