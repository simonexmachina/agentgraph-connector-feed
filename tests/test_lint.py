from __future__ import annotations

import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parents[1]


def test_ruff() -> None:
    for command in (("check", "."), ("format", "--check", ".")):
        result = subprocess.run(
            [sys.executable, "-m", "ruff", *command],
            cwd=PROJECT_ROOT,
            capture_output=True,
            check=False,
            text=True,
        )
        assert result.returncode == 0, result.stdout + result.stderr
