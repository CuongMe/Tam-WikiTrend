from __future__ import annotations

import json
from pathlib import Path


def test_notebooks_are_valid_json_with_compilable_code_cells() -> None:
    notebooks = sorted((Path(__file__).parents[1] / "notebooks").glob("*.ipynb"))
    assert notebooks
    for path in notebooks:
        payload = json.loads(path.read_text(encoding="utf-8"))
        assert payload["nbformat"] == 4
        for cell in payload["cells"]:
            if cell["cell_type"] == "code":
                compile("".join(cell.get("source", [])), str(path), "exec")
