"""Structural validation of the shipped example notebooks.

Full ``nbval`` execution is deliberately **not** used here. The example
notebook downloads external datasets and drives a live Spark/Flink cluster
whose executor environment is only available inside a BiJuTy-managed SLURM
allocation, so top-to-bottom execution is neither hermetic nor deterministic
and would defeat the fast, offline guarantees of the rest of the suite.

Instead these tests provide the cheap, execution-free half of notebook
regression checking: the notebook must be valid nbformat, contain well-formed
cells, hold syntactically valid Python (when it has no IPython magics), and
must not have committed a failing cell output.
"""

from __future__ import annotations

import ast
import json
from pathlib import Path

import pytest

NOTEBOOK_DIR = Path(__file__).resolve().parents[1] / "example"

IPYTHON_MAGIC_PREFIXES = ("%", "!", "?")


def _notebook_paths() -> list[Path]:
    if not NOTEBOOK_DIR.is_dir():
        return []
    return sorted(NOTEBOOK_DIR.glob("*.ipynb"))


def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _has_ipython_magic(source: str) -> bool:
    return any(
        line.lstrip().startswith(IPYTHON_MAGIC_PREFIXES)
        for line in source.splitlines()
    )


@pytest.mark.parametrize("path", _notebook_paths(), ids=lambda p: p.name)
class TestExampleNotebooks:
    def test_is_valid_nbformat_json(self, path: Path):
        notebook = _load(path)

        assert notebook.get("nbformat") == 4
        assert isinstance(notebook.get("cells"), list)
        assert notebook["cells"], "notebook has no cells"

    def test_cells_are_well_formed(self, path: Path):
        notebook = _load(path)

        for index, cell in enumerate(notebook["cells"]):
            assert cell.get("cell_type") in {"code", "markdown", "raw"}, index
            assert isinstance(cell.get("source"), list), index

    def test_code_cells_compile(self, path: Path):
        notebook = _load(path)

        for index, cell in enumerate(notebook["cells"]):
            if cell["cell_type"] != "code":
                continue
            source = "".join(cell["source"])
            if _has_ipython_magic(source):
                continue
            try:
                ast.parse(source)
            except SyntaxError as error:
                pytest.fail(
                    f"{path.name} cell {index} is not valid Python: {error}")

    def test_no_committed_error_outputs(self, path: Path):
        notebook = _load(path)

        for index, cell in enumerate(notebook["cells"]):
            for output in cell.get("outputs", []):
                assert output.get("output_type") != "error", (
                    f"{path.name} cell {index} contains a stored error output")
