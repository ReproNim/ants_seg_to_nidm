"""Every third-party module the package imports is a declared requirement.

nibabel and requests used to arrive only through PyNIDM's own
dependencies, so a PyNIDM release that dropped them would break the import.
"""
import ast
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
# import name -> distribution name, where they differ
DISTRIBUTION = {"nidm": "pynidm"}


def _install_requires():
    tree = ast.parse((ROOT / "setup.py").read_text())
    for node in ast.walk(tree):
        if isinstance(node, ast.keyword) and node.arg == "install_requires":
            names = ast.literal_eval(node.value)
            return {re.split(r"[\[<>=~! ]", name, maxsplit=1)[0].lower() for name in names}
    raise AssertionError("setup.py has no install_requires")


def _imported_top_level_modules():
    modules = set()
    for source in (ROOT / "ants_seg_to_nidm").glob("*.py"):
        for node in ast.walk(ast.parse(source.read_text())):
            if isinstance(node, ast.Import):
                modules.update(alias.name.split(".")[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
                modules.add(node.module.split(".")[0])
    return modules


def test_third_party_imports_are_declared():
    third_party = {m for m in _imported_top_level_modules()
                   if m not in sys.stdlib_module_names and m not in ("ants_seg_to_nidm", "__future__")}
    assert third_party  # positive control: the package does import pandas, rdflib, ...
    declared = _install_requires()
    missing = sorted(m for m in third_party if DISTRIBUTION.get(m, m).lower() not in declared)
    assert not missing, f"imported but not in install_requires: {missing}"
