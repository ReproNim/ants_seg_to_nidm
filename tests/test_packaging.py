"""Every third-party module the package imports is a declared requirement.

nibabel and requests used to arrive only through PyNIDM's own
dependencies, so a PyNIDM release that dropped them would break the import.
"""
import ast
import importlib.util
import os
import re
import sys
import sysconfig
from pathlib import Path

import pytest

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


def _stdlib_by_location(module):
    """Whether module is in the standard library, judged by where it is
    installed: built in, frozen, or under the interpreter's stdlib directory
    outside site-packages. Used before Python 3.10, which added
    sys.stdlib_module_names; a module that is not installed is not stdlib."""
    if module in sys.builtin_module_names:
        return True
    spec = importlib.util.find_spec(module)
    if spec is None:
        return False
    if spec.origin in ("built-in", "frozen"):
        return True
    locations = [spec.origin] if spec.origin else list(spec.submodule_search_locations or [])
    stdlib = os.path.realpath(sysconfig.get_paths()["stdlib"])
    return bool(locations) and all(
        os.path.realpath(p).startswith(stdlib + os.sep)
        and "site-packages" not in os.path.realpath(p).split(os.sep)
        for p in locations)


def _is_stdlib(module):
    names = getattr(sys, "stdlib_module_names", None)
    return module in names if names is not None else _stdlib_by_location(module)


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
                   if not _is_stdlib(m) and m not in ("ants_seg_to_nidm", "__future__")}
    assert third_party  # positive control: the package does import pandas, rdflib, ...
    declared = _install_requires()
    missing = sorted(m for m in third_party if DISTRIBUTION.get(m, m).lower() not in declared)
    assert not missing, f"imported but not in install_requires: {missing}"


@pytest.mark.skipif(not hasattr(sys, "stdlib_module_names"),
                    reason="needs Python 3.10+ to compare against sys.stdlib_module_names")
def test_the_stdlib_check_used_before_python_310_agrees():
    modules = _imported_top_level_modules() - {"ants_seg_to_nidm", "__future__"}
    assert {m for m in modules if _stdlib_by_location(m)} == \
        {m for m in modules if m in sys.stdlib_module_names}
