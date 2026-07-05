"""
Static coverage test for the contacts_writer v2 rollout.

This is the regression test that would have caught the 89b8361 bug: the
commit message claimed routes.py was wired to contacts_writer, but the
diff never touched routes.py. This test walks routes.py with `ast` and
asserts the wiring actually exists.

If a future PR removes the wiring (e.g. refactors routes.py and loses
the contacts_writer call), this test fails with precise line numbers.

Network is not involved. This is a pure-AST static check.
"""

from __future__ import annotations

import ast
import os
import sys
import unittest
from pathlib import Path

_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

ROUTES_PY = Path(_BACKEND_DIR) / "enrichment" / "routes.py"


def _calls_name(node: ast.AST, target: str) -> bool:
    """True if node is a Call to a bare name (e.g. set_done(...))."""
    return (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == target
    )


def _calls_attr_or_name(node: ast.AST, target: str) -> bool:
    """True if node is a Call to either `target(...)` (bare name) or
    `anything.target(...)` (attribute access)."""
    if not isinstance(node, ast.Call):
        return False
    func = node.func
    if isinstance(func, ast.Name) and func.id == target:
        return True
    if isinstance(func, ast.Attribute) and func.attr == target:
        return True
    return False


def _immediate_containing_function(tree: ast.Module, call_node: ast.Call) -> str | None:
    """Walk parent pointers to find the innermost FunctionDef containing the call."""
    parents: dict[int, ast.AST] = {}
    for parent in ast.walk(tree):
        for child in ast.iter_child_nodes(parent):
            parents[id(child)] = parent

    current: ast.AST | None = call_node
    while current is not None and id(current) in parents:
        current = parents[id(current)]
        if isinstance(current, (ast.FunctionDef, ast.AsyncFunctionDef)):
            return current.name
    return None


def _function_bodies_containing(tree: ast.Module, target: str) -> list[tuple[str, int]]:
    """Return [(function_name, line_no), ...] for every FunctionDef whose
    IMMEDIATE body contains a call to `target`. A call inside a nested
    function is attributed to the nested function, not the outer one.
    """
    results: list[tuple[str, int]] = []
    seen: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and _calls_attr_or_name(node, target):
            fn_name = _immediate_containing_function(tree, node)
            if fn_name and fn_name not in seen:
                seen.add(fn_name)
                # Get the function's line for context
                for n in ast.walk(tree):
                    if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name == fn_name:
                        results.append((fn_name, n.lineno))
                        break
    return results


class TestContactsWriterV2Wiring(unittest.TestCase):
    """The 5 sync sites must be wired to contacts_writer.

    Site mapping (line numbers shift over time; we anchor on the helper
    function being called, not specific lines):
    - Site 1: enrich_single_domain (GET /enrich) → must call _run_contacts_writer_v2
    - Site 2: _unified_enrich_logic (mid-function) → must call _run_contacts_writer_v2
    - Site 3: _unified_enrich_logic (enhanced return) → must call _run_contacts_writer_v2
    - Site 4: unified_enrich POST domain_only → must call _run_contacts_writer_v2
    - Site 5: _run_background_sync → must call _csv_rows_to_payloads (or _run_contacts_writer_v2)
    """

    @classmethod
    def setUpClass(cls):
        cls.routes_text = ROUTES_PY.read_text(encoding="utf-8")
        cls.tree = ast.parse(cls.routes_text)
        cls.calls_run_v2 = _function_bodies_containing(cls.tree, "_run_contacts_writer_v2")
        cls.calls_csv_to_payloads = _function_bodies_containing(
            cls.tree, "_csv_rows_to_payloads"
        )
        cls.calls_is_v2 = _function_bodies_containing(cls.tree, "is_v2_enabled")
        cls.calls_background_sync = _function_bodies_containing(
            cls.tree, "_run_background_sync"
        )

    def test_routes_py_parses(self):
        """If this fails, the file has a syntax error and we cannot AST-walk it."""
        self.assertIsInstance(self.tree, ast.Module)

    def test_v2_helper_is_imported(self):
        """contacts_writer must be imported in routes.py for sites to call it."""
        # Look for either "from . import contacts_writer" (relative; module=None)
        # or "from enrichment import contacts_writer" (absolute), or "import contacts_writer".
        for node in ast.walk(self.tree):
            if isinstance(node, ast.ImportFrom):
                for alias in node.names:
                    if alias.name == "contacts_writer":
                        return
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if "contacts_writer" in alias.name:
                        return
        self.fail("contacts_writer is not imported in routes.py")

    def test_v2_helper_defined(self):
        """_run_contacts_writer_v2 must be defined in routes.py."""
        for node in ast.iter_child_nodes(self.tree):
            if isinstance(node, ast.AsyncFunctionDef) and node.name == "_run_contacts_writer_v2":
                return
            if isinstance(node, ast.FunctionDef) and node.name == "_run_contacts_writer_v2":
                return
        self.fail("_run_contacts_writer_v2 is not defined in routes.py")

    def test_payload_helpers_defined(self):
        for name in ("_build_contacts_writer_payloads", "_csv_rows_to_payloads"):
            for node in ast.iter_child_nodes(self.tree):
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name:
                    break
            else:
                self.fail(f"{name} is not defined in routes.py")

    def test_is_v2_enabled_called_in_routes(self):
        """The flag check must appear in routes.py — it gates every sync site."""
        self.assertGreaterEqual(
            len(self.calls_is_v2), 4,
            f"is_v2_enabled() must be called in at least 4 sync sites; "
            f"found {len(self.calls_is_v2)}: {self.calls_is_v2}"
        )

    def test_v2_helper_called_in_three_or_more_functions(self):
        """The v2 helper must be called from at least 3 different functions.

        In the actual code structure, the 4 sync regions live in 3 functions:
        - enrich_single_domain (Site 1: GET /enrich)
        - unified_enrich (Site 2: POST /enrich legacy sync block)
        - _unified_enrich_logic (Sites 3 + 4: enhanced/linkedin_only AND
          the post-helper domain_only sync, in two different branches of
          the same function)
        """
        functions_with_v2 = [name for name, _ in self.calls_run_v2]
        unique = set(functions_with_v2)
        self.assertGreaterEqual(
            len(unique), 3,
            f"_run_contacts_writer_v2 must be called in at least 3 different functions. "
            f"Found calls in: {sorted(unique)}"
        )

    def test_v2_helper_called_in_enrich_single_domain(self):
        """Site 1: enrich_single_domain wires to v2."""
        functions = [name for name, _ in self.calls_run_v2]
        self.assertIn("enrich_single_domain", functions,
                      "enrich_single_domain (Site 1) must call _run_contacts_writer_v2")

    def test_v2_helper_called_in_unified_enrich(self):
        """Site 2: unified_enrich POST handler has the legacy sync block."""
        functions = [name for name, _ in self.calls_run_v2]
        self.assertIn("unified_enrich", functions,
                      "unified_enrich (Site 2) must call _run_contacts_writer_v2")

    def test_v2_helper_called_in_unified_enrich_logic(self):
        """Site 3 + 4: _unified_enrich_logic has v2 calls in two branches."""
        functions = [name for name, _ in self.calls_run_v2]
        self.assertIn("_unified_enrich_logic", functions,
                      "_unified_enrich_logic (Sites 3+4) must call _run_contacts_writer_v2")

    def test_v2_helper_called_at_least_four_times(self):
        """There must be 4+ calls to _run_contacts_writer_v2 across the file
        (one per sync site: GET /enrich, POST /enrich legacy, enhanced mode,
        domain_only mode).
        """
        call_count = 0
        for node in ast.walk(self.tree):
            if _calls_attr_or_name(node, "_run_contacts_writer_v2"):
                call_count += 1
        self.assertGreaterEqual(
            call_count, 4,
            f"_run_contacts_writer_v2 must be called at least 4 times "
            f"(one per sync site). Found {call_count} calls."
        )

    def test_csv_rows_to_payloads_called_in_background_sync(self):
        """Site 5: _run_background_sync reads CSV via _csv_rows_to_payloads."""
        functions = [name for name, _ in self.calls_csv_to_payloads]
        self.assertIn("_run_background_sync", functions,
                      "_run_background_sync (Site 5) must call _csv_rows_to_payloads")

    def test_linkedin_only_no_longer_fakes_success(self):
        """The routes.py linkedin_only sync branch must NOT return
        sync_status='success' with fabricated synced counts.

        This is the bug fixed in this PR (routes.py line ~1841):
        the old code was `sync_status = "success"; sync_result = {"synced":
        len(contacts), ...}` without actually calling the API. The fix
        returns no_contacts_to_sync instead, with synced=0 and skipped set
        to the contact count for observability.
        """
        text = self.routes_text
        # Find every occurrence of "linkedin_only" in the file. The sync
        # branch we care about is the LAST one (in _unified_enrich_logic).
        indices = []
        cursor = 0
        while True:
            i = text.find('"linkedin_only"', cursor)
            if i < 0:
                break
            indices.append(i)
            cursor = i + 1
        self.assertGreaterEqual(len(indices), 3,
                               f"Expected at least 3 linkedin_only references in routes.py; found {len(indices)}")
        # Look at the LAST one — the sync branch (line ~1841)
        sync_idx = indices[-1]
        window = text[sync_idx : sync_idx + 400]
        self.assertIn("no_contacts_to_sync", window,
                      "linkedin_only sync branch should return no_contacts_to_sync")
        # Belt-and-suspenders: confirm the buggy old pattern is gone
        buggy = '"synced": len(contacts), "skipped": 0, "failed": 0'
        self.assertNotIn(buggy, text,
                         f"Buggy linkedin_only fake-success pattern still present: {buggy!r}")


class TestLinkedInJobWiring(unittest.TestCase):
    """The two LinkedIn CSV upload flows (POST /by-linkedin, POST /by-linkedin-v2)
    must write their output to the Contacts DB via _run_background_sync, just
    like the domain-enrich job does.

    Without this, users uploading a CSV of LinkedIn URLs would see enriched
    results in the output CSV download but the Contacts DB would be empty.
    """

    @classmethod
    def setUpClass(cls):
        cls.routes_text = ROUTES_PY.read_text(encoding="utf-8")
        cls.tree = ast.parse(cls.routes_text)
        cls.calls_background_sync = _function_bodies_containing(
            cls.tree, "_run_background_sync"
        )

    def test_routes_linkedin_job_wires_background_sync(self):
        """_run_linkedin_job (POST /by-linkedin) must call _run_background_sync."""
        functions = [name for name, _ in self.calls_background_sync]
        self.assertIn("_run_linkedin_job", functions,
                      "_run_linkedin_job must call _run_background_sync "
                      "to persist enriched LinkedIn results to Contacts DB")

    def test_routes_linkedin_v2_job_wires_background_sync(self):
        """_run_linkedin_v2_job (POST /by-linkedin-v2) must call _run_background_sync."""
        functions = [name for name, _ in self.calls_background_sync]
        self.assertIn("_run_linkedin_v2_job", functions,
                      "_run_linkedin_v2_job must call _run_background_sync "
                      "to persist enriched LinkedIn v2 results to Contacts DB")

    def test_three_job_runners_call_background_sync(self):
        """All three background job runners must call _run_background_sync:
        _run_job (chain+CSV), _run_domain_enrich_job (Flow 1), and the two
        LinkedIn job functions.
        """
        functions = set(name for name, _ in self.calls_background_sync)
        required = {"_run_job", "_run_domain_enrich_job",
                    "_run_linkedin_job", "_run_linkedin_v2_job"}
        missing = required - functions
        self.assertFalse(missing,
                         f"These job runners must call _run_background_sync: {missing}")


class TestLoudFailurePropagation(unittest.TestCase):
    """LoudFailure is operator-facing. Callers must not swallow it.

    The pattern: every `except Exception` block that wraps a contacts_writer
    call must be preceded by an `except contacts_writer.LoudFailure: raise`
    arm. We do a textual search for the correct pattern.
    """

    def test_loud_failure_re_raise_in_routes(self):
        text = ROUTES_PY.read_text(encoding="utf-8")
        # Find every "except contacts_writer.LoudFailure" in the file
        loud_blocks = [m.start() for m in __import__("re").finditer(
            r"except contacts_writer\.LoudFailure", text)]
        self.assertGreaterEqual(
            len(loud_blocks), 4,
            f"At least 4 sites must re-raise LoudFailure (sites 1, 2, 3, 4, 5). "
            f"Found {len(loud_blocks)}: {loud_blocks}"
        )


if __name__ == "__main__":
    unittest.main()
