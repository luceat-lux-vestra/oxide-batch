#!/usr/bin/env python3
"""Adversarial offline tests for the research-only Cargo metadata doc-link hints."""
import copy
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import research_rustdoc_link_hints as h


def pkg(name, version="1.0.0", *, library=True, ident=None):
    return {
        "id": ident or f"registry+test/{name}/{version}",
        "name": name,
        "version": version,
        "targets": [{"name": name.replace("-", "_"), "kind": ["lib"]}] if library else [
            {"name": name, "kind": ["bin"]},
        ],
    }


def graph(*packages):
    return {"packages": list(packages), "resolve": {
        "nodes": [{"id": p["id"]} for p in packages],
    }}


class HintSafety(unittest.TestCase):
    def root(self, td):
        base = Path(td) / "cargo-target"
        base.mkdir()
        return base / "doc"

    def test_distinct_names_create_a_new_complete_inventory(self):
        with tempfile.TemporaryDirectory() as d:
            root = self.root(d)
            result = h.generate(graph(pkg("first"), pkg("second")), root)
            self.assertEqual(result["crate_names"], 2)
            self.assertEqual((root / "first/index.html").read_bytes(), h.PLACEHOLDER)
            self.assertEqual((root / "second/index.html").read_bytes(), h.PLACEHOLDER)
            self.assertFalse(result["documentation_is_real"])

    def test_known_duplicate_version_names_are_reported(self):
        with tempfile.TemporaryDirectory() as d:
            root = self.root(d)
            result = h.generate(
                graph(pkg("sha2", "0.10.9"), pkg("sha2", "0.11.0")),
                root,
            )
            self.assertEqual(result["reviewed_collisions"], {"sha2": ["0.10.9", "0.11.0"]})
            self.assertEqual(result["crate_names"], 1)

    def test_unknown_collision_fails_before_any_write(self):
        with tempfile.TemporaryDirectory() as d:
            root = self.root(d)
            with self.assertRaisesRegex(h.HintError, "unreviewed duplicate"):
                h.generate(graph(pkg("future", "1.0.0"), pkg("future", "2.0.0")), root)
            self.assertFalse(root.exists())

    def test_known_duplicate_changes_are_not_silently_accepted(self):
        with tempfile.TemporaryDirectory() as d:
            root = self.root(d)
            with self.assertRaisesRegex(h.HintError, "unreviewed duplicate"):
                h.generate(graph(pkg("sha2", "0.10.9"), pkg("sha2", "0.12.0")), root)
            self.assertFalse(root.exists())

    def test_existing_documents_are_never_overwritten(self):
        with tempfile.TemporaryDirectory() as d:
            root = self.root(d)
            (root / "serde_json").mkdir(parents=True)
            sentinel = root / "serde_json" / "index.html"
            sentinel.write_text("DO NOT REPLACE")
            with self.assertRaisesRegex(h.HintError, "existing documents"):
                h.generate(graph(pkg("serde_json")), root)
            self.assertEqual(sentinel.read_text(), "DO NOT REPLACE")

    def test_refuse_symlink_document_root(self):
        with tempfile.TemporaryDirectory() as d:
            root = self.root(d)
            target = Path(d) / "target"
            target.mkdir()
            root.symlink_to(target, target_is_directory=True)
            with self.assertRaises(h.HintError):
                h.generate(graph(pkg("serde_json")), root)
            self.assertEqual(list(target.iterdir()), [])

    def test_refuse_symlink_parent(self):
        with tempfile.TemporaryDirectory() as d:
            target = Path(d) / "target"
            target.mkdir()
            link = Path(d) / "target-link"
            link.symlink_to(target, target_is_directory=True)
            with self.assertRaisesRegex(h.HintError, "symlink"):
                h.generate(graph(pkg("serde_json")), link / "doc")

    def test_reject_unsafe_crate_names(self):
        for name in ["../escape", "/escape", "hyphen-name", "", "with space"]:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as d:
                root = self.root(d)
                broken = pkg("valid")
                broken["targets"][0]["name"] = name
                with self.assertRaisesRegex(h.HintError, "unsafe or invalid"):
                    h.generate(graph(broken), root)
                self.assertFalse(root.exists())

    def test_mismatched_resolve_graph_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            root = self.root(d)
            data = graph(pkg("foo"), pkg("bar"))
            data["resolve"]["nodes"] = [{"id": data["packages"][0]["id"]}]
            with self.assertRaisesRegex(h.HintError, "resolved package ids"):
                h.generate(data, root)
            self.assertFalse(root.exists())

    def test_missing_metadata_and_no_library_rejected(self):
        for data in [{}, [], graph(pkg("binary", library=False))]:
            with self.subTest(data=data), tempfile.TemporaryDirectory() as d:
                root = self.root(d)
                with self.assertRaises(h.HintError):
                    h.generate(data, root)
                self.assertFalse(root.exists())

    def test_metadata_validation_precedes_filesystem_modification(self):
        with tempfile.TemporaryDirectory() as d:
            root = self.root(d)
            bad = graph(pkg("bad"))
            bad["packages"][0]["targets"][0]["kind"] = "lib"
            with self.assertRaises(h.HintError):
                h.generate(bad, root)
            self.assertFalse(root.exists())

    def test_report_determinism_independent_of_package_order(self):
        data = graph(pkg("bravo"), pkg("alpha"), pkg("delta"))
        with tempfile.TemporaryDirectory() as d:
            root = self.root(d)
            first = h.generate(data, root)
        with tempfile.TemporaryDirectory() as d:
            root = self.root(d)
            second = h.generate(graph(*reversed(data["packages"])), root)
        self.assertEqual(first, second)

    def test_injected_write_error_cleans_staging_and_does_not_publish(self):
        with tempfile.TemporaryDirectory() as d:
            root = self.root(d)
            original = Path.write_bytes
            def error_on_placeholder(path, data):
                if path.name == "index.html":
                    raise OSError("disk failure")
                return original(path, data)
            with mock.patch.object(Path, "write_bytes", error_on_placeholder):
                with self.assertRaisesRegex(OSError, "disk failure"):
                    h.generate(graph(pkg("alpha")), root)
            self.assertFalse(root.exists())
            self.assertFalse(any(root.parent.iterdir()))


if __name__ == "__main__":
    unittest.main(verbosity=2)
