import importlib.util
import sys
import unittest
from pathlib import Path


SCRIPT = Path(__file__).with_name("workstream_graph.py")
SPEC = importlib.util.spec_from_file_location("workstream_graph", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader
sys.modules["workstream_graph"] = MODULE
SPEC.loader.exec_module(MODULE)


class GraphTests(unittest.TestCase):
    def plan(self):
        return {"graph_review_required": True, "root": {"stable_key": "source-root", "title": "Plan", "plan_revision": "sha"},
                "children": [{"key": "section-a", "title": "A", "line": 2}, {"key": "section-b", "title": "B", "line": 4}]}

    def test_review_is_required_before_child_creation(self):
        with self.assertRaises(MODULE.GraphReviewRequired):
            MODULE.build_operations(self.plan())

    def test_repeated_intake_updates_existing_graph_without_duplicates(self):
        first = MODULE.build_operations(self.plan(), accepted_keys={"section-a", "section-b"})
        second = MODULE.build_operations(
            self.plan(), existing_root={"identifier": "GEN-37"},
            existing_children=[{"identifier": "GEN-38", "stable_key": "section-a"}],
            accepted_keys={"section-a", "section-b"},
        )
        self.assertEqual([op["action"] for op in first], ["create_root", "create_child", "create_child"])
        self.assertEqual([op["action"] for op in second], ["update_root", "update_child", "create_child"])
        self.assertEqual([op["stable_key"] for op in second], ["source-root", "section-a", "section-b"])

    def test_unknown_review_key_fails_closed(self):
        with self.assertRaises(MODULE.GraphReviewRequired):
            MODULE.build_operations(self.plan(), accepted_keys={"missing"})


if __name__ == "__main__":
    unittest.main()
