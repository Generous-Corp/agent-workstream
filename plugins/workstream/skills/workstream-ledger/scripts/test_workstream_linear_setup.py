import unittest

from workstream_linear import LinearTransportError
from workstream_linear_setup import inspect_route


class FakeClient:
    def execute(self, query, _variables):
        if "WorkstreamLinearViewer" in query:
            return {"viewer": {"id": "user-1", "name": "Ada"}}
        if "WorkstreamLinearTeams" in query:
            return {"teams": {"nodes": [
                {
                    "id": "team-1",
                    "key": "APP",
                    "name": "Application",
                    "organization": {"id": "workspace-1", "name": "Example"},
                }
            ], "pageInfo": {"hasNextPage": False, "endCursor": None}}}
        if "WorkstreamLinearProjects" in query:
            return {"projects": {"nodes": [
                {
                    "id": "project-1",
                    "name": "Launch",
                    "teams": {"nodes": [{"id": "team-1", "key": "APP", "name": "Application"}]},
                },
                {
                    "id": "project-2",
                    "name": "Other",
                    "teams": {"nodes": [{"id": "team-2", "key": "OPS", "name": "Operations"}]},
                },
            ], "pageInfo": {"hasNextPage": False, "endCursor": None}}}
        raise AssertionError("unexpected query")


class LinearSetupTests(unittest.TestCase):
    def test_lists_route_inventory_without_a_selection(self):
        result = inspect_route(FakeClient())
        self.assertEqual(result["workspaces"], [{"id": "workspace-1", "name": "Example"}])
        self.assertEqual(result["teams"][0]["id"], "team-1")
        self.assertFalse(result["selection"]["valid"])

    def test_validates_team_project_and_derives_workspace(self):
        result = inspect_route(FakeClient(), team_id="team-1", project_id="project-1")
        self.assertEqual(result["selection"], {
            "valid": True,
            "workspace_id": "workspace-1",
            "team_id": "team-1",
            "project_id": "project-1",
        })

    def test_rejects_cross_team_project(self):
        with self.assertRaisesRegex(LinearTransportError, "not associated"):
            inspect_route(FakeClient(), team_id="team-1", project_id="project-2")


if __name__ == "__main__":
    unittest.main()
