#!/usr/bin/env python3
import hashlib
import io
import json
import unittest
import uuid
from unittest import mock

import workstream_child_dependencies as dependency_module
from workstream_child_dependencies import (
    ChildDependencyError, LinearChildDependencyAdapter,
    dependency_relation_id, reduce_dependency_readback,
)
from workstream_delta import Delta, event_id_for
from workstream_linear import LinearTransportError
from workstream_linear_events import encode_event_comment
from workstream_linear_projection import (
    build_projection_event, encode_projection_comment, projection_slot_id,
)


PLAN = "a" * 64
ROOT_ID = "10000000-0000-4000-8000-000000000001"
CHILD_A_ID = "20000000-0000-4000-8000-000000000001"
CHILD_B_ID = "20000000-0000-4000-8000-000000000002"
CHILD_C_ID = "20000000-0000-4000-8000-000000000003"
AUTHORITY = {
    "workspace_id": "workspace", "team_id": "team", "project_id": "project",
    "root_issue_id": ROOT_ID, "root_identifier": "GEN-37",
}
A = {"issue_id": CHILD_A_ID, "identifier": "GEN-43"}
B = {"issue_id": CHILD_B_ID, "identifier": "GEN-44"}
C = {"issue_id": CHILD_C_ID, "identifier": "GEN-45"}
FRONTIER = {
    "material_revision": 0, "projection_revision": 0, "graph_revision": 0,
    "graph_sha256": hashlib.sha256(b"[]").hexdigest(),
}


def graph_sha256(relations):
    payload = json.dumps(
        relations, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


class FakeLinear:
    def __init__(self):
        self.calls = []
        self.comments = {"GEN-37": []}
        self.relations = []
        self.children = [self.child(A), self.child(B), self.child(C)]
        self.root_reads = 0
        self.before_root_read = None
        self.comment_reads = 0
        self.before_comment_read = None
        self.lose_next_create_response = False
        self.page_size = 250

    @staticmethod
    def team():
        return {"id": "team", "organization": {"id": "workspace"}}

    @classmethod
    def child(cls, identity, *, parent_id=ROOT_ID, project_id="project"):
        return {
            "id": identity["issue_id"], "identifier": identity["identifier"],
            "title": identity["identifier"], "description": f"Plan revision: {PLAN}",
            "url": "https://linear.test/child", "updatedAt": "now",
            "parent": {"id": parent_id, "identifier": "GEN-37"},
            "project": {"id": project_id}, "team": cls.team(), "assignee": None,
            "state": {"id": "todo", "name": "Todo", "type": "unstarted"},
            "comments": {"nodes": [], "pageInfo": {"hasNextPage": False, "endCursor": None}},
        }

    @classmethod
    def root(cls):
        return {
            "id": ROOT_ID, "identifier": "GEN-37", "title": "Root",
            "description": f"Plan revision: {PLAN}\nLedger revision: 0",
            "url": "https://linear.test/root", "updatedAt": "now", "parent": None,
            "project": {"id": "project"}, "team": cls.team(), "assignee": None,
            "state": {"id": "started", "name": "In Progress", "type": "started"},
        }

    def identity_by_id(self, issue_id):
        child = next(item for item in self.children if item["id"] == issue_id)
        return {"issue_id": child["id"], "identifier": child["identifier"]}

    def native_relation(self, blocker=A, blocked=B, *, relation_id=None, relation_type="blocks"):
        return {
            "id": relation_id or dependency_relation_id(
                authority=AUTHORITY, blocker=blocker, blocked=blocked,
            ),
            "type": relation_type, "archivedAt": None,
            "issue": self.child(blocker), "relatedIssue": self.child(blocked),
        }

    def add_material(self):
        payload = {"changes": [{"kind": "requirement", "text": "concurrent"}]}
        delta = Delta(
            event_id_for("GEN-37", "boundary", payload, 0), "GEN-37", "boundary",
            "user_turn", payload, 0, "2026-08-29T00:00:00Z",
        )
        self.comments["GEN-37"].append({
            "id": str(uuid.uuid4()), "body": encode_event_comment(delta),
            "createdAt": "2026-08-29T00:00:01Z",
            "updatedAt": "2026-08-29T00:00:01Z",
        })

    def add_projection(self):
        authority = {key: AUTHORITY[key] for key in (
            "workspace_id", "team_id", "project_id", "root_issue_id",
        )}
        event = build_projection_event(
            workstream_id="GEN-37", kind="provenance", key="concurrent",
            value={"agent": "other", "machine": "M5", "session_id": "session"},
            plan_revision=PLAN, expected_revision=0,
            created_at="2026-08-29T00:00:00Z", authority=authority,
        )
        self.comments["GEN-37"].append({
            "id": projection_slot_id("GEN-37", PLAN, 0, authority),
            "body": encode_projection_comment(event),
            "createdAt": "now", "updatedAt": "now",
        })

    def relation_page(self, identifier, inverse, after):
        identity = next(item for item in self.children if item["identifier"] == identifier)
        records = [relation for relation in self.relations if (
            relation["relatedIssue"]["id"] == identity["id"] if inverse
            else relation["issue"]["id"] == identity["id"]
        )]
        start = int(after or 0)
        nodes = records[start:start + self.page_size]
        end = start + len(nodes)
        return identity, nodes, end < len(records), str(end) if end < len(records) else None

    def execute(self, query, variables):
        self.calls.append((query, variables))
        if "query WorkstreamRoute" in query:
            return {
                "team": self.team(),
                "project": {"id": "project", "teams": {"nodes": [{"id": "team"}]}},
            }
        if "query WorkstreamResumeRoot" in query:
            self.root_reads += 1
            if self.before_root_read:
                self.before_root_read(self)
            return {"issue": {
                **self.root(),
                "children": {"nodes": [dict(item) for item in self.children],
                             "pageInfo": {"hasNextPage": False, "endCursor": None}},
            }}
        if "query WorkstreamDeltaComments" in query:
            self.comment_reads += 1
            if self.before_comment_read:
                self.before_comment_read(self)
            return {"issue": {
                "id": ROOT_ID, "identifier": "GEN-37", "team": self.team(),
                "project": {"id": "project"},
                "comments": {"nodes": list(self.comments["GEN-37"]),
                             "pageInfo": {"hasNextPage": False, "endCursor": None}},
            }}
        if "query WorkstreamChildRelations" in query or "query WorkstreamChildInverseRelations" in query:
            inverse = "InverseRelations" in query
            issue, nodes, more, cursor = self.relation_page(
                variables["issueId"], inverse, variables.get("after"),
            )
            field = "inverseRelations" if inverse else "relations"
            return {"issue": {
                "id": issue["id"], "identifier": issue["identifier"],
                "parent": issue["parent"], "team": issue["team"],
                "project": issue["project"],
                field: {"nodes": nodes, "pageInfo": {
                    "hasNextPage": more, "endCursor": cursor,
                }},
            }}
        if "query WorkstreamChildDependencyCapabilities" in query:
            return {
                "relationInput": {"inputFields": [
                    {"name": name} for name in ("id", "issueId", "relatedIssueId", "type")
                ]},
                "relationType": {"enumValues": [{"name": "blocks"}]},
                "issueType": {"fields": [
                    {"name": "relations"}, {"name": "inverseRelations"},
                ]},
                "queryType": {"fields": [{
                    "name": "issueRelations",
                    "args": [{"name": "includeArchived"}],
                }]},
            }
        if "query WorkstreamChildDependencySlots" in query:
            start = int(variables.get("after") or 0)
            nodes = self.relations[start:start + self.page_size]
            end = start + len(nodes)
            more = end < len(self.relations)
            return {"issueRelations": {
                "nodes": nodes,
                "pageInfo": {
                    "hasNextPage": more,
                    "endCursor": str(end) if more else None,
                },
            }}
        if "query WorkstreamProjectionCommentCreateCapability" in query:
            return {"__type": {"inputFields": [
                {"name": "id"}, {"name": "issueId"}, {"name": "body"},
            ]}}
        if "commentCreate" in query:
            data = variables["input"]
            if any(item["id"] == data["id"] for item in self.comments["GEN-37"]):
                raise LinearTransportError("duplicate comment id")
            comment = {
                "id": data["id"], "body": data["body"],
                "createdAt": "2026-08-29T00:00:00Z",
                "updatedAt": "2026-08-29T00:00:00Z",
            }
            self.comments["GEN-37"].append(comment)
            return {"commentCreate": {"success": True, "comment": comment}}
        if "issueRelationCreate" in query:
            data = variables["input"]
            if any(item["id"] == data["id"] for item in self.relations):
                raise LinearTransportError("duplicate relation id")
            relation = self.native_relation(
                self.identity_by_id(data["issueId"]),
                self.identity_by_id(data["relatedIssueId"]),
                relation_id=data["id"], relation_type=data["type"],
            )
            self.relations.append(relation)
            if self.lose_next_create_response:
                self.lose_next_create_response = False
                raise TimeoutError("lost response after commit")
            return {"issueRelationCreate": {"success": True, "issueRelation": relation}}
        raise AssertionError(query)


class ChildDependencyTests(unittest.TestCase):
    def adapter(self, fake):
        return LinearChildDependencyAdapter(
            fake, workspace_id="workspace", team_id="team", project_id="project",
            root_issue_id=ROOT_ID, root_identifier="GEN-37", plan_revision=PLAN,
        )

    @staticmethod
    def relation(source=A, relation_type="blocks", target=B):
        return {"source": source, "type": relation_type, "target": target}

    def apply(self, fake, relations=None, frontier=None, children=None):
        return self.adapter(fake).apply_batch(
            owned_children=children or [A, B, C],
            relations=relations or [self.relation()],
            expected_frontier=frontier or FRONTIER,
        )

    def test_batch_writes_one_native_relation_with_exact_inverse_readback(self):
        fake = FakeLinear()
        result = self.apply(fake)

        expected_id = dependency_relation_id(authority=AUTHORITY, blocker=A, blocked=B)
        self.assertEqual(result["writes"], 1)
        self.assertEqual(result["relations"], [{
            "id": expected_id, "type": "blocks", "blocker": A,
            "blocked": B, "inverse_type": "blocked_by",
        }])
        self.assertEqual(uuid.UUID(expected_id).version, 4)
        mutations = [(query, variables) for query, variables in fake.calls
                     if "issueRelationCreate" in query]
        self.assertEqual(len(mutations), 1)
        self.assertEqual(mutations[0][1]["input"], {
            "id": expected_id, "issueId": CHILD_A_ID,
            "relatedIssueId": CHILD_B_ID, "type": "blocks",
        })
        self.assertFalse(any("issueCreate" in query or "projectCreate" in query
                             for query, _ in mutations))

    def test_blocked_by_input_preserves_exact_direction(self):
        result = self.apply(FakeLinear(), [self.relation(B, "blocked_by", A)])
        self.assertEqual(result["relations"][0]["blocker"], A)
        self.assertEqual(result["relations"][0]["blocked"], B)

    def test_exact_replay_is_zero_write(self):
        fake = FakeLinear()
        first = self.apply(fake)
        fake.calls.clear()
        second = self.apply(fake)
        self.assertEqual(first["batch_id"], second["batch_id"])
        self.assertEqual(second["writes"], 0)
        self.assertFalse(any(
            "issueRelationCreate" in query or "commentCreate" in query
            for query, _ in fake.calls
        ))

    def test_lost_response_converges_and_replay_is_zero_write(self):
        fake = FakeLinear()
        fake.lose_next_create_response = True
        self.assertEqual(self.apply(fake)["writes"], 1)
        self.assertEqual(len(fake.relations), 1)
        fake.calls.clear()
        self.assertEqual(self.apply(fake)["writes"], 0)
        self.assertFalse(any(
            "issueRelationCreate" in query or "commentCreate" in query
            for query, _ in fake.calls
        ))

    def test_material_after_authorization_does_not_strand_derived_cache(self):
        fake = FakeLinear()
        injected = False

        def inject(client):
            nonlocal injected
            if client.comment_reads == 7 and not injected:
                injected = True
                client.add_material()

        fake.before_comment_read = inject
        first = self.apply(fake)
        self.assertTrue(injected)
        self.assertEqual(first["writes"], 1)
        self.assertEqual(first["frontier_after"]["material_revision"], 1)
        self.assertEqual(len(fake.relations), 1)
        fake.calls.clear()
        replay = self.apply(fake)
        self.assertEqual(replay["writes"], 0)
        self.assertEqual(replay["batch_id"], first["batch_id"])
        self.assertFalse(any(
            "issueRelationCreate" in query or "commentCreate" in query
            for query, _ in fake.calls
        ))

    def test_archived_deterministic_slot_is_visible_and_refuses(self):
        fake = FakeLinear()
        archived = fake.native_relation(A, B)
        archived["archivedAt"] = "2026-08-28T00:00:00Z"
        fake.relations.append(archived)
        with self.assertRaisesRegex(
            ChildDependencyError, "archived_dependency_in_active_readback",
        ):
            self.apply(fake)
        relation_queries = [
            query for query, _ in fake.calls
            if "WorkstreamChildRelations" in query
            or "WorkstreamChildInverseRelations" in query
        ]
        self.assertTrue(relation_queries)
        self.assertTrue(all("includeArchived: true" in query for query in relation_queries))
        self.assertFalse(any(
            "issueRelationCreate" in query or "commentCreate" in query
            for query, _ in fake.calls
        ))

    def test_occupied_deterministic_slot_refuses_before_authorization(self):
        fake = FakeLinear()
        occupied = fake.native_relation(
            A, C,
            relation_id=dependency_relation_id(
                authority=AUTHORITY, blocker=A, blocked=B,
            ),
            relation_type="related",
        )
        fake.relations.append(occupied)
        with self.assertRaisesRegex(
            ChildDependencyError, "dependency_relation_slot_occupied",
        ):
            self.apply(fake)
        self.assertFalse(any(
            "issueRelationCreate" in query or "commentCreate" in query
            for query, _ in fake.calls
        ))

    def test_complete_paginated_forward_and_inverse_readback(self):
        fake = FakeLinear()
        fake.page_size = 1
        result = self.apply(fake, [
            self.relation(A, "blocks", B), self.relation(A, "blocks", C),
        ])
        self.assertEqual(result["writes"], 2)
        fake.calls.clear()
        original = dict(FRONTIER)
        replay = self.apply(fake, [
            self.relation(A, "blocks", B), self.relation(A, "blocks", C),
        ], original)
        self.assertEqual(replay["writes"], 0)
        self.assertTrue(any(variables.get("after") == "1" for query, variables in fake.calls
                            if "WorkstreamChildRelations" in query))

    def test_each_frontier_advance_before_create_refuses_zero_write(self):
        mutations = {
            "material": lambda client: client.add_material(),
            "projection": lambda client: client.add_projection(),
            "graph": lambda client: client.relations.append(client.native_relation(A, C)),
        }
        for label, mutation in mutations.items():
            with self.subTest(frontier=label):
                fake = FakeLinear()
                applied = False

                def inject(client):
                    nonlocal applied
                    if client.root_reads == 4 and not applied:
                        applied = True
                        mutation(client)
                fake.before_root_read = inject
                with self.assertRaisesRegex(ChildDependencyError, "frontier_changed"):
                    self.apply(fake)
                self.assertFalse(any("issueRelationCreate" in query for query, _ in fake.calls))

    def test_same_count_graph_replacement_refuses_before_write(self):
        reviewed = {
            "id": dependency_relation_id(authority=AUTHORITY, blocker=A, blocked=B),
            "type": "blocks", "blocker": A, "blocked": B,
            "inverse_type": "blocked_by",
        }
        frontier = {
            **FRONTIER, "graph_revision": 1,
            "graph_sha256": graph_sha256([reviewed]),
        }
        fake = FakeLinear()
        fake.relations.append(fake.native_relation(B, A))
        with self.assertRaisesRegex(ChildDependencyError, "frontier_changed"):
            self.apply(fake, [self.relation(B, "blocks", C)], frontier)
        self.assertFalse(any("issueRelationCreate" in query for query, _ in fake.calls))

    def test_self_duplicate_conflict_and_wrong_type_refuse_before_write(self):
        cases = [
            ([self.relation(A, "blocks", A)], "self_dependency"),
            ([self.relation(), self.relation()], "duplicate_dependency"),
            ([self.relation(), self.relation(A, "blocked_by", B)],
             "conflicting_dependency_direction"),
            ([self.relation(A, "related", B)], "invalid_dependency_type"),
        ]
        for relations, error in cases:
            with self.subTest(error=error):
                fake = FakeLinear()
                with self.assertRaisesRegex(ChildDependencyError, error):
                    self.apply(fake, relations)
                self.assertFalse(any("issueRelationCreate" in query for query, _ in fake.calls))

    def test_ambiguous_incomplete_cross_root_and_route_refuse_before_write(self):
        cases = []
        ambiguous = FakeLinear()
        cases.append((ambiguous, [A, A, B], "ambiguous_owned_child"))
        incomplete = FakeLinear()
        cases.append((incomplete, [A, B], "incomplete_owned_child"))
        cross_root = FakeLinear()
        cross_root.children[1] = cross_root.child(
            B, parent_id="90000000-0000-4000-8000-000000000009",
        )
        cases.append((cross_root, [A, B, C], "cross_root_dependency"))
        wrong_project = FakeLinear()
        wrong_project.children[0] = wrong_project.child(A, project_id="other")
        cases.append((wrong_project, [A, B, C], "configured project"))
        for fake, children, error in cases:
            with self.subTest(error=error):
                with self.assertRaisesRegex(LinearTransportError, error):
                    self.apply(fake, children=children)
                self.assertFalse(any("issueRelationCreate" in query for query, _ in fake.calls))

    def test_nondeterministic_duplicate_and_missing_inverse_are_planted_mutations(self):
        fake = FakeLinear()
        relation = fake.native_relation(
            A, B, relation_id="90000000-0000-4000-8000-000000000009",
        )
        surfaces = {
            CHILD_A_ID: {"relations": [relation], "inverse_relations": []},
            CHILD_B_ID: {"relations": [], "inverse_relations": [relation]},
        }
        with self.assertRaisesRegex(ChildDependencyError, "non_deterministic"):
            reduce_dependency_readback(
                surfaces, authority=AUTHORITY,
                owned_children={CHILD_A_ID: A, CHILD_B_ID: B},
            )
        relation = fake.native_relation(A, B)
        surfaces[CHILD_A_ID]["relations"] = [relation]
        surfaces[CHILD_B_ID]["inverse_relations"] = []
        with self.assertRaisesRegex(ChildDependencyError, "inverse_missing"):
            reduce_dependency_readback(
                surfaces, authority=AUTHORITY,
                owned_children={CHILD_A_ID: A, CHILD_B_ID: B},
            )

        relation = fake.native_relation(A, B)
        relation["relatedIssue"] = fake.child(B, project_id="other")
        surfaces[CHILD_A_ID]["relations"] = [relation]
        surfaces[CHILD_B_ID]["inverse_relations"] = [relation]
        with self.assertRaisesRegex(ChildDependencyError, "endpoint_route_mismatch"):
            reduce_dependency_readback(
                surfaces, authority=AUTHORITY,
                owned_children={CHILD_A_ID: A, CHILD_B_ID: B},
            )

    def test_native_id_capability_is_required_before_mutation(self):
        class NoIDFake(FakeLinear):
            def execute(self, query, variables):
                if "WorkstreamChildDependencyCapabilities" in query:
                    self.calls.append((query, variables))
                    return {
                        "relationInput": {"inputFields": [
                            {"name": "issueId"}, {"name": "relatedIssueId"}, {"name": "type"},
                        ]},
                        "relationType": {"enumValues": [{"name": "blocks"}]},
                        "issueType": {"fields": [
                            {"name": "relations"}, {"name": "inverseRelations"},
                        ]},
                    }
                return super().execute(query, variables)

        fake = NoIDFake()
        with self.assertRaisesRegex(ChildDependencyError, "id_capability_unavailable"):
            self.apply(fake)
        self.assertFalse(any("issueRelationCreate" in query for query, _ in fake.calls))

    def test_root_identity_mismatch_refuses_before_mutation(self):
        fake = FakeLinear()
        adapter = LinearChildDependencyAdapter(
            fake, workspace_id="workspace", team_id="team", project_id="project",
            root_issue_id="90000000-0000-4000-8000-000000000009",
            root_identifier="GEN-37", plan_revision=PLAN,
        )
        with self.assertRaisesRegex(ChildDependencyError, "root_identity_mismatch"):
            adapter.apply_batch(
                owned_children=[A, B, C], relations=[self.relation()],
                expected_frontier=FRONTIER,
            )
        self.assertFalse(any("issueRelationCreate" in query for query, _ in fake.calls))

    def test_supported_json_cli_requires_exact_request_and_emits_receipt(self):
        fake = FakeLinear()
        request = {
            "schema_version": 1,
            "authority": AUTHORITY,
            "plan_revision": PLAN,
            "owned_children": [A, B, C],
            "relations": [self.relation()],
            "expected_frontier": FRONTIER,
        }
        stdin = io.StringIO(json.dumps(request))
        stdout = io.StringIO()
        with (
            mock.patch.object(dependency_module, "load_linear_api_key", return_value="secret"),
            mock.patch.object(dependency_module, "HttpGraphQLClient", return_value=fake),
            mock.patch.object(dependency_module.sys, "stdin", stdin),
            mock.patch.object(dependency_module.sys, "stdout", stdout),
        ):
            self.assertEqual(
                dependency_module.main(["--request", "-", "--apply"]), 0,
            )
        receipt = json.loads(stdout.getvalue())
        self.assertEqual(receipt["writes"], 1)
        self.assertEqual(receipt["authority"], AUTHORITY)
        self.assertFalse(any(
            "issueCreate" in query or "projectCreate" in query
            for query, _ in fake.calls
        ))

        with self.assertRaisesRegex(
            ChildDependencyError, "invalid_dependency_request_fields",
        ):
            dependency_module.apply_dependency_request(fake, {"schema_version": 1})


if __name__ == "__main__":
    unittest.main()
