#!/usr/bin/env python3
import os
import ssl
import threading
import unittest
import uuid
from unittest import mock

from workstream_http import default_ssl_context
from workstream_graph import GraphReviewRequired
from workstream_linear import (
    bootstrap_linear_route,
    HttpGraphQLClient,
    LinearGraphQLTransport,
    LinearTransportError,
    MARKER,
    deterministic_existing_root_child_id,
    deterministic_issue_id,
    parse_next_action,
    parse_plan_revision,
    parse_root_revision,
)
from workstream_linear_projection import (
    build_projection_event, encode_projection_comment, LinearProjectionAdapter,
)


class FakeClient:
    def __init__(self):
        self.issues = []
        self.next_id = 1
        self.calls = []

    def execute(self, query, variables):
        self.calls.append((query, variables))
        if "query WorkstreamRoute" in query:
            return {
                "team": {"id": variables["teamId"], "organization": {"id": "workspace"}},
                "project": {
                    "id": variables["projectId"],
                    "teams": {"nodes": [{"id": variables["teamId"]}]},
                },
            }
        if "query WorkstreamIssues" in query:
            return {"team": {"issues": {"nodes": self.issues[:]}}}
        if "issueCreate" in query:
            data = variables["input"]
            issue_id = data.get("id", f"id-{self.next_id}")
            if any(issue["id"] == issue_id for issue in self.issues):
                raise LinearTransportError("duplicate issue id")
            issue = {"id": issue_id, "identifier": f"GEN-{self.next_id}", "title": data["title"], "description": data["description"], "url": f"https://linear.test/{self.next_id}", "updatedAt": "now", "state": {"name": "Todo", "type": "unstarted"}, "parent": {"id": data.get("parentId")} if data.get("parentId") else None, "project": {"id": data["projectId"]} if data.get("projectId") else None, "team": {"id": data["teamId"], "organization": {"id": "workspace"}}}
            self.next_id += 1
            self.issues.append(issue)
            return {"issueCreate": {"success": True, "issue": issue}}
        if "issueUpdate" in query:
            identifier = variables["id"]
            issue = next(i for i in self.issues if i["identifier"] == identifier or i["id"] == identifier)
            issue.update({k: v for k, v in variables["input"].items() if k in {"title", "description"}})
            return {"issueUpdate": {"success": True, "issue": issue}}
        raise AssertionError("unexpected mutation")


class UUIDv4ValidatingFakeClient(FakeClient):
    """Mirror Linear's live client-supplied issue-ID constraint."""

    def execute(self, query, variables):
        if "issueCreate" in query:
            issue_id = variables["input"]["id"]
            parsed = uuid.UUID(issue_id)
            if (
                str(parsed) != issue_id
                or parsed.version != 4
                or parsed.variant != uuid.RFC_4122
            ):
                raise LinearTransportError("id must be a UUID")
        return super().execute(query, variables)


class FakeChildAuthorization:
    def __init__(self, failure=None):
        self.failure = failure
        self.event = None
        self.reserve_calls = 0
        self.assert_calls = 0

    def reserve_child_extension(self, **values):
        self.reserve_calls += 1
        if self.failure:
            raise self.failure
        route = {
            "workspace_id": "workspace", "team_id": "team",
            "project_id": "project",
            "root_issue_id": "409c1423-f949-4655-9f5f-d3213d7b434f",
        }
        value = {
            "root_issue_id": route["root_issue_id"],
            "route": route,
            "source": values["source"],
            "plan_revision": values["source"]["sha256"],
            "reviewed_candidate_key": values["reviewed_candidate_key"],
            "child_issue_id": values["child_issue_id"],
            "expected_material_revision": values["expected_material_revision"],
            "expected_projection_revision": values["expected_projection_revision"],
            "initial_state": "planned_pending_projection",
        }
        self.event = build_projection_event(
            workstream_id="GEN-37", kind="child_extension_authorization",
            key=values["child_issue_id"], value=value,
            plan_revision=values["source"]["sha256"],
            expected_revision=values["expected_projection_revision"],
            created_at="1970-01-01T00:00:00Z", authority=route,
        )
        return {"event": self.event, "remote_id": "comment-a", "revision": 8}

    def assert_child_extension_authorized(self, event):
        self.assert_calls += 1
        if self.failure:
            raise self.failure
        if event != self.event:
            raise LinearTransportError("authorization mismatch")
        return {"event": event, "remote_id": "comment-a", "revision": 8}


class LinearTransportTests(unittest.TestCase):
    def test_token_only_bootstrap_reads_exact_issue_route(self):
        client = mock.Mock()
        client.execute.return_value = {"issue": {
            "id": "root-uuid", "identifier": "GEN-37",
            "team": {"id": "team", "organization": {"id": "workspace"}},
            "project": {"id": "project", "teams": {"nodes": [{"id": "team"}]}},
        }}
        self.assertEqual(
            bootstrap_linear_route(client, "gen-37"),
            {"workspace_id": "workspace", "team_id": "team",
             "project_id": "project", "root_issue_id": "root-uuid"},
        )
        client.execute.assert_called_once()

    def test_token_only_bootstrap_refuses_project_team_mismatch(self):
        client = mock.Mock()
        client.execute.return_value = {"issue": {
            "id": "root-uuid", "identifier": "GEN-37",
            "team": {"id": "team", "organization": {"id": "workspace"}},
            "project": {"id": "project", "teams": {"nodes": [{"id": "other"}]}},
        }}
        with self.assertRaisesRegex(LinearTransportError, "not associated"):
            bootstrap_linear_route(client, "GEN-37")

    def test_http_client_passes_an_explicit_ssl_context(self):
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        response = mock.MagicMock()
        response.__enter__.return_value = response
        response.__exit__.return_value = False
        with mock.patch("workstream_linear.urllib.request.urlopen", return_value=response) as urlopen, \
             mock.patch("workstream_linear.json.load", return_value={"data": {"ok": True}}):
            result = HttpGraphQLClient("token", ssl_context=context).execute("query { ok }", {})

        self.assertEqual(result, {"ok": True})
        self.assertIs(urlopen.call_args.kwargs["context"], context)

    def test_macos_framework_python_uses_system_ca_bundle(self):
        paths = ssl.DefaultVerifyPaths(None, None, "SSL_CERT_FILE", "/missing", "SSL_CERT_DIR", "/missing-dir")
        fallback = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        with mock.patch("workstream_http.sys.platform", "darwin"), \
             mock.patch("workstream_http.ssl.get_default_verify_paths", return_value=paths), \
             mock.patch("workstream_http.os.path.isfile", side_effect=lambda path: path == "/etc/ssl/cert.pem"), \
             mock.patch("workstream_http.ssl.create_default_context", return_value=fallback) as create:
            self.assertIs(default_ssl_context(), fallback)

        create.assert_called_once_with(cafile="/etc/ssl/cert.pem")

    def test_existing_default_ca_configuration_is_preserved(self):
        paths = ssl.DefaultVerifyPaths("/configured.pem", None, "SSL_CERT_FILE", "/configured.pem", "SSL_CERT_DIR", "/certs")
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        with mock.patch("workstream_http.sys.platform", "darwin"), \
             mock.patch("workstream_http.ssl.get_default_verify_paths", return_value=paths), \
             mock.patch("workstream_http.os.path.isfile") as isfile, \
             mock.patch("workstream_http.ssl.create_default_context", return_value=context) as create:
            self.assertIs(default_ssl_context(), context)

        create.assert_called_once_with()
        isfile.assert_not_called()

    def plan(self):
        return {"graph_review_required": True, "root": {"stable_key": "source-demo", "title": "Demo", "plan_revision": "sha-demo"}, "children": [{"key": "a", "stable_key": "a", "title": "Build"}]}

    def routed_transport(self, fake):
        return LinearGraphQLTransport(
            fake, team_id="team", workspace_id="workspace", project_id="project"
        )

    def legacy_extension_plan(self):
        plan = self.plan()
        plan["source"] = {
            "identity": "plan:legacy", "sha256": "sha-demo", "bytes": 10,
        }
        return plan

    def legacy_root(self):
        return {
            "id": "409c1423-f949-4655-9f5f-d3213d7b434f",
            "identifier": "GEN-37",
            "title": "Legacy root",
            "description": "Plan revision: sha-demo\nLedger revision: 3",
            "url": "https://linear.test/37",
            "updatedAt": "now",
            "state": {"name": "In Progress", "type": "started"},
            "parent": None,
            "project": {"id": "project"},
            "team": {
                "id": "team", "organization": {"id": "workspace"},
            },
        }

    def extend_legacy(
        self, transport, *, authorization_adapter=None, expected_frontier=None,
    ):
        frontier = expected_frontier or {
            "material_revision": 3, "projection_revision": 7,
        }
        return transport.extend_existing_root_reviewed_child(
            self.legacy_extension_plan(),
            root_issue_id="409c1423-f949-4655-9f5f-d3213d7b434f",
            reviewed_candidate_key="a",
            source_revision="sha-demo",
            plan_revision="sha-demo",
            expected_frontier=frontier,
            authorization_adapter=authorization_adapter or FakeChildAuthorization(),
        )

    def test_reviewed_plan_creates_one_root_and_child(self):
        fake = FakeClient()
        result = LinearGraphQLTransport(fake, team_id="team").apply_reviewed_plan(self.plan(), accepted_keys={"a"})
        self.assertEqual(len(fake.issues), 2)
        self.assertEqual(result["root"]["description"].splitlines()[0], MARKER.pattern.replace("([^ >]+)", "source-demo"))

    def test_configured_route_is_verified_and_applied_to_every_create(self):
        fake = FakeClient()
        transport = LinearGraphQLTransport(
            fake, team_id="team", workspace_id="workspace", project_id="project"
        )
        transport.apply_reviewed_plan(self.plan(), accepted_keys={"a"})

        route_calls = [variables for query, variables in fake.calls if "WorkstreamRoute" in query]
        create_inputs = [variables["input"] for query, variables in fake.calls if "issueCreate" in query]
        self.assertEqual(route_calls, [{"teamId": "team", "projectId": "project"}])
        self.assertEqual(len(create_inputs), 2)
        self.assertTrue(all(item["projectId"] == "project" for item in create_inputs))
        self.assertTrue(all(item.get("id") for item in create_inputs))

    def test_deterministic_issue_ids_are_scoped_by_route_root_and_child(self):
        root = deterministic_issue_id(
            workspace_id="workspace", team_id="team", project_id="project",
            root_stable_key="source-demo",
        )
        self.assertEqual(root, "eac384c6-5f7c-4afc-bbd9-618cedf902a2")
        self.assertEqual(uuid.UUID(root).version, 4)
        self.assertEqual(uuid.UUID(root).variant, uuid.RFC_4122)
        self.assertEqual(
            root,
            deterministic_issue_id(
                workspace_id="workspace", team_id="team", project_id="project",
                root_stable_key="source-demo",
            ),
        )
        child = deterministic_issue_id(
            workspace_id="workspace", team_id="team", project_id="project",
            root_stable_key="source-demo", child_stable_key="a",
        )
        self.assertEqual(child, "e94bc97d-505d-45ba-a8e7-4b6002c785b0")
        self.assertEqual(uuid.UUID(child).version, 4)
        self.assertEqual(uuid.UUID(child).variant, uuid.RFC_4122)
        self.assertNotEqual(root, child)
        self.assertNotEqual(
            child,
            deterministic_issue_id(
                workspace_id="workspace", team_id="team", project_id="project",
                root_stable_key="source-other", child_stable_key="a",
            ),
        )
        self.assertNotEqual(
            child,
            deterministic_issue_id(
                workspace_id="workspace", team_id="team", project_id="project",
                root_stable_key="source-demo", child_stable_key="b",
            ),
        )
        self.assertNotEqual(
            root,
            deterministic_issue_id(
                workspace_id="workspace", team_id="team", project_id="other",
                root_stable_key="source-demo",
            ),
        )

    def test_routed_intake_uses_linear_compatible_uuid_v4_ids(self):
        fake = UUIDv4ValidatingFakeClient()
        result = self.routed_transport(fake).intake_reviewed_plan(
            self.plan(), accepted_keys={"a"}
        )

        self.assertEqual(len(fake.issues), 2)
        self.assertTrue(all(uuid.UUID(issue["id"]).version == 4 for issue in fake.issues))
        self.assertEqual(result["receipts"]["root"]["id"], fake.issues[0]["id"])

    def test_existing_legacy_root_extension_creates_only_scoped_child(self):
        fake = UUIDv4ValidatingFakeClient()
        fake.issues.append(self.legacy_root())

        result = self.extend_legacy(self.routed_transport(fake))

        child_id = deterministic_existing_root_child_id(
            workspace_id="workspace", team_id="team", project_id="project",
            root_issue_id="409c1423-f949-4655-9f5f-d3213d7b434f",
            child_stable_key="a",
        )
        self.assertEqual(len(fake.issues), 2)
        self.assertEqual(result["root"]["identifier"], "GEN-37")
        self.assertEqual(result["receipt"]["id"], child_id)
        self.assertEqual(result["receipt"]["parent_id"], self.legacy_root()["id"])
        self.assertEqual(result["receipt"]["disposition"], "created")
        self.assertEqual(result["initial_state"], "planned_pending_projection")
        self.assertEqual(sum("issueCreate" in query for query, _ in fake.calls), 1)
        create_input = next(
            variables["input"] for query, variables in fake.calls
            if "issueCreate" in query
        )
        self.assertNotIn("stateId", create_input)
        self.assertNotIn("assigneeId", create_input)

    def test_existing_root_extension_replay_is_a_zero_write_no_op(self):
        fake = FakeClient()
        fake.issues.append(self.legacy_root())
        transport = self.routed_transport(fake)
        first = self.extend_legacy(transport)
        fake.calls.clear()

        second = self.extend_legacy(transport)

        self.assertEqual(first["receipt"]["id"], second["receipt"]["id"])
        self.assertEqual(second["receipt"]["disposition"], "existing")
        self.assertEqual(second["issues"], [])
        self.assertFalse(any("issueCreate" in query for query, _ in fake.calls))

    def test_existing_root_extension_lost_response_converges(self):
        class LostResponseFake(FakeClient):
            def execute(self, query, variables):
                result = super().execute(query, variables)
                if "issueCreate" in query:
                    raise TimeoutError("response lost after commit")
                return result

        fake = LostResponseFake()
        fake.issues.append(self.legacy_root())

        result = self.extend_legacy(self.routed_transport(fake))

        self.assertEqual(len(fake.issues), 2)
        self.assertEqual(result["receipt"]["disposition"], "converged")

    def test_existing_root_extension_recovers_crash_after_authorization(self):
        class CrashBeforeCreateFake(FakeClient):
            def __init__(self):
                super().__init__()
                self.crash = True

            def execute(self, query, variables):
                if "issueCreate" in query and self.crash:
                    self.crash = False
                    raise OSError("crash after authorization before create")
                return super().execute(query, variables)

        fake = CrashBeforeCreateFake()
        fake.issues.append(self.legacy_root())
        authorization = FakeChildAuthorization()
        with self.assertRaisesRegex(
            LinearTransportError, "intake_create_unconfirmed",
        ):
            self.extend_legacy(
                self.routed_transport(fake), authorization_adapter=authorization,
            )

        result = self.extend_legacy(
            self.routed_transport(fake), authorization_adapter=authorization,
        )
        self.assertEqual(result["receipt"]["disposition"], "created")
        self.assertEqual(authorization.reserve_calls, 2)

    def test_existing_root_extension_collision_fails_closed(self):
        fake = FakeClient()
        fake.issues.append(self.legacy_root())
        child_id = deterministic_existing_root_child_id(
            workspace_id="workspace", team_id="team", project_id="project",
            root_issue_id=self.legacy_root()["id"], child_stable_key="a",
        )
        fake.issues.append({
            **self.legacy_root(),
            "id": child_id,
            "identifier": "GEN-99",
            "title": "Collision",
            "description": "<!-- workstream-key:other -->\nPlan revision: sha-demo",
        })

        with self.assertRaisesRegex(LinearTransportError, "intake_identity_collision"):
            self.extend_legacy(self.routed_transport(fake))
        self.assertEqual(len(fake.issues), 2)

    def test_existing_root_extension_stale_frontier_is_zero_write(self):
        fake = FakeClient()
        fake.issues.append(self.legacy_root())

        with self.assertRaisesRegex(
            LinearTransportError, "existing_root_frontier_changed_reload_required",
        ):
            self.extend_legacy(
                self.routed_transport(fake),
                authorization_adapter=FakeChildAuthorization(
                    LinearTransportError(
                        "existing_root_frontier_changed_reload_required"
                    )
                ),
            )

        self.assertFalse(any("issueCreate" in query for query, _ in fake.calls))

    def test_existing_root_extension_mismatched_authorization_is_zero_write(self):
        class WrongRouteAuthorization(FakeChildAuthorization):
            def reserve_child_extension(self, **values):
                receipt = super().reserve_child_extension(**values)
                receipt["event"] = {
                    **receipt["event"],
                    "authority": {
                        **receipt["event"]["authority"],
                        "root_issue_id": "wrong-root",
                    },
                }
                return receipt

        fake = FakeClient()
        fake.issues.append(self.legacy_root())
        with self.assertRaisesRegex(
            LinearTransportError, "child_extension_authorization_receipt_mismatch",
        ):
            self.extend_legacy(
                self.routed_transport(fake),
                authorization_adapter=WrongRouteAuthorization(),
            )

        self.assertFalse(any("issueCreate" in query for query, _ in fake.calls))

    def test_existing_root_extension_concurrent_advance_before_create_is_zero_write(self):
        fake = FakeClient()
        fake.issues.append(self.legacy_root())
        authorization = FakeChildAuthorization()

        def superseded(_event):
            raise LinearTransportError(
                "child_extension_authorization_superseded_or_conflicting"
            )

        authorization.assert_child_extension_authorized = superseded

        with self.assertRaisesRegex(
            LinearTransportError,
            "child_extension_authorization_superseded_or_conflicting",
        ):
            self.extend_legacy(
                self.routed_transport(fake), authorization_adapter=authorization,
            )

        self.assertFalse(any("issueCreate" in query for query, _ in fake.calls))

    def test_existing_root_extension_planted_projection_race_never_creates_child(self):
        class CombinedRaceClient(FakeClient):
            def __init__(self):
                super().__init__()
                self.comments = []
                self.injected = False

            def execute(self, query, variables):
                if "WorkstreamProjectionCommentCreateCapability" in query:
                    return {"__type": {"inputFields": [{"name": "id"}]}}
                if "query WorkstreamDeltaComments" in query:
                    return {"issue": {
                        "id": self.issues[0]["id"], "identifier": "GEN-37",
                        "team": self.issues[0]["team"],
                        "project": self.issues[0]["project"],
                        "comments": {
                            "nodes": list(self.comments),
                            "pageInfo": {"hasNextPage": False, "endCursor": None},
                        },
                    }}
                if "commentCreate" in query:
                    if not self.injected:
                        self.injected = True
                        winner = build_projection_event(
                            workstream_id="GEN-37",
                            kind="child_extension_authorization",
                            key="bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
                            value={
                                "root_issue_id": self.issues[0]["id"],
                                "route": {
                                    "workspace_id": "workspace", "team_id": "team",
                                    "project_id": "project",
                                    "root_issue_id": self.issues[0]["id"],
                                },
                                "source": {
                                    "identity": "plan:legacy", "sha256": "sha-demo",
                                },
                                "plan_revision": "sha-demo",
                                "reviewed_candidate_key": "candidate-b",
                                "child_issue_id": (
                                    "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
                                ),
                                "expected_material_revision": 0,
                                "expected_projection_revision": 0,
                                "initial_state": "planned_pending_projection",
                            },
                            plan_revision="sha-demo", expected_revision=0,
                            created_at="2026-08-20T00:00:00Z",
                            authority={
                                "workspace_id": "workspace", "team_id": "team",
                                "project_id": "project",
                                "root_issue_id": self.issues[0]["id"],
                            },
                        )
                        self.comments.append({
                            "id": variables["input"]["id"],
                            "body": encode_projection_comment(winner),
                            "createdAt": "2026-08-20T00:00:00Z",
                            "updatedAt": "2026-08-20T00:00:00Z",
                        })
                        raise LinearTransportError("duplicate comment id")
                return super().execute(query, variables)

        client = CombinedRaceClient()
        client.issues.append(self.legacy_root())
        authorization = LinearProjectionAdapter(
            client, issue_id="GEN-37", workstream_id="GEN-37",
            plan_revision="sha-demo", workspace_id="workspace", team_id="team",
            project_id="project", root_issue_id=self.legacy_root()["id"],
        )

        with self.assertRaisesRegex(
            LinearTransportError, "projection_slot_lost_reload_required",
        ):
            self.extend_legacy(
                self.routed_transport(client),
                authorization_adapter=authorization,
                expected_frontier={
                    "material_revision": 0, "projection_revision": 0,
                },
            )

        self.assertEqual(len(client.issues), 1)
        self.assertFalse(any("issueCreate" in query for query, _ in client.calls))

    def test_existing_root_extension_requires_exact_reviewed_source_before_network(self):
        fake = FakeClient()
        fake.issues.append(self.legacy_root())
        transport = self.routed_transport(fake)

        with self.assertRaisesRegex(GraphReviewRequired, "source revision"):
            transport.extend_existing_root_reviewed_child(
                self.legacy_extension_plan(),
                root_issue_id=self.legacy_root()["id"],
                reviewed_candidate_key="a",
                source_revision="sha-other",
                plan_revision="sha-demo",
                expected_frontier={
                    "material_revision": 3, "projection_revision": 7,
                },
                authorization_adapter=FakeChildAuthorization(),
            )

        self.assertEqual(fake.calls, [])

    def test_live_uuid_contract_fake_rejects_legacy_uuid_v5(self):
        fake = UUIDv4ValidatingFakeClient()
        with self.assertRaisesRegex(LinearTransportError, "id must be a UUID"):
            fake.execute("mutation { issueCreate }", {"input": {
                "id": "eac384c6-5f7c-5afc-bbd9-618cedf902a2",
            }})
        self.assertEqual(fake.issues, [])

    def test_routed_intake_is_idempotent_and_returns_exact_receipts(self):
        fake = FakeClient()
        transport = self.routed_transport(fake)
        first = transport.intake_reviewed_plan(self.plan(), accepted_keys={"a"})
        second = transport.intake_reviewed_plan(self.plan(), accepted_keys={"a"})

        self.assertEqual(len(fake.issues), 2)
        self.assertEqual(first["receipts"]["root"]["id"], second["receipts"]["root"]["id"])
        self.assertEqual(first["receipts"]["children"][0]["id"], second["receipts"]["children"][0]["id"])
        self.assertEqual(second["receipts"]["root"]["disposition"], "existing")
        self.assertEqual(second["receipts"]["children"][0]["disposition"], "existing")
        self.assertEqual(sum("issueCreate" in query for query, _ in fake.calls), 2)

    def test_concurrent_first_intake_converges_without_duplicate_or_delete(self):
        class ConcurrentFake(UUIDv4ValidatingFakeClient):
            def __init__(self):
                super().__init__()
                self.lock = threading.Lock()
                self.first_reads = set()
                self.barrier = threading.Barrier(2)

            def execute(self, query, variables):
                if "query WorkstreamIssues" in query:
                    thread = threading.get_ident()
                    with self.lock:
                        first = thread not in self.first_reads
                        self.first_reads.add(thread)
                    if first:
                        self.barrier.wait(timeout=2)
                with self.lock:
                    return super().execute(query, variables)

        fake = ConcurrentFake()
        results = []
        failures = []

        def intake():
            try:
                results.append(
                    self.routed_transport(fake).intake_reviewed_plan(
                        self.plan(), accepted_keys={"a"}
                    )
                )
            except Exception as error:  # pragma: no cover - asserted below
                failures.append(error)

        threads = [threading.Thread(target=intake) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=5)

        self.assertFalse(any(thread.is_alive() for thread in threads))
        self.assertEqual(failures, [])
        self.assertEqual(len(results), 2)
        self.assertEqual(len(fake.issues), 2)
        self.assertEqual(
            {result["receipts"]["root"]["id"] for result in results},
            {fake.issues[0]["id"]},
        )
        self.assertFalse(any("issueUpdate" in query or "issueDelete" in query for query, _ in fake.calls))

    def test_committed_create_with_lost_response_converges_by_readback(self):
        class LostResponseFake(UUIDv4ValidatingFakeClient):
            def __init__(self):
                super().__init__()
                self.lose_next_create = True

            def execute(self, query, variables):
                result = super().execute(query, variables)
                if "issueCreate" in query and self.lose_next_create:
                    self.lose_next_create = False
                    raise TimeoutError("response lost after commit")
                return result

        fake = LostResponseFake()
        result = self.routed_transport(fake).intake_reviewed_plan(
            self.plan(), accepted_keys={"a"}
        )

        self.assertEqual(len(fake.issues), 2)
        self.assertEqual(result["receipts"]["root"]["disposition"], "converged")
        self.assertEqual(result["receipts"]["children"][0]["disposition"], "created")

    def test_same_plan_can_add_a_newly_reviewed_missing_child(self):
        fake = FakeClient()
        transport = self.routed_transport(fake)
        plan = self.plan()
        plan["children"].append({"key": "b", "stable_key": "b", "title": "Verify"})

        transport.intake_reviewed_plan(plan, accepted_keys={"a"})
        result = transport.intake_reviewed_plan(plan, accepted_keys={"a", "b"})

        self.assertEqual(len(fake.issues), 3)
        dispositions = {
            receipt["stable_key"]: receipt["disposition"]
            for receipt in result["receipts"]["children"]
        }
        self.assertEqual(dispositions, {"a": "existing", "b": "created"})

    def test_changed_plan_still_refuses_without_remote_cas(self):
        fake = FakeClient()
        transport = self.routed_transport(fake)
        transport.intake_reviewed_plan(self.plan(), accepted_keys={"a"})
        changed = self.plan()
        changed["root"]["plan_revision"] = "sha-changed"
        fake.calls.clear()

        with self.assertRaisesRegex(LinearTransportError, "remote_cas_unavailable"):
            transport.intake_reviewed_plan(changed, accepted_keys={"a"})

        self.assertFalse(any("issueCreate" in query or "issueUpdate" in query for query, _ in fake.calls))

    def test_deterministic_id_collision_fails_closed(self):
        fake = FakeClient()
        root_id = deterministic_issue_id(
            workspace_id="workspace", team_id="team", project_id="project",
            root_stable_key="source-demo",
        )
        fake.issues.append({
            "id": root_id, "identifier": "GEN-99", "title": "Unrelated",
            "description": "<!-- workstream-key:other -->\nPlan revision: sha-demo",
            "url": "https://linear.test/99", "updatedAt": "now", "parent": None,
            "project": {"id": "project"},
            "team": {"id": "team", "organization": {"id": "workspace"}},
        })

        with self.assertRaisesRegex(LinearTransportError, "intake_identity_collision"):
            self.routed_transport(fake).intake_reviewed_plan(
                self.plan(), accepted_keys={"a"}
            )
        self.assertEqual(len(fake.issues), 1)

    def test_routed_missed_review_fails_before_network(self):
        fake = FakeClient()
        with self.assertRaises(GraphReviewRequired):
            self.routed_transport(fake).intake_reviewed_plan(
                self.plan(), accepted_keys=None
            )
        self.assertEqual(fake.calls, [])

    def test_from_config_requires_and_consumes_complete_route(self):
        fake = FakeClient()
        route = {
            "workspace_id": "workspace", "team_id": "team", "project_id": "project"
        }
        with mock.patch("workstream_config.resolve_linear_route", return_value=(route, None)):
            transport = LinearGraphQLTransport.from_config(fake)
        self.assertEqual(transport.workspace_id, "workspace")
        self.assertEqual(transport.team_id, "team")
        self.assertEqual(transport.project_id, "project")

    def test_workspace_mismatch_fails_before_issue_read_or_write(self):
        fake = FakeClient()
        transport = LinearGraphQLTransport(
            fake, team_id="team", workspace_id="wrong", project_id="project"
        )
        with self.assertRaisesRegex(LinearTransportError, "not in the configured workspace"):
            transport.snapshot()
        self.assertFalse(any("WorkstreamIssues" in query or "issueCreate" in query for query, _ in fake.calls))

    def test_project_fence_excludes_same_marker_from_another_project(self):
        fake = FakeClient()
        other = {
            "id": "other-root", "identifier": "GEN-99", "title": "Demo",
            "description": "<!-- workstream-key:source-demo -->\nPlan revision: sha-demo",
            "url": "https://linear.test/99", "updatedAt": "now",
            "state": {"name": "Todo", "type": "unstarted"}, "parent": None,
            "project": {"id": "other-project"},
        }
        fake.issues.append(other)
        transport = LinearGraphQLTransport(
            fake, team_id="team", workspace_id="workspace", project_id="project"
        )

        result = transport.apply_reviewed_plan(self.plan(), accepted_keys={"a"})

        self.assertNotEqual(result["root"]["id"], "other-root")
        self.assertEqual(len(fake.issues), 3)

    def test_repeated_intake_uses_existing_markers(self):
        fake = FakeClient()
        transport = LinearGraphQLTransport(fake, team_id="team")
        transport.apply_reviewed_plan(self.plan(), accepted_keys={"a"})
        transport.apply_reviewed_plan(self.plan(), accepted_keys={"a"})
        self.assertEqual(len(fake.issues), 2)
        self.assertEqual(sum("issueCreate" in query for query, _ in fake.calls), 2)
        self.assertEqual(sum("issueUpdate" in query for query, _ in fake.calls), 0)

    def test_reviewed_child_next_action_round_trips_through_live_snapshot(self):
        fake = FakeClient()
        plan = self.plan()
        plan["root"]["next_action"] = "Review child graph."
        plan["children"][0]["next_action"] = "Run focused tests."
        transport = LinearGraphQLTransport(fake, team_id="team")
        transport.apply_reviewed_plan(plan, accepted_keys={"a"})

        snapshot = transport.snapshot_for_root("GEN-1")

        self.assertEqual(snapshot["root"]["next_action"], "Review child graph.")
        self.assertEqual(snapshot["children"][0]["next_action"], "Run focused tests.")
        self.assertEqual(snapshot["root"]["revision"], 0)

    def test_resume_collects_child_comments_in_one_graph_read_then_paginates(self):
        root = {
            "id": "root-id", "identifier": "GEN-37", "title": "Root",
            "description": "Plan revision: sha\nLedger revision: 0\nNext action: root",
            "url": "https://linear/GEN-37", "updatedAt": "now",
            "state": {"name": "In Progress", "type": "started"},
            "team": {"id": "team", "organization": {"id": "workspace"}},
            "project": {"id": "project"},
        }

        def child(identifier, comment_id, *, has_next=False):
            return {
                "id": identifier.lower(), "identifier": identifier, "title": identifier,
                "description": f"Next action: resume {identifier}",
                "url": f"https://linear/{identifier}", "updatedAt": "now",
                "state": {"name": "In Progress", "type": "started"},
                "parent": {"id": "root-id", "identifier": "GEN-37"},
                "team": {"id": "team", "organization": {"id": "workspace"}},
                "project": {"id": "project"},
                "comments": {
                    "nodes": [{"id": comment_id, "body": "plain"}],
                    "pageInfo": {
                        "hasNextPage": has_next,
                        "endCursor": "child-cursor" if has_next else None,
                    },
                },
            }

        client = mock.Mock()

        def execute(query, variables):
            if "WorkstreamResumeRoot" in query:
                return {"issue": {
                    **root,
                    "children": {
                        "nodes": [child("GEN-38", "comment-a"),
                                  child("GEN-43", "comment-b", has_next=True)],
                        "pageInfo": {"hasNextPage": False, "endCursor": None},
                    },
                }}
            if "WorkstreamResumeComments" in query:
                self.assertEqual(variables, {
                    "issueId": "GEN-43", "after": "child-cursor",
                })
                continuation = child("GEN-43", "ignored")
                return {"issue": {
                    **continuation,
                    "comments": {
                        "nodes": [{"id": "comment-c", "body": "plain"}],
                        "pageInfo": {"hasNextPage": False, "endCursor": None},
                    },
                }}
            self.fail("unexpected query")

        client.execute.side_effect = execute
        snapshot = LinearGraphQLTransport(client, team_id="team").snapshot_for_root(
            "GEN-37", include_child_comments=True,
        )

        self.assertEqual(
            [comment["id"] for comment in snapshot["child_comments"]["GEN-38"]],
            ["comment-a"],
        )
        self.assertEqual(
            [comment["id"] for comment in snapshot["child_comments"]["GEN-43"]],
            ["comment-b", "comment-c"],
        )
        self.assertEqual(client.execute.call_count, 2)

    def test_resume_refuses_null_child_connection(self):
        client = mock.Mock()
        client.execute.return_value = {"issue": {
            "id": "root-id", "identifier": "GEN-37", "title": "Root",
            "description": "Plan revision: sha\nLedger revision: 0\nNext action: root",
            "url": "https://linear/GEN-37", "updatedAt": "now",
            "state": {"name": "In Progress", "type": "started"},
            "team": {"id": "team", "organization": {"id": "workspace"}},
            "project": {"id": "project"}, "children": None,
        }}

        with self.assertRaisesRegex(LinearTransportError, "child connection"):
            LinearGraphQLTransport(client, team_id="team").snapshot_for_root(
                "GEN-37", include_child_comments=True,
            )

    def test_resume_refuses_null_comment_continuation(self):
        client = mock.Mock()
        client.execute.return_value = {"issue": {
            "id": "child-id", "identifier": "GEN-43", "comments": None,
        }}
        transport = LinearGraphQLTransport(client, team_id="team")

        with self.assertRaisesRegex(LinearTransportError, "comment connection"):
            transport._remaining_resume_comments(
                {"id": "child-id", "identifier": "GEN-43"},
                {"hasNextPage": True, "endCursor": "cursor"},
            )

    def test_completed_state_type_does_not_require_child_comment_log(self):
        child = {
            "id": "child-id", "identifier": "GEN-43", "title": "Child",
            "description": "completed without a next action",
            "url": "https://linear/GEN-43", "updatedAt": "now",
            "state": {"id": "state-done", "name": "QA Accepted", "type": "completed"},
            "assignee": {"id": "assignee-daniel"},
            "parent": {"id": "root-id", "identifier": "GEN-37"},
            "team": {"id": "team", "organization": {"id": "workspace"}},
            "project": {"id": "project"},
        }
        client = mock.Mock()
        client.execute.return_value = {"issue": {
            "id": "root-id", "identifier": "GEN-37", "title": "Root",
            "description": "Plan revision: sha\nLedger revision: 0\nNext action: root",
            "url": "https://linear/GEN-37", "updatedAt": "now",
            "state": {"name": "In Progress", "type": "started"},
            "team": {"id": "team", "organization": {"id": "workspace"}},
            "project": {"id": "project"},
            "children": {
                "nodes": [child],
                "pageInfo": {"hasNextPage": False, "endCursor": None},
            },
        }}

        snapshot = LinearGraphQLTransport(client, team_id="team").snapshot_for_root(
            "GEN-37", include_child_comments=True,
        )

        self.assertEqual(snapshot["children"][0]["status"], "QA Accepted")
        self.assertEqual(snapshot["children"][0]["status_type"], "completed")
        self.assertEqual(snapshot["children"][0]["state_id"], "state-done")
        self.assertEqual(snapshot["children"][0]["assignee"]["id"], "assignee-daniel")
        self.assertEqual(snapshot["child_comments"], {})

    def test_repeated_intake_preserves_existing_mutable_next_action(self):
        fake = FakeClient()
        transport = LinearGraphQLTransport(fake, team_id="team")
        plan = self.plan()
        plan["root"]["next_action"] = "Static plan action."
        transport.apply_reviewed_plan(plan, accepted_keys={"a"})
        fake.issues[0]["description"] = fake.issues[0]["description"].replace(
            "Current next action: Static plan action.",
            "Current next action: Keep this live action.",
        )
        fake.issues[0]["description"] += "\n\nWhy: preserve this human-authored context."

        transport.apply_reviewed_plan(plan, accepted_keys={"a"})

        description = fake.issues[0]["description"]
        self.assertEqual(parse_next_action(description), "Keep this live action.")
        self.assertIn("Why: preserve this human-authored context.", description)
        self.assertNotIn("Static plan action.", description)

    def test_unreviewed_plan_fails_before_network_mutation(self):
        fake = FakeClient()
        plan = self.plan()
        transport = LinearGraphQLTransport(fake, team_id="team")
        with self.assertRaises(ValueError):
            transport.apply_reviewed_plan(plan, accepted_keys=None)
        self.assertEqual(len(fake.issues), 0)

    def test_expected_revision_refuses_root_overwrite_without_remote_cas(self):
        fake = FakeClient()
        transport = LinearGraphQLTransport(fake, team_id="team")
        transport.apply_reviewed_plan(self.plan(), accepted_keys={"a"})
        fake.calls.clear()

        with self.assertRaisesRegex(LinearTransportError, "remote_cas_unavailable"):
            transport.apply_reviewed_plan(
                self.plan(), accepted_keys={"a"}, expected_revision=0
            )

        self.assertFalse(any("issueUpdate" in query for query, _ in fake.calls))

    def test_expected_revision_refuses_initial_create_without_remote_cas(self):
        fake = FakeClient()
        with self.assertRaisesRegex(LinearTransportError, "remote_cas_unavailable"):
            LinearGraphQLTransport(fake, team_id="team").apply_reviewed_plan(
                self.plan(), accepted_keys={"a"}, expected_revision=0
            )
        self.assertFalse(any("issueCreate" in query for query, _ in fake.calls))

    def test_changed_plan_refuses_existing_graph_without_remote_cas(self):
        fake = FakeClient()
        transport = LinearGraphQLTransport(fake, team_id="team")
        transport.apply_reviewed_plan(self.plan(), accepted_keys={"a"})
        changed = self.plan()
        changed["root"]["plan_revision"] = "sha-changed"
        fake.calls.clear()

        with self.assertRaisesRegex(LinearTransportError, "remote_cas_unavailable"):
            transport.apply_reviewed_plan(changed, accepted_keys={"a"})

        self.assertEqual(len(fake.calls), 1)
        self.assertIn("query WorkstreamIssues", fake.calls[0][0])

    def test_root_snapshot_is_bounded_and_token_addressable(self):
        fake = FakeClient()
        transport = LinearGraphQLTransport(fake, team_id="team")
        transport.apply_reviewed_plan(self.plan(), accepted_keys={"a"})
        snapshot = transport.snapshot_for_root("GEN-1")
        self.assertEqual(snapshot["root"]["identifier"], "GEN-1")
        self.assertEqual(len(snapshot["children"]), 1)
        self.assertEqual(snapshot["children"][0]["status"], "Todo")

    def test_snapshot_paginates_before_resolving_root_and_children(self):
        class PagedClient:
            def __init__(self):
                self.afters = []

            def execute(self, query, variables):
                self.afters.append(variables["after"])
                if variables["after"] is None:
                    return {"team": {"issues": {
                        "nodes": [{"id": "unrelated", "identifier": "GEN-999"}],
                        "pageInfo": {"hasNextPage": True, "endCursor": "page-2"},
                    }}}
                return {"team": {"issues": {
                    "nodes": [
                        {
                            "id": "root", "identifier": "GEN-37", "title": "Root",
                            "description": "Plan revision: plan\nLedger revision: 2\nCurrent next action: Continue.",
                            "url": "https://linear.test/GEN-37",
                            "state": {"name": "In Progress"}, "parent": None,
                        },
                        {
                            "id": "child", "identifier": "GEN-38", "title": "Child",
                            "description": "Current next action: Finish.",
                            "url": "https://linear.test/GEN-38",
                            "state": {"name": "Todo"}, "parent": {"id": "root"},
                        },
                    ],
                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                }}}

        client = PagedClient()
        snapshot = LinearGraphQLTransport(client, team_id="team").snapshot_for_root("GEN-37")
        self.assertEqual(client.afters, [None, "page-2"])
        self.assertEqual(snapshot["root"]["identifier"], "GEN-37")
        self.assertEqual([child["identifier"] for child in snapshot["children"]], ["GEN-38"])

    def test_next_action_parser_accepts_plain_and_markdown_bold_labels(self):
        self.assertEqual(
            parse_next_action("Current next action (2026-08-20): Re-run the canary."),
            "Re-run the canary.",
        )
        self.assertEqual(
            parse_next_action("**Current next action (2026-08-21):** Review the receipt."),
            "Review the receipt.",
        )
        self.assertEqual(
            parse_next_action("- **Current next action:** Resume from the root."),
            "Resume from the root.",
        )
        self.assertEqual(
            parse_next_action("Next action: Continue the live proof."),
            "Continue the live proof.",
        )
        self.assertEqual(
            parse_next_action(
                "Acceptance: preserve durable state. Next action: Run the canary."
            ),
            "Run the canary.",
        )
        self.assertIsNone(parse_next_action("MCP transport next action: not canonical"))

    def test_live_root_legacy_revision_labels_are_readable(self):
        description = (
            "Exact intake identity: plan revision SHA-256 "
            "`458a99c16cec2cdc649e26bb973fcfb0eb28f7a9e1b05335a78272db8745ffa1`.\n\n"
            "Ledger CAS revision: 1 (adapter-owned material-state revision)."
        )
        self.assertEqual(
            parse_plan_revision(description),
            "458a99c16cec2cdc649e26bb973fcfb0eb28f7a9e1b05335a78272db8745ffa1",
        )
        self.assertEqual(parse_root_revision(description), 1)

    def test_live_snapshot_resume_uses_next_actions_from_descriptions(self):
        fake = FakeClient()
        fake.issues = [
            {
                "id": "id-1", "identifier": "GEN-1", "title": "Demo",
                "description": "Plan revision: sha-demo\nLedger revision: 3\n**Current next action (2026-08-20):** Resume safely.",
                "url": "https://linear.test/1", "updatedAt": "now",
                "state": {"name": "In Progress", "type": "started"}, "parent": None,
            },
            {
                "id": "id-2", "identifier": "GEN-2", "title": "Build",
                "description": "Current next action (2026-08-20): Run focused tests.",
                "url": "https://linear.test/2", "updatedAt": "now",
                "state": {"name": "Todo", "type": "unstarted"},
                "parent": {"id": "id-1", "identifier": "GEN-1"},
            },
        ]
        snapshot = LinearGraphQLTransport(fake, team_id="team").snapshot_for_root("GEN-1")
        self.assertEqual(snapshot["root"]["next_action"], "Resume safely.")
        self.assertEqual(snapshot["children"][0]["next_action"], "Run focused tests.")

        from workstream_resume import compact_context
        context = compact_context(snapshot, "GEN-1")
        self.assertEqual(context["next_action"], "Resume safely.")
        self.assertEqual(context["children"][0]["next_action"], "Run focused tests.")


if __name__ == "__main__":
    unittest.main()
