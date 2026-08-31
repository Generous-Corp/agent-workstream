import contextlib
import copy
import base64
import hashlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from test_workstream_linear_events import FakeCommentClient
import test_workstream_linear_events as linear_event_tests
import test_workstream_generation_transition as generation_tests
from workstream_delta import Delta
from workstream_checkpoint import build_checkpoint
from workstream_generation import GenerationTransport, build_retirement_proof
from workstream_linear_checkpoints import (
    encode_checkpoint_comment, reduce_checkpoint_comments,
)
from workstream_linear_events import (
    EVENT_PREFIX, _canonical_event, encode_event_comment,
    encode_ledger_reservation, material_frontier, reduce_event_comments,
)
from workstream_linear_projection import (
    build_projection_event, reduce_projection_comments, select_plan_generation,
)
import workstream_material_repair as MODULE
import workstream_resume


STRICT_CANDIDATE = "2fcce119a856ca34e509c7fe45a8f03f1a0d982c9c4d77047600805c0fcb261f"


class FailingCreateClient(FakeCommentClient):
    def __init__(self, *, append_before_error=False):
        super().__init__()
        self.append_before_error = append_before_error

    def execute(self, query, variables):
        if "commentCreate" in query:
            if self.append_before_error:
                super().execute(query, variables)
            raise OSError("simulated lost HTTP response")
        return super().execute(query, variables)


class FinalPreflightRaceClient(FakeCommentClient):
    def __init__(self):
        super().__init__()
        self.material_reads = 0

    def execute(self, query, variables):
        if "query WorkstreamDeltaComments" in query:
            self.material_reads += 1
            if self.material_reads == 2:
                raced = Delta(
                    "raced-revision-58", "GEN-37", "progress", "other-writer",
                    {"progress": "concurrent"}, 57,
                    "2026-08-30T00:00:59Z",
                )
                self.comments.append({
                    "id": "raced-remote", "body": encode_event_comment(raced),
                    "createdAt": raced.created_at, "updatedAt": raced.created_at,
                })
        return super().execute(query, variables)


class MaterialRepairCliTests(unittest.TestCase):
    def setUp(self):
        fixture = linear_event_tests.LinearCommentEventAdapterTests()._repair_fixture()
        comments, payload, _checkpoint, _projection, _generation, route, source, _graph = fixture
        projection_comments = copy.deepcopy(comments[2:-1])
        material_comments = []
        for revision in range(55):
            event = Delta(
                f"history-{revision}", "GEN-37", "progress", "system",
                {"progress": f"historical {revision}"}, revision,
                f"2026-08-29T00:00:{revision:02d}Z",
            )
            material_comments.append({
                "id": f"history-remote-{revision}",
                "body": encode_event_comment(event),
            })
        originals = reduce_event_comments(
            comments[:2], workstream_id="GEN-37",
        ).events
        for offset, binding in enumerate(payload["target_bindings"]):
            original = originals[offset]
            event = Delta(
                original.event_id, original.workstream_id, original.kind,
                original.source, original.payload, 55 + offset, original.created_at,
            )
            canonical = _canonical_event(event)
            encoded = base64.urlsafe_b64encode(json.dumps(
                canonical, sort_keys=True, separators=(",", ":"),
            ).encode()).decode().rstrip("=")
            body = f"{EVENT_PREFIX}{encoded} -->"
            material_comments.append({"id": f"remote-{offset}", "body": body})
            binding.update({
                "comment_body_sha256": hashlib.sha256(body.encode()).hexdigest(),
                "canonical_event_sha256": canonical["sha256"],
                "original_expected_revision": 55 + offset,
                "original_index_zero_based": 55 + offset,
                "original_applied_revision": 56 + offset,
            })
        self.comments = material_comments + projection_comments
        self.payload = copy.deepcopy(payload)
        self.route = route
        self.source = source
        self.graph = {
            "root": {
                "id": route["root_issue_id"], "identifier": "GEN-37",
                "url": "https://linear.test/GEN-37", "title": "repair",
                "status": "In Progress", "status_type": "started",
                "state_id": "state-started", "plan_revision": "a" * 64,
                "revision": 57, "next_action": "stale",
                "team": {"id": route["team_id"],
                         "organization": {"id": route["workspace_id"]}},
                "project": {"id": route["project_id"]},
            },
            "children": [{
                "id": f"child-{number}", "identifier": f"GEN-{number}",
                "url": f"https://linear.test/GEN-{number}", "title": "child",
                "status": "In Progress", "status_type": "started",
                "state_id": "state-started", "next_action": "continue",
                "parent": {"id": route["root_issue_id"], "identifier": "GEN-37"},
                "team": {"id": route["team_id"],
                         "organization": {"id": route["workspace_id"]}},
                "project": {"id": route["project_id"]},
            } for number in (38, 39)],
            "decisions": [], "provenance": [],
        }
        raw = reduce_event_comments(self.comments, workstream_id="GEN-37")
        generation = select_plan_generation(
            self.comments, workstream_id="GEN-37",
            description_plan_revision="a" * 64,
            authenticated_route=route,
        )
        projection = reduce_projection_comments(
            self.comments, workstream_id="GEN-37",
            expected_plan_revision=generation["plan_revision"],
            authenticated_route=route, authenticated_source=source,
        )
        checkpoints = reduce_checkpoint_comments(
            self.comments, workstream_id="GEN-37",
        )
        self.payload.update({
            "raw_frontier": material_frontier(raw),
            "checkpoint_frontier": MODULE._checkpoint_repair_frontier(checkpoints),
            "projection_frontier": MODULE._projection_repair_frontier(projection),
            "generation": {
                "plan_revision": generation["plan_revision"],
                "transition_tip_event_id": generation["transition_tip_event_id"],
                "activation_epoch": generation["activation_epoch"],
                "authority_origin": generation["authority_origin"],
            },
            "issue_graph_frontier": MODULE._issue_graph_repair_frontier(
                self.graph, [], {},
            ),
            "strict_target_candidate_sha256": STRICT_CANDIDATE,
        })

    def _client(self, cls=FakeCommentClient):
        client = cls() if cls is not FakeCommentClient else FakeCommentClient()
        client.comments = copy.deepcopy(self.comments)
        client.workspace_id = self.route["workspace_id"]
        client.team_id = self.route["team_id"]
        client.project_id = self.route["project_id"]
        client.root_issue_id = self.route["root_issue_id"]
        return client

    def _invoke(self, argv, client, *, fetched_artifact=None, plan_source=None):
        transport = mock.Mock()
        transport.snapshot_for_root.return_value = copy.deepcopy(self.graph)
        stdout = io.StringIO()
        stderr = io.StringIO()
        artifact_path = Path(argv[argv.index("--review-artifact") + 1])
        artifact_bytes = artifact_path.read_bytes()
        remote_bytes = (
            artifact_bytes if fetched_artifact is None else fetched_artifact
        )
        with contextlib.ExitStack() as stack:
            stack.enter_context(mock.patch.object(
                MODULE, "source_bytes",
                side_effect=lambda identity, _canonical: (remote_bytes, identity),
            ))
            stack.enter_context(mock.patch.object(MODULE, "load_linear_api_key", return_value="token"))
            stack.enter_context(mock.patch.object(MODULE, "HttpGraphQLClient", return_value=client))
            stack.enter_context(mock.patch.object(MODULE, "resolve_linear_route", return_value=(None, None)))
            stack.enter_context(mock.patch.object(
                MODULE, "resolve_authenticated_issue_route", return_value=self.route,
            ))
            stack.enter_context(mock.patch.object(
                MODULE, "plan_payload", return_value={
                    "source": self.source if plan_source is None else plan_source,
                },
            ))
            stack.enter_context(mock.patch.object(
                MODULE, "LinearGraphQLTransport", return_value=transport,
            ))
            stack.enter_context(mock.patch.object(MODULE.sys, "argv", argv))
            stack.enter_context(mock.patch.object(MODULE.sys, "stdout", stdout))
            stack.enter_context(mock.patch.object(MODULE.sys, "stderr", stderr))
            code = MODULE.main()
        return code, stdout.getvalue(), stderr.getvalue()

    def _prepare(self, directory, client, *, plan_source=None):
        artifact_path = Path(directory) / "reviewed-targets.json"
        reviewed = {
            "schema_version": 1, "workstream_id": "GEN-37",
            "target_bindings": self.payload["target_bindings"],
        }
        artifact_bytes = json.dumps(reviewed, sort_keys=True).encode()
        artifact_path.write_bytes(artifact_bytes)
        self.payload["review_artifact"] = {
            "identity": "https://github.com/review/repo/blob/" + "1" * 40 + "/repairs/gen37.json",
            "repository": "github.com/review/repo", "commit": "1" * 40,
            "path": "repairs/gen37.json",
            "sha256": hashlib.sha256(artifact_bytes).hexdigest(),
            "reviewed_at": "2026-08-30T00:01:00Z",
        }
        seed = {
            key: self.payload[key] for key in (
                "schema_version", "workstream_id", "target_bindings",
                "authenticated_route", "authenticated_source", "generation",
                "review_artifact", "strict_target_candidate_sha256",
            )
        }
        payload_path = Path(directory) / "payload.json"
        payload_path.write_text(json.dumps(seed), encoding="utf-8")
        code, output, error = self._invoke([
            "workstream_material_repair.py", "GEN-37", "--manifest", str(payload_path),
            "--review-artifact", str(artifact_path), "--plan-source", "plan",
            "--prepare",
        ], client, plan_source=plan_source)
        self.assertEqual((code, error), (0, ""))
        outer = json.loads(output)
        manifest_path = Path(directory) / "manifest.json"
        manifest_path.write_text(json.dumps(outer), encoding="utf-8")
        return manifest_path, artifact_path, outer

    def test_prepare_normalizes_real_plan_source_bytes_without_write(self):
        client = self._client()
        with tempfile.TemporaryDirectory() as directory:
            _manifest_path, _artifact_path, outer = self._prepare(
                directory, client,
                plan_source={**self.source, "bytes": 65753},
            )
        self.assertEqual(outer["payload"]["authenticated_source"], self.source)
        self.assertNotIn("bytes", outer["payload"]["authenticated_source"])
        self.assertFalse(any(
            "commentCreate" in query for query, _variables in client.calls
        ))

    def test_identity_or_digest_source_drift_still_refuses_without_write(self):
        prepare_client = self._client()
        with tempfile.TemporaryDirectory() as directory:
            manifest_path, artifact_path, _outer = self._prepare(
                directory, prepare_client,
            )
            argv = [
                "workstream_material_repair.py", "GEN-37",
                "--manifest", str(manifest_path),
                "--review-artifact", str(artifact_path),
                "--plan-source", "plan",
            ]
            for changed in (
                {**self.source, "identity": "different-plan", "bytes": 65753},
                {**self.source, "sha256": "b" * 64, "bytes": 65753},
            ):
                client = self._client()
                with self.subTest(source=changed):
                    code, output, error = self._invoke(
                        argv, client, plan_source=changed,
                    )
                    self.assertEqual((code, output), (2, ""))
                    self.assertIn(
                        "material_semantic_repair_authenticated_source_drift",
                        error,
                    )
                    self.assertFalse(any(
                        "commentCreate" in query
                        for query, _variables in client.calls
                    ))

    def test_prepare_apply_replay_is_one_rev58_control_and_zero_duplicate(self):
        client = self._client()
        with tempfile.TemporaryDirectory() as directory:
            manifest_path, artifact_path, outer = self._prepare(directory, client)
            dry_argv = [
                "workstream_material_repair.py", "GEN-37", "--manifest", str(manifest_path),
                "--review-artifact", str(artifact_path), "--plan-source", "plan",
            ]
            code, output, error = self._invoke(dry_argv, client)
            self.assertEqual((code, error), (0, ""))
            preview = json.loads(output)
            for gate in (
                "production_compact_resume", "production_full_resume",
                "strict_generation_candidate",
            ):
                self.assertEqual(
                    preview["postwrite_validation"][gate],
                    "external_gate_required",
                )
            argv = [*dry_argv, "--apply"]
            code, output, error = self._invoke(argv, client)
            self.assertEqual((code, error), (0, ""))
            applied = json.loads(output)
            self.assertEqual(applied["expected_revision"], 57)
            self.assertEqual(applied["receipt"]["revision"], 58)
            self.assertEqual(applied["repair_count"], 2)
            self.assertEqual(applied["recovery_state"], "complete")
            for gate in (
                "production_compact_resume", "production_full_resume",
                "strict_generation_candidate",
            ):
                self.assertEqual(
                    applied["postwrite_validation"][gate],
                    "external_gate_required",
                )
            self.assertEqual(
                outer["payload"]["postwrite_oracle"]["strict_target_candidate_sha256"],
                STRICT_CANDIDATE,
            )
            resumed = workstream_resume.add_material_history(
                copy.deepcopy(self.graph), client.comments, "GEN-37",
                authenticated_route=self.route, authenticated_source=self.source,
            )
            compact = workstream_resume.compact_context(resumed, "GEN-37")
            full = workstream_resume.compact_context(
                resumed, "GEN-37", include_history=True, max_items=200,
                max_bytes=100 * 1024,
            )
            self.assertEqual(compact["next_action"], "new")
            self.assertEqual(compact["material_semantic_repair"]["count"], 2)
            self.assertEqual(len(full["raw_material_events"]), 58)
            self.assertEqual(len(full["material_semantic_repairs"]), 2)
            writes = len([call for call in client.calls if "commentCreate" in call[0]])
            code, output, error = self._invoke(argv, client)
            self.assertEqual((code, error), (0, ""))
            replay = json.loads(output)
            self.assertTrue(replay["replay"])
            self.assertEqual(replay["postwrite_validation"]["exact_manifest_replay"], "valid")
            for gate in (
                "production_compact_resume", "production_full_resume",
                "strict_generation_candidate",
            ):
                self.assertEqual(
                    replay["postwrite_validation"][gate],
                    "external_gate_required",
                )
            self.assertEqual(
                len([call for call in client.calls if "commentCreate" in call[0]]), writes,
            )

    def test_successor_generation_source_projection_child_and_old_manifest_replay(self):
        client = self._client()
        with tempfile.TemporaryDirectory() as directory:
            manifest_path, artifact_path, _outer = self._prepare(directory, client)
            argv = [
                "workstream_material_repair.py", "GEN-37",
                "--manifest", str(manifest_path),
                "--review-artifact", str(artifact_path),
                "--plan-source", "plan", "--apply",
            ]
            code, _output, error = self._invoke(argv, client)
            self.assertEqual((code, error), (0, ""))

            successor = generation_tests.FakeClient()
            successor.comments = copy.deepcopy(client.comments)
            loader = generation_tests.Loader(successor)
            generation_tests.project_full(
                successor, generation_tests.NEW,
                identity=f"https://example.test/{generation_tests.NEW}",
            )
            old_state = generation_tests.adapter(
                successor, generation_tests.OLD,
            ).state()
            retirement = build_retirement_proof(
                predecessor_plan_revision=generation_tests.OLD,
                retired_at="2026-08-30T01:00:00Z", retired_writer_epoch=0,
                provenance_event_ids=[
                    event["event_id"] for event in old_state.events
                    if event["kind"] == "provenance"
                ],
                checkpoint_event_ids=[],
            )
            GenerationTransport(
                successor, issue_id="GEN-37", workstream_id="GEN-37",
                authority=generation_tests.AUTHORITY,
                candidate_loader=loader,
                legacy_description_plan_revision=generation_tests.OLD,
            ).activate(
                target_plan_revision=generation_tests.NEW,
                created_at="2026-08-30T01:00:01Z", retirement=retirement,
            )
            target = generation_tests.adapter(successor, generation_tests.NEW)
            successor_source = {
                "identity": f"https://example.test/{generation_tests.NEW}",
                "sha256": generation_tests.NEW,
            }
            successor_plan = {
                "graph_review_required": True,
                "source": {**successor_source, "bytes": 10},
                "root": {"plan_revision": generation_tests.NEW},
                "children": [{
                    "key": "successor-child", "title": "Successor child",
                    "next_action": "Validate the repaired successor.",
                    "description": (
                        "**Successor child.** Validate the repaired successor."
                    ),
                    "content_schema_version": 1,
                }],
            }
            child_result = generation_tests.LinearGraphQLTransport(
                successor, team_id=self.route["team_id"],
                workspace_id=self.route["workspace_id"],
                project_id=self.route["project_id"],
            ).extend_existing_root_reviewed_child(
                successor_plan, root_issue_id=self.route["root_issue_id"],
                reviewed_candidate_key="successor-child",
                source_revision=generation_tests.NEW,
                plan_revision=generation_tests.NEW,
                expected_frontier={
                    "material_revision": 58,
                    "projection_revision": target.state().revision,
                }, state_id="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
                assignee_id=None, unassigned=True,
                authorization_adapter=target,
            )
            self.assertEqual(child_result["receipt"]["disposition"], "created")
            self.assertEqual(len(successor.children), 1)
            successor.children[0]["state"] = {
                "id": "cccccccc-cccc-4ccc-8ccc-cccccccccccc",
                "name": "In Progress", "type": "started",
            }
            successor.children[0]["updatedAt"] = "transitioned"
            successor_projection = target.state()
            scope_event = next(
                event for event in successor_projection.events
                if event["kind"] == "scope"
            )
            evolved_scope = copy.deepcopy(successor_projection.snapshot["scope"])
            evolved_scope["child_ownership"]["GEN-38"] = "github.com:id:R_repo"
            target.append(build_projection_event(
                workstream_id="GEN-37", kind="scope", key="root",
                value=evolved_scope, plan_revision=generation_tests.NEW,
                expected_revision=successor_projection.revision,
                created_at="2026-08-30T01:00:02Z",
                supersedes_event_id=scope_event["event_id"],
                authority=self.route,
            ))
            original_execute = successor.execute

            def execute_with_resume_children(query, variables):
                if "query WorkstreamResumeRoot" in query:
                    children = copy.deepcopy(successor.children)
                    for child in children:
                        child["comments"] = {
                            "nodes": [], "pageInfo": {
                                "hasNextPage": False, "endCursor": None,
                            },
                        }
                    return {"issue": {**successor.root_issue(), "children": {
                        "nodes": children, "pageInfo": {
                            "hasNextPage": False, "endCursor": None,
                        },
                    }}}
                return original_execute(query, variables)

            successor.execute = execute_with_resume_children

            def production_resume(include_history=False):
                stdout, stderr = io.StringIO(), io.StringIO()
                resume_argv = [
                    "workstream_resume.py", "GEN-37", "--plan-source", "plan",
                    "--max-items", "400", "--max-bytes", str(300 * 1024),
                ]
                if include_history:
                    resume_argv.append("--include-history")
                with mock.patch.object(
                    workstream_resume, "load_linear_api_key", return_value="token",
                ), mock.patch.object(
                    workstream_resume, "HttpGraphQLClient", return_value=successor,
                ), mock.patch.object(
                    workstream_resume, "resolve_linear_route", return_value=(None, None),
                ), mock.patch.object(
                    workstream_resume, "resolve_authenticated_issue_route",
                    return_value=self.route,
                ), mock.patch.object(
                    workstream_resume, "plan_payload",
                    return_value={"source": {**successor_source, "bytes": 10}},
                ), mock.patch.object(
                    workstream_resume.sys, "argv", resume_argv,
                ), mock.patch.object(
                    workstream_resume.sys, "stdout", stdout,
                ), mock.patch.object(
                    workstream_resume.sys, "stderr", stderr,
                ):
                    code = workstream_resume.main()
                self.assertEqual((code, stderr.getvalue()), (0, ""))
                return json.loads(stdout.getvalue())

            mutations_before_resume = len(successor.mutations)
            compact = production_resume()
            full = production_resume(include_history=True)
            self.assertEqual(compact["resume_authority"], "full")
            self.assertEqual(compact["plan_revision"], generation_tests.NEW)
            self.assertEqual(compact["children"][0]["status"], "In Progress")
            self.assertTrue(any(
                event["kind"] == "child_extension_authorization"
                for event in full["projection_events"]
            ))
            self.assertEqual(len(successor.mutations), mutations_before_resume)

            mutations_before = len(successor.mutations)
            code, output, error = self._invoke(argv, successor)
            self.assertEqual((code, error), (0, ""))
            replay = json.loads(output)
            self.assertTrue(replay["replay"])
            self.assertEqual(
                replay["postwrite_validation"]["repair_reducer"],
                "valid_historical_proof",
            )
            self.assertEqual(len(successor.mutations), mutations_before)

    def test_exact_manifest_replay_rejects_any_remote_control_body_edit_zero_write(self):
        for mutation in (
            "prefix_prose", "suffix_prose", "leading_whitespace",
            "trailing_newline", "second_marker",
        ):
            client = self._client()
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as directory:
                manifest_path, artifact_path, outer = self._prepare(directory, client)
                argv = [
                    "workstream_material_repair.py", "GEN-37",
                    "--manifest", str(manifest_path),
                    "--review-artifact", str(artifact_path),
                    "--plan-source", "plan", "--apply",
                ]
                code, _output, error = self._invoke(argv, client)
                self.assertEqual((code, error), (0, ""))
                slot = outer["control"]["remote_slot_id"]
                pinned = [item for item in client.comments if item["id"] == slot]
                self.assertEqual(len(pinned), 1)
                original = pinned[0]["body"]
                pinned[0]["body"] = {
                    "prefix_prose": "reviewed prose\n" + original,
                    "suffix_prose": original + "\nreviewed prose",
                    "leading_whitespace": " " + original,
                    "trailing_newline": original + "\n",
                    "second_marker": original + "\n" + original,
                }[mutation]
                writes = len([
                    call for call in client.calls if "commentCreate" in call[0]
                ])
                code, output, error = self._invoke(argv, client)
                self.assertEqual((code, output), (2, ""))
                self.assertTrue(
                    "material_repair_pinned_comment_body_mismatch" in error
                    or "exactly one v1 marker" in error
                    or "malformed_event_marker" in error,
                    error,
                )
                self.assertEqual(len([
                    call for call in client.calls if "commentCreate" in call[0]
                ]), writes)

    def test_lost_response_is_classified_and_exact_manifest_reconciles(self):
        prepare_client = self._client()
        with tempfile.TemporaryDirectory() as directory:
            manifest_path, artifact_path, _outer = self._prepare(directory, prepare_client)
            argv = [
                "workstream_material_repair.py", "GEN-37", "--manifest", str(manifest_path),
                "--review-artifact", str(artifact_path), "--plan-source", "plan", "--apply",
            ]
            unknown = FailingCreateClient(append_before_error=False)
            unknown.comments = copy.deepcopy(self.comments)
            unknown.workspace_id, unknown.team_id = self.route["workspace_id"], self.route["team_id"]
            unknown.project_id, unknown.root_issue_id = self.route["project_id"], self.route["root_issue_id"]
            code, output, _error = self._invoke(argv, unknown)
            self.assertEqual(code, 3)
            self.assertEqual(json.loads(output)["recovery_state"], "outcome_unknown_replay_required")

            observed = FailingCreateClient(append_before_error=True)
            observed.comments = copy.deepcopy(self.comments)
            observed.workspace_id, observed.team_id = self.route["workspace_id"], self.route["team_id"]
            observed.project_id, observed.root_issue_id = self.route["project_id"], self.route["root_issue_id"]
            code, output, _error = self._invoke(argv, observed)
            self.assertEqual(code, 3)
            partial = json.loads(output)
            self.assertEqual(partial["recovery_state"], "durable_partial_replay_required")
            self.assertIsNotNone(partial["receipt"])
            observed.append_before_error = False
            code, output, error = self._invoke(argv, observed)
            self.assertEqual((code, error), (0, ""))
            self.assertTrue(json.loads(output)["replay"])

    def test_internal_preappend_read_oserror_is_known_zero_write_refusal(self):
        prepare_client = self._client()
        with tempfile.TemporaryDirectory() as directory:
            manifest_path, artifact_path, _outer = self._prepare(
                directory, prepare_client,
            )
            client = self._client()
            original_execute = client.execute
            reads = 0

            def fail_internal_read(query, variables):
                nonlocal reads
                if "query WorkstreamDeltaComments" in query:
                    reads += 1
                    if reads == 3:
                        raise OSError("preappend read unavailable")
                return original_execute(query, variables)

            client.execute = fail_internal_read
            code, output, error = self._invoke([
                "workstream_material_repair.py", "GEN-37",
                "--manifest", str(manifest_path),
                "--review-artifact", str(artifact_path),
                "--plan-source", "plan", "--apply",
            ], client)
            self.assertEqual((code, output), (2, ""))
            self.assertIn("pinned_repair_prewrite_unavailable", error)
            self.assertFalse(any(
                "commentCreate" in call[0] for call in client.calls
            ))

    def test_review_artifact_digest_or_content_mismatch_refuses_before_remote_read(self):
        with tempfile.TemporaryDirectory() as directory:
            artifact = Path(directory) / "artifact.json"
            artifact.write_text("{}", encoding="utf-8")
            payload = copy.deepcopy(self.payload)
            payload["review_artifact"]["sha256"] = hashlib.sha256(b"{}").hexdigest()
            with self.assertRaisesRegex(ValueError, "content_mismatch"):
                MODULE._verify_review_artifact(payload, str(artifact))

    def test_final_complete_preflight_race_refuses_with_zero_writes(self):
        prepare_client = self._client()
        with tempfile.TemporaryDirectory() as directory:
            manifest_path, artifact_path, _outer = self._prepare(directory, prepare_client)
            racing = FinalPreflightRaceClient()
            racing.comments = copy.deepcopy(self.comments)
            racing.workspace_id, racing.team_id = self.route["workspace_id"], self.route["team_id"]
            racing.project_id, racing.root_issue_id = self.route["project_id"], self.route["root_issue_id"]
            code, _output, error = self._invoke([
                "workstream_material_repair.py", "GEN-37",
                "--manifest", str(manifest_path),
                "--review-artifact", str(artifact_path),
                "--plan-source", "plan", "--apply",
            ], racing)
            self.assertEqual(code, 2)
            self.assertIn("final_prewrite_fence_drift", error)
            self.assertFalse(any("commentCreate" in call[0] for call in racing.calls))

    def test_pinned_apply_refuses_checkpoint_or_reservation_internal_read_race(self):
        for race_kind in ("checkpoint", "reservation"):
            prepare_client = self._client()
            with self.subTest(race_kind=race_kind), tempfile.TemporaryDirectory() as directory:
                manifest_path, artifact_path, outer = self._prepare(
                    directory, prepare_client,
                )
                racing = self._client()
                expected_slot = outer["control"]["remote_slot_id"]
                if race_kind == "checkpoint":
                    checkpoint = build_checkpoint(
                        workstream_id="GEN-37", boundary_id="repair-race",
                        root_revision=57, plan_revision="a" * 64,
                        before_status="In Progress", after_status="In Progress",
                        execution={
                            "agent": "codex", "provider": "openai",
                            "session_id": "race", "machine": "test",
                            "worktree": {"state": "safe", "path": "/repo",
                                         "branch": "main", "head": "1" * 40},
                        },
                        exact_head="1" * 40, evidence=[], blocker=None,
                        next_action="continue", predecessor_event_id=None,
                    )
                    planted = encode_checkpoint_comment(checkpoint)
                else:
                    projection = reduce_projection_comments(
                        racing.comments, workstream_id="GEN-37",
                        expected_plan_revision="a" * 64,
                        authenticated_route=self.route,
                        authenticated_source=self.source,
                    )
                    scope_event = next(
                        event for event in projection.events
                        if event["kind"] == "scope"
                    )
                    intended = build_projection_event(
                        workstream_id="GEN-37", kind="scope", key="root",
                        value=projection.snapshot["scope"],
                        plan_revision="a" * 64,
                        expected_revision=projection.revision,
                        created_at="2026-08-30T00:01:01Z",
                        supersedes_event_id=scope_event["event_id"],
                        authority=self.route,
                    )
                    reservation = {
                        "schema_version": 1, "workstream_id": "GEN-37",
                        "material_revision": 57,
                        "intent_kind": "repository_identity_projection",
                        "plan_revision": "a" * 64,
                        "projection_revision": projection.revision,
                        "projection_frontier_ids": [
                            projection.remote_ids[event["event_id"]]
                            for event in projection.events
                        ],
                        "frontier_ids": [], "authority": self.route,
                        "intent_event": intended,
                        "intent_sha256": hashlib.sha256(json.dumps(
                            intended, sort_keys=True, separators=(",", ":"),
                        ).encode()).hexdigest(),
                    }
                    planted = encode_ledger_reservation(reservation)
                original_execute = racing.execute
                reads = 0

                def inject_on_internal_read(query, variables):
                    nonlocal reads
                    if "query WorkstreamDeltaComments" in query:
                        reads += 1
                        if reads == 3:
                            racing.comments.append({
                                "id": expected_slot, "body": planted,
                                "createdAt": "race", "updatedAt": "race",
                            })
                    return original_execute(query, variables)

                racing.execute = inject_on_internal_read
                code, _output, error = self._invoke([
                    "workstream_material_repair.py", "GEN-37",
                    "--manifest", str(manifest_path),
                    "--review-artifact", str(artifact_path),
                    "--plan-source", "plan", "--apply",
                ], racing)
                self.assertEqual(code, 2)
                self.assertIn(
                    "pinned_repair_serialization_frontier_drift"
                    if race_kind == "checkpoint"
                    else "pinned_repair_serialization_authority_drift",
                    error,
                )
                self.assertFalse(any(
                    "commentCreate" in call[0] for call in racing.calls
                ))

    def test_review_artifact_requires_authenticated_remote_byte_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "targets.json"
            reviewed = {
                "schema_version": 1, "workstream_id": "GEN-37",
                "target_bindings": self.payload["target_bindings"],
            }
            material = json.dumps(reviewed, sort_keys=True).encode()
            path.write_bytes(material)
            identity = (
                "https://github.com/danielraffel/pulp-planning/blob/"
                "d72cba77c7841e4d866dc3825fa134a5d1d43730/targets.json"
            )
            payload = {**reviewed, "review_artifact": {
                "identity": identity,
                "repository": "github.com/danielraffel/pulp-planning",
                "commit": "d72cba77c7841e4d866dc3825fa134a5d1d43730",
                "path": "targets.json",
                "sha256": hashlib.sha256(material).hexdigest(),
                "reviewed_at": "2026-08-30T11:13:05Z",
            }}
            with mock.patch.object(
                MODULE, "source_bytes", return_value=(material, identity),
            ) as fetch:
                MODULE._verify_review_artifact(payload, str(path))
            fetch.assert_called_once_with(identity, identity)
            for fetched, fetched_identity in (
                (b"tampered", identity),
                (material, identity + "?wrong"),
            ):
                with self.subTest(fetched_identity=fetched_identity), \
                     mock.patch.object(
                         MODULE, "source_bytes",
                         return_value=(fetched, fetched_identity),
                     ), self.assertRaisesRegex(ValueError, "remote_mismatch"):
                    MODULE._verify_review_artifact(payload, str(path))

    def test_review_artifact_requires_canonical_immutable_github_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "targets.json"
            reviewed = {
                "schema_version": 1, "workstream_id": "GEN-37",
                "target_bindings": self.payload["target_bindings"],
            }
            material = json.dumps(reviewed, sort_keys=True).encode()
            path.write_bytes(material)
            base = {
                "identity": (
                    "https://github.com/review/repo/blob/" + "1" * 40
                    + "/repairs/targets.json"
                ),
                "repository": "github.com/review/repo", "commit": "1" * 40,
                "path": "repairs/targets.json",
                "sha256": hashlib.sha256(material).hexdigest(),
                "reviewed_at": "2026-08-30T11:13:05Z",
            }
            hostile = (
                {"repository": "evil.example/review/repo",
                 "identity": "https://evil.example/review/repo/blob/" + "1" * 40 + "/repairs/targets.json"},
                {"repository": "github.com.evil/review/repo",
                 "identity": "https://github.com.evil/review/repo/blob/" + "1" * 40 + "/repairs/targets.json"},
                {"repository": "github.com/Review/repo",
                 "identity": "https://github.com/Review/repo/blob/" + "1" * 40 + "/repairs/targets.json"},
                {"commit": "A" * 40,
                 "identity": "https://github.com/review/repo/blob/" + "A" * 40 + "/repairs/targets.json"},
                {"commit": "main",
                 "identity": "https://github.com/review/repo/blob/main/repairs/targets.json"},
                {"path": "../targets.json",
                 "identity": "https://github.com/review/repo/blob/" + "1" * 40 + "/../targets.json"},
                {"path": "repairs//targets.json",
                 "identity": "https://github.com/review/repo/blob/" + "1" * 40 + "/repairs//targets.json"},
                {"path": "repairs/%2e%2e/targets.json",
                 "identity": "https://github.com/review/repo/blob/" + "1" * 40 + "/repairs/%2e%2e/targets.json"},
                {"path": "repairs/targets.json?raw=1",
                 "identity": "https://github.com/review/repo/blob/" + "1" * 40 + "/repairs/targets.json?raw=1"},
                {"identity": base["identity"] + "?raw=1"},
            )
            for mutation in hostile:
                artifact = {**base, **mutation}
                payload = {**reviewed, "review_artifact": artifact}
                with self.subTest(mutation=mutation), mock.patch.object(
                    MODULE, "source_bytes",
                ) as fetch, self.assertRaisesRegex(ValueError, "identity_mismatch"):
                    MODULE._verify_review_artifact(payload, str(path))
                fetch.assert_not_called()

    def test_remote_review_artifact_mismatch_refuses_before_linear_read(self):
        prepare_client = self._client()
        with tempfile.TemporaryDirectory() as directory:
            manifest_path, artifact_path, _outer = self._prepare(
                directory, prepare_client,
            )
            client = self._client()
            code, output, error = self._invoke([
                "workstream_material_repair.py", "GEN-37",
                "--manifest", str(manifest_path),
                "--review-artifact", str(artifact_path),
                "--plan-source", "plan", "--apply",
            ], client, fetched_artifact=b"remote tamper")
            self.assertEqual((code, output), (2, ""))
            self.assertIn("review_artifact_remote_mismatch", error)
            self.assertEqual(client.calls, [])


if __name__ == "__main__":
    unittest.main()
