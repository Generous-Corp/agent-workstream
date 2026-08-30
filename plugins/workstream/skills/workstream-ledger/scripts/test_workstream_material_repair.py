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
from workstream_delta import Delta
from workstream_checkpoint import build_checkpoint
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

    def _invoke(self, argv, client):
        transport = mock.Mock()
        transport.snapshot_for_root.return_value = copy.deepcopy(self.graph)
        stdout = io.StringIO()
        stderr = io.StringIO()
        with contextlib.ExitStack() as stack:
            stack.enter_context(mock.patch.object(MODULE, "load_linear_api_key", return_value="token"))
            stack.enter_context(mock.patch.object(MODULE, "HttpGraphQLClient", return_value=client))
            stack.enter_context(mock.patch.object(MODULE, "resolve_linear_route", return_value=(None, None)))
            stack.enter_context(mock.patch.object(
                MODULE, "resolve_authenticated_issue_route", return_value=self.route,
            ))
            stack.enter_context(mock.patch.object(
                MODULE, "plan_payload", return_value={"source": self.source},
            ))
            stack.enter_context(mock.patch.object(
                MODULE, "LinearGraphQLTransport", return_value=transport,
            ))
            stack.enter_context(mock.patch.object(MODULE.sys, "argv", argv))
            stack.enter_context(mock.patch.object(MODULE.sys, "stdout", stdout))
            stack.enter_context(mock.patch.object(MODULE.sys, "stderr", stderr))
            code = MODULE.main()
        return code, stdout.getvalue(), stderr.getvalue()

    def _prepare(self, directory, client):
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
        ], client)
        self.assertEqual((code, error), (0, ""))
        outer = json.loads(output)
        manifest_path = Path(directory) / "manifest.json"
        manifest_path.write_text(json.dumps(outer), encoding="utf-8")
        return manifest_path, artifact_path, outer

    def test_prepare_apply_replay_is_one_rev58_control_and_zero_duplicate(self):
        client = self._client()
        with tempfile.TemporaryDirectory() as directory:
            manifest_path, artifact_path, outer = self._prepare(directory, client)
            argv = [
                "workstream_material_repair.py", "GEN-37", "--manifest", str(manifest_path),
                "--review-artifact", str(artifact_path), "--plan-source", "plan", "--apply",
            ]
            code, output, error = self._invoke(argv, client)
            self.assertEqual((code, error), (0, ""))
            applied = json.loads(output)
            self.assertEqual(applied["expected_revision"], 57)
            self.assertEqual(applied["receipt"]["revision"], 58)
            self.assertEqual(applied["repair_count"], 2)
            self.assertEqual(applied["recovery_state"], "complete")
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
            self.assertEqual(
                len([call for call in client.calls if "commentCreate" in call[0]]), writes,
            )

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

    def test_live_reviewed_gen37_target_artifact_matches_immutable_authority(self):
        path = Path(
            "/Users/danielraffel/Code/pulp-planning-gen37-material-repair-20260830/"
            "artifacts/2026-08-30-gen37-material-semantic-repair-targets.json"
        )
        if not path.exists():
            self.skipTest("local immutable planning artifact unavailable")
        reviewed = json.loads(path.read_text(encoding="utf-8"))
        payload = {**reviewed, "review_artifact": {
            "identity": (
                "https://github.com/danielraffel/pulp-planning/blob/"
                "d72cba77c7841e4d866dc3825fa134a5d1d43730/"
                "artifacts/2026-08-30-gen37-material-semantic-repair-targets.json"
            ),
            "repository": "github.com/danielraffel/pulp-planning",
            "commit": "d72cba77c7841e4d866dc3825fa134a5d1d43730",
            "path": "artifacts/2026-08-30-gen37-material-semantic-repair-targets.json",
            "sha256": "3ad4d9e1e8344727f3c56086a06f7857ee35d578a0630012be393b31b4ba6c12",
            "reviewed_at": "2026-08-30T11:13:05Z",
        }}
        MODULE._verify_review_artifact(payload, str(path))


if __name__ == "__main__":
    unittest.main()
