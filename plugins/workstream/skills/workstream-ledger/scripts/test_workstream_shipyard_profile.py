#!/usr/bin/env python3

from copy import deepcopy
import hashlib
import io
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

import workstream_shipyard_profile as MODULE
from workstream_child_dependencies import (
    dependency_relation_id, dependency_material_frontier_sha256,
)
from workstream_linear_projection import (
    build_projection_event, encode_projection_comment, projection_slot_id,
)


HEAD = "1" * 40
PLAN = "a" * 64


class ShipyardProfileTests(unittest.TestCase):
    def git(self, root: Path) -> MODULE.GitIdentity:
        return MODULE.GitIdentity(
            root=root,
            repository_coordinate="github.com/generous-corp/agent-workstream",
            repository="generous-corp/agent-workstream",
            head=HEAD,
            branch="feature/profile",
        )

    def context(
        self, root: Path, *, provider: str = "codex", service: str = "openai",
    ) -> dict:
        worktree = {
            "state": "safe",
            "path": str(root),
            "branch": "feature/profile",
            "head": HEAD,
        }
        checkpoint_id = "wsc_" + "2" * 32
        checkpoint = {
            "workstream_id": "GEN-37",
            "checkpoint_event_id": checkpoint_id,
            "root_revision": 3,
            "plan_revision": PLAN,
            "status": {"before": "In Progress", "after": "In Progress"},
            "exact_head": HEAD,
            "blocker": None,
            "next_action": "hand exact head to Shipyard",
            "worktree": worktree,
            "acknowledgement": {
                "state": "remote_acknowledged",
                "remote_id": "linear-comment-3",
                "applied_revision": 3,
            },
            "evidence": {
                "count": 1,
                "items": [{"kind": "focused_test", "id": "profile-contract"}],
                "sha256": "3" * 64,
            },
            "provenance": {
                "count": 2,
                "latest": {
                    "event_id": checkpoint_id,
                    "agent": provider,
                    "provider": service,
                    "session_id": "provider-session-7",
                    "machine": "M5",
                    "worktree": worktree,
                },
                "sha256": "4" * 64,
            },
        }
        return {
            "workstream_id": "GEN-37",
            "context_url": "https://linear.app/generous-corp/issue/GEN-37/profile",
            "plan_revision": PLAN,
            "root_revision": 3,
            "issue_revision": 9,
            "status": "In Progress",
            "next_action": "hand exact head to Shipyard",
            "children": [],
            "decisions": [],
            "choice_events": [],
            "scope": {
                "namespace": "generous-corp",
                "primary_repository": "github.com:id:R_agent_workstream",
                "linear": {
                    "workspace_id": "workspace",
                    "team_id": "team",
                    "project_id": "project",
                    "root_issue_id": "33333333-3333-4333-8333-333333333333",
                },
                "repositories": [{
                    "slug": "github.com/generous-corp/agent-workstream",
                    "aliases": [],
                    "exact_head": HEAD,
                    "provider_repository_id": "R_agent_workstream",
                }],
            },
            "relations": [],
            "dependency_graph": {
                "schema_version": 1,
                "authority": "child_dependency_authorization",
                "plan_revision": PLAN,
                "route": {
                    "workspace_id": "workspace",
                    "team_id": "team",
                    "project_id": "project",
                    "root_issue_id": "33333333-3333-4333-8333-333333333333",
                },
                "revision": 0,
                "sha256": hashlib.sha256(b"[]").hexdigest(),
                "authorization_batches": [],
                "relations": [],
                "native_readback": "relations_and_inverseRelations",
                "ignored_non_dependency_count": 0,
                "observed_frontier": {
                    "material_revision": 3,
                    "projection_revision": 4,
                    "graph_revision": 0,
                    "graph_sha256": hashlib.sha256(b"[]").hexdigest(),
                },
                "root_readback_sha256": "6" * 64,
            },
            "evidence_contracts": [],
            "surface_availability": {
                "scope": "available",
                "relations": "available",
                "choice_events": "available",
                "evidence_contracts": "available",
                "material_events": "available",
                "dependency_graph": "available",
                "latest_checkpoint": "available",
            },
            "provenance": [{"kind": "plan", "sha256": PLAN}],
            "material_event_revision": 3,
            "latest_checkpoint": checkpoint,
            "checkpoint_recovery": {"state": "current", "stale_plan_count": 0},
            "uncheckpointed_material_obligations": [],
            "source": {"identity": "plan:demo", "sha256": PLAN},
            "disposition": {
                "disposition": "attach",
                "remote_head": HEAD,
                "recovered_from_checkpoint": checkpoint_id,
            },
            "projection_revision": 4,
            "projection_recovery": {"state": "current", "stale_plan_count": 0},
            "lifecycle_recovery": None,
            "projection_quarantine": {
                "count": 0,
                "sha256": "5" * 64,
                "latest": None,
            },
            "quarantine_disposition": None,
            "authenticated_route": {
                "workspace_id": "workspace",
                "team_id": "team",
                "project_id": "project",
                "root_issue_id": "33333333-3333-4333-8333-333333333333",
            },
            "authenticated_source": {"identity": "plan:demo", "sha256": PLAN},
            "history": {"included": False},
            "resume_authority": "full",
        }

    def test_codex_profile_is_exact_deterministic_shipyard_v1(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            context = self.context(root)
            profile = MODULE.build_launch_profile(
                context, "GEN-37", self.git(root),
                model="gpt-5.6-sol", reasoning_effort="medium",
            )
            repeated = MODULE.build_launch_profile(
                deepcopy(context), "GEN-37", self.git(root),
                model="gpt-5.6-sol", reasoning_effort="medium",
            )

        self.assertEqual(profile, repeated)
        self.assertEqual(profile["schema_version"], 1)
        self.assertEqual(profile["provider"], {
            "provider_id": "codex",
            "model_id": "gpt-5.6-sol",
            "reasoning_effort": "medium",
        })
        self.assertEqual(profile["launch_argv"], [
            "codex", "--model", "gpt-5.6-sol", "-c",
            'model_reasoning_effort="medium"',
        ])
        self.assertEqual(profile["resume_argv"], [
            "codex", "resume", "--model", "gpt-5.6-sol", "-c",
            'model_reasoning_effort="medium"', "provider-session-7",
        ])
        self.assertEqual(profile["checkpoint"]["generation"], 2)
        self.assertEqual(
            profile["continuation_bootstrap"]["dependency_graph_sha256"],
            hashlib.sha256(b"[]").hexdigest(),
        )
        self.assertRegex(
            profile["continuation_bootstrap"]["dependency_graph_digest"],
            r"^[0-9a-f]{64}$",
        )
        self.assertEqual(
            profile["continuation_bootstrap"]["expected_resume_context_digest"],
            MODULE._resume_context_digest(context),
        )
        self.assertRegex(profile["checkpoint"]["digest"], r"^[0-9a-f]{64}$")
        self.assertRegex(
            profile["continuation_bootstrap"]["success_continuation_digest"],
            r"^[0-9a-f]{64}$",
        )
        self.assertNotEqual(
            profile["continuation_bootstrap"]["success_continuation_digest"],
            profile["continuation_bootstrap"]["failure_continuation_digest"],
        )
        self.assertEqual(profile["recovery_policy"], "exact_session_then_fresh_checkpoint")

    def test_resume_digest_is_stable_across_child_connection_order(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            context = self.context(root)
            context["children"] = [
                {"identifier": "GEN-39", "title": "second"},
                {"identifier": "GEN-38", "title": "first"},
            ]
            reversed_context = deepcopy(context)
            reversed_context["children"].reverse()
            first = MODULE.build_launch_profile(
                context, "GEN-37", self.git(root),
                model="gpt-5.6-sol", reasoning_effort="medium",
            )
            second = MODULE.build_launch_profile(
                reversed_context, "GEN-37", self.git(root),
                model="gpt-5.6-sol", reasoning_effort="medium",
            )
            self.assertEqual(first, second)

    def test_launch_profile_rejects_missing_null_or_tampered_dependency_graph(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            for mutation in ("missing", "null", "body"):
                with self.subTest(mutation=mutation):
                    context = self.context(root)
                    if mutation == "missing":
                        context.pop("dependency_graph")
                    elif mutation == "null":
                        context["dependency_graph"] = None
                    else:
                        context["dependency_graph"]["sha256"] = "f" * 64
                    with self.assertRaisesRegex(
                        MODULE.ShipyardProfileError,
                        "resume_dependency_graph_(missing|invalid)",
                    ):
                        MODULE.build_launch_profile(
                            context, "GEN-37", self.git(root),
                            model="gpt-5.6-sol", reasoning_effort="medium",
                        )

    def test_launch_profile_rejects_semantically_forged_dependency_authority(self):
        def digest(value):
            return hashlib.sha256(json.dumps(
                value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
            ).encode()).hexdigest()

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            context = self.context(root)
            authority = {
                **context["authenticated_route"], "root_identifier": "GEN-37",
            }
            blocker = {
                "issue_id": "20000000-0000-4000-8000-000000000001",
                "identifier": "GEN-43",
            }
            blocked = {
                "issue_id": "20000000-0000-4000-8000-000000000002",
                "identifier": "GEN-44",
            }
            repository_key = "github.com:id:R_agent_workstream"
            context["scope"]["child_ownership"] = {
                "GEN-43": repository_key, "GEN-44": repository_key,
            }
            context["children"] = [
                {"id": blocker["issue_id"], "identifier": blocker["identifier"]},
                {"id": blocked["issue_id"], "identifier": blocked["identifier"]},
            ]
            relation = {
                "id": dependency_relation_id(
                    authority=authority, blocker=blocker, blocked=blocked,
                ),
                "type": "blocks", "blocker": blocker, "blocked": blocked,
                "inverse_type": "blocked_by",
            }
            before = {
                "material_revision": 0, "projection_revision": 0,
                "graph_revision": 0, "graph_sha256": digest([]),
            }
            batch_id = "wsdb_" + digest([
                "workstream-child-dependency-native-batch-v1", authority,
                PLAN, before, [relation],
            ])[:32]
            route = context["authenticated_route"]
            event = build_projection_event(
                workstream_id="GEN-37", kind="child_dependency_authorization",
                key=batch_id, plan_revision=PLAN, expected_revision=0,
                created_at="2026-08-31T00:00:00Z", authority=route,
                value={
                    "root_issue_id": route["root_issue_id"], "route": route,
                    "plan_revision": PLAN, "batch_id": batch_id,
                    "relation_ids": [relation["id"]],
                    "relations_sha256": digest([relation]),
                    "expected_material_revision": 0,
                    "expected_material_frontier_sha256": (
                        dependency_material_frontier_sha256([], {}, [], revision=0)
                    ),
                    "expected_projection_revision": 0,
                    "expected_graph_revision": 0,
                    "expected_graph_sha256": digest([]),
                    "initial_state": "owned_children_validated",
                },
            )
            comment = {
                "id": projection_slot_id("GEN-37", PLAN, 0, route),
                "body": encode_projection_comment(event),
                "createdAt": "2026-08-31T00:00:00Z",
                "updatedAt": "2026-08-31T00:00:00Z",
            }
            context["material_event_revision"] = 0
            context["root_revision"] = 0
            context["latest_checkpoint"]["root_revision"] = 0
            context["projection_revision"] = 1
            context["dependency_graph"].update({
                "revision": 1, "sha256": digest([relation]),
                "relations": [relation],
                "authorization_batches": [{
                    "batch_id": batch_id, "event_id": event["event_id"],
                    "relation_ids": [relation["id"]],
                    "relations_sha256": digest([relation]),
                    "expected_material_revision": 0,
                    "expected_material_frontier_sha256": (
                        dependency_material_frontier_sha256([], {}, [], revision=0)
                    ),
                    "expected_projection_revision": 0,
                    "expected_graph_revision": 0,
                    "expected_graph_sha256": digest([]),
                }],
                "validation_authority": {
                    "owned_children": [blocker, blocked],
                    "comments": [comment],
                },
            })
            context["dependency_graph"]["observed_frontier"].update({
                "material_revision": 0, "projection_revision": 1,
                "graph_revision": 1, "graph_sha256": digest([relation]),
            })
            context["dependency_authority"] = {
                "owned_children": [blocker, blocked],
                "authorization_events": [event],
                "material_event_ids": [],
            }
            MODULE.build_launch_profile(
                context, "GEN-37", self.git(root),
                model="gpt-5.6-sol", reasoning_effort="medium",
            )

            forged = deepcopy(context)
            forged["dependency_graph"]["relations"][0]["blocked"] = {
                "issue_id": "20000000-0000-4000-8000-000000000003",
                "identifier": "GEN-45",
            }
            changed = forged["dependency_graph"]["relations"][0]
            changed["id"] = dependency_relation_id(
                authority=authority, blocker=blocker, blocked=changed["blocked"],
            )
            forged_batch_id = "wsdb_" + digest([
                "workstream-child-dependency-native-batch-v1", authority,
                PLAN, before, [changed],
            ])[:32]
            forged_event = build_projection_event(
                workstream_id="GEN-37", kind="child_dependency_authorization",
                key=forged_batch_id, plan_revision=PLAN, expected_revision=0,
                created_at="2026-08-31T00:00:00Z", authority=route,
                value={
                    **event["value"], "batch_id": forged_batch_id,
                    "relation_ids": [changed["id"]],
                    "relations_sha256": digest([changed]),
                },
            )
            forged["dependency_graph"]["sha256"] = digest([changed])
            forged["dependency_graph"]["observed_frontier"][
                "graph_sha256"
            ] = digest([changed])
            forged["dependency_graph"]["authorization_batches"][0].update({
                "batch_id": forged_batch_id,
                "event_id": forged_event["event_id"],
                "relation_ids": [changed["id"]],
                "relations_sha256": digest([changed]),
            })
            forged["dependency_graph"]["validation_authority"]["comments"] = [{
                **comment,
                "id": projection_slot_id("GEN-37", PLAN, 0, route),
                "body": encode_projection_comment(forged_event),
            }]
            forged["dependency_graph"]["validation_authority"][
                "owned_children"
            ] = [blocker, changed["blocked"]]
            with self.assertRaisesRegex(
                MODULE.ShipyardProfileError, "resume_dependency_graph_invalid",
            ):
                MODULE.build_launch_profile(
                    forged, "GEN-37", self.git(root),
                    model="gpt-5.6-sol", reasoning_effort="medium",
                )

            stale_receipt = deepcopy(context)
            stale_event = build_projection_event(
                workstream_id="GEN-37", kind="child_dependency_authorization",
                key=batch_id, plan_revision=PLAN, expected_revision=0,
                created_at="2026-08-31T00:00:01Z", authority=route,
                value=deepcopy(event["value"]),
            )
            stale_receipt["dependency_graph"]["authorization_batches"][0][
                "event_id"
            ] = stale_event["event_id"]
            stale_receipt["dependency_graph"]["validation_authority"][
                "comments"
            ] = [{
                **comment, "body": encode_projection_comment(stale_event),
                "createdAt": "2026-08-31T00:00:01Z",
                "updatedAt": "2026-08-31T00:00:01Z",
            }]
            with self.assertRaisesRegex(
                MODULE.ShipyardProfileError, "resume_dependency_graph_invalid",
            ):
                MODULE.build_launch_profile(
                    stale_receipt, "GEN-37", self.git(root),
                    model="gpt-5.6-sol", reasoning_effort="medium",
                )

    def test_digest_bound_resume_envelope_requires_exact_hydration(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            context = self.context(root)
            context["context_schema"] = {
                "name": "agent-workstream.resume-context", "version": 2,
                "representation": "compact_validated",
                "envelope": "fixed_frontier_authority_v1",
            }
            context["deferred_audit_detail"] = {
                "state": "fixed_frontier_authority_envelope",
                "audit_route": {
                    "command": (
                        "workstreamctl resume GEN-37 "
                        "--max-bytes 2147483647 --max-items 2147483647"
                    ),
                    "command_role": "display_only",
                    "launcher": "current_workstream_resume_skill_script",
                    "args": [
                        "GEN-37", "--max-bytes", "2147483647",
                        "--max-items", "2147483647",
                    ],
                    "representation": "compact_validated",
                },
            }
            with self.assertRaisesRegex(
                MODULE.ShipyardProfileError,
                "resume_envelope_requires_compact_validated_hydration",
            ):
                MODULE.build_launch_profile(
                    context, "GEN-37", self.git(root),
                    model="gpt-5.6-sol", reasoning_effort="medium",
                )

    def test_claude_profile_uses_prompt_free_native_grammar(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            context = self.context(root, provider="claude", service="anthropic")
            profile = MODULE.build_launch_profile(
                context, "GEN-37", self.git(root),
                model="claude-opus-4-1", reasoning_effort="high",
            )
            self.assertEqual(profile["launch_argv"], [
                "claude", "--model", "claude-opus-4-1", "--effort", "high",
            ])
            self.assertEqual(profile["resume_argv"], [
                "claude", "--model", "claude-opus-4-1", "--effort", "high",
                "--resume", "provider-session-7",
            ])
            with self.assertRaisesRegex(
                MODULE.ShipyardProfileError, "claude_ultra_effort_unsupported",
            ):
                MODULE.build_launch_profile(
                    context, "GEN-37", self.git(root),
                    model="claude-opus-4-1", reasoning_effort="ultra",
                )

    def test_resume_checkpoint_and_repository_drift_fail_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            base = self.context(root)
            mutations = {
                "inspection": lambda value: value.update(resume_authority="inspection_only"),
                "missing_checkpoint": lambda value: value.update(latest_checkpoint=None),
                "unacknowledged": lambda value: value["latest_checkpoint"]["acknowledgement"].update(state="pending"),
                "later_material": lambda value: value.update(root_revision=4, material_event_revision=4),
                "unsafe_worktree": lambda value: value["latest_checkpoint"]["worktree"].update(state="dirty"),
                "scope_head": lambda value: value["scope"]["repositories"][0].update(exact_head="6" * 40),
                "disposition_head": lambda value: value["disposition"].update(remote_head="7" * 40),
                "successor_disposition": lambda value: value["disposition"].update(disposition="create_successor"),
                "superseded": lambda value: value.update(status="Superseded"),
                "quarantine": lambda value: value["projection_quarantine"].update(count=1),
                "obligation": lambda value: value["uncheckpointed_material_obligations"].append({"kind": "requirement"}),
                "ambiguous_session": lambda value: value["latest_checkpoint"]["provenance"]["latest"].update(session_id="unknown"),
                "unsafe_session": lambda value: value["latest_checkpoint"]["provenance"]["latest"].update(session_id="session 7"),
                "ambiguous_provider": lambda value: value["latest_checkpoint"]["provenance"]["latest"].update(provider="unknown"),
                "query_context": lambda value: value.update(context_url=value["context_url"] + "?token=bad"),
                "empty_query_context": lambda value: value.update(context_url=value["context_url"] + "?"),
                "empty_fragment_context": lambda value: value.update(context_url=value["context_url"] + "#"),
                "uppercase_scheme_context": lambda value: value.update(context_url=value["context_url"].replace("https://", "HTTPS://")),
            }
            for name, mutate in mutations.items():
                with self.subTest(name=name):
                    candidate = deepcopy(base)
                    mutate(candidate)
                    with self.assertRaises(MODULE.ShipyardProfileError):
                        MODULE.build_launch_profile(
                            candidate, "GEN-37", self.git(root),
                            model="gpt-5.6-sol", reasoning_effort="medium",
                        )

            for token in ("A1-1", "GEN-01", "ABCDEFGHIJKLMNOPQ-1"):
                with self.subTest(token=token), self.assertRaisesRegex(
                    MODULE.ShipyardProfileError, "workstream_handle_is_not_shipyard",
                ):
                    MODULE.build_launch_profile(
                        base, token, self.git(root),
                        model="gpt-5.6-sol", reasoning_effort="medium",
                    )

            incompatible_git = MODULE.GitIdentity(
                root=root,
                repository_coordinate="github.com/generous-corp/.github",
                repository="generous-corp/.github",
                head=HEAD,
                branch="feature/profile",
            )
            with self.assertRaisesRegex(
                MODULE.ShipyardProfileError, "git_identity_is_not_shipyard",
            ):
                MODULE.build_launch_profile(
                    base, "GEN-37", incompatible_git,
                    model="gpt-5.6-sol", reasoning_effort="medium",
                )

    def test_git_inspection_requires_clean_exact_active_lineage(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            subprocess.run(["git", "init", "-b", "feature/profile", str(root)], check=True, capture_output=True)
            subprocess.run(["git", "-C", str(root), "config", "user.email", "test@example.com"], check=True)
            subprocess.run(["git", "-C", str(root), "config", "user.name", "Profile Test"], check=True)
            (root / "tracked.txt").write_text("tracked\n", encoding="utf-8")
            subprocess.run(["git", "-C", str(root), "add", "tracked.txt"], check=True)
            subprocess.run(["git", "-C", str(root), "commit", "-m", "fixture"], check=True, capture_output=True)
            subprocess.run([
                "git", "-C", str(root), "remote", "add", "origin",
                "git@github.com:Generous-Corp/agent-workstream.git",
            ], check=True)
            head = subprocess.run(
                ["git", "-C", str(root), "rev-parse", "HEAD"],
                check=True, capture_output=True, text=True,
            ).stdout.strip()
            key = "branch.feature/profile.pulpWorktree"
            for field, value in (
                ("Status", "active"), ("DurableSha", head), ("LastPath", str(root)),
            ):
                subprocess.run([
                    "git", "-C", str(root), "config", "--local", f"{key}{field}", value,
                ], check=True)

            identity = MODULE.inspect_git_worktree(root)
            self.assertEqual(identity.repository, "generous-corp/agent-workstream")
            self.assertEqual(identity.head, head)
            self.assertEqual(identity.branch, "feature/profile")
            (root / "untracked.txt").write_text("dirty\n", encoding="utf-8")
            with self.assertRaisesRegex(MODULE.ShipyardProfileError, "worktree_not_clean"):
                MODULE.inspect_git_worktree(root)

    @unittest.skipUnless(sys.platform == "darwin", "macOS owner/ACL contract")
    def test_atomic_output_is_owner_only_and_refuses_existing_or_public_parent(self):
        profile = {"schema_version": 1}
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory)
            parent.chmod(0o700)
            output = parent / "profile.json"
            written = MODULE.write_private_profile(output, profile)
            self.assertEqual(json.loads(written.read_text()), profile)
            self.assertEqual(stat.S_IMODE(written.stat().st_mode), 0o600)
            with self.assertRaisesRegex(
                MODULE.ShipyardProfileError, "output_path_already_exists",
            ):
                MODULE.write_private_profile(output, profile)
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory)
            parent.chmod(0o755)
            with self.assertRaisesRegex(
                MODULE.ShipyardProfileError, "output_parent_must_be_private",
            ):
                MODULE.write_private_profile(parent / "profile.json", profile)

    @unittest.skipUnless(sys.platform == "darwin", "macOS atomic publication contract")
    def test_atomic_output_never_clobbers_a_concurrent_winner(self):
        profile = {"schema_version": 1}
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory)
            parent.chmod(0o700)
            output = parent / "profile.json"

            def win_race(source, destination, **_kwargs):
                descriptor = os.open(
                    destination,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                    0o600,
                    dir_fd=_kwargs["dst_dir_fd"],
                )
                try:
                    os.write(descriptor, b'{"winner":true}\n')
                finally:
                    os.close(descriptor)
                raise FileExistsError(destination)

            with mock.patch.object(MODULE.os, "link", side_effect=win_race), \
                 self.assertRaisesRegex(MODULE.ShipyardProfileError, "atomic_output_failed"):
                MODULE.write_private_profile(output, profile)
            self.assertEqual(json.loads(output.read_text()), {"winner": True})

    @unittest.skipUnless(sys.platform == "darwin", "macOS inode publication contract")
    def test_atomic_output_refuses_after_link_inode_replacement(self):
        profile = {"schema_version": 1}
        real_link = os.link
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory)
            parent.chmod(0o700)
            output = parent / "profile.json"

            def replace_after_link(source, destination, **kwargs):
                real_link(source, destination, **kwargs)
                os.unlink(destination, dir_fd=kwargs["dst_dir_fd"])
                descriptor = os.open(
                    destination,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                    0o600,
                    dir_fd=kwargs["dst_dir_fd"],
                )
                try:
                    os.write(descriptor, b'{"replacement":true}\n')
                finally:
                    os.close(descriptor)

            with mock.patch.object(MODULE.os, "link", side_effect=replace_after_link), \
                 self.assertRaisesRegex(
                     MODULE.ShipyardProfileError,
                     "output_file_is_not_private_and_canonical",
                 ):
                MODULE.write_private_profile(output, profile)
            self.assertEqual(json.loads(output.read_text()), {"replacement": True})

    @unittest.skipUnless(sys.platform == "darwin", "macOS extended ACL contract")
    def test_output_rejects_private_mode_directory_with_extended_acl(self):
        profile = {"schema_version": 1}
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory)
            parent.chmod(0o700)
            subprocess.run([
                "chmod", "+a",
                "everyone allow read,readattr,readextattr,readsecurity,"
                "file_inherit,directory_inherit",
                str(parent),
            ], check=True)
            try:
                with self.assertRaisesRegex(
                    MODULE.ShipyardProfileError, "output_parent_has_extended_acl",
                ):
                    MODULE.write_private_profile(parent / "profile.json", profile)
                self.assertFalse((parent / "profile.json").exists())
            finally:
                subprocess.run(["chmod", "-N", str(parent)], check=True)

    def test_authenticated_resume_subprocess_is_fixed_full_authority(self):
        completed = subprocess.CompletedProcess(
            args=[], returncode=0,
            stdout=json.dumps({"resume_authority": "full"}).encode(), stderr=b"",
        )
        with tempfile.TemporaryDirectory() as directory, mock.patch.object(
            MODULE.subprocess, "run", return_value=completed,
        ) as run:
            observed = MODULE.load_authenticated_resume(
                "GEN-37", repo_path=directory,
                config="/private/config.json", plan_source="/private/PLAN.md",
            )
        self.assertEqual(observed["resume_authority"], "full")
        command = run.call_args.args[0]
        self.assertNotIn("--inspection-only", command)
        self.assertEqual(command[2], "GEN-37")
        self.assertIn("--max-bytes", command)
        self.assertIn("--max-items", command)

    def test_authenticated_resume_hydrates_envelope_for_profile_without_prompt(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            hydrated = self.context(root)
            envelope = {
                "context_schema": {
                    "name": "agent-workstream.resume-context", "version": 2,
                    "representation": "compact_validated",
                    "envelope": "fixed_frontier_authority_v1",
                },
                "workstream_id": "GEN-37", "plan_revision": PLAN,
                "root_revision": hydrated["root_revision"],
                "resume_authority": "full",
                "deferred_audit_detail": {
                    "full_context_sha256": hashlib.sha256(
                        MODULE._canonical(hydrated)
                    ).hexdigest(),
                    "audit_route": {
                        "command": "workstreamctl resume GEN-37 --max-bytes 999999",
                        "launcher": "current_workstream_resume_skill_script",
                        "args": ["GEN-37", "--max-bytes", "999999"],
                        "representation": "compact_validated",
                    },
                },
            }
            results = [
                subprocess.CompletedProcess(
                    args=[], returncode=0,
                    stdout=json.dumps(envelope).encode(), stderr=b"",
                ),
                subprocess.CompletedProcess(
                    args=[], returncode=0,
                    stdout=json.dumps(hydrated).encode(), stderr=b"",
                ),
            ]
            with mock.patch.object(
                MODULE.subprocess, "run", side_effect=results,
            ) as run:
                observed = MODULE.load_authenticated_resume(
                    "GEN-37", repo_path=root,
                )
            profile = MODULE.build_launch_profile(
                observed, "GEN-37", self.git(root),
                model="gpt-5.6-sol", reasoning_effort="medium",
            )

        self.assertEqual(observed, hydrated)
        self.assertEqual(profile["schema_version"], 1)
        self.assertEqual(run.call_count, 2)
        hydration_command = run.call_args_list[1].args[0]
        self.assertEqual(
            hydration_command[hydration_command.index("--max-bytes") + 1],
            str(MODULE.HYDRATED_RESUME_MAX_BYTES),
        )
        self.assertNotIn("--include-history", hydration_command)

    def test_output_must_be_outside_worktree(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            private = root / ".private"
            private.mkdir(mode=0o700)
            with self.assertRaisesRegex(
                MODULE.ShipyardProfileError, "output_must_be_outside_bound_worktree",
            ):
                MODULE._output_outside_worktree(private / "profile.json", root)

    def test_ambient_session_must_match_checkpoint_session_when_present(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            profile = MODULE.build_launch_profile(
                self.context(root), "GEN-37", self.git(root),
                model="gpt-5.6-sol", reasoning_effort="medium",
            )
            MODULE._validate_ambient_session(
                profile, {"CODEX_THREAD_ID": "provider-session-7"},
            )
            with self.assertRaisesRegex(
                MODULE.ShipyardProfileError,
                "checkpoint_does_not_match_ambient_agent_session",
            ):
                MODULE._validate_ambient_session(
                    profile, {"CODEX_THREAD_ID": "different-session"},
                )
            with self.assertRaisesRegex(
                MODULE.ShipyardProfileError, "ambient_agent_session_is_ambiguous",
            ):
                MODULE._validate_ambient_session(profile, {
                    "CODEX_THREAD_ID": "provider-session-7",
                    "CLAUDE_CODE_SESSION_ID": "provider-session-7",
                })


if __name__ == "__main__":
    unittest.main()
