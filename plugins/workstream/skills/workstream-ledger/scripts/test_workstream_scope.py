#!/usr/bin/env python3
import unittest

from workstream_scope import (
    canonical_repository, relation_target_key, repository_key, ScopeError,
    validate_relation_graph, validate_relations, validate_scope,
)


class WorkstreamScopeTests(unittest.TestCase):
    def scope(self):
        return {
            "namespace": "pulp-playback",
            "linear": {"workspace_id": "ws-private", "team_id": "team-gen", "project_id": "project-pulp",
                       "root_issue_id": "33333333-3333-4333-8333-333333333333",
                       "route_verification": {
                           "workspace_id": "ws-private", "team_id": "team-gen", "project_id": "project-pulp",
                           "root_issue_id": "33333333-3333-4333-8333-333333333333",
                           "observed_at": "2026-08-21T11:00:00Z",
                           "evidence": [{"kind": "authenticated_linear_readback", "authenticated": True,
                                         "workspace_id": "ws-private", "team_id": "team-gen",
                                         "project_id": "project-pulp",
                                         "root_issue_id": "33333333-3333-4333-8333-333333333333"}],
                       }},
            "primary_repository": "github.com:id:R_pulp",
            "repositories": [
                {"slug": "github.com/generous-corp/pulp", "exact_head": "a" * 40,
                 "provider_repository_id": "R_pulp", "aliases": [],
                 "identity_resolution": {"provider_repository_id": "R_pulp",
                                         "resolved_slug": "github.com/generous-corp/pulp",
                                         "observed_at": "2026-08-21T11:00:00Z",
                                         "evidence": [{"kind": "authenticated_provider_readback",
                                                       "authenticated": True,
                                                       "provider_repository_id": "R_pulp",
                                                       "resolved_slug": "github.com/generous-corp/pulp"}]},
                 "identity_updates": [], "evidence": [{"kind": "tests"}]},
                {"slug": "github.com/generous-corp/vellum", "exact_head": "b" * 40,
                 "provider_repository_id": "R_vellum", "aliases": [],
                 "identity_resolution": {"provider_repository_id": "R_vellum",
                                         "resolved_slug": "github.com/generous-corp/vellum",
                                         "observed_at": "2026-08-21T11:00:00Z",
                                         "evidence": [{"kind": "authenticated_provider_readback",
                                                       "authenticated": True,
                                                       "provider_repository_id": "R_vellum",
                                                       "resolved_slug": "github.com/generous-corp/vellum"}]},
                 "identity_updates": [], "evidence": [{"kind": "compatibility"}]},
            ],
            "child_ownership": {"GEN-38": "github.com:id:R_pulp", "GEN-39": "github.com:id:R_vellum"},
        }

    def test_multi_repository_scope_uses_explicit_namespace_and_child_ownership(self):
        validate_scope(self.scope(), root_id="GEN-37", child_ids={"GEN-38", "GEN-39"})

    def test_title_is_not_a_namespace_and_every_child_must_have_repository_owner(self):
        scope = self.scope()
        scope.pop("namespace")
        with self.assertRaisesRegex(ScopeError, "invalid_namespace"):
            validate_scope(scope, root_id="GEN-37", child_ids={"GEN-38", "GEN-39"})
        scope = self.scope()
        del scope["child_ownership"]["GEN-39"]
        with self.assertRaisesRegex(ScopeError, "unowned_children"):
            validate_scope(scope, root_id="GEN-37", child_ids={"GEN-38", "GEN-39"})

    def test_repository_heads_and_evidence_are_repository_qualified(self):
        scope = self.scope()
        scope["repositories"][1]["exact_head"] = "working-tree"
        with self.assertRaisesRegex(ScopeError, "invalid_repository_head"):
            validate_scope(scope, root_id="GEN-37", child_ids={"GEN-38", "GEN-39"})

    def test_remote_coordinates_distinguish_same_repo_name_under_different_owners(self):
        scope = self.scope()
        scope["repositories"].append({"slug": "github.com/danielraffel/pulp", "exact_head": "c" * 40,
                                      "provider_repository_id": "R_personal_pulp", "aliases": [],
                                      "identity_resolution": {
                                          "provider_repository_id": "R_personal_pulp",
                                          "resolved_slug": "github.com/danielraffel/pulp",
                                          "observed_at": "2026-08-21T11:00:00Z",
                                          "evidence": [{"kind": "authenticated_provider_readback",
                                                        "authenticated": True,
                                                        "provider_repository_id": "R_personal_pulp",
                                                        "resolved_slug": "github.com/danielraffel/pulp"}]},
                                      "identity_updates": [], "evidence": []})
        validate_scope(scope, root_id="GEN-37", child_ids={"GEN-38", "GEN-39"})
        scope["repositories"][-1]["path"] = "/workspace/project"
        with self.assertRaisesRegex(ScopeError, "local_path_is_not_repository_identity"):
            validate_scope(scope, root_id="GEN-37", child_ids={"GEN-38", "GEN-39"})

    def test_git_remote_forms_normalize_and_github_case_cannot_duplicate(self):
        self.assertEqual(canonical_repository("git@github.com:Generous-Corp/vellum.git"),
                         "github.com/generous-corp/vellum")
        self.assertEqual(canonical_repository("https://github.com/danielraffel/Shipyard.git"),
                         "github.com/danielraffel/shipyard")
        scope = self.scope()
        scope["repositories"].append({"slug": "github.com/Generous-Corp/PULP", "exact_head": "c" * 40,
                                      "provider_repository_id": "R_duplicate", "aliases": [],
                                      "identity_resolution": {},
                                      "identity_updates": [], "evidence": []})
        with self.assertRaisesRegex(ScopeError, "repository_not_canonical"):
            validate_scope(scope, root_id="GEN-37", child_ids={"GEN-38", "GEN-39"})

    def test_linear_workspace_team_and_project_are_explicit_routing_identity(self):
        for field in ("workspace_id", "team_id", "project_id", "root_issue_id"):
            scope = self.scope()
            del scope["linear"][field]
            with self.assertRaisesRegex(ScopeError, "linear_destination_missing"):
                validate_scope(scope, root_id="GEN-37", child_ids={"GEN-38", "GEN-39"})

    def test_transfer_alias_resolves_to_immutable_provider_identity(self):
        scope = self.scope()
        repository = scope["repositories"][0]
        repository["aliases"] = ["github.com/danielraffel/pulp"]
        repository["identity_updates"] = [{
            "from": "github.com/danielraffel/pulp",
            "to": "github.com/generous-corp/pulp",
            "repository_key": "github.com:id:R_pulp", "provider_repository_id": "R_pulp",
            "observed_at": "2026-08-21T12:00:00Z",
            "evidence": [{"kind": "authenticated_provider_readback", "authenticated": True,
                          "repository_key": "github.com:id:R_pulp",
                          "provider_repository_id": "R_pulp",
                          "requested_slug": "github.com/danielraffel/pulp",
                          "resolved_slug": "github.com/generous-corp/pulp"}],
        }]
        validate_scope(scope, root_id="GEN-37", child_ids={"GEN-38", "GEN-39"})
        self.assertEqual(repository_key(repository), "github.com:id:R_pulp")

    def test_alias_collision_and_stale_alias_routing_fail_closed(self):
        scope = self.scope()
        scope["repositories"][0]["aliases"] = ["github.com/legacy/pulp"]
        scope["repositories"][0]["identity_updates"] = [{
            "from": "github.com/legacy/pulp", "to": "github.com/generous-corp/pulp",
            "repository_key": "github.com:id:R_pulp", "provider_repository_id": "R_pulp",
            "observed_at": "2026-08-21T12:00:00Z",
            "evidence": [{"kind": "authenticated_provider_readback", "authenticated": True,
                          "repository_key": "github.com:id:R_pulp", "provider_repository_id": "R_pulp",
                          "requested_slug": "github.com/legacy/pulp",
                          "resolved_slug": "github.com/generous-corp/pulp"}],
        }]
        scope["repositories"][1]["aliases"] = ["github.com/legacy/pulp"]
        scope["repositories"][1]["identity_updates"] = [{
            "from": "github.com/legacy/pulp", "to": "github.com/generous-corp/vellum",
            "repository_key": "github.com:id:R_vellum", "provider_repository_id": "R_vellum",
            "observed_at": "2026-08-21T12:00:00Z",
            "evidence": [{"kind": "authenticated_provider_readback", "authenticated": True,
                          "repository_key": "github.com:id:R_vellum", "provider_repository_id": "R_vellum",
                          "requested_slug": "github.com/legacy/pulp",
                          "resolved_slug": "github.com/generous-corp/vellum"}],
        }]
        with self.assertRaisesRegex(ScopeError, "repository_alias_collision"):
            validate_scope(scope, root_id="GEN-37", child_ids={"GEN-38", "GEN-39"})

        scope = self.scope()
        scope["repositories"][0]["aliases"] = ["github.com/legacy/pulp"]
        scope["repositories"][0]["identity_updates"] = [{
            "from": "github.com/legacy/pulp", "to": "github.com/someone-else/pulp",
            "repository_key": "github.com:id:R_pulp", "provider_repository_id": "R_pulp",
            "observed_at": "2026-08-21T12:00:00Z",
            "evidence": [{"kind": "authenticated_provider_readback", "authenticated": True,
                          "repository_key": "github.com:id:R_pulp", "provider_repository_id": "R_pulp",
                          "requested_slug": "github.com/legacy/pulp",
                          "resolved_slug": "github.com/someone-else/pulp"}],
        }]
        with self.assertRaisesRegex(ScopeError, "stale_alias_routing"):
            validate_scope(scope, root_id="GEN-37", child_ids={"GEN-38", "GEN-39"})

    def test_coordinate_fallback_requires_verified_redirect_resolution(self):
        repository = {"slug": "git.example.com/Team/repo", "aliases": [],
                      "identity_updates": [], "exact_head": "a" * 40, "evidence": []}
        with self.assertRaisesRegex(ScopeError, "equivalence_unproven"):
            repository_key(repository)
        repository["identity_resolution"] = {
            "provider_repository_id": None, "resolved_slug": "git.example.com/Team/repo",
            "observed_at": "2026-08-21T12:00:00Z",
            "evidence": [{"kind": "authenticated_provider_readback", "authenticated": True,
                          "provider_repository_id": None,
                          "resolved_slug": "git.example.com/Team/repo"}],
        }
        self.assertEqual(repository_key(repository),
                         "git.example.com:coordinate:git.example.com/Team/repo")

    def test_provider_id_must_match_authenticated_current_slug_readback(self):
        scope = self.scope()
        scope["repositories"][0]["provider_repository_id"] = "R_invented"
        with self.assertRaisesRegex(ScopeError, "provider_repository_id_mismatch"):
            validate_scope(scope, root_id="GEN-37", child_ids={"GEN-38", "GEN-39"})

    def test_stale_redirect_resolution_cannot_claim_current_slug(self):
        scope = self.scope()
        scope["repositories"][0]["identity_resolution"]["resolved_slug"] = "github.com/danielraffel/pulp"
        with self.assertRaisesRegex(ScopeError, "stale_repository_resolution"):
            validate_scope(scope, root_id="GEN-37", child_ids={"GEN-38", "GEN-39"})

    def test_alias_update_cannot_switch_to_another_provider_id(self):
        scope = self.scope()
        repository = scope["repositories"][0]
        repository["aliases"] = ["github.com/danielraffel/pulp"]
        repository["identity_updates"] = [{
            "from": "github.com/danielraffel/pulp", "to": "github.com/generous-corp/pulp",
            "repository_key": "github.com:id:R_other", "provider_repository_id": "R_other",
            "observed_at": "2026-08-21T12:00:00Z",
            "evidence": [{"kind": "authenticated_provider_readback", "authenticated": True,
                          "repository_key": "github.com:id:R_other", "provider_repository_id": "R_other",
                          "requested_slug": "github.com/danielraffel/pulp",
                          "resolved_slug": "github.com/generous-corp/pulp"}],
        }]
        with self.assertRaisesRegex(ScopeError, "unverified_identity_update"):
            validate_scope(scope, root_id="GEN-37", child_ids={"GEN-38", "GEN-39"})

    def test_alias_receipt_must_bind_the_exact_old_requested_coordinate(self):
        scope = self.scope()
        repository = scope["repositories"][0]
        repository["aliases"] = ["github.com/unrelated/pulp"]
        repository["identity_updates"] = [{
            "from": "github.com/unrelated/pulp", "to": "github.com/generous-corp/pulp",
            "repository_key": "github.com:id:R_pulp", "provider_repository_id": "R_pulp",
            "observed_at": "2026-08-21T12:00:00Z",
            "evidence": [{"kind": "authenticated_provider_readback", "authenticated": True,
                          "repository_key": "github.com:id:R_pulp", "provider_repository_id": "R_pulp",
                          "requested_slug": "github.com/danielraffel/pulp",
                          "resolved_slug": "github.com/generous-corp/pulp"}],
        }]
        with self.assertRaisesRegex(ScopeError, "unverified_identity_update"):
            validate_scope(scope, root_id="GEN-37", child_ids={"GEN-38", "GEN-39"})

    def test_linear_route_readback_must_bind_project_and_root(self):
        scope = self.scope()
        scope["linear"]["route_verification"]["project_id"] = "project-wrong"
        with self.assertRaisesRegex(ScopeError, "linear_route_readback_mismatch"):
            validate_scope(scope, root_id="GEN-37", child_ids={"GEN-38", "GEN-39"})

    def test_typed_relations_reject_self_links_duplicates_and_unknown_types(self):
        target = {"workspace_id": "ws-other", "issue_id": "11111111-1111-4111-8111-111111111111",
                  "identifier": "GEN-50"}
        validate_relations([
            {"type": "blocks", "target": target},
            {"type": "related", "target": {**target, "issue_id": "22222222-2222-4222-8222-222222222222", "identifier": "GEN-51"}},
        ], root_id="GEN-37")
        for relations, error in (
            ([{"type": "blocks", "target": {**target, "identifier": "GEN-37"}}], "self_relation"),
            ([{"type": "depends", "target": target}], "unknown_relation_type"),
            ([{"type": "related", "target": target}, {"type": "related", "target": target}], "duplicate_relation"),
        ):
            with self.assertRaisesRegex(ScopeError, error):
                validate_relations(relations, root_id="GEN-37")

    def test_relation_token_without_immutable_workspace_issue_identity_is_rejected(self):
        with self.assertRaisesRegex(ScopeError, "invalid_relation_target"):
            validate_relations([{"type": "related", "target": "GEN-50"}], root_id="GEN-37")

    def test_same_display_token_in_another_workspace_is_not_self_identity(self):
        validate_relations([{"type": "related", "target": {
            "workspace_id": "ws-other", "issue_id": "11111111-1111-4111-8111-111111111111",
            "identifier": "GEN-37",
        }}], root_id="GEN-37", workspace_id="ws-current",
                           root_issue_id="33333333-3333-4333-8333-333333333333")

    def test_directed_relation_inverse_resolves_identically_from_either_root(self):
        left = {"workspace_id": "ws", "issue_id": "11111111-1111-4111-8111-111111111111",
                "identifier": "GEN-37"}
        right = {"workspace_id": "ws", "issue_id": "22222222-2222-4222-8222-222222222222",
                 "identifier": "GEN-50"}
        left_relations = [{"type": "blocks", "target": right}]
        right_relations = [{"type": "blocked_by", "target": left}]
        validate_relation_graph(
            left_relations, root_id=left["identifier"], workspace_id=left["workspace_id"],
            root_issue_id=left["issue_id"],
            resolve_target={relation_target_key(right): {**right, "relations": right_relations}},
        )
        validate_relation_graph(
            right_relations, root_id=right["identifier"], workspace_id=right["workspace_id"],
            root_issue_id=right["issue_id"],
            resolve_target={relation_target_key(left): {**left, "relations": left_relations}},
        )

    def test_relation_graph_rejects_dangling_missing_and_contradictory_inverse(self):
        root = {"workspace_id": "ws", "issue_id": "11111111-1111-4111-8111-111111111111",
                "identifier": "GEN-37"}
        target = {"workspace_id": "ws", "issue_id": "22222222-2222-4222-8222-222222222222",
                  "identifier": "GEN-50"}
        relation = [{"type": "blocks", "target": target}]
        with self.assertRaisesRegex(ScopeError, "dangling_relation_target:GEN-50"):
            validate_relation_graph(
                relation, root_id=root["identifier"], workspace_id=root["workspace_id"],
                root_issue_id=root["issue_id"], resolve_target={},
            )
        with self.assertRaisesRegex(ScopeError, "missing_relation_inverse:blocks:GEN-50"):
            validate_relation_graph(
                relation, root_id=root["identifier"], workspace_id=root["workspace_id"],
                root_issue_id=root["issue_id"],
                resolve_target={relation_target_key(target): {**target, "relations": []}},
            )
        with self.assertRaisesRegex(ScopeError, "contradictory_relation_inverse:blocks:GEN-50"):
            validate_relation_graph(
                relation, root_id=root["identifier"], workspace_id=root["workspace_id"],
                root_issue_id=root["issue_id"],
                resolve_target={relation_target_key(target): {
                    **target, "relations": [{"type": "blocks", "target": root}],
                }},
            )

    def test_related_is_informational_but_directed_self_contradiction_refuses(self):
        target = {"workspace_id": "ws", "issue_id": "22222222-2222-4222-8222-222222222222",
                  "identifier": "GEN-50"}
        validate_relation_graph(
            [{"type": "related", "target": target}], root_id="GEN-37",
            workspace_id="ws", root_issue_id="11111111-1111-4111-8111-111111111111",
            resolve_target={relation_target_key(target): {**target, "relations": []}},
        )
        with self.assertRaisesRegex(ScopeError, "contradictory_relation"):
            validate_relations([
                {"type": "blocks", "target": target},
                {"type": "blocked_by", "target": target},
            ], root_id="GEN-37", workspace_id="ws",
               root_issue_id="11111111-1111-4111-8111-111111111111")


if __name__ == "__main__":
    unittest.main()
