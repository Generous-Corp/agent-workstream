#!/usr/bin/env python3
from __future__ import annotations

from copy import deepcopy
import unittest

from test_workstream_child_state import (
    CHILD_ID, FakeChildStateClient, PLAN, ROOT_ID, ROUTE,
)
from workstream_child_proposal import (
    append_proposal, build_proposal, pending_proposal_obligations,
    proposal_slot_id,
)
from workstream_generation import (
    GenerationTransport, WorkstreamGenerationError, _digest,
    build_retirement_proof, generation_quarantine_metadata,
    reduce_generation_checkpoint_comments,
)
from workstream_linear_events import LinearEventError, reduce_event_comments
from workstream_linear_projection import (
    LinearProjectionAdapter, build_projection_event,
    child_mutation_authorizations_from_comments, reduce_projection_comments,
)


NEW = "b" * 64
AUTHORITY = {**ROUTE, "root_issue_id": ROOT_ID}


class ProposalGenerationRaceTests(unittest.TestCase):
    def projection(self, client, plan=PLAN):
        return LinearProjectionAdapter(
            client, issue_id="GEN-37", workstream_id="GEN-37",
            plan_revision=plan, **AUTHORITY,
        )

    def prepare_candidate(self, client):
        predecessor = self.projection(client)
        predecessor.append(build_projection_event(
            workstream_id="GEN-37", kind="provenance", key="generation",
            value={
                "agent": "test", "machine": "test", "session_id": "old",
                "worktree": {"state": "safe", "head": "d" * 40},
            },
            plan_revision=PLAN, expected_revision=3,
            created_at="predecessor-3", authority=AUTHORITY,
        ))
        predecessor.append(build_projection_event(
            workstream_id="GEN-37", kind="disposition", key="root",
            value={
                "disposition": "attach", "remote_head": "d" * 40,
                "recovered_from_checkpoint": None,
            },
            plan_revision=PLAN, expected_revision=4,
            created_at="predecessor-4", authority=AUTHORITY,
        ))
        target = self.projection(client, NEW)
        scope = FakeChildStateClient.scope_event({
            "GEN-38": "github.com:id:R_repo",
        })
        scope = build_projection_event(
            workstream_id="GEN-37", kind="scope", key="root",
            value=deepcopy(scope["value"]), plan_revision=NEW,
            expected_revision=0, created_at="candidate-0", authority=AUTHORITY,
        )
        target.append(scope)
        target.append(build_projection_event(
            workstream_id="GEN-37", kind="source", key="root",
            value={"identity": "https://example.test/new", "sha256": NEW},
            plan_revision=NEW, expected_revision=1,
            created_at="candidate-1", authority=AUTHORITY,
        ))
        target.append(build_projection_event(
            workstream_id="GEN-37", kind="provenance", key="generation",
            value={
                "agent": "test", "machine": "test", "session_id": "race",
                "worktree": {"state": "safe", "head": "e" * 40},
            },
            plan_revision=NEW, expected_revision=2,
            created_at="candidate-2", authority=AUTHORITY,
        ))
        target.append(build_projection_event(
            workstream_id="GEN-37", kind="disposition", key="root",
            value={
                "disposition": "attach", "remote_head": "e" * 40,
                "recovered_from_checkpoint": None,
            },
            plan_revision=NEW, expected_revision=3,
            created_at="candidate-3", authority=AUTHORITY,
        ))

    def candidate_loader(self, client):
        def load(plan):
            comments = deepcopy(client.root_comments)
            state = reduce_projection_comments(
                comments, workstream_id="GEN-37",
                expected_plan_revision=plan, authenticated_route=AUTHORITY,
            )
            source = state.snapshot["source"]
            checkpoints = reduce_generation_checkpoint_comments(
                comments, workstream_id="GEN-37",
                authenticated_route=AUTHORITY,
            )
            checkpoint_ids = sorted(
                item["event_id"] for item in checkpoints.checkpoints
                if item["plan_revision"] == plan
            )
            material = reduce_event_comments(comments, workstream_id="GEN-37")
            surface = {
                "plan": plan, "material": material.revision,
                "checkpoints": checkpoint_ids, "projection": state.revision,
                "events": [event["event_id"] for event in state.events],
            }
            return {
                "resume_authority": "full", "plan_revision": plan,
                "authenticated_route": AUTHORITY,
                "source": {
                    "identity": source.get("identity") or source.get("url"),
                    "sha256": source["sha256"],
                },
                "material_revision": material.revision,
                "checkpoint_event_ids": checkpoint_ids,
                "projection_revision": state.revision,
                "graph_frontier_sha256": _digest("stable"),
                "snapshot_sha256": _digest(surface),
                "quarantined_legacy_writes": generation_quarantine_metadata(
                    comments, workstream_id="GEN-37",
                ),
            }
        return load

    def retirement(self, client):
        state = self.projection(client).state()
        checkpoints = reduce_generation_checkpoint_comments(
            client.root_comments, workstream_id="GEN-37",
            authenticated_route=AUTHORITY,
        )
        return build_retirement_proof(
            predecessor_plan_revision=PLAN, retired_at="now",
            retired_writer_epoch=0,
            provenance_event_ids=sorted(
                event["event_id"] for event in state.events
                if event["kind"] == "provenance"
            ),
            checkpoint_event_ids=sorted(
                item["event_id"] for item in checkpoints.checkpoints
                if item["plan_revision"] == PLAN
            ),
        )

    def proposal_and_intent(self, client):
        proposal = build_proposal(
            "event", {
                "event_id": "proposal-generation-race",
                "workstream_id": "GEN-38", "kind": "progress",
                "source": "user_turn", "payload": {"winner": "child"},
                "expected_revision": 0, "created_at": "now",
            },
            child_workstream_id="GEN-38", child_issue_id=CHILD_ID,
            plan_revision=PLAN,
        )
        projection = self.projection(client)
        selected = projection.select_owned_child_generation(
            description_plan_revision=PLAN,
            child_workstream_id="GEN-38", child_issue_id=CHILD_ID,
        )
        generation = {key: selected[key] for key in (
            "plan_revision", "description_plan_revision",
            "transition_tip_event_id", "activation_epoch", "authority_origin",
            "workstream_id", "authority", "source",
        )}
        arguments = {
            "proposal": proposal,
            "proposal_remote_id": proposal_slot_id(
                CHILD_ID, proposal["proposal_id"],
            ),
            "child_identity": {
                "identifier": "GEN-38", "id": CHILD_ID,
                "parent_issue_id": ROOT_ID, "route": ROUTE,
            },
            "generation_authority": generation,
            "scope_event_id": selected["scope_event_id"],
            "scope_value_sha256": selected["scope_value_sha256"],
            "repository_owner": selected["child_repository_owner"],
            "child_origin": selected["child_origin"],
            "expected_projection_revision": selected["projection_revision"],
        }
        return proposal, projection, arguments

    def test_child_publication_and_generation_activation_have_one_winner(self):
        # Child publication linearizes first: generation refuses, then the
        # proposal is published and authorized without becoming stranded.
        child_client = FakeChildStateClient()
        self.prepare_candidate(child_client)
        proposal, projection, arguments = self.proposal_and_intent(child_client)
        intent = projection.reserve_child_mutation(
            **arguments, publish_intent=True,
        )
        generation = GenerationTransport(
            child_client, issue_id="GEN-37", workstream_id="GEN-37",
            authority=AUTHORITY,
            candidate_loader=self.candidate_loader(child_client),
            legacy_description_plan_revision=PLAN,
        )
        with self.assertRaisesRegex(LinearEventError, "ledger_boundary_reserved"):
            generation.activate(
                target_plan_revision=NEW, created_at="now",
                retirement=self.retirement(child_client),
            )
        receipt = append_proposal(
            child_client, proposal, reservation=intent["reservation"],
        )
        projection.reserve_child_mutation(**arguments)
        authorizations = child_mutation_authorizations_from_comments(
            child_client.root_comments, workstream_id="GEN-37",
            description_plan_revision=PLAN, authenticated_route=AUTHORITY,
        )
        self.assertEqual(pending_proposal_obligations(
            child_client.child_comments, authorizations,
            child_workstream_id="GEN-38", child_issue_id=CHILD_ID,
            plan_revision=PLAN,
        ), [])
        self.assertEqual(receipt["disposition"], "created")

        # Generation's root reservation linearizes first: publication cannot
        # reserve or write a child proposal, and exact activation can finish.
        generation_client = FakeChildStateClient()
        self.prepare_candidate(generation_client)
        proposal, projection, arguments = self.proposal_and_intent(
            generation_client,
        )
        generation = GenerationTransport(
            generation_client, issue_id="GEN-37", workstream_id="GEN-37",
            authority=AUTHORITY,
            candidate_loader=self.candidate_loader(generation_client),
            legacy_description_plan_revision=PLAN,
        )
        retirement = self.retirement(generation_client)
        comments = deepcopy(generation_client.root_comments)
        candidate = generation._candidate(NEW, comments)
        reservation = generation._reservation(
            comments=comments, mode="activate", from_plan=PLAN, to_plan=NEW,
            epoch=0, previous_control=None, candidate=candidate,
            retirement=retirement, created_at="now",
        )
        generation._append_reservation(reservation)
        with self.assertRaisesRegex(
            WorkstreamGenerationError, "generation_boundary_reserved",
        ):
            projection.reserve_child_mutation(**arguments, publish_intent=True)
        self.assertEqual(generation_client.child_comments, [])
        activated = generation.activate(
            target_plan_revision=NEW, created_at="now", retirement=retirement,
        )
        self.assertEqual(activated["activated_plan_revision"], NEW)
        self.assertEqual(generation_client.child_comments, [])


if __name__ == "__main__":
    unittest.main()
