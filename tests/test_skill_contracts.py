import unittest
import json
from pathlib import Path
import subprocess
import sys
import tempfile


class SkillContractTests(unittest.TestCase):
    def skill(self) -> str:
        return (
            Path(__file__).parents[1]
            / "plugins/workstream/skills/workstream-ledger/SKILL.md"
        ).read_text()

    def resume_skill(self) -> str:
        return (
            Path(__file__).parents[1]
            / "plugins/workstream/skills/workstream-resume/SKILL.md"
        ).read_text()

    def usage(self) -> str:
        return (Path(__file__).parents[1] / "USAGE.md").read_text()

    def test_start_restore_makes_ingress_recovery_conditional(self):
        skill = self.skill()
        start = skill.split("## Start or restore", 1)[1].split(
            "## Maintain on substantive turns", 1
        )[0]
        condition = start.index("Only when a stable external ingress integration")
        flush = start.index("workstream_ingress.py\" flush")
        skip = start.index("A normal plugin installation skips this step")
        no_probe = start.index("Do not invoke ingress merely to probe")
        self.assertLess(condition, flush)
        self.assertLess(flush, skip)
        self.assertLess(skip, no_probe)

    def test_cold_handle_resume_is_first_bounded_action(self):
        resume = self.resume_skill()
        command = resume.index('python3 "<absolute directory of this SKILL.md>/scripts/workstream_resume.py"')
        substitute = resume.index("Substitute the runtime-supplied directory")
        cwd_warning = resume.index("On the cold path, before repository, memory, worktree")
        path_warning = resume.index("Do not probe `workstreamctl` on `PATH`")
        history_warning = resume.index("`--include-history` during initial recovery")
        self.assertLess(command, substitute)
        self.assertLess(substitute, cwd_warning)
        self.assertLess(cwd_warning, path_warning)
        self.assertLess(path_warning, history_warning)

    def test_optional_tab_binding_cannot_replace_resume_authority(self):
        resume = " ".join(self.resume_skill().split())
        recovery = resume.index("Success requires `resume_authority` to be `full`")
        binding = resume.index("After successful recovery, carry the token")
        unavailable = resume.index("an unavailable/unresolved adapter is an optional no-op")
        no_downgrade = resume.index("never downgrades `resume_authority: full`")
        refusal = resume.index("resume refusal denies authority")
        self.assertLess(recovery, binding)
        self.assertLess(binding, unavailable)
        self.assertLess(unavailable, no_downgrade)
        self.assertLess(no_downgrade, refusal)

    def test_handle_and_visible_title_authority_are_explicit(self):
        resume = " ".join(self.resume_skill().split())
        self.assertIn("current user message directly", resume)
        self.assertIn("That message is the only cold-start handle source", resume)
        for excluded in (
            "Hook or developer text", "cwd", "memory", "prior transcript",
        ):
            self.assertIn(excluded, resume)
        self.assertIn("Visible tabs never grant authority", resume)
        self.assertIn(
            "`updated`/`unchanged` plus exact readback",
            resume,
        )

    def test_resume_threads_exact_project_and_automatic_title_provenance(self):
        resume = " ".join(self.resume_skill().split())
        self.assertIn("exact `project_name` from that full result", resume)
        self.assertIn("--project-name \"<exact recovered project_name>\"", resume)
        self.assertIn("--automatic-title", resume)
        self.assertIn("never infer it from cwd, shell, or title shape", resume)
        self.assertIn("Missing needed provenance", resume)

    def test_execution_verb_proceeds_after_full_authority_without_reconfirmation(self):
        resume = " ".join(self.resume_skill().split())
        full = resume.index("After full authority")
        verbs = resume.index("`execute`, `continue`, `finish`, or `resume`")
        proceed = resume.index("perform the current next action immediately")
        no_confirmation = resume.index("Never stop for redundant confirmation")
        self.assertLess(full, verbs)
        self.assertLess(verbs, proceed)
        self.assertLess(proceed, no_confirmation)

    def test_warm_session_continue_does_not_repeat_resume_or_claim_mutation_authority(self):
        resume = " ".join(self.resume_skill().split())
        warm = resume.index("It is warm only when this exact provider session")
        retained = resume.index("material/projection/checkpoint frontiers")
        bypass = resume.index("do not rerun resume merely to reconfirm authority")
        delivery = resume.index("independently fenced exact-head Shipyard delivery")
        command = resume.index("Cold/fresh requests")
        mutation = resume.index("Before any Linear mutation")
        policy = resume.index("agent workflow policy, not a daemon-enforced grant")
        self.assertLess(warm, retained)
        self.assertLess(retained, bypass)
        self.assertLess(bypass, delivery)
        self.assertLess(delivery, command)
        self.assertLess(command, mutation)
        self.assertLess(mutation, policy)
        self.assertIn("durable local material-delta journal", resume)
        self.assertIn("The plugin installs no hosted runtime or reusable authority cache", resume)

    def test_cold_or_consequential_boundary_requires_live_recovery(self):
        resume = " ".join(self.resume_skill().split())
        self.assertIn("A pasted result, prior provider session", resume)
        self.assertIn("One bare continuation nudge may select that sole warm retained workstream", resume)
        self.assertIn("status checks", resume)
        self.assertIn("must run this as the first functional command", resume)
        self.assertIn("agent/session handoff", resume)
        self.assertIn("perform live resume and reconcile the pending journal", resume)
        self.assertIn("Auth, semantic, generation, budget", resume)

    def test_unreachable_degraded_runtime_is_not_packaged(self):
        scripts = (
            Path(__file__).parents[1]
            / "plugins/workstream/skills/workstream-ledger/scripts"
        )
        self.assertFalse((scripts / "workstream_degraded_execution.py").exists())
        self.assertFalse((scripts / "test_workstream_degraded_execution.py").exists())
        self.assertNotIn("workstream_degraded_execution.py", self.skill())

    def test_ledger_can_activate_on_a_later_exact_warm_turn(self):
        ledger = " ".join(self.skill().split())
        required = (
            "on a later warm turn when exact same-provider-session retained-full "
            "bindings satisfy `workstream-resume` warm classification"
        )
        self.assertIn(required, ledger)
        frontmatter = ledger.split("---", 2)[1]
        self.assertIn(required.replace("`", ""), frontmatter.replace("`", ""))
        self.assertNotIn(
            "only after workstream-resume has returned a bounded authoritative "
            "snapshot in this turn",
            ledger,
        )

    def test_usage_distinguishes_cold_recovery_from_warm_continue(self):
        usage = " ".join(self.usage().split())
        self.assertIn(
            "For a cold/fresh handle request, status check, or consequential "
            "tracking or lifecycle boundary, the agent's first command is",
            usage,
        )
        self.assertIn("A warm `continue` nudge does not repeat resume", usage)
        self.assertIn(
            "Live resume and reconciliation are still required before any Linear mutation",
            usage,
        )
        self.assertNotIn(
            "For a handle/URL/tab-title resume request, with or without additional "
            "instructions, the agent's first command is",
            usage,
        )

    def test_delivery_and_authority_handoffs_are_not_conflated(self):
        for name, document in (
            ("resume", self.resume_skill()),
            ("ledger", self.skill()),
            ("usage", self.usage()),
        ):
            flat = " ".join(document.replace("**", "").split())
            with self.subTest(document=name):
                self.assertIn(
                    "Shipyard delivery handoff means exact-head custody submission "
                    "to Shipyard under its own fences and is allowed",
                    flat,
                )
                self.assertIn(
                    "agent/session handoff means transferring workstream execution "
                    "authority to another agent/session and requires live recovery "
                    "and certification",
                    flat,
                )

    def test_tracked_shipyard_delivery_cannot_drop_workstream_correlation(self):
        ledger = self.skill()
        bridge = (
            Path(__file__).parents[1]
            / "plugins/workstream/skills/workstream-ledger/references/"
              "shipyard-launch-profile.md"
        ).read_text()
        self.assertIn("a bare `shipyard ship --pr` is validation only", ledger)
        self.assertIn("shipyard pr --workstream-id", ledger)
        self.assertIn("shipyard runner steward-handoff", ledger)
        self.assertIn("Do not substitute bare `shipyard ship --pr`", bridge)

    def test_resume_entry_is_small_and_owns_fresh_handle_trigger(self):
        resume = self.resume_skill()
        ledger = self.skill()
        flat = " ".join(resume.split())
        self.assertLess(len(resume.encode()), 5500)
        self.assertLessEqual(len(resume.splitlines()), 90)
        self.assertIn("existing workstream handle", resume)
        self.assertIn("after workstream-resume has returned", ledger)
        command = resume.split("```sh", 1)[1].split("```", 1)[0]
        self.assertNotIn("--include-history", command)
        self.assertNotIn("--inspection-only", command)
        self.assertIn("Success requires `resume_authority`", resume)
        self.assertIn("cwd, environment, memory, and prior transcript handles", flat)
        self.assertIn("For a status-only request, report the bounded snapshot and stop", flat)
        self.assertIn("do not load `workstream-ledger`", flat)

    def test_resume_entry_packaging_and_versions(self):
        root = Path(__file__).parents[1]
        plugin = root / "plugins/workstream"
        codex = json.loads((plugin / ".codex-plugin/plugin.json").read_text())
        claude = json.loads((plugin / ".claude-plugin/plugin.json").read_text())
        self.assertEqual(codex["version"], claude["version"])
        self.assertEqual(codex["version"], "0.4.52")
        shim = plugin / "skills/workstream-resume/scripts/workstream_resume.py"
        target = plugin / "skills/workstream-ledger/scripts/workstream_resume.py"
        self.assertTrue(shim.is_file())
        self.assertTrue(target.is_file())
        self.assertTrue((plugin / "skills/workstream-resume/scripts/workstream_tab.py").is_file())

    def test_resume_shim_forwards_exact_argv_and_missing_target_refuses(self):
        source = (
            Path(__file__).parents[1]
            / "plugins/workstream/skills/workstream-resume/scripts/workstream_resume.py"
        )
        with tempfile.TemporaryDirectory() as directory:
            skills = Path(directory) / "skills"
            shim = skills / "workstream-resume/scripts/workstream_resume.py"
            shim.parent.mkdir(parents=True)
            shim.write_bytes(source.read_bytes())
            missing = subprocess.run(
                [sys.executable, str(shim), "GEN-37"], capture_output=True, text=True,
            )
            self.assertEqual(missing.returncode, 2)
            self.assertIn("workstream_resume_runtime_unavailable", missing.stderr)

            target = skills / "workstream-ledger/scripts/workstream_resume.py"
            target.parent.mkdir(parents=True)
            target.write_text("import json, sys; print(json.dumps(sys.argv))\n")
            forwarded = subprocess.run(
                [sys.executable, str(shim), "GEN-37"], capture_output=True,
                text=True, check=True,
            )
            self.assertEqual(json.loads(forwarded.stdout), [str(target.resolve()), "GEN-37"])

    def test_tab_shim_forwards_exact_title_provenance_argv(self):
        source = (
            Path(__file__).parents[1]
            / "plugins/workstream/skills/workstream-resume/scripts/workstream_tab.py"
        )
        with tempfile.TemporaryDirectory() as directory:
            skills = Path(directory) / "skills"
            shim = skills / "workstream-resume/scripts/workstream_tab.py"
            shim.parent.mkdir(parents=True)
            shim.write_bytes(source.read_bytes())
            target = skills / "workstream-ledger/scripts/workstream_tab.py"
            target.parent.mkdir(parents=True)
            target.write_text("import json, sys; print(json.dumps(sys.argv))\n")
            args = [
                "GEN-37", "--project-name", "Linear Integration",
                "--automatic-title", "~/Code/pulp",
            ]

            forwarded = subprocess.run(
                [sys.executable, str(shim), *args], capture_output=True,
                text=True, check=True,
            )

            self.assertEqual(
                json.loads(forwarded.stdout), [str(target.resolve()), *args],
            )


if __name__ == "__main__":
    unittest.main()
