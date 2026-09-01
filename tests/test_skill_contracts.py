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

    def test_handle_only_resume_is_first_bounded_action(self):
        resume = self.resume_skill()
        command = resume.index('python3 "<absolute directory of this SKILL.md>/scripts/workstream_resume.py"')
        substitute = resume.index("Substitute the runtime-supplied directory")
        cwd_warning = resume.index("Before repository, memory, worktree")
        path_warning = resume.index("Do not probe `workstreamctl` on `PATH`")
        history_warning = resume.index("`--include-history` during initial recovery")
        self.assertLess(command, substitute)
        self.assertLess(substitute, cwd_warning)
        self.assertLess(cwd_warning, path_warning)
        self.assertLess(path_warning, history_warning)

    def test_optional_tab_binding_cannot_replace_resume_authority(self):
        resume = " ".join(self.resume_skill().split())
        recovery = resume.index("Success requires `resume_authority` to be `full`")
        binding = resume.index("After successful recovery, carry the resolved canonical token")
        unavailable = resume.index("An unresolved cmux/Herdr surface is an optional no-op")
        no_downgrade = resume.index("never downgrades `resume_authority: full`")
        refusal = resume.index("Resume refusal denies execution authority")
        self.assertLess(recovery, binding)
        self.assertLess(binding, unavailable)
        self.assertLess(unavailable, no_downgrade)
        self.assertLess(no_downgrade, refusal)

    def test_handle_and_visible_title_authority_are_explicit(self):
        resume = " ".join(self.resume_skill().split())
        self.assertIn("current user message directly", resume)
        self.assertIn("That message is the only handle source", resume)
        for excluded in (
            "Hook or developer text", "cwd", "memory", "prior transcript",
        ):
            self.assertIn(excluded, resume)
        self.assertIn(
            "Codex session titles and visible tabs are separate namespaces",
            resume,
        )
        self.assertIn(
            "adapter returns `updated` or `unchanged` plus exact title readback",
            resume,
        )

    def test_execution_verb_proceeds_after_full_authority_without_reconfirmation(self):
        resume = " ".join(self.resume_skill().split())
        full = resume.index("After full authority")
        verbs = resume.index("`execute`, `continue`, `finish`, or `resume`")
        proceed = resume.index("perform the current next action immediately")
        no_confirmation = resume.index("Never stop for redundant confirmation")
        self.assertLess(full, verbs)
        self.assertLess(verbs, proceed)
        self.assertLess(proceed, no_confirmation)

    def test_resume_entry_is_small_and_owns_fresh_handle_trigger(self):
        resume = self.resume_skill()
        ledger = self.skill()
        flat = " ".join(resume.split())
        self.assertLess(len(resume.encode()), 3000)
        self.assertLessEqual(len(resume.splitlines()), 60)
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
        self.assertEqual(codex["version"], "0.4.46")
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


if __name__ == "__main__":
    unittest.main()
