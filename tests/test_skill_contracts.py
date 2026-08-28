import unittest
from pathlib import Path


class SkillContractTests(unittest.TestCase):
    def skill(self) -> str:
        return (
            Path(__file__).parents[1]
            / "plugins/workstream/skills/workstream-ledger/SKILL.md"
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
        resume = self.skill().split("### Fresh-session resume", 1)[1].split(
            "### Linear graph operations", 1
        )[0]
        command = resume.index('python3 "<absolute directory of the SKILL.md loaded for this turn>')
        substitute = resume.index("Substitute the runtime-supplied loaded skill path directly")
        cwd_warning = resume.index("Run it before reading repository instructions")
        path_warning = resume.index("Do not probe `workstreamctl` on `PATH`")
        history_warning = resume.index("The initial recovery command always omits")
        self.assertLess(command, substitute)
        self.assertLess(substitute, cwd_warning)
        self.assertLess(cwd_warning, path_warning)
        self.assertLess(path_warning, history_warning)


if __name__ == "__main__":
    unittest.main()
