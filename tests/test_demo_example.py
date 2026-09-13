"""Keep ``examples/demo_pipeline.py`` honest: run it and check the walkthrough.

The demo is the first thing a newcomer runs, so it must keep working and must
keep saying the same things. This module runs it once as a subprocess and
asserts on exit status and on the phrases that make the walkthrough readable.
"""

import subprocess
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DEMO = REPO_ROOT / "examples" / "demo_pipeline.py"


class DemoExampleTests(unittest.TestCase):
    """Run the offline demo once and assert on its real output."""

    @classmethod
    def setUpClass(cls):
        cls.completed = subprocess.run(
            [sys.executable, str(DEMO)],
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
            timeout=300,
        )
        cls.stdout = cls.completed.stdout

    def assertPresent(self, phrase):
        self.assertIn(phrase, self.stdout, "demo output lost the phrase: " + phrase)

    def test_demo_exits_zero(self):
        self.assertEqual(
            self.completed.returncode,
            0,
            "demo failed with stderr:\n" + self.completed.stderr,
        )

    def test_demo_prints_no_traceback(self):
        self.assertNotIn("Traceback (most recent call last)", self.stdout)
        self.assertNotIn("Traceback (most recent call last)", self.completed.stderr)

    def test_walkthrough_names_every_stage(self):
        for stage in ("STAGE 1: DISCOVERY", "STAGE 2: DEDUPE AND INGESTION", "STAGE 3: ENRICHMENT",
                      "STAGE 4: QUALIFICATION", "STAGE 5: DRAFTING", "STAGE 6: NOTIFICATION"):
            with self.subTest(stage=stage):
                self.assertPresent(stage)

    def test_demo_shows_dedupe_and_rejection_outcomes(self):
        self.assertPresent("leads created: 1 (three submitted records)")
        self.assertPresent("duplicate source record: routed to existing run 1")
        self.assertPresent("ingestion=INACTIVE_BUSINESS")

    def test_demo_shows_the_qualification_score_and_reason(self):
        self.assertPresent("score: 100/100 -> STRONG")
        self.assertPresent("points awarded per rubric component:")
        self.assertPresent("evidence signals")

    def test_demo_stops_at_human_review_and_does_not_claim_delivery(self):
        self.assertPresent("PIPELINE STOPPED FOR HUMAN REVIEW")
        self.assertPresent("draft 1 is in state REVIEW_PENDING and waiting for an operator")
        self.assertPresent("no email, message, or social contact was sent, and none will be")
        self.assertPresent("draft 1 version 1 state: REVIEW_PENDING")

    def test_demo_delivers_only_through_the_console_backend(self):
        self.assertPresent("[notification] SENT")
        self.assertPresent("notification outbox: ok=True")
        self.assertPresent("the console backend above printed the operator card to stdout")

    def test_demo_uses_fixture_providers_and_a_non_canonical_database(self):
        self.assertPresent("Temporary database (non-canonical, deleted on exit):")
        self.assertPresent("FixtureDraftingProvider")
        self.assertPresent("data/paldo_os_outbound.sqlite3 (repo-relative)")

    def test_demo_output_stays_plain_ascii(self):
        text = "\n".join(line for line in self.stdout.splitlines() if "/paldo-demo-" not in line)
        self.assertEqual(text, text.encode("ascii", "strict").decode("ascii"))


if __name__ == "__main__":
    unittest.main()
