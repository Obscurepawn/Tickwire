#!/usr/bin/env python3
"""Regression tests for completed, per-model TLC coverage aggregation."""
from pathlib import Path
import tempfile
import unittest
import shutil
import subprocess

from tlc_coverage import CoverageError, check_logs


def block(a=1, b=0, model="Example"):
    return (
        "The coverage statistics at 2026-10-21 00:00:00\n"
        f"<Init line 1, col 1 to line 1, col 4 of module {model}>: 1:1\n"
        f"<A line 2, col 1 to line 2, col 1 of module {model}>: 0:{a}\n"
        f"<B line 3, col 1 to line 3, col 1 of module {model}>: 0:{b}\n"
        "End of statistics.\n"
    )


SUCCESS = "Model checking completed. No error has been found.\n"
SUMMARY = "100 states generated, 10 distinct states found, 0 states left on queue.\n"
FINISHED = "Finished in 01s at (2026-10-21 00:00:01)\n"


def run(a=1, b=0, model="Example"):
    return SUCCESS + block(a, b, model) + SUMMARY + FINISHED


class CoverageTests(unittest.TestCase):
    def check(self, *logs):
        with tempfile.TemporaryDirectory() as directory:
            paths = []
            for index, log in enumerate(logs):
                path = Path(directory) / f"{index}.log"
                path.write_text(log)
                paths.append(path)
            return check_logs(paths, "Example")

    def test_boundary_configurations_complement_each_other(self):
        result = self.check(run(5, 0), run(0, 7))
        self.assertEqual(result["runs"], 2)
        self.assertEqual(result["entries"], 3)
        self.assertEqual(sorted(result["generated"].values()), [2, 5, 7])

    def test_only_final_coverage_counts(self):
        result = self.check(block(999, 0) + run(2, 3))
        self.assertEqual(sorted(result["generated"].values()), [1, 2, 3])

    def test_intermediate_coverage_cannot_hide_missing_final_action(self):
        with self.assertRaises(CoverageError):
            self.check(block(1, 999) + run(1, 0))

    def test_all_zero_action_fails_across_configurations(self):
        with self.assertRaises(CoverageError):
            self.check(run(1, 0), run(2, 0))

    def test_incomplete_configuration_cannot_borrow_another_success(self):
        with self.assertRaises(CoverageError):
            self.check(run(1, 0), block(0, 1) + SUMMARY + FINISHED)

    def test_truncated_final_coverage_is_rejected(self):
        with self.assertRaises(CoverageError):
            self.check(run(1, 1) + block(1, 1).split("End of statistics.")[0])

    def test_unfinished_log_is_rejected(self):
        with self.assertRaises(CoverageError):
            self.check(run(1, 1).replace(FINISHED, ""))

    def test_nonzero_queue_is_rejected(self):
        with self.assertRaises(CoverageError):
            self.check(run(1, 1).replace("0 states left", "4 states left"))

    def test_failed_run_is_rejected_even_with_success_text(self):
        with self.assertRaises(CoverageError):
            self.check(run(1, 1) + "Error: Invariant violated.\n")

    def test_concatenated_runs_are_rejected(self):
        with self.assertRaises(CoverageError):
            self.check(run(1, 1) + run(1, 1))

    def test_different_model_and_action_sets_are_rejected(self):
        with self.assertRaises(CoverageError):
            self.check(run(1, 1), run(1, 1, "Other"))
        with self.assertRaises(CoverageError):
            self.check(run(1, 1), run(1, 1).replace("<B line", "<C line"))

    def test_duplicate_and_malformed_entries_are_rejected(self):
        entry = "<A line 2, col 1 to line 2, col 1 of module Example>: 0:1\n"
        with self.assertRaises(CoverageError):
            self.check(run(1, 1).replace(entry, entry * 2))
        with self.assertRaises(CoverageError):
            self.check(run(1, 1).replace(": 0:1", ": invalid"))

    def test_large_model_footer_and_comma_statistics(self):
        log = run(1, 1).replace(
            "End of statistics.",
            "End of statistics (please note that for performance reasons large models\n"
            "are best checked with coverage and cost statistics disabled).",
        ).replace("100 states", "1,000 states")
        self.assertEqual(self.check(log)["runs"], 1)

    def test_no_logs_is_rejected(self):
        with self.assertRaises(CoverageError):
            self.check()

    def test_committed_gateway_logs_cover_58_entries(self):
        directory = Path(__file__).resolve().parents[1] / (
            "tla/execution_models/account_gateway/L0_guarded_account_output/04_evidence"
        )
        paths = [directory / f"L0_AccountGateway_v2{suffix}.log"
                 for suffix in ("", "_boundary", "_two", "_crashes")]
        result = check_logs(paths, "L0_AccountGateway_v2")
        self.assertEqual(result["runs"], 4)
        self.assertEqual(result["entries"], 58)
        self.assertTrue(all(value > 0 for value in result["generated"].values()))


class RunnerTests(unittest.TestCase):
    def exercise(self, invariant):
        repo = Path(__file__).resolve().parents[1]
        jar = repo / "tla/tools/tla2tools.jar"
        if not jar.exists() or shutil.which("java") is None:
            self.skipTest("local pinned TLC jar and Java required for runner smoke")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            scripts = root / "scripts"
            scripts.mkdir()
            for name in ("check-tla.sh", "tlc_coverage.py"):
                shutil.copyfile(repo / "scripts" / name, scripts / name)
            tools = root / "tla/tools"
            tools.mkdir(parents=True)
            (tools / "tla2tools.jar").symlink_to(jar)
            models = root / "tla/execution_models/tiny/02_models"
            models.mkdir(parents=True)
            (models / "Tiny.tla").write_text(
                "---- MODULE Tiny ----\nEXTENDS Naturals\n"
                "CONSTANT Mode\nVARIABLE x\n"
                "Init == x = 0\n"
                "A == /\\ Mode = 1 /\\ x = 0 /\\ x' = 1\n"
                "B == /\\ Mode = 2 /\\ x = 0 /\\ x' = 1\n"
                "Next == A \\/ B\n"
                f"Safe == {invariant}\n====\n"
            )
            for mode in (1, 2):
                (models / f"mode{mode}.cfg").write_text(
                    f"CONSTANT Mode = {mode}\nINIT Init\nNEXT Next\n"
                    "INVARIANT Safe\nCHECK_DEADLOCK FALSE\n"
                )
            return subprocess.run(
                ["bash", str(scripts / "check-tla.sh")],
                capture_output=True, text=True, timeout=60,
            )

    def test_runner_aggregates_complementary_real_tlc_runs(self):
        result = self.exercise("x \\in 0..1")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(result.stdout.count(SUCCESS.strip()), 2)
        self.assertIn('"runs": 2', result.stdout)
        self.assertIn('"entries": 3', result.stdout)

    def test_failed_real_tlc_configuration_stops_runner(self):
        result = self.exercise("Mode = 1 \\/ x = 0")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Invariant Safe is violated.", result.stdout)
        self.assertNotIn('"runs": 2', result.stdout)


if __name__ == "__main__":
    unittest.main()
