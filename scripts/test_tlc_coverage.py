#!/usr/bin/env python3
"""Regression tests for completed, per-model TLC coverage aggregation."""
from pathlib import Path
import tempfile
import unittest
import shutil
import subprocess

from tlc_coverage import CoverageError, check_logs
from tlc_native_overrides import emit_override_classes


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

    def test_different_models_and_moved_declarations_are_rejected(self):
        with self.assertRaises(CoverageError):
            self.check(run(1, 1), run(1, 1, "Other"))
        with self.assertRaises(CoverageError):
            self.check(run(1, 1), run(1, 1).replace("<B line 3", "<B line 4"))

    def test_distinct_call_sites_share_a_declaration_but_each_requires_coverage(self):
        declaration = "A line 2, col 1 to line 2, col 1 of module Example"
        first = f"{declaration} (215 25 215 59)"
        second = f"{declaration} (216 25 216 72)"

        def with_sites(a, b):
            return run(1, 1).replace(
                f"<{declaration}>: 0:1", f"<{first}>: 0:{a}\n<{second}>: 0:{b}")

        totals = self.check(with_sites(2, 0), with_sites(0, 3))["generated"]
        self.assertEqual((totals[first], totals[second]), (2, 3))
        with self.assertRaisesRegex(CoverageError, "uncovered across all configurations"):
            self.check(with_sites(2, 0))
        with self.assertRaisesRegex(CoverageError, "declaration changed for action A"):
            self.check(with_sites(1, 1), with_sites(1, 1).replace("line 2,", "line 4,"))

    def test_conditional_next_actions_are_merged_across_configurations(self):
        only_a = run(2, 0).replace(
            "<B line 3, col 1 to line 3, col 1 of module Example>: 0:0\n", "")
        only_b = run(0, 3).replace(
            "<A line 2, col 1 to line 2, col 1 of module Example>: 0:0\n", "")
        self.assertEqual(sorted(self.check(only_a, only_b)["generated"].values()), [2, 2, 3])

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

    def test_scaling_logs_validate_completion_alongside_coverage_logs(self):
        scaling_ok = SUCCESS + SUMMARY + FINISHED
        with tempfile.TemporaryDirectory() as directory:
            cov_p = Path(directory) / "cov.log"
            scl_p = Path(directory) / "scale.log"
            cov_p.write_text(run(3, 4))
            scl_p.write_text(scaling_ok)
            res = check_logs([cov_p], "Example", scaling_paths=[scl_p])
            self.assertEqual(res["runs"], 2)
            self.assertEqual(res["entries"], 3)

            for bad in (
                scaling_ok.replace(FINISHED, ""),
                scaling_ok.replace("0 states left", "2 states left"),
                scaling_ok + "Error: Invariant Safe is violated.\n",
                scaling_ok + scaling_ok,
            ):
                scl_p.write_text(bad)
                with self.assertRaises(CoverageError):
                    check_logs([cov_p], "Example", scaling_paths=[scl_p])

            scl_p.write_text(scaling_ok)
            with self.assertRaises(CoverageError):
                check_logs([], "Example", scaling_paths=[scl_p])

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

    def test_private_product_configs_have_complementary_next_branches(self):
        directory = Path(__file__).resolve().parents[1] / (
            "tla/execution_models/binance_private_session/L0_session_lifecycle/"
            "04_evidence/runs/pre-final-sample-v2"
        )
        paths = [directory / f"L0_PrivateSession{suffix}.log"
                 for suffix in ("", "_logon", "_usdm", "_buffer")]
        result = check_logs(paths, "L0_PrivateSession")
        self.assertEqual(result["runs"], 4)
        self.assertEqual(result["entries"], 31)


class RunnerTests(unittest.TestCase):
    def exercise(self, invariant, conditional=False):
        repo = Path(__file__).resolve().parents[1]
        jar = repo / "tla/tools/tla2tools.jar"
        if not jar.exists() or shutil.which("java") is None:
            self.skipTest("local pinned TLC jar and Java required for runner smoke")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            scripts = root / "scripts"
            scripts.mkdir()
            for name in ("check-tla.sh", "tlc_coverage.py", "tlc_native_overrides.py"):
                shutil.copyfile(repo / "scripts" / name, scripts / name)
            tools = root / "tla/tools"
            tools.mkdir(parents=True)
            (tools / "tla2tools.jar").symlink_to(jar)
            models = root / "tla/execution_models/tiny/02_models"
            models.mkdir(parents=True)
            next_expr = "IF Mode = 1 THEN ModeA ELSE ModeB" if conditional else "A \\/ B"
            branches = (
                "Shared == /\\ x = 1 /\\ UNCHANGED x\n"
                "ModeA == Shared \\/ A\nModeB == Shared \\/ B\n"
            ) if conditional else ""
            (models / "Tiny.tla").write_text(
                "---- MODULE Tiny ----\nEXTENDS Naturals\n"
                "CONSTANT Mode\nVARIABLE x\n"
                "Init == x = 0\n"
                "A == /\\ Mode = 1 /\\ x = 0 /\\ x' = 1\n"
                "B == /\\ Mode = 2 /\\ x = 0 /\\ x' = 1\n"
                f"{branches}"
                f"Next == {next_expr}\n"
                f"Safe == {invariant}\nSpec == Init /\\ [][Next]_x\n====\n"
            )
            for mode in (1, 2):
                behavior = "SPECIFICATION Spec" if conditional else "INIT Init\nNEXT Next"
                (models / f"mode{mode}.cfg").write_text(
                    f"CONSTANT Mode = {mode}\n{behavior}\n"
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

    def test_runner_accepts_conditional_next_root_coverage(self):
        result = self.exercise("x \\in 0..1", conditional=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        # TLC reports the small conditional expression as one Next entry.
        # The actual private-session logs above exercise branch-specific entries.
        self.assertIn('"entries": 2', result.stdout)

    def test_runner_supports_multiple_model_families_in_parallel(self):
        repo = Path(__file__).resolve().parents[1]
        jar = repo / "tla/tools/tla2tools.jar"
        if not jar.exists() or shutil.which("java") is None:
            self.skipTest("local pinned TLC jar and Java required for runner smoke")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            scripts = root / "scripts"
            scripts.mkdir()
            for name in ("check-tla.sh", "tlc_coverage.py", "tlc_native_overrides.py"):
                shutil.copyfile(repo / "scripts" / name, scripts / name)
            tools = root / "tla/tools"
            tools.mkdir(parents=True)
            (tools / "tla2tools.jar").symlink_to(jar)
            for fam, mod in (("alpha", "Alpha"), ("beta", "Beta"), ("unused", "Unused")):
                models = root / f"tla/execution_models/{fam}/L0_smoke/02_models"
                models.mkdir(parents=True)
                (models / f"{mod}.tla").write_text(
                    f"---- MODULE {mod} ----\nEXTENDS Naturals\nVARIABLE x\n"
                    "Init == x = 0\nNext == x = 0 /\\ x' = 1\nSafe == x \\in 0..1\n====\n"
                )
                (models / f"{mod}.cfg").write_text(
                    "INIT Init\nNEXT Next\nINVARIANT Safe\nCHECK_DEADLOCK FALSE\n"
                )
            result = subprocess.run(
                ["bash", str(scripts / "check-tla.sh"), "beta", "alpha"],
                capture_output=True, text=True, timeout=60,
            )
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertIn('"model": "Alpha"', result.stdout)
            self.assertIn('"model": "Beta"', result.stdout)
            self.assertNotIn('"model": "Unused"', result.stdout)
            self.assertLess(result.stdout.index('"model": "Alpha"'), result.stdout.index('"model": "Beta"'))

    def test_native_override_classes_preserve_word_semantics(self):
        repo = Path(__file__).resolve().parents[1]
        jar = repo / "tla/tools/tla2tools.jar"
        if not jar.exists() or shutil.which("java") is None:
            self.skipTest("local pinned TLC jar and Java required for override smoke")
        with tempfile.TemporaryDirectory() as directory:
            out = Path(directory)
            emit_override_classes(out, include_depth_sync=True)
            for cls in (
                "TLCOverrides.class",
                "L0_CalibrationOverrides.class",
                "L0_DepthSyncOverrides.class",
            ):
                p = out / "tlc2" / "overrides" / cls
                self.assertTrue(p.is_file(), cls)
                self.assertEqual(p.read_bytes()[:4], b"\xca\xfe\xba\xbe")
            # Verify `L0_CalibrationOverrides` produces identical Word values to TLA+ formulas:
            (out / "L0_Calibration.tla").write_text(
                "---- MODULE L0_Calibration ----\nEXTENDS Naturals\n"
                "Word == [1..10 -> 0..999]\n"
                "Zero == [i \\in 1..10 |-> 0]\nOne == [Zero EXCEPT ![1] = 1]\n"
                "Add(a, b) == Zero\nSub(a, b) == Zero\nSatSub(a, b) == Zero\n"
                "Lt(a, b) == FALSE\nLe(a, b) == FALSE\n"
                "Shift(a, k) == Zero\nDrop(a, k) == Zero\nCeilDrop(a, k) == Zero\n"
                "Half(a) == Zero\nMid(a, b) == Zero\nQuantized(a, k) == Zero\n"
                "FractionCeil(a, m) == Zero\nUp(a, m) == Zero\nDown(a, m) == Zero\n"
                "VARIABLE x\n"
                "W999 == [Zero EXCEPT ![1] = 999]\n"
                "Init == x = Add(W999, One)\n"
                "Next == UNCHANGED x\n"
                "Safe == /\\ x = [Zero EXCEPT ![2] = 1]\n"
                "        /\\ Lt(W999, x)\n"
                "        /\\ Sub(x, One) = W999\n"
                "        /\\ SatSub(One, x) = Zero\n"
                "        /\\ CeilDrop(One, 3) = One\n"
                "        /\\ Up(One, 1) = [Zero EXCEPT ![1] = 2]\n"
                "        /\\ Down(One, 1) = Zero\n"
                "====\n"
            )
            (out / "L0_Calibration.cfg").write_text(
                "INIT Init\nNEXT Next\nINVARIANT Safe\nCHECK_DEADLOCK FALSE\n"
            )
            res = subprocess.run(
                [
                    "java",
                    "-Dtickwire.tlc.maxId=2",
                    "-cp",
                    f"{out}:{jar}",
                    "tlc2.TLC",
                    "-checkpoint",
                    "0",
                    "-workers",
                    "1",
                    "-config",
                    "L0_Calibration.cfg",
                    "L0_Calibration.tla",
                ],
                cwd=out,
                capture_output=True,
                text=True,
                timeout=30,
            )
            self.assertEqual(res.returncode, 0, res.stdout + res.stderr)
            self.assertIn(SUCCESS.strip(), res.stdout)

    def test_depth_sync_overrides_preserve_spot_and_usdm_replay_semantics(self):
        repo = Path(__file__).resolve().parents[1]
        jar = repo / "tla/tools/tla2tools.jar"
        if not jar.exists() or shutil.which("java") is None:
            self.skipTest("local pinned TLC jar and Java required for override smoke")
        with tempfile.TemporaryDirectory() as directory:
            out = Path(directory)
            emit_override_classes(out, include_depth_sync=True)
            (out / "L0_DepthSync.tla").write_text(
                "---- MODULE L0_DepthSync ----\n"
                "EXTENDS Integers, Sequences\n"
                "CONSTANT Policy\n"
                "No == -1\n"
                "Events(t, g) == {}\n"
                "Classify(b, e) == \"Stale\"\n"
                "Apply(b, e) == b\n"
                "RECURSIVE Replay(_, _, _)\n"
                "Replay(b, q, a) == [book |-> b, applied |-> a]\n"
                "VARIABLE ok\n"
                "B0 == [id |-> 1, stamp |-> No, qty |-> 0, phase |-> \"Bridging\", gen |-> 1]\n"
                "E1 == [first |-> 0, last |-> 1, previous |-> 0, stamp |-> 1, gen |-> 1, qty |-> 1, valid |-> TRUE]\n"
                "E2 == [first |-> 2, last |-> 2, previous |-> 1, stamp |-> 2, gen |-> 1, qty |-> 0, valid |-> TRUE]\n"
                "R == Replay(B0, <<E1, E2>>, 0)\n"
                "Init == ok = (\n"
                "  /\\ Events(1, 2) # {}\n"
                "  /\\ IF Policy = \"Spot\"\n"
                "     THEN Classify(B0, E1) = \"Stale\" /\\ R.applied = 1 /\\ R.book.id = 2 /\\ R.book.phase = \"Live\"\n"
                "     ELSE Classify(B0, E1) = \"Apply\" /\\ R.applied = 2 /\\ R.book.id = 2 /\\ R.book.phase = \"Live\"\n"
                ")\n"
                "Next == UNCHANGED ok\n"
                "Safe == ok = TRUE\n"
                "====\n"
            )
            for pol in ("Spot", "UsdM"):
                (out / f"{pol}.cfg").write_text(
                    f'CONSTANT Policy = "{pol}"\n'
                    "INIT Init\nNEXT Next\nINVARIANT Safe\nCHECK_DEADLOCK FALSE\n"
                )
                res = subprocess.run(
                    [
                        "java",
                        "-Dtickwire.tlc.maxId=2",
                        f"-Dtickwire.tlc.policy={pol}",
                        "-cp",
                        f"{out}:{jar}",
                        "tlc2.TLC",
                        "-checkpoint",
                        "0",
                        "-workers",
                        "1",
                        "-config",
                        f"{pol}.cfg",
                        "L0_DepthSync.tla",
                    ],
                    cwd=out,
                    capture_output=True,
                    text=True,
                    timeout=30,
                )
                self.assertEqual(res.returncode, 0, res.stdout + res.stderr)
                self.assertIn(SUCCESS.strip(), res.stdout)


if __name__ == "__main__":
    unittest.main()
