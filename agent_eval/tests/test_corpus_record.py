"""Contracts for the deterministic, create-only projector corpus."""

from __future__ import annotations

import argparse
import base64
import contextlib
import io
import json
import shutil
import tempfile
import unittest
from pathlib import Path

from agent_eval import corpus_record as recorder
from agent_eval import v2_control, v2_receipts
from agent_eval.tests import v2_obs_corpus as harvest

SEED = b"\x00" * 32


def _tmp_run_dir(root: Path, game_id: str) -> Path:
    source = harvest.INCIDENT_ROOT / game_id
    target = root / game_id
    shutil.copytree(source, target)
    (target / "manifest.json").write_bytes(
        json.dumps(
            {
                "game_id": game_id,
                "control_protocol": "full-control-v2",
                "current_turn": 52,
                "finished_at": 1785916079.657395,
                "error": None,
                "config": {
                    "ruleset": "classic",
                    "mode": "single",
                    "places": 2,
                    "seeds": [2138428963],
                    "seats": [
                        {
                            "id": "place-1",
                            "name": "AgentPlace1",
                            "type": "external",
                            "controller_label": "test-controller",
                            "controller_fingerprint": "f" * 64,
                        }
                    ],
                },
            }
        ).encode("utf-8")
    )
    return target


def _record_args(run_dir: Path, out: Path, **overrides: object) -> argparse.Namespace:
    values: dict[str, object] = {
        "run_dir": [str(run_dir)],
        "runs_root": str(recorder.DEFAULT_RUNS_ROOT),
        "game": [],
        "out": str(out),
        "per_game": 1,
        "turn": [],
        "player": [0],
        "sections": "all",
        "limit": 16,
        "page_limit": 4,
        "follow_cursors": True,
        "entropy_seed": recorder.DEFAULT_ENTROPY_SEED,
        "live_margin": 0.0,
    }
    values.update(overrides)
    return argparse.Namespace(**values)


def _tree(root: Path) -> dict[str, tuple[int, int, bool]]:
    return {
        str(path.relative_to(root)): (
            path.stat().st_mtime_ns,
            path.stat().st_size,
            path.is_dir(),
        )
        for path in sorted(root.rglob("*"))
    }


class CanonicalBytesTests(unittest.TestCase):
    def test_canonical_json_matches_the_repository_codec(self):
        for value in (
            {"b": 1, "a": [1, 2, {"z": None, "y": True}]},
            {"unicode": "café ✓", "empty": {}, "list": []},
            {"float": 1.5, "int": 3, "neg": -0.25},
        ):
            with self.subTest(value=value):
                self.assertEqual(
                    recorder.canonical_json(value),
                    v2_receipts._canonical_json(value),
                )

    def test_canonical_json_rejects_non_finite_numbers(self):
        with self.assertRaises(ValueError):
            recorder.canonical_json({"nan": float("nan")})

    def test_canonical_lines_is_ndjson(self):
        self.assertEqual(
            recorder.canonical_lines([{"a": 1}, {"b": 2}]),
            b'{"a":1}\n{"b":2}\n',
        )
        self.assertEqual(recorder.canonical_lines([]), b"")


class DeterminismTests(unittest.TestCase):
    def test_counter_entropy_is_stable_seeded_and_length_preserving(self):
        first = recorder._CounterEntropy(SEED)
        second = recorder._CounterEntropy(SEED)
        self.assertEqual(first.token_bytes(64), second.token_bytes(64))
        self.assertNotEqual(
            recorder._CounterEntropy(SEED).token_bytes(32),
            recorder._CounterEntropy(b"\x11" * 32).token_bytes(32),
        )
        token = recorder._CounterEntropy(SEED).token_urlsafe(24)
        self.assertNotIn("=", token)
        self.assertEqual(len(base64.urlsafe_b64decode(token + "==")), 24)

    def test_entropy_and_clock_are_restored_even_on_failure(self):
        before = (v2_control.secrets, v2_control.time)
        with self.assertRaises(RuntimeError):
            with recorder.deterministic_entropy(SEED):
                self.assertIsInstance(v2_control.secrets, recorder._CounterEntropy)
                self.assertEqual(v2_control.time.time(), recorder.FROZEN_WALL_TIME)
                raise RuntimeError("boom")
        self.assertEqual((v2_control.secrets, v2_control.time), before)

    def test_moved_entropy_surface_fails_loudly(self):
        original = v2_control.secrets
        v2_control.secrets = object()  # ty: ignore[invalid-assignment]
        try:
            with self.assertRaisesRegex(recorder.CorpusError, "silent no-op"):
                recorder.assert_entropy_surface()
        finally:
            v2_control.secrets = original

    def test_selftest_proves_stability_and_seed_sensitivity(self):
        result = recorder.selftest()
        self.assertTrue(result["stable"])
        self.assertTrue(result["seed_sensitive"])
        self.assertEqual(len(result["digest"]), 64)


class SectionAndSaveTests(unittest.TestCase):
    def test_named_section_groups_are_real(self):
        self.assertEqual(recorder.resolve_sections("direct"), recorder.DIRECT_SECTIONS)
        self.assertTrue(
            set(recorder.resolve_sections("all")) <= set(v2_control._STATE_SECTIONS)
        )
        with self.assertRaisesRegex(recorder.CorpusError, "not_a_section"):
            recorder.resolve_sections("overview,not_a_section")

    def test_save_sampling_keeps_even_coverage_and_the_tail(self):
        paths = [Path(f"turn-{turn:04d}-auto.sav.gz") for turn in range(1, 11)]
        chosen, skipped = recorder.select_saves(
            paths,
            per_game=3,
            live_margin_s=0,
        )
        self.assertEqual(chosen[-1], paths[-1])
        self.assertEqual(len(chosen), 3)
        self.assertEqual(skipped, [])

    def test_explicit_turns_override_live_tail_filter(self):
        paths = [Path("turn-0001-auto.sav.gz"), Path("turn-0002-auto.sav.gz")]
        chosen, skipped = recorder.select_saves(
            paths,
            per_game=1,
            turns=[2],
            live_margin_s=999,
            now=0,
        )
        self.assertEqual(chosen, [paths[1]])
        self.assertEqual(skipped, [])

    def test_pins_follow_the_projector_row_schema(self):
        baseline = recorder.corpus_pins()["row_field_schema_digest"]
        original = v2_control._ROW_FIELDS
        v2_control._ROW_FIELDS = {**original, "TEST_ONLY": ("x",)}  # type: ignore[assignment]
        try:
            self.assertNotEqual(
                recorder.corpus_pins()["row_field_schema_digest"],
                baseline,
            )
        finally:
            v2_control._ROW_FIELDS = original  # type: ignore[assignment]


class RecordSampleTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        sample = harvest.incident_sample(harvest.ENTERTAINER_INCIDENT)
        if sample is None:  # pragma: no cover - committed fixture
            raise unittest.SkipTest("committed incident save is missing")
        cls.sample = sample

    def _record(self, **overrides: object) -> recorder.RecordedSample:
        values: dict[str, object] = {
            "game_id": recorder.FALLBACK_GAME_ID,
            "seed": SEED,
            "sections": recorder.DIRECT_SECTIONS,
            "limit": 16,
            "page_limit": 4,
            "follow_cursors": True,
        }
        values.update(overrides)
        return recorder.record_sample(self.sample, 0, **values)  # type: ignore[arg-type]

    def test_same_seed_is_byte_identical_and_other_seed_changes_only_pages(self):
        baseline = self._record()
        self.assertEqual(baseline.files, self._record().files)
        other = self._record(seed=b"\x11" * 32)
        self.assertEqual(set(baseline.files), set(other.files))
        self.assertNotEqual(
            baseline.files["pages/state/t0052-p0.overview.l16.json"],
            other.files["pages/state/t0052-p0.overview.l16.json"],
        )
        self.assertEqual(
            baseline.files["bundles/t0052-p0.rows.jsonl"],
            other.files["bundles/t0052-p0.rows.jsonl"],
        )

    def test_rows_and_bundle_digest_agree(self):
        sample = self._record()
        bundle = json.loads(sample.files["bundles/t0052-p0.bundle.json"])
        self.assertEqual(bundle["row_digest"], sample.row_digest)
        self.assertEqual(bundle["row_count"], len(sample.rows))
        rows = tuple(
            json.loads(line)
            for line in sample.files["bundles/t0052-p0.rows.jsonl"].splitlines()
        )
        self.assertEqual(rows, sample.rows)

    def test_cursor_chains_reach_their_terminal_page(self):
        sample = self._record()
        chains = [path for path in sample.files if path.endswith(".chain.jsonl")]
        self.assertTrue(chains)
        for path in chains:
            pages = [json.loads(line) for line in sample.files[path].splitlines()]
            self.assertGreater(len(pages), 1)
            self.assertIsNone(pages[-1]["page"]["next_cursor"])
            self.assertTrue(all(page["page"]["next_cursor"] for page in pages[:-1]))

    def test_pages_keep_the_full_envelope_and_legal_actions_oracle(self):
        sample = self._record()
        self.assertTrue(any("legal_actions" in path for path in sample.page_paths))
        for path in sample.page_paths:
            payloads = (
                sample.files[path].splitlines()
                if path.endswith(".jsonl")
                else [sample.files[path]]
            )
            for payload in payloads:
                page = json.loads(payload)
                self.assertEqual(page["schema_version"], 2)
                self.assertEqual(page["control_protocol"], "full-control-v2")
                self.assertEqual(page["game_id"], recorder.FALLBACK_GAME_ID)
                self.assertEqual(page["agent_id"], recorder.CORPUS_AGENT_ID)
                self.assertEqual(
                    sorted(page["state_revision"]),
                    ["revision", "state_token", "turn"],
                )
                self.assertIn("items", page["page"])

    def test_unreachable_sections_are_explicit_gaps(self):
        sample = self._record(
            sections=(*recorder.DIRECT_SECTIONS, "map_tiles"),
        )
        self.assertIn("unreachable_section", {note["kind"] for note in sample.notes})
        self.assertFalse(any("map_tiles" in path for path in sample.page_paths))

    def test_emission_refuses_duplicate_and_traversal_paths(self):
        sample = recorder.RecordedSample(
            key="t0001-p0",
            turn=1,
            player_index=0,
            save_name="x",
            rows=(),
            row_digest="",
        )
        recorder._emit(sample, "pages/state/a.json", b"{}", page=True)
        with self.assertRaises(recorder.CorpusError):
            recorder._emit(sample, "pages/state/a.json", b"{}", page=True)
        with self.assertRaises(recorder.CorpusError):
            recorder._emit(sample, "bundles/../../../escape", b"{}")


class EndToEndTests(unittest.TestCase):
    def setUp(self):
        self._scratch = tempfile.TemporaryDirectory(prefix="corpus-e2e-")
        self.root = Path(self._scratch.name)
        self.game_id = harvest.ENTERTAINER_INCIDENT[0]
        self.run_dir = _tmp_run_dir(self.root / "runs", self.game_id)
        self.out = self.root / "corpus"
        self.stream = io.StringIO()

    def tearDown(self):
        self._scratch.cleanup()

    def _record(self, out: Path | None = None, **overrides: object) -> int:
        return recorder.command_record(
            _record_args(self.run_dir, self.out if out is None else out, **overrides),
            self.stream,
        )

    @property
    def target(self) -> Path:
        return self.out / self.game_id

    def test_recording_is_minimal_deterministic_and_self_describing(self):
        first = recorder.build_corpora(_record_args(self.run_dir, self.out))[0]
        second = recorder.build_corpora(_record_args(self.run_dir, self.out))[0]
        self.assertEqual(first.rendered(), second.rendered())
        self.assertEqual(self._record(), 0)
        self.assertTrue((self.target / "index.json").is_file())
        self.assertTrue((self.target / "gaps.json").is_file())
        self.assertFalse((self.target / "MANIFEST.sha256").exists())
        self.assertFalse((self.target / "frames").exists())
        self.assertFalse((self.target / "ids").exists())
        index = json.loads((self.target / "index.json").read_bytes())
        self.assertEqual(index["schema_version"], recorder.CORPUS_SCHEMA_VERSION)
        self.assertTrue(index["samples"])
        self.assertTrue(index["files"])
        self.assertEqual(
            index["corpus_digest"],
            recorder.sha256_hex(
                recorder.canonical_json(
                    {
                        path: entry["sha256"]
                        for path, entry in sorted(index["files"].items())
                    }
                )
            ),
        )
        gaps = json.loads((self.target / "gaps.json").read_bytes())
        self.assertEqual(gaps["corpus_gaps"], harvest.CORPUS_GAPS)
        self.assertIn("reconstructed", gaps["detail"])

    def test_recording_never_touches_the_source_run(self):
        before = _tree(self.run_dir)
        self.assertEqual(self._record(), 0)
        self.assertEqual(_tree(self.run_dir), before)

    def test_create_only_writer_refuses_existing_output_unchanged(self):
        self.assertEqual(self._record(), 0)
        before = recorder.read_corpus_files(self.target)
        with self.assertRaisesRegex(recorder.CorpusError, "create-only"):
            recorder.write_corpus(
                recorder.build_corpora(_record_args(self.run_dir, self.out))[0],
                self.out,
            )
        self.assertEqual(recorder.read_corpus_files(self.target), before)

    def test_writer_refuses_output_inside_the_source_run(self):
        corpus = recorder.build_corpora(_record_args(self.run_dir, self.out))[0]
        with self.assertRaisesRegex(recorder.CorpusError, "inside source run"):
            recorder.write_corpus(corpus, self.run_dir / "corpus")
        self.assertFalse((self.run_dir / "corpus").exists())

    def test_verify_catches_tamper_missing_unexpected_and_schema(self):
        self.assertEqual(self._record(), 0)
        args = argparse.Namespace(corpus=str(self.target), strict=False)
        self.assertEqual(recorder.command_verify(args, self.stream), 0)

        victim = self.target / "gaps.json"
        original = victim.read_bytes()
        victim.write_bytes(original + b" ")
        self.assertEqual(recorder.command_verify(args, self.stream), 1)
        victim.write_bytes(original)

        victim.unlink()
        self.assertEqual(recorder.command_verify(args, self.stream), 1)
        victim.write_bytes(original)

        extra = self.target / "pages" / "unexpected.json"
        extra.write_bytes(b"{}")
        self.assertEqual(recorder.command_verify(args, self.stream), 1)
        extra.unlink()

        index_path = self.target / "index.json"
        index = json.loads(index_path.read_bytes())
        index["schema_version"] = 1
        index_path.write_bytes(recorder.pretty_json(index))
        self.assertEqual(recorder.command_verify(args, self.stream), 1)
        output = self.stream.getvalue()
        self.assertIn("digest mismatch", output)
        self.assertIn("missing gaps.json", output)
        self.assertIn("unexpected pages/unexpected.json", output)
        self.assertIn("re-record the corpus", output)

    def test_strict_verify_compares_a_fresh_derivation_to_disk(self):
        self.assertEqual(self._record(), 0)
        strict = argparse.Namespace(corpus=str(self.target), strict=True)
        self.assertEqual(recorder.command_verify(strict, self.stream), 0)
        self.assertIn("re-derived byte-identically", self.stream.getvalue())

        page = next(self.target.rglob("*.overview.l16.json"))
        document = json.loads(page.read_bytes())
        document["page"]["items"] = []
        page.write_bytes(recorder.canonical_json(document))
        index_path = self.target / "index.json"
        index = json.loads(index_path.read_bytes())
        relpath = page.relative_to(self.target).as_posix()
        payload = page.read_bytes()
        index["files"][relpath] = {
            "sha256": recorder.sha256_hex(payload),
            "bytes": len(payload),
        }
        index["corpus_digest"] = recorder.sha256_hex(
            recorder.canonical_json(
                {
                    path: entry["sha256"]
                    for path, entry in sorted(index["files"].items())
                }
            )
        )
        index_path.write_bytes(recorder.pretty_json(index))
        plain = argparse.Namespace(corpus=str(self.target), strict=False)
        self.assertEqual(recorder.command_verify(plain, io.StringIO()), 0)
        strict_output = io.StringIO()
        self.assertEqual(recorder.command_verify(strict, strict_output), 1)
        self.assertIn("differs from re-derivation", strict_output.getvalue())

    def test_strict_verify_names_an_unavailable_source(self):
        self.assertEqual(self._record(), 0)
        shutil.rmtree(self.run_dir)
        output = io.StringIO()
        self.assertEqual(
            recorder.command_verify(
                argparse.Namespace(corpus=str(self.target), strict=True),
                output,
            ),
            0,
        )
        self.assertIn("SKIP strict", output.getvalue())
        self.assertIn("re-derivation was not", output.getvalue())


class CommandLineTests(unittest.TestCase):
    def test_parser_exposes_only_record_and_verify(self):
        parser = recorder.build_parser()
        self.assertEqual(parser.parse_args(["record"]).command, "record")
        self.assertEqual(
            parser.parse_args(["verify", "--corpus", "x"]).command,
            "verify",
        )
        for removed in ("--force", "--frames", "--stamp", "--dry-run"):
            with (
                self.subTest(removed=removed),
                self.assertRaises(SystemExit),
                contextlib.redirect_stderr(io.StringIO()),
            ):
                parser.parse_args(["record", removed])

    def test_main_records_and_verifies_end_to_end(self):
        with tempfile.TemporaryDirectory(prefix="corpus-cli-") as scratch:
            root = Path(scratch)
            game_id = harvest.ENTERTAINER_INCIDENT[0]
            run_dir = _tmp_run_dir(root / "runs", game_id)
            out = root / "corpus"
            stream = io.StringIO()
            code = recorder.main(
                [
                    "record",
                    "--run-dir",
                    str(run_dir),
                    "--out",
                    str(out),
                    "--per-game",
                    "1",
                    "--player",
                    "0",
                    "--limit",
                    "16",
                    "--page-limit",
                    "4",
                    "--live-margin",
                    "0",
                ],
                stream=stream,
            )
            self.assertEqual(code, 0)
            self.assertEqual(
                recorder.main(
                    [
                        "verify",
                        "--corpus",
                        str(out / game_id),
                        "--strict",
                    ],
                    stream=stream,
                ),
                0,
            )


if __name__ == "__main__":
    unittest.main()
