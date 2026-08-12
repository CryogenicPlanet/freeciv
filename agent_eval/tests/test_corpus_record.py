"""The corpus recorder is an oracle, so its own guarantees have to be proved.

A golden corpus is only worth what its determinism claim is worth.  If the
entropy swap silently stopped taking effect, or if the strict verifier compared
a rebuild against itself, or if the recorder wrote so much as a cache file into
a run directory a live game was using, the corpus would still *look* fine and
would be worthless -- or worse, actively harmful.  Each of those is a test here.

Nothing in this file reads the live ``.agent-eval/runs`` root.  The end-to-end
tests build a run directory in a temp dir out of the two **committed** incident
saves (``agent_eval/tests/fixtures/incidents``), which is the same escape hatch
``v2_obs_corpus`` invented after relying on a gitignored directory made the
campaign's strongest empirical result silently SKIP everywhere else.
"""

from __future__ import annotations

import argparse
import base64
import io
import json
from pathlib import Path
import shutil
import sys
import tempfile
import unittest

from agent_eval import corpus_record as recorder
from agent_eval import headless_sidecar as native
from agent_eval import v2_control
from agent_eval import v2_receipts
from agent_eval.tests import v2_obs_corpus as harvest
from agent_eval.tests import v2_obs_fixtures as fixtures

SEED = b"\x00" * 32


def _tmp_run_dir(root: Path, game_id: str) -> Path:
    """A run directory built from a committed incident save.

    A *copy*, always.  A test that recorded against the real runs root would be
    reading files a live game may be writing, which is exactly the interference
    the recorder is designed not to cause.
    """
    source = harvest.INCIDENT_ROOT / game_id
    target = root / game_id
    shutil.copytree(source, target)
    (target / "manifest.json").write_bytes(json.dumps({
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
            "seats": [{
                "id": "place-1", "name": "AgentPlace1", "type": "external",
                "controller_label": "test-controller",
                "controller_fingerprint": "f" * 64,
            }],
        },
        "resolved_places": [
            {"controller": "agent", "player_name": "AgentPlace1", "place": 1},
        ],
    }).encode("utf-8"))
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
        "frames": True,
        "entropy_seed": recorder.DEFAULT_ENTROPY_SEED,
        "live_margin": 0.0,
        "stamp": False,
        "dry_run": False,
        "force": False,
    }
    values.update(overrides)
    return argparse.Namespace(**values)


def _tree(root: Path) -> dict[str, tuple[int, int, bool]]:
    return {
        str(path.relative_to(root)): (
            path.stat().st_mtime_ns, path.stat().st_size, path.is_dir(),
        )
        for path in sorted(root.rglob("*"))
    }


# --------------------------------------------------------------------------
# Canonical bytes
# --------------------------------------------------------------------------


class CanonicalBytesTests(unittest.TestCase):
    """The corpus and the wire must agree on what canonical means."""

    def test_canonical_json_is_the_repo_canonical_form(self):
        """Not a re-implementation: the same bytes ``v2_receipts`` writes.

        ``arena/wire``'s ``canon.ts`` was written to reproduce that encoding
        byte for byte, so a corpus encoded any other way would fail parity for
        a reason that has nothing to do with the projector.
        """
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

    def test_canonical_lines_terminates_every_line_and_adds_no_blank(self):
        payload = recorder.canonical_lines([{"a": 1}, {"b": 2}])
        self.assertEqual(payload, b'{"a":1}\n{"b":2}\n')
        self.assertFalse(payload.endswith(b"\n\n"))
        self.assertEqual(recorder.canonical_lines([]), b"")

    def test_pretty_json_content_matches_the_canonical_content(self):
        """Indentation is for humans and must never move a digest."""
        value = {"b": [3, 2, 1], "a": {"n": None}}
        self.assertEqual(
            json.loads(recorder.pretty_json(value).decode("utf-8")),
            json.loads(recorder.canonical_json(value).decode("utf-8")),
        )
        self.assertTrue(recorder.pretty_json(value).endswith(b"\n"))


# --------------------------------------------------------------------------
# Determinism
# --------------------------------------------------------------------------


class DeterministicEntropyTests(unittest.TestCase):

    def test_counter_entropy_is_reproducible_and_seed_sensitive(self):
        first = recorder._CounterEntropy(SEED)
        second = recorder._CounterEntropy(SEED)
        other = recorder._CounterEntropy(b"\x11" * 32)
        self.assertEqual(first.token_bytes(32), second.token_bytes(32))
        self.assertNotEqual(
            recorder._CounterEntropy(SEED).token_bytes(32),
            other.token_bytes(32),
        )

    def test_counter_entropy_honours_requested_lengths(self):
        source = recorder._CounterEntropy(SEED)
        for length in (1, 12, 24, 32, 64, 100):
            with self.subTest(length=length):
                self.assertEqual(len(source.token_bytes(length)), length)
        token = recorder._CounterEntropy(SEED).token_urlsafe(24)
        self.assertNotIn("=", token)
        self.assertEqual(len(base64.urlsafe_b64decode(token + "==")), 24)

    def test_the_swap_is_installed_and_then_fully_restored(self):
        before = (v2_control.secrets, v2_control.time)
        with recorder.deterministic_entropy(SEED):
            self.assertIsInstance(v2_control.secrets, recorder._CounterEntropy)
            self.assertIsInstance(v2_control.time, recorder._FrozenClock)
            self.assertEqual(v2_control.time.time(), recorder.FROZEN_WALL_TIME)
        self.assertEqual((v2_control.secrets, v2_control.time), before)

    def test_the_swap_is_restored_even_when_the_body_raises(self):
        before = (v2_control.secrets, v2_control.time)
        with self.assertRaises(RuntimeError):
            with recorder.deterministic_entropy(SEED):
                raise RuntimeError("boom")
        self.assertEqual((v2_control.secrets, v2_control.time), before)

    def test_a_moved_entropy_surface_fails_loudly_rather_than_silently(self):
        """The single most fragile line of the design, pinned.

        If a refactor writes ``from secrets import token_bytes`` inside
        ``v2_control``, patching the module attribute becomes a no-op and the
        recorder would emit a corpus that only claims to be reproducible.  That
        must be an exception, not a surprise found months later by a parity rig.
        """
        original = v2_control.secrets
        v2_control.secrets = object()  # type: ignore[assignment]
        try:
            with self.assertRaises(recorder.CorpusError) as caught:
                recorder.assert_entropy_surface()
            self.assertIn("silent no-op", str(caught.exception))
        finally:
            v2_control.secrets = original

    def test_selftest_proves_stability_and_seed_sensitivity(self):
        result = recorder.selftest()
        self.assertTrue(result["stable"])
        self.assertTrue(result["seed_sensitive"])
        self.assertEqual(len(result["digest"]), 64)

    def test_without_the_swap_the_same_page_is_not_reproducible(self):
        """The control case: proof the swap is doing the work, not luck."""
        observation = fixtures.observation(fixtures.case("unswapped"))

        def render() -> bytes:
            control = v2_control.V2SeatControl(
                recorder.FALLBACK_GAME_ID, recorder.CORPUS_AGENT_ID, 1,
            )
            return recorder.canonical_json(
                control.state_page(observation, "overview", 16),
            )

        self.assertNotEqual(render(), render())

    def test_sample_seed_separates_samples_and_is_stable(self):
        self.assertEqual(
            recorder.sample_seed(SEED, "t0001-p0"),
            recorder.sample_seed(SEED, "t0001-p0"),
        )
        self.assertNotEqual(
            recorder.sample_seed(SEED, "t0001-p0"),
            recorder.sample_seed(SEED, "t0001-p1"),
        )


# --------------------------------------------------------------------------
# Frame synthesis
# --------------------------------------------------------------------------


class FrameRecordTests(unittest.TestCase):

    def test_length_is_encoded_bytes_and_base64_is_authoritative(self):
        record = recorder.frame_record(3, "c2h", "READY\tCafé")
        self.assertEqual(record["len"], len("READY\tCafé".encode("utf-8")))
        self.assertNotEqual(record["len"], len("READY\tCafé"))
        self.assertEqual(
            base64.b64decode(record["payload_b64"]).decode("utf-8"),
            record["payload"],
        )

    def test_forbidden_control_bytes_are_refused(self):
        """Every C0 control except TAB is illegal in a frame; a corpus that
        recorded one would teach a port to accept an unparseable frame."""
        for payload in ("bad\nframe", "bad\x00frame", "bad\x7fframe"):
            with self.subTest(payload=payload):
                with self.assertRaises(recorder.CorpusError):
                    recorder.frame_record(0, "c2h", payload)
        recorder.frame_record(0, "c2h", "good\tframe")

    def test_frame_bounds_and_direction_are_enforced(self):
        with self.assertRaises(recorder.CorpusError):
            recorder.frame_record(0, "c2h", "")
        with self.assertRaises(recorder.CorpusError):
            recorder.frame_record(0, "c2h", "x" * (native.MAX_FRAME + 1))
        with self.assertRaises(recorder.CorpusError):
            recorder.frame_record(0, "sideways", "x")

    def test_handshake_caps_frame_passes_the_real_validator(self):
        """The recorded CAPS frame is the one ``start_and_take`` accepts.

        ``_validate_native_caps`` pins six fields, the exact capability tuple in
        exact order, the encoding, the frame cap and the schema id.  Asserting
        against the validator rather than against a retyped string is what makes
        the corpus unable to drift.
        """
        frames = recorder.handshake_frames("AgentPlace1")
        caps = [item for item in frames if item["payload"].startswith("CAPS\t")]
        self.assertEqual(len(caps), 1)
        native._validate_native_caps(caps[0]["payload"])

    def test_handshake_is_the_exact_bootstrap_sequence(self):
        frames = recorder.handshake_frames("AgentPlace1")
        self.assertEqual(
            [(item["dir"], item["payload"].split("\t")[0]) for item in frames],
            [
                ("c2h", "HELLO"), ("h2c", "HELLO"), ("c2h", "HELLO"),
                ("c2h", "CAPS"), ("h2c", "TAKE"), ("c2h", "TAKE"),
                ("c2h", "TAKE"), ("c2h", "TAKE"), ("c2h", "READY"),
            ],
        )
        self.assertEqual([item["seq"] for item in frames], list(range(9)))
        self.assertEqual(frames[0]["payload"], "HELLO\t1\tfreeciv-agent")

    def test_an_unsafe_player_name_falls_back_rather_than_emitting_it(self):
        self.assertEqual(recorder.safe_player_name("Agent\tPlace"), "AgentPlace1")
        self.assertEqual(recorder.safe_player_name(None), "AgentPlace1")
        self.assertEqual(recorder.safe_player_name("Player-2"), "Player-2")

    def test_observation_frames_page_the_whole_bundle_exactly_once(self):
        rows = fixtures.build_rows(fixtures.case("frames"))
        frames = recorder.observation_frames(11, rows)
        kinds = [item["payload"].split("\t")[0] for item in frames]
        self.assertEqual(kinds[0], "OBS_OPEN")
        self.assertEqual(kinds[1], "OBS_OPENED")
        self.assertEqual(kinds.count("ROW"), len(rows))
        pages = -(-len(rows) // native.MAX_OBSERVATION_PAGE)
        self.assertEqual(kinds.count("OBS_PAGE"), pages)
        self.assertEqual(kinds.count("PAGE_BEGIN"), pages)
        self.assertEqual(kinds.count("PAGE_END"), pages)
        self.assertEqual([item["seq"] for item in frames], list(range(len(frames))))

    def test_observation_frame_field_counts_match_the_parser(self):
        rows = fixtures.build_rows(fixtures.case("frames"))
        expected = {
            "OBS_OPEN": 3, "OBS_OPENED": 5, "OBS_PAGE": 5,
            "PAGE_BEGIN": 7, "ROW": 5, "PAGE_END": 4,
        }
        for item in recorder.observation_frames(11, rows):
            fields = item["payload"].split("\t")
            with self.subTest(seq=item["seq"], kind=fields[0]):
                self.assertEqual(len(fields), expected[fields[0]])

    def test_every_row_frame_percent_decodes_back_to_its_row(self):
        """The encoding is round-tripped against the sidecar's own decoder.

        A port that reimplements ``_percent_encode`` has to agree on the
        unreserved set and on uppercase hex; this is the assertion that makes
        the corpus able to catch a disagreement.
        """
        rows = fixtures.build_rows(fixtures.case("frames"))
        seen = 0
        for item in recorder.observation_frames(11, rows):
            if item.get("note") != "row":
                continue
            encoded = item["payload"].split("\t")[4]
            decoded = native._percent_decode(
                encoded, maximum_bytes=native.MAX_OBSERVATION_ROW_BYTES,
            )
            self.assertEqual(decoded, rows[item["row_index"]])
            seen += 1
        self.assertEqual(seen, len(rows))

    def test_page_offsets_and_counts_are_the_arithmetic_the_reader_checks(self):
        rows = tuple(f"row{index:04d} k=v" for index in range(35))
        begins = [
            item["payload"].split("\t")
            for item in recorder.observation_frames(7, rows)
            if item["payload"].startswith("PAGE_BEGIN\t")
        ]
        self.assertEqual(
            [(int(item[4]), int(item[5]), int(item[6])) for item in begins],
            [(0, 16, 35), (16, 16, 35), (32, 3, 35)],
        )
        ends = [
            int(item["payload"].split("\t")[3])
            for item in recorder.observation_frames(7, rows)
            if item["payload"].startswith("PAGE_END\t")
        ]
        self.assertEqual(ends, [16, 32, 35])

    def test_async_frames_carry_the_two_and_ten_field_shapes(self):
        frames = recorder.async_frames(11, 52)
        state = frames[0]["payload"].split("\t")
        self.assertEqual(len(state), 2)
        self.assertEqual(state[0], "STATE_AVAILABLE")
        for item in frames[1:]:
            fields = item["payload"].split("\t")
            with self.subTest(payload=item["payload"]):
                self.assertEqual(len(fields), 10)
                self.assertEqual(fields[0], "PHASE_AVAILABLE")
                phase, mode, count = int(fields[3]), fields[4], int(fields[5])
                self.assertIn(mode, native._PHASE_MODES)
                self.assertLess(phase, count)
                if mode == "concurrent":
                    # The invariant _demultiplex_locked enforces: a concurrent
                    # phase is phase 0 of 1 and is always active.
                    self.assertEqual((phase, count, fields[6]), (0, 1, "1"))

    def test_async_frames_never_claim_ready_without_the_preconditions(self):
        """``ready`` implies active, alive and not-done; a corpus exemplar that
        broke that would be a frame the real reader rejects."""
        for item in recorder.async_frames(11, 52)[1:]:
            active, alive, done, ready = item["payload"].split("\t")[6:10]
            with self.subTest(payload=item["payload"]):
                if ready == "1":
                    self.assertEqual((active, alive, done), ("1", "1", "0"))

    def test_a_turn_below_one_is_lifted_because_the_reader_refuses_zero(self):
        payload = recorder.async_frames(11, 0)[1]["payload"]
        self.assertEqual(payload.split("\t")[2], "1")


# --------------------------------------------------------------------------
# Id map
# --------------------------------------------------------------------------


class IdMapTests(unittest.TestCase):

    def test_walk_strings_visits_keys_in_sorted_order(self):
        value = {"b": "second", "a": "first", "c": ["third", {"d": "fourth"}]}
        self.assertEqual(
            list(recorder.walk_strings(value)),
            ["a", "first", "b", "second", "c", "third", "d", "fourth"],
        )

    def test_slots_follow_first_appearance_and_are_a_bijection(self):
        page = {
            "items": [
                {"id": "city_" + "a" * 32, "tile_id": "tile_" + "b" * 32},
                {"id": "city_" + "c" * 32},
            ],
            "next_cursor": "cursor_" + "D" * 32,
        }
        mapping = recorder.build_idmap([page])
        self.assertEqual(mapping["forward"]["city_" + "a" * 32], "city#0")
        self.assertEqual(mapping["forward"]["city_" + "c" * 32], "city#1")
        self.assertEqual(mapping["forward"]["tile_" + "b" * 32], "tile#0")
        self.assertEqual(mapping["forward"]["cursor_" + "D" * 32], "cursor#0")
        self.assertEqual(mapping["prefixes"], ["city", "cursor", "tile"])
        self.assertEqual(
            {slot: opaque for opaque, slot in mapping["forward"].items()},
            mapping["reverse"],
        )

    def test_non_opaque_strings_are_left_alone(self):
        mapping = recorder.build_idmap([
            {"section": "overview", "state": "running", "name": "City0",
             "hex_but_short": "city_" + "a" * 31},
        ])
        self.assertEqual(mapping["forward"], {})

    def test_the_map_does_not_depend_on_dict_insertion_order(self):
        first = {"z": "unit_" + "1" * 32, "a": "unit_" + "2" * 32}
        second = {"a": "unit_" + "2" * 32, "z": "unit_" + "1" * 32}
        self.assertEqual(
            recorder.build_idmap([first]), recorder.build_idmap([second]),
        )


# --------------------------------------------------------------------------
# Selection and configuration
# --------------------------------------------------------------------------


class SectionResolutionTests(unittest.TestCase):

    def test_the_named_groups_resolve_to_the_measured_sets(self):
        self.assertEqual(
            recorder.resolve_sections("direct"), recorder.DIRECT_SECTIONS,
        )
        self.assertEqual(
            set(recorder.resolve_sections("all")),
            set(recorder.DIRECT_SECTIONS) | set(recorder.CITY_SCOPED_SECTIONS)
            | {"tile_window", "diplomacy_clauses"},
        )
        self.assertEqual(
            recorder.resolve_sections("overview,cities"),
            ("overview", "cities"),
        )

    def test_an_unknown_section_is_refused_by_name(self):
        with self.assertRaises(recorder.CorpusError) as caught:
            recorder.resolve_sections("overview,not_a_section")
        self.assertIn("not_a_section", str(caught.exception))

    def test_every_declared_section_is_a_real_state_section(self):
        declared = (
            set(recorder.DIRECT_SECTIONS) | set(recorder.CITY_SCOPED_SECTIONS)
            | set(recorder.UNREACHABLE_SECTIONS)
            | {"tile_window", "diplomacy_clauses"}
        )
        self.assertEqual(declared, set(v2_control._STATE_SECTIONS))

    def test_the_unreachable_sections_really_are_unreachable(self):
        """Pinned because it is a finding, not a design choice.

        ``state_page`` on these escapes as a bare ``KeyError``, not a typed
        ``V2ControlError``: ``_snapshot``'s catch-all does not cover
        ``_state_section_values``, which ``state_page`` calls outside it.  It is
        latent rather than live -- the supervisor routes these through
        ``prepare_state_scope`` -- but if it ever becomes typed, or ever starts
        working, this test says so instead of the corpus quietly changing shape.
        """
        observation = fixtures.observation(fixtures.case("unreachable"))
        with recorder.deterministic_entropy(SEED):
            control = v2_control.V2SeatControl(
                recorder.FALLBACK_GAME_ID, recorder.CORPUS_AGENT_ID, 1,
            )
            for section in recorder.UNREACHABLE_SECTIONS:
                with self.subTest(section=section):
                    with self.assertRaises(KeyError):
                        control.state_page(observation, section, 16)

    def test_the_direct_sections_all_serve_a_page(self):
        observation = fixtures.observation(fixtures.case("direct"))
        with recorder.deterministic_entropy(SEED):
            control = v2_control.V2SeatControl(
                recorder.FALLBACK_GAME_ID, recorder.CORPUS_AGENT_ID, 1,
            )
            for section in recorder.DIRECT_SECTIONS:
                with self.subTest(section=section):
                    page = control.state_page(observation, section, 16)
                    self.assertEqual(page["page"]["section"], section)
                    self.assertEqual(page["control_protocol"], "full-control-v2")


class SaveSelectionTests(unittest.TestCase):

    def _paths(self, root: Path, turns) -> list[Path]:
        saves = root / "saves"
        saves.mkdir(parents=True, exist_ok=True)
        made = []
        for turn in turns:
            path = saves / f"turn-{turn:04d}-auto.sav.gz"
            path.write_bytes(b"placeholder")
            made.append(path)
        return made

    def test_the_series_is_thinned_evenly_and_the_last_turn_is_kept(self):
        with tempfile.TemporaryDirectory() as scratch:
            paths = self._paths(Path(scratch), range(1, 21))
            chosen, skipped = select = recorder.select_saves(
                paths, per_game=4, live_margin_s=0.0,
            )
            self.assertEqual(skipped, [])
            self.assertEqual(len(chosen), 4)
            self.assertEqual(chosen[0], paths[0])
            self.assertEqual(chosen[-1], paths[-1])
            self.assertEqual(select, recorder.select_saves(
                paths, per_game=4, live_margin_s=0.0,
            ))

    def test_explicit_turns_override_the_sampling(self):
        with tempfile.TemporaryDirectory() as scratch:
            paths = self._paths(Path(scratch), range(1, 21))
            chosen, _skipped = recorder.select_saves(
                paths, per_game=2, turns=(3, 17), live_margin_s=0.0,
            )
            self.assertEqual(
                [recorder.save_turn(path) for path in chosen], [3, 17],
            )

    def test_a_freshly_written_tail_save_is_skipped_and_reported(self):
        """The tail of a live run is the file most likely to be mid-write.

        Excluding it costs one turn of coverage and removes the whole race; the
        skip is recorded so a corpus taken against a live game is *visibly*
        partial rather than quietly short.
        """
        with tempfile.TemporaryDirectory() as scratch:
            paths = self._paths(Path(scratch), range(1, 6))
            chosen, skipped = recorder.select_saves(
                paths, per_game=5, live_margin_s=120.0,
            )
            self.assertNotIn(paths[-1], chosen)
            self.assertEqual(
                skipped, [{"path": paths[-1].name, "reason": "live_margin"}],
            )

    def test_a_named_turn_beats_the_live_margin(self):
        with tempfile.TemporaryDirectory() as scratch:
            paths = self._paths(Path(scratch), range(1, 6))
            chosen, skipped = recorder.select_saves(
                paths, per_game=5, turns=(5,), live_margin_s=120.0,
            )
            self.assertEqual(chosen, [paths[-1]])
            self.assertEqual(skipped, [])


class PinsTests(unittest.TestCase):

    def test_pins_are_read_from_the_modules_rather_than_retyped(self):
        pins = recorder.corpus_pins()
        self.assertEqual(pins["max_frame"], native.MAX_FRAME)
        self.assertEqual(
            pins["max_page_items"], v2_control.MAX_PAGE_ITEMS,
        )
        self.assertEqual(
            pins["native_capabilities"], list(native.NATIVE_CAPABILITIES),
        )
        self.assertEqual(
            pins["native_observation_action_schema_id"],
            v2_control.NATIVE_OBSERVATION_ACTION_SCHEMA_ID,
        )
        self.assertEqual(
            pins["cursor_ttl_seconds"], v2_control.CURSOR_TTL_SECONDS,
        )

    def test_the_row_field_digest_moves_when_the_row_schema_moves(self):
        baseline = recorder.corpus_pins()["row_field_schema_digest"]
        original = v2_control._ROW_FIELDS
        mutated = dict(original)
        mutated["meta"] = tuple(original["meta"]) + ("invented_field",)
        v2_control._ROW_FIELDS = mutated  # type: ignore[assignment]
        try:
            self.assertNotEqual(
                recorder.corpus_pins()["row_field_schema_digest"], baseline,
            )
        finally:
            v2_control._ROW_FIELDS = original  # type: ignore[assignment]


# --------------------------------------------------------------------------
# Sample recording
# --------------------------------------------------------------------------


class RecordSampleTests(unittest.TestCase):

    def setUp(self):
        sample = harvest.incident_sample(harvest.ENTERTAINER_INCIDENT)
        if sample is None:  # pragma: no cover - the fixture is committed
            self.skipTest("the committed incident save is missing")
        self.sample = sample

    def _record(self, **overrides: object) -> recorder.RecordedSample:
        values: dict[str, object] = {
            "game_id": recorder.FALLBACK_GAME_ID,
            "seed": SEED,
            "sections": recorder.DIRECT_SECTIONS,
            "limit": 16,
            "page_limit": 4,
            "follow_cursors": True,
            "want_frames": True,
            "player_name": "AgentPlace1",
        }
        values.update(overrides)
        return recorder.record_sample(self.sample, 0, **values)  # type: ignore[arg-type]

    def test_a_sample_is_byte_identical_across_renders(self):
        self.assertEqual(self._record().files, self._record().files)

    def test_a_different_seed_changes_the_opaque_ids(self):
        baseline = self._record()
        other = self._record(seed=b"\x11" * 32)
        self.assertEqual(set(baseline.files), set(other.files))
        page = "pages/state/t0052-p0.overview.l16.json"
        self.assertNotEqual(baseline.files[page], other.files[page])
        # The bundle is upstream of the seat, so it must be unaffected.
        rows = "bundles/t0052-p0.rows.jsonl"
        self.assertEqual(baseline.files[rows], other.files[rows])

    def test_the_row_digest_matches_the_projector_convention(self):
        sample = self._record()
        bundle = json.loads(
            sample.files["bundles/t0052-p0.bundle.json"].decode("utf-8"),
        )
        self.assertEqual(bundle["row_digest"], sample.row_digest)
        self.assertEqual(bundle["row_count"], len(sample.rows))
        self.assertEqual(bundle["revision_source"], "fixture-constant")
        lines = sample.files["bundles/t0052-p0.rows.jsonl"].decode("utf-8")
        self.assertEqual(
            tuple(json.loads(line) for line in lines.splitlines()), sample.rows,
        )

    def test_cursor_chains_are_followed_to_the_end(self):
        """A corpus that stops at page 1 does not cover the size planner."""
        sample = self._record()
        chains = [
            path for path in sample.files if path.endswith(".chain.jsonl")
        ]
        self.assertTrue(chains, "no multi-page chain was recorded")
        for path in chains:
            with self.subTest(path=path):
                pages = [
                    json.loads(line) for line in
                    sample.files[path].decode("utf-8").splitlines()
                ]
                self.assertGreater(len(pages), 1)
                self.assertIsNone(pages[-1]["page"]["next_cursor"])
                for page in pages[:-1]:
                    self.assertIsNotNone(page["page"]["next_cursor"])

    def test_not_following_cursors_stops_at_page_one(self):
        sample = self._record(follow_cursors=False)
        self.assertFalse(
            [path for path in sample.files if path.endswith(".chain.jsonl")],
        )

    def test_every_recorded_page_carries_the_full_envelope(self):
        sample = self._record()
        for path in sample.page_paths:
            payload = sample.files[path].decode("utf-8")
            pages = (
                [json.loads(line) for line in payload.splitlines()]
                if path.endswith(".jsonl") else [json.loads(payload)]
            )
            for page in pages:
                with self.subTest(path=path):
                    self.assertEqual(page["schema_version"], 2)
                    self.assertEqual(page["control_protocol"], "full-control-v2")
                    self.assertEqual(page["game_id"], recorder.FALLBACK_GAME_ID)
                    self.assertEqual(page["agent_id"], recorder.CORPUS_AGENT_ID)
                    revision = page["state_revision"]
                    self.assertEqual(
                        sorted(revision), ["revision", "state_token", "turn"],
                    )
                    self.assertTrue(revision["state_token"].startswith("state_"))
                    self.assertIn("items", page["page"])
                    self.assertIn("total_items", page["page"])

    def test_the_frozen_clock_makes_cursor_expiry_a_constant(self):
        sample = self._record()
        expiries = set()
        for path in sample.page_paths:
            payload = sample.files[path].decode("utf-8")
            pages = (
                [json.loads(line) for line in payload.splitlines()]
                if path.endswith(".jsonl") else [json.loads(payload)]
            )
            for page in pages:
                value = page["page"].get("cursor_expires_at")
                if value is not None:
                    expiries.add(value)
        self.assertTrue(expiries, "no page carried a cursor expiry")
        self.assertEqual(len(expiries), 1)

    def test_frames_can_be_switched_off(self):
        self.assertFalse(self._record(want_frames=False).frame_paths)
        self.assertEqual(len(self._record().frame_paths), 3)

    def test_the_idmap_covers_every_opaque_id_in_every_page(self):
        sample = self._record()
        mapping = json.loads(
            sample.files["ids/t0052-p0.idmap.json"].decode("utf-8"),
        )
        for path in sample.page_paths:
            payload = sample.files[path].decode("utf-8")
            pages = (
                [json.loads(line) for line in payload.splitlines()]
                if path.endswith(".jsonl") else [json.loads(payload)]
            )
            for page in pages:
                for text in recorder.walk_strings(page):
                    if (
                        recorder.OPAQUE_ID_RE.fullmatch(text) is not None
                        or recorder.CURSOR_ID_RE.fullmatch(text) is not None
                    ):
                        with self.subTest(opaque=text):
                            self.assertIn(text, mapping["forward"])

    def test_asking_for_an_unreachable_section_records_a_gap_not_a_page(self):
        sample = self._record(
            sections=recorder.DIRECT_SECTIONS + ("map_tiles",),
        )
        kinds = {note["kind"] for note in sample.notes}
        self.assertIn("unreachable_section", kinds)
        self.assertFalse(
            [path for path in sample.page_paths if "map_tiles" in path],
        )

    def test_emitting_the_same_path_twice_is_refused(self):
        sample = recorder.RecordedSample(
            key="t0001-p0", turn=1, player_index=0, save_name="x",
            rows=(), row_digest="",
        )
        recorder._emit(sample, "pages/state/a.json", b"{}", kind="page")
        with self.assertRaises(recorder.CorpusError):
            recorder._emit(sample, "pages/state/a.json", b"{}", kind="page")
        with self.assertRaises(recorder.CorpusError):
            recorder._emit(sample, "../escape.json", b"{}", kind="page")

    def test_a_traversal_segment_is_refused_wherever_it_appears(self):
        """The guard has to guard *mid-path*, not just at the front.

        The old whole-string pattern only required the first character to be
        alphanumeric, so ``bundles/../../../etc/x`` matched it.  Nothing built
        such a path -- relpaths come from digit-only turn keys and a fixed
        section allowlist -- but a guard that does not guard is worse than no
        guard, because the next contributor trusts it.
        """
        for relpath in (
            "bundles/../../../etc/x", "pages/state/../../escape.json",
            "pages//double.json", "/absolute.json", "pages/./here.json",
            "..", "", "pages/" + "x" * 300,
        ):
            with self.subTest(relpath=relpath):
                self.assertFalse(recorder.is_safe_relpath(relpath))
        for relpath in (
            "index.json", "bundles/t0001-p0.rows.jsonl",
            "pages/state/t0001-p0.overview.l16.json",
        ):
            with self.subTest(relpath=relpath):
                self.assertTrue(recorder.is_safe_relpath(relpath))
        sample = recorder.RecordedSample(
            key="t0001-p0", turn=1, player_index=0, save_name="x",
            rows=(), row_digest="",
        )
        with self.assertRaises(recorder.CorpusError) as caught:
            recorder._emit(sample, "bundles/../../../etc/x", b"{}", kind="page")
        self.assertIn("unsafe corpus path", str(caught.exception))

    def test_only_the_recorders_own_path_shapes_are_emittable(self):
        """The pruner's authority and the emitter's must be the same set.

        ``write_corpus`` deletes only :func:`is_corpus_relpath` shapes, so an
        emitter that invented a fourth directory would leave files no recording
        ever reclaims.
        """
        for relpath in ("index.json", "MANIFEST.sha256", "gaps.json",
                        "provenance.json", "pages/state/x.json",
                        "bundles/x.jsonl", "frames/x.jsonl", "ids/x.json"):
            with self.subTest(relpath=relpath):
                self.assertTrue(recorder.is_corpus_relpath(relpath))
        for relpath in ("saves/turn-0001-auto.sav.gz", "manifest.json",
                        "README.md", "scratch/thing.bin", "../escape.json"):
            with self.subTest(relpath=relpath):
                self.assertFalse(recorder.is_corpus_relpath(relpath))


# --------------------------------------------------------------------------
# End to end, against a copy of a committed run directory
# --------------------------------------------------------------------------


class EndToEndTests(unittest.TestCase):
    """The whole pipeline, on a throwaway copy of a real (committed) save."""

    def setUp(self):
        self._scratch = tempfile.TemporaryDirectory(prefix="corpus-e2e-")
        self.root = Path(self._scratch.name)
        self.run_dir = _tmp_run_dir(
            self.root / "runs", harvest.ENTERTAINER_INCIDENT[0],
        )
        self.out = self.root / "corpus"
        self.stream = io.StringIO()

    def tearDown(self):
        self._scratch.cleanup()

    def _record(self, **overrides: object) -> int:
        return recorder.command_record(
            _record_args(self.run_dir, self.out, **overrides), self.stream,
        )

    def test_a_recording_writes_an_index_a_manifest_and_the_artifacts(self):
        self.assertEqual(self._record(), 0)
        target = self.out / harvest.ENTERTAINER_INCIDENT[0]
        index = json.loads((target / "index.json").read_bytes().decode("utf-8"))
        self.assertEqual(index["format"], recorder.CORPUS_FORMAT)
        self.assertEqual(index["generated_by"]["recorder_mode"], "offline-synthesised")
        self.assertEqual(len(index["samples"]), 1)
        self.assertEqual(index["samples"][0]["key"], "t0052-p0")
        self.assertEqual(index["source"]["manifest"]["ruleset"], "classic")
        for relpath, entry in index["files"].items():
            with self.subTest(relpath=relpath):
                payload = (target / relpath).read_bytes()
                self.assertEqual(recorder.sha256_hex(payload), entry["sha256"])
                self.assertEqual(len(payload), entry["bytes"])
        # MANIFEST.sha256 is sha256sum-checkable and lists every emitted file.
        # It cannot list itself, and it cannot list index.json (which quotes
        # its digests), so index.json covers the manifest instead -- otherwise
        # the manifest is the one file in the tree that nothing hashes, and
        # `verify` says "N files verified" over a forged one.
        manifest = (target / "MANIFEST.sha256").read_text(encoding="utf-8")
        listed = recorder.parse_manifest(manifest.encode("utf-8"))
        self.assertEqual(
            sorted(listed), sorted(set(index["files"]) - {"MANIFEST.sha256"}),
        )
        for relpath, digest in listed.items():
            with self.subTest(relpath=relpath):
                self.assertEqual(digest, index["files"][relpath]["sha256"])
        self.assertNotIn("index.json", listed)
        self.assertNotIn("MANIFEST.sha256", listed)
        self.assertEqual(
            index["files"]["MANIFEST.sha256"]["sha256"],
            recorder.sha256_hex(manifest.encode("utf-8")),
        )
        self.assertEqual(index["schema_version"], recorder.CORPUS_SCHEMA_VERSION)

    def test_the_corpus_digest_is_the_hash_of_the_file_hashes(self):
        self.assertEqual(self._record(), 0)
        target = self.out / harvest.ENTERTAINER_INCIDENT[0]
        index = json.loads((target / "index.json").read_bytes().decode("utf-8"))
        self.assertEqual(
            index["corpus_digest"],
            recorder.sha256_hex(recorder.canonical_json({
                path: entry["sha256"]
                for path, entry in sorted(index["files"].items())
            })),
        )

    def test_re_recording_is_byte_identical(self):
        """The claim the whole corpus rests on, tested on every file."""
        self.assertEqual(self._record(), 0)
        target = self.out / harvest.ENTERTAINER_INCIDENT[0]
        first = recorder.read_corpus_files(target)
        self.assertEqual(self._record(), 0)
        second = recorder.read_corpus_files(target)
        self.assertEqual(sorted(first), sorted(second))
        self.assertEqual(first, second)
        self.assertGreater(len(first), 30)

    def test_the_stamp_is_the_only_thing_that_varies_and_is_opt_in(self):
        self.assertEqual(self._record(), 0)
        target = self.out / harvest.ENTERTAINER_INCIDENT[0]
        self.assertFalse((target / "provenance.json").exists())
        self.assertEqual(self._record(stamp=True), 0)
        self.assertTrue((target / "provenance.json").exists())
        stamp = json.loads((target / "provenance.json").read_bytes())
        self.assertIn("recorded_at", stamp)
        index = json.loads((target / "index.json").read_bytes())
        self.assertNotIn("provenance.json", index["files"])

    def test_a_stamp_does_not_make_the_next_recording_need_force(self):
        """The stamp carries the clock, so it differs on every run.

        If it counted toward the collision check, ``--stamp`` would silently
        turn into a permanent ``--force`` requirement.
        """
        self.assertEqual(self._record(stamp=True), 0)
        self.assertEqual(self._record(stamp=True), 0)
        self.assertEqual(self._record(), 0)
        target = self.out / harvest.ENTERTAINER_INCIDENT[0]
        self.assertFalse(
            (target / "provenance.json").exists(),
            "an unstamped recording must not leave a stale stamp behind",
        )

    def test_recording_touches_nothing_in_the_run_directory(self):
        """The rule that matters most: a live game must not notice this tool.

        Not a cache, not a lock, not a ``.tmp``, and no mtime moved -- which is
        why the recorder uses ``save_replay``'s low-level readers rather than
        its public entry points, which write a derived cache.
        """
        before = _tree(self.run_dir)
        self.assertEqual(self._record(), 0)
        self.assertEqual(_tree(self.run_dir), before)

    def test_the_output_never_lands_inside_the_run_directory(self):
        self.assertEqual(self._record(), 0)
        self.assertNotIn(
            self.out.resolve(), self.run_dir.resolve().parents,
        )
        self.assertFalse(
            str(self.out.resolve()).startswith(str(self.run_dir.resolve())),
        )

    def test_verify_passes_and_catches_a_tampered_byte(self):
        self.assertEqual(self._record(), 0)
        target = self.out / harvest.ENTERTAINER_INCIDENT[0]
        args = argparse.Namespace(corpus=str(target), strict=False)
        self.assertEqual(recorder.command_verify(args, self.stream), 0)
        victim = target / "bundles" / "t0052-p0.bundle.json"
        victim.write_bytes(victim.read_bytes() + b" ")
        self.assertEqual(recorder.command_verify(args, self.stream), 1)
        self.assertIn("digest mismatch", self.stream.getvalue())

    def test_verify_strict_re_derives_and_compares_against_disk(self):
        """Strict must compare the rebuild to what is *on disk*.

        Comparing a rebuild to its own in-memory bytes proves nothing, and is
        precisely the vacuous shape this gate exists to prevent -- so the test
        corrupts a recorded file and requires strict to notice.
        """
        self.assertEqual(self._record(), 0)
        target = self.out / harvest.ENTERTAINER_INCIDENT[0]
        args = argparse.Namespace(corpus=str(target), strict=True)
        self.assertEqual(recorder.command_verify(args, self.stream), 0)
        self.assertIn("re-derived byte-identically", self.stream.getvalue())

        page = next(
            path for path in sorted(target.rglob("*.json"))
            if path.name.endswith(".overview.l16.json")
        )
        document = json.loads(page.read_bytes())
        document["page"]["items"] = []
        page.write_bytes(recorder.canonical_json(document))
        # Re-point index.json *and* MANIFEST.sha256 at the tampered bytes so
        # the plain digest checks all pass and only the strict re-derivation
        # can catch it.  (Re-pointing index.json alone no longer suffices: the
        # manifest cross-check notices the two views disagreeing, which is its
        # own test below.)
        index_path = target / "index.json"
        index = json.loads(index_path.read_bytes())
        relpath = page.relative_to(target).as_posix()
        payload = page.read_bytes()
        index["files"][relpath] = {
            "sha256": recorder.sha256_hex(payload), "bytes": len(payload),
        }
        manifest_path = target / "MANIFEST.sha256"
        listing = recorder.parse_manifest(manifest_path.read_bytes())
        listing[relpath] = recorder.sha256_hex(payload)
        manifest_bytes = b"".join(
            f"{listing[name]}  {name}\n".encode("utf-8")
            for name in sorted(listing)
        )
        manifest_path.write_bytes(manifest_bytes)
        index["files"]["MANIFEST.sha256"] = {
            "sha256": recorder.sha256_hex(manifest_bytes),
            "bytes": len(manifest_bytes),
        }
        index["corpus_digest"] = recorder.sha256_hex(recorder.canonical_json({
            path: entry["sha256"]
            for path, entry in sorted(index["files"].items())
        }))
        index_path.write_bytes(recorder.pretty_json(index))
        self.stream = io.StringIO()
        self.assertEqual(recorder.command_verify(
            argparse.Namespace(corpus=str(target), strict=False), self.stream,
        ), 0)
        self.assertEqual(recorder.command_verify(args, self.stream), 1)
        self.assertIn("differs from its re-derivation", self.stream.getvalue())

    def test_verify_catches_a_tampered_manifest(self):
        """The manifest used to be outside every digest.

        ``index['files']`` was built before ``rendered()`` added
        MANIFEST.sha256, and ``verify`` only walks ``index['files']`` -- so a
        rewritten manifest verified clean and printed "N files verified".
        """
        self.assertEqual(self._record(), 0)
        target = self.out / harvest.ENTERTAINER_INCIDENT[0]
        args = argparse.Namespace(corpus=str(target), strict=False)
        self.assertEqual(recorder.command_verify(args, self.stream), 0)
        manifest = target / "MANIFEST.sha256"
        listing = recorder.parse_manifest(manifest.read_bytes())
        victim = sorted(listing)[0]
        listing[victim] = "0" * 64
        manifest.write_bytes(b"".join(
            f"{listing[name]}  {name}\n".encode("utf-8")
            for name in sorted(listing)
        ))
        self.stream = io.StringIO()
        self.assertEqual(recorder.command_verify(args, self.stream), 1)
        self.assertIn("digest mismatch MANIFEST.sha256", self.stream.getvalue())

    def test_verify_catches_a_manifest_that_disagrees_with_the_index(self):
        """Two views of the same corpus; neither may be rewritten alone."""
        self.assertEqual(self._record(), 0)
        target = self.out / harvest.ENTERTAINER_INCIDENT[0]
        index_path = target / "index.json"
        index = json.loads(index_path.read_bytes())
        victim = "gaps.json"
        index["files"][victim] = dict(index["files"][victim], sha256="1" * 64)
        index["corpus_digest"] = recorder.sha256_hex(recorder.canonical_json({
            path: entry["sha256"]
            for path, entry in sorted(index["files"].items())
        }))
        index_path.write_bytes(recorder.pretty_json(index))
        code = recorder.command_verify(
            argparse.Namespace(corpus=str(target), strict=False), self.stream,
        )
        self.assertEqual(code, 1)
        self.assertIn("MANIFEST.sha256 disagrees with index.json", self.stream.getvalue())

    def test_verify_catches_a_file_nobody_indexed(self):
        """An injected file is invisible to a per-entry walk of the index.

        index.json is self-consistent about its own contents by construction,
        so "every file index.json names hashes to what index.json says" is not
        a statement about the tree -- only about index.json.
        """
        self.assertEqual(self._record(), 0)
        target = self.out / harvest.ENTERTAINER_INCIDENT[0]
        evil = target / "pages" / "state" / "EVIL.json"
        evil.write_bytes(b'{"owned":true}')
        code = recorder.command_verify(
            argparse.Namespace(corpus=str(target), strict=False), self.stream,
        )
        self.assertEqual(code, 1)
        self.assertIn("unexpected pages/state/EVIL.json", self.stream.getvalue())

    def test_an_injected_file_is_caught_even_when_strict_has_to_skip(self):
        """The machine the corpus exists to serve is the one without the run.

        ``.agent-eval/runs`` is gitignored and gets pruned, so a corpus shipped
        to a port's CI always takes strict's SKIP path.  Before, that path
        printed "the recorded digests still verified" and returned 0 with the
        injected file still sitting in the tree.
        """
        self.assertEqual(self._record(), 0)
        target = self.out / harvest.ENTERTAINER_INCIDENT[0]
        (target / "pages" / "state" / "EVIL.json").write_bytes(b"{}")
        shutil.rmtree(self.run_dir)
        code = recorder.command_verify(
            argparse.Namespace(corpus=str(target), strict=True), self.stream,
        )
        self.assertEqual(code, 1)
        self.assertIn("unexpected pages/state/EVIL.json", self.stream.getvalue())
        self.assertNotIn("SKIP strict", self.stream.getvalue())

    def test_the_strict_skip_says_what_it_did_and_did_not_check(self):
        """SKIP is the ordinary case wherever the corpus is consumed, so its
        wording is the reader's whole picture of how much was proved."""
        self.assertEqual(self._record(), 0)
        target = self.out / harvest.ENTERTAINER_INCIDENT[0]
        shutil.rmtree(self.run_dir)
        code = recorder.command_verify(
            argparse.Namespace(corpus=str(target), strict=True), self.stream,
        )
        self.assertEqual(code, 0)
        output = self.stream.getvalue()
        self.assertIn("SKIP strict", output)
        self.assertIn("Already checked", output)
        self.assertIn("Not checked", output)
        self.assertIn("MANIFEST.sha256", output)

    def test_a_corpus_from_another_schema_version_is_refused_not_half_checked(self):
        """Verifying a schema-1 corpus with schema-2 code would report checks
        it cannot perform: at schema 1 the manifest was in no digest."""
        self.assertEqual(self._record(), 0)
        target = self.out / harvest.ENTERTAINER_INCIDENT[0]
        index_path = target / "index.json"
        index = json.loads(index_path.read_bytes())
        index["schema_version"] = 1
        index_path.write_bytes(recorder.pretty_json(index))
        code = recorder.command_verify(
            argparse.Namespace(corpus=str(target), strict=False), self.stream,
        )
        self.assertEqual(code, 1)
        self.assertIn("re-record the corpus", self.stream.getvalue())

    def test_the_stamp_stays_outside_the_verified_file_set(self):
        self.assertEqual(self._record(stamp=True), 0)
        target = self.out / harvest.ENTERTAINER_INCIDENT[0]
        self.assertTrue((target / "provenance.json").is_file())
        self.assertEqual(recorder.command_verify(
            argparse.Namespace(corpus=str(target), strict=False), self.stream,
        ), 0)

    def test_strict_names_source_drift_instead_of_crying_mismatch(self):
        """A live run keeps writing saves; strict must not call that a bug.

        Reporting the input moving as a byte mismatch would train a reader to
        ignore the one signal this gate exists to give.  Observed for real:
        an unfinished run gained two turns between record and verify.
        """
        self.assertEqual(self._record(), 0)
        target = self.out / harvest.ENTERTAINER_INCIDENT[0]
        args = argparse.Namespace(corpus=str(target), strict=True)
        self.assertEqual(recorder.command_verify(args, self.stream), 0)

        index_path = target / "index.json"
        index = json.loads(index_path.read_bytes())
        index["source"]["saves"][0]["sha256"] = "0" * 64
        index_path.write_bytes(recorder.pretty_json(index))
        self.stream = io.StringIO()
        self.assertEqual(recorder.command_verify(args, self.stream), 0)
        output = self.stream.getvalue()
        self.assertIn("SKIP strict", output)
        self.assertIn("advanced since this corpus was recorded", output)
        self.assertNotIn("FAIL strict", output)

    def test_a_new_save_appearing_counts_as_source_drift(self):
        """The live shape exactly: the run gains a turn after recording."""
        self.assertEqual(self._record(), 0)
        target = self.out / harvest.ENTERTAINER_INCIDENT[0]
        existing = self.run_dir / "saves" / "turn-0052-auto.sav.gz"
        (self.run_dir / "saves" / "turn-0053-auto.sav.gz").write_bytes(
            existing.read_bytes(),
        )
        self.stream = io.StringIO()
        code = recorder.command_verify(
            argparse.Namespace(corpus=str(target), strict=True), self.stream,
        )
        self.assertEqual(code, 0)
        self.assertIn("SKIP strict", self.stream.getvalue())
        self.assertNotIn("FAIL strict", self.stream.getvalue())

    def test_a_differing_existing_corpus_is_refused_without_force(self):
        self.assertEqual(self._record(), 0)
        target = self.out / harvest.ENTERTAINER_INCIDENT[0]
        (target / "gaps.json").write_bytes(b'{"tampered":true}')
        with self.assertRaises(recorder.CorpusError) as caught:
            self._record()
        self.assertEqual(caught.exception.exit_code, 3)
        self.assertEqual(self._record(force=True), 0)

    def test_a_stale_file_from_an_older_recording_is_removed(self):
        self.assertEqual(self._record(), 0)
        target = self.out / harvest.ENTERTAINER_INCIDENT[0]
        stale = target / "pages" / "state" / "t9999-p9.overview.l16.json"
        stale.write_bytes(b"{}")
        self.assertEqual(self._record(force=True), 0)
        self.assertFalse(stale.exists())

    def test_a_dry_run_writes_nothing(self):
        self.assertEqual(self._record(dry_run=True), 0)
        self.assertFalse(self.out.exists())

    def test_two_run_directories_produce_independent_corpora(self):
        second = _tmp_run_dir(
            self.root / "runs", harvest.SIDECAR_EXIT_INCIDENT[0],
        )
        args = _record_args(self.run_dir, self.out)
        args.run_dir = [str(self.run_dir), str(second)]
        self.assertEqual(recorder.command_record(args, self.stream), 0)
        first_index = json.loads(
            (self.out / harvest.ENTERTAINER_INCIDENT[0] / "index.json").read_bytes(),
        )
        second_index = json.loads(
            (self.out / harvest.SIDECAR_EXIT_INCIDENT[0] / "index.json").read_bytes(),
        )
        self.assertNotEqual(
            first_index["corpus_digest"], second_index["corpus_digest"],
        )
        self.assertEqual(second_index["samples"][0]["key"], "t0066-p0")

    def test_a_run_without_a_manifest_still_records(self):
        """A supervisor killed mid-bootstrap leaves no manifest.

        That is the interrupted shape most worth recording, so a missing
        manifest must degrade to a provenance note rather than a refusal.
        """
        (self.run_dir / "manifest.json").unlink()
        self.assertEqual(self._record(), 0)
        index = json.loads((
            self.out / harvest.ENTERTAINER_INCIDENT[0] / "index.json"
        ).read_bytes())
        self.assertEqual(index["source"]["manifest"], {"present": False})
        self.assertEqual(index["source"]["player_name"], "AgentPlace1")

    def test_an_unparseable_manifest_is_reported_rather_than_fatal(self):
        (self.run_dir / "manifest.json").write_bytes(b"{not json")
        self.assertEqual(self._record(), 0)
        index = json.loads((
            self.out / harvest.ENTERTAINER_INCIDENT[0] / "index.json"
        ).read_bytes())
        self.assertEqual(
            index["source"]["manifest"], {"present": True, "readable": False},
        )

    def test_gaps_are_carried_verbatim_from_the_pinned_inventory(self):
        """A corpus that quietly dropped a gap would be selling synthesis as
        capture, which is the one thing this tool must never do."""
        self.assertEqual(self._record(), 0)
        gaps = json.loads((
            self.out / harvest.ENTERTAINER_INCIDENT[0] / "gaps.json"
        ).read_bytes())
        self.assertEqual(gaps["corpus_gaps"], dict(harvest.CORPUS_GAPS))
        self.assertEqual(gaps["recorder_mode"], "offline-synthesised")
        self.assertIn("synthesised-from-bundle", gaps["frames"])
        self.assertEqual(
            gaps["unreachable_sections"], list(recorder.UNREACHABLE_SECTIONS),
        )

    def test_gaps_carry_the_frame_direction_legend(self):
        """``dir`` is two letters and appears nowhere else in the tree.

        A port author consuming only the corpus had to guess or go read the
        recorder's source, and getting it backwards inverts every replay.  The
        mapping is the one in ``headless_sidecar._bootstrap``: the C client
        writes ``c2h``, the Python host writes ``h2c``.
        """
        self.assertEqual(self._record(), 0)
        target = self.out / harvest.ENTERTAINER_INCIDENT[0]
        gaps = json.loads((target / "gaps.json").read_bytes())
        legend = gaps["frame_directions"]
        self.assertIn("client-to-host", legend["c2h"])
        self.assertIn("host-to-client", legend["h2c"])
        directions = {
            json.loads(line)["dir"]
            for path in sorted((target / "frames").iterdir())
            for line in path.read_text(encoding="utf-8").splitlines()
        }
        self.assertEqual(directions, {"c2h", "h2c"})
        for code in directions:
            with self.subTest(code=code):
                self.assertIn(code, legend)

    def test_gaps_name_every_capability_the_corpus_never_frames(self):
        """The CAPS frame advertises verbs no fixture demonstrates.

        ACT/ACT_CAP/ACT_RELATION_CAP and the SCOPE families appear only inside
        the CAPS payload string and index.json's pins.  The scope path is
        precisely what ``UNREACHABLE_SECTIONS`` says the supervisor uses for
        map_tiles/pregame_*/unit_route, so the port is told those sections are
        unreachable and given no framing oracle for the mechanism that reaches
        them.  A gap that is not declared is the failure mode this recorder
        exists to prevent.
        """
        self.assertEqual(self._record(), 0)
        target = self.out / harvest.ENTERTAINER_INCIDENT[0]
        gaps = json.loads((target / "gaps.json").read_bytes())
        verbs = gaps["frame_verbs"]
        framed = {
            json.loads(line)["payload"].split("\t")[0]
            for path in sorted((target / "frames").iterdir())
            for line in path.read_text(encoding="utf-8").splitlines()
        }
        self.assertEqual(sorted(framed), verbs["recorded"])
        self.assertEqual(
            verbs["advertised_but_never_framed"],
            [
                capability for capability in native.NATIVE_CAPABILITIES
                if capability not in framed
            ],
        )
        for capability in ("ACT", "ACT_CAP", "ACT_RELATION_CAP",
                           "SCOPE_OPEN", "SCOPE_PAGE"):
            with self.subTest(capability=capability):
                self.assertIn(capability, verbs["advertised_but_never_framed"])
        for capability in ("OBS_OPEN", "OBS_PAGE", "STATE_AVAILABLE",
                           "PHASE_AVAILABLE"):
            with self.subTest(capability=capability):
                self.assertIn(capability, verbs["recorded"])
        self.assertIn("prepare_state_scope", verbs["detail"])

    def test_gaps_admit_the_traces_are_exemplars_not_a_session(self):
        """Three files, each restarting ``seq`` at 0, with no global order.

        ``async_frames``'s own docstring says the notifications may interleave
        anywhere after CAPS and that the interleaving is what
        ``_demultiplex_locked``/``_wait_message`` exist to get right -- and no
        fixture shows one.  Defensible as designed; not defensible undeclared.
        """
        self.assertEqual(self._record(), 0)
        target = self.out / harvest.ENTERTAINER_INCIDENT[0]
        for path in sorted((target / "frames").iterdir()):
            with self.subTest(path=path.name):
                first = json.loads(path.read_text(encoding="utf-8").splitlines()[0])
                self.assertEqual(first["seq"], 0)
        note = json.loads((target / "gaps.json").read_bytes())["frame_traces"]
        self.assertIn("not one replayable session", note)
        self.assertIn("interleav", note)
        self.assertIn("_demultiplex_locked", note)

    def test_the_verbs_gap_is_read_off_the_frames_rather_than_retyped(self):
        """Switch the frames off and every capability is unframed."""
        self.assertEqual(self._record(frames=False), 0)
        gaps = json.loads((
            self.out / harvest.ENTERTAINER_INCIDENT[0] / "gaps.json"
        ).read_bytes())
        self.assertEqual(gaps["frame_verbs"]["recorded"], [])
        self.assertEqual(
            gaps["frame_verbs"]["advertised_but_never_framed"],
            list(native.NATIVE_CAPABILITIES),
        )

    def test_a_bad_limit_is_refused_before_anything_is_written(self):
        with self.assertRaises(recorder.CorpusError):
            self._record(limit=99)
        with self.assertRaises(recorder.CorpusError):
            self._record(page_limit=99)
        self.assertFalse(self.out.exists())

    def test_a_bad_entropy_seed_is_refused(self):
        with self.assertRaises(recorder.CorpusError):
            self._record(entropy_seed="not-hex")
        with self.assertRaises(recorder.CorpusError):
            self._record(entropy_seed="")

    def test_strict_replay_args_reproduce_the_recorded_settings(self):
        self.assertEqual(self._record(), 0)
        target = self.out / harvest.ENTERTAINER_INCIDENT[0]
        index = json.loads((target / "index.json").read_bytes())
        replay = recorder.strict_replay_args(index, self.run_dir)
        self.assertEqual(replay.limit, 16)
        self.assertEqual(replay.page_limit, 4)
        self.assertEqual(replay.player, [0])
        self.assertEqual(replay.live_margin, 0.0)
        self.assertEqual(
            replay.sections.split(","),
            index["generated_by"]["settings"]["sections"],
        )


# --------------------------------------------------------------------------
# The one failure that cannot be undone
# --------------------------------------------------------------------------


def _synthetic_run_dir(root: Path, game_id: str = "game_synthetic") -> Path:
    """A run directory's *shape*, built from scratch in a temp dir.

    Never a real one.  These tests are about the case where writing would have
    deleted a game's savegames, and the only responsible way to test that is
    against a directory nothing depends on.
    """
    run_dir = root / game_id
    saves = run_dir / "saves"
    saves.mkdir(parents=True)
    for turn in range(1, 9):
        (saves / f"turn-{turn:04d}-auto.sav.gz").write_bytes(b"not a save")
    (run_dir / "manifest.json").write_bytes(b'{"game_id":"synthetic"}')
    return run_dir


class DestructiveWriteGuardTests(unittest.TestCase):
    """``--out`` aimed at a runs root used to delete the run.

    ``write_corpus`` prunes every file the current rendering did not produce,
    so ``record --out .agent-eval/runs --force`` unlinked a run directory's
    entire savegame series and its manifest, exit 0, no warning.  The saves are
    the only durable substrate the corpus is built from, and on this machine
    they may belong to a game that is running right now.  The module docstring
    already promised output never lands inside a run directory; these tests are
    that promise made enforceable.
    """

    def setUp(self):
        self._scratch = tempfile.TemporaryDirectory(prefix="corpus-guard-")
        self.root = Path(self._scratch.name)
        self.runs = self.root / "runs"
        self.run_dir = _tmp_run_dir(self.runs, harvest.ENTERTAINER_INCIDENT[0])
        self.stream = io.StringIO()

    def tearDown(self):
        self._scratch.cleanup()

    def test_recording_on_top_of_a_run_directory_is_refused_and_deletes_nothing(self):
        """The exact reproduction: --out is the runs root, so the corpus
        directory *is* the run directory."""
        before = _tree(self.run_dir)
        args = _record_args(self.run_dir, self.runs, force=True)
        with self.assertRaises(recorder.CorpusError) as caught:
            recorder.command_record(args, self.stream)
        self.assertEqual(
            caught.exception.exit_code, recorder.UNSAFE_TARGET_EXIT_CODE,
        )
        message = str(caught.exception)
        self.assertIn("run directory", message)
        self.assertIn("saves/", message)
        self.assertEqual(_tree(self.run_dir), before)
        self.assertTrue((self.run_dir / "manifest.json").is_file())
        self.assertTrue(list((self.run_dir / "saves").iterdir()))

    def test_a_dry_run_delivers_the_same_refusal_as_the_real_one(self):
        """The dry run is what a careful operator does first."""
        args = _record_args(self.run_dir, self.runs, dry_run=True)
        with self.assertRaises(recorder.CorpusError) as caught:
            recorder.command_record(args, self.stream)
        self.assertEqual(
            caught.exception.exit_code, recorder.UNSAFE_TARGET_EXIT_CODE,
        )

    def test_a_runs_root_is_refused_even_when_the_game_id_is_new(self):
        """No collision with an existing run is needed for this to be wrong.

        The target directory would be created fresh next to live run
        directories, and the next recording with a different ``--game`` would
        prune it -- so the refusal is about ``--out``, not about the target.
        """
        elsewhere = _tmp_run_dir(
            self.root / "elsewhere", harvest.SIDECAR_EXIT_INCIDENT[0],
        )
        _synthetic_run_dir(self.root / "other-runs")
        args = _record_args(elsewhere, self.root / "other-runs", force=True)
        with self.assertRaises(recorder.CorpusError) as caught:
            recorder.command_record(args, self.stream)
        self.assertEqual(
            caught.exception.exit_code, recorder.UNSAFE_TARGET_EXIT_CODE,
        )
        self.assertIn("runs root", str(caught.exception))

    def test_the_default_runs_root_is_refused_without_inspecting_it(self):
        """Named explicitly, because it is the path a typo actually reaches."""
        corpus = recorder.GameCorpus(game_id="game_x", files={}, index={})
        with self.assertRaises(recorder.CorpusError) as caught:
            recorder.write_corpus(
                corpus, recorder.DEFAULT_RUNS_ROOT / "nested", force=True,
            )
        self.assertEqual(
            caught.exception.exit_code, recorder.UNSAFE_TARGET_EXIT_CODE,
        )
        self.assertIn("runs root", str(caught.exception))

    def test_a_target_inside_a_run_directory_is_refused(self):
        run_dir = _synthetic_run_dir(self.root / "solo")
        corpus = recorder.GameCorpus(game_id="game_x", files={}, index={})
        with self.assertRaises(recorder.CorpusError) as caught:
            recorder.write_corpus(corpus, run_dir / "corpus", force=True)
        self.assertEqual(
            caught.exception.exit_code, recorder.UNSAFE_TARGET_EXIT_CODE,
        )
        self.assertIn("ancestor", str(caught.exception))

    def test_run_dir_markers_are_reported_and_a_corpus_dir_carries_none(self):
        run_dir = _synthetic_run_dir(self.root / "markers")
        self.assertEqual(
            recorder.run_dir_markers(run_dir), ["saves/", "manifest.json"],
        )
        (run_dir / "manifest.json").unlink()
        self.assertEqual(recorder.run_dir_markers(run_dir), ["saves/"])
        self.assertTrue(recorder.looks_like_runs_root(self.root / "markers"))

        out = self.root / "corpus"
        self.assertEqual(recorder.command_record(
            _record_args(self.run_dir, out), self.stream,
        ), 0)
        target = out / harvest.ENTERTAINER_INCIDENT[0]
        self.assertEqual(recorder.run_dir_markers(target), [])
        self.assertFalse(recorder.looks_like_runs_root(out))

    def test_the_pruner_reclaims_only_what_the_recorder_could_have_written(self):
        """Second guard, independent of the first.

        A file the recorder cannot emit is a file it does not delete.  It does
        not get to hide either: it costs a ``--force``, and ``verify`` names it.
        """
        out = self.root / "corpus"
        self.assertEqual(recorder.command_record(
            _record_args(self.run_dir, out), self.stream,
        ), 0)
        target = out / harvest.ENTERTAINER_INCIDENT[0]
        foreign = target / "README.md"
        foreign.write_bytes(b"someone else's file\n")
        buried = target / "scratch" / "keepme.bin"
        buried.parent.mkdir()
        buried.write_bytes(b"\x00\x01")
        stale = target / "pages" / "state" / "t9999-p9.overview.l16.json"
        stale.write_bytes(b"{}")

        with self.assertRaises(recorder.CorpusError) as caught:
            recorder.command_record(_record_args(self.run_dir, out), self.stream)
        self.assertEqual(caught.exception.exit_code, 3)

        self.assertEqual(recorder.command_record(
            _record_args(self.run_dir, out, force=True), self.stream,
        ), 0)
        self.assertFalse(stale.exists())
        self.assertEqual(foreign.read_bytes(), b"someone else's file\n")
        self.assertEqual(buried.read_bytes(), b"\x00\x01")

        stream = io.StringIO()
        code = recorder.command_verify(
            argparse.Namespace(corpus=str(target), strict=False), stream,
        )
        self.assertEqual(code, 1)
        self.assertIn("unexpected README.md", stream.getvalue())
        self.assertIn("unexpected scratch/keepme.bin", stream.getvalue())

    def test_the_ancestor_guard_does_not_fire_on_an_ordinary_directory(self):
        """A guard that cries wolf gets switched off, so its edges are pinned.

        ``saves/`` with neither a manifest nor an autosave in it holds nothing
        a recording could destroy, and a bare ``manifest.json`` is a plausible
        file to meet in an unrelated tree.
        """
        run_dir = _synthetic_run_dir(self.root / "real")
        self.assertTrue(recorder.holds_savegames(run_dir))
        (run_dir / "manifest.json").unlink()
        self.assertTrue(recorder.holds_savegames(run_dir))

        hollow = self.root / "hollow"
        (hollow / "saves").mkdir(parents=True)
        self.assertFalse(recorder.holds_savegames(hollow))
        manifest_only = self.root / "manifest-only"
        manifest_only.mkdir()
        (manifest_only / "manifest.json").write_bytes(b"{}")
        self.assertFalse(recorder.holds_savegames(manifest_only))
        self.assertFalse(recorder.looks_like_runs_root(self.root / "hollow"))

        # The repo's own default out root must stay writable, or the guard has
        # broken the tool it protects.
        recorder.assert_safe_target(
            recorder.DEFAULT_OUT_ROOT / "game_x", recorder.DEFAULT_OUT_ROOT,
        )

    def test_the_collision_check_refuses_before_reading_a_single_byte(self):
        """Pointed at a directory with a thousand savegames, the old check read
        every one of them into memory before it could refuse."""
        target = _synthetic_run_dir(self.root / "big")
        payload = {"index.json": b"{}", "gaps.json": b"{}"}
        original = recorder._file_sha256
        recorder._file_sha256 = lambda path: self.fail(  # type: ignore[assignment]
            f"the collision check hashed {path} to answer a name mismatch",
        )
        try:
            self.assertTrue(recorder.differs_from_disk(target, payload))
        finally:
            recorder._file_sha256 = original  # type: ignore[assignment]

    def test_an_empty_or_matching_target_is_not_a_collision(self):
        empty = self.root / "empty"
        empty.mkdir()
        self.assertFalse(recorder.differs_from_disk(empty, {"index.json": b"{}"}))
        (empty / "index.json").write_bytes(b"{}")
        self.assertFalse(recorder.differs_from_disk(empty, {"index.json": b"{}"}))
        self.assertTrue(recorder.differs_from_disk(empty, {"index.json": b"{ }"}))


# --------------------------------------------------------------------------
# CLI surface
# --------------------------------------------------------------------------


class CommandLineTests(unittest.TestCase):

    def test_live_refuses_and_names_the_deferral_and_its_reason(self):
        """Deferred loudly, with the reason, not silently missing.

        Even a read-only GET consumes cursor-registry slots on a live seat, and
        the supervisor's own ``phase.end`` discovery draws on that reserve -- so
        a recorder that pages a live seat can evict the seat's own cursors.
        """
        args = argparse.Namespace(
            base_url="http://localhost:1", game="game_x", token_file=None,
            out="/dev/null", poll_seconds=1.0, max_revisions=1,
        )
        with self.assertRaises(recorder.CorpusError) as caught:
            recorder.command_live(args, io.StringIO())
        message = str(caught.exception)
        self.assertIn("'live' subcommand is deferred", message)
        self.assertIn("cursor-registry slots", message)
        self.assertIn("record", message)
        self.assertEqual(caught.exception.exit_code, recorder.DEFERRED_EXIT_CODE)

    def test_live_exits_through_a_documented_code_not_a_traceback(self):
        """A deferral is a decision, not a crash.

        ``NotImplementedError`` escaped ``main``'s except clauses and reached
        the shell as a stack trace, which is the one failure delivery this CLI
        does not otherwise have.
        """
        stream = io.StringIO()
        errors = io.StringIO()
        saved = sys.stderr
        sys.stderr = errors
        try:
            code = recorder.main(
                ["live", "--base-url", "http://localhost:1", "--game", "g"],
                stream,
            )
        finally:
            sys.stderr = saved
        self.assertEqual(code, recorder.DEFERRED_EXIT_CODE)
        self.assertIn("deferred to phase 2", errors.getvalue())

    def test_the_dead_save_ref_dataclass_is_gone(self):
        """It was defined and never constructed anywhere in the module."""
        self.assertFalse(hasattr(recorder, "SaveRef"))

    def test_the_parser_exposes_every_subcommand(self):
        parser = recorder.build_parser()
        for argv in (
            ["record", "--run-dir", "x"],
            ["verify", "--corpus", "x"],
            ["list"],
            ["live", "--base-url", "u", "--game", "g"],
        ):
            with self.subTest(argv=argv):
                self.assertEqual(parser.parse_args(argv).command, argv[0])

    def test_list_reports_an_empty_runs_root_as_an_error_not_a_crash(self):
        stream = io.StringIO()
        with tempfile.TemporaryDirectory() as scratch:
            missing = Path(scratch) / "absent"
            code = recorder.main(
                ["list", "--runs-root", str(missing)], stream,
            )
        self.assertEqual(code, 2)

    def test_list_renders_json_when_asked(self):
        stream = io.StringIO()
        with tempfile.TemporaryDirectory() as scratch:
            root = Path(scratch) / "runs"
            _tmp_run_dir(root, harvest.ENTERTAINER_INCIDENT[0])
            code = recorder.main(
                ["list", "--runs-root", str(root), "--json"], stream,
            )
        self.assertEqual(code, 0)
        rows = json.loads(stream.getvalue())
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["game_id"], harvest.ENTERTAINER_INCIDENT[0])
        self.assertEqual(rows[0]["saves"], 1)
        self.assertEqual(rows[0]["last_turn"], 52)

    def test_main_turns_a_corpus_error_into_its_exit_code(self):
        stream = io.StringIO()
        with tempfile.TemporaryDirectory() as scratch:
            code = recorder.main(
                ["verify", "--corpus", str(Path(scratch) / "nothing")], stream,
            )
        self.assertEqual(code, 2)

    def test_main_records_end_to_end_through_argv(self):
        stream = io.StringIO()
        with tempfile.TemporaryDirectory(prefix="corpus-cli-") as scratch:
            root = Path(scratch)
            run_dir = _tmp_run_dir(root / "runs", harvest.ENTERTAINER_INCIDENT[0])
            out = root / "corpus"
            code = recorder.main([
                "record", "--run-dir", str(run_dir), "--out", str(out),
                "--sections", "overview,cities", "--no-frames",
                "--live-margin", "0", "--per-game", "1",
            ], stream)
            self.assertEqual(code, 0)
            target = out / harvest.ENTERTAINER_INCIDENT[0]
            self.assertTrue((target / "index.json").is_file())
            self.assertFalse((target / "frames").exists())
            self.assertEqual(recorder.main(
                ["verify", "--corpus", str(target), "--strict"], stream,
            ), 0)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
