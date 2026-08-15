"""Create and verify deterministic projector corpora from saved games.

The corpus is an offline oracle for the v2 observation projector.  It rebuilds
native rows from committed or historical saves, then records exact
``state_page`` and ``legal_actions_page`` bytes under deterministic entropy and
a frozen clock.  ``gaps.json`` states that the rows are reconstructed rather
than captured.

Recording is read-only with respect to the source run and create-only with
respect to the destination: an existing corpus is never overwritten or pruned.
``verify --strict`` re-derives the corpus when the source run is still present.
"""

from __future__ import annotations

import argparse
import base64
import contextlib
import hashlib
import json
import re
import secrets as _stdlib_secrets
import sys
import tempfile
import time as _stdlib_time
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from agent_eval import v2_control
from agent_eval.tests import v2_obs_corpus as harvest
from agent_eval.tests import v2_obs_fixtures as fixtures

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_RUNS_ROOT = REPO_ROOT / ".agent-eval" / "runs"
DEFAULT_OUT_ROOT = REPO_ROOT / ".agent-eval" / "corpus"

TOOL_NAME = "agent_eval.corpus_record"
TOOL_VERSION = 3
CORPUS_FORMAT = "freeciv-v2-golden-corpus"
CORPUS_SCHEMA_VERSION = 3
DEFAULT_ENTROPY_SEED = "00" * 32
FROZEN_WALL_TIME = 1700000000.0
FROZEN_MONOTONIC = 1000.0
CORPUS_AGENT_ID = "agent_corpus"
FALLBACK_GAME_ID = "game_corpus"
FIXTURE_GENERATION = 1
FIXTURE_NATIVE_REVISION = 11

DIRECT_SECTIONS: tuple[str, ...] = (
    "overview",
    "cities",
    "units",
    "city_sites",
    "known_tiles",
    "research",
    "governments",
    "multipliers",
    "spaceship",
    "infrastructure",
    "diplomacy",
    "tombstones",
    "votes",
    "chat",
    "chat_recipients",
)
CITY_SCOPED_SECTIONS: tuple[str, ...] = (
    "city_detail",
    "city_citizens",
    "city_build_choices",
    "city_worklist",
    "city_improvements",
    "city_trade_routes",
    "city_governor",
    "city_worker_tasks",
)
UNREACHABLE_SECTIONS: tuple[str, ...] = (
    "map_tiles",
    "pregame_nations",
    "pregame_styles",
    "pregame_teams",
    "unit_route",
)

_SAFE_SEGMENT_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
_MAX_RELPATH_BYTES = 200


class CorpusError(Exception):
    """A recorder failure with an exit code the CLI reports verbatim."""

    def __init__(self, message: str, exit_code: int = 2) -> None:
        super().__init__(message)
        self.exit_code = exit_code


def is_safe_relpath(relpath: str) -> bool:
    """Whether ``relpath`` is bounded, relative, and traversal-free."""
    if not relpath or len(relpath.encode("utf-8")) > _MAX_RELPATH_BYTES:
        return False
    return all(
        _SAFE_SEGMENT_RE.fullmatch(segment) is not None
        for segment in relpath.split("/")
    )


def canonical_json(value: Any) -> bytes:
    """The canonical JSON form shared with ``v2_receipts`` and arena/wire."""
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def canonical_lines(values: Iterable[Any]) -> bytes:
    return b"".join(canonical_json(item) + b"\n" for item in values)


def pretty_json(value: Any) -> bytes:
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            indent=2,
            allow_nan=False,
        ).encode("utf-8")
        + b"\n"
    )


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class _CounterEntropy:
    """A deterministic replacement for the ``secrets`` surface in v2_control."""

    def __init__(self, seed: bytes) -> None:
        self._seed = bytes(seed)
        self._counter = 0

    def _block(self) -> bytes:
        block = hashlib.sha256(
            self._seed + self._counter.to_bytes(8, "big"),
        ).digest()
        self._counter += 1
        return block

    def token_bytes(self, nbytes: int | None = None) -> bytes:
        wanted = 32 if nbytes is None else int(nbytes)
        if wanted < 0:
            raise ValueError("token_bytes: negative length")
        chunks: list[bytes] = []
        while sum(map(len, chunks)) < wanted:
            chunks.append(self._block())
        return b"".join(chunks)[:wanted]

    def token_urlsafe(self, nbytes: int | None = None) -> str:
        raw = self.token_bytes(32 if nbytes is None else nbytes)
        return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


class _FrozenClock:
    def __init__(
        self,
        wall: float = FROZEN_WALL_TIME,
        monotonic: float = FROZEN_MONOTONIC,
    ) -> None:
        self._wall = wall
        self._monotonic = monotonic

    def time(self) -> float:
        return self._wall

    def monotonic(self) -> float:
        return self._monotonic


def assert_entropy_surface() -> None:
    """Fail if the deterministic module swap would silently do nothing."""
    if getattr(v2_control, "secrets", None) is not _stdlib_secrets:
        raise CorpusError(
            "corpus_record: v2_control.secrets changed; the determinism swap "
            "would be a silent no-op",
        )
    if getattr(v2_control, "time", None) is not _stdlib_time:
        raise CorpusError(
            "corpus_record: v2_control.time changed; the determinism swap "
            "would be a silent no-op",
        )


@contextlib.contextmanager
def deterministic_entropy(seed: bytes) -> Iterator[None]:
    assert_entropy_surface()
    saved_secrets = v2_control.secrets
    saved_time = v2_control.time
    v2_control.secrets = _CounterEntropy(seed)  # ty: ignore[invalid-assignment]
    v2_control.time = _FrozenClock()  # ty: ignore[invalid-assignment]
    try:
        yield
    finally:
        v2_control.secrets = saved_secrets
        v2_control.time = saved_time


def sample_seed(seed: bytes, key: str) -> bytes:
    return hashlib.sha256(seed + b"\x00" + key.encode("utf-8")).digest()


@dataclass(frozen=True)
class RunSource:
    run_dir: Path
    game_id: str
    manifest: dict[str, Any]
    saves: tuple[Path, ...]


def _relative_to_repo(path: Path) -> str:
    try:
        return path.resolve().relative_to(REPO_ROOT).as_posix()
    except ValueError:
        return path.resolve().as_posix()


def _manifest_facts(run_dir: Path) -> dict[str, Any]:
    path = run_dir / "manifest.json"
    if not path.is_file():
        return {"present": False}
    try:
        with open(path, "rb") as handle:
            document = json.loads(handle.read().decode("utf-8"))
    except (OSError, ValueError, UnicodeDecodeError):
        return {"present": True, "readable": False}
    if not isinstance(document, Mapping):
        return {"present": True, "readable": False}
    config = document.get("config")
    config = config if isinstance(config, Mapping) else {}
    seats_raw = config.get("seats")
    seats: list[dict[str, Any]] = []
    if isinstance(seats_raw, Sequence) and not isinstance(seats_raw, (str, bytes)):
        for seat in seats_raw:
            if isinstance(seat, Mapping):
                seats.append(
                    {
                        "id": seat.get("id"),
                        "name": seat.get("name"),
                        "type": seat.get("type"),
                        "controller_label": seat.get("controller_label"),
                        "controller_fingerprint": seat.get("controller_fingerprint"),
                    }
                )
    seeds_raw = config.get("seeds")
    seeds = (
        [
            int(item)
            for item in seeds_raw
            if isinstance(item, int) and not isinstance(item, bool)
        ]
        if isinstance(seeds_raw, Sequence) and not isinstance(seeds_raw, (str, bytes))
        else []
    )
    return {
        "present": True,
        "readable": True,
        "control_protocol": document.get("control_protocol"),
        "ruleset": config.get("ruleset"),
        "mode": config.get("mode"),
        "places": config.get("places"),
        "seeds": seeds,
        "seats": seats,
        "current_turn": document.get("current_turn"),
        "finished": document.get("finished_at") is not None,
        "error": document.get("error"),
    }


def read_run_source(run_dir: Path) -> RunSource:
    """Inventory a run through read-only save readers."""
    resolved = run_dir.resolve()
    if not resolved.is_dir():
        raise CorpusError(f"corpus_record: not a directory: {run_dir}")
    saves = harvest.save_paths(resolved.parent).get(resolved.name, [])
    return RunSource(
        run_dir=resolved,
        game_id=resolved.name,
        manifest=_manifest_facts(resolved),
        saves=tuple(saves),
    )


def save_turn(path: Path) -> int:
    match = harvest.SAVE_NAME.match(path.name)
    return int(match.group(1)) if match is not None else -1


def select_saves(
    saves: Sequence[Path],
    *,
    per_game: int,
    turns: Sequence[int] = (),
    live_margin_s: float = 120.0,
    now: float | None = None,
) -> tuple[list[Path], list[dict[str, str]]]:
    """Choose stable saves; explicit turns override sampling and live margin."""
    ordered = list(saves)
    skipped: list[dict[str, str]] = []
    if turns:
        wanted = set(map(int, turns))
        return ([path for path in ordered if save_turn(path) in wanted], skipped)
    candidates = list(ordered)
    if candidates and live_margin_s > 0:
        tail = candidates[-1]
        moment = _stdlib_time.time() if now is None else now
        try:
            age = moment - tail.stat().st_mtime
        except OSError:
            age = live_margin_s + 1.0
        if age < live_margin_s:
            skipped.append({"path": tail.name, "reason": "live_margin"})
            candidates.pop()
    chosen = harvest._spread(candidates, per_game)
    if candidates and candidates[-1] not in chosen:
        chosen.append(candidates[-1])
    return chosen, skipped


@dataclass
class RecordedSample:
    key: str
    turn: int
    player_index: int
    save_name: str
    rows: tuple[str, ...]
    row_digest: str
    files: dict[str, bytes] = field(default_factory=dict)
    page_paths: list[str] = field(default_factory=list)
    notes: list[dict[str, Any]] = field(default_factory=list)


def _row_digest(rows: Sequence[str]) -> str:
    return sha256_hex(b"\x00".join(row.encode("ascii") for row in rows))


def _drain(
    control: v2_control.V2SeatControl,
    first: Mapping[str, Any],
    endpoint: str,
    follow: bool,
) -> list[dict[str, Any]]:
    pages = [dict(first)]
    if not follow:
        return pages
    guard = 0
    cursor = first.get("page", {}).get("next_cursor")
    while isinstance(cursor, str) and cursor:
        guard += 1
        if guard > v2_control.MAX_CURSOR_CHAIN_PAGES:
            raise CorpusError(
                "corpus_record: cursor chain exceeded MAX_CURSOR_CHAIN_PAGES",
            )
        page = control.continue_page(cursor, endpoint=endpoint)
        pages.append(dict(page))
        cursor = page.get("page", {}).get("next_cursor")
    return pages


def _first_id(page: Mapping[str, Any], keys: Sequence[str]) -> str | None:
    items = page.get("page", {}).get("items")
    if not isinstance(items, Sequence) or not items:
        return None
    head = items[0]
    if not isinstance(head, Mapping):
        return None
    for key in keys:
        value = head.get(key)
        if isinstance(value, str) and value:
            return value
    return None


def _emit(
    sample: RecordedSample, relpath: str, payload: bytes, *, page: bool = False
) -> None:
    if not is_safe_relpath(relpath):
        raise CorpusError(f"corpus_record: unsafe corpus path {relpath!r}")
    if relpath in sample.files:
        raise CorpusError(f"corpus_record: duplicate corpus path {relpath!r}")
    sample.files[relpath] = payload
    if page:
        sample.page_paths.append(relpath)


def record_sample(
    corpus_sample: harvest.CorpusSample,
    player_index: int,
    *,
    game_id: str,
    seed: bytes,
    sections: Sequence[str],
    limit: int,
    page_limit: int,
    follow_cursors: bool,
) -> RecordedSample:
    """Render one reconstructed empire into bundles and projector pages."""
    key = f"t{corpus_sample.turn:04d}-p{player_index}"
    rows = fixtures.build_rows(harvest.case_from_player(corpus_sample, player_index))
    observation = {
        "generation": FIXTURE_GENERATION,
        "native_revision": FIXTURE_NATIVE_REVISION,
        "rows": rows,
    }
    sample = RecordedSample(
        key=key,
        turn=corpus_sample.turn,
        player_index=player_index,
        save_name=corpus_sample.path.name,
        rows=rows,
        row_digest=_row_digest(rows),
    )

    with deterministic_entropy(sample_seed(seed, key)):
        control = v2_control.V2SeatControl(
            game_id,
            CORPUS_AGENT_ID,
            FIXTURE_GENERATION,
        )
        direct = [name for name in sections if name in DIRECT_SECTIONS]
        city_scoped = [name for name in sections if name in CITY_SCOPED_SECTIONS]
        rendered: dict[str, list[dict[str, Any]]] = {}
        for section in direct:
            rendered[f"state/{section}.l{limit:02d}"] = _drain(
                control,
                control.state_page(observation, section, limit),
                "state",
                follow_cursors,
            )
        for section in ("cities", "known_tiles"):
            if section in direct and page_limit < limit:
                rendered[f"state/{section}.l{page_limit:02d}"] = _drain(
                    control,
                    control.state_page(observation, section, page_limit),
                    "state",
                    follow_cursors,
                )

        cities = rendered.get(f"state/cities.l{limit:02d}")
        city_id = _first_id(cities[0], ("id", "city_id")) if cities else None
        for section in city_scoped:
            if city_id is None:
                sample.notes.append(
                    {
                        "kind": "scope_unavailable",
                        "section": section,
                        "detail": "the bundle has no city to scope on",
                    }
                )
                continue
            rendered[f"state/{section}.l{limit:02d}.a0"] = _drain(
                control,
                control.state_page(observation, section, limit, actor_id=city_id),
                "state",
                follow_cursors,
            )

        tiles = rendered.get(f"state/known_tiles.l{limit:02d}")
        tile_id = _first_id(tiles[0], ("id", "tile_id")) if tiles else None
        if "tile_window" in sections:
            if tile_id is None:
                sample.notes.append(
                    {
                        "kind": "scope_unavailable",
                        "section": "tile_window",
                        "detail": "the bundle has no known tile to centre on",
                    }
                )
            else:
                rendered[f"state/tile_window.l{limit:02d}.c0.r1"] = _drain(
                    control,
                    control.state_page(
                        observation,
                        "tile_window",
                        limit,
                        center_id=tile_id,
                        radius=1,
                    ),
                    "state",
                    follow_cursors,
                )

        diplomacy = rendered.get(f"state/diplomacy.l{limit:02d}")
        relation_id = (
            _first_id(
                diplomacy[0],
                ("relation_id", "id"),
            )
            if diplomacy
            else None
        )
        if "diplomacy_clauses" in sections:
            if relation_id is None:
                sample.notes.append(
                    {
                        "kind": "scope_unavailable",
                        "section": "diplomacy_clauses",
                        "detail": "the bundle has no contacted player",
                    }
                )
            else:
                rendered[f"state/diplomacy_clauses.l{limit:02d}.k0"] = _drain(
                    control,
                    control.state_page(
                        observation,
                        "diplomacy_clauses",
                        limit,
                        relation_id=relation_id,
                    ),
                    "state",
                    follow_cursors,
                )

        rendered[f"legal_actions/l{limit:02d}"] = _drain(
            control,
            control.legal_actions_page(observation, limit),
            "legal_actions",
            follow_cursors,
        )
        if page_limit < limit:
            rendered[f"legal_actions/l{page_limit:02d}"] = _drain(
                control,
                control.legal_actions_page(observation, page_limit),
                "legal_actions",
                follow_cursors,
            )

    for section in UNREACHABLE_SECTIONS:
        if section in sections:
            sample.notes.append(
                {
                    "kind": "unreachable_section",
                    "section": section,
                    "detail": (
                        "state_page cannot serve this section; it requires a live "
                        "prepare_state_scope/materialize_state_scope round trip"
                    ),
                }
            )

    rows_path = f"bundles/{key}.rows.jsonl"
    _emit(sample, rows_path, canonical_lines(rows))
    _emit(
        sample,
        f"bundles/{key}.bundle.json",
        canonical_json(
            {
                "generation": FIXTURE_GENERATION,
                "native_revision": FIXTURE_NATIVE_REVISION,
                "revision_source": "fixture-constant",
                "row_count": len(rows),
                "row_digest": sample.row_digest,
                "rows_file": rows_path,
            }
        ),
    )
    for name in sorted(rendered):
        chain = rendered[name]
        suffix = "json" if len(chain) == 1 else "chain.jsonl"
        payload = (
            canonical_json(chain[0]) if len(chain) == 1 else canonical_lines(chain)
        )
        path = f"pages/{name.replace('/', f'/{key}.', 1)}.{suffix}"
        _emit(sample, path, payload, page=True)
    return sample


def corpus_pins() -> dict[str, Any]:
    """Projector constants that a port must reproduce."""
    return {
        "full_control_schema_version": 2,
        "control_protocol": "full-control-v2",
        "native_observation_action_schema_id": v2_control.NATIVE_OBSERVATION_ACTION_SCHEMA_ID,
        "max_page_items": v2_control.MAX_PAGE_ITEMS,
        "max_native_row_bytes": v2_control.MAX_NATIVE_ROW_BYTES,
        "max_bundled_rows": v2_control.MAX_BUNDLED_ROWS,
        "cursor_ttl_seconds": v2_control.CURSOR_TTL_SECONDS,
        "row_field_schema_digest": sha256_hex(
            canonical_json(
                {
                    kind: list(order)
                    for kind, order in sorted(v2_control._ROW_FIELDS.items())
                }
            )
        ),
    }


def resolve_sections(spec: str) -> tuple[str, ...]:
    scoped = (*CITY_SCOPED_SECTIONS, "tile_window", "diplomacy_clauses")
    if spec == "direct":
        return DIRECT_SECTIONS
    if spec == "scoped":
        return scoped
    if spec == "all":
        return DIRECT_SECTIONS + scoped
    names = tuple(item.strip() for item in spec.split(",") if item.strip())
    unknown = [name for name in names if name not in v2_control._STATE_SECTIONS]
    if unknown:
        raise CorpusError(f"corpus_record: unknown section(s): {', '.join(unknown)}")
    return names


@dataclass
class GameCorpus:
    game_id: str
    source_run_dir: Path
    files: dict[str, bytes]
    index: dict[str, Any]

    def rendered(self) -> dict[str, bytes]:
        return {**self.files, "index.json": pretty_json(self.index)}


def _file_sha256(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    with open(path, "rb") as handle:
        while chunk := handle.read(1 << 20):
            digest.update(chunk)
            size += len(chunk)
    return digest.hexdigest(), size


def record_game(
    source: RunSource,
    *,
    seed: bytes,
    sections: Sequence[str],
    per_game: int,
    turns: Sequence[int],
    players: Sequence[int],
    limit: int,
    page_limit: int,
    follow_cursors: bool,
    live_margin_s: float,
) -> GameCorpus:
    game_id = (
        source.game_id
        if v2_control._OPAQUE_OWNER.fullmatch(source.game_id) is not None
        else FALLBACK_GAME_ID
    )
    chosen, skipped = select_saves(
        source.saves,
        per_game=per_game,
        turns=turns,
        live_margin_s=live_margin_s,
    )
    files: dict[str, bytes] = {}
    sample_index: list[dict[str, Any]] = []
    save_index: list[dict[str, Any]] = []
    notes: list[dict[str, Any]] = []
    seen_keys: set[str] = set()

    for path in chosen:
        loaded = harvest.load_sample(path)
        if loaded is None:
            skipped.append({"path": path.name, "reason": "unstable_or_unparsable"})
            continue
        digest, size = _file_sha256(path)
        save_index.append(
            {
                "path": f"saves/{path.name}",
                "turn": loaded.turn,
                "sha256": digest,
                "size": size,
            }
        )
        wanted = (
            [index for index in players if 0 <= index < len(loaded.players)]
            if players
            else [player.index for player in loaded.players if player.cities]
        )
        if not wanted:
            notes.append(
                {
                    "kind": "no_recordable_player",
                    "turn": loaded.turn,
                    "detail": "no player in this save owns a city",
                }
            )
            continue
        for player_index in wanted:
            sample = record_sample(
                loaded,
                player_index,
                game_id=game_id,
                seed=seed,
                sections=sections,
                limit=limit,
                page_limit=page_limit,
                follow_cursors=follow_cursors,
            )
            if sample.key in seen_keys:
                raise CorpusError(
                    f"corpus_record: duplicate sample key {sample.key!r}",
                )
            seen_keys.add(sample.key)
            files.update(sample.files)
            sample_index.append(
                {
                    "key": sample.key,
                    "turn": sample.turn,
                    "player_index": sample.player_index,
                    "save": f"saves/{sample.save_name}",
                    "row_count": len(sample.rows),
                    "row_digest": sample.row_digest,
                    "generation": FIXTURE_GENERATION,
                    "native_revision": FIXTURE_NATIVE_REVISION,
                    "revision_source": "fixture-constant",
                    "artifacts": {
                        "bundle": f"bundles/{sample.key}.bundle.json",
                        "rows": f"bundles/{sample.key}.rows.jsonl",
                        "pages": sorted(sample.page_paths),
                    },
                    "notes": sample.notes,
                }
            )

    files["gaps.json"] = canonical_json(
        {
            "corpus_gaps": dict(harvest.CORPUS_GAPS),
            "recorder_mode": "offline-synthesised",
            "detail": (
                "Rows are reconstructed from saves; they are not captured native "
                "traffic. The page and legal-actions projector code is exercised."
            ),
            "unreachable_sections": list(UNREACHABLE_SECTIONS),
            "notes": notes,
        }
    )
    file_hashes = {
        path: {"sha256": sha256_hex(payload), "bytes": len(payload)}
        for path, payload in sorted(files.items())
    }
    index = {
        "schema_version": CORPUS_SCHEMA_VERSION,
        "format": CORPUS_FORMAT,
        "game_id": game_id,
        "generated_by": {
            "tool": TOOL_NAME,
            "tool_version": TOOL_VERSION,
            "recorder_mode": "offline-synthesised",
            "entropy_seed": seed.hex(),
            "clock": {"time": FROZEN_WALL_TIME, "monotonic": FROZEN_MONOTONIC},
            "settings": {
                "sections": list(sections),
                "limit": limit,
                "page_limit": page_limit,
                "follow_cursors": follow_cursors,
                "per_game": per_game,
                "turns": list(turns),
                "players": list(players),
                "live_margin_s": live_margin_s,
            },
        },
        "pins": corpus_pins(),
        "source": {
            "run_dir": _relative_to_repo(source.run_dir),
            "manifest": source.manifest,
            "saves_available": len(source.saves),
            "saves": save_index,
            "skipped": skipped,
        },
        "samples": sample_index,
        "files": file_hashes,
        "corpus_digest": sha256_hex(
            canonical_json(
                {path: entry["sha256"] for path, entry in file_hashes.items()}
            )
        ),
    }
    return GameCorpus(
        game_id=game_id,
        source_run_dir=source.run_dir,
        files=files,
        index=index,
    )


def _target_for(corpus: GameCorpus, out_root: Path) -> Path:
    target = out_root / corpus.game_id
    resolved = target.resolve()
    source = corpus.source_run_dir.resolve()
    if resolved == source or source in resolved.parents:
        raise CorpusError(
            f"corpus_record: refusing to create output inside source run {source}",
        )
    if target.exists():
        raise CorpusError(
            f"corpus_record: {target} already exists; corpora are create-only",
            exit_code=3,
        )
    return target


def write_corpus(corpus: GameCorpus, out_root: Path) -> tuple[Path, int]:
    """Create a fresh corpus.  Never overwrite, prune, or unlink."""
    target = _target_for(corpus, out_root)
    payload = corpus.rendered()
    try:
        target.mkdir(parents=True, exist_ok=False)
        written = 0
        for relpath in sorted(payload):
            path = target / relpath
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(payload[relpath])
            written += len(payload[relpath])
    except FileExistsError as exc:
        raise CorpusError(
            f"corpus_record: {target} already exists; corpora are create-only",
            exit_code=3,
        ) from exc
    except OSError as exc:
        raise CorpusError(f"corpus_record: could not create {target}: {exc}") from exc
    return target, written


def read_corpus_files(target: Path) -> dict[str, bytes]:
    return {
        path.relative_to(target).as_posix(): path.read_bytes()
        for path in sorted(target.rglob("*"))
        if path.is_file()
    }


def selftest() -> dict[str, Any]:
    """Prove same-seed stability and different-seed sensitivity."""
    observation = fixtures.observation(fixtures.case("corpus-selftest"))

    def render(seed: bytes) -> bytes:
        with deterministic_entropy(seed):
            control = v2_control.V2SeatControl(
                FALLBACK_GAME_ID,
                CORPUS_AGENT_ID,
                FIXTURE_GENERATION,
            )
            pages = _drain(
                control,
                control.state_page(observation, "cities", 1),
                "state",
                True,
            )
            pages.append(control.legal_actions_page(observation, 4))
            return canonical_json(pages)

    baseline = render(b"\x00" * 32)
    if baseline != render(b"\x00" * 32):
        raise CorpusError("corpus_record: same-seed determinism self-test failed")
    if baseline == render(b"\x11" * 32):
        raise CorpusError("corpus_record: entropy seed is not load-bearing")
    return {"stable": True, "seed_sensitive": True, "digest": sha256_hex(baseline)}


def _seed_bytes(text: str) -> bytes:
    try:
        raw = bytes.fromhex(text)
    except ValueError as exc:
        raise CorpusError(
            f"corpus_record: --entropy-seed is not hex: {text!r}",
        ) from exc
    if not raw:
        raise CorpusError("corpus_record: --entropy-seed must not be empty")
    return raw


def _run_dirs(args: argparse.Namespace) -> list[Path]:
    if args.run_dir:
        return [Path(item) for item in args.run_dir]
    root = Path(args.runs_root)
    if not root.is_dir():
        raise CorpusError(f"corpus_record: no runs root at {root}")
    wanted = set(args.game or ())
    return [
        path
        for path in sorted(root.iterdir())
        if path.is_dir()
        and (path / "saves").is_dir()
        and (not wanted or path.name in wanted)
    ]


def build_corpora(args: argparse.Namespace) -> list[GameCorpus]:
    seed = _seed_bytes(args.entropy_seed)
    sections = resolve_sections(args.sections)
    corpora: list[GameCorpus] = []
    for run_dir in _run_dirs(args):
        source = read_run_source(run_dir)
        if source.saves:
            corpora.append(
                record_game(
                    source,
                    seed=seed,
                    sections=sections,
                    per_game=args.per_game,
                    turns=tuple(args.turn or ()),
                    players=tuple(args.player or ()),
                    limit=args.limit,
                    page_limit=args.page_limit,
                    follow_cursors=args.follow_cursors,
                    live_margin_s=args.live_margin,
                )
            )
    return corpora


def command_record(args: argparse.Namespace, stream: Any) -> int:
    checks = selftest()
    print(f"determinism self-test ok ({checks['digest'][:16]})", file=stream)
    if not 1 <= args.limit <= v2_control.MAX_PAGE_ITEMS:
        raise CorpusError(
            f"corpus_record: --limit must be 1..{v2_control.MAX_PAGE_ITEMS}",
        )
    if not 1 <= args.page_limit <= args.limit:
        raise CorpusError("corpus_record: --page-limit must be 1..--limit")
    corpora = build_corpora(args)
    if not corpora:
        print("corpus_record: no run directory had an autosave", file=stream)
        return 2
    out_root = Path(args.out)
    for corpus in corpora:
        target, written = write_corpus(corpus, out_root)
        print(
            f"{corpus.game_id}: {len(corpus.index['samples'])} samples, "
            f"{len(corpus.rendered())} files, {written} bytes -> {target} "
            f"[{corpus.index['corpus_digest'][:16]}]",
            file=stream,
        )
    return 0


def command_verify(args: argparse.Namespace, stream: Any) -> int:
    target = Path(args.corpus)
    index_path = target / "index.json"
    if not index_path.is_file():
        raise CorpusError(f"corpus_record: no index.json under {target}")
    try:
        index = json.loads(index_path.read_bytes().decode("utf-8"))
    except (OSError, ValueError, UnicodeDecodeError) as exc:
        raise CorpusError(f"corpus_record: unreadable index.json: {exc}") from exc
    schema = index.get("schema_version")
    if schema != CORPUS_SCHEMA_VERSION:
        print(
            f"FAIL schema_version {schema!r}; this recorder verifies "
            f"{CORPUS_SCHEMA_VERSION} only -- re-record the corpus",
            file=stream,
        )
        return 1

    failures: list[str] = []
    indexed: dict[str, Any] = index.get("files", {})
    for relpath, entry in sorted(indexed.items()):
        path = target / relpath
        if not path.is_file():
            failures.append(f"missing {relpath}")
            continue
        digest, size = _file_sha256(path)
        if digest != entry.get("sha256"):
            failures.append(f"digest mismatch {relpath}")
        elif size != entry.get("bytes"):
            failures.append(f"size mismatch {relpath}")
    actual = set(read_corpus_files(target))
    failures.extend(
        f"unexpected {relpath}"
        for relpath in sorted(actual - set(indexed) - {"index.json"})
    )
    recomputed = sha256_hex(
        canonical_json(
            {path: entry["sha256"] for path, entry in sorted(indexed.items())}
        )
    )
    if recomputed != index.get("corpus_digest"):
        failures.append("corpus_digest mismatch")
    for line in failures:
        print(f"FAIL {line}", file=stream)
    if failures:
        return 1
    print(
        f"{index.get('game_id')}: {len(indexed)} files verified "
        f"[{index.get('corpus_digest', '')[:16]}]",
        file=stream,
    )
    return _verify_strict(target, index, stream) if args.strict else 0


def strict_replay_args(index: Mapping[str, Any], run_dir: Path) -> argparse.Namespace:
    generated = index.get("generated_by", {})
    settings = generated.get("settings", {})
    return argparse.Namespace(
        run_dir=[str(run_dir)],
        runs_root=str(DEFAULT_RUNS_ROOT),
        game=[],
        entropy_seed=str(generated.get("entropy_seed", DEFAULT_ENTROPY_SEED)),
        sections=",".join(settings.get("sections", DIRECT_SECTIONS)),
        per_game=int(settings.get("per_game", 8)),
        turn=[int(item) for item in settings.get("turns", [])],
        player=[int(item) for item in settings.get("players", [])],
        limit=int(settings.get("limit", v2_control.MAX_PAGE_ITEMS)),
        page_limit=int(settings.get("page_limit", 4)),
        follow_cursors=bool(settings.get("follow_cursors", True)),
        live_margin=float(settings.get("live_margin_s", 120.0)),
    )


def _verify_strict(target: Path, index: Mapping[str, Any], stream: Any) -> int:
    source = index.get("source", {})
    run_dir = REPO_ROOT / str(source.get("run_dir", ""))
    if not (run_dir / "saves").is_dir():
        print(
            f"SKIP strict: source run {run_dir} is unavailable. Indexed "
            "digests and the closed file set were verified; projector "
            "re-derivation was not.",
            file=stream,
        )
        return 0
    rebuilt = build_corpora(strict_replay_args(index, run_dir))
    if len(rebuilt) != 1:
        print(
            f"FAIL strict: re-derivation produced {len(rebuilt)} corpora", file=stream
        )
        return 1
    derived_source = rebuilt[0].index.get("source", {})
    if source != derived_source:
        print(
            "SKIP strict: the source run changed after recording. Indexed "
            "digests verified; record from a finished run for re-derivation.",
            file=stream,
        )
        return 0
    with tempfile.TemporaryDirectory(prefix="corpus-verify-") as scratch:
        written, _ = write_corpus(rebuilt[0], Path(scratch))
        derived = read_corpus_files(written)
    on_disk = read_corpus_files(target)
    differing = sorted(
        set(derived) ^ set(on_disk)
        | {
            path
            for path in set(derived) & set(on_disk)
            if derived[path] != on_disk[path]
        },
    )
    for path in differing:
        print(f"FAIL strict: {path} differs from re-derivation", file=stream)
    if differing:
        return 1
    print(f"strict: {len(on_disk)} files re-derived byte-identically", file=stream)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python3 -m agent_eval.corpus_record",
        description="Record deterministic v2 projector pages from autosaves.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    record = subparsers.add_parser("record", help="create a corpus")
    record.add_argument("--run-dir", action="append", default=[])
    record.add_argument("--runs-root", default=str(DEFAULT_RUNS_ROOT))
    record.add_argument("--game", action="append", default=[])
    record.add_argument("--out", default=str(DEFAULT_OUT_ROOT))
    record.add_argument("--per-game", type=int, default=8)
    record.add_argument("--turn", action="append", type=int, default=[])
    record.add_argument("--player", action="append", type=int, default=[])
    record.add_argument("--sections", default="all")
    record.add_argument("--limit", type=int, default=v2_control.MAX_PAGE_ITEMS)
    record.add_argument("--page-limit", type=int, default=4)
    record.add_argument(
        "--follow-cursors",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    record.add_argument("--entropy-seed", default=DEFAULT_ENTROPY_SEED)
    record.add_argument("--live-margin", type=float, default=120.0)

    verify = subparsers.add_parser("verify", help="verify a recorded corpus")
    verify.add_argument("--corpus", required=True)
    verify.add_argument("--strict", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None, stream: Any = None) -> int:
    out = sys.stdout if stream is None else stream
    args = build_parser().parse_args(list(argv) if argv is not None else None)
    try:
        return {
            "record": command_record,
            "verify": command_verify,
        }[args.command](args, out)
    except CorpusError as exc:
        print(str(exc), file=sys.stderr)
        return exc.exit_code
    except v2_control.V2ControlError as exc:
        print(f"corpus_record: projector failure: {exc.code}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
