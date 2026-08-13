"""The golden corpus recorder: a byte-reproducible oracle for the v2 ports.

Why this exists
---------------
``agent_eval/v2_control.py`` (the observation projector) and
``agent_eval/headless_sidecar.py`` (the native IPC boundary) are being ported.
A port needs an oracle it can diff against, and "run both and eyeball it" is
not one.  This module writes that oracle: a table of

    (native observation rows, endpoint, section, limit, scope) -> exact page bytes

plus a protocol-faithful IPC frame trace, over the machine's own historical
empires, in canonical JSON, with a SHA-256 manifest.  Two recordings of the
same saves on two machines produce the same ``corpus_digest``; a port that
reproduces the digest has reproduced the projector.

What it is *not*
----------------
It is not captured live traffic, and it must never be sold as such.  Nothing in
``.agent-eval/runs/*/`` holds a single native observation row or a single IPC
frame -- the only durable substrate is the server autosave, so the row bundle
is *synthesised* from it by :mod:`agent_eval.tests.v2_obs_corpus` and
:mod:`agent_eval.tests.v2_obs_fixtures`, and every synthesised field family is
named in ``CORPUS_GAPS``, which is copied verbatim into ``gaps.json``.  The
page *code* is exercised for real; the row *content* is a reconstruction.  In
particular the legal-action catalog is computed by the C client against the
ruleset at request time and is never serialised, so the recorded
``legal_actions`` page pins the projector's treatment of action rows and is not
evidence about which actions the client would offer.  Frame traces are labelled
``synthesised-from-bundle`` for the same reason.  They are also *shape*
exemplars rather than session replays: each trace file restarts ``seq`` at 0,
nothing orders them against each other, and the corpus frames neither the
action verbs nor the scope verbs the recorded CAPS frame advertises.
``gaps.json`` says all of that in ``frame_verbs`` and ``frame_traces``, because
a gap nobody declared is the failure this recorder exists to prevent.

Safety
------
Run directories are shared with games that may be running right now.  This tool
is strictly read-only against them: it opens saves ``"rb"`` through
:mod:`agent_eval.save_replay`'s *low-level* readers (never the public entry
points, which write a derived cache), creates no path under a run directory,
takes no lock, starts/signals/connects to no process, and skips the tail save
of a run whose mtime is younger than ``--live-margin`` because that is the file
most likely to be mid-write.  All output goes under ``--out``, which defaults to
``.agent-eval/corpus`` -- a sibling of ``runs/``, never inside it.

"Never inside it" is *enforced*, not merely defaulted.  :func:`write_corpus`
prunes files the current rendering did not produce, so an ``--out`` aimed at a
runs root would have deleted a run's savegames and manifest -- the only durable
substrate the whole corpus is built from.  Two independent guards stop that:
:func:`assert_safe_target` refuses a target that carries a run directory's
markers or that sits inside (or is) a runs root, and the pruner is scoped to
the corpus's own path shapes (:func:`is_corpus_relpath`), so a file the recorder
could never have written is never a file the recorder deletes.

Exit codes
----------
``0`` success, ``1`` a verification failure, ``2`` a usage or input error,
``3`` an existing corpus that differs (pass ``--force``), ``4`` a refusal to
write to an unsafe target.  Every failure path returns one of these; none of
them is a traceback.

Determinism
-----------
``v2_control`` reaches entropy and the clock only through its module-level
``secrets`` and ``time`` names.  :func:`deterministic_entropy` swaps both for
reproducible stand-ins for the duration of a recording, which makes every
opaque id, every cursor token and every ``cursor_expires_at`` a pure function of
the seed.  An import-time guard fails loudly if a future refactor moves entropy
off those two names, so the recorder can never silently emit a corpus that only
looks reproducible.  The real fix is a private ``_entropy=``/``_clock=`` keyword
on ``V2SeatControl``; until that lands the swap is confined to this module.
"""

from __future__ import annotations

import argparse
import base64
import contextlib
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
import hashlib
import json
from pathlib import Path
import re
import secrets as _stdlib_secrets
import sys
import tempfile
import time as _stdlib_time
from typing import Any

from agent_eval import headless_sidecar as native
from agent_eval import v2_control
from agent_eval.tests import v2_obs_corpus as harvest
from agent_eval.tests import v2_obs_fixtures as fixtures

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RUNS_ROOT = REPO_ROOT / ".agent-eval" / "runs"
DEFAULT_OUT_ROOT = REPO_ROOT / ".agent-eval" / "corpus"

TOOL_NAME = "agent_eval.corpus_record"
#: Bumped whenever the recorded bytes change for the same inputs.  A parity rig
#: compares digests across machines, so a silent format change is a false
#: mismatch report; this is what makes it a loud one.
TOOL_VERSION = 2
CORPUS_FORMAT = "freeciv-v2-golden-corpus"
#: 2 hash-covers ``MANIFEST.sha256`` from ``index.json`` and declares the frame
#: vocabulary gaps.  ``verify`` refuses a corpus recorded at any other version
#: rather than half-checking it: at version 1 neither ``MANIFEST.sha256`` nor an
#: injected extra file was inside any digest, so a "verified" line was weaker
#: than it read.
CORPUS_SCHEMA_VERSION = 2

#: Default entropy seed.  Any hex string works; the corpus records which one it
#: used, and two corpora recorded with different seeds are legitimately unequal.
DEFAULT_ENTROPY_SEED = "00" * 32
#: The frozen clock.  A fixed wall time makes ``cursor_expires_at`` a constant,
#: and a fixed monotonic clock means no cursor or page chain can expire midway
#: through a recording -- a corpus must not depend on how fast the machine is.
FROZEN_WALL_TIME = 1700000000.0
FROZEN_MONOTONIC = 1000.0

#: The seat identity every recorded page carries.  The game id is the real run's
#: when it satisfies ``v2_control``'s owner grammar (it always does for ids this
#: harness mints), because a page that names a different game than its directory
#: is a trap for whoever reads the corpus later.
CORPUS_AGENT_ID = "agent_corpus"
FALLBACK_GAME_ID = "game_corpus"

#: The fixture revision series.  ``_snapshot`` refuses a non-increasing
#: ``native_revision`` on a seat, so every sample gets its own fresh seat and
#: these are constants rather than the run's real revisions -- which are not
#: recoverable from a save anyway.  ``index.json`` says so in
#: ``revision_source``; a reader who assumes this is the game's real revision
#: will be wrong.
FIXTURE_GENERATION = 1
FIXTURE_NATIVE_REVISION = 11

#: Sections ``supervisor`` routes straight into ``state_page`` with no scope.
#: Measured against a real synthesised bundle, not guessed.
DIRECT_SECTIONS: tuple[str, ...] = (
    "overview", "cities", "units", "city_sites", "known_tiles", "research",
    "governments", "multipliers", "spaceship", "infrastructure", "diplomacy",
    "tombstones", "votes", "chat", "chat_recipients",
)
#: Sections that need ``actor_id`` = a city id drawn from the ``cities`` page.
CITY_SCOPED_SECTIONS: tuple[str, ...] = (
    "city_detail", "city_citizens", "city_build_choices", "city_worklist",
    "city_improvements", "city_trade_routes", "city_governor",
    "city_worker_tasks",
)
#: Sections ``state_page`` cannot serve at all: the supervisor routes them
#: through ``prepare_state_scope``/``materialize_state_scope`` instead, which
#: needs a live native round trip.  Recorded as gaps, never as pages.
UNREACHABLE_SECTIONS: tuple[str, ...] = (
    "map_tiles", "pregame_nations", "pregame_styles", "pregame_teams",
    "unit_route",
)

#: An opaque projector id: ``<prefix>_<32 lowercase hex>``.  Matching on shape
#: rather than on a hardcoded prefix list means a new id family added to
#: ``v2_control`` lands in the id map automatically instead of silently
#: escaping it.
OPAQUE_ID_RE = re.compile(r"^([a-z][a-z0-9_]*)_([0-9a-f]{32})$")
CURSOR_ID_RE = re.compile(r"^cursor_[A-Za-z0-9_-]{6,64}$")

#: The synthetic IPC request token and its snapshot serial.  Both are protocol
#: tokens, not identities, so they are constants.
FRAME_REQUEST_ID = "obs1"
FRAME_SNAPSHOT_SERIAL = 1

_PLAYER_NAME_RE = re.compile(r"^[A-Za-z0-9._~-]{1,48}$")
#: One path *segment*.  Anchoring on the segment rather than on the whole
#: string is what makes this a traversal guard: a segment must begin with an
#: alphanumeric, so ``.``, ``..`` and a leading ``/`` cannot match.  The
#: previous whole-string form allowed ``..`` in the middle
#: (``bundles/../../../etc/x``), which read as a guard without being one.
_SAFE_SEGMENT_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
_MAX_RELPATH_BYTES = 200

#: The only shapes the recorder itself ever writes.  :func:`write_corpus`
#: deletes stale files, and it deletes *only* paths of these shapes, so a file
#: the recorder could not have produced is a file it will not remove.
CORPUS_ROOT_FILES = frozenset({
    "index.json", "MANIFEST.sha256", "gaps.json", "provenance.json",
})
CORPUS_DIRS = frozenset({"bundles", "pages", "frames", "ids"})
#: What ``verify`` tolerates on disk without an entry in ``index['files']``:
#: ``index.json`` cannot hash itself, and ``provenance.json`` is deliberately
#: outside the byte-identity guarantee because it carries the clock.
VERIFY_ALLOWLIST = frozenset({"index.json", "provenance.json"})


class CorpusError(Exception):
    """A recorder failure with an exit code the CLI reports verbatim."""

    def __init__(self, message: str, exit_code: int = 2) -> None:
        super().__init__(message)
        self.exit_code = exit_code


#: ``--out`` (or the corpus directory under it) is somewhere writing would
#: destroy something.  Its own code because "I refused to delete your saves" is
#: a different event from "this corpus differs, pass --force".
UNSAFE_TARGET_EXIT_CODE = 4


def is_safe_relpath(relpath: str) -> bool:
    """True for a path the recorder may write under the corpus directory.

    Relative, no traversal, no empty segment, no absolute prefix, bounded.
    """
    if not relpath or len(relpath.encode("utf-8")) > _MAX_RELPATH_BYTES:
        return False
    return all(
        _SAFE_SEGMENT_RE.fullmatch(segment) is not None
        for segment in relpath.split("/")
    )


def is_corpus_relpath(relpath: str) -> bool:
    """True for the path shapes this recorder emits, and nothing else.

    The pruner's whole authority: anything else under the corpus directory was
    put there by something that is not this tool, and deleting it is not this
    tool's business.
    """
    if not is_safe_relpath(relpath):
        return False
    head, _, rest = relpath.partition("/")
    if not rest:
        return relpath in CORPUS_ROOT_FILES
    return head in CORPUS_DIRS


# --------------------------------------------------------------------------
# Canonical bytes
# --------------------------------------------------------------------------


def canonical_json(value: Any) -> bytes:
    """The repo's one canonical JSON encoding.

    Identical to ``v2_receipts._canonical_json`` and ``supervisor._canonical``,
    which is also what ``arena/wire``'s ``canon.ts`` was written to reproduce
    byte for byte.  Every hash-covered file in the corpus goes through here.
    """
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def canonical_lines(values: Iterable[Any]) -> bytes:
    """One canonical object per line, ``\\n``-terminated, no trailing blank."""
    return b"".join(canonical_json(item) + b"\n" for item in values)


def pretty_json(value: Any) -> bytes:
    """``index.json``'s form: a human reads it, so it is indented.

    Its hash-covered content is always the compact form, so the indentation can
    never move a digest.
    """
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False,
    ).encode("utf-8") + b"\n"


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


# --------------------------------------------------------------------------
# Determinism
# --------------------------------------------------------------------------


class _CounterEntropy:
    """A counter DRBG exposing exactly the ``secrets`` surface v2_control uses.

    ``sha256(seed || counter)`` is not a cryptographic PRNG design worth
    defending; it does not have to be.  Nothing recorded here is a secret --
    the whole point is that the "secret" is published so a port can reproduce
    the HMACs it keys.
    """

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
        produced = 0
        while produced < wanted:
            block = self._block()
            chunks.append(block)
            produced += len(block)
        return b"".join(chunks)[:wanted]

    def token_urlsafe(self, nbytes: int | None = None) -> str:
        raw = self.token_bytes(32 if nbytes is None else nbytes)
        return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


class _FrozenClock:
    """``time.time``/``time.monotonic`` pinned so nothing expires mid-record."""

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
    """Fail loudly if v2_control stopped reaching entropy through its modules.

    The swap in :func:`deterministic_entropy` works only because
    ``v2_control.secrets`` and ``v2_control.time`` are the module objects.  If a
    refactor writes ``from secrets import token_bytes``, the swap becomes a
    no-op and the recorder would emit a corpus that is not reproducible while
    still claiming to be.  That failure must be an exception, not a surprise.
    """
    if getattr(v2_control, "secrets", None) is not _stdlib_secrets:
        raise CorpusError(
            "corpus_record: v2_control.secrets is no longer the stdlib "
            "secrets module; the determinism swap would be a silent no-op. "
            "Add an _entropy= keyword to V2SeatControl and use it here.",
        )
    if getattr(v2_control, "time", None) is not _stdlib_time:
        raise CorpusError(
            "corpus_record: v2_control.time is no longer the stdlib time "
            "module; the determinism swap would be a silent no-op. "
            "Add a _clock= keyword to V2SeatControl and use it here.",
        )


@contextlib.contextmanager
def deterministic_entropy(seed: bytes) -> Iterator[None]:
    """Swap ``v2_control``'s entropy and clock for reproducible stand-ins.

    Restores both unconditionally.  This is the only place in the module that
    touches another module's internals, and it is the single most fragile line
    of the design -- hence :func:`assert_entropy_surface` and the two-render
    self-test in :func:`selftest`.
    """
    assert_entropy_surface()
    saved_secrets = v2_control.secrets
    saved_time = v2_control.time
    v2_control.secrets = _CounterEntropy(seed)  # type: ignore[assignment]
    v2_control.time = _FrozenClock()  # type: ignore[assignment]
    try:
        yield
    finally:
        v2_control.secrets = saved_secrets
        v2_control.time = saved_time


def sample_seed(seed: bytes, key: str) -> bytes:
    """Per-sample entropy, so sample order can never change sample bytes."""
    return hashlib.sha256(seed + b"\x00" + key.encode("utf-8")).digest()


# --------------------------------------------------------------------------
# IPC frame synthesis
# --------------------------------------------------------------------------


def frame_record(seq: int, direction: str, payload: str, **extra: Any) -> dict[str, Any]:
    """One framed IPC message, in the form a frame-shape fixture consumes.

    ``dir`` is the direction, and its legend lives in ``gaps.json``: ``c2h`` is
    the C client -> the Python sidecar host, ``h2c`` the reverse, matching
    ``headless_sidecar._bootstrap``.  A consumer that reads it backwards
    inverts every replay it builds, so the vocabulary ships with the corpus
    rather than only with this source file.

    ``payload_b64`` is the authority and ``payload`` is the human-diffable
    rendering of the same bytes; they are proven to agree before the record is
    built, so a consumer never has to choose between them.  ``len`` is the value
    in the 4-byte big-endian header, i.e. the encoded length, not the character
    count.
    """
    if direction not in {"c2h", "h2c"}:
        raise CorpusError(f"corpus_record: bad frame direction {direction!r}")
    raw = payload.encode("utf-8")
    if not 1 <= len(raw) <= native.MAX_FRAME:
        raise CorpusError(
            f"corpus_record: frame {seq} is {len(raw)} bytes, outside "
            f"1..{native.MAX_FRAME}",
        )
    if len(raw.translate(None, delete=native._FORBIDDEN_FRAME_BYTES)) != len(raw):
        raise CorpusError(f"corpus_record: frame {seq} carries a forbidden control byte")
    encoded = base64.b64encode(raw).decode("ascii")
    if base64.b64decode(encoded) != raw:
        raise CorpusError(f"corpus_record: frame {seq} base64 round trip disagrees")
    record: dict[str, Any] = {
        "seq": seq,
        "dir": direction,
        "len": len(raw),
        "payload": payload,
        "payload_b64": encoded,
    }
    record.update(extra)
    return record


def frame_verbs(records: Iterable[Mapping[str, Any]]) -> list[str]:
    """The distinct leading tokens of a frame trace, sorted.

    Read off the frames that were actually emitted rather than retyped, so the
    "advertised but never framed" gap in ``gaps.json`` cannot go stale when a
    new emitter is added here or a new capability is added to the sidecar.
    """
    return sorted({
        str(record["payload"]).split("\t", 1)[0] for record in records
    })


def safe_player_name(name: str | None) -> str:
    """A READY payload the native token grammar accepts."""
    if isinstance(name, str) and _PLAYER_NAME_RE.fullmatch(name) is not None:
        return name
    return "AgentPlace1"


def handshake_frames(player_name: str) -> list[dict[str, Any]]:
    """The exact bootstrap ``HeadlessSidecar.start_and_take`` accepts.

    ``TAKE\\tREADY`` precedes ``READY\\t<player>`` because that is the order the
    bootstrap loop treats as the ordinary case: the take-progress flag has to be
    set before the identity frame commits.  The other legal order (READY first,
    held until the TAKE acknowledgement) is a separate shape and is not what a
    baseline trace should teach a port to expect.
    """
    ready = safe_player_name(player_name)
    payloads = (
        ("c2h", "HELLO\t1\tfreeciv-agent"),
        ("h2c", "HELLO\t1"),
        ("c2h", "HELLO\tOK\t1"),
        ("c2h", native.NATIVE_CAPS),
        ("h2c", "TAKE"),
        ("c2h", "TAKE\tQUEUED"),
        ("c2h", "TAKE\tCOMMAND_SENT"),
        ("c2h", "TAKE\tREADY"),
        ("c2h", f"READY\t{ready}"),
    )
    return [
        frame_record(seq, direction, payload)
        for seq, (direction, payload) in enumerate(payloads)
    ]


def observation_frames(
    revision: int, rows: Sequence[str],
) -> list[dict[str, Any]]:
    """``OBS_OPEN`` through the last ``PAGE_END`` for one whole bundle.

    This is a protocol encoder, not a capture.  What it pins is the framing and
    paging arithmetic a port has to get exactly right -- the 4-byte header, the
    8192-byte cap, the percent-encoding, the 16-rows-per-page walk, the
    ``s{rev}-{n}`` snapshot token, and the offset/next_offset accounting.  What
    the C client actually emits is not knowable from a savegame.
    """
    snapshot = f"s{revision}-{FRAME_SNAPSHOT_SERIAL}"
    total = len(rows)
    frames: list[dict[str, Any]] = []
    frames.append(frame_record(
        0, "h2c", f"OBS_OPEN\t{FRAME_REQUEST_ID}\tstate",
    ))
    frames.append(frame_record(
        1, "c2h",
        f"OBS_OPENED\t{FRAME_REQUEST_ID}\t{snapshot}\t{revision}\t{total}",
    ))
    seq = 2
    for offset in range(0, total, native.MAX_OBSERVATION_PAGE):
        count = min(native.MAX_OBSERVATION_PAGE, total - offset)
        frames.append(frame_record(
            seq, "h2c",
            f"OBS_PAGE\t{FRAME_REQUEST_ID}\t{snapshot}\t{offset}"
            f"\t{native.MAX_OBSERVATION_PAGE}",
        ))
        seq += 1
        frames.append(frame_record(
            seq, "c2h",
            f"PAGE_BEGIN\t{FRAME_REQUEST_ID}\t{snapshot}\t{revision}"
            f"\t{offset}\t{count}\t{total}",
        ))
        seq += 1
        for index in range(offset, offset + count):
            encoded = native._percent_encode(rows[index])
            if len(rows[index].encode("utf-8")) > native.MAX_OBSERVATION_ROW_BYTES:
                raise CorpusError(
                    f"corpus_record: row {index} exceeds "
                    f"MAX_OBSERVATION_ROW_BYTES; the bundle is not emittable",
                )
            frames.append(frame_record(
                seq, "c2h",
                f"ROW\t{FRAME_REQUEST_ID}\t{snapshot}\t{index}\t{encoded}",
                note="row", row_index=index,
            ))
            seq += 1
        frames.append(frame_record(
            seq, "c2h",
            f"PAGE_END\t{FRAME_REQUEST_ID}\t{snapshot}\t{offset + count}",
        ))
        seq += 1
    return frames


def async_frames(revision: int, turn: int) -> list[dict[str, Any]]:
    """The two notifications that may interleave anywhere after CAPS.

    Interleaving is precisely what ``_demultiplex_locked`` and ``_wait_message``
    exist to get right, so a port needs exemplars of both shapes with their
    exact field counts (2 and 10) and the invariants the 10-field one carries.
    """
    safe_turn = max(int(turn), 1)
    return [
        frame_record(0, "c2h", f"STATE_AVAILABLE\t{revision}"),
        frame_record(
            1, "c2h",
            "PHASE_AVAILABLE\t{rev}\t{turn}\t1\tplayers_alternate\t2"
            "\t1\t1\t0\t0".format(rev=revision, turn=safe_turn),
        ),
        frame_record(
            2, "c2h",
            f"PHASE_AVAILABLE\t{revision + 1}\t{safe_turn}\t0"
            f"\tconcurrent\t1\t1\t1\t0\t0",
        ),
    ]


# --------------------------------------------------------------------------
# Opaque id map
# --------------------------------------------------------------------------


def walk_strings(value: Any) -> Iterator[str]:
    """Depth-first over a JSON value, keys sorted, yielding every string.

    Sorted keys make the traversal -- and therefore the slot numbering -- a
    function of the content rather than of dict insertion order.
    """
    if isinstance(value, str):
        yield value
    elif isinstance(value, Mapping):
        for key in sorted(value):
            yield str(key)
            yield from walk_strings(value[key])
    elif isinstance(value, (list, tuple)):
        for item in value:
            yield from walk_strings(item)


def build_idmap(pages: Sequence[Any]) -> dict[str, Any]:
    """Map every opaque id to a stable symbolic slot, both directions.

    Pages are stored raw because a port that reimplements ``_mac`` can
    reproduce the ids exactly, and that is a genuinely valuable parity target.
    A port that legitimately chooses a different id scheme still needs the
    corpus to be diffable, which is what this is for: slots are assigned by
    first appearance in the canonical traversal order, so the map is itself
    deterministic.
    """
    forward: dict[str, str] = {}
    counters: dict[str, int] = {}
    for page in pages:
        for text in walk_strings(page):
            if text in forward:
                continue
            if CURSOR_ID_RE.fullmatch(text) is not None:
                prefix = "cursor"
            else:
                match = OPAQUE_ID_RE.fullmatch(text)
                if match is None:
                    continue
                prefix = match.group(1)
            slot = counters.get(prefix, 0)
            counters[prefix] = slot + 1
            forward[text] = f"{prefix}#{slot}"
    return {
        "forward": forward,
        "reverse": {slot: opaque for opaque, slot in forward.items()},
        "prefixes": sorted(counters),
    }


# --------------------------------------------------------------------------
# Run directory reading (strictly read-only)
# --------------------------------------------------------------------------


# A ``SaveRef`` dataclass lived here and was never constructed: the save
# inventory is written straight into ``index['source']['saves']`` as plain
# dicts by :func:`record_game`.  Dead scaffolding in a module whose job is to
# be trusted is a claim that something is modelled when it is not.


@dataclass(frozen=True)
class RunSource:
    """Everything a run directory contributes, read without touching it."""

    run_dir: Path
    game_id: str
    manifest: dict[str, Any]
    saves: tuple[Path, ...]
    player_name: str


def _relative_to_repo(path: Path) -> str:
    try:
        return path.resolve().relative_to(REPO_ROOT).as_posix()
    except ValueError:
        return path.resolve().as_posix()


def _manifest_facts(run_dir: Path) -> tuple[dict[str, Any], str]:
    """Provenance the corpus quotes, plus the seat's player name for READY.

    A missing or unparseable manifest is not fatal: a save is still a save, and
    a run whose manifest was never written (a supervisor killed mid-bootstrap)
    is exactly the interrupted shape worth recording.
    """
    path = run_dir / "manifest.json"
    if not path.is_file():
        return ({"present": False}, "AgentPlace1")
    try:
        with open(path, "rb") as handle:
            document = json.loads(handle.read().decode("utf-8"))
    except (OSError, ValueError, UnicodeDecodeError):
        return ({"present": True, "readable": False}, "AgentPlace1")
    if not isinstance(document, Mapping):
        return ({"present": True, "readable": False}, "AgentPlace1")
    config = document.get("config")
    config = config if isinstance(config, Mapping) else {}
    seats_raw = config.get("seats")
    seats: list[dict[str, Any]] = []
    if isinstance(seats_raw, Sequence) and not isinstance(seats_raw, (str, bytes)):
        for seat in seats_raw:
            if not isinstance(seat, Mapping):
                continue
            seats.append({
                "id": seat.get("id"),
                "name": seat.get("name"),
                "type": seat.get("type"),
                "controller_label": seat.get("controller_label"),
                "controller_fingerprint": seat.get("controller_fingerprint"),
            })
    places_raw = document.get("resolved_places")
    player_name = "AgentPlace1"
    if isinstance(places_raw, Sequence) and not isinstance(places_raw, (str, bytes)):
        for place in places_raw:
            if isinstance(place, Mapping) and place.get("controller") == "agent":
                candidate = place.get("player_name")
                if isinstance(candidate, str):
                    player_name = safe_player_name(candidate)
                break
    seeds_raw = config.get("seeds")
    seeds = [
        int(item) for item in seeds_raw
        if isinstance(item, int) and not isinstance(item, bool)
    ] if isinstance(seeds_raw, Sequence) and not isinstance(seeds_raw, (str, bytes)) else []
    facts: dict[str, Any] = {
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
    return (facts, player_name)


def read_run_source(run_dir: Path) -> RunSource:
    """Open a run directory read-only and inventory its autosaves."""
    resolved = run_dir.resolve()
    if not resolved.is_dir():
        raise CorpusError(f"corpus_record: not a directory: {run_dir}")
    saves = harvest.save_paths(resolved.parent).get(resolved.name, [])
    facts, player_name = _manifest_facts(resolved)
    return RunSource(
        run_dir=resolved,
        game_id=resolved.name,
        manifest=facts,
        saves=tuple(saves),
        player_name=player_name,
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
    """Choose which saves to record, and say what was skipped and why.

    Explicit ``--turn`` wins over sampling.  The tail save of a run whose mtime
    is younger than ``live_margin_s`` is excluded unless named explicitly: it is
    the file most likely to be half-written right now, and skipping it costs one
    turn of coverage while removing the whole race.
    """
    ordered = list(saves)
    skipped: list[dict[str, str]] = []
    if turns:
        wanted = set(int(turn) for turn in turns)
        chosen = [path for path in ordered if save_turn(path) in wanted]
        return (chosen, skipped)
    candidates = list(ordered)
    if candidates and live_margin_s > 0:
        tail = candidates[-1]
        moment = _stdlib_time.time() if now is None else now
        try:
            age = moment - tail.stat().st_mtime
        except OSError:
            age = live_margin_s + 1.0
        if age < live_margin_s:
            skipped.append({
                "path": tail.name,
                "reason": "live_margin",
            })
            candidates = candidates[:-1]
    chosen = harvest._spread(candidates, per_game)
    if candidates and candidates[-1] not in chosen:
        chosen.append(candidates[-1])
    return (chosen, skipped)


# --------------------------------------------------------------------------
# Recording one sample
# --------------------------------------------------------------------------


@dataclass
class RecordedSample:
    """One (turn, player) empire, fully rendered into corpus files."""

    key: str
    turn: int
    player_index: int
    save_name: str
    rows: tuple[str, ...]
    row_digest: str
    files: dict[str, bytes] = field(default_factory=dict)
    page_paths: list[str] = field(default_factory=list)
    frame_paths: list[str] = field(default_factory=list)
    #: Every frame verb this sample's traces contain, for the gap declaration.
    frame_verbs: list[str] = field(default_factory=list)
    notes: list[dict[str, Any]] = field(default_factory=list)


def _row_digest(rows: Sequence[str]) -> str:
    return sha256_hex(b"\x00".join(row.encode("ascii") for row in rows))


def _drain(
    control: v2_control.V2SeatControl, first: Mapping[str, Any], endpoint: str,
    follow: bool,
) -> list[dict[str, Any]]:
    """Walk a cursor chain to its end, or stop at page 1.

    Everything past page 1 is the page-chain planner, which is the subtlest
    thing in ``v2_control``; a corpus that stops at page 1 does not cover it.
    """
    pages = [dict(first)]
    if not follow:
        return pages
    guard = 0
    cursor = first.get("page", {}).get("next_cursor")
    while isinstance(cursor, str) and cursor:
        guard += 1
        # The projector's own ceiling, not an arbitrary number: a chain longer
        # than v2_control can legitimately plan is a cycle, i.e. a projector
        # bug, and the recorder should say so rather than spin.
        if guard > v2_control.MAX_CURSOR_CHAIN_PAGES:
            raise CorpusError(
                "corpus_record: a cursor chain exceeded "
                f"MAX_CURSOR_CHAIN_PAGES ({v2_control.MAX_CURSOR_CHAIN_PAGES}); "
                "the page chain does not terminate",
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
    sample: RecordedSample, relpath: str, payload: bytes, *, kind: str,
) -> None:
    if not is_safe_relpath(relpath):
        raise CorpusError(f"corpus_record: unsafe corpus path {relpath!r}")
    if not is_corpus_relpath(relpath):
        raise CorpusError(
            f"corpus_record: {relpath!r} is not a corpus path shape; the "
            "pruner in write_corpus would never reclaim it",
        )
    if relpath in sample.files:
        raise CorpusError(f"corpus_record: duplicate corpus path {relpath!r}")
    sample.files[relpath] = payload
    if kind == "page":
        sample.page_paths.append(relpath)
    elif kind == "frames":
        sample.frame_paths.append(relpath)


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
    want_frames: bool,
    player_name: str,
    case_label: str | None = None,
    case_mutator: Any = None,
) -> RecordedSample:
    """Render one harvested empire into every artifact the corpus keeps.

    A fresh ``V2SeatControl`` per sample is deliberate: ``_snapshot`` refuses a
    non-increasing ``native_revision`` on a seat, so sharing one across samples
    would make the corpus depend on the order the saves happened to be walked.
    """
    key = f"t{corpus_sample.turn:04d}-p{player_index}"
    case = harvest.case_from_player(
        corpus_sample, player_index, label=case_label,
    )
    if case_mutator is not None:
        case = case_mutator(case)
    rows = fixtures.build_rows(case)
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

    ordered_pages: list[Any] = []
    with deterministic_entropy(sample_seed(seed, key)):
        control = v2_control.V2SeatControl(
            game_id, CORPUS_AGENT_ID, FIXTURE_GENERATION,
        )
        direct = [name for name in sections if name in DIRECT_SECTIONS]
        city_scoped = [name for name in sections if name in CITY_SCOPED_SECTIONS]
        rendered: dict[str, list[dict[str, Any]]] = {}
        for section in direct:
            chain = _drain(
                control, control.state_page(observation, section, limit),
                "state", follow_cursors,
            )
            rendered[f"state/{section}.l{limit:02d}"] = chain
        # A second, smaller limit on the two sections most likely to overflow,
        # so the size planner is exercised even when the empire is small.
        for section in ("cities", "known_tiles"):
            if section not in direct or page_limit >= limit:
                continue
            chain = _drain(
                control, control.state_page(observation, section, page_limit),
                "state", follow_cursors,
            )
            rendered[f"state/{section}.l{page_limit:02d}"] = chain

        city_id = None
        cities_chain = rendered.get(f"state/cities.l{limit:02d}")
        if cities_chain:
            city_id = _first_id(cities_chain[0], ("id", "city_id"))
        for section in city_scoped:
            if city_id is None:
                sample.notes.append({
                    "kind": "scope_unavailable",
                    "section": section,
                    "detail": "the bundle has no city to scope on",
                })
                continue
            chain = _drain(
                control,
                control.state_page(observation, section, limit, actor_id=city_id),
                "state", follow_cursors,
            )
            rendered[f"state/{section}.l{limit:02d}.a0"] = chain

        tiles_chain = rendered.get(f"state/known_tiles.l{limit:02d}")
        tile_id = _first_id(tiles_chain[0], ("id", "tile_id")) if tiles_chain else None
        if "tile_window" in sections:
            if tile_id is None:
                sample.notes.append({
                    "kind": "scope_unavailable",
                    "section": "tile_window",
                    "detail": "the bundle has no known tile to centre on",
                })
            else:
                chain = _drain(
                    control,
                    control.state_page(
                        observation, "tile_window", limit,
                        center_id=tile_id, radius=1,
                    ),
                    "state", follow_cursors,
                )
                rendered[f"state/tile_window.l{limit:02d}.c0.r1"] = chain

        relation_id = None
        diplomacy_chain = rendered.get(f"state/diplomacy.l{limit:02d}")
        if diplomacy_chain:
            relation_id = _first_id(diplomacy_chain[0], ("relation_id", "id"))
        if "diplomacy_clauses" in sections:
            if relation_id is None:
                sample.notes.append({
                    "kind": "scope_unavailable",
                    "section": "diplomacy_clauses",
                    "detail": "the bundle has no contacted player",
                })
            else:
                chain = _drain(
                    control,
                    control.state_page(
                        observation, "diplomacy_clauses", limit,
                        relation_id=relation_id,
                    ),
                    "state", follow_cursors,
                )
                rendered[f"state/diplomacy_clauses.l{limit:02d}.k0"] = chain

        rendered[f"legal_actions/l{limit:02d}"] = _drain(
            control, control.legal_actions_page(observation, limit),
            "legal_actions", follow_cursors,
        )
        if page_limit < limit:
            rendered[f"legal_actions/l{page_limit:02d}"] = _drain(
                control, control.legal_actions_page(observation, page_limit),
                "legal_actions", follow_cursors,
            )

    for name in UNREACHABLE_SECTIONS:
        if name in sections:
            sample.notes.append({
                "kind": "unreachable_section",
                "section": name,
                "detail": (
                    "state_page cannot serve this section; the supervisor "
                    "routes it through prepare_state_scope/"
                    "materialize_state_scope, which needs a live native round "
                    "trip. v2_control raises a bare KeyError here rather than "
                    "a typed V2ControlError -- latent, since the supervisor "
                    "never routes it this way."
                ),
            })

    rows_path = f"bundles/{key}.rows.jsonl"
    _emit(sample, rows_path, canonical_lines(rows), kind="bundle")
    _emit(sample, f"bundles/{key}.bundle.json", canonical_json({
        "generation": FIXTURE_GENERATION,
        "native_revision": FIXTURE_NATIVE_REVISION,
        "revision_source": "fixture-constant",
        "row_count": len(rows),
        "row_digest": sample.row_digest,
        "rows_file": rows_path,
    }), kind="bundle")

    for name in sorted(rendered):
        chain = rendered[name]
        ordered_pages.extend(chain)
        if len(chain) == 1:
            _emit(
                sample, f"pages/{name.replace('/', f'/{key}.', 1)}.json",
                canonical_json(chain[0]), kind="page",
            )
        else:
            _emit(
                sample, f"pages/{name.replace('/', f'/{key}.', 1)}.chain.jsonl",
                canonical_lines(chain), kind="page",
            )

    if want_frames:
        traces = {
            "handshake": handshake_frames(player_name),
            "observation": observation_frames(FIXTURE_NATIVE_REVISION, rows),
            "async": async_frames(FIXTURE_NATIVE_REVISION, sample.turn),
        }
        for name in sorted(traces):
            _emit(
                sample, f"frames/{key}.{name}.jsonl",
                canonical_lines(traces[name]), kind="frames",
            )
        sample.frame_verbs = frame_verbs(
            record for trace in traces.values() for record in trace
        )

    _emit(
        sample, f"ids/{key}.idmap.json",
        canonical_json(build_idmap(ordered_pages)), kind="ids",
    )
    return sample


# --------------------------------------------------------------------------
# Pins: the constants a port has to agree on
# --------------------------------------------------------------------------


def corpus_pins() -> dict[str, Any]:
    """Read from the modules, never retyped, so a pin cannot go stale."""
    return {
        "full_control_schema_version": 2,
        "control_protocol": "full-control-v2",
        "native_observation_action_schema_id":
            v2_control.NATIVE_OBSERVATION_ACTION_SCHEMA_ID,
        "native_protocol_version": native.NATIVE_PROTOCOL_VERSION,
        "native_capabilities": list(native.NATIVE_CAPABILITIES),
        "native_encoding": native.NATIVE_ENCODING,
        "max_frame": native.MAX_FRAME,
        "max_page_items": v2_control.MAX_PAGE_ITEMS,
        "max_observation_page": native.MAX_OBSERVATION_PAGE,
        "max_observation_row_bytes": native.MAX_OBSERVATION_ROW_BYTES,
        "max_native_row_bytes": v2_control.MAX_NATIVE_ROW_BYTES,
        "max_bundled_rows": v2_control.MAX_BUNDLED_ROWS,
        "cursor_ttl_seconds": v2_control.CURSOR_TTL_SECONDS,
        "row_field_schema_digest": sha256_hex(canonical_json({
            kind: list(order)
            for kind, order in sorted(v2_control._ROW_FIELDS.items())
        })),
    }


def resolve_sections(spec: str) -> tuple[str, ...]:
    """``all`` | ``direct`` | ``scoped`` | an explicit comma list."""
    scoped = CITY_SCOPED_SECTIONS + ("tile_window", "diplomacy_clauses")
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


# --------------------------------------------------------------------------
# Recording one game
# --------------------------------------------------------------------------


def render_manifest(files: Mapping[str, bytes]) -> bytes:
    """The ``sha256sum -c`` listing of the emitted files, sorted by path.

    ``index.json`` cannot appear (it quotes these digests) and neither can the
    manifest itself, so the manifest is *not* self-certifying -- which is why
    :func:`record_game` puts its digest in ``index['files']``.
    """
    return b"".join(
        f"{sha256_hex(files[path])}  {path}\n".encode("utf-8")
        for path in sorted(files)
    )


def parse_manifest(payload: bytes) -> dict[str, str]:
    """The inverse of :func:`render_manifest`; raises on a malformed line."""
    listing: dict[str, str] = {}
    for line in payload.decode("utf-8").splitlines():
        digest, separator, relpath = line.partition("  ")
        if not separator or len(digest) != 64 or not relpath:
            raise CorpusError(f"corpus_record: malformed manifest line {line!r}")
        listing[relpath] = digest
    return listing


@dataclass
class GameCorpus:
    game_id: str
    files: dict[str, bytes]
    index: dict[str, Any]

    def rendered(self) -> dict[str, bytes]:
        """Every byte the corpus directory holds, keyed by relative path."""
        payload = dict(self.files)
        payload["MANIFEST.sha256"] = render_manifest(self.files)
        payload["index.json"] = pretty_json(self.index)
        return payload


def _file_sha256(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    with open(path, "rb") as handle:
        while True:
            chunk = handle.read(1 << 20)
            if not chunk:
                break
            digest.update(chunk)
            size += len(chunk)
    return (digest.hexdigest(), size)


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
    want_frames: bool,
    live_margin_s: float,
) -> GameCorpus:
    """Record every selected (turn, player) of one run directory."""
    game_id = (
        source.game_id
        if v2_control._OPAQUE_OWNER.fullmatch(source.game_id) is not None
        else FALLBACK_GAME_ID
    )
    chosen, skipped = select_saves(
        source.saves, per_game=per_game, turns=turns,
        live_margin_s=live_margin_s,
    )
    files: dict[str, bytes] = {}
    sample_index: list[dict[str, Any]] = []
    save_index: list[dict[str, Any]] = []
    notes: list[dict[str, Any]] = []
    seen_keys: set[str] = set()
    seen_verbs: set[str] = set()

    for path in chosen:
        loaded = harvest.load_sample(path)
        if loaded is None:
            skipped.append({"path": path.name, "reason": "unstable_or_unparsable"})
            continue
        digest, size = _file_sha256(path)
        save_index.append({
            "path": f"saves/{path.name}",
            "turn": loaded.turn,
            "sha256": digest,
            "size": size,
        })
        wanted = (
            [index for index in players if 0 <= index < len(loaded.players)]
            if players else
            [
                player.index for player in loaded.players
                if player.cities
            ]
        )
        if not wanted:
            notes.append({
                "kind": "no_recordable_player",
                "turn": loaded.turn,
                "detail": "no player in this save owns a city",
            })
            continue
        for player_index in wanted:
            sample = record_sample(
                loaded, player_index,
                game_id=game_id,
                seed=seed,
                sections=sections,
                limit=limit,
                page_limit=page_limit,
                follow_cursors=follow_cursors,
                want_frames=want_frames,
                player_name=source.player_name,
            )
            if sample.key in seen_keys:
                raise CorpusError(
                    f"corpus_record: duplicate sample key {sample.key!r}; "
                    "two saves claim the same turn",
                )
            seen_keys.add(sample.key)
            seen_verbs.update(sample.frame_verbs)
            for relpath, payload in sample.files.items():
                files[relpath] = payload
            sample_index.append({
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
                    "idmap": f"ids/{sample.key}.idmap.json",
                    "pages": sorted(sample.page_paths),
                    "frames": sorted(sample.frame_paths),
                },
                "notes": sample.notes,
            })

    files["gaps.json"] = canonical_json({
        "corpus_gaps": dict(harvest.CORPUS_GAPS),
        "recorder_mode": "offline-synthesised",
        "unreachable_sections": list(UNREACHABLE_SECTIONS),
        "frames": (
            "synthesised-from-bundle: a protocol-faithful encoding of the "
            "bundle using headless_sidecar's own constants, not a capture. "
            "No IPC frame exists anywhere on disk in any run directory."
        ),
        "frame_directions": {
            "c2h": (
                "client-to-host: the C freeciv-agent client -> the Python "
                "sidecar host (headless_sidecar._bootstrap reads these)."
            ),
            "h2c": (
                "host-to-client: the Python sidecar host -> the C client "
                "(headless_sidecar._bootstrap writes these)."
            ),
            "detail": (
                "The legend is here because the frame files carry only the "
                "two-letter code, and a port that reads it backwards inverts "
                "every replay it builds from this corpus."
            ),
        },
        "frame_verbs": {
            "recorded": sorted(seen_verbs),
            "advertised_but_never_framed": [
                capability for capability in native.NATIVE_CAPABILITIES
                if capability not in seen_verbs
            ],
            "detail": (
                "The recorded CAPS frame advertises every capability in "
                "headless_sidecar.NATIVE_CAPABILITIES, but this corpus frames "
                "only the handshake, one OBS_OPEN..PAGE_END walk and the two "
                "async notifications. The action verbs (ACT, ACT_CAP, "
                "ACT_RELATION_CAP, TARGET_ACTION) and the scope verbs "
                "(SCOPE_OPEN/SCOPE_PAGE and the STATE_SCOPE_*/RELATION_SCOPE_* "
                "families) appear nowhere except inside the CAPS payload "
                "string and index.json's pins block -- there is no frame "
                "exemplar for either path. That matters most for scope: "
                "unreachable_sections above says the supervisor reaches those "
                "sections through prepare_state_scope/materialize_state_scope, "
                "i.e. through exactly the mechanism this corpus does not "
                "frame. A port gets no framing oracle for them from here."
            ),
        },
        "frame_traces": (
            "Each of the per-sample frame files restarts seq at 0 and carries "
            "no cross-file ordering: they are independent shape exemplars, not "
            "one replayable session. In particular no fixture shows "
            "STATE_AVAILABLE or PHASE_AVAILABLE interleaved into the "
            "OBS_OPEN..PAGE_END walk, even though that interleaving is what "
            "headless_sidecar._demultiplex_locked and _wait_message exist to "
            "get right. Treat these as framing and arithmetic exemplars; they "
            "are not a session replay."
        ),
        "notes": notes,
    })

    file_hashes = {
        path: {"sha256": sha256_hex(payload), "bytes": len(payload)}
        for path, payload in sorted(files.items())
    }
    # MANIFEST.sha256 is a rendered file like any other, so it belongs inside
    # index.json's digest set and inside corpus_digest.  At schema 1 it was
    # outside both, which meant `verify` could not see a tampered manifest and
    # still printed "N files verified".
    manifest_payload = render_manifest(files)
    file_hashes["MANIFEST.sha256"] = {
        "sha256": sha256_hex(manifest_payload),
        "bytes": len(manifest_payload),
    }
    file_hashes = dict(sorted(file_hashes.items()))
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
                "frames": want_frames,
                "per_game": per_game,
                "turns": list(turns),
                "players": list(players),
                # Recorded so `verify --strict` can reproduce the *selection*
                # and not merely the rendering.  A replay pins it to 0 would
                # silently include a save the original excluded.
                "live_margin_s": live_margin_s,
            },
        },
        "pins": corpus_pins(),
        "source": {
            "run_dir": _relative_to_repo(source.run_dir),
            "manifest": source.manifest,
            "player_name": source.player_name,
            "saves_available": len(source.saves),
            "saves": save_index,
            "skipped": skipped,
        },
        "samples": sample_index,
        "files": file_hashes,
        "corpus_digest": sha256_hex(canonical_json({
            path: entry["sha256"] for path, entry in file_hashes.items()
        })),
    }
    return GameCorpus(game_id=game_id, files=files, index=index)


# --------------------------------------------------------------------------
# Writing
# --------------------------------------------------------------------------


def run_dir_markers(path: Path) -> list[str]:
    """Which run-directory markers a path carries, in report order.

    A run directory is ``saves/`` plus a ``manifest.json`` the supervisor
    writes; a run killed mid-bootstrap has only the first.  Either alone is
    enough to refuse, because the cost of a false positive is "pick another
    ``--out``" and the cost of a false negative is a deleted savegame series.
    """
    markers: list[str] = []
    if (path / "saves").is_dir():
        markers.append("saves/")
    if (path / "manifest.json").is_file():
        markers.append("manifest.json")
    return markers


def holds_savegames(path: Path) -> bool:
    """True when a directory is a run directory with something to lose.

    Stricter than :func:`run_dir_markers` on purpose.  The markers test guards
    the directory the recorder is about to *prune*, where any doubt should be
    fatal; this one is applied to ancestors and to ``--out``'s children, where
    an unrelated ``manifest.json`` is a plausible thing to meet and refusing on
    it would be a false alarm.  A ``saves/`` with neither a manifest nor an
    autosave in it holds nothing a recording could destroy.
    """
    saves = path / "saves"
    if not saves.is_dir():
        return False
    if (path / "manifest.json").is_file():
        return True
    try:
        return any(
            harvest.SAVE_NAME.match(child.name) is not None
            for child in saves.iterdir()
        )
    except OSError:
        return True


def looks_like_runs_root(path: Path) -> bool:
    """True when a directory's children are run directories."""
    try:
        children = sorted(path.iterdir())
    except OSError:
        return False
    return any(child.is_dir() and holds_savegames(child) for child in children)


def assert_safe_target(target: Path, out_root: Path) -> None:
    """Refuse to write where writing would destroy a run directory.

    :func:`write_corpus` prunes; a pruner pointed at ``.agent-eval/runs`` would
    unlink a run's savegames and its manifest, which is the only durable
    substrate this corpus is built from and may belong to a game that is
    running right now.  The module docstring promised output never lands inside
    a run directory; this is the promise made enforceable.
    """
    resolved = target.resolve()
    out_resolved = out_root.resolve()
    markers = run_dir_markers(resolved)
    if markers:
        raise CorpusError(
            f"corpus_record: refusing to write a corpus into {resolved}: it "
            f"holds {' and '.join(markers)}, so it is a run directory, not a "
            "corpus directory. Writing here would delete savegames. Point "
            f"--out at a sibling of runs/, e.g. {DEFAULT_OUT_ROOT}.",
            exit_code=UNSAFE_TARGET_EXIT_CODE,
        )
    for ancestor in resolved.parents:
        if holds_savegames(ancestor):
            raise CorpusError(
                f"corpus_record: refusing to write a corpus into {resolved}: "
                f"its ancestor {ancestor} is a run directory holding "
                "savegames. All output must go outside a run directory.",
                exit_code=UNSAFE_TARGET_EXIT_CODE,
            )
    runs_root = DEFAULT_RUNS_ROOT.resolve()
    if out_resolved == runs_root or runs_root in out_resolved.parents:
        raise CorpusError(
            f"corpus_record: refusing to write a corpus into {out_resolved}: "
            f"it is inside the runs root {runs_root}.",
            exit_code=UNSAFE_TARGET_EXIT_CODE,
        )
    if looks_like_runs_root(out_resolved):
        raise CorpusError(
            f"corpus_record: refusing to write a corpus into {out_resolved}: "
            "it holds run directories, so --out is a runs root. A recording "
            "prunes files it did not write; here that is a game's savegames.",
            exit_code=UNSAFE_TARGET_EXIT_CODE,
        )


def corpus_file_sizes(target: Path) -> dict[str, int]:
    """Relative path -> size for every file under ``target``; no bytes read."""
    sizes: dict[str, int] = {}
    for path in sorted(target.rglob("*")):
        if path.is_file():
            sizes[path.relative_to(target).as_posix()] = path.stat().st_size
    return sizes


def differs_from_disk(target: Path, payload: Mapping[str, bytes]) -> bool:
    """Whether ``target`` already holds a corpus unequal to ``payload``.

    Names first, then sizes, then digests, and the digests are streamed one
    file at a time.  The previous form read every file under the target into
    memory before it could refuse -- pointed at a run directory with a thousand
    savegames, that was a multi-gigabyte read to reach a refusal.

    provenance.json carries the clock, so it differs by construction on every
    run; comparing it here would turn ``--stamp`` into a permanent ``--force``
    requirement.
    """
    on_disk = {
        relpath: size for relpath, size in corpus_file_sizes(target).items()
        if relpath != "provenance.json"
    }
    if not on_disk:
        return False
    if set(on_disk) != set(payload):
        return True
    if any(on_disk[relpath] != len(payload[relpath]) for relpath in on_disk):
        return True
    return any(
        _file_sha256(target / relpath)[0] != sha256_hex(payload[relpath])
        for relpath in sorted(on_disk)
    )


def write_corpus(
    corpus: GameCorpus, out_root: Path, *, force: bool,
) -> tuple[Path, int]:
    """Write one game's corpus, refusing to clobber a differing one.

    ``--force`` overrides the collision check and nothing else: it is not a
    licence to prune a directory that is not a corpus, which is what
    :func:`assert_safe_target` and the path-shape scoping below exist to say.
    """
    target = out_root / corpus.game_id
    assert_safe_target(target, out_root)
    payload = corpus.rendered()
    if target.exists() and not force and differs_from_disk(target, payload):
        raise CorpusError(
            f"corpus_record: {target} already holds a different corpus; "
            "pass --force to overwrite",
            exit_code=3,
        )
    written = 0
    for relpath in sorted(payload):
        path = target / relpath
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload[relpath])
        written += len(payload[relpath])
    # A file left over from an earlier, differently-configured recording would
    # be quietly served as part of this corpus and would not appear in
    # index.json, so it goes.  A stale stamp goes with it: a provenance file
    # describing a recording that no longer exists is worse than none.
    #
    # Only paths of the recorder's own shapes are eligible.  A file this tool
    # could not have written is not this tool's to delete -- and it does not
    # get to hide either: it fails the collision check without --force, and
    # `verify` reports it as unexpected.
    stale = [
        path for path in sorted(target.rglob("*"))
        if path.is_file()
        and is_corpus_relpath(path.relative_to(target).as_posix())
        and path.relative_to(target).as_posix() not in payload
    ]
    for path in stale:
        path.unlink()
    return (target, written)


def read_corpus_files(target: Path) -> dict[str, bytes]:
    return {
        path.relative_to(target).as_posix(): path.read_bytes()
        for path in sorted(target.rglob("*")) if path.is_file()
    }


# --------------------------------------------------------------------------
# Self-test: the whole determinism guarantee, in two renders
# --------------------------------------------------------------------------


def selftest() -> dict[str, Any]:
    """Prove the entropy swap is load-bearing before recording anything.

    Two renders with the same seed must agree byte for byte; a render with a
    different seed must not.  Cheap, and it is the entire guarantee -- without
    the second assertion a swap that silently stopped working would still look
    green.
    """
    case = fixtures.case("corpus-selftest")
    observation = fixtures.observation(case)

    def render(seed: bytes) -> bytes:
        with deterministic_entropy(seed):
            control = v2_control.V2SeatControl(
                FALLBACK_GAME_ID, CORPUS_AGENT_ID, FIXTURE_GENERATION,
            )
            first = control.state_page(observation, "cities", 1)
            pages = _drain(control, first, "state", True)
            pages.append(control.legal_actions_page(observation, 4))
            return canonical_json(pages)

    baseline = render(b"\x00" * 32)
    repeat = render(b"\x00" * 32)
    other = render(b"\x11" * 32)
    if baseline != repeat:
        raise CorpusError(
            "corpus_record: determinism self-test failed -- two renders with "
            "the same seed disagree",
        )
    if baseline == other:
        raise CorpusError(
            "corpus_record: determinism self-test failed -- the seed is not "
            "load-bearing, so the entropy swap is not in effect",
        )
    return {
        "stable": True,
        "seed_sensitive": True,
        "digest": sha256_hex(baseline),
    }


# --------------------------------------------------------------------------
# Subcommands
# --------------------------------------------------------------------------


def _seed_bytes(text: str) -> bytes:
    try:
        raw = bytes.fromhex(text)
    except ValueError as exc:
        raise CorpusError(f"corpus_record: --entropy-seed is not hex: {text!r}") from exc
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
        path for path in sorted(root.iterdir())
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
        if not source.saves:
            continue
        corpora.append(record_game(
            source,
            seed=seed,
            sections=sections,
            per_game=args.per_game,
            turns=tuple(args.turn or ()),
            players=tuple(args.player or ()),
            limit=args.limit,
            page_limit=args.page_limit,
            follow_cursors=args.follow_cursors,
            want_frames=args.frames,
            live_margin_s=args.live_margin,
        ))
    return corpora


def command_record(args: argparse.Namespace, stream: Any) -> int:
    checks = selftest()
    print(
        f"determinism self-test ok (digest {checks['digest'][:16]})",
        file=stream,
    )
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
        if args.dry_run:
            # A dry run is what a careful operator does first, so it has to
            # deliver the same refusal the real run would rather than print a
            # reassuring summary of a recording that could never be written.
            assert_safe_target(out_root / corpus.game_id, out_root)
            print(
                f"{corpus.game_id}: {len(corpus.index['samples'])} samples, "
                f"{len(corpus.rendered())} files (dry run)",
                file=stream,
            )
            continue
        target, written = write_corpus(corpus, out_root, force=args.force)
        if args.stamp:
            _write_stamp(target)
        print(
            f"{corpus.game_id}: {len(corpus.index['samples'])} samples, "
            f"{len(corpus.rendered())} files, {written} bytes -> {target} "
            f"[{corpus.index['corpus_digest'][:16]}]",
            file=stream,
        )
    return 0


def _write_stamp(target: Path) -> None:
    """The environment stamp, deliberately outside every digest and every diff.

    ``recorded_at``/``python``/``git_commit`` are useful forensics and are
    exactly the fields that would break the byte-identity claim, so they live in
    their own opt-in file rather than in ``index.json``.
    """
    (target / "provenance.json").write_bytes(pretty_json({
        "recorded_at": _stdlib_time.strftime(
            "%Y-%m-%dT%H:%M:%SZ", _stdlib_time.gmtime(),
        ),
        "python": sys.version.split()[0],
        "platform": sys.platform,
        "note": (
            "Not hash-covered and not part of the byte-identity guarantee. "
            "Written only with --stamp."
        ),
    }))


def command_verify(args: argparse.Namespace, stream: Any) -> int:
    target = Path(args.corpus)
    index_path = target / "index.json"
    if not index_path.is_file():
        raise CorpusError(f"corpus_record: no index.json under {target}")
    index = json.loads(index_path.read_bytes().decode("utf-8"))
    failures: list[str] = []
    schema = index.get("schema_version")
    if schema != CORPUS_SCHEMA_VERSION:
        # Not pedantry: at schema 1 neither MANIFEST.sha256 nor an injected
        # extra file was covered by anything, so verifying such a corpus with
        # this code would report checks it did not actually perform.  Return
        # here rather than adding to the pile -- every later check is defined
        # against this version's layout, and their complaints would read as
        # findings about the corpus rather than about the version skew.
        print(
            f"FAIL schema_version {schema!r}; this recorder verifies "
            f"{CORPUS_SCHEMA_VERSION} only -- re-record the corpus",
            file=stream,
        )
        return 1
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
    # The file *set*, which no digest in index.json can speak for: an injected
    # file is invisible to a per-entry walk of the index, and index.json is
    # self-consistent about its own contents by construction.
    permitted = set(indexed) | VERIFY_ALLOWLIST
    failures.extend(
        f"unexpected {relpath}"
        for relpath in sorted(set(corpus_file_sizes(target)) - permitted)
    )
    # MANIFEST.sha256 is hash-covered above; this says the two views of the
    # corpus also agree with each other, so neither can be rewritten alone.
    manifest_path = target / "MANIFEST.sha256"
    if manifest_path.is_file():
        try:
            listing = parse_manifest(manifest_path.read_bytes())
        except (CorpusError, UnicodeDecodeError) as exc:
            failures.append(f"unreadable MANIFEST.sha256: {exc}")
        else:
            expected = {
                relpath: entry.get("sha256")
                for relpath, entry in indexed.items()
                if relpath != "MANIFEST.sha256"
            }
            if listing != expected:
                drift = sorted(
                    set(listing) ^ set(expected)
                    | {
                        relpath for relpath in set(listing) & set(expected)
                        if listing[relpath] != expected[relpath]
                    },
                )
                failures.append(
                    "MANIFEST.sha256 disagrees with index.json on "
                    f"{', '.join(drift[:5])}",
                )
    recomputed = sha256_hex(canonical_json({
        path: entry["sha256"] for path, entry in sorted(indexed.items())
    }))
    if recomputed != index.get("corpus_digest"):
        failures.append("corpus_digest mismatch")
    for line in failures:
        print(f"FAIL {line}", file=stream)
    if failures:
        return 1
    print(
        f"{index.get('game_id')}: {len(index.get('files', {}))} files verified "
        f"[{index.get('corpus_digest', '')[:16]}]",
        file=stream,
    )
    if not args.strict:
        return 0
    return _verify_strict(target, index, stream)


def strict_replay_args(index: Mapping[str, Any], run_dir: Path) -> argparse.Namespace:
    """Reconstruct the exact ``record`` invocation an index was written by.

    Every knob is taken from ``generated_by.settings`` verbatim rather than
    inferred from the recorded samples: inferring the turn list from the samples
    silently drops the turns that produced *no* sample, which changes the save
    inventory and the gap notes and would make a faithful re-derivation look
    like a mismatch.
    """
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
        frames=bool(settings.get("frames", True)),
        live_margin=float(settings.get("live_margin_s", 120.0)),
    )


def _verify_strict(target: Path, index: Mapping[str, Any], stream: Any) -> int:
    """Re-derive the corpus and demand byte identity with what is on disk.

    The comparison is against ``target``, not against the re-derivation's own
    in-memory bytes -- comparing a rebuild to itself proves nothing, and is the
    exact shape of vacuous test this gate exists to prevent.  The re-derivation
    is written through :func:`write_corpus` into a temp directory first so the
    writer is on trial too, not just the renderer.
    """
    source = index.get("source", {})
    run_dir = REPO_ROOT / str(source.get("run_dir", ""))
    if not (run_dir / "saves").is_dir():
        # This is the ordinary case on any machine but the recording one --
        # .agent-eval/runs is gitignored and gets pruned -- so the message says
        # exactly what the non-strict pass did and did not establish rather
        # than implying the whole gate ran.
        print(
            f"SKIP strict: the source run dir {run_dir} is gone, so the "
            "corpus cannot be re-derived here. Already checked: every indexed "
            "digest, MANIFEST.sha256 itself, and that the tree holds no file "
            "index.json does not name. Not checked: that these bytes are what "
            "the projector produces -- for that, compare corpus_digest against "
            "a recording made where the run dir lives.",
            file=stream,
        )
        return 0
    rebuilt = build_corpora(strict_replay_args(index, run_dir))
    if len(rebuilt) != 1:
        print(f"FAIL strict: re-derivation produced {len(rebuilt)} corpora", file=stream)
        return 1

    # A live game keeps writing saves, so an unpinned corpus of one samples a
    # different turn series every time it is recorded.  That is the *input*
    # moving, not the projector disagreeing, and reporting it as a byte
    # mismatch would train a reader to ignore the one signal this gate exists
    # to give.  Name it, and say how to make it verifiable.
    # ``source`` is exactly the block of facts about the run directory -- its
    # manifest, its save inventory, the digests of the saves that were read and
    # the ones that were skipped.  Nothing in it depends on the projector, so
    # any inequality here means the directory moved and nothing else.
    derived_source = rebuilt[0].index.get("source", {})
    if source != derived_source:
        recorded_saves = {
            str(item.get("path")): str(item.get("sha256"))
            for item in source.get("saves", [])
        }
        derived_saves = {
            str(item.get("path")): str(item.get("sha256"))
            for item in derived_source.get("saves", [])
        }
        drift = sorted(set(recorded_saves) ^ set(derived_saves)) or [
            f"saves_available {source.get('saves_available')} -> "
            f"{derived_source.get('saves_available')}",
        ]
        print(
            "SKIP strict: the source run advanced since this corpus was "
            f"recorded ({', '.join(drift[:3])}). The recorded digests still "
            "verified. A run that is still being written cannot be strictly "
            "verified after the fact; record it from a finished run, or "
            "compare corpus_digest values recorded in one pass.",
            file=stream,
        )
        return 0

    with tempfile.TemporaryDirectory(prefix="corpus-verify-") as scratch:
        written, _ = write_corpus(rebuilt[0], Path(scratch), force=True)
        derived = read_corpus_files(written)
    on_disk = {
        path: payload for path, payload in read_corpus_files(target).items()
        # Written only with --stamp, deliberately outside every digest and
        # outside the byte-identity guarantee because it carries the clock.
        if path != "provenance.json"
    }
    differing = sorted(
        set(derived) ^ set(on_disk)
        | {
            path for path in set(derived) & set(on_disk)
            if derived[path] != on_disk[path]
        }
    )
    for path in differing:
        print(f"FAIL strict: {path} differs from its re-derivation", file=stream)
    if differing:
        return 1
    print(
        f"strict: {len(on_disk)} files re-derived byte-identically",
        file=stream,
    )
    return 0


def command_list(args: argparse.Namespace, stream: Any) -> int:
    root = Path(args.runs_root)
    if not root.is_dir():
        raise CorpusError(f"corpus_record: no runs root at {root}")
    rows: list[dict[str, Any]] = []
    now = _stdlib_time.time()
    for game_id, saves in harvest.save_paths(root).items():
        facts, _name = _manifest_facts(root / game_id)
        try:
            age = now - saves[-1].stat().st_mtime
        except OSError:
            age = float("inf")
        rows.append({
            "game_id": game_id,
            "saves": len(saves),
            "first_turn": save_turn(saves[0]),
            "last_turn": save_turn(saves[-1]),
            "control_protocol": facts.get("control_protocol"),
            "finished": facts.get("finished"),
            "looks_live": age < args.live_margin,
        })
    if args.json:
        print(canonical_json(rows).decode("utf-8"), file=stream)
        return 0
    for row in rows:
        print(
            f"{row['game_id']}  saves={row['saves']:<5} "
            f"turns={row['first_turn']}..{row['last_turn']:<5} "
            f"protocol={row['control_protocol'] or '-':<17} "
            f"finished={str(row['finished']):<5} "
            f"live={'yes' if row['looks_live'] else 'no'}",
            file=stream,
        )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python3 -m agent_eval.corpus_record",
        description=(
            "Record a byte-reproducible golden corpus of v2 observation pages "
            "and IPC frame traces from this machine's own autosaves."
        ),
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    record = subparsers.add_parser(
        "record", help="record a corpus from run directories (read-only)",
    )
    record.add_argument("--run-dir", action="append", default=[],
                        help="a .agent-eval/runs/<game_id> directory; repeatable")
    record.add_argument("--runs-root", default=str(DEFAULT_RUNS_ROOT))
    record.add_argument("--game", action="append", default=[],
                        help="filter --runs-root by game id; repeatable")
    record.add_argument(
        "--out", default=str(DEFAULT_OUT_ROOT),
        help=(
            "corpus root; must be outside every runs root, because a "
            "recording prunes files it did not write"
        ),
    )
    record.add_argument("--per-game", type=int, default=8,
                        help="evenly spaced saves per game; the last is always kept")
    record.add_argument("--turn", action="append", type=int, default=[],
                        help="record exactly these turns; overrides --per-game")
    record.add_argument("--player", action="append", type=int, default=[],
                        help="record exactly these player indexes")
    record.add_argument("--sections", default="all",
                        help="all | direct | scoped | a comma-separated list")
    record.add_argument("--limit", type=int, default=v2_control.MAX_PAGE_ITEMS)
    record.add_argument("--page-limit", type=int, default=4,
                        help="also record one chain at this smaller limit")
    record.add_argument("--follow-cursors", action=argparse.BooleanOptionalAction,
                        default=True)
    record.add_argument("--frames", action=argparse.BooleanOptionalAction,
                        default=True)
    record.add_argument("--entropy-seed", default=DEFAULT_ENTROPY_SEED)
    record.add_argument("--live-margin", type=float, default=120.0,
                        help="skip a run's tail save if it is younger than this")
    record.add_argument("--stamp", action="store_true",
                        help="also write provenance.json (breaks byte identity)")
    record.add_argument("--dry-run", action="store_true")
    record.add_argument("--force", action="store_true")

    verify = subparsers.add_parser(
        "verify", help="recompute every digest in a recorded corpus",
    )
    verify.add_argument("--corpus", required=True)
    verify.add_argument("--strict", action="store_true",
                        help="also re-derive the corpus and demand byte identity")

    listing = subparsers.add_parser("list", help="inventory run directories")
    listing.add_argument("--runs-root", default=str(DEFAULT_RUNS_ROOT))
    listing.add_argument("--live-margin", type=float, default=120.0)
    listing.add_argument("--json", action="store_true")

    return parser


def main(argv: Sequence[str] | None = None, stream: Any = None) -> int:
    out = sys.stdout if stream is None else stream
    args = build_parser().parse_args(list(argv) if argv is not None else None)
    handlers = {
        "record": command_record,
        "verify": command_verify,
        "list": command_list,
    }
    try:
        return handlers[args.command](args, out)
    except CorpusError as exc:
        print(str(exc), file=sys.stderr)
        return exc.exit_code
    except v2_control.V2ControlError as exc:
        print(
            f"corpus_record: a sample failed to project: {exc.code}. That is "
            "either a projector bug or a real shape nobody thought of.",
            file=sys.stderr,
        )
        return 1


__all__ = [
    "CITY_SCOPED_SECTIONS", "CORPUS_FORMAT", "CORPUS_SCHEMA_VERSION",
    "CorpusError", "DIRECT_SECTIONS", "GameCorpus", "RecordedSample",
    "RunSource", "TOOL_VERSION", "UNREACHABLE_SECTIONS", "assert_safe_target",
    "async_frames", "build_idmap", "build_parser", "canonical_json",
    "canonical_lines", "corpus_file_sizes", "corpus_pins",
    "deterministic_entropy", "differs_from_disk", "frame_record", "frame_verbs",
    "handshake_frames", "holds_savegames", "is_corpus_relpath", "is_safe_relpath",
    "looks_like_runs_root", "main", "observation_frames", "parse_manifest",
    "pretty_json", "read_run_source", "record_game", "record_sample",
    "render_manifest", "resolve_sections", "run_dir_markers", "sample_seed",
    "select_saves", "selftest", "sha256_hex", "walk_strings", "write_corpus",
]


if __name__ == "__main__":
    raise SystemExit(main())
