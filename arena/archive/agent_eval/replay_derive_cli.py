"""One-shot JSON bridge to the three savegame derivations.

**This module is an explicit interim layer and is replaced in phase 4.**  The
TypeScript replay gateway (``@arena/harness``) needs the same three
derivations the Python gateway calls through ``_default_replay_loader`` /
``_default_board_loader`` / ``_default_events_loader``
(``agent_eval/replay_gateway.py:262-320``), and reimplementing the savegame
parsers in TypeScript is phase 4 work.  Until then the TypeScript service
spawns this module once per derivation and reads one JSON document off stdout.
When phase 4 lands a native parser, delete this file: nothing in ``agent_eval``
imports it, and nothing ever should.

It is deliberately a *thin* wrapper.  It parses no savegame, applies no public
projection, invents no defaults beyond the ones the loaders already document,
and passes ``cache_root`` straight through so the on-disk cache discipline
(``save_replay._cache_directory``/``_cache_path``,
``game_events._events_cache_path``) is exactly the discipline the Python
gateway gets.  Two processes deriving the same game concurrently would race in
that cache, which is why the caller serializes its calls the way the gateway
serializes them behind ``server.replay_lock``
(``agent_eval/replay_gateway.py:1298``).

Usage::

    python3 -m agent_eval.replay_derive_cli --op replay \\
        --runs-root RUNS --game-id ID --cache-root CACHE \\
        [--after-turn N] [--limit N] [--complete]
    python3 -m agent_eval.replay_derive_cli --op board \\
        --runs-root RUNS --game-id ID --cache-root CACHE --turn N
    python3 -m agent_eval.replay_derive_cli --op events \\
        --runs-root RUNS --game-id ID --cache-root CACHE [--complete]

``resolved_places`` is read from **stdin** as a JSON array — the loaders take
it positionally and it can be large enough to be unpleasant in ``argv``.  An
empty stdin (or a tty) means ``[]``, which is the loaders' own default.

Contract with the caller, all three parts load-bearing:

* **stdout** on success is one canonical JSON document and nothing else:
  ``json.dumps(value, sort_keys=True, separators=(",", ":"),
  ensure_ascii=False)`` — byte-identical to the gateway's ``_canonical``
  (``agent_eval/replay_gateway.py:123``), with no trailing newline.
* **exit code** carries the classification the gateway's routes make from the
  loader exception (``replay_gateway.py:1712-1723`` and peers):
  ``EXIT_NOT_FOUND`` for ``FileNotFoundError`` (the routes' 404),
  ``EXIT_UNAVAILABLE`` for ``OSError``/``UnicodeError``/``JSONDecodeError``/
  ``ValueError`` and for a non-mapping result (the routes' 503),
  ``EXIT_USAGE`` for a bad invocation.
* **stderr** carries a diagnostic string.  It is *private*: the gateway's rule
  that loader exception text never reaches a response body
  (``test_events_loader_failures_stay_public``) is the caller's to keep, and
  the caller must not render it.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Mapping, Sequence
from typing import Any

#: Success: stdout holds the canonical JSON document.
EXIT_OK = 0
#: Bad invocation (argparse also exits with this).
EXIT_USAGE = 2
#: ``FileNotFoundError`` from the loader — the gateway's 404 branch.
EXIT_NOT_FOUND = 3
#: Any other public loader failure — the gateway's 503 branch.
EXIT_UNAVAILABLE = 4

#: The three derivations, spelled the way the gateway's routes spell them.
OPERATIONS = ("replay", "board", "events")


def _canonical(value: Any) -> str:
    """``replay_gateway._canonical`` as text: sorted keys, tight separators."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python3 -m agent_eval.replay_derive_cli",
        description="Derive one replay page, board snapshot, or event log as JSON.",
    )
    parser.add_argument("--op", required=True, choices=OPERATIONS)
    parser.add_argument("--runs-root", required=True)
    parser.add_argument("--game-id", required=True)
    parser.add_argument("--cache-root", required=True)
    parser.add_argument("--after-turn", type=int, default=0)
    parser.add_argument("--limit", type=int, default=250)
    parser.add_argument("--turn", type=int, default=None)
    parser.add_argument("--complete", action="store_true")
    return parser


def _read_places() -> list[Any]:
    """``resolved_places`` from stdin; an empty or interactive stdin means ``[]``."""
    if sys.stdin is None or sys.stdin.isatty():
        return []
    raw = sys.stdin.buffer.read()
    if not raw.strip():
        return []
    value = json.loads(raw.decode("utf-8"))
    if not isinstance(value, list):
        raise ValueError("resolved_places must be a JSON array")
    return value


def _derive(
    operation: str,
    runs_root: str,
    game_id: str,
    places: Sequence[Any],
    *,
    after_turn: int,
    limit: int,
    turn: int | None,
    cache_root: str,
    complete: bool,
) -> Any:
    """Call exactly the loader the gateway's default loaders call."""
    # Imported late for the same reason the gateway does it (`:265`): the
    # savegame reconstruction stack is optional, and importing it eagerly would
    # make an unrelated `--op events` invocation pay for it.
    if operation == "replay":
        from .save_replay import replay_from_autosaves

        return replay_from_autosaves(
            runs_root,
            game_id,
            places,
            after_turn=after_turn,
            limit=limit,
            cache_root=cache_root,
            complete=complete,
        )
    if operation == "board":
        from .save_replay import board_from_autosave

        # `turn` is required here, not defaulted: `_board_query` (`:1799`) makes
        # a missing `turn` a 400, so a board derivation with no turn is a caller
        # bug, never a request.
        return board_from_autosave(
            runs_root,
            game_id,
            places,
            turn=turn,
            cache_root=cache_root,
        )
    from .game_events import events_from_autosaves

    return events_from_autosaves(
        runs_root,
        game_id,
        places,
        cache_root=cache_root,
        complete=complete,
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    if args.op == "board" and args.turn is None:
        parser.error("--turn is required for --op board")

    try:
        places = _read_places()
    except (OSError, UnicodeError, ValueError) as exc:
        print(f"usage: resolved_places could not be read: {exc}", file=sys.stderr)
        return EXIT_USAGE

    try:
        value = _derive(
            args.op,
            args.runs_root,
            args.game_id,
            places,
            after_turn=args.after_turn,
            limit=args.limit,
            turn=args.turn,
            cache_root=args.cache_root,
            complete=args.complete,
        )
    except FileNotFoundError as exc:
        # `FileNotFoundError` is an `OSError`, so it has to be caught first —
        # exactly as the gateway's routes order their handlers (`:1712`).
        print(f"not-found: {exc}", file=sys.stderr)
        return EXIT_NOT_FOUND
    except (OSError, UnicodeError, json.JSONDecodeError, ValueError) as exc:
        # `ValueError` covers `SaveReplayError`; `JSONDecodeError` is a
        # `ValueError` too, and is named for the reader's benefit.
        print(f"unavailable: {exc}", file=sys.stderr)
        return EXIT_UNAVAILABLE

    if not isinstance(value, Mapping):
        print("unavailable: derivation did not return a mapping", file=sys.stderr)
        return EXIT_UNAVAILABLE

    try:
        rendered = _canonical(value)
    except (TypeError, ValueError, UnicodeError) as exc:
        print(f"unavailable: derivation is not serializable: {exc}", file=sys.stderr)
        return EXIT_UNAVAILABLE

    sys.stdout.write(rendered)
    sys.stdout.flush()
    return EXIT_OK


if __name__ == "__main__":  # pragma: no cover - exercised as a subprocess
    raise SystemExit(main())
