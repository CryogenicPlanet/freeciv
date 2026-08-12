/**
 * `/v1/games/{id}/board.json` — one turn's map snapshot.
 *
 * `_board` (`agent_eval/replay_gateway.py:1819-1879`) is
 * {@link viewerJsonRoute} with the board loader in the hole, so the whole of
 * this module is `_board_query` (`:1800-1817`) plus the wiring.  The upstream
 * leg, the fallback matrix, the byte relay and the two size errors all live in
 * `./replay.ts`; nothing about them may be re-decided here.
 *
 * ## What makes this route different from its siblings
 *
 * 1. **`turn` is required.**  `_replay_query` has defaults for both of its
 *    parameters, so `replay.json` with no query is legal; `_board_query`
 *    demands `set(values) == {"turn"}` (`:1802`), so `board.json` with no query
 *    at all is a 400 `board query requires exactly one turn`.  A port that
 *    reaches for "optional with a default" gets a 200 where Python has a 400.
 * 2. **Three distinct 400s, in this order** — the shape of the query
 *    (`boardQueryTurn`), then `int()` (`boardTurnNotInteger`), then the range
 *    (`boardTurnNotPositive`).  `?turn=abc&extra=1` is the *first* message, not
 *    the second: the key set is tested before any value is parsed.
 * 3. **No `complete` flag.**  `_board` computes no state (`:1855`); the board
 *    loader takes `(runs_root, game_id, places, *, turn, cache_root)` and
 *    nothing else, so a terminal run and a live one derive identically.
 *
 * @module
 */

import { Either, Effect, Option } from 'effect';
import { BadRequest } from '../../errors.ts';
import { ReplayDerivation } from '../../services/derivation.ts';
import { parsePythonInt } from '../../services/upstream.ts';
import type { BoardJsonRoute } from '../dispatch.ts';
import {
  parseQuery,
  toLoaderInteger,
  type ViewerRouteEffect,
  viewerJsonRoute,
} from './replay.ts';

/** The one key `_board_query` accepts (`:1802`). */
export const BOARD_QUERY_KEY = 'turn';

/** A validated board query, plus the string that goes upstream. */
export interface BoardQuery {
  /** `> 0`, and unbounded above: Python's `int` has no ceiling. */
  readonly turn: bigint;
  /** `urlencode({"turn": turn})` (`:1817`) — the canonical `turn=N`. */
  readonly normalizedQuery: string;
}

/**
 * `_board_query` (`:1800-1817`).
 *
 * `set(values) != {"turn"}` is one test that covers three rejections — a
 * missing `turn`, an extra key, and (with the `len` test beside it) a repeated
 * `turn` — and all three answer with the same message.
 */
export const boardQuery = (query: string): Either.Either<BoardQuery, BadRequest> => {
  const values = parseQuery(query);
  const turns = values.get(BOARD_QUERY_KEY);
  if (values.size !== 1 || turns === undefined || turns.length !== 1) {
    return Either.left(new BadRequest({ problem: 'boardQueryTurn' }));
  }
  const turn = parsePythonInt(turns[0] ?? '');
  if (Option.isNone(turn)) {
    return Either.left(new BadRequest({ problem: 'boardTurnNotInteger' }));
  }
  if (turn.value <= 0n) {
    return Either.left(new BadRequest({ problem: 'boardTurnNotPositive' }));
  }
  return Either.right({ turn: turn.value, normalizedQuery: `${BOARD_QUERY_KEY}=${turn.value}` });
};

/**
 * `_board` (`:1819-1879`).
 *
 * The query parse precedes every byte of I/O, so each of the three 400s is a
 * promise that upstream was never opened.
 */
export const boardRoute = (route: BoardJsonRoute): ViewerRouteEffect =>
  Either.match(boardQuery(route.query), {
    onLeft: (problem): ViewerRouteEffect => Effect.fail(problem),
    onRight: (query) =>
      viewerJsonRoute({
        operation: 'board',
        gameId: route.gameId,
        upstreamPath: route.upstreamPath,
        upstreamQuery: query.normalizedQuery,
        derive: (context) =>
          Effect.flatMap(ReplayDerivation, (derivation) =>
            derivation.board({
              gameId: route.gameId,
              places: context.places,
              turn: toLoaderInteger(query.turn),
            }),
          ),
        // `_bounded_json(200, board)` (`:1879`): the loader's document, whole.
        project: (value) => value,
      }),
  });
