/** Board query parsing and the shared viewer-route pipeline. */

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
