/**
 * `/v1/games` — `_games` (`:1600`), the index and its four branches.
 *
 * Bare `:NNN` citations are `agent_eval/replay_gateway.py`.
 *
 * ## Upstream first, disk second — and "disk" is not one rule
 *
 * The route opens the supervisor before touching the filesystem and reaches
 * for disk only when upstream *provably* has nothing.  The behavior dossier's
 * §4 matrix exists because a port that unifies the branches is wrong in three
 * separate places:
 *
 * | | upstream 2xx | 404 / 405 | offline | anything else |
 * |---|---|---|---|---|
 * | `/v1/games` | merge, **bounded**, re-serialized | raw disk index, **no relabel**, unbounded | disk rows **relabelled**, unbounded | problem |
 * | `status` / `result` (`./archive.ts`) | **relay bytes** | terminal archive | terminal archive (*identical*) | problem |
 *
 * Three consequences worth naming because each one is invisible until a
 * differential run catches it:
 *
 * 1. **`/v1/games` is the one JSON route whose 2xx is not relayed.**  Every
 *    other route hands the upstream's exact bytes back (`_send`, `:1898`);
 *    this one parses, merges the disk rows the upstream did not claim, and
 *    re-emits through the canonical writer.  An upstream body
 *    `{"schema_version": 1, "games": []}` comes back key-sorted and
 *    space-free — and every numeric literal comes back spelled the way
 *    `json.loads` read it, which is why the parse is `parsePythonJson` and not
 *    `JSON.parse` (`../../python-json.ts`).
 * 2. **Only one of the two disk branches relabels.**  Offline rewrites
 *    non-terminal runs as `interrupted` and drops the ones with no recorded
 *    turn; upstream-404/405 serves `_disk_games_index` *raw* (`:1650`), so
 *    lobby husks are visible and their `outcome.status` is `"pending"` — a
 *    value unreachable through any other branch (trap B3).
 * 3. **No route falls back on 5xx.**  A 500 from upstream is a 500 downstream
 *    carrying `upstream returned HTTP 500`, never masked by stale disk data
 *    (`test_games_500_never_masks_failure_with_disk_index`).  That is the one
 *    integrity property the whole matrix exists to protect, and it is the
 *    single most likely port bug: a `catch → serve from disk`.
 *
 * The archive half of that table lives in `./archive.ts`, which serves all
 * four JSON views rather than the two this module used to duplicate.
 *
 * ## What this module refuses to do
 *
 * It builds no responses.  The handler answers with a
 * {@link GatewayJsonPayload} — a status and bytes — or fails with a
 * `../../errors.ts` value that `../respond.ts` renders, once, at the router
 * edge.  Nothing here knows what a header is.
 *
 * @module
 */

import { type CanonRecord, type CanonValue, Gateway } from '@arena/wire';
import { Effect, Either, Option } from 'effect';
import { liveGameIds } from '../../archive.ts';
import { GATEWAY_GAMES_INDEX_PATH, isUpstreamFallbackStatus } from '../../constants.ts';
import {
  type ArchiveUnavailable,
  type GatewayError,
  gatewayErrorFromUpstream,
  type InternalError,
  UpstreamHttpError,
  UpstreamInvalid,
} from '../../errors.ts';
import { isCanonRecord, parsePythonJson } from '../../python-json.ts';
import { RunsRepository } from '../../services/runs.ts';
import { isUpstreamBody, UpstreamClient, type UpstreamJson } from '../../services/upstream.ts';
import { boundedGatewayJson, gatewayJson, type GatewayJsonPayload } from '../json.ts';

// ---------------------------------------------------------------------------
// The upstream leg
// ---------------------------------------------------------------------------

/**
 * `except UpstreamUnavailable` (`:1602`) as a value.
 *
 * {@link Option.none} is "the supervisor is provably gone" — a transport
 * failure or the three-condition Portless probe, and nothing else.  Every other
 * failure is mapped through `../../errors.ts#gatewayErrorFromUpstream` (the one
 * crossing between the client's vocabulary and the taxonomy's) and stays a
 * failure, which is what keeps a 5xx, a redirect and an oversized body off the
 * disk path.
 */
const upstreamOrOffline = (
  path: string,
): Effect.Effect<Option.Option<UpstreamJson>, GatewayError, UpstreamClient> =>
  Effect.flatMap(UpstreamClient, (upstream) =>
    upstream.jsonOrStatus({ path }).pipe(
      Effect.map(Option.some<UpstreamJson>),
      Effect.catchTag('UpstreamOffline', () => Effect.succeedNone),
      Effect.mapError(gatewayErrorFromUpstream),
    ),
  );

// ---------------------------------------------------------------------------
// /v1/games
// ---------------------------------------------------------------------------

/** `{"schema_version": 1, "games": [...]}` — the envelope all four branches share. */
const gamesEnvelope = (games: readonly CanonValue[]): CanonRecord => ({
  schema_version: BigInt(Gateway.ARCHIVE_SCHEMA_VERSION),
  games,
});

const UTF8_STRICT = new TextDecoder('utf-8', { fatal: true });

/**
 * `json.loads(body)["games"]`, with both of `_games`' 502s (`:1617`, `:1623`).
 *
 * Python raises `upstreamGamesIndexInvalid` for a body that is not UTF-8, is
 * not JSON, is not an object, or whose `games` is not a list — four conditions,
 * one message.  This is the **only** place a 2xx upstream body is inspected at
 * all; every other route relays bytes and therefore can never produce it
 * (trap B3).
 *
 * The reader is `parsePythonJson`, not `JSON.parse`: this is also the only JSON
 * route that **re-serializes** what it read (§4.1 property 1), so each numeric
 * literal has to come back out spelled the way `json.loads` read it.  An
 * upstream `"current_turn": 7` must re-emit as `7` and an upstream
 * `"current_turn": 7.0` as `7.0`, and a reader that collapsed the two would
 * get exactly one of them wrong with no assertion downstream able to see it.
 *
 * One deliberate narrowing: `json.loads(bytes)` runs CPython's
 * `detect_encoding`, so a UTF-16/32 body with a BOM parses there and is a 502
 * here.  A UTF-8 BOM is accepted by both (`TextDecoder` strips it).  The
 * supervisor writes `_canonical` output — UTF-8, no BOM — so the gap is
 * unreachable from any producer in this system.
 */
const upstreamGamesRows = (
  body: Uint8Array,
): Either.Either<readonly CanonValue[], UpstreamInvalid> =>
  Either.flatMap(
    Either.mapLeft(
      Either.try(() => UTF8_STRICT.decode(body)),
      () => new UpstreamInvalid(),
    ),
    (text) =>
      Either.flatMap(
        Either.mapLeft(parsePythonJson(text), () => new UpstreamInvalid()),
        (document) => {
          const rows = isCanonRecord(document) ? document['games'] : undefined;
          return Array.isArray(rows) ? Either.right(rows) : Either.left(new UpstreamInvalid());
        },
      ),
  );

/**
 * The upstream-2xx branch (`:1613-1639`) — merge, bound, re-serialize.
 *
 * Upstream rows come **first, in their own order**, then the disk rows the
 * upstream did not claim, in `_disk_games_index`'s `(created_at, game_id)`
 * descending order.  The merged list is never re-sorted (`:1633`), so a
 * response reads "live games, then archives".
 *
 * `live_ids` tolerates junk: a non-object element, or one whose `game_id` is
 * not a well-formed id, contributes nothing to the set **and is still relayed**
 * inside `games`.
 */
const mergedGamesIndex = (
  body: Uint8Array,
): Effect.Effect<
  GatewayJsonPayload,
  ArchiveUnavailable | InternalError | UpstreamInvalid,
  RunsRepository
> =>
  Effect.flatMap(upstreamGamesRows(body), (rows) =>
    Effect.flatMap(RunsRepository, (runs) =>
      Effect.flatMap(runs.diskRowsWithInterrupted(liveGameIds(rows)), (diskRows) =>
        boundedGatewayJson(gamesEnvelope([...rows, ...diskRows])),
      ),
    ),
  );

/**
 * `_games` (`:1600-1652`) — dispatch step 2, four branches, three serializers.
 *
 * The upstream request is always the bare `/v1/games`: the route rejected any
 * query with a 400 before reaching here (`:1980`), and none is ever forwarded.
 */
export type GamesIndexEffect = Effect.Effect<
  GatewayJsonPayload,
  GatewayError,
  RunsRepository | UpstreamClient
>;

export const gamesIndexRoute: GamesIndexEffect = Effect.flatMap(
  upstreamOrOffline(GATEWAY_GAMES_INDEX_PATH),
  Option.match({
    // Offline (`:1604`): the disk view **with** interrupted relabeling and an
    // empty `live_ids`, through `_json` — unbounded.
    onNone: (): GamesIndexEffect =>
      Effect.flatMap(RunsRepository, (runs) =>
        Effect.flatMap(runs.diskRowsWithInterrupted(new Set<string>()), (rows) =>
          gatewayJson(gamesEnvelope(rows)),
        ),
      ),
    onSome: (result): GamesIndexEffect =>
      isUpstreamBody(result)
        ? mergedGamesIndex(result.body)
        : isUpstreamFallbackStatus(result.status)
          ? // 404/405 (`:1650`): `_disk_games_index` **raw** — no relabeling,
            // lobby husks visible, `outcome.status` `"pending"`, unbounded.
            Effect.flatMap(RunsRepository, (runs) =>
              Effect.flatMap(runs.diskGamesIndex(), gatewayJson),
            )
          : // Everything else, 5xx included (`:1641`): relay the problem.
            Effect.fail(new UpstreamHttpError({ upstreamStatus: result.status })),
  }),
);

// ---------------------------------------------------------------------------
// What used to live here
// ---------------------------------------------------------------------------
//
// A second `_archive_json_route` (`:1887`) — `status` and `result` only, with
// its own `archiveFallback` and its own `ArchiveValueRoute` union — was
// implemented in this module and never wired: `../../server.ts` dispatches all
// four JSON archive views to `./archive.ts`, which serves `watch.json` and
// `frames` as well.  It agreed with the served copy, but nothing made it, and
// the ~15 parity assertions that exercised it were proving a property of code
// no request could reach.  Deleted; `test/gateway/routes-health-games.test.ts`
// now drives `./archive.ts#archiveJsonRoute` for the same matrix.
