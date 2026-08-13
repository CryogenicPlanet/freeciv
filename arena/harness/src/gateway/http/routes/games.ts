/**
 * `/v1/games`: offline uses interrupted disk rows, 404/405 uses raw disk rows, 2xx is decoded,
 * merged, and reserialized, and every other status is an error.
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
