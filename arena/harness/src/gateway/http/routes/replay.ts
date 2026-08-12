/**
 * `/v1/games/{id}/replay.json` — and the skeleton its two siblings share.
 *
 * Every bare `:NNN` cites `agent_eval/replay_gateway.py`.
 *
 * ## Why three route modules and one skeleton
 *
 * `_replay` (`:1655`), `_events` (`:1727`) and `_board` (`:1819`) are the same
 * seventeen lines three times over.  The behavior dossier §4.3 writes them as
 * one program with three holes in it:
 *
 * ```
 * <query parse>                              # differs; runs BEFORE any network I/O
 * status, body = upstream(path, normalized)  # 2xx -> relay the exact bytes
 * offline or 404/405 -> manifest -> loader   # differs: which loader, which kwargs
 * anything else      -> upstreamProblem(status)
 * _bounded_json(200, project(value))         # differs: events projects, the others do not
 * ```
 *
 * {@link viewerJsonRoute} is that program; `./board.ts` and `./events.ts` fill
 * the holes.  It lives here rather than in a fourth module because this file is
 * the family's namesake — the Python mutex is called `replay_lock`, the dossier
 * calls the family "the replay routes", and a route module that only forwards
 * to a helper is harder to read than one that owns it.
 *
 * ## The three things this module refuses to do
 *
 * 1. **It never builds an error response.**  Every refusal is a
 *    `Data.TaggedError` from `../../errors.ts`; the single response site at the
 *    router edge renders it (`:2038`).  The two functions here that produce
 *    bytes — {@link relayUpstreamJson} and `../json.ts#boundedJsonResponse` —
 *    produce *success* bytes.
 * 2. **It never re-serializes a 2xx from upstream.**  `_send(status, body, …)`
 *    (`:1666`, `:1742`, `:1830`) relays the bytes `_read_json_response`
 *    returned, so upstream key order, whitespace and float spelling survive.
 *    A `JSON.parse`/`stringify` round trip here would be invisible in a unit
 *    test and fatal in the parity rig.
 * 3. **It never merges the two 8 MiB errors.**  An oversized *upstream* body is
 *    502 `upstreamJsonTooLarge` and does **not** fall back to disk (§5.1); an
 *    oversized *locally built* body is 503 `archiveJsonTooLarge` (§5.2).
 *    Different status, different message, different subject.
 *
 * ## The one deliberate simplification
 *
 * Python's offline branch reads the manifest as
 * `_terminal_archive(...).manifest` and falls back to `_read_manifest` on **any**
 * `GatewayProblem` (`:1687-1693`).  `_terminal_archive` opens with
 * `_read_manifest` (`:774`) and returns *that same dict*, and every extra
 * failure it can add is a `GatewayProblem` that the `except` swallows — so the
 * manifest, and every error the request can end with, is identical to calling
 * `_read_manifest` alone.  Dossier §4.3 states the equivalence and permits the
 * collapse provided the swallow is kept; collapsing keeps it by construction
 * (there is nothing left to swallow) and costs one fewer `report.json` read.
 * The property the swallow protects — *an interrupted, non-terminal run still
 * serves its disk replay when upstream is gone* — is pinned by
 * `test/gateway/routes-replay.test.ts`.
 *
 * @module
 */

import {
  type CanonRecord,
  type CanonValue,
  Gateway,
  type GameId,
  isJsonObject,
  isTerminalRunState,
  type JsonObject,
  type JsonValue,
} from '@arena/wire';
import { HttpServerResponse } from '@effect/platform';
import { Effect, Either, Option } from 'effect';
import { manifestState } from '../../archive.ts';
import { isUpstreamFallbackStatus } from '../../constants.ts';
import {
  ArchiveUnavailable,
  type ArchiveUnavailableProblem,
  BadRequest,
  type GatewayError,
  gatewayErrorFromUpstream,
  NotFound,
  type NotFoundProblem,
  UpstreamHttpError,
} from '../../errors.ts';
import { publicPlaces, untrustedField } from '../../public.ts';
import {
  type DerivationError,
  type DerivationOperation,
  ReplayDerivation,
  type ResolvedPlaces,
} from '../../services/derivation.ts';
import { RunsRepository } from '../../services/runs.ts';
import {
  isUpstreamBody,
  parsePythonInt,
  UpstreamClient,
  type UpstreamJson,
} from '../../services/upstream.ts';
import type { ReplayJsonRoute } from '../dispatch.ts';
import { boundedJsonResponse } from '../json.ts';
import { gatewayJsonResponse } from '../respond.ts';

// ---------------------------------------------------------------------------
// urllib.parse.unquote / parse_qs
// ---------------------------------------------------------------------------

/**
 * CPython's `_asciire = re.compile('([\\x00-\\x7f]+)')`.
 *
 * `unquote` splits on it so that a percent escape can only ever be assembled
 * from bytes inside **one** ASCII run: `"%C3é%A9"` decodes as two separate
 * invalid sequences, not as `é`.  `String.prototype.split` with a capturing
 * group keeps the separators, which is exactly `re.split`'s shape, so the two
 * loops line up term for term.
 *
 * Spelled `\p{ASCII}` rather than as a `\x00-\x7f` range: the property escape
 * *is* U+0000-U+007F, and writing the range would put control characters into
 * a regular expression for no gain.
 */
const ASCII_RUN_RE = /(\p{ASCII}+)/u;

const HEX_PAIR_RE = /^[0-9A-Fa-f]{2}$/;

const PERCENT_BYTE = 0x25;

/** A run already known to be ASCII, as the bytes CPython would have. */
const asciiBytes = (run: string): readonly number[] =>
  [...run].map((character) => character.codePointAt(0) ?? 0);

/**
 * `unquote_to_bytes` (CPython `urllib/parse.py`): split on `%`, and a fragment
 * whose first two characters are **not** hex digits keeps its literal `%`.
 * `"%ZZ"` and `"%4"` therefore survive as text, which is what makes
 * `?turn=%zz` a `boardTurnNotInteger` rather than a decode error.
 */
const unquoteToBytes = (run: string): readonly number[] => {
  const parts = run.split('%');
  return [
    ...asciiBytes(parts[0] ?? ''),
    ...parts.slice(1).flatMap((part): readonly number[] => {
      const head = part.slice(0, 2);
      return HEX_PAIR_RE.test(head)
        ? [Number.parseInt(head, 16), ...asciiBytes(part.slice(2))]
        : [PERCENT_BYTE, ...asciiBytes(part)];
    }),
  ];
};

/**
 * `errors="replace"`.  A malformed byte sequence becomes U+FFFD instead of
 * raising, so no query string can produce a 500.
 *
 * WHATWG's decoder and CPython's both follow the Unicode "maximal subpart"
 * recommendation for how *many* replacement characters a truncated sequence
 * produces, so the two agree on everything reachable from a query string.
 */
const REPLACING_DECODER = new TextDecoder('utf-8', { fatal: false });

/** `urllib.parse.unquote(value, encoding="utf-8", errors="replace")`. */
export const pythonUnquote = (text: string): string =>
  text.includes('%')
    ? text
        .split(ASCII_RUN_RE)
        .map((bit, index) =>
          index % 2 === 1 ? REPLACING_DECODER.decode(Uint8Array.from(unquoteToBytes(bit))) : bit,
        )
        .join('')
    : text;

/** `+` is a space *before* the escapes are decoded, so `%2B` stays a plus. */
const decodeField = (raw: string): string => pythonUnquote(raw.replaceAll('+', ' '));

/**
 * `urllib.parse.parse_qs(query, keep_blank_values=True)` — the exact call all
 * three query parsers make (`:1556`, `:1801`).
 *
 * Four behaviors a port loses by reaching for `URLSearchParams`:
 *
 * - an empty field (`"a=1&&b=2"`) is **skipped**, not a blank key;
 * - a field with no `=` is a key with a blank value, because
 *   `keep_blank_values=True` (`URLSearchParams` agrees, `parse_qs`'s default
 *   does not — this is why the flag is written out);
 * - keys keep **first-appearance order** and repeats group under one key,
 *   which is how `len(items) != 1` detects a duplicate;
 * - `;` is not a separator (CPython ≥ 3.10), and neither is anything else.
 *
 * The values are percent-decoded (trap B4): `?turn=%205` reaches
 * {@link parsePythonInt} as `" 5"`, which is a valid `5`.
 */
export const parseQuery = (query: string): ReadonlyMap<string, readonly string[]> => {
  const pairs = query
    .split('&')
    .flatMap((field): readonly (readonly [string, string])[] => {
      if (field === '') return [];
      const split = field.indexOf('=');
      return [
        split === -1
          ? [decodeField(field), '']
          : [decodeField(field.slice(0, split)), decodeField(field.slice(split + 1))],
      ];
    });
  return new Map(
    [...Map.groupBy(pairs, ([name]) => name)].map(
      ([name, group]) => [name, group.map(([, value]) => value)] as const,
    ),
  );
};

// ---------------------------------------------------------------------------
// _replay_query (:1555) and its two integers
// ---------------------------------------------------------------------------

/** `after_turn`'s default (`:1566`). */
export const REPLAY_DEFAULT_AFTER_TURN = 0n;

/** `limit`'s default (`:1567`) and its ceiling (`:1571`). */
export const REPLAY_DEFAULT_LIMIT = 250n;

/** `not 1 <= limit <= 250` (`:1571`). */
export const REPLAY_LIMIT_RANGE = { minimum: 1n, maximum: 250n } as const;

/** The two keys `_replay_query` accepts, and nothing else (`:1557`). */
export const REPLAY_QUERY_KEYS = ['after_turn', 'limit'] as const;

/** A validated replay query, plus the string that goes upstream. */
export interface ReplayQuery {
  readonly afterTurn: bigint;
  readonly limit: bigint;
  /**
   * `urlencode({"after_turn": …, "limit": …})` (`:1576`) — **always both keys,
   * always this order**, whatever the client sent.  Both values are
   * non-negative decimals, so `quote_plus` escapes nothing.
   */
  readonly normalizedQuery: string;
}

/**
 * `_replay_query` (`:1555-1580`).
 *
 * `bigint`, because Python's `int` is unbounded and the normalized query has to
 * carry the digits the client sent: `?after_turn=99999999999999999999` is
 * forwarded verbatim, and a `number` would forward `1e+20`.
 */
export const replayQuery = (query: string): Either.Either<ReplayQuery, BadRequest> => {
  const values = parseQuery(query);
  const known: ReadonlySet<string> = new Set<string>(REPLAY_QUERY_KEYS);
  // `:1557` — one test, two ways to fail it: an unknown key, or any key at all
  // that appears more than once.
  if (
    [...values.keys()].some((name) => !known.has(name)) ||
    [...values.values()].some((items) => items.length !== 1)
  ) {
    return Either.left(new BadRequest({ problem: 'replayQueryDuplicates' }));
  }
  const afterTurn = Option.match(Option.fromNullable(values.get('after_turn')?.[0]), {
    onNone: () => Option.some(REPLAY_DEFAULT_AFTER_TURN),
    onSome: parsePythonInt,
  });
  const limit = Option.match(Option.fromNullable(values.get('limit')?.[0]), {
    onNone: () => Option.some(REPLAY_DEFAULT_LIMIT),
    onSome: parsePythonInt,
  });
  if (Option.isNone(afterTurn) || Option.isNone(limit)) {
    return Either.left(new BadRequest({ problem: 'replayQueryNotIntegers' }));
  }
  if (
    afterTurn.value < 0n ||
    limit.value < REPLAY_LIMIT_RANGE.minimum ||
    limit.value > REPLAY_LIMIT_RANGE.maximum
  ) {
    return Either.left(new BadRequest({ problem: 'replayQueryOutOfRange' }));
  }
  return Either.right({
    afterTurn: afterTurn.value,
    limit: limit.value,
    normalizedQuery: `after_turn=${afterTurn.value}&limit=${limit.value}`,
  });
};

/**
 * A Python `int` as the loaders' `number`.
 *
 * `ReplayDerivationInput.afterTurn` and `BoardDerivationInput.turn` are
 * `number`, and Python's are not bounded — so a query that passed validation
 * can still name a turn no double can spell.  Saturating is the least-lying
 * option available at this seam: the loaders compare `turn > after_turn` over
 * turns that fit in six digits on disk, so a saturated value selects the same
 * (empty) set of savegames as the exact one.  The single observable difference
 * is the `next_after_turn` echoed by a replay derived with
 * `after_turn > 2**53`, which no viewer requests.
 *
 * Reported with this port rather than papered over: the fix is a `bigint` in
 * `services/derivation.ts`'s input types.
 */
export const toLoaderInteger = (value: bigint): number =>
  value > BigInt(Number.MAX_SAFE_INTEGER) ? Number.MAX_SAFE_INTEGER : Number(value);

// ---------------------------------------------------------------------------
// The two failure translations
// ---------------------------------------------------------------------------

/** `except FileNotFoundError` (`:1712`, `:1784`, `:1866`) — one 404 per loader. */
const DERIVATION_NOT_FOUND: { readonly [K in DerivationOperation]: NotFoundProblem } = {
  replay: 'replayArtifactsNotFound',
  board: 'boardSnapshotNotFound',
  events: 'eventArtifactsNotFound',
};

/** `except (OSError, UnicodeError, JSONDecodeError, ValueError)` — one 503 per loader. */
const DERIVATION_UNAVAILABLE: { readonly [K in DerivationOperation]: ArchiveUnavailableProblem } = {
  replay: 'replayTelemetryUnavailable',
  board: 'boardSnapshotUnavailable',
  events: 'eventsUnavailable',
};

/**
 * A derivation failure as the taxonomy.
 *
 * The service already classifies (`derivationProblem`); this names the *class*
 * that carries the classification, which is what the renderer needs.  The two
 * tables are pinned to `derivationProblem`'s status and message in
 * `test/gateway/routes-replay.test.ts` for all six combinations, so neither
 * table can be edited alone.  The failure's `detail` is dropped here and never
 * reaches a body — `test_events_loader_failures_stay_public`.
 */
export const gatewayErrorFromDerivation = (error: DerivationError): GatewayError =>
  error._tag === 'DerivationArtifactsMissing'
    ? new NotFound({ problem: DERIVATION_NOT_FOUND[error.operation] })
    : new ArchiveUnavailable({
        problem: DERIVATION_UNAVAILABLE[error.operation],
        cause: error,
      });

// ---------------------------------------------------------------------------
// Bodies
// ---------------------------------------------------------------------------

/**
 * `_send`'s `Cache-Control` (`:1360`).
 *
 * `_send` passes `{"Cache-Control": "no-store"}` on **every** body it writes —
 * a relayed upstream 2xx included — so the header is not a property of the
 * problem path even though wire names the constant after it.  Aliased rather
 * than re-spelled: the string belongs to `@arena/wire`.
 */
const SEND_CACHE_CONTROL = Gateway.GATEWAY_PROBLEM_CACHE_CONTROL;

/**
 * `_send(status, body, "application/json; charset=utf-8")` (`:1666`, `:1742`,
 * `:1830`) — the upstream 2xx, byte for byte, under its own status.
 */
export const relayUpstreamJson = (
  status: number,
  body: Uint8Array,
): HttpServerResponse.HttpServerResponse =>
  gatewayJsonResponse({ status, body, cacheControl: SEND_CACHE_CONTROL });

// ---------------------------------------------------------------------------
// resolved_places, on the way to a loader
// ---------------------------------------------------------------------------

/**
 * A canonical value as plain JSON.
 *
 * `_public_places` produces Python `int`s for `place`; the port's
 * {@link publicPlaces} produces `bigint`, which is right for the canonical
 * writer and wrong for `JSON.stringify` — which throws on one.  The derivation
 * service ships `resolved_places` to its backend as JSON, so the bigints have
 * to come back down to numbers exactly once, here, at the seam.
 *
 * Only `place` is ever a `bigint` in a place row, and a place is a seat index;
 * a manifest claiming a seat beyond 2**53 loses precision, and nothing else
 * can.
 */
export const canonToJson = (value: CanonValue): JsonValue =>
  typeof value === 'bigint'
    ? Number(value)
    : Array.isArray(value)
      ? value.map(canonToJson)
      : typeof value === 'object' && value !== null
        ? Object.fromEntries(
            Object.entries(value).map(([key, entry]) => [key, canonToJson(entry)]),
          )
        : value;

/**
 * `_public_places(manifest.get("resolved_places"), manifest)` (`:1699`,
 * `:1773`, `:1856`) — the loaders' third positional argument.
 *
 * The projection is not optional on this path: `game_events` keys its cache on
 * a digest of exactly these rows, so handing the loader the raw manifest rows
 * would silently split the cache the Python gateway shares.
 */
export const derivationPlaces = (manifest: JsonObject): ResolvedPlaces =>
  publicPlaces(untrustedField(manifest, 'resolved_places'), manifest).flatMap((place) => {
    const json = canonToJson(place);
    // Total: `publicPlaces` returns records and `canonToJson` preserves shape,
    // so the empty arm is unreachable — and dropping a row rather than
    // inventing `{}` is the failure a loader could survive.
    return isJsonObject(json) ? [json] : [];
  });

// ---------------------------------------------------------------------------
// The shared skeleton
// ---------------------------------------------------------------------------

/** What the manifest contributes to a loader call. */
export interface DerivationContext {
  /** `_read_manifest`'s raw document. */
  readonly manifest: JsonObject;
  /** `_public_places(...)`, already projected. */
  readonly places: ResolvedPlaces;
  /**
   * `state in TERMINAL_STATES` computed from the **manifest**, never from
   * upstream (`:1700`, `:1774`).  `_board` computes no state (`:1855`) and its
   * spec ignores this field.
   */
  readonly complete: boolean;
}

/** The three holes in `_replay`/`_board`/`_events`. */
export interface ViewerRouteSpec {
  /** Which loader, and therefore which pair of failure messages. */
  readonly operation: DerivationOperation;
  readonly gameId: GameId;
  /** The dispatcher's canonically rebuilt `f"/v1/games/{id}/{name}"`. */
  readonly upstreamPath: string;
  /** The re-encoded query, `''` for `events.json`, which accepts none. */
  readonly upstreamQuery: string;
  readonly derive: (
    context: DerivationContext,
  ) => Effect.Effect<CanonRecord, DerivationError, ReplayDerivation>;
  /**
   * `_public_events` for `events.json` (`:1797`); identity for the other two.
   *
   * The identity arm is only sound because the derivation service hands back a
   * `CanonRecord` — a document whose integers are still `bigint` — rather than
   * a `JSON.parse` result.  A `JsonObject` here would spell every Python `int`
   * in a replay page as a float (`../../python-json.ts`).
   */
  readonly project: (value: CanonRecord) => CanonValue;
}

/** Every service a viewer route reaches for. */
export type ViewerRouteServices = UpstreamClient | RunsRepository | ReplayDerivation;

/** The effect a viewer route is. */
export type ViewerRouteEffect = Effect.Effect<
  HttpServerResponse.HttpServerResponse,
  GatewayError,
  ViewerRouteServices
>;

/**
 * The upstream leg.  `Option.none()` is `upstream_offline = True` (`:1662`) —
 * the *only* failure that selects disk; everything else is already the
 * taxonomy.
 */
const askUpstream = (
  spec: ViewerRouteSpec,
): Effect.Effect<Option.Option<UpstreamJson>, GatewayError, UpstreamClient> =>
  Effect.flatMap(UpstreamClient, (client) =>
    client
      .jsonOrStatus({ path: spec.upstreamPath, query: spec.upstreamQuery })
      .pipe(
        Effect.map(Option.some<UpstreamJson>),
        Effect.catchTag('UpstreamOffline', () => Effect.succeed(Option.none<UpstreamJson>())),
        Effect.mapError(gatewayErrorFromUpstream),
      ),
  );

/**
 * The disk leg: manifest, places, loader, canonical body.
 *
 * Identical for the offline and the 404/405 arms — see the module docstring for
 * why Python's two spellings collapse to one.
 */
const deriveFromDisk = (spec: ViewerRouteSpec): ViewerRouteEffect =>
  Effect.gen(function* () {
    const runs = yield* RunsRepository;
    const manifest = yield* runs.readManifest(spec.gameId);
    const derived = yield* Effect.mapError(
      spec.derive({
        manifest,
        places: derivationPlaces(manifest),
        complete: isTerminalRunState(manifestState(manifest)),
      }),
      gatewayErrorFromDerivation,
    );
    return yield* boundedJsonResponse(spec.project(derived));
  });

/**
 * `_replay`/`_board`/`_events` minus their query parsers — the fallback matrix
 * row for the viewer family (dossier §4.3), and nothing else.
 *
 * The order of the branches is the Python order, and it is load-bearing: the
 * 2xx relay precedes the status filter, and the status filter precedes the
 * fallback, so a 500 from upstream can never be answered with disk data
 * (`test_games_500_never_masks_failure_with_disk_index`'s promise, kept for
 * this family too).
 */
export const viewerJsonRoute = (spec: ViewerRouteSpec): ViewerRouteEffect =>
  Effect.flatMap(
    askUpstream(spec),
    Option.match({
      // `except UpstreamUnavailable` (`:1660`).
      onNone: () => deriveFromDisk(spec),
      onSome: (result): ViewerRouteEffect =>
        isUpstreamBody(result)
          ? Effect.succeed(relayUpstreamJson(result.status, result.body))
          : isUpstreamFallbackStatus(result.status)
            ? deriveFromDisk(spec)
            : Effect.fail(new UpstreamHttpError({ upstreamStatus: result.status })),
    }),
  );

// ---------------------------------------------------------------------------
// The route
// ---------------------------------------------------------------------------

/**
 * `_replay` (`:1655-1725`).
 *
 * The query parse runs first and, on failure, ends the request without opening
 * a socket — a 400 here is a promise that nothing was proxied.
 */
export const replayRoute = (route: ReplayJsonRoute): ViewerRouteEffect =>
  Either.match(replayQuery(route.query), {
    onLeft: (problem): ViewerRouteEffect => Effect.fail(problem),
    onRight: (query) =>
      viewerJsonRoute({
        operation: 'replay',
        gameId: route.gameId,
        upstreamPath: route.upstreamPath,
        upstreamQuery: query.normalizedQuery,
        derive: (context) =>
          Effect.flatMap(ReplayDerivation, (derivation) =>
            derivation.replay({
              gameId: route.gameId,
              places: context.places,
              afterTurn: toLoaderInteger(query.afterTurn),
              limit: toLoaderInteger(query.limit),
              complete: context.complete,
            }),
          ),
        // `_bounded_json(200, replay)` (`:1725`): the loader's document, whole.
        project: (value) => value,
      }),
  });
