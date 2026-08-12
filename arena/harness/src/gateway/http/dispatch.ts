/**
 * Pure ordered request dispatch. Body rejection precedes path parsing; game-id validation precedes
 * the query gate; viewer routes precede that gate. Paths are never percent-decoded.
 */

import { Array as Arr, Either, Option } from 'effect';
import {
  decodeFrameIndexFromPngName,
  FRAME_INDEX_RE,
  FrameIndex as FrameIndexBrand,
  type FrameIndex,
  type GameId,
  isGameId,
} from '@arena/wire';
import {
  GATEWAY_GAMES_INDEX_PATH,
  GATEWAY_GAMES_PREFIX,
  GATEWAY_GET_METHOD,
  GATEWAY_HEALTH_PATH,
  GATEWAY_ROUTE_SEGMENTS,
  type GatewayMethod,
} from '../constants.ts';
import { BadRequest, MethodNotAllowed, NotFound } from '../errors.ts';

// ---------------------------------------------------------------------------
// The request, as dispatch sees it
// ---------------------------------------------------------------------------

/**
 * What `_reject_body` (`:1384-1396`) concluded about the inbound entity.
 *
 * Three-valued rather than a `hasBody` boolean, because Python distinguishes
 * two rejections and a port that merges them loses a message:
 *
 * - `'invalid-content-length'` — `int(Content-Length)` raised (`:1390`).
 *   Python's `int()`, so `" 5 "`, `"+5"`, `"5_0"` and Unicode digits all
 *   *parse*; only what `int()` refuses lands here.
 * - `'present'` — `Transfer-Encoding` was present with **any** value, or
 *   `int(Content-Length) != 0` (`:1394`).  `Content-Length: 0` is accepted.
 * - `'absent'` — neither.
 *
 * Classifying headers is the socket edge's job (it owns the Python-`int`
 * parse); dispatch consumes the verdict so it stays pure.
 */
export type RequestBodySignal = 'absent' | 'present' | 'invalid-content-length';

// ---------------------------------------------------------------------------
// Problems
// ---------------------------------------------------------------------------

/**
 * Everything dispatch can refuse with — three of the nine classes in
 * `../errors.ts`, which is the *whole* taxonomy the one response site renders.
 *
 * Dispatch does not own an error type of its own, and must not: a second
 * `BadRequest` would be a second place a message could be worded, and the
 * viewer prints `payload.error` verbatim.  The narrower payloads are named
 * below so a reader can see, without opening the handlers, that a router 400 is
 * only ever one of five messages and a router 404 is only ever `notFound`.
 */
export type DispatchProblem = BadRequest | NotFound | MethodNotAllowed;

/** The five 400s reachable from dispatch. The query parsers own the other seven. */
export type DispatchBadRequestProblem =
  | 'invalidContentLength'
  | 'getRequestBody'
  | 'healthQuery'
  | 'gamesIndexQuery'
  | 'viewerRouteQuery';

/**
 * The router's own 404 (`:1993`, `:2037`).  Both sites write the identical
 * body — there is no "which one" on the wire, and inventing one would be a
 * divergence, so dispatch does not carry the distinction.
 */
export const DISPATCH_NOT_FOUND_PROBLEM = 'notFound';

const badRequest = (problem: DispatchBadRequestProblem): BadRequest => new BadRequest({ problem });

const notFound = (): NotFound => new NotFound({ problem: DISPATCH_NOT_FOUND_PROBLEM });

// ---------------------------------------------------------------------------
// Decisions
// ---------------------------------------------------------------------------

/** `/health` — `identity_payload()`, served locally, never proxied (`:1970`). */
export interface HealthRoute {
  readonly _tag: 'Health';
}

/** `/v1/games` — `_games()`, the four-branch index (`:1978`). */
export interface GamesIndexRoute {
  readonly _tag: 'GamesIndex';
}

/**
 * The half of a per-game decision every route family carries.
 *
 * `upstreamPath` is the path to concatenate onto the configured service URL —
 * **not** always a reconstruction of the route.  The archive families forward
 * `parsed.path` verbatim (`:2011`, `:2023`), so `/v1/games/{id}/status/`
 * proxies *with* its trailing slash; the viewer families rebuild an f-string
 * (`:1657`, `:1733`, `:1821`), so `/v1/games/{id}/replay.json/` proxies
 * *without* it.  Two rules in one dispatcher, and both are load-bearing.
 */
interface GameRoute {
  readonly gameId: GameId;
  readonly upstreamPath: string;
}

/**
 * `/v1/games/{id}/status`, and the bare `/v1/games/{id}` that aliases it
 * (`:2010`).
 *
 * `bareId` is the alias flag, and it changes what upstream sees: the bare form
 * proxies `/v1/games/{id}` *without* `/status`.
 */
export interface ArchiveStatusRoute extends GameRoute {
  readonly _tag: 'ArchiveStatus';
  readonly bareId: boolean;
}

/** `/v1/games/{id}/result` (`:2013`). */
export interface ArchiveResultRoute extends GameRoute {
  readonly _tag: 'ArchiveResult';
}

/** `/v1/games/{id}/watch.json` (`:2016`). */
export interface ArchiveWatchRoute extends GameRoute {
  readonly _tag: 'ArchiveWatch';
}

/** `/v1/games/{id}/frames` — the frame *listing*, JSON (`:2019`). */
export interface ArchiveFramesRoute extends GameRoute {
  readonly _tag: 'ArchiveFrames';
}

/**
 * A route whose raw query string the handler owns.  `query` is exactly
 * `parsed.query` — empty string means no query, and a bare trailing `?` is
 * therefore no query.  It is **not** what goes upstream: the handlers
 * re-encode it canonically (`?after_turn=N&limit=N`, `?turn=N`) before
 * forwarding.
 */
interface ViewerRoute extends GameRoute {
  readonly query: string;
}

/** `/v1/games/{id}/replay.json` (`:1996`). */
export interface ReplayJsonRoute extends ViewerRoute {
  readonly _tag: 'ReplayJson';
}

/** `/v1/games/{id}/board.json` (`:1999`). */
export interface BoardJsonRoute extends ViewerRoute {
  readonly _tag: 'BoardJson';
}

/** `/v1/games/{id}/events.json` (`:2002`) — the handler 400s on any query. */
export interface EventsJsonRoute extends ViewerRoute {
  readonly _tag: 'EventsJson';
}

/** `/v1/games/{id}/frames/{n}.png` (`:2028`), index already decoded. */
export interface FramePngRoute extends GameRoute {
  readonly _tag: 'FramePng';
  readonly index: FrameIndex;
}

/** `/v1/games/{id}/frames/latest.png` — `index=None` (`:2022`). */
export interface LatestFramePngRoute extends GameRoute {
  readonly _tag: 'LatestFramePng';
}

/** `/v1/games/{id}/video.mp4` — `video=True` (`:2025`). */
export interface VideoMp4Route extends GameRoute {
  readonly _tag: 'VideoMp4';
}

/** The four routes handled by `_archive_json_route` (`:1887`). */
export type ArchiveJsonRoute =
  | ArchiveStatusRoute
  | ArchiveResultRoute
  | ArchiveWatchRoute
  | ArchiveFramesRoute;

/** The three routes handled by `_replay`/`_board`/`_events`. */
export type ViewerJsonRoute = ReplayJsonRoute | BoardJsonRoute | EventsJsonRoute;

/** The three routes handled by `_archive_binary_route` (`:1935`). */
export type ArchiveBinaryRoute = FramePngRoute | LatestFramePngRoute | VideoMp4Route;

/** Every route `do_GET` can reach. Twelve, counting the bare-id alias as one. */
export type RouteDecision =
  | HealthRoute
  | GamesIndexRoute
  | ArchiveJsonRoute
  | ViewerJsonRoute
  | ArchiveBinaryRoute;

/** The `kind` argument `_archive_json_route` is called with (`:1887`). */
export type ArchiveJsonView = 'status' | 'result' | 'watch' | 'frames';

/** Archive-JSON route tag → the projection `_archive_json_route` selects. */
export const ARCHIVE_JSON_VIEWS = {
  ArchiveStatus: 'status',
  ArchiveResult: 'result',
  ArchiveWatch: 'watch',
  ArchiveFrames: 'frames',
} satisfies { readonly [T in ArchiveJsonRoute['_tag']]: ArchiveJsonView };

// ---------------------------------------------------------------------------
// Path normalization
// ---------------------------------------------------------------------------

/**
 * The stdlib's leading-slash collapse (CPython gh-87389,
 * `http.server.BaseHTTPRequestHandler.parse_request`): `if
 * path.startswith('//'): path = '/' + path.lstrip('/')`.
 *
 * So `//v1/games` **is** `/v1/games` — it serves the index, and the *upstream*
 * request carries the collapsed single-slash form.  Bun's `Request.url` keeps
 * the doubled slash, so a port that skips this 404s where Python 200s.
 *
 * Applied here to the path component, and idempotent, so the socket edge may
 * (and should) also apply it to the **raw request target** before URL parsing:
 * only there can `GET //evil.example/v1/games` be made to behave like Python,
 * which collapses first and then finds no `netloc` at all.
 */
export const collapseLeadingSlashes = (path: string): string =>
  path.startsWith('//') ? `/${path.replace(/^\/+/, '')}` : path;

/** `path.strip("/")` — Python strips **both** ends, which is why `/v1/games/{id}/status/` routes. */
const stripSlashes = (path: string): string => path.replace(/^\/+/, '').replace(/\/+$/, '');

const segmentsEqual = (suffix: ReadonlyArray<string>, expected: ReadonlyArray<string>): boolean =>
  suffix.length === expected.length && expected.every((segment, at) => suffix[at] === segment);

// ---------------------------------------------------------------------------
// The two suffix ladders, as data
// ---------------------------------------------------------------------------

/** Everything a suffix rule may look at. */
interface RouteContext {
  readonly gameId: GameId;
  /** `parts[3:]` — the segments after the game id (`:1995`). */
  readonly suffix: ReadonlyArray<string>;
  /** `parsed.path`, leading slashes collapsed: what the archive families forward verbatim. */
  readonly dispatchedPath: string;
  /** `parsed.query`, raw. */
  readonly query: string;
}

interface SuffixRule {
  readonly decide: (context: RouteContext) => Option.Option<RouteDecision>;
}

const onSuffix = (
  expected: ReadonlyArray<string>,
  build: (context: RouteContext) => RouteDecision,
): SuffixRule => ({
  decide: (context) =>
    segmentsEqual(context.suffix, expected) ? Option.some(build(context)) : Option.none(),
});

/** `f"/v1/games/{game_id}/{segment}"` — the viewer families' canonical rebuild. */
const viewerUpstreamPath = (gameId: GameId, segment: string): string =>
  `${GATEWAY_GAMES_INDEX_PATH}/${gameId}/${segment}`;

/**
 * An index `FRAME_INDEX_RE` accepts and no archive can hold.
 *
 * On disk a frame is `ARCHIVE_PNG_RE = ^([0-9]{6})\.png$` — six digits, so the
 * largest index that can ever be *listed* is `999999`.  Anything at or above
 * `2 ** 53 - 1` therefore misses every lookup by construction, which is exactly
 * what a ~310-digit index does in Python.
 */
const UNREACHABLE_FRAME_INDEX: FrameIndex = FrameIndexBrand.make(Number.MAX_SAFE_INTEGER);

/**
 * `int(suffix[1][:-4])` (`:2034`) — total, for every name the regex admits.
 *
 * Python's `int` is arbitrary-precision and its lookup is an equality against
 * the indices the archive listing produced, so `99999999999999999999999999.png`
 * routes to the frame handler and comes back `404 map frame does not exist`.
 * A JS `number` cannot hold that value, and the two obvious ports of this line
 * are both wrong:
 *
 * - **Reject it at the router.**  That is a `404 not found` — the same status
 *   with a different body, which byte parity reports as a divergence.
 * - **Hand it to `decodeFrameIndexFromPngName` anyway.**  Measured: that
 *   **throws**.  Wire's `FrameIndexFromPngName` is a `Schema.transform` whose
 *   decode calls `FrameIndex.make(Number(digits))`, and `Schema.int()` is
 *   `Number.isSafeInteger` — so `1e26` and `2 ** 53 + 1` fail the brand
 *   *inside* the transformation, where a thrown `ParseError` escapes the
 *   `Either` the "tolerant" decoder promises.  In the gateway that defect
 *   surfaced as a bare `500` with an empty body and none of the gateway's own
 *   headers, in every scenario (the parity matrix's `frame-index-overflow-500`
 *   finding).  `arena/wire` is upstream of this package and is not edited from
 *   here; the width test below keeps the throwing input away from it.
 *
 * So: names that fit a safe integer go through wire's decoder, which remains
 * the single definition of the grammar; the ones that do not are routed with
 * {@link UNREACHABLE_FRAME_INDEX}, which reaches `selectFramePng` and misses.
 * Same route, same status, same bytes as CPython.
 */
const frameIndexFromPngName = (name: string): Option.Option<FrameIndex> => {
  if (!FRAME_INDEX_RE.test(name)) return Option.none();
  const digits = name.slice(0, -4);
  return Option.some(
    Number.isSafeInteger(Number(digits))
      ? Option.getOrElse(
          Either.getRight(decodeFrameIndexFromPngName(name)),
          () => UNREACHABLE_FRAME_INDEX,
        )
      : UNREACHABLE_FRAME_INDEX,
  );
};

/**
 * `:1996-2004` — matched **before** the query gate, which is the only reason
 * any route may carry a query string.
 */
const PRE_QUERY_ROUTES: ReadonlyArray<SuffixRule> = [
  onSuffix([GATEWAY_ROUTE_SEGMENTS.replayJson], (context) => ({
    _tag: 'ReplayJson',
    gameId: context.gameId,
    upstreamPath: viewerUpstreamPath(context.gameId, GATEWAY_ROUTE_SEGMENTS.replayJson),
    query: context.query,
  })),
  onSuffix([GATEWAY_ROUTE_SEGMENTS.boardJson], (context) => ({
    _tag: 'BoardJson',
    gameId: context.gameId,
    upstreamPath: viewerUpstreamPath(context.gameId, GATEWAY_ROUTE_SEGMENTS.boardJson),
    query: context.query,
  })),
  onSuffix([GATEWAY_ROUTE_SEGMENTS.eventsJson], (context) => ({
    _tag: 'EventsJson',
    gameId: context.gameId,
    upstreamPath: viewerUpstreamPath(context.gameId, GATEWAY_ROUTE_SEGMENTS.eventsJson),
    query: context.query,
  })),
];

/**
 * `:2010-2035` — matched **after** the query gate, so every rung here is
 * unreachable with a non-empty query.
 */
const POST_QUERY_ROUTES: ReadonlyArray<SuffixRule> = [
  {
    decide: (context) =>
      context.suffix.length === 0 || segmentsEqual(context.suffix, [GATEWAY_ROUTE_SEGMENTS.status])
        ? Option.some({
            _tag: 'ArchiveStatus',
            gameId: context.gameId,
            upstreamPath: context.dispatchedPath,
            bareId: context.suffix.length === 0,
          } satisfies ArchiveStatusRoute)
        : Option.none(),
  },
  onSuffix([GATEWAY_ROUTE_SEGMENTS.result], (context) => ({
    _tag: 'ArchiveResult',
    gameId: context.gameId,
    upstreamPath: context.dispatchedPath,
  })),
  onSuffix([GATEWAY_ROUTE_SEGMENTS.watchJson], (context) => ({
    _tag: 'ArchiveWatch',
    gameId: context.gameId,
    upstreamPath: context.dispatchedPath,
  })),
  onSuffix([GATEWAY_ROUTE_SEGMENTS.frames], (context) => ({
    _tag: 'ArchiveFrames',
    gameId: context.gameId,
    upstreamPath: context.dispatchedPath,
  })),
  onSuffix(
    [GATEWAY_ROUTE_SEGMENTS.frames, GATEWAY_ROUTE_SEGMENTS.latestPng],
    (context) => ({
      _tag: 'LatestFramePng',
      gameId: context.gameId,
      upstreamPath: context.dispatchedPath,
    }),
  ),
  onSuffix([GATEWAY_ROUTE_SEGMENTS.videoMp4], (context) => ({
    _tag: 'VideoMp4',
    gameId: context.gameId,
    upstreamPath: context.dispatchedPath,
  })),
  {
    // `:2028` — `len(suffix) == 2 and suffix[0] == "frames" and
    // FRAME_INDEX_RE.fullmatch(suffix[1])`, then `int(suffix[1][:-4])`.
    //
    // The grammar and the parse are one step here: wire's
    // `FrameIndexFromPngName` *is* `FRAME_INDEX_RE` followed by that `int`, so
    // a name the decoder refuses is a name Python's regex refuses — `00.png`
    // and `007.png` fall through to the `:2037` 404 without touching upstream,
    // exactly as they do in Python. Arbitrary-width indices are normalized by
    // `frameIndexFromPngName` so they still reach the archive-specific 404.
    decide: (context) =>
      context.suffix.length === 2 && context.suffix[0] === GATEWAY_ROUTE_SEGMENTS.frames
        ? Option.map(
            frameIndexFromPngName(context.suffix[1] ?? ''),
            (index): RouteDecision => ({
              _tag: 'FramePng',
              gameId: context.gameId,
              upstreamPath: context.dispatchedPath,
              index,
            }),
          )
        : Option.none(),
  },
];

const matchLadder = (
  ladder: ReadonlyArray<SuffixRule>,
  context: RouteContext,
): Option.Option<RouteDecision> => Arr.findFirst(ladder, (rule) => rule.decide(context));

// ---------------------------------------------------------------------------
// The procedure
// ---------------------------------------------------------------------------

const rejectBody = (body: RequestBodySignal): Option.Option<BadRequest> => {
  // `:1390` first: `int(Content-Length)` raises before the presence test runs,
  // so a garbage length beats a `Transfer-Encoding` that is also present.
  if (body === 'invalid-content-length') return Option.some(badRequest('invalidContentLength'));
  if (body === 'present') return Option.some(badRequest('getRequestBody'));
  return Option.none();
};

/**
 * Decide what a request is for, in `do_GET`'s exact order.
 *
 * @param method - a verb the handler is reachable by.  Everything else —
 *   `TRACE`, `CONNECT`, an invented verb — never reaches `do_GET`: the stdlib
 *   answers `501 Unsupported method ('X')` as HTML, which is the socket edge's
 *   job.  `isGatewayMethod` is the narrowing that keeps that decision visible.
 * @param path - `urlsplit(target).path`: fragment already dropped, `netloc`
 *   already ignored, and **never** percent-decoded.
 * @param query - `urlsplit(target).query`, raw and undecoded.  `''` means no
 *   query, so a bare trailing `?` is no query.
 * @param body - `_reject_body`'s verdict; see {@link RequestBodySignal}.
 */
export const dispatch = (
  method: GatewayMethod,
  path: string,
  query: string,
  body: RequestBodySignal,
): Either.Either<RouteDecision, DispatchProblem> => {
  // `:2059-2064` — `do_HEAD`/`do_POST`/… are `_method_not_allowed`, and they
  // never call `_reject_body`, so 405 outranks the body 400s.
  if (method !== GATEWAY_GET_METHOD) return Either.left(new MethodNotAllowed());

  // `:1967` — before path parsing, before any I/O.
  const rejected = rejectBody(body);
  if (Option.isSome(rejected)) return Either.left(rejected.value);

  const dispatchedPath = collapseLeadingSlashes(path);

  // `:1970` — exact match; unbounded, never proxied.
  if (dispatchedPath === GATEWAY_HEALTH_PATH) {
    return query === '' ? Either.right({ _tag: 'Health' }) : Either.left(badRequest('healthQuery'));
  }

  // `:1978` — exact match. `/v1/games/` is *not* this route.
  if (dispatchedPath === GATEWAY_GAMES_INDEX_PATH) {
    return query === ''
      ? Either.right({ _tag: 'GamesIndex' })
      : Either.left(badRequest('gamesIndexQuery'));
  }

  // `:1987` — `path.strip("/").split("/")`: both ends stripped, so a trailing
  // slash routes (and is still forwarded upstream, trap B1).
  const parts = stripSlashes(dispatchedPath).split('/');

  // `:1988` — before the query check and before any I/O.
  const candidate = parts[2] ?? '';
  if (
    parts.length < 3 ||
    !GATEWAY_GAMES_PREFIX.every((segment, at) => parts[at] === segment) ||
    !isGameId(candidate)
  ) {
    return Either.left(notFound());
  }

  const context: RouteContext = {
    gameId: candidate,
    suffix: parts.slice(3),
    dispatchedPath,
    query,
  };

  // `:1996-2004` — the three query-bearing routes.
  const viewer = matchLadder(PRE_QUERY_ROUTES, context);
  if (Option.isSome(viewer)) return Either.right(viewer.value);

  // `:2005` — after the viewer routes, before the remaining routes *and*
  // before the final 404: `/v1/games/{valid}/nonsense?x=1` is this 400.
  if (query !== '') return Either.left(badRequest('viewerRouteQuery'));

  // `:2010-2035`.
  const archive = matchLadder(POST_QUERY_ROUTES, context);
  if (Option.isSome(archive)) return Either.right(archive.value);

  // `:2037`.
  return Either.left(notFound());
};
