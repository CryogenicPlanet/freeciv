/**
 * `do_GET`'s dispatch, as a pure ordered decision procedure.
 *
 * `agent_eval/replay_gateway.py:1965-2044` is a seventeen-step ladder of
 * `if ... return` statements, and **the order is the contract**.  Not one step
 * commutes with its neighbours:
 *
 * - the game-id check (`:1988`) precedes the query check (`:2005`), so
 *   `/v1/games/short/status?x=1` is a 404 and not a 400;
 * - the query check precedes the final 404 (`:2037`), so
 *   `/v1/games/{valid}/nonsense?x=1` is a 400 *about query parameters for a
 *   route that does not exist*;
 * - `replay.json`/`board.json`/`events.json` (`:1996-2004`) precede the query
 *   check, which is the only reason any route may carry a query at all;
 * - `_reject_body` (`:1967`) precedes path parsing, so a GET with a body on an
 *   unroutable path is a 400 and nothing is proxied.
 *
 * Match-order drift is therefore the bug class this module exists to prevent,
 * and the reason dispatch is a *pure function over four values* rather than a
 * router table wired into a server: the parity rig can enumerate it, and
 * `test/gateway/dispatch.test.ts` does.  The two suffix ladders are literal
 * data — {@link PRE_QUERY_ROUTES} and {@link POST_QUERY_ROUTES}, each rule
 * carrying the Python line it transcribes — so reordering a route is a visible
 * edit to an array, not a subtle move of a nested `if`.
 *
 * ## What this module deliberately does not do
 *
 * - **No I/O, no upstream, no disk.** A `Right` says only *which handler*, on
 *   *which game*, with *which upstream path*.  The fallback matrix is the
 *   handlers' business.
 * - **No error responses.** A `Left` is a {@link DispatchProblem} *value*
 *   carrying the verbatim `@arena/wire` message and its status; the single
 *   response site at the router edge renders it (`:2038`).
 * - **No query parsing.** `replay.json`/`board.json`/`events.json` receive the
 *   raw query string and reject or normalize it themselves (`:1554`, `:1799`,
 *   `:1728`) — including `events.json`, which 400s on *any* query but does so
 *   from the handler, so a non-empty query still dispatches here.
 * - **No percent-decoding, ever.** `parsed.path` keeps `%2F` literal, which is
 *   what makes `/v1/games/{id}%2Fstatus` a 404 rather than a traversal.
 *
 * Every bare `:NNN` citation is `agent_eval/replay_gateway.py`.
 *
 * @module
 */

import { Array as Arr, Either, Option } from 'effect';
import { decodeFrameIndexFromPngName, type FrameIndex, type GameId, isGameId } from '@arena/wire';
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

/** The discriminant of a {@link RouteDecision}. */
export type RouteTag = RouteDecision['_tag'];

/**
 * Every route tag, in `do_GET` match order.
 *
 * Exported so a test can assert coverage rather than trust a reviewer to
 * notice a route nobody exercised.
 */
export const ROUTE_TAGS = [
  'Health',
  'GamesIndex',
  'ReplayJson',
  'BoardJson',
  'EventsJson',
  'ArchiveStatus',
  'ArchiveResult',
  'ArchiveWatch',
  'ArchiveFrames',
  'LatestFramePng',
  'VideoMp4',
  'FramePng',
] as const satisfies ReadonlyArray<RouteTag>;

/**
 * Which Python handler a decision belongs to.  The fallback matrix differs per
 * family and must not be unified — see the behavior dossier §4.
 */
export type RouteFamily = 'health' | 'gamesIndex' | 'archiveJson' | 'viewerJson' | 'archiveBinary';

/** Route tag → handler family.  Total by construction. */
export const ROUTE_FAMILY: { readonly [T in RouteTag]: RouteFamily } = {
  Health: 'health',
  GamesIndex: 'gamesIndex',
  ArchiveStatus: 'archiveJson',
  ArchiveResult: 'archiveJson',
  ArchiveWatch: 'archiveJson',
  ArchiveFrames: 'archiveJson',
  ReplayJson: 'viewerJson',
  BoardJson: 'viewerJson',
  EventsJson: 'viewerJson',
  FramePng: 'archiveBinary',
  LatestFramePng: 'archiveBinary',
  VideoMp4: 'archiveBinary',
};

/** The `kind` argument `_archive_json_route` is called with (`:1887`). */
export type ArchiveJsonView = 'status' | 'result' | 'watch' | 'frames';

/** Archive-JSON route tag → the projection `_archive_json_route` selects. */
export const ARCHIVE_JSON_VIEWS: { readonly [T in ArchiveJsonRoute['_tag']]: ArchiveJsonView } = {
  ArchiveStatus: 'status',
  ArchiveResult: 'result',
  ArchiveWatch: 'watch',
  ArchiveFrames: 'frames',
};

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

/** One rung of a ladder: the Python line it transcribes, and its decision. */
export interface SuffixRule {
  /** Line of the `if` in `agent_eval/replay_gateway.py`. */
  readonly line: number;
  readonly decide: (context: RouteContext) => Option.Option<RouteDecision>;
}

const onSuffix = (
  line: number,
  expected: ReadonlyArray<string>,
  build: (context: RouteContext) => RouteDecision,
): SuffixRule => ({
  line,
  decide: (context) =>
    segmentsEqual(context.suffix, expected) ? Option.some(build(context)) : Option.none(),
});

/** `f"/v1/games/{game_id}/{segment}"` — the viewer families' canonical rebuild. */
const viewerUpstreamPath = (gameId: GameId, segment: string): string =>
  `${GATEWAY_GAMES_INDEX_PATH}/${gameId}/${segment}`;

/**
 * `:1996-2004` — matched **before** the query gate, which is the only reason
 * any route may carry a query string.
 */
export const PRE_QUERY_ROUTES: ReadonlyArray<SuffixRule> = [
  onSuffix(1996, [GATEWAY_ROUTE_SEGMENTS.replayJson], (context) => ({
    _tag: 'ReplayJson',
    gameId: context.gameId,
    upstreamPath: viewerUpstreamPath(context.gameId, GATEWAY_ROUTE_SEGMENTS.replayJson),
    query: context.query,
  })),
  onSuffix(1999, [GATEWAY_ROUTE_SEGMENTS.boardJson], (context) => ({
    _tag: 'BoardJson',
    gameId: context.gameId,
    upstreamPath: viewerUpstreamPath(context.gameId, GATEWAY_ROUTE_SEGMENTS.boardJson),
    query: context.query,
  })),
  onSuffix(2002, [GATEWAY_ROUTE_SEGMENTS.eventsJson], (context) => ({
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
export const POST_QUERY_ROUTES: ReadonlyArray<SuffixRule> = [
  {
    // `:2010` — `if not suffix or suffix == ["status"]`, one branch, two forms.
    line: 2010,
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
  onSuffix(2013, [GATEWAY_ROUTE_SEGMENTS.result], (context) => ({
    _tag: 'ArchiveResult',
    gameId: context.gameId,
    upstreamPath: context.dispatchedPath,
  })),
  onSuffix(2016, [GATEWAY_ROUTE_SEGMENTS.watchJson], (context) => ({
    _tag: 'ArchiveWatch',
    gameId: context.gameId,
    upstreamPath: context.dispatchedPath,
  })),
  onSuffix(2019, [GATEWAY_ROUTE_SEGMENTS.frames], (context) => ({
    _tag: 'ArchiveFrames',
    gameId: context.gameId,
    upstreamPath: context.dispatchedPath,
  })),
  onSuffix(
    2022,
    [GATEWAY_ROUTE_SEGMENTS.frames, GATEWAY_ROUTE_SEGMENTS.latestPng],
    (context) => ({
      _tag: 'LatestFramePng',
      gameId: context.gameId,
      upstreamPath: context.dispatchedPath,
    }),
  ),
  onSuffix(2025, [GATEWAY_ROUTE_SEGMENTS.videoMp4], (context) => ({
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
    // exactly as they do in Python.
    //
    // One bounded divergence: Python's `int` is arbitrary-precision, so a
    // ~310-digit index routes there and 404s later at the archive lookup,
    // while here it is not a finite JS number and 404s now.  Same status, a
    // different message (`not found` vs `map frame does not exist`), only for
    // inputs no archive can hold — six digits is the on-disk width.
    line: 2028,
    decide: (context) =>
      context.suffix.length === 2 && context.suffix[0] === GATEWAY_ROUTE_SEGMENTS.frames
        ? Option.map(
            Either.getRight(decodeFrameIndexFromPngName(context.suffix[1] ?? '')),
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
