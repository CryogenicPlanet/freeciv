/**
 * The replay gateway's error taxonomy — every failure a handler is allowed to
 * produce, as a value.
 *
 * ## Why these nine classes and not thirty-seven
 *
 * Python raises one exception type, `GatewayProblem(status, message)`
 * (`agent_eval/replay_gateway.py:80`), with `UpstreamUnavailable` (`:85`) as
 * its single subclass.  The subclass exists because *one* failure has to be
 * distinguishable at a `except` site: only `UpstreamUnavailable` selects the
 * disk-fallback arm of the fallback matrix, and only on the routes that catch
 * it (behavior dossier §4).  Every other problem unwinds to the one response
 * site at `:2038` untouched.
 *
 * A port that copied that shape — one error carrying a free-form status and
 * string — would let any handler invent a message, which is precisely the
 * regression the wire catalogue exists to prevent.  So the taxonomy here is the
 * *catalogue*, partitioned by status:
 *
 * | Class | Status | Carries |
 * |---|---|---|
 * | {@link BadRequest} | 400 | one of {@link BAD_REQUEST_PROBLEMS} |
 * | {@link NotFound} | 404 | one of {@link NOT_FOUND_PROBLEMS} |
 * | {@link MethodNotAllowed} | 405 | nothing (`Allow: GET` is added when it is rendered) |
 * | {@link InternalError} | 500 | the defect, for the log only |
 * | {@link UpstreamUnavailable} | 502 | why the supervisor is gone |
 * | {@link UpstreamTooLarge} | 502 | nothing |
 * | {@link UpstreamInvalid} | 502 | nothing |
 * | {@link UpstreamHttpError} | the upstream's own status, 3xx → 502 | that status |
 * | {@link ArchiveUnavailable} | 503 | one of {@link ARCHIVE_UNAVAILABLE_PROBLEMS} |
 *
 * The partition is total and disjoint: `test/gateway/respond.test.ts` checks
 * that the four name lists plus the five fixed-message classes cover
 * `GATEWAY_PROBLEM_MESSAGES` exactly once each, and that every name's status in
 * `GATEWAY_PROBLEM_STATUS` equals the status of the class that owns it.  A
 * message added to `@arena/wire` and not placed here fails that test.
 *
 * ## Strings are never written here
 *
 * A class carries the *symbolic name* of its message
 * (`new NotFound({ problem: 'terminalArchiveNotFound' })`); the text comes from
 * `GATEWAY_PROBLEM_MESSAGES` at construction.  There is no literal message in
 * this file, and there must never be one — the viewer renders `payload.error`
 * verbatim, so a reworded string is a user-visible regression.
 *
 * ## Rendering lives next door
 *
 * These values know their status and their message and nothing else.  Bytes,
 * headers, and the catch sites are `./http/respond.ts`, which is the single
 * place a response is built (behavior dossier §0, "ONE response site").
 *
 * @module
 */

import { Gateway } from '@arena/wire';
import { Data, Match } from 'effect';
import type { UpstreamFailure } from './services/upstream.ts';

const MESSAGES = Gateway.GATEWAY_PROBLEM_MESSAGES;

// ---------------------------------------------------------------------------
// 400 — the request was refused on its shape, before any I/O
// ---------------------------------------------------------------------------

/**
 * Every 400 the gateway can emit, in dispatch order.
 *
 * The first two come from `_reject_body` (`:1384-1396`), which runs before path
 * parsing and before the network — a GET with a body on an unroutable path is a
 * 400, not a 404.  The next three are the per-route query gates (`:1972`,
 * `:1980`, `:2006`), and the rest are the two query parsers plus `_events`'
 * blanket refusal, all of which run before the upstream is opened.
 */
export const BAD_REQUEST_PROBLEMS = [
  'invalidContentLength',
  'getRequestBody',
  'healthQuery',
  'gamesIndexQuery',
  'viewerRouteQuery',
  'eventsQuery',
  'replayQueryDuplicates',
  'replayQueryNotIntegers',
  'replayQueryOutOfRange',
  'boardQueryTurn',
  'boardTurnNotInteger',
  'boardTurnNotPositive',
] as const satisfies readonly Gateway.GatewayProblemName[];

/** A message {@link BadRequest} may carry. */
export type BadRequestProblem = (typeof BAD_REQUEST_PROBLEMS)[number];

/** The request was malformed for this route.  Nothing was proxied. */
export class BadRequest extends Data.TaggedError('BadRequest')<{
  readonly problem: BadRequestProblem;
}> {
  /** `HTTPStatus.BAD_REQUEST`. */
  readonly status = 400;
  /** The public text, from the wire catalogue. */
  override readonly message: Gateway.GatewayProblemMessage = MESSAGES[this.problem];
}

// ---------------------------------------------------------------------------
// 404 — there is nothing to serve
// ---------------------------------------------------------------------------

/**
 * Every 404, from the three that mean "no such route" to the ten that mean "the
 * route is real and the artifact is not".
 *
 * `notFound` is the router's own (`:1993`, `:2037`, and the unreachable
 * `:1932`); `gameNotFound` is the manifest reader's (`:559-569`); the rest name
 * a specific missing file.  A caller experiences all of them as one condition,
 * which is exactly why the strings must not be interchanged.
 */
export const NOT_FOUND_PROBLEMS = [
  'notFound',
  'gameNotFound',
  'manifestNotFound',
  'reportNotFound',
  'terminalArchiveNotFound',
  'archiveDataNotFound',
  'archiveFileNotFound',
  'noMapFrames',
  'mapFrameDoesNotExist',
  'replayVideoNotFound',
  'replayArtifactsNotFound',
  'eventArtifactsNotFound',
  'boardSnapshotNotFound',
] as const satisfies readonly Gateway.GatewayProblemName[];

/** A message {@link NotFound} may carry. */
export type NotFoundProblem = (typeof NOT_FOUND_PROBLEMS)[number];

/** No route matched, or the artifact the route names is absent. */
export class NotFound extends Data.TaggedError('NotFound')<{
  readonly problem: NotFoundProblem;
}> {
  /** `HTTPStatus.NOT_FOUND`. */
  readonly status = 404;
  /** The public text, from the wire catalogue. */
  override readonly message: Gateway.GatewayProblemMessage = MESSAGES[this.problem];
}

// ---------------------------------------------------------------------------
// 405 — a verb the gateway maps but does not serve
// ---------------------------------------------------------------------------

/**
 * A mapped non-GET verb (`do_HEAD/POST/PUT/PATCH/DELETE/OPTIONS =
 * _method_not_allowed`, `:2059-2064`).
 *
 * Two facts a port loses easily.  **This is not every non-GET verb**: a verb
 * with no `do_<VERB>` — `TRACE`, `CONNECT`, anything invented — is rejected by
 * the stdlib before gateway code runs, as a `501` HTML page (behavior dossier
 * §1.1).  And **HEAD lands here**, not on the GET handler, so `HEAD /health` is
 * a 405; Bun's adapter then strips the body while keeping `Content-Length: 30`,
 * which is the one allowed body divergence in the port (§7.2).
 *
 * The `Allow: GET` header is added by `./http/respond.ts` — it is a property of
 * the response, not of the failure.
 */
export class MethodNotAllowed extends Data.TaggedError('MethodNotAllowed') {
  /** `HTTPStatus.METHOD_NOT_ALLOWED`. */
  readonly status = 405;
  /** The public text, from the wire catalogue. */
  override readonly message: Gateway.GatewayProblemMessage = MESSAGES.methodNotAllowed;
}

// ---------------------------------------------------------------------------
// 500 — the catch-all
// ---------------------------------------------------------------------------

/**
 * A defect: something no handler declared.  `do_GET`'s bare `except Exception`
 * (`:2040-2044`) answers 500 with a fixed message and **never** the exception
 * text — `test_events_loader_failures_stay_public` (pytest dossier E1) is the
 * assertion that a private path in a raised error does not reach the body.
 *
 * `cause` is carried for the log and is dropped on the floor by the renderer;
 * `./http/respond.ts` is where that promise is kept, and the "a defect does not
 * leak" test is what enforces it.
 */
export class InternalError extends Data.TaggedError('InternalError')<{
  readonly cause: unknown;
}> {
  /** `HTTPStatus.INTERNAL_SERVER_ERROR`. */
  readonly status = 500;
  /** The public text, from the wire catalogue.  Never the cause. */
  override readonly message: Gateway.GatewayProblemMessage = MESSAGES.internalError;
}

// ---------------------------------------------------------------------------
// 502 — the upstream supervisor misbehaved
// ---------------------------------------------------------------------------

/** How `_open_upstream` concluded the supervisor is gone. */
export type UpstreamUnavailableReason =
  /** The portless-forwarder signature: 502 + `X-Portless: 1` + `text/html` (`:1419-1432`). */
  | 'portless'
  /** `OSError` / `HTTPException`: refused, DNS, timeout, garbled response (`:1434`). */
  | 'transport';

/**
 * The upstream is *provably* gone — the only failure that selects a disk
 * fallback (behavior dossier §4).
 *
 * The distinction from a relayed 502 is load-bearing and narrow: all three of
 * status 502, `X-Portless: 1`, and a `text/html` media type are required
 * (`test_exact_portless_offline_502_uses_safe_archive_fallback`, pytest dossier
 * X1).  A port that treats any 502 as "offline" serves an archive where Python
 * relays an error.
 *
 * `reason` never changes the response — both sites emit the same message
 * (`:1431`, `:1437`) — it exists so the log can say which one happened.
 */
export class UpstreamUnavailable extends Data.TaggedError('UpstreamUnavailable')<{
  readonly reason: UpstreamUnavailableReason;
  readonly cause?: unknown;
}> {
  /** `HTTPStatus.BAD_GATEWAY`. */
  readonly status = 502;
  /** The public text, from the wire catalogue. */
  override readonly message: Gateway.GatewayProblemMessage = MESSAGES.upstreamUnavailable;
}

/**
 * The upstream JSON body exceeded `MAX_PROXY_JSON_BYTES` (8 MiB) — either its
 * `Content-Length` said so before a byte was read (`:1527`) or the capped read
 * came back over the limit (`:1535`).
 *
 * Not a fallback trigger: this is a plain `GatewayProblem`, so an oversized
 * upstream body never reaches disk (§5.1).
 */
export class UpstreamTooLarge extends Data.TaggedError('UpstreamTooLarge') {
  /** `HTTPStatus.BAD_GATEWAY`. */
  readonly status = 502;
  /** The public text, from the wire catalogue. */
  override readonly message: Gateway.GatewayProblemMessage = MESSAGES.upstreamJsonTooLarge;
}

/**
 * `/v1/games` came back 2xx but was not an index: the body did not parse, or
 * its `games` was not a list (`:1617`, `:1623`).
 *
 * The narrowness matters — this is the *only* JSON route that inspects a 2xx
 * body at all (§4.1, trap B3).  Every other 2xx is relayed as bytes and can
 * therefore never produce this.
 */
export class UpstreamInvalid extends Data.TaggedError('UpstreamInvalid') {
  /** `HTTPStatus.BAD_GATEWAY`. */
  readonly status = 502;
  /** The public text, from the wire catalogue. */
  override readonly message: Gateway.GatewayProblemMessage = MESSAGES.upstreamGamesIndexInvalid;
}

/**
 * The upstream answered with a status the gateway will not fall back on and
 * will not relay as a body: everything outside 2xx and `{404, 405}`.
 *
 * Two behaviors in one class, both from `upstreamProblem`:
 *
 * - **3xx → 502** `upstream redirects are not allowed`.  `_NoRedirect`
 *   (`:102-106`) makes urllib surface the redirect as a response rather than
 *   following it, and no second request is ever made
 *   (`test_upstream_redirect_cannot_escape_the_public_allowlist`).
 * - **anything else → itself**.  A 500 upstream is a 500 downstream carrying
 *   `upstream returned HTTP 500`; disk data must not mask it
 *   (`test_games_500_never_masks_failure_with_disk_index`).
 *
 * On a binary route this still renders as a JSON problem, not as a passthrough
 * — `_stream_upstream`'s non-2xx arm calls `_problem` (`:1453-1462`, trap B5).
 */
export class UpstreamHttpError extends Data.TaggedError('UpstreamHttpError')<{
  readonly upstreamStatus: number;
}> {
  /** The relayed status: the upstream's own, except 3xx which becomes 502. */
  readonly status: number = Gateway.upstreamProblem(this.upstreamStatus).status;
  /** `upstream returned HTTP {n}`, or the redirect refusal for a 3xx. */
  override readonly message: string = Gateway.upstreamProblem(this.upstreamStatus).message;
}

// ---------------------------------------------------------------------------
// 503 — it is here, and it cannot be served
// ---------------------------------------------------------------------------

/**
 * Every 503: an archive file that exists but is unreadable (`:584`/`:590`/
 * `:596`), a body the gateway built that came out over 8 MiB (`:1375`), and the
 * three loaders' non-`FileNotFoundError` failures (`:1716`, `:1788`, `:1870`).
 *
 * The loader arm is the one with a leak risk: `OSError`, `UnicodeError`,
 * `JSONDecodeError` and `ValueError` — which includes `SaveReplayError` — all
 * map here, and the raised text is discarded (pytest dossier E1).
 */
export const ARCHIVE_UNAVAILABLE_PROBLEMS = [
  'manifestUnavailable',
  'reportUnavailable',
  'archiveJsonTooLarge',
  'replayTelemetryUnavailable',
  'eventsUnavailable',
  'boardSnapshotUnavailable',
] as const satisfies readonly Gateway.GatewayProblemName[];

/** A message {@link ArchiveUnavailable} may carry. */
export type ArchiveUnavailableProblem = (typeof ARCHIVE_UNAVAILABLE_PROBLEMS)[number];

/**
 * The artifact is present and cannot be turned into a response.
 *
 * `cause` is carried for the log only; the body is the catalogue message, and
 * `test/gateway/respond.test.ts` pins that a cause carrying a private path does
 * not reach the wire.
 */
export class ArchiveUnavailable extends Data.TaggedError('ArchiveUnavailable')<{
  readonly problem: ArchiveUnavailableProblem;
  readonly cause?: unknown;
}> {
  /** `HTTPStatus.SERVICE_UNAVAILABLE`. */
  readonly status = 503;
  /** The public text, from the wire catalogue. */
  override readonly message: Gateway.GatewayProblemMessage = MESSAGES[this.problem];
}

// ---------------------------------------------------------------------------
// The union
// ---------------------------------------------------------------------------

/**
 * Everything a gateway handler may fail with.
 *
 * This is the `E` channel of every route: a handler returns
 * `Effect<HttpServerResponse, GatewayError, R>` and builds no error response
 * itself.  `./http/respond.ts` renders the union, once, at the router edge.
 */
export type GatewayError =
  | ArchiveUnavailable
  | BadRequest
  | InternalError
  | MethodNotAllowed
  | NotFound
  | UpstreamHttpError
  | UpstreamInvalid
  | UpstreamTooLarge
  | UpstreamUnavailable;

/** The `_tag` of a {@link GatewayError} — the key of every per-class table. */
export type GatewayErrorTag = GatewayError['_tag'];

/**
 * Every tag, so a table over the taxonomy can be checked for totality at
 * runtime as well as in the type system.
 */
export const GATEWAY_ERROR_TAGS = [
  'ArchiveUnavailable',
  'BadRequest',
  'InternalError',
  'MethodNotAllowed',
  'NotFound',
  'UpstreamHttpError',
  'UpstreamInvalid',
  'UpstreamTooLarge',
  'UpstreamUnavailable',
] as const satisfies readonly GatewayErrorTag[];

/**
 * The `(status, message)` pair Python's `raise GatewayProblem(status, message)`
 * carries — the whole of what a failure contributes to a response.
 *
 * Everything else about the response (bytes, content type, cache control, the
 * 405's `Allow`) is the renderer's, so that no handler can influence it.
 */
export const gatewayProblem = (error: GatewayError): Gateway.GatewayProblemResponse => ({
  status: error.status,
  message: error.message,
});

// ---------------------------------------------------------------------------
// The crossing from the upstream client's vocabulary
// ---------------------------------------------------------------------------

/**
 * `./services/upstream.ts`'s failures as the taxonomy the response site
 * renders — the **only** place the two vocabularies meet.
 *
 * They are two vocabularies on purpose: the client says *what happened on the
 * socket*, the taxonomy says *what the client is told*.  It lives here, beside
 * the taxonomy, because it existed three times — twice under the same name in
 * `./http/routes/archive.ts` and `./http/routes/replay.ts`, which had already
 * drifted on the `UpstreamBodyError` arm (one logged the bare socket error, the
 * other the tagged value carrying the URL and the read-vs-timeout reason), and
 * once more as a three-armed `upstreamGatewayError` in
 * `./http/routes/games.ts`.  Two tests pinned two copies and none compared
 * them.  The import is type-only apart from the tag strings, so the taxonomy
 * still depends on nothing at runtime.
 *
 * The mapping is *not* one-to-one on status, and each arm is a decision:
 *
 * - `UpstreamOffline` → 502, and it is the only one that ever reaches a route
 *   that has not already caught it to select the disk fallback.  A `timeout`
 *   collapses into `transport` because Python cannot tell them apart either:
 *   `socket.timeout` is an `OSError`, so `:1434` raises the same
 *   `UpstreamUnavailable` a refused connection does, and `reason` exists only
 *   for the log.
 * - `UpstreamJsonTooLarge` → 502 `upstreamJsonTooLarge`, **not** a fallback
 *   (§5.1): an oversized upstream body must never be replaced by disk data.
 * - `UpstreamRedirect` → `upstreamProblem(status)`, the 502 redirect refusal
 *   for the 3xx that is the only status arriving this way.
 * - `UpstreamBodyError` → 500.  In Python the socket error is raised *outside*
 *   `_open_upstream`'s `try` (`:1548`) and lands on `do_GET`'s catch-all
 *   (`:2040`); "the socket died mid-body" is not the claim "the service is
 *   unavailable", and only the latter may substitute disk for live data.  The
 *   whole tagged failure is the `cause`, so the log keeps the URL and the
 *   reason.
 *
 * `Match.exhaustive`: a failure added to `UpstreamClient` without an answer
 * here is a type error, not a silent 500.
 */
export const gatewayErrorFromUpstream: (failure: UpstreamFailure) => GatewayError = Match.type<
  UpstreamFailure
>().pipe(
  Match.tag(
    'UpstreamOffline',
    (failure): GatewayError =>
      new UpstreamUnavailable({
        reason: failure.reason === 'portless' ? 'portless' : 'transport',
        cause: failure.cause,
      }),
  ),
  Match.tag('UpstreamJsonTooLarge', (): GatewayError => new UpstreamTooLarge()),
  Match.tag(
    'UpstreamRedirect',
    (failure): GatewayError => new UpstreamHttpError({ upstreamStatus: failure.status }),
  ),
  Match.tag('UpstreamBodyError', (failure): GatewayError => new InternalError({ cause: failure })),
  Match.exhaustive,
);

/** True for the one upstream failure that selects the disk fallback (§4). */
export const isUpstreamOffline = (failure: UpstreamFailure): boolean =>
  failure._tag === 'UpstreamOffline';
