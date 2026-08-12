/**
 * The socket edge: everything between a Bun `Request` and the one response
 * site, plus the startup order the ready file depends on.
 *
 * This module is the assembly, not the behavior.  Dispatch is a pure function
 * (`./http/dispatch.ts`), every refusal is a value (`./errors.ts`), every
 * refusal is rendered exactly once (`./http/respond.ts`), and each route family
 * is its own module under `./http/routes/`.  What is left — and what lives
 * here — is the four things only the edge can do:
 *
 * 1. **Recover the request line Python saw.**  `BaseHTTPRequestHandler` hands
 *    `do_GET` a `urlsplit(self.path)`; Bun hands us an absolute URL.  The
 *    fragment must be dropped, the query must be the *raw* string (`''` means
 *    no query, so a bare trailing `?` is no query), and the path must **never**
 *    be percent-decoded — `%2F` staying literal is the only reason
 *    `/v1/games/{id}%2Fstatus` is a 404 rather than a traversal (pytest dossier
 *    D5).
 * 2. **Classify the entity body from headers alone.**  `_reject_body`
 *    (`:1384-1396`) never reads the socket, and it could not here anyway: spike
 *    S4 proved Bun discards a GET entity-body outright (`request.body === null`)
 *    while still delivering `Content-Length`/`Transfer-Encoding`.  Detection
 *    yes, payload no — which is exactly what the Python needs.
 * 3. **Answer the verbs `do_GET` never sees.**  A verb with no `do_<VERB>` is
 *    rejected by the stdlib *before* any gateway code runs, as an **HTML** page
 *    with `Connection: close` and none of the security headers (behavior
 *    dossier §1.1).  `TRACE` is `501 Unsupported method ('TRACE')`, not the
 *    JSON 405 — a port that answers 405 for every non-GET verb is *more*
 *    correct than Python and fails byte parity.  {@link unsupportedMethod} is
 *    that page, transcribed from `http.server`'s
 *    `DEFAULT_ERROR_MESSAGE_FORMAT`; the JSON 405 belongs to the six *mapped*
 *    verbs and comes out of `dispatch` like every other refusal.
 *
 * ## Bun has pre-handler rejections of its own, and they are not Python's
 *
 * `http.server` rejects a malformed request line before `do_GET`; Bun rejects a
 * malformed *message* before our `fetch` handler, and it draws the line in a
 * different place.  Measured against this very app (`test/gateway/server.test.ts`):
 *
 * | Request | Python | Bun |
 * |---|---|---|
 * | `TRACE` / `CONNECT` | `501 …` HTML | reaches us → {@link unsupportedMethod} |
 * | an invented verb (`FOO`) | `501 …` HTML | connection closed, **no response** |
 * | `Content-Length: abc` \| `5_0` | `400 invalid Content-Length` (ours) | `400 Bad Request` (Bun's, bodiless) |
 * | `Transfer-Encoding: identity` | `400 GET request bodies…` (ours) | `400 Bad Request` (Bun's) |
 * | `Transfer-Encoding: chunked` | `400 GET request bodies…` | reaches us → same 400 |
 * | a space in the target | `400 Bad request syntax` HTML | `505 HTTP Version Not Supported` |
 * | two `Content-Length: 0` | accepted (`.get` takes the first) | joined to `"0, 0"` → our 400 |
 *
 * None of these are fixable at this layer — the bytes never reach Effect code —
 * so they are written down instead, and {@link bodySignal} keeps the *decision*
 * testable as a pure function even where the socket will not deliver the input.
 * 4. **Publish the ready record last.**  Spike S4's headline finding:
 *    `BunHttpServer.make` calls `Bun.serve` immediately, with a built-in
 *    `404 not found` handler, so the port is bound and *answering* before
 *    `serve(app)` attaches ours.  The ready file therefore goes out **after**
 *    `serve()` returns, never merely after the address is readable, or a client
 *    that reads the record and connects instantly is told 404 by code that is
 *    not ours.  `test/gateway/server.test.ts` pins this from the client side:
 *    the ready sink itself performs the request.
 *
 * Every bare `:NNN` citation is `agent_eval/replay_gateway.py`.
 *
 * ## What this module deliberately does not do
 *
 * - **It builds no error body.**  The 501 above is the stdlib's, produced
 *   before routing exists; everything downstream of {@link dispatch} fails with
 *   a `../errors.ts` value and {@link respondGateway} renders it.
 * - **It passes no `routes` option to `BunHttpServer.make`.**  Bun's router
 *   would answer before our handler and take the method contract with it
 *   (spike law).
 * - **It does not special-case HEAD.**  `dispatch` answers 405 with the GET
 *   body computed in full; the Bun adapter strips the body and keeps
 *   `Content-Length`, which is what Python's HEAD does on the wire (§7.2, §15).
 *
 * @module
 */

import { annotate, Observability, withWideEvent } from '@arena/telemetry';
import { Gateway } from '@arena/wire';
import { FileSystem, type Headers, HttpServerRequest, HttpServerResponse } from '@effect/platform';
import type { PlatformError } from '@effect/platform/Error';
import { BunHttpServer } from '@effect/platform-bun';
import { Effect, Either, Match, Option, type Scope } from 'effect';
import { GatewayConfig, pythonRepr } from './config.ts';
import { isGatewayMethod } from './constants.ts';
import type { GatewayError } from './errors.ts';
import { dispatch, type RequestBodySignal, type RouteDecision } from './http/dispatch.ts';
import { jsonPayloadResponse } from './http/json.ts';
import { respondGateway } from './http/respond.ts';
import { archiveBinaryRoute, archiveJsonRoute, type ArchiveRouteOptions } from './http/routes/archive.ts';
import { boardRoute } from './http/routes/board.ts';
import { eventsRoute } from './http/routes/events.ts';
import { gamesIndexRoute } from './http/routes/games.ts';
import {
  type GatewayIdentityService,
  GatewayIdentity,
  healthRoute,
  makeGatewayIdentity,
} from './http/routes/health.ts';
import { replayRoute } from './http/routes/replay.ts';
import { ReplayDerivation } from './services/derivation.ts';
import { ReadyFile, type ReadyFileError, type ReadyPublication } from './services/ready-file.ts';
import { RunsRepository } from './services/runs.ts';
import { parsePythonInt, UpstreamClient } from './services/upstream.ts';

// ---------------------------------------------------------------------------
// The stdlib error page — pre-router, HTML, no security headers
// ---------------------------------------------------------------------------

/**
 * `BaseHTTPRequestHandler.error_content_type`.
 *
 * Note the *absence* of a space after the semicolon: the stdlib writes
 * `text/html;charset=utf-8` where the gateway's own JSON writes
 * `application/json; charset=utf-8`.  Two different spellings on one server,
 * and a parity rig comparing header values will see it.
 */
export const STDLIB_ERROR_CONTENT_TYPE = 'text/html;charset=utf-8';

/** `html.escape(text, quote=False)` — `&`, `<`, `>` and nothing else. */
export const htmlEscape = (text: string): string =>
  text.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');

/**
 * `BaseHTTPRequestHandler.DEFAULT_ERROR_MESSAGE_FORMAT`, filled in.
 *
 * Transcribed from the interpreter this repo runs (CPython 3.14.6) rather than
 * from memory — the `<style>` block with `color-scheme` was added in 3.13 and
 * changes the byte length.  The 501 page below measures 483 bytes, which is
 * what the behavior dossier's live probe recorded off the wire (§14).
 *
 * `%(code)d` and `%(code)s` are the same integer twice; `message` and `explain`
 * arrive HTML-escaped and are followed by a literal `.` in the template.
 */
export const stdlibErrorPage = (code: number, message: string, explain: string): string =>
  `<!DOCTYPE HTML>
<html lang="en">
    <head>
        <meta charset="utf-8">
        <style type="text/css">
            :root {
                color-scheme: light dark;
            }
        </style>
        <title>Error response</title>
    </head>
    <body>
        <h1>Error response</h1>
        <p>Error code: ${code}</p>
        <p>Message: ${htmlEscape(message)}.</p>
        <p>Error code explanation: ${code} - ${htmlEscape(explain)}.</p>
    </body>
</html>
`;

/** `BaseHTTPRequestHandler.responses[HTTPStatus.NOT_IMPLEMENTED][1]`. */
export const UNSUPPORTED_METHOD_EXPLAIN = 'Server does not support this operation';

/**
 * `send_error(501, f"Unsupported method ({self.command!r})")` — the reason
 * phrase *and* the message body carry the verb, in Python's `repr` quoting.
 */
export const unsupportedMethodMessage = (method: string): string =>
  `Unsupported method (${pythonRepr(method)})`;

/**
 * The whole pre-router refusal, as a response.
 *
 * What is missing is the point: no `X-Content-Type-Options`, no
 * `Referrer-Policy`, no `Cache-Control`.  `send_error` writes them nowhere
 * because it never reaches `_send_headers` (`:1335`), so a port that adds its
 * security pair "for consistency" diverges on the first `TRACE`.
 *
 * Two things are stated here that Bun then overrules, measured rather than
 * assumed (see the test):
 *
 * - **`statusText`.**  Python's reason phrase is the message —
 *   `HTTP/1.0 501 Unsupported method ('TRACE')`.  Bun writes the *canonical*
 *   phrase for the status whatever the `Response` carries, so the wire says
 *   `501 Not Implemented`.  It is set anyway because it is the correct value
 *   and the only place a reader would look for it; the divergence is the
 *   server's, not this function's.
 * - **`Connection: close`.**  The stdlib states it because HTTP/1.0 closes
 *   every response; Bun owns its own connection management.  The header is
 *   ours, the socket is not.
 */
export const unsupportedMethod = (method: string): HttpServerResponse.HttpServerResponse => {
  const message = unsupportedMethodMessage(method);
  return HttpServerResponse.text(stdlibErrorPage(501, message, UNSUPPORTED_METHOD_EXPLAIN), {
    status: 501,
    statusText: message,
    contentType: STDLIB_ERROR_CONTENT_TYPE,
    headers: { connection: 'close' },
  });
};

// ---------------------------------------------------------------------------
// The request line, as do_GET sees it
// ---------------------------------------------------------------------------

/** `urlsplit(target)`'s two interesting components, undecoded. */
export interface RequestTarget {
  /** `parsed.path` — never percent-decoded, leading slashes not yet collapsed. */
  readonly path: string;
  /** `parsed.query` — raw; `''` is "no query", which is what a bare `?` gives. */
  readonly query: string;
}

/**
 * Split what the Bun adapter hands us the way `urlsplit` splits `self.path`.
 *
 * The adapter has already removed the scheme and authority (its `removeHost`),
 * which is `urlsplit`'s "absolute-form targets are accepted, `netloc` ignored"
 * (`GET http://evil.invalid/health` → 200, dossier §1.3).  Two steps remain,
 * and their order matters: the **fragment goes first**, so `/health#a?b` has no
 * query at all, exactly as `urlsplit` reports it.
 */
export const splitTarget = (target: string): RequestTarget => {
  const hash = target.indexOf('#');
  const withoutFragment = hash === -1 ? target : target.slice(0, hash);
  const mark = withoutFragment.indexOf('?');
  return mark === -1
    ? { path: withoutFragment, query: '' }
    : { path: withoutFragment.slice(0, mark), query: withoutFragment.slice(mark + 1) };
};

/**
 * `_reject_body` (`:1384-1396`) as a verdict over headers.
 *
 * ```python
 * transfer_encoding = self.headers.get("Transfer-Encoding")
 * length            = self.headers.get("Content-Length")
 * has_length        = length is not None and int(length) != 0
 * if transfer_encoding or has_length: ...
 * ```
 *
 * `has_length` is evaluated *before* the `if`, so an unparseable
 * `Content-Length` beats a `Transfer-Encoding` that is also present — the
 * request is `invalid Content-Length`, not `GET request bodies are not
 * accepted`.  `int()` is Python's, so `" 5 "`, `"+5"`, `"5_0"` and Unicode
 * digits all parse and are non-zero; `parsePythonInt` is the shared
 * implementation (`./services/upstream.ts`), not a second one.
 *
 * `Content-Length: 0` is accepted (`int("0") != 0` is false).
 *
 * One bounded divergence: a request carrying *two* `Content-Length` headers
 * reaches Python as the first value (`Headers.get`) and reaches us as the
 * comma-joined pair, which `int()` refuses — so a duplicated length is a 400
 * here and possibly a 200 there.  Bun joins before any Effect code runs; the
 * alternative is not available at this layer.
 */
export const bodySignal = (headers: Headers.Headers): RequestBodySignal => {
  const length = headers['content-length'];
  const parsed = length === undefined ? Option.none<bigint>() : parsePythonInt(length);
  if (length !== undefined && Option.isNone(parsed)) return 'invalid-content-length';
  if (headers['transfer-encoding'] !== undefined) return 'present';
  return Option.isSome(parsed) && parsed.value !== 0n ? 'present' : 'absent';
};

// ---------------------------------------------------------------------------
// Route decision → response
// ---------------------------------------------------------------------------

/** `_archive_base()` (`:1881`) and `absolute_watch` (`:1916`), read off the identity. */
export const archiveOptions = (identity: GatewayIdentityService): ArchiveRouteOptions => ({
  base: identity.archiveBase,
  absoluteWatch: identity.absoluteWatch,
});

/** Every service a route may reach for, plus the per-request `Scope`. */
export type GatewayRouteServices =
  | GatewayIdentity
  | ReplayDerivation
  | RunsRepository
  | Scope.Scope
  | UpstreamClient;

/**
 * The twelve routes, dispatched to their five handler families.
 *
 * The `Match` is exhaustive over `RouteDecision['_tag']`, which is the property
 * worth having: a route added to `./http/dispatch.ts` and forgotten here is a
 * compile error rather than a 500 in production.
 */
export const handleRoute = (
  decision: RouteDecision,
): Effect.Effect<HttpServerResponse.HttpServerResponse, GatewayError, GatewayRouteServices> =>
  Match.value(decision).pipe(
    Match.tag('Health', () => Effect.map(healthRoute, jsonPayloadResponse)),
    Match.tag('GamesIndex', () => Effect.map(gamesIndexRoute, jsonPayloadResponse)),
    Match.tag('ReplayJson', (route) => replayRoute(route)),
    Match.tag('BoardJson', (route) => boardRoute(route)),
    Match.tag('EventsJson', (route) => eventsRoute(route)),
    Match.tag('ArchiveStatus', 'ArchiveResult', 'ArchiveWatch', 'ArchiveFrames', (route) =>
      Effect.flatMap(GatewayIdentity, (identity) =>
        archiveJsonRoute(route, archiveOptions(identity)),
      ),
    ),
    Match.tag('FramePng', 'LatestFramePng', 'VideoMp4', (route) => archiveBinaryRoute(route)),
    Match.exhaustive,
  );

// ---------------------------------------------------------------------------
// The app
// ---------------------------------------------------------------------------

/** The name every request's wide event is recorded under. */
export const GATEWAY_REQUEST_EVENT = 'gateway.request';

/**
 * The `gateway.route` value for a verb that never reaches `dispatch`.
 *
 * Not a `RouteTag` — deliberately, so a corpus query over the twelve real
 * routes cannot pick it up by accident, and so the one class of request the
 * dispatcher never sees is still visible in the corpus.
 */
export const UNSUPPORTED_METHOD_ROUTE = 'UnsupportedMethod';

/**
 * The request, as one wide event's worth of fields.
 *
 * The query is recorded as a *presence* flag rather than as its value: a viewer
 * query is user input and the corpus is not a place to accumulate it.  The path
 * is recorded whole because it is the only way to tell which archive a 404 was
 * about, and it is already public — it is what the client typed.
 */
const requestFields = (
  method: string,
  target: RequestTarget,
): Readonly<Record<string, unknown>> => ({
  'http.method': method,
  'http.path': target.path,
  'http.query': target.query !== '',
});

/**
 * One request, from verb to bytes.
 *
 * The order is `do_GET`'s and the dossier's: the unmapped-verb 501 precedes
 * everything (the stdlib rejects it before dispatch exists), then dispatch,
 * then the handler, then the single response site.  `respondGateway` is applied
 * to the *whole* of dispatch-plus-handler, which is what makes a `Left` from
 * `dispatch` and a failure from a route indistinguishable on the wire — they
 * are the same `except GatewayProblem` in Python (`:2038`).
 */
export const handleRequest = (
  request: HttpServerRequest.HttpServerRequest,
): Effect.Effect<
  HttpServerResponse.HttpServerResponse,
  never,
  GatewayRouteServices | Observability
> => {
  const target = splitTarget(request.url);
  const method = request.method;
  const served = isGatewayMethod(method)
    ? respondGateway(
        Either.match(dispatch(method, target.path, target.query, bodySignal(request.headers)), {
          onLeft: (problem) => Effect.fail<GatewayError>(problem),
          onRight: (decision) =>
            Effect.zipRight(annotate({ 'gateway.route': decision._tag }), handleRoute(decision)),
        }),
      )
    : Effect.succeed(unsupportedMethod(method)).pipe(
        Effect.tap(() => annotate({ 'gateway.route': UNSUPPORTED_METHOD_ROUTE })),
      );

  // The request's own fields are annotated *before* the work, so a refusal —
  // which never reaches a route and therefore never claims a `gateway.route` —
  // still carries the method and the path it was refused for.
  return Effect.zipRight(annotate(requestFields(method, target)), served).pipe(
    Effect.tap((response) => annotate({ 'http.status_code': response.status })),
    withWideEvent(GATEWAY_REQUEST_EVENT),
  );
};

/**
 * The `HttpApp` itself.
 *
 * `Scope.Scope` in the requirements is the *per-request* scope that
 * `HttpApp.toHandled` installs — the one `./http/routes/archive.ts` registers
 * its upstream reader cancellation and its open file descriptor on, so a
 * client that disconnects mid-video releases both.  It is not the server's
 * scope and must not be confused with it.
 */
export const gatewayApp: Effect.Effect<
  HttpServerResponse.HttpServerResponse,
  never,
  GatewayRouteServices | HttpServerRequest.HttpServerRequest | Observability
> = Effect.flatMap(HttpServerRequest.HttpServerRequest, handleRequest);

// ---------------------------------------------------------------------------
// Startup
// ---------------------------------------------------------------------------

/** What a started gateway tells its caller. */
export interface GatewayHandle {
  /** The compressed loopback literal the socket is bound to. */
  readonly host: string;
  /** The **bound** port: `--port 0` asks, and this is the answer (`:1312`). */
  readonly port: number;
  /** `http://{host}:{port}`, IPv6 bracketed — the identity's `url`. */
  readonly url: string;
  /** The `/health` body, the ready record and the stdout line, as one value. */
  readonly identity: GatewayIdentityService;
  /** What the ready publisher wrote, or `Option.none()`s when it is disabled. */
  readonly ready: ReadyPublication;
}

/** Everything a caller must supply before {@link serveGateway} can run. */
export type GatewayServerServices =
  | FileSystem.FileSystem
  | GatewayConfig
  | Observability
  | ReadyFile
  | ReplayDerivation
  | RunsRepository
  | Scope.Scope
  | UpstreamClient;

/**
 * Starting the gateway can fail two ways, and both are Python `OSError`s that
 * `main` reports as `error: …` on stderr with exit 2 (`:2205-2207`):
 * `cache_root.mkdir` (`:2092`) and the ready-file publish (`:2153-2154`).
 *
 * A refused *bind* is not in this union: `BunHttpServer.make` declares no error
 * channel, so an address already in use arrives as a defect.  It still exits 2
 * — {@link ../main.ts}'s teardown maps every non-interrupted exit that way —
 * but it is not a value this signature can promise.
 */
export type GatewayServeError = PlatformError | ReadyFileError;

/**
 * `run_replay_gateway` (`:2114-2171`), in the order the Python performs it and
 * with the one substitution Bun forces.
 *
 * ```
 * make_replay_gateway_server(...)   binds; --port 0 is resolved here   :2093
 * cache_root.mkdir(parents=True)   before anything is served          :2092
 * identity_payload()               reads the *bound* port             :1302
 * ── the substitution ──────────────────────────────────────────────────────
 * serve()                          Python: serve_forever() last
 *                                  Bun:    MUST precede the ready file
 * ── /the substitution ─────────────────────────────────────────────────────
 * _acquire_ready_lock + write      + print the canonical line         :2153
 * ```
 *
 * Python may publish before serving because `ThreadingHTTPServer.__init__` has
 * already bound *and listened*, and nothing answers on that socket but its own
 * handler.  `BunHttpServer.make` also binds immediately — but it binds with
 * Bun's built-in `404 not found` handler attached, so between `make` and
 * `serve` the port answers 404 from library code (spike S4, asserted).
 * Publishing there would hand out a URL that lies for as long as the layer
 * stack takes to build.  Hence: serve first, publish second.
 *
 * The finalizers unwind in the mirror order — ready file unlinked (identity and
 * pid guarded), then the lock released, then the socket closed — because
 * `ReadyFile.publish` registers on *this* scope and `Scope` releases in
 * reverse acquisition order, which is Python's `finally` block (`:2164-2169`)
 * for free.
 */
export const serveGateway: Effect.Effect<
  GatewayHandle,
  GatewayServeError,
  GatewayServerServices
> = Effect.gen(function* () {
  const config = yield* GatewayConfig;
  const fileSystem = yield* FileSystem.FileSystem;

  const server = yield* BunHttpServer.make({ port: config.port, hostname: config.host });
  const address = server.address;
  const port = address._tag === 'TcpAddress' ? address.port : config.port;

  // `:2092` — created before the gateway serves anything, and never touched
  // again: the loaders own everything inside it.
  yield* fileSystem.makeDirectory(config.cacheRoot, { recursive: true });

  const identity = makeGatewayIdentity({ config, boundPort: port, pid: process.pid });

  // Spike law: the app is attached before anyone is told where to find it.
  yield* server.serve(Effect.provideService(gatewayApp, GatewayIdentity, identity));

  const readyFile = yield* ReadyFile;
  const ready = yield* readyFile.publish(identity.payload);

  return {
    host: config.host,
    port,
    url: Gateway.gatewaySelfUrl(config.host, port),
    identity,
    ready,
  };
});

/**
 * The gateway as a long-running program: start, then stay up until interrupted.
 *
 * `serve_forever()` (`:2158`) has no exit of its own — the Python leaves it
 * through `KeyboardInterrupt` or `stop_event`, and either way the `finally`
 * block runs.  `Effect.never` is that loop; interruption runs the scope's
 * finalizers, which is the same unwind.
 */
export const runGatewayForever: Effect.Effect<never, GatewayServeError, GatewayServerServices> =
  Effect.zipRight(serveGateway, Effect.never);
