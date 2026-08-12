/**
 * The single site where a gateway error becomes a response.
 *
 * ## One response site
 *
 * `agent_eval/replay_gateway.py` funnels every non-2xx body through `_problem`
 * (`:1381`), reached from exactly two places: the `except GatewayProblem` at
 * `:2038` and the `except Exception` at `:2040`.  No handler writes an error;
 * handlers `raise`, and the router edge renders.  The behavior dossier calls
 * this the port's first invariant, and it is the reason this module exists as a
 * module: {@link toResponse} is the *only* function in the harness that turns a
 * failure into bytes, and {@link respondGateway} is the only place it is
 * called.  A route module that imports `HttpServerResponse` to build a 404 has
 * broken the invariant even if the bytes happen to match.
 *
 * ## What is fixed, and what is per class
 *
 * `_send_headers` (`:1335-1355`) writes, in this order: the content type, the
 * content length, `X-Content-Type-Options: nosniff`, `Referrer-Policy:
 * no-referrer`, then either the caller's header mapping or — when there is none
 * and the status is ≥ 400 — `Cache-Control: no-store`.  The security pair is
 * unconditional on *every* response the gateway writes, proxied bodies
 * included; the Python suite never asserts it (pytest dossier U-H1/U-H2), so it
 * is asserted here.
 *
 * Two things vary by class and are therefore tables rather than constants:
 * {@link PROBLEM_CACHE_CONTROL} and {@link PROBLEM_HEADERS}.  Today the first is
 * uniformly `no-store` and the second is empty except for the 405's
 * `Allow: GET` (`:2046-2057`) — the tables are keyed by tag so that a class
 * that ever needs to differ has one place to say so, and so that the test can
 * prove the current answer for every tag rather than for the one that was
 * remembered.
 *
 * ## Bytes, not objects
 *
 * The body is `_canonical({"error": message})` — sorted keys, `(",", ":")`
 * separators, `ensure_ascii=False`, UTF-8 — produced by
 * `@arena/wire`'s {@link Gateway.gatewayProblemBytes} rather than by
 * `JSON.stringify`, because the parity rig compares bytes.  `Content-Length` is
 * the length of those bytes; the Bun adapter derives it from the body and
 * strips the body (only the body) from a HEAD, which is how `HEAD /health`
 * keeps `Content-Length: 30` while sending nothing (dossier §7.2, §15).
 *
 * @module
 */

import { Gateway } from '@arena/wire';
import { HttpServerResponse } from '@effect/platform';
import { Cause, Effect, Either } from 'effect';
import { GATEWAY_SECURITY_HEADERS } from '../constants.ts';
import {
  type GatewayError,
  type GatewayErrorTag,
  gatewayProblem,
  InternalError,
} from '../errors.ts';

// ---------------------------------------------------------------------------
// Headers
// ---------------------------------------------------------------------------

/**
 * The two headers `_send_headers` adds to everything (`:1347-1348`).
 *
 * Unconditional means unconditional: gateway-built JSON, relayed upstream
 * bodies, and archive files all carry them.  They are the only headers the
 * gateway adds that are not about the body it is sending.
 *
 * Note what is *absent* and must stay absent: no `Access-Control-*`, no `Vary`,
 * on any route (§7.4).
 *
 * Re-exported from `../constants.ts`, which is the module whose docstring
 * claims to hold the service's vocabulary; this module used to declare a second
 * copy in a different shape, and the two could drift with nothing to notice.
 */
export { GATEWAY_SECURITY_HEADERS } from '../constants.ts';

/** Add the security pair to a response built elsewhere (proxy and file paths). */
export const withSecurityHeaders = (response: HttpServerResponse.HttpServerResponse): HttpServerResponse.HttpServerResponse =>
  HttpServerResponse.setHeaders(response, GATEWAY_SECURITY_HEADERS);

/**
 * `Cache-Control` per error class.
 *
 * Uniformly `no-store` today: `_problem` goes through `_json`, which passes no
 * header mapping, so `_send_headers`' `status >= 400` default fires for all of
 * them (`:1353-1354`).  Keyed by tag anyway — the mapped type makes the table
 * total, so a new class cannot be added to the taxonomy without answering this.
 */
export const PROBLEM_CACHE_CONTROL: { readonly [T in GatewayErrorTag]: string } = {
  ArchiveUnavailable: Gateway.GATEWAY_PROBLEM_CACHE_CONTROL,
  BadRequest: Gateway.GATEWAY_PROBLEM_CACHE_CONTROL,
  InternalError: Gateway.GATEWAY_PROBLEM_CACHE_CONTROL,
  MethodNotAllowed: Gateway.GATEWAY_PROBLEM_CACHE_CONTROL,
  NotFound: Gateway.GATEWAY_PROBLEM_CACHE_CONTROL,
  UpstreamHttpError: Gateway.GATEWAY_PROBLEM_CACHE_CONTROL,
  UpstreamInvalid: Gateway.GATEWAY_PROBLEM_CACHE_CONTROL,
  UpstreamTooLarge: Gateway.GATEWAY_PROBLEM_CACHE_CONTROL,
  UpstreamUnavailable: Gateway.GATEWAY_PROBLEM_CACHE_CONTROL,
};

const NO_EXTRA_HEADERS: Readonly<Record<string, string>> = {};

/**
 * Headers a class adds beyond the security pair and its cache control.
 *
 * Only the 405 has any: `Allow: GET` (`:2050`), which
 * `test_private_routes_and_all_mutations_never_reach_upstream` asserts by exact
 * value for all six mapped verbs — HEAD included.
 */
export const PROBLEM_HEADERS: { readonly [T in GatewayErrorTag]: Readonly<Record<string, string>> } =
  {
    ArchiveUnavailable: NO_EXTRA_HEADERS,
    BadRequest: NO_EXTRA_HEADERS,
    InternalError: NO_EXTRA_HEADERS,
    MethodNotAllowed: { allow: Gateway.GATEWAY_METHOD_NOT_ALLOWED_ALLOW },
    NotFound: NO_EXTRA_HEADERS,
    UpstreamHttpError: NO_EXTRA_HEADERS,
    UpstreamInvalid: NO_EXTRA_HEADERS,
    UpstreamTooLarge: NO_EXTRA_HEADERS,
    UpstreamUnavailable: NO_EXTRA_HEADERS,
  };

// ---------------------------------------------------------------------------
// Bodies
// ---------------------------------------------------------------------------

const encodeAscii = (text: string): Uint8Array => new TextEncoder().encode(text);

/**
 * `{"error": "the replay gateway encountered an internal error"}` as bytes.
 *
 * The `getOrElse` arm is unreachable — the message is ASCII and
 * `canonicalBytes` only refuses lone surrogates, non-finite floats and depth —
 * but it is written rather than asserted so that this module has no partial
 * function in it at all.
 */
const INTERNAL_ERROR_BODY: Uint8Array = Either.getOrElse(
  Gateway.gatewayProblemBytes(Gateway.GATEWAY_PROBLEM_MESSAGES.internalError),
  () => encodeAscii(`{"error":${JSON.stringify(Gateway.GATEWAY_PROBLEM_MESSAGES.internalError)}}`),
);

/** A status and the exact bytes that will be written under it. */
export interface RenderedProblem {
  readonly status: number;
  readonly body: Uint8Array;
}

/**
 * The problem's canonical body, or the 500 when the message cannot be encoded.
 *
 * Python's `_canonical` calls `.encode("utf-8")`, which raises
 * `UnicodeEncodeError` on a lone surrogate; the raise happens *inside* the
 * `except GatewayProblem` handler, so the request ends as a 500 with no body of
 * its own choosing (`problem.ts` trap T1).  Reproduced here as a value: an
 * unencodable message downgrades to the internal-error body, and the status
 * goes with it.
 *
 * Unreachable from the taxonomy — every catalogue message is ASCII, and the one
 * open form is `upstream returned HTTP ${number}` — so this exists to keep the
 * renderer total, and the test drives it directly.
 */
export const renderProblemBody = (problem: Gateway.GatewayProblemResponse): RenderedProblem =>
  Either.match(Gateway.gatewayProblemBytes(problem.message), {
    onLeft: (): RenderedProblem => ({ status: 500, body: INTERNAL_ERROR_BODY }),
    onRight: (body): RenderedProblem => ({ status: problem.status, body }),
  });

// ---------------------------------------------------------------------------
// Responses
// ---------------------------------------------------------------------------

/**
 * A gateway-built JSON response: canonical bytes in, `_send`'s headers out
 * (`:1357-1370`).
 *
 * Success bodies (`_json`, `_bounded_json`) and problem bodies take the same
 * path in Python and take the same path here — `Content-Type: application/json;
 * charset=utf-8`, the security pair, and a caller-chosen `Cache-Control` which
 * is `no-store` for everything the gateway serializes itself.  Bounding the
 * body at 8 MiB is the caller's job (`_bounded_json`, `:1372-1379`); by the
 * time bytes reach here the decision is made.
 */
export const gatewayJsonResponse = (options: {
  readonly status: number;
  readonly body: Uint8Array;
  readonly cacheControl: string;
  readonly headers?: Readonly<Record<string, string>>;
}): HttpServerResponse.HttpServerResponse =>
  HttpServerResponse.uint8Array(options.body, {
    status: options.status,
    contentType: Gateway.GATEWAY_PROBLEM_CONTENT_TYPE,
    headers: {
      ...GATEWAY_SECURITY_HEADERS,
      ...(options.headers ?? NO_EXTRA_HEADERS),
      'cache-control': options.cacheControl,
    },
  });

/**
 * THE renderer: a failure value becomes the response, and nothing else does.
 *
 * Total by construction — the status and message come from
 * {@link gatewayProblem}, the cache control and extra headers from tables the
 * type system keeps total over the taxonomy, and the body from a byte encoder
 * that cannot fail.  Nothing about the error's `cause` reaches the response.
 */
export const toResponse = (error: GatewayError): HttpServerResponse.HttpServerResponse => {
  const rendered = renderProblemBody(gatewayProblem(error));
  return gatewayJsonResponse({
    status: rendered.status,
    body: rendered.body,
    cacheControl: PROBLEM_CACHE_CONTROL[error._tag],
    headers: PROBLEM_HEADERS[error._tag],
  });
};

// ---------------------------------------------------------------------------
// The router edge
// ---------------------------------------------------------------------------

/**
 * `except GatewayProblem` (`:2038`): the declared failures become responses.
 *
 * Applied once, to the whole dispatch, so that a handler's `E` channel is its
 * only way of refusing a request.
 */
export const catchGatewayErrors = <R>(
  self: Effect.Effect<HttpServerResponse.HttpServerResponse, GatewayError, R>,
): Effect.Effect<HttpServerResponse.HttpServerResponse, never, R> =>
  Effect.catchAll(self, (error) => Effect.succeed(toResponse(error)));

/**
 * `except Exception` (`:2040`): a defect becomes a 500 with the fixed message.
 *
 * Three properties, in the order they bite:
 *
 * 1. **Nothing leaks.**  The cause is logged and dropped; the body is the
 *    catalogue's `internalError` and nothing else.  Python's promise
 *    (`test_events_loader_failures_stay_public`) is that a raised message
 *    containing a private path never reaches a client.
 * 2. **Interruption stays interruption.**  A cancelled fiber is not a server
 *    error; rendering one as a 500 would make shutdown look like a fault, so an
 *    interrupted-only cause is re-failed unchanged.
 * 3. **It runs outermost.**  Applied after {@link catchGatewayErrors}, so a
 *    defect raised while rendering a problem still lands here.
 */
export const catchGatewayDefects = <R>(
  self: Effect.Effect<HttpServerResponse.HttpServerResponse, never, R>,
): Effect.Effect<HttpServerResponse.HttpServerResponse, never, R> =>
  Effect.catchAllCause(self, (cause) =>
    Cause.isInterruptedOnly(cause)
      ? Effect.failCause(cause)
      : Effect.zipRight(
          Effect.logError('gateway request failed', cause),
          Effect.succeed(toResponse(new InternalError({ cause }))),
        ),
  );

/**
 * The pair, in the order `do_GET`'s two `except` clauses sit: declared failures
 * first, defects outside them.  This is what the server pipeline wraps the
 * dispatch in — one call, one response site.
 */
export const respondGateway = <R>(
  self: Effect.Effect<HttpServerResponse.HttpServerResponse, GatewayError, R>,
): Effect.Effect<HttpServerResponse.HttpServerResponse, never, R> =>
  catchGatewayDefects(catchGatewayErrors(self));
