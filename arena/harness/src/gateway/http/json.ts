/**
 * `_json` and `_bounded_json` — the two serializers every JSON route ends in.
 *
 * Bare `:NNN` citations are `agent_eval/replay_gateway.py`.
 *
 * ## Why this is a module of its own
 *
 * `_bounded_json` (`:1372-1379`) is eleven lines of Python and it existed three
 * times here: once in `./routes/health.ts` returning a payload, once in
 * `./routes/archive.ts` and once in `./routes/replay.ts`, each returning a
 * response.  All three encoded the same rule — canonicalize, measure the
 * *encoded* bytes, 503 `archiveJsonTooLarge` over the cap, 500 on a value the
 * canonical writer refuses — and each was one edit away from disagreeing with
 * the others about the cap or the status.  That failure mode is not
 * hypothetical in this tree: two copies of `gatewayErrorFromUpstream` had
 * already drifted before anyone noticed.
 *
 * So the rule lives here, once, and the route modules import it.
 *
 * ## Which serializer is per *branch*, not per route
 *
 * `/v1/games` is bounded on its upstream-2xx merge and **unbounded** on both of
 * its disk branches (dossier §5.3, trap B3); `/health` is unbounded (`:1976`).
 * Treating them alike invents a 503 CPython never emits, so both functions are
 * exported and the caller states which one Python called.
 *
 * ## Handlers return values, not responses
 *
 * A route answers with a {@link GatewayJsonPayload} — a status and the exact
 * bytes — or fails with a `../errors.ts` value; `./respond.ts` is the single
 * site that turns either into an `HttpServerResponse`.  {@link
 * jsonPayloadResponse} and {@link boundedJsonResponse} are the *success* half
 * of that, kept here beside the bytes they wrap: they build no error response
 * and they are the reason no route module needs its own header discipline.
 *
 * Serializing is nonetheless the handler's job, deliberately: Python's
 * `_bounded_json` raises `GatewayProblem(503, …)` from inside the route
 * (`:1375`), so the cap has to be applied where the failure can still be a
 * route failure.  By the time bytes reach the renderer the decision is made.
 *
 * @module
 */

import { CANON_UTF8, canonicalBytes, type CanonValue, Gateway } from '@arena/wire';
import type { HttpServerResponse } from '@effect/platform';
import { Either } from 'effect';
import { MAX_PROXY_JSON_BYTES } from '../constants.ts';
import { ArchiveUnavailable, InternalError } from '../errors.ts';
import { gatewayJsonResponse } from './respond.ts';

// ---------------------------------------------------------------------------
// What a JSON route answers with
// ---------------------------------------------------------------------------

/**
 * Where the bytes came from.
 *
 * Not a header and not a status — the response is byte-identical either way,
 * because `_send` (`:1357`) forces `application/json; charset=utf-8`,
 * `Cache-Control: no-store` and the security pair on *both* paths and computes
 * `Content-Length` from the retained bytes (§6).  It is carried so a test can
 * assert *provenance* rather than infer it: "this body was relayed" and "this
 * body happens to equal what we would have built" are different claims, and
 * only the first one proves the 2xx passthrough never re-serialized.
 */
export type GatewayJsonSource =
  /** `_json` / `_bounded_json` — the gateway canonicalized it. */
  | 'gateway'
  /** `_send(status, body, …)` — the upstream's exact bytes (`:1898`). */
  | 'upstream';

/**
 * The success value of every JSON route: a status and the bytes that will be
 * written under it.
 *
 * `status` is not always 200.  A relayed 2xx carries the *upstream's* status
 * verbatim (`_send(status, …)`, not `_send(200, …)`), so a 201 or a 204 from
 * the supervisor reaches the client as itself (§6).
 */
export interface GatewayJsonPayload {
  readonly status: number;
  readonly body: Uint8Array;
  readonly source: GatewayJsonSource;
}

/** `HTTPStatus.OK` — the status both serializers below are always called with. */
export const GATEWAY_OK_STATUS = 200;

/**
 * A 2xx body relayed byte for byte (`_send`, `:1898` / `:1666` / `:1742` /
 * `:1830`).
 *
 * The bytes are never decoded, never re-encoded and never revalidated: upstream
 * key order, whitespace, float spelling and non-ASCII escaping all survive,
 * which is what `test_watch_json_is_byte_preserved_and_credentials_are_not_forwarded`
 * asserts by `body == upstream.watch_body`.
 */
export const relayedJsonPayload = (status: number, body: Uint8Array): GatewayJsonPayload => ({
  status,
  body,
  source: 'upstream',
});

// ---------------------------------------------------------------------------
// _json / _bounded_json
// ---------------------------------------------------------------------------

/**
 * `_canonical(value)` (`:123-126`) — `sort_keys=True`, `(",", ":")`,
 * `ensure_ascii=False`, UTF-8.
 *
 * The failure is Python's `UnicodeEncodeError` / `ValueError` escaping
 * `_canonical` *inside* the handler, which the `except Exception` at `:2040`
 * turns into a 500 with the fixed message.  Unreachable from the projections —
 * they emit no lone surrogates, no `NaN`, no 1000-deep nesting — and modelled
 * anyway so that this module contains no partial function.
 */
const canonicalPayload = (value: CanonValue): Either.Either<Uint8Array, InternalError> =>
  Either.mapLeft(canonicalBytes(value, CANON_UTF8), (cause) => new InternalError({ cause }));

/**
 * `_json(200, value)` (`:1367`) — canonical bytes, **no size ceiling**.
 *
 * The unbounded branches are `/health` (`:1976`), `/v1/games` when the upstream
 * is offline (`:1604`) and `/v1/games` on an upstream 404/405 (`:1650`).  All
 * three can legally exceed 8 MiB; only the merge branch is capped, and treating
 * them alike would invent a 503 CPython never emits (dossier §5.3).
 *
 * An `Either`, which is an `Effect` with no requirements: the callers that want
 * one get it for free, and the callers that want to decide synchronously (the
 * two response builders below) do not have to run anything.
 */
export const gatewayJson = (
  value: CanonValue,
): Either.Either<GatewayJsonPayload, InternalError> =>
  Either.map(canonicalPayload(value), (body) => ({
    status: GATEWAY_OK_STATUS,
    body,
    source: 'gateway',
  }));

/**
 * `_bounded_json(200, value)` (`:1372`) — canonicalize, *then* measure, then
 * refuse.
 *
 * The order is observable: the body is built in full before it is rejected, so
 * the cap is on the response rather than on the work.  Over the cap is a **503
 * `archive JSON response is too large`** — a different status, message and
 * subject from the 502 an oversized *upstream* body produces (`:1527`/`:1535`).
 * Wire names them `archiveJsonTooLarge` and `upstreamJsonTooLarge`; merging
 * them is dossier §5.2's named trap.
 */
export const boundedGatewayJson = (
  value: CanonValue,
): Either.Either<GatewayJsonPayload, ArchiveUnavailable | InternalError> =>
  Either.flatMap(gatewayJson(value), (payload) =>
    payload.body.byteLength > MAX_PROXY_JSON_BYTES
      ? Either.left(new ArchiveUnavailable({ problem: 'archiveJsonTooLarge' }))
      : Either.right(payload),
  );

// ---------------------------------------------------------------------------
// The success responses
// ---------------------------------------------------------------------------

/**
 * A gateway-serialized JSON payload as the response `_send` would have written
 * (`:1357-1370`).
 *
 * Not an error path and not a second response site: `../errors.ts` values are
 * still rendered only by `./respond.ts#respondGateway`.  This is the success
 * half, so that a handler can hand back *bytes plus status* — a shape a test
 * can assert provenance on, which a finished response cannot carry.
 * `Cache-Control: no-store` is unconditional on gateway-built JSON, success
 * bodies included (dossier §7.1).
 */
export const jsonPayloadResponse = (
  payload: GatewayJsonPayload,
): HttpServerResponse.HttpServerResponse =>
  gatewayJsonResponse({
    status: payload.status,
    body: payload.body,
    cacheControl: Gateway.GATEWAY_PROBLEM_CACHE_CONTROL,
  });

/**
 * {@link boundedGatewayJson} straight to a response, for the route families
 * that answer with one rather than with a payload.
 *
 * The single spelling of `_bounded_json`-then-`_send`: `./routes/archive.ts`
 * and `./routes/replay.ts` both call this, and neither may re-derive the cap.
 */
export const boundedJsonResponse = (
  value: CanonValue,
): Either.Either<
  HttpServerResponse.HttpServerResponse,
  ArchiveUnavailable | InternalError
> => Either.map(boundedGatewayJson(value), jsonPayloadResponse);
