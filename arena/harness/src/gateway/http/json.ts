/**
 * Canonical JSON payloads. Only `_bounded_json` branches apply the 8 MiB post-encoding ceiling;
 * relayed bodies bypass serialization and preserve their status and bytes.
 */

import { CANON_UTF8, canonicalBytes, type CanonValue, Gateway } from '@arena/wire';
import type { HttpServerResponse } from '@effect/platform';
import { Either, Option, Predicate } from 'effect';
import { MAX_PROXY_JSON_BYTES } from '../constants.ts';
import { ArchiveUnavailable, InternalError } from '../errors.ts';
import { isCanonRecord } from '../python-json.ts';
import { gatewayJsonResponse } from './respond.ts';

// ---------------------------------------------------------------------------
// What a JSON route answers with
// ---------------------------------------------------------------------------

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
}

// ---------------------------------------------------------------------------
// _json / _bounded_json
// ---------------------------------------------------------------------------

/**
 * `_canonical(value)` (`:123-126`) — `sort_keys=True`, `(",", ":")`,
 * `ensure_ascii=False`, UTF-8.
 *
 * Python's default `json.dumps` also emits `NaN` and `±Infinity`. Wire's
 * repository-wide canonical writer deliberately uses `allow_nan=False`, so
 * this gateway-only fallback substitutes those tokens after canonicalizing a
 * collision-free string marker. Ordinary values still use the shared writer.
 */
const pythonNonFiniteBytes = (value: CanonValue): Option.Option<Uint8Array> => {
  const strings = new Set<string>();
  const rememberStrings = (item: CanonValue): void => {
    if (Predicate.isString(item)) strings.add(item);
    else if (Array.isArray(item)) item.forEach(rememberStrings);
    else if (isCanonRecord(item)) {
      Object.entries(item).forEach(([key, child]) => {
        strings.add(key);
        rememberStrings(child);
      });
    }
  };
  rememberStrings(value);

  const markers = new Map<string, string>();
  let next = 0;
  const freshMarker = (): string => {
    let marker = '';
    do {
      marker = `__arena_python_nonfinite_${String(next)}__`;
      next += 1;
    } while (strings.has(marker));
    strings.add(marker);
    return marker;
  };
  const replace = (item: CanonValue): CanonValue => {
    if (Predicate.isNumber(item) && !Number.isFinite(item)) {
      const marker = freshMarker();
      markers.set(marker, Number.isNaN(item) ? 'NaN' : item > 0 ? 'Infinity' : '-Infinity');
      return marker;
    }
    if (Array.isArray(item)) return item.map(replace);
    if (isCanonRecord(item)) {
      return Object.fromEntries(Object.entries(item).map(([key, child]) => [key, replace(child)]));
    }
    return item;
  };
  const encoded = Either.getRight(canonicalBytes(replace(value), CANON_UTF8));
  if (Option.isNone(encoded) || markers.size === 0) return Option.none();
  let text = new TextDecoder().decode(encoded.value);
  for (const [marker, token] of markers) text = text.replaceAll(JSON.stringify(marker), token);
  return Option.some(new TextEncoder().encode(text));
};

const canonicalPayload = (value: CanonValue): Either.Either<Uint8Array, InternalError> =>
  Either.match(canonicalBytes(value, CANON_UTF8), {
    onLeft: (cause) => Option.match(pythonNonFiniteBytes(value), {
      onNone: () => Either.left(new InternalError({ cause })),
      onSome: Either.right,
    }),
    onRight: Either.right,
  });

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
  Either.map(canonicalPayload(value), (body) => ({ status: 200, body }));

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
