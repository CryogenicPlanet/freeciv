/**
 * `UpstreamClient` — the gateway's only door to the Freeciv supervisor.
 *
 * This is the port of `_open_upstream` / `_upstream_json_or_status` /
 * `_read_json_response` / `_stream_upstream`'s reading half
 * (`agent_eval/replay_gateway.py:1398-1552`).  Every bare `:NNN` below cites
 * that file.
 *
 * ## What this module is responsible for
 *
 * Exactly one thing: turning "the gateway wants `path?query` from upstream"
 * into either bytes, a status, or a typed failure — **without ever buffering
 * more than it is allowed to**.  Two entry points, because Python has two:
 *
 * - {@link UpstreamClientApi.jsonOrStatus} (`:1541`) — 2xx bodies are read
 *   incrementally and retained up to `MAX_PROXY_JSON_BYTES + 1` bytes so the
 *   `len(value) > MAX` test (`:1534`) can be made without a 9 MiB string ever
 *   existing; non-2xx bodies are read-and-discarded up to
 *   `MAX_PROXY_ERROR_BYTES` (`:1550`) and the status comes back as a value,
 *   because 404/405 are *fallback triggers* for the routes, not errors.
 * - {@link UpstreamClientApi.openBinary} (`:1938`) — a `Scope`-owned reader
 *   plus the `PROXY_RESPONSE_HEADERS` subset (`:56-62`), for the frame/video
 *   routes that stream rather than buffer.
 *
 * ## What it is deliberately NOT responsible for
 *
 * - **It builds no responses.**  Failures are `Data.TaggedError` values
 *   carrying the *public* problem (`status` + message from
 *   `@arena/wire`'s `GATEWAY_PROBLEM_MESSAGES`) through {@link UpstreamFailure};
 *   the one response site at the router edge renders them.  Nothing here
 *   knows what a `Response` looks like.
 * - **It does not decide fallback.**  A 404 is a value; whether that means
 *   "serve the archive" (`_archive_json_route`) or "serve the raw disk index"
 *   (`_games`) differs per route family and belongs to the routes.
 * - **It never decodes a body.**  2xx bytes are `Uint8Array` end to end
 *   (`:1898` relays them verbatim); a `Response.text()` round trip would
 *   destroy the byte parity the whole port is measured on.
 *
 * ## The three subtleties that are easy to lose in a port
 *
 * 1. **The Portless probe (`:1419-1432`) is not "any 502".**  All three of
 *    `status == 502`, `X-Portless: 1` and a media type of exactly `text/html`
 *    are required before the gateway is allowed to say "offline" and reach for
 *    disk.  Two of the three is an ordinary 502 relay — see
 *    {@link isPortlessOffline}.
 * 2. **A redirect is never followed and never relayed** (`:102-106`).  The
 *    fetch is `redirect: 'manual'`, and on the JSON path a 3xx surfaces as
 *    {@link UpstreamRedirect}, whose problem is wire's
 *    `upstreamProblem(status)` — a 502 `upstream redirects are not allowed`.
 *    Making it a *failure* rather than a status value is what stops a route
 *    from accidentally treating a 302 as a fallback trigger.  The binary path
 *    keeps it a status ({@link UpstreamBinaryResponse.status}) because
 *    `_stream_upstream` (`:1453`) drains it first and the same
 *    `upstreamProblem` mapping applies at the route.
 * 3. **No inbound header is ever copied** (`:1409`).  The request headers are
 *    a fixed set ({@link upstreamRequestHeaders}); `Authorization`, `Cookie`,
 *    `X-Forwarded-*` and `Range` do not cross this boundary.
 *
 * ## Every failure here has a tag no other module claims
 *
 * These four classes sit in a *different* channel from `../errors.ts`'s
 * taxonomy, and two of them used to share a `_tag` string with a taxonomy class
 * — `UpstreamUnavailable` and `UpstreamHttpError` each existed twice, as two
 * unrelated classes with two different shapes.  A tag is a **runtime**
 * discriminator: `Effect.catchTag('UpstreamHttpError', …)` on a channel that
 * could hold either would dispatch on the string, read `error.status` off a
 * value that has none, and render `status: undefined`.  The type system kept
 * the channels apart, so it was latent — and duplicating a tag defeats the one
 * guarantee `Data.TaggedError` exists to give.  Renamed to
 * {@link UpstreamOffline} and {@link UpstreamRedirect}, which are also the
 * better names: this module reports *what happened on the socket*, and
 * `../errors.ts` reports *what the client is told*.
 *
 * ## Divergences from CPython, chosen deliberately
 *
 * - Python's socket timeout applies per read, and a read that times out
 *   escapes `_upstream_json_or_status` uncaught → the `do_GET` catch-all
 *   (`:2040`) → **500 internal error**, not a 502.  That is reproduced:
 *   {@link UpstreamBodyError} carries the 500, while only the request phase
 *   maps to {@link UpstreamOffline} (`:1434`).
 * - `Headers.get` joins duplicate response headers with `", "` where Python's
 *   `email.message.Message.get` returns the first.  Only reachable when an
 *   upstream sends a duplicate `ETag`/`Cache-Control`; not worth a raw-header
 *   parser.
 * - The Portless probe's drain is `Effect.ignore`d: in Python a read error
 *   inside that `except` block escapes to the 500 handler, which is a strictly
 *   worse answer than the 502 the probe already decided on.
 *
 * @module
 */

import { Gateway } from '@arena/wire';
import { Chunk, Context, Data, Duration, Effect, Layer, Option, Ref, type Scope, Stream } from 'effect';
import {
  DEFAULT_UPSTREAM_TIMEOUT_SECONDS,
  MAX_PROXY_ERROR_BYTES,
  MAX_PROXY_JSON_BYTES,
  PROXY_RESPONSE_HEADERS,
  UPSTREAM_ACCEPT_BINARY,
  UPSTREAM_ACCEPT_JSON,
  UPSTREAM_PORTLESS_HEADER,
  UPSTREAM_PORTLESS_MEDIA_TYPE,
  UPSTREAM_PORTLESS_VALUE,
} from '../constants.ts';

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------
//
// Transcribed once, in `../constants.ts`, and imported here.  This module used
// to re-declare all six of them plus the portless triple inline; the copies
// were the ones that ran, so raising the 8 MiB cap in the module documented as
// the service's vocabulary moved the *outbound* `_bounded_json` limit and left
// the *inbound* upstream read limit where it was — and five separate mutations
// of `constants.ts` survived the whole suite because nothing read it.

/**
 * `config.upstream_timeout_s` default (`:2181`), as a `Duration`.
 *
 * Derived from the number in `../constants.ts` rather than restated, so the two
 * cannot disagree about how long ten seconds is.
 */
export const DEFAULT_UPSTREAM_TIMEOUT: Duration.Duration = Duration.seconds(
  DEFAULT_UPSTREAM_TIMEOUT_SECONDS,
);

// ---------------------------------------------------------------------------
// Python `int()`
// ---------------------------------------------------------------------------

const PYTHON_INT_RE = /^[+-]?\p{Nd}(?:_?\p{Nd})*$/u;
const DECIMAL_DIGIT_RE = /^\p{Nd}$/u;
const DIGIT_OFFSETS = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9] as const;

const digitValue = (character: string): bigint => {
  const code = character.codePointAt(0) ?? 0;
  // Every Unicode decimal block is ten contiguous code points starting at its
  // own zero, so the value is the distance down to the first non-digit.
  const offset = DIGIT_OFFSETS.find(
    (candidate) => !DECIMAL_DIGIT_RE.test(String.fromCodePoint(code - candidate - 1)),
  );
  return BigInt(offset ?? 0);
};

/**
 * Python's `int(str)`, which is **not** `Number()`: `"5_0"` is 50, `"+3"` is 3,
 * `" 5 "` is 5, and Unicode decimal digits (`"١٢"`) parse.
 *
 * Reachable here from an upstream `Content-Length` (`:1523`), where a value
 * that fails to parse is *ignored* rather than fatal (`except ValueError:
 * pass`, `:1531`) — hence `Option` rather than a failure.
 *
 * `bigint`, because Python's `int` is unbounded and the value is compared
 * against a cap; a `Content-Length` of `10**30` must not round to something
 * smaller.
 *
 * @remarks Also the parser the query and `Content-Length` admission code needs
 * (trap B4).  There is one of it: `../server.ts#bodySignal`,
 * `../http/routes/replay.ts#replayQuery` and `../http/routes/board.ts` all
 * import this function rather than transcribing `int()` again.
 */
export const parsePythonInt = (text: string): Option.Option<bigint> => {
  const trimmed = text.trim();
  if (!PYTHON_INT_RE.test(trimmed)) return Option.none();
  const signed = trimmed.startsWith('+') || trimmed.startsWith('-');
  const digits = [...(signed ? trimmed.slice(1) : trimmed)].filter(
    (character) => character !== '_',
  );
  const magnitude = digits.reduce((total, character) => total * 10n + digitValue(character), 0n);
  return Option.some(trimmed.startsWith('-') ? -magnitude : magnitude);
};

// ---------------------------------------------------------------------------
// Failures
// ---------------------------------------------------------------------------

const problemOf = (name: Gateway.GatewayProblemName): Gateway.GatewayProblemResponse => {
  const message = Gateway.GATEWAY_PROBLEM_MESSAGES[name];
  return { status: Gateway.GATEWAY_PROBLEM_STATUS[message], message };
};

/**
 * `UpstreamOffline` (`:85`, raised at `:1429`/`:1435`) — the upstream is
 * *provably gone*, which is the one condition (besides a 404/405) that lets a
 * route serve the archive instead.
 *
 * `reason` records which of the two Python paths this was: a transport failure
 * (`except (OSError, http.client.HTTPException)`), the socket timeout, or the
 * Portless probe.  Routes must not branch on it — it exists for telemetry and
 * for tests that need to prove the probe fired rather than the connect.
 */
export class UpstreamOffline extends Data.TaggedError('UpstreamOffline')<{
  readonly reason: 'transport' | 'timeout' | 'portless';
  readonly url: string;
  readonly cause: unknown;
}> {
  /** 502 `the upstream Freeciv service is unavailable`. */
  get problem(): Gateway.GatewayProblemResponse {
    return problemOf('upstreamUnavailable');
  }
}

/**
 * `:1527` / `:1535` — the upstream JSON body is over `MAX_PROXY_JSON_BYTES`.
 *
 * `source` distinguishes the two guards, and the distinction is observable:
 * the `Content-Length` guard rejects **before a byte is read** (`bytesRead`
 * is 0), the read guard rejects after crossing the cap.  Neither is an
 * `UpstreamOffline`, so an oversized body never falls back to disk
 * (`test_replay_gateway.py:1133`).
 */
export class UpstreamJsonTooLarge extends Data.TaggedError('UpstreamJsonTooLarge')<{
  readonly source: 'content-length' | 'body';
  readonly capBytes: number;
  readonly bytesRead: number;
  /**
   * How many of {@link bytesRead} were actually *held in memory*.
   *
   * The two differ and the difference is the whole point of `advance`: the
   * socket may overshoot the read budget by up to one chunk, retention never
   * does.  Recorded here because it is otherwise unobservable — the cap test
   * could only assert `bytesRead`, so deleting the truncation in `advance`
   * (making it retain whole chunks past the cap) survived the entire suite.
   */
  readonly bytesRetained: number;
  readonly url: string;
}> {
  /** 502 `the upstream JSON response is too large`. */
  get problem(): Gateway.GatewayProblemResponse {
    return problemOf('upstreamJsonTooLarge');
  }
}

/**
 * A 3xx from upstream (`:102-106` refuses to follow it).
 *
 * Only 3xx surfaces this way.  Every other non-2xx status is a *value*
 * ({@link UpstreamStatus}) because 404 and 405 are fallback triggers and the
 * relay statuses differ per route family; a redirect has exactly one meaning
 * everywhere, so making it unignorable is free correctness.
 */
export class UpstreamRedirect extends Data.TaggedError('UpstreamRedirect')<{
  readonly status: number;
  readonly url: string;
}> {
  /** `upstreamProblem(status)`: 502 `upstream redirects are not allowed` for a 3xx. */
  get problem(): Gateway.GatewayProblemResponse {
    return Gateway.upstreamProblem(this.status);
  }
}

/**
 * The body failed *while being read* — a truncated response, a disconnect, or
 * a read that outran the timeout.
 *
 * In Python this is an `OSError`/`HTTPException` raised outside
 * `_open_upstream`'s try (`:1548`), so it lands on the `do_GET` catch-all
 * (`:2040`) as a **500**, not a 502.  Reproduced, deliberately: "the socket
 * died mid-body" is not the same claim as "the service is unavailable", and
 * only the latter is allowed to substitute disk data for live data.
 */
export class UpstreamBodyError extends Data.TaggedError('UpstreamBodyError')<{
  readonly reason: 'read' | 'timeout';
  readonly url: string;
  readonly cause: unknown;
}> {
  /** 500 `the replay gateway encountered an internal error`. */
  get problem(): Gateway.GatewayProblemResponse {
    return problemOf('internalError');
  }
}

/** Everything {@link UpstreamClient} can fail with. */
export type UpstreamFailure =
  | UpstreamOffline
  | UpstreamJsonTooLarge
  | UpstreamRedirect
  | UpstreamBodyError;

/**
 * The public problem an upstream failure renders as, for the single response
 * site.  Total: adding a failure without a problem is a type error.
 */
export const upstreamFailureProblem = (
  failure: UpstreamFailure,
): Gateway.GatewayProblemResponse => failure.problem;

// ---------------------------------------------------------------------------
// Request / response values
// ---------------------------------------------------------------------------

/**
 * What to ask upstream for.  `path` is the already-dispatched path — the
 * routes hand over `parsed.path` verbatim for the archive routes (trap B1) and
 * a canonically rebuilt path for replay/board/events; `query` is the
 * *normalized* query string (`after_turn=N&limit=N`), empty for no query.
 */
export interface UpstreamRequest {
  readonly path: string;
  readonly query?: string | undefined;
}

/** A 2xx: the exact bytes, never decoded (`:1898` relays them as-is). */
export interface UpstreamBody {
  readonly _tag: 'UpstreamBody';
  readonly status: number;
  readonly body: Uint8Array;
}

/** A non-2xx, non-3xx: the body was drained and dropped, the status is the answer. */
export interface UpstreamStatus {
  readonly _tag: 'UpstreamStatus';
  readonly status: number;
}

/** `_upstream_json_or_status`'s `tuple[int, bytes | None]` (`:1541`), as a union. */
export type UpstreamJson = UpstreamBody | UpstreamStatus;

/** True for the branch that carries bytes. */
export const isUpstreamBody = (value: UpstreamJson): value is UpstreamBody =>
  value._tag === 'UpstreamBody';

/**
 * An open upstream response whose body is still on the socket.
 *
 * The reader is owned by the `Scope` this was acquired in: closing the scope
 * cancels it, which is what actually closes the connection when a route
 * decides (on a 404) that it wants the archive instead.
 */
export interface UpstreamBinaryResponse {
  /**
   * The upstream status, verbatim — including a 3xx.  `_archive_binary_route`
   * (`:1948-1955`) branches on it: 2xx streams, 404/405 falls back to the
   * local archive *without draining*, and anything else drains and renders
   * `upstreamProblem(status)`.  That branch is the route's, not the client's.
   */
  readonly status: number;
  /** The {@link PROXY_RESPONSE_HEADERS} upstream actually sent, in declaration order. */
  readonly headers: ReadonlyArray<readonly [string, string]>;
  /** The body, pulled one chunk at a time; nothing is ever buffered whole. */
  readonly stream: Stream.Stream<Uint8Array, UpstreamBodyError>;
  /**
   * `_stream_upstream`'s non-2xx arm (`:1454`): read and discard at most
   * `MAX_PROXY_ERROR_BYTES`, then stop.  Resolves to the number of bytes taken
   * off the socket.
   */
  readonly drainError: Effect.Effect<number, UpstreamBodyError>;
}

// ---------------------------------------------------------------------------
// Small pure helpers
// ---------------------------------------------------------------------------

/** `2xx`. */
export const isSuccessStatus = (status: number): boolean => status >= 200 && status < 300;

/** `3xx` — never followed, never relayed. */
export const isRedirectStatus = (status: number): boolean => status >= 300 && status < 400;

/** `content_type.split(";", 1)[0].strip().lower()` (`:1424`). */
export const mediaType = (contentType: string | null): string =>
  (contentType ?? '').split(';', 1)[0]?.trim().toLowerCase() ?? '';

/**
 * The Portless probe (`:1419-1432`): a port-forwarder announcing that its
 * target is gone.  All three conditions, or it is an ordinary 502 to relay.
 */
export const isPortlessOffline = (response: Response): boolean =>
  response.status === 502 &&
  response.headers.get(UPSTREAM_PORTLESS_HEADER) === UPSTREAM_PORTLESS_VALUE &&
  mediaType(response.headers.get('Content-Type')) === UPSTREAM_PORTLESS_MEDIA_TYPE;

/**
 * `_upstream_url` (`:1398`) — plain concatenation, so a configured path prefix
 * is prepended to everything and a trailing slash handed in by the dispatcher
 * is forwarded verbatim (trap B1).  `service_url` must already be normalized
 * (no trailing slash).
 */
export const upstreamUrl = (serviceUrl: string, request: UpstreamRequest): string => {
  const base = serviceUrl + request.path;
  const query = request.query ?? '';
  return query === '' ? base : `${base}?${query}`;
};

/**
 * The complete set of request headers (`:1409`).  Nothing inbound is copied.
 *
 * `Accept-Encoding: identity` is what urllib sends and is load-bearing for
 * byte parity: a negotiated `gzip` would hand us a body the upstream did not
 * write and a `Content-Length` that does not describe it.
 */
export const upstreamRequestHeaders = (accept: string): Record<string, string> => ({
  Accept: accept,
  'Accept-Encoding': 'identity',
});

/** The allowlisted response headers, in `PROXY_RESPONSE_HEADERS` order. */
export const proxyHeaders = (headers: Headers): ReadonlyArray<readonly [string, string]> =>
  PROXY_RESPONSE_HEADERS.flatMap((name) => {
    const value = headers.get(name);
    return value === null ? [] : [[name, value] as const];
  });

// ---------------------------------------------------------------------------
// Bounded reading
// ---------------------------------------------------------------------------

/**
 * The structural slice of `ReadableStreamDefaultReader` used here.
 *
 * Structural on purpose: naming the DOM type at the `getReader()` call site
 * makes TypeScript pick the BYOB overload (spike S5), and a null body needs a
 * stand-in reader anyway.
 */
interface BodyReader {
  readonly read: () => Promise<{ readonly done: boolean; readonly value?: Uint8Array | undefined }>;
  readonly cancel: () => Promise<void>;
}

const EMPTY_READER: BodyReader = {
  read: () => Promise.resolve({ done: true }),
  cancel: () => Promise.resolve(),
};

interface ReadState {
  readonly chunks: Chunk.Chunk<Uint8Array>;
  readonly retained: number;
  readonly read: number;
}

const EMPTY_READ: ReadState = { chunks: Chunk.empty(), retained: 0, read: 0 };

interface ReadLimits {
  /** Stop pulling once this many bytes have come off the socket. */
  readonly readBytes: number;
  /** Keep at most this many of them. */
  readonly retainBytes: number;
}

const advance = (state: ReadState, chunk: Uint8Array, retainBytes: number): ReadState => {
  const room = retainBytes - state.retained;
  // `slice` copies: a retained chunk must not alias a buffer the reader may reuse.
  const keep =
    room <= 0 ? null : chunk.byteLength <= room ? chunk.slice() : chunk.slice(0, room);
  return {
    chunks: keep === null ? state.chunks : Chunk.append(state.chunks, keep),
    retained: state.retained + (keep === null ? 0 : keep.byteLength),
    read: state.read + chunk.byteLength,
  };
};

const concatBytes = (chunks: ReadonlyArray<Uint8Array>, total: number): Uint8Array =>
  chunks.reduce<{ readonly out: Uint8Array; readonly offset: number }>(
    (accumulator, chunk) => {
      accumulator.out.set(chunk, accumulator.offset);
      return { out: accumulator.out, offset: accumulator.offset + chunk.byteLength };
    },
    { out: new Uint8Array(total), offset: 0 },
  ).out;

/**
 * One chunk, bounded by the configured timeout.
 *
 * `Option.none()` is end of stream.  The timeout is `Effect.timeoutFail`
 * rather than `AbortSignal.timeout`, so a test can drive it with `TestClock`
 * instead of waiting ten real seconds.
 */
const readChunk = (
  reader: BodyReader,
  url: string,
  timeout: Duration.Duration,
): Effect.Effect<Option.Option<Uint8Array>, UpstreamBodyError> =>
  Effect.tryPromise({
    try: () => reader.read(),
    catch: (cause) => new UpstreamBodyError({ reason: 'read', url, cause }),
  }).pipe(
    Effect.timeoutFail({
      duration: timeout,
      onTimeout: () => new UpstreamBodyError({ reason: 'timeout', url, cause: null }),
    }),
    Effect.map((result) =>
      result.done || result.value === undefined ? Option.none() : Option.some(result.value),
    ),
  );

const readLoop = (
  reader: BodyReader,
  url: string,
  limits: ReadLimits,
  timeout: Duration.Duration,
  state: Ref.Ref<ReadState>,
): Effect.Effect<ReadState, UpstreamBodyError> =>
  Ref.get(state).pipe(
    Effect.flatMap((current) =>
      current.read >= limits.readBytes
        ? Effect.succeed(current)
        : readChunk(reader, url, timeout).pipe(
            Effect.flatMap(
              Option.match({
                onNone: () => Effect.succeed(current),
                onSome: (chunk) =>
                  Ref.update(state, (previous) =>
                    advance(previous, chunk, limits.retainBytes),
                  ).pipe(Effect.flatMap(() => readLoop(reader, url, limits, timeout, state))),
              }),
            ),
          ),
    ),
  );

/**
 * Read at most `limits.readBytes` off the socket, retaining at most
 * `limits.retainBytes` of them.
 *
 * The loop stops on the first chunk that reaches the read budget, so the
 * *socket* may overshoot by up to one chunk while *retention* never does —
 * which is exactly the shape spike S5 measured, and the reason nothing here
 * can be made to hold 9 MiB.
 */
const readBounded = (
  reader: BodyReader,
  url: string,
  limits: ReadLimits,
  timeout: Duration.Duration,
): Effect.Effect<ReadState, UpstreamBodyError> =>
  Ref.make(EMPTY_READ).pipe(
    Effect.flatMap((state) => readLoop(reader, url, limits, timeout, state)),
  );

/**
 * A reader owned by the enclosing `Scope`: the release cancels it, which runs
 * on success, failure and interruption alike.  Without that, abandoning a
 * response mid-body leaks the connection.
 */
const acquireReader = (
  response: Response,
): Effect.Effect<BodyReader, never, Scope.Scope> =>
  Effect.acquireRelease(
    Effect.sync((): BodyReader => response.body?.getReader() ?? EMPTY_READER),
    // `tryPromise`, not `promise`: cancelling a reader whose stream already
    // errored rejects, and `Effect.promise` would turn that into a *defect*
    // that escapes `Effect.ignore` and poisons the scope's exit.
    (reader) => Effect.ignore(Effect.tryPromise(() => reader.cancel())),
  );

const bodyStream = (
  reader: BodyReader,
  url: string,
  timeout: Duration.Duration,
): Stream.Stream<Uint8Array, UpstreamBodyError> =>
  Stream.repeatEffectOption(
    readChunk(reader, url, timeout).pipe(
      Effect.mapError((error) => Option.some(error)),
      Effect.flatMap(
        Option.match({
          onNone: () => Effect.fail(Option.none<UpstreamBodyError>()),
          onSome: (chunk) => Effect.succeed(chunk),
        }),
      ),
    ),
  );

// ---------------------------------------------------------------------------
// The service
// ---------------------------------------------------------------------------

/** The shape of `fetch` this client needs — narrow enough for a stub to satisfy. */
export type FetchLike = (url: string, init: RequestInit) => Promise<Response>;

export interface UpstreamClientOptions {
  /** Normalized upstream service URL (`_normalize_service_url`, `:129`): no trailing slash. */
  readonly serviceUrl: string;
  /** `config.upstream_timeout_s`; defaults to {@link DEFAULT_UPSTREAM_TIMEOUT}. */
  readonly timeout?: Duration.DurationInput | undefined;
  /** Injected transport. Defaults to the global `fetch`. */
  readonly fetch?: FetchLike | undefined;
}

export interface UpstreamClientApi {
  /** The URL a request resolves to — exposed so routes and tests can assert on it. */
  readonly url: (request: UpstreamRequest) => string;
  /**
   * `_upstream_json_or_status` (`:1541`): 2xx bytes (capped), any other status
   * as a value after its body is drained, 3xx as {@link UpstreamRedirect}.
   */
  readonly jsonOrStatus: (
    request: UpstreamRequest,
  ) => Effect.Effect<
    UpstreamJson,
    UpstreamOffline | UpstreamJsonTooLarge | UpstreamRedirect | UpstreamBodyError
  >;
  /**
   * `_open_upstream(accept="*&#47;*")` (`:1938`): the response with its body
   * still on the socket, for the streaming routes.  Requires a `Scope`, which
   * owns the reader.
   */
  readonly openBinary: (
    request: UpstreamRequest,
  ) => Effect.Effect<UpstreamBinaryResponse, UpstreamOffline, Scope.Scope>;
}

const requestUpstream = (
  fetchImpl: FetchLike,
  url: string,
  accept: string,
  timeout: Duration.Duration,
): Effect.Effect<Response, UpstreamOffline> =>
  Effect.tryPromise({
    try: (signal) =>
      fetchImpl(url, {
        method: 'GET',
        redirect: 'manual',
        headers: upstreamRequestHeaders(accept),
        signal,
      }),
    catch: (cause) => new UpstreamOffline({ reason: 'transport', url, cause }),
  }).pipe(
    Effect.timeoutFail({
      duration: timeout,
      onTimeout: () => new UpstreamOffline({ reason: 'timeout', url, cause: null }),
    }),
  );

/** The probe, plus its 64 KiB drain-and-close (`:1425-1432`). */
const probePortless = (
  url: string,
  response: Response,
  timeout: Duration.Duration,
): Effect.Effect<Response, UpstreamOffline> =>
  isPortlessOffline(response)
    ? Effect.scoped(
        acquireReader(response).pipe(
          Effect.flatMap((reader) =>
            readBounded(
              reader,
              url,
              { readBytes: MAX_PROXY_ERROR_BYTES, retainBytes: 0 },
              timeout,
            ),
          ),
          Effect.ignore,
        ),
      ).pipe(
        Effect.andThen(
          Effect.fail(new UpstreamOffline({ reason: 'portless', url, cause: null })),
        ),
      )
    : Effect.succeed(response);

/** `_read_json_response` (`:1521`): the `Content-Length` guard, then the read guard. */
const readJsonBody = (
  url: string,
  response: Response,
  timeout: Duration.Duration,
): Effect.Effect<Uint8Array, UpstreamJsonTooLarge | UpstreamBodyError, Scope.Scope> => {
  const declared = response.headers.get('Content-Length');
  const oversizeByHeader = Option.match(
    declared === null ? Option.none() : parsePythonInt(declared),
    {
      onNone: () => false,
      onSome: (length) => length > BigInt(MAX_PROXY_JSON_BYTES),
    },
  );
  return oversizeByHeader
    ? Effect.fail(
        new UpstreamJsonTooLarge({
          source: 'content-length',
          capBytes: MAX_PROXY_JSON_BYTES,
          bytesRead: 0,
          bytesRetained: 0,
          url,
        }),
      )
    : acquireReader(response).pipe(
        Effect.flatMap((reader) =>
          readBounded(
            reader,
            url,
            {
              // `response.read(MAX + 1)`: one byte past the cap is what makes
              // `len(value) > MAX` decidable.
              readBytes: MAX_PROXY_JSON_BYTES + 1,
              retainBytes: MAX_PROXY_JSON_BYTES + 1,
            },
            timeout,
          ),
        ),
        Effect.flatMap((state) =>
          state.retained > MAX_PROXY_JSON_BYTES
            ? Effect.fail(
                new UpstreamJsonTooLarge({
                  source: 'body',
                  capBytes: MAX_PROXY_JSON_BYTES,
                  bytesRead: state.read,
                  bytesRetained: state.retained,
                  url,
                }),
              )
            : Effect.succeed(
                concatBytes(Chunk.toReadonlyArray(state.chunks), state.retained),
              ),
        ),
      );
};

/** The 2xx arm: relayable bytes. */
const jsonSuccess = (
  url: string,
  response: Response,
  timeout: Duration.Duration,
): Effect.Effect<UpstreamJson, UpstreamJsonTooLarge | UpstreamBodyError, Scope.Scope> =>
  readJsonBody(url, response, timeout).pipe(
    Effect.map(
      (body): UpstreamJson => ({ _tag: 'UpstreamBody', status: response.status, body }),
    ),
  );

/**
 * The non-2xx arm (`:1549-1551`): drain up to 64 KiB and drop it, then answer
 * with the status — unless it is a redirect, which no route may treat as a
 * fallback trigger.
 */
const jsonFailure = (
  url: string,
  response: Response,
  timeout: Duration.Duration,
): Effect.Effect<UpstreamJson, UpstreamRedirect | UpstreamBodyError, Scope.Scope> =>
  acquireReader(response).pipe(
    Effect.flatMap((reader) =>
      readBounded(reader, url, { readBytes: MAX_PROXY_ERROR_BYTES, retainBytes: 0 }, timeout),
    ),
    Effect.flatMap(
      (): Effect.Effect<UpstreamJson, UpstreamRedirect> =>
        isRedirectStatus(response.status)
          ? Effect.fail(new UpstreamRedirect({ status: response.status, url }))
          : Effect.succeed({ _tag: 'UpstreamStatus', status: response.status }),
    ),
  );

/** Build the client. The layers below are thin wrappers over this. */
export const make = (options: UpstreamClientOptions): UpstreamClientApi => {
  const fetchImpl: FetchLike =
    options.fetch ?? ((url, init) => globalThis.fetch(url, init));
  const timeout = Duration.decode(options.timeout ?? DEFAULT_UPSTREAM_TIMEOUT);
  const url = (request: UpstreamRequest): string => upstreamUrl(options.serviceUrl, request);

  const open = (
    request: UpstreamRequest,
    accept: string,
  ): Effect.Effect<Response, UpstreamOffline> =>
    requestUpstream(fetchImpl, url(request), accept, timeout).pipe(
      Effect.flatMap((response) => probePortless(url(request), response, timeout)),
    );

  return {
    url,

    jsonOrStatus: (request) =>
      Effect.scoped(
        open(request, UPSTREAM_ACCEPT_JSON).pipe(
          Effect.flatMap(
            (
              response,
            ): Effect.Effect<
              UpstreamJson,
              UpstreamJsonTooLarge | UpstreamRedirect | UpstreamBodyError,
              Scope.Scope
            > =>
              isSuccessStatus(response.status)
                ? jsonSuccess(url(request), response, timeout)
                : jsonFailure(url(request), response, timeout),
          ),
        ),
      ),

    openBinary: (request) =>
      open(request, UPSTREAM_ACCEPT_BINARY).pipe(
        Effect.flatMap((response) =>
          acquireReader(response).pipe(
            Effect.map(
              (reader): UpstreamBinaryResponse => ({
                status: response.status,
                headers: proxyHeaders(response.headers),
                stream: bodyStream(reader, url(request), timeout),
                drainError: readBounded(
                  reader,
                  url(request),
                  { readBytes: MAX_PROXY_ERROR_BYTES, retainBytes: 0 },
                  timeout,
                ).pipe(Effect.map((state) => state.read)),
              }),
            ),
          ),
        ),
      ),
  };
};

/**
 * The gateway's upstream client.
 *
 * A `Context.Tag` rather than a module-level singleton: the parity rig runs two
 * gateways in one process, and a hidden global would make them share a
 * transport.
 */
export class UpstreamClient extends Context.Tag('@arena/harness/gateway/UpstreamClient')<
  UpstreamClient,
  UpstreamClientApi
>() {}

/** Live layer: the global `fetch`, `redirect: 'manual'`, fixed request headers. */
export const layerLive = (
  options: Omit<UpstreamClientOptions, 'fetch'>,
): Layer.Layer<UpstreamClient> => Layer.succeed(UpstreamClient, make(options));

/** Test layer: same client, injected transport. No socket required. */
export const layerTest = (
  options: UpstreamClientOptions & { readonly fetch: FetchLike },
): Layer.Layer<UpstreamClient> => Layer.succeed(UpstreamClient, make(options));
