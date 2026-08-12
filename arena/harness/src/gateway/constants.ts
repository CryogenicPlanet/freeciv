/**
 * The replay gateway's own constants — the ones `@arena/wire` does not own.
 *
 * Wire owns everything that travels *inside* a payload: the message catalogue
 * and its statuses (`Gateway.GATEWAY_PROBLEM_MESSAGES`), the id grammars
 * (`GAME_ID_RE`, `FRAME_INDEX_RE`), the archive path builders, the archive
 * content types and `ARCHIVE_BINARY_CACHE_CONTROL`.  None of that is repeated
 * here; this module is the *service's* vocabulary — the literals that appear in
 * `agent_eval/replay_gateway.py` and nowhere on the wire as data:
 *
 * - the fixed route paths and path segments `do_GET` matches (`:1965-2037`),
 * - the HTTP methods the handler answers at all (`:2059-2064`),
 * - the proxy's size caps, header allowlist and fixed request headers
 *   (`:54-62`, `:1402-1438`),
 * - the security headers written on every response (`:1347-1348`),
 * - the "upstream has nothing" status set that gates every disk fallback.
 *
 * Every citation `:NNN` is `agent_eval/replay_gateway.py`.
 *
 * @module
 */

// ---------------------------------------------------------------------------
// Methods
// ---------------------------------------------------------------------------

/**
 * The one verb the gateway serves (`do_GET`, `:1965`).
 */
export const GATEWAY_GET_METHOD = 'GET';

/**
 * The five-plus-one verbs wired to `_method_not_allowed` (`:2059-2064`).
 *
 * `HEAD` is in this list, which is the load-bearing part: the gateway does
 * **not** answer HEAD with the GET headers, it answers 405 — and Python still
 * writes the 30-byte body (Bun's adapter strips it; the headers must match).
 */
export const GATEWAY_REJECTED_METHODS = [
  'HEAD',
  'POST',
  'PUT',
  'PATCH',
  'DELETE',
  'OPTIONS',
] as const;

/**
 * Every verb `BaseHTTPRequestHandler` has a `do_<VERB>` for, and therefore
 * every verb that can reach a JSON response at all.
 *
 * Anything else — `TRACE`, `CONNECT`, `FOO` — never reaches the handler: the
 * stdlib answers `501 Unsupported method ('TRACE')` as **HTML** with
 * `Connection: close` and none of the security headers.  That rejection
 * belongs to the socket edge, not to {@link module:./http/dispatch}, which is
 * why `dispatch` takes a {@link GatewayMethod} rather than a `string`.
 */
export const GATEWAY_METHODS = [GATEWAY_GET_METHOD, ...GATEWAY_REJECTED_METHODS] as const;

/** A verb the gateway's handler is reachable by. */
export type GatewayMethod = (typeof GATEWAY_METHODS)[number];

const GATEWAY_METHOD_SET: ReadonlySet<string> = new Set<string>(GATEWAY_METHODS);

/**
 * True when `method` is one the handler answers.  False means the stdlib's
 * HTML `501 Unsupported method` — see {@link GATEWAY_METHODS}.
 */
export const isGatewayMethod = (method: string): method is GatewayMethod =>
  GATEWAY_METHOD_SET.has(method);

// ---------------------------------------------------------------------------
// Routes
// ---------------------------------------------------------------------------

/** `/health` — matched exactly, served locally, never proxied (`:1970`). */
export const GATEWAY_HEALTH_PATH = '/health';

/** `/v1/games` — matched exactly (`:1978`). A trailing slash is a 404. */
export const GATEWAY_GAMES_INDEX_PATH = '/v1/games';

/**
 * `parts[:2] != ["v1", "games"]` (`:1990`) — the prefix every per-game route
 * must present after `path.strip("/").split("/")`.
 */
export const GATEWAY_GAMES_PREFIX = ['v1', 'games'] as const;

/**
 * The path segments `do_GET` compares a route suffix against (`:1996-2031`).
 *
 * Spelled once, here, so the dispatcher and its tests cannot disagree about a
 * literal — and so `frames` cannot be quietly typoed into a route that
 * silently 404s.
 */
export const GATEWAY_ROUTE_SEGMENTS = {
  /** `suffix == ["replay.json"]` (`:1996`). */
  replayJson: 'replay.json',
  /** `suffix == ["board.json"]` (`:1999`). */
  boardJson: 'board.json',
  /** `suffix == ["events.json"]` (`:2002`). */
  eventsJson: 'events.json',
  /** `suffix == ["status"]`, and the empty suffix aliases it (`:2010`). */
  status: 'status',
  /** `suffix == ["result"]` (`:2013`). */
  result: 'result',
  /** `suffix == ["watch.json"]` (`:2016`). */
  watchJson: 'watch.json',
  /** `suffix == ["frames"]`, and the head of the two binary frame routes (`:2019`). */
  frames: 'frames',
  /** `suffix == ["frames", "latest.png"]` (`:2022`). */
  latestPng: 'latest.png',
  /** `suffix == ["video.mp4"]` (`:2025`). */
  videoMp4: 'video.mp4',
} as const satisfies Record<string, string>;

// ---------------------------------------------------------------------------
// Proxy limits
// ---------------------------------------------------------------------------

/**
 * `MAX_PROXY_JSON_BYTES = 8 * 1024 * 1024` (`:55`).
 *
 * Two different responses hang off this one number and they must not be
 * merged: an *upstream* body over the cap is a 502
 * (`upstreamJsonTooLarge`, `:1527`/`:1535`), the gateway's *own* serialized
 * body over the cap is a 503 (`archiveJsonTooLarge`, `:1375`).
 */
export const MAX_PROXY_JSON_BYTES = 8 * 1024 * 1024;

/**
 * `MAX_PROXY_ERROR_BYTES = 64 * 1024` (`:54`) — how much of a non-2xx upstream
 * body is read before the problem is written (`:1426`, `:1454`, `:1550`).
 *
 * It is a read-and-discard: the bytes are never inspected and never forwarded.
 */
export const MAX_PROXY_ERROR_BYTES = 64 * 1024;

/** `upstream_timeout_s: float = 10` (`:192`) — applies to connect *and* read. */
export const DEFAULT_UPSTREAM_TIMEOUT_SECONDS = 10;

/**
 * The only upstream statuses that let a route fall back to disk (`:1641`,
 * `:1907`, `:1673`, `:1747`, `:1835`, `:1954`).
 *
 * Everything else — 5xx included — is relayed as a problem.  A port that
 * widens this set masks upstream failure with stale disk data, which is the
 * single integrity property the fallback matrix exists to protect.
 */
export const UPSTREAM_FALLBACK_STATUSES = [404, 405] as const;

const UPSTREAM_FALLBACK_STATUS_SET: ReadonlySet<number> = new Set<number>(
  UPSTREAM_FALLBACK_STATUSES,
);

/**
 * True when an upstream status permits the disk fallback.
 *
 * Derived from {@link UPSTREAM_FALLBACK_STATUSES} rather than re-spelling
 * `404 || 405` two lines below it: the predicate and the list are one fact, and
 * a maintainer who widens the list must not be able to leave the predicate
 * behind (mutation survivor E6).
 */
export const isUpstreamFallbackStatus = (status: number): boolean =>
  UPSTREAM_FALLBACK_STATUS_SET.has(status);

// ---------------------------------------------------------------------------
// The upstream request
// ---------------------------------------------------------------------------

/** `Accept` on a JSON upstream request (`:1403`, the default argument). */
export const UPSTREAM_ACCEPT_JSON = 'application/json';

/** `Accept` on a binary upstream request (`:1444`, `:1942`). */
export const UPSTREAM_ACCEPT_BINARY = '*/*';

/**
 * The portless probe (`:1417-1432`) — the **only** 502 that means "offline".
 *
 * All three must hold: status 502, this header set to exactly this value, and
 * a `Content-Type` whose media type (before `;`, trimmed, lowercased) is
 * exactly this type.  Two out of three is an ordinary 502 relay.
 */
export const UPSTREAM_PORTLESS_HEADER = 'X-Portless';
/** @see {@link UPSTREAM_PORTLESS_HEADER} */
export const UPSTREAM_PORTLESS_VALUE = '1';
/** @see {@link UPSTREAM_PORTLESS_HEADER} */
export const UPSTREAM_PORTLESS_MEDIA_TYPE = 'text/html';

// ---------------------------------------------------------------------------
// The downstream response
// ---------------------------------------------------------------------------

/**
 * `PROXY_RESPONSE_HEADERS` (`:56-62`) — the whole allowlist copied from a
 * proxied 2xx, **in this order** (`_stream_upstream`, `:1463-1482`).
 *
 * Everything else is dropped: `Set-Cookie`, `Server`, `Vary`, `X-*`,
 * `Content-Encoding`, `Content-Disposition`, `Accept-Ranges`.  A proxied 2xx
 * therefore carries no `Cache-Control` at all unless upstream sent one.
 */
export const PROXY_RESPONSE_HEADERS = [
  'Content-Type',
  'Content-Length',
  'Cache-Control',
  'ETag',
  'Last-Modified',
] as const;

/**
 * The two headers `_send_headers` writes on **every** response (`:1347-1348`),
 * proxied bodies and stdlib-shaped errors alike — with the one exception of
 * the pre-router stdlib rejections (501/400/414/431), which never reach it.
 *
 * Lower-cased keys because that is the shape `HttpServerResponse.setHeaders`
 * takes and HTTP field names are case-insensitive; Python writes them
 * `X-Content-Type-Options` / `Referrer-Policy`, and the smoke rig compares the
 * two servers' headers case-folded for exactly that reason.
 *
 * This used to exist twice — here as an array of pairs that nothing imported,
 * and in `./http/respond.ts` as the record that actually reaches the wire.
 * Dropping a pair from the dead copy changed no test, which is how the
 * duplication was found (mutation survivor E3).
 */
export const GATEWAY_SECURITY_HEADERS = {
  'x-content-type-options': 'nosniff',
  'referrer-policy': 'no-referrer',
} as const satisfies Readonly<Record<string, string>>;
