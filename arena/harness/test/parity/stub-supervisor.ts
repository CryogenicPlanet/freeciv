/**
 * Controlled upstream personalities for the differential rig. Each mode is a
 * separate server and answers every target consistently, keeping gateway
 * routing out of the fixture decision.
 *
 * Modes cover non-canonical JSON relay (including load-bearing `7.0`), 404/405
 * fallback, redirect refusal, portless 502 detection, streamed oversize JSON,
 * read timeout, and binary/header relay. Every server binds `127.0.0.1:0` and
 * must not be used as a released-port connect-down fixture.
 */

// ---------------------------------------------------------------------------
// Modes
// ---------------------------------------------------------------------------

/** Every mode, in the order the self-tests walk them. */
export const STUB_MODES = [
  'ok-json',
  'not-found-404',
  'method-405',
  'redirect-302',
  'portless',
  'oversize-9mib',
  'hang',
  'binary-ok',
] as const;

/** @see {@link STUB_MODES} */
export type StubMode = (typeof STUB_MODES)[number];

// ---------------------------------------------------------------------------
// The captured request log
// ---------------------------------------------------------------------------

/**
 * One forwarded request, as the upstream saw it.
 *
 * `target` is `pathname + search` with **every byte the gateway sent** — the
 * trailing slash that `_archive_json_route` forwards verbatim and the viewer
 * routes strip is visible here, and telling the two apart is most of what the
 * log is for.
 *
 * `headers` has lower-cased names; a repeated name arrives already joined with
 * `", "` by `Headers`, which is the same shape both gateways would see.
 */
export interface RecordedRequest {
  readonly method: string;
  readonly target: string;
  readonly headers: Readonly<Record<string, string>>;
  /** `performance.now()` at arrival — the `hang` mode's only observable. */
  readonly receivedAt: number;
}

/**
 * A running stub, scoped to its owner: whoever calls {@link makeStub} closes it.
 *
 * `await using stub = makeStub('ok-json')` releases at the end of the block.
 * `close()` is idempotent.
 */
export interface StubHandle extends AsyncDisposable {
  readonly mode: StubMode;
  /** The ephemeral port it bound. `http://127.0.0.1:${port}` is {@link StubHandle.origin}. */
  readonly port: number;
  /** Ready to hand to `--service-url`. Always `127.0.0.1`, never `localhost`. */
  readonly origin: string;
  /** Every request, in arrival order. A snapshot: later arrivals do not mutate it. */
  readonly requests: () => ReadonlyArray<RecordedRequest>;
  /**
   * Body chunks the client actually pulled — meaningful in `oversize-9mib`
   * only, where "did the gateway stop at the cap" has no other witness.
   */
  readonly chunksPulled: () => number;
  readonly close: () => Promise<void>;
}

// ---------------------------------------------------------------------------
// ok-json — the bodies, and their deliberate spacing
// ---------------------------------------------------------------------------

/** A well-formed `GAME_ID_RE` id (`^[A-Za-z0-9_-]{20,80}$`) that is also a `game_` id. */
export const STUB_LIVE_GAME_ID = 'game_stubUpstreamLiveRow01';

/** Relayed by the allowlist (`PROXY_RESPONSE_HEADERS`) — assert it survives. */
export const OK_JSON_ETAG = '"stub-ok-json-v1"';
/** Relayed by the allowlist — assert it survives. */
export const OK_JSON_CACHE_CONTROL = 'public, max-age=42';
/** Dropped by the allowlist — assert it does **not** survive. */
export const DROPPED_SET_COOKIE = 'session=stub-secret';
/** Dropped by the allowlist — assert it does **not** survive. */
export const DROPPED_LEAK_HEADER = 'x-stub-leak';

/**
 * `{"schema_version": 1, "games": [...]}` — the one upstream body a gateway
 * parses and re-emits (`http/routes/games.ts`), so its spacing is expected to
 * be *lost* and its `7.0` is expected to survive.
 */
const OK_JSON_INDEX_BODY = `{ "schema_version": 1,  "games": [ { "state": "running" ,"game_id": ${JSON.stringify(
  STUB_LIVE_GAME_ID,
)},  "current_turn": 7.0 } ] }`;

/**
 * Bodies keyed by the route's last path segment — every one of them relayed
 * byte for byte, every one of them unsorted and over-spaced on purpose.
 */
const OK_JSON_BY_SUFFIX: Readonly<Record<string, string>> = {
  status: `{ "state": "running" ,  "game_id": ${JSON.stringify(
    STUB_LIVE_GAME_ID,
  )}, "outcome": { "status": "running" },  "current_turn": 7.0 }`,
  result: `{ "outcome": { "status": "victory",  "winner": "AgentPlace1" } ,"schema_version": 1 }`,
  'watch.json': `{ "zeta": 1,  "alpha": [2, 3] ,"n": 1.5 }`,
  frames: `{ "frames": [ { "turn": 1,  "index": 0 } ] ,"schema_version": 1 }`,
  'replay.json': `{ "rows": [ { "turn": 1,  "kind": "turn_begin" } ] ,"schema_version": 1 }`,
  'board.json': `{ "board": { "width": 8,  "height": 8 } ,"turn": 7.0 }`,
  'events.json': `{ "events": [ { "turn": 1,  "kind": "city_built" } ] ,"schema_version": 1 }`,
};

/** `GAME_ID_RE` (`arena/wire/src/ids.ts:35`), re-spelled so this file imports nothing from `src`. */
const GAME_ID_SHAPE = /^[A-Za-z0-9_-]{20,80}$/;

/**
 * The exact bytes `ok-json` answers a target with — the rig's expectation
 * oracle, not just the stub's implementation detail.
 *
 * Chosen by *shape*, never by the raw target: a trailing slash, a doubled
 * leading slash and a query string all pick the same body, so a parity
 * difference in what the gateway forwarded shows up in
 * {@link StubHandle.requests} (where every byte is kept) instead of silently
 * changing the body under comparison.
 */
export const okJsonBodyFor = (target: string): string => {
  const path = target.split('?')[0] ?? '';
  const trimmed = path.replace(/\/+$/, '');
  if (trimmed.endsWith('/v1/games')) return OK_JSON_INDEX_BODY;
  const last = trimmed.split('/').at(-1) ?? '';
  const known = OK_JSON_BY_SUFFIX[last];
  if (known !== undefined) return known;
  // The bare-id alias `/v1/games/{id}`, which upstream sees without `/status`.
  if (GAME_ID_SHAPE.test(last)) return OK_JSON_BY_SUFFIX['status'] ?? '';
  return `{ "stub": "ok-json",  "target": ${JSON.stringify(target)} }`;
};

// ---------------------------------------------------------------------------
// The other modes' fixed bodies
// ---------------------------------------------------------------------------

/** 404 is a *fallback* status: the gateway must serve disk, not relay this. */
export const NOT_FOUND_BODY = `{ "error": "no such game",  "stub": "not-found-404" }`;
/** The second fallback status; reached by a different line in both gateways. */
export const METHOD_NOT_ALLOWED_BODY = `{ "error": "method not allowed",  "stub": "method-405" }`;
/** @see {@link METHOD_NOT_ALLOWED_BODY} */
export const METHOD_ALLOW_HEADER = 'GET, HEAD';

/**
 * Relative on purpose: `redirect-302` answers *every* target with a 302, so a
 * gateway that followed would loop here and the loop would be visible as a
 * pile of entries in {@link StubHandle.requests}.  A gateway that refuses
 * leaves exactly one.
 */
export const REDIRECT_LOCATION = '/v1/games';
/** @see {@link REDIRECT_LOCATION} */
export const REDIRECT_BODY = '<html><body>moved</body></html>';

/**
 * The portless forwarder's own error page.  Small on purpose: the probe drains
 * the body and the drain is capped at 64 KiB, and this fixture is about the
 * three-condition signature, not the cap.
 */
export const PORTLESS_BODY = '<html><body>no port forwarded</body></html>';
/** Exactly this — no `charset`, no parameter. The probe compares media types. */
export const PORTLESS_CONTENT_TYPE = 'text/html';

// ---------------------------------------------------------------------------
// oversize-9mib
// ---------------------------------------------------------------------------

const OVERSIZE_HEAD = '{"items":["';
const OVERSIZE_TAIL = '"]}';
/** 256 KiB, the chunk both gateways' readers see whole. */
export const OVERSIZE_CHUNK_BYTES = 256 * 1024;
/** 36 × 256 KiB = 9 MiB of payload — comfortably past the 8 MiB cap. */
export const OVERSIZE_CHUNKS = 36;
/** What a client that read to EOF would count. Nothing under test should. */
export const OVERSIZE_TOTAL_BYTES =
  OVERSIZE_HEAD.length + OVERSIZE_CHUNKS * OVERSIZE_CHUNK_BYTES + OVERSIZE_TAIL.length;

// ---------------------------------------------------------------------------
// binary-ok
// ---------------------------------------------------------------------------

/** Relayed by the allowlist, and the fixture for the conditional request. */
export const BINARY_ETAG = '"stub-mp4-v1"';
/** Relayed by the allowlist. A fixed date: parity compares bytes. */
export const BINARY_LAST_MODIFIED = 'Wed, 21 Oct 2015 07:28:00 GMT';

const asciiBytes = (text: string): ReadonlyArray<number> =>
  Array.from(text, (character) => character.charCodeAt(0));

/**
 * A 32-byte ISO-BMFF `ftyp` box — `isom`, minor version 512, three compatible
 * brands.  Not a playable video; it is the shortest byte string that `file(1)`
 * and a content sniffer both call an mp4, which is what "the gateway did not
 * transcode or truncate it" needs.
 */
export const MP4_BYTES: Uint8Array = new Uint8Array([
  0x00,
  0x00,
  0x00,
  0x20,
  ...asciiBytes('ftyp'),
  ...asciiBytes('isom'),
  0x00,
  0x00,
  0x02,
  0x00,
  ...asciiBytes('isomiso2mp41'),
]);

/** An 8-byte PNG signature, for the frame routes, which are binary too. */
export const PNG_BYTES: Uint8Array = new Uint8Array([
  0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a,
]);

// ---------------------------------------------------------------------------
// The server
// ---------------------------------------------------------------------------

const encoder = new TextEncoder();

interface StubState {
  readonly log: Array<RecordedRequest>;
  /** Resolvers for the requests `hang` is holding open. */
  readonly pending: Set<() => void>;
  readonly counters: { pulled: number };
}

const recordRequest = (state: StubState, request: Request): URL => {
  const url = new URL(request.url);
  state.log.push({
    method: request.method,
    target: `${url.pathname}${url.search}`,
    headers: Object.fromEntries(
      Array.from(request.headers, ([name, value]) => [name.toLowerCase(), value]),
    ),
    receivedAt: performance.now(),
  });
  return url;
};

const jsonResponse = (
  body: string,
  status: number,
  extra: Readonly<Record<string, string>>,
): Response =>
  new Response(body, {
    status,
    headers: { 'content-type': 'application/json', ...extra },
  });

/**
 * 9 MiB produced chunk by chunk, so the stub holds one chunk and the count of
 * chunks it was *asked* for is a real measurement of how far the client read.
 *
 * No `Content-Length`: a streamed body goes out chunked, which is the leg where
 * the cap has to fire on bytes read rather than on a declared length.  The 1 ms
 * pause keeps a loopback socket's buffers from swallowing the whole body before
 * the client has read a byte, which would make {@link StubHandle.chunksPulled}
 * meaningless.
 */
const oversizeBody = (state: StubState): ReadableStream<Uint8Array> => {
  const chunk = new Uint8Array(OVERSIZE_CHUNK_BYTES).fill(0x61); // 'a'
  const cursor = { sent: 0, headSent: false };
  return new ReadableStream<Uint8Array>({
    async pull(controller) {
      if (!cursor.headSent) {
        cursor.headSent = true;
        controller.enqueue(encoder.encode(OVERSIZE_HEAD));
        return;
      }
      if (cursor.sent >= OVERSIZE_CHUNKS) {
        controller.enqueue(encoder.encode(OVERSIZE_TAIL));
        controller.close();
        return;
      }
      cursor.sent += 1;
      state.counters.pulled += 1;
      // slice() so each enqueue owns its buffer.
      controller.enqueue(chunk.slice());
      await Bun.sleep(1);
    },
  });
};

/**
 * A request that is accepted, read, and then never answered.
 *
 * It resolves only when the stub closes or the client walks away, so it
 * outlasts any `--upstream-timeout-s` the rig can set — the timeout is the
 * thing under test, and a stub that gave up first would test nothing.
 * Resolving on close (rather than leaking the promise) is what lets
 * {@link StubHandle.close} return without Bun waiting on an in-flight handler.
 */
const hangForever = (state: StubState, request: Request): Promise<Response> =>
  // oxlint-disable-next-line effecttsgo/new-promise -- `Bun.serve`'s fetch hook takes a Promise, and there is no Effect on this side of the boundary to run
  new Promise<Response>((resolve) => {
    const settle = (): void => {
      state.pending.delete(settle);
      resolve(new Response(null, { status: 503 }));
    };
    state.pending.add(settle);
    request.signal.addEventListener('abort', settle, { once: true });
  });

const binaryResponse = (url: URL, request: Request): Response => {
  const conditionalHeaders = {
    etag: BINARY_ETAG,
    'last-modified': BINARY_LAST_MODIFIED,
  };
  // The conditional leg: proof the gateway forwarded `If-None-Match` at all.
  if (request.headers.get('if-none-match') === BINARY_ETAG) {
    return new Response(null, { status: 304, headers: conditionalHeaders });
  }
  const png = url.pathname.endsWith('.png');
  const body = png ? PNG_BYTES : MP4_BYTES;
  return new Response(body.slice(), {
    headers: {
      'content-type': png ? 'image/png' : 'video/mp4',
      'content-length': String(body.byteLength),
      ...conditionalHeaders,
      // Three headers the relay allowlist drops. Their absence downstream is
      // an assertion; their presence here is what makes it one.
      'accept-ranges': 'bytes',
      'set-cookie': DROPPED_SET_COOKIE,
      [DROPPED_LEAK_HEADER]: 'leak-me',
    },
  });
};

/**
 * The whole behavior table — one entry per mode, and the type makes it one.
 *
 * `Record<StubMode, …>` rather than a `switch`: adding a mode to
 * {@link STUB_MODES} without teaching the server how to serve it is then a
 * compile error rather than a fixture that answers 404 by accident.
 */
const HANDLERS: Readonly<
  Record<StubMode, (state: StubState, url: URL, request: Request) => Response | Promise<Response>>
> = {
  'ok-json': (_state, url) =>
    jsonResponse(okJsonBodyFor(`${url.pathname}${url.search}`), 200, {
      etag: OK_JSON_ETAG,
      'cache-control': OK_JSON_CACHE_CONTROL,
      'set-cookie': DROPPED_SET_COOKIE,
      [DROPPED_LEAK_HEADER]: 'leak-me',
    }),

  'not-found-404': () => jsonResponse(NOT_FOUND_BODY, 404, {}),

  'method-405': () =>
    jsonResponse(METHOD_NOT_ALLOWED_BODY, 405, { allow: METHOD_ALLOW_HEADER }),

  'redirect-302': () =>
    new Response(REDIRECT_BODY, {
      status: 302,
      headers: { location: REDIRECT_LOCATION, 'content-type': 'text/html' },
    }),

  portless: () =>
    new Response(PORTLESS_BODY, {
      status: 502,
      headers: { 'content-type': PORTLESS_CONTENT_TYPE, 'x-portless': '1' },
    }),

  'oversize-9mib': (state) =>
    new Response(oversizeBody(state), { headers: { 'content-type': 'application/json' } }),

  hang: (state, _url, request) => hangForever(state, request),

  'binary-ok': (_state, url, request) => binaryResponse(url, request),
};

/**
 * Start one stub in one mode. The caller owns it; close it or use `await using`.
 *
 * `idleTimeout: 0` because `hang` must be allowed to hold a connection open
 * past any timeout the rig configures — Bun's default would close it at ten
 * seconds and turn the read timeout under test into a transport error.
 */
export const makeStub = (mode: StubMode): StubHandle => {
  const state: StubState = { log: [], pending: new Set(), counters: { pulled: 0 } };

  const server = Bun.serve({
    port: 0,
    hostname: '127.0.0.1',
    idleTimeout: 0,
    fetch: (request) => HANDLERS[mode](state, recordRequest(state, request), request),
  });

  const close = async (): Promise<void> => {
    // Release the held requests first: `stop(true)` should not have to race a
    // handler that, by construction, never finishes on its own.
    state.pending.forEach((settle) => {
      settle();
    });
    await server.stop(true);
  };

  // `Bun.Server.port` is optional in the type because a unix-socket server has
  // none; this one always binds TCP, and the URL carries the same number.
  const port = server.port ?? Number(server.url.port);

  return {
    mode,
    port,
    origin: `http://127.0.0.1:${port}`,
    requests: () => state.log.slice(),
    chunksPulled: () => state.counters.pulled,
    close,
    [Symbol.asyncDispose]: close,
  };
};
