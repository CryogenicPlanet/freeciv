/**
 * `UpstreamClient` against a stub upstream that misbehaves the way a real
 * supervisor does.
 *
 * The suite is organized around the six things a port of `_open_upstream` /
 * `_upstream_json_or_status` gets wrong: the 8 MiB cap, the 64 KiB drain, the
 * refused redirect, the three-condition Portless probe, the timeout, and a
 * disconnect mid-stream.  Two rigs are used deliberately:
 *
 * - a real `Bun.serve` stub for everything that has to be true *on a socket* —
 *   that a cancelled reader actually stops the upstream, that bytes survive
 *   the round trip unchanged, that a 2 MiB error body is not drained whole;
 * - an injected `fetch` for the cases a socket cannot produce on demand — a
 *   `Content-Length` of `8_388_609` (Python's `int()`, not `Number()`), a
 *   transport that never answers (driven by `TestClock` instead of ten real
 *   seconds), and the exact `RequestInit` the live layer builds.
 *
 * Every stub process is owned here: one server, stopped in `afterAll`.
 */
import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { Gateway } from '@arena/wire';
import { Chunk, Duration, Effect, Fiber, Option, Stream, TestClock, TestContext } from 'effect';
import {
  MAX_PROXY_ERROR_BYTES,
  MAX_PROXY_JSON_BYTES,
  PROXY_RESPONSE_HEADERS,
} from 'src/gateway/constants.ts';
import {
  DEFAULT_UPSTREAM_TIMEOUT,
  type FetchLike,
  isUpstreamBody,
  layerLive,
  layerTest,
  make,
  mediaType,
  parsePythonInt,
  UpstreamClient,
  type UpstreamClientApi,
  upstreamFailureProblem,
  type UpstreamJson,
  upstreamUrl,
} from 'src/gateway/services/upstream.ts';

const MiB = 1024 * 1024;
const ERROR_CHUNK_BYTES = 8 * 1024;
const ERROR_CHUNKS = 256; // 2 MiB
const JSON_CHUNK_BYTES = 256 * 1024;
const JSON_CHUNKS = 36; // 9 MiB

const encoder = new TextEncoder();

interface RecordedRequest {
  readonly method: string;
  readonly target: string;
  readonly headers: Readonly<Record<string, string>>;
}

interface Stub {
  readonly url: string;
  readonly requests: () => ReadonlyArray<RecordedRequest>;
  readonly pulled: (route: string) => number;
  readonly cancels: (route: string) => number;
  readonly stop: () => void;
}

/**
 * A body produced chunk by chunk, counting how many chunks the client actually
 * pulled and whether it walked away.  That count is the evidence for "drained
 * 64 KiB, not 2 MiB": nothing else observes it from the client side.
 */
const countedBody = (
  route: string,
  chunk: Uint8Array,
  chunkCount: number,
  delayMs: number,
  counters: { pulled: Record<string, number>; cancels: Record<string, number> },
): ReadableStream<Uint8Array> => {
  const cursor = { sent: 0 };
  return new ReadableStream<Uint8Array>({
    async pull(controller) {
      if (cursor.sent >= chunkCount) {
        controller.close();
        return;
      }
      cursor.sent += 1;
      counters.pulled[route] = (counters.pulled[route] ?? 0) + 1;
      controller.enqueue(chunk.slice());
      // A loopback socket will happily swallow a 2 MiB body into its buffers
      // before the client has read a byte, which would make the pull count
      // meaningless. The delay makes "how much did the upstream have to
      // produce" an actual observation of the client's drain.
      if (delayMs > 0) await Bun.sleep(delayMs);
    },
    cancel() {
      counters.cancels[route] = (counters.cancels[route] ?? 0) + 1;
    },
  });
};

const startStub = (): Stub => {
  const requests: Array<RecordedRequest> = [];
  const counters: { pulled: Record<string, number>; cancels: Record<string, number> } = {
    pulled: {},
    cancels: {},
  };

  const errorChunk = new Uint8Array(ERROR_CHUNK_BYTES).fill(0x45);
  const jsonChunk = new Uint8Array(JSON_CHUNK_BYTES).fill(0x61);
  const pngBody = new Uint8Array([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]);
  // Deliberately not canonical JSON: byte parity means these exact bytes.
  const watchBody = encoder.encode('{"zeta":1,  "alpha":[2,3] ,"png_url":"http://up/x.png"}');

  const handlers: Record<string, () => Response> = {
    '/watch.json': () =>
      new Response(watchBody, { headers: { 'content-type': 'application/json' } }),

    '/empty': () => new Response(null, { status: 204 }),

    // 8 MiB + 1 with a real Content-Length: the pre-read guard's territory.
    '/json/oversize-declared': () =>
      new Response(new Uint8Array(MAX_PROXY_JSON_BYTES + 1), {
        headers: { 'content-type': 'application/json' },
      }),

    // 9 MiB streamed, so there is no Content-Length and only the read guard
    // can catch it.
    '/json/oversize-streamed': () =>
      new Response(countedBody('/json/oversize-streamed', jsonChunk, JSON_CHUNKS, 0, counters), {
        headers: { 'content-type': 'application/json' },
      }),

    '/status/404': () => new Response('{"error":"nope"}', { status: 404 }),

    '/status/500-huge': () =>
      new Response(countedBody('/status/500-huge', errorChunk, ERROR_CHUNKS, 1, counters), {
        status: 500,
        headers: { 'content-type': 'text/plain' },
      }),

    '/redirect': () =>
      new Response(null, { status: 302, headers: { location: '/watch.json' } }),

    '/portless/offline': () =>
      new Response(countedBody('/portless/offline', errorChunk, ERROR_CHUNKS, 1, counters), {
        status: 502,
        // Mixed case + a parameter: `mediaType` has to normalize both.
        headers: { 'content-type': 'text/HTML; charset=utf-8', 'x-portless': '1' },
      }),

    '/portless/zero': () =>
      new Response('<html>gone</html>', {
        status: 502,
        headers: { 'content-type': 'text/html', 'x-portless': '0' },
      }),

    '/portless/json': () =>
      new Response('{"error":"bad gateway"}', {
        status: 502,
        headers: { 'content-type': 'application/json', 'x-portless': '1' },
      }),

    '/frames/0.png': () =>
      new Response(pngBody, {
        headers: {
          'content-type': 'image/png',
          'cache-control': 'public, max-age=2',
          etag: '"frame"',
          'last-modified': 'Wed, 12 Aug 2026 09:25:16 GMT',
          'set-cookie': 'session=secret',
          'x-custom': 'leak-me',
        },
      }),

    '/frames/500.png': () =>
      new Response(countedBody('/frames/500.png', errorChunk, ERROR_CHUNKS, 1, counters), {
        status: 500,
        headers: { 'content-type': 'text/plain' },
      }),

    '/endless': () =>
      new Response(
        new ReadableStream<Uint8Array>({
          async pull(controller) {
            controller.enqueue(new Uint8Array(ERROR_CHUNK_BYTES).fill(0x2e));
            await Bun.sleep(15);
          },
          cancel() {
            counters.cancels['/endless'] = (counters.cancels['/endless'] ?? 0) + 1;
          },
        }),
        { headers: { 'content-type': 'application/octet-stream' } },
      ),

    '/truncated': () =>
      new Response(
        new ReadableStream<Uint8Array>({
          pull(controller) {
            const sent = (counters.pulled['/truncated'] ?? 0) + 1;
            counters.pulled['/truncated'] = sent;
            if (sent > 2) {
              controller.error(new Error('upstream died mid-body'));
              return;
            }
            controller.enqueue(new Uint8Array(ERROR_CHUNK_BYTES).fill(0x21));
          },
        }),
        { headers: { 'content-type': 'video/mp4' } },
      ),
  };

  const server = Bun.serve({
    port: 0,
    idleTimeout: 0,
    fetch(request) {
      const url = new URL(request.url);
      requests.push({
        method: request.method,
        target: `${url.pathname}${url.search}`,
        headers: Object.fromEntries(request.headers.entries()),
      });
      // The Python FakeUpstreamServer strips the same prefix, which is how the
      // configured-path-prefix case is exercised.
      const path = url.pathname.startsWith('/freeciv')
        ? url.pathname.slice('/freeciv'.length)
        : url.pathname;
      const handler = handlers[path];
      return handler === undefined
        ? new Response('{"error":"stub: no route"}', { status: 404 })
        : handler();
    },
  });

  return {
    url: server.url.origin,
    requests: () => requests,
    pulled: (route) => counters.pulled[route] ?? 0,
    cancels: (route) => counters.cancels[route] ?? 0,
    stop: () => {
      void server.stop(true);
    },
  };
};

const stub: { current: Stub | null } = { current: null };
const upstream = (): Stub => {
  const current = stub.current;
  if (current === null) throw new Error('stub upstream not started');
  return current;
};

const client = (): UpstreamClientApi => make({ serviceUrl: upstream().url });

const run = <A, E>(effect: Effect.Effect<A, E>): Promise<A> => Effect.runPromise(effect);
const runFail = <A, E>(effect: Effect.Effect<A, E>): Promise<E> =>
  Effect.runPromise(Effect.flip(effect));

const lastRequest = (): RecordedRequest => {
  const seen = upstream().requests();
  const last = seen[seen.length - 1];
  if (last === undefined) throw new Error('no upstream request was recorded');
  return last;
};

beforeAll(() => {
  stub.current = startStub();
});

afterAll(() => {
  stub.current?.stop();
});

// ---------------------------------------------------------------------------

describe('URL construction (:1398)', () => {
  test('plain concatenation, prefix preserved, empty query means no query', () => {
    expect(upstreamUrl('http://h:1/freeciv', { path: '/v1/games' })).toBe(
      'http://h:1/freeciv/v1/games',
    );
    expect(upstreamUrl('http://h:1', { path: '/v1/games/x/replay.json', query: '' })).toBe(
      'http://h:1/v1/games/x/replay.json',
    );
    expect(
      upstreamUrl('http://h:1', {
        path: '/v1/games/x/replay.json',
        query: 'after_turn=2&limit=3',
      }),
    ).toBe('http://h:1/v1/games/x/replay.json?after_turn=2&limit=3');
    // Trap B1: a trailing slash handed in by the dispatcher is forwarded.
    expect(upstreamUrl('http://h:1', { path: '/v1/games/x/status/' })).toBe(
      'http://h:1/v1/games/x/status/',
    );
  });

  test('the configured prefix and the normalized query reach the socket', async () => {
    const prefixed = make({ serviceUrl: `${upstream().url}/freeciv` });
    const result = await run(
      prefixed.jsonOrStatus({ path: '/watch.json', query: 'after_turn=2&limit=3' }),
    );
    expect(result.status).toBe(200);
    expect(lastRequest().target).toBe('/freeciv/watch.json?after_turn=2&limit=3');
  });
});

describe('jsonOrStatus 2xx (:1541, :6)', () => {
  test('the body is relayed byte for byte and never re-serialized', async () => {
    const result = await run(client().jsonOrStatus({ path: '/watch.json' }));
    expect(isUpstreamBody(result)).toBe(true);
    if (!isUpstreamBody(result)) return;
    expect(result.status).toBe(200);
    expect(new TextDecoder().decode(result.body)).toBe(
      '{"zeta":1,  "alpha":[2,3] ,"png_url":"http://up/x.png"}',
    );
  });

  test('a 204 relays its own status with an empty body', async () => {
    const result = await run(client().jsonOrStatus({ path: '/empty' }));
    expect(result.status).toBe(204);
    expect(isUpstreamBody(result) ? result.body.byteLength : -1).toBe(0);
  });

  test('the request headers are a fixed set — nothing inbound is copied (:1409)', async () => {
    await run(client().jsonOrStatus({ path: '/watch.json' }));
    const headers = lastRequest().headers;
    expect(lastRequest().method).toBe('GET');
    expect(headers['accept']).toBe('application/json');
    expect(headers['accept-encoding']).toBe('identity');
    expect(headers['authorization']).toBeUndefined();
    expect(headers['cookie']).toBeUndefined();
    expect(headers['x-forwarded-host']).toBeUndefined();
    expect(headers['range']).toBeUndefined();
  });
});

describe('the 8 MiB cap (:1521)', () => {
  test('a declared Content-Length over the cap is refused before a byte is read', async () => {
    const failure = await runFail(client().jsonOrStatus({ path: '/json/oversize-declared' }));
    expect(failure._tag).toBe('UpstreamJsonTooLarge');
    if (failure._tag !== 'UpstreamJsonTooLarge') return;
    expect(failure.source).toBe('content-length');
    expect(failure.bytesRead).toBe(0);
    expect(failure.bytesRetained).toBe(0);
    expect(upstreamFailureProblem(failure)).toEqual({
      status: 502,
      message: Gateway.GATEWAY_PROBLEM_MESSAGES.upstreamJsonTooLarge,
    });
  });

  test('a streamed 9 MiB body is abandoned just past the cap, not drained', async () => {
    const failure = await runFail(client().jsonOrStatus({ path: '/json/oversize-streamed' }));
    expect(failure._tag).toBe('UpstreamJsonTooLarge');
    if (failure._tag !== 'UpstreamJsonTooLarge') return;
    expect(failure.source).toBe('body');
    // Crossed the cap...
    expect(failure.bytesRead).toBeGreaterThan(MAX_PROXY_JSON_BYTES);
    // ...and stopped there: one chunk of socket overshoot, not another megabyte.
    expect(failure.bytesRead).toBeLessThanOrEqual(MAX_PROXY_JSON_BYTES + MiB);
    expect(failure.bytesRead).toBeLessThan(9 * MiB);
    // ...and, the part that costs memory: RETENTION never overshoots at all.
    // `advance` truncates the chunk that crosses the cap, so the client holds
    // exactly `MAX + 1` bytes — one byte past the cap is what makes
    // `len(value) > MAX` decidable — however large the chunks are.  Asserting
    // only `bytesRead` left this unguarded: replacing the truncation with a
    // plain `chunk.slice()` survived the whole suite while letting peak
    // retention run a full chunk past the documented ceiling.
    expect(failure.bytesRetained).toBe(MAX_PROXY_JSON_BYTES + 1);
    expect(failure.bytesRetained).toBeLessThan(failure.bytesRead);
    console.log(
      `  [upstream] streamed-cap: read ${failure.bytesRead} bytes, retained ` +
        `${failure.bytesRetained} (cap ${MAX_PROXY_JSON_BYTES}, socket overshoot ` +
        `${failure.bytesRead - MAX_PROXY_JSON_BYTES})`,
    );
    // Note what is *not* asserted: the stub's pull count. This body has no
    // inter-chunk delay, and a loopback socket buffers the tail regardless of
    // whether the client reads it, so the server side is not evidence here.
    // The client-side byte count above is.
  }, 30_000);

  test('an oversized body is NOT an UpstreamOffline, so no route can fall back on it', async () => {
    const failure = await runFail(client().jsonOrStatus({ path: '/json/oversize-declared' }));
    expect(failure._tag).not.toBe('UpstreamOffline');
  });
});

describe('the 64 KiB drain (:1550)', () => {
  test('a 2 MiB 500 body yields the status and is not read whole', async () => {
    const before = upstream().pulled('/status/500-huge');
    const result = await run(client().jsonOrStatus({ path: '/status/500-huge' }));
    expect(result).toEqual({ _tag: 'UpstreamStatus', status: 500 } satisfies UpstreamJson);
    const pulled = upstream().pulled('/status/500-huge') - before;
    console.log(`  [upstream] 500 drain: upstream produced ${pulled} of ${ERROR_CHUNKS} chunks`);
    // 64 KiB is 8 chunks of the 256 this body has. Socket buffering makes the
    // exact number implementation-defined, but "all of it" is a bug.
    expect(pulled).toBeLessThan(ERROR_CHUNKS / 2);
  });

  test('404 comes back as a value — it is a fallback trigger, not a failure', async () => {
    const result = await run(client().jsonOrStatus({ path: '/status/404' }));
    expect(result).toEqual({ _tag: 'UpstreamStatus', status: 404 } satisfies UpstreamJson);
  });
});

describe('redirects are never followed (:102-106)', () => {
  test('a 302 is an UpstreamRedirect rendering as 502, after exactly one request', async () => {
    const before = upstream().requests().length;
    const failure = await runFail(client().jsonOrStatus({ path: '/redirect' }));
    expect(failure._tag).toBe('UpstreamRedirect');
    if (failure._tag !== 'UpstreamRedirect') return;
    expect(failure.status).toBe(302);
    expect(upstreamFailureProblem(failure)).toEqual({
      status: 502,
      message: Gateway.GATEWAY_PROBLEM_MESSAGES.upstreamRedirect,
    });
    // The Location was not fetched.
    expect(upstream().requests().length - before).toBe(1);
    expect(lastRequest().target).toBe('/redirect');
  });

  test('a relayed non-redirect status keeps its own status and message', () => {
    // The mapping the router applies to an UpstreamStatus the route rejects.
    expect(Gateway.upstreamProblem(500)).toEqual({
      status: 500,
      message: 'upstream returned HTTP 500',
    });
  });
});

describe('the Portless probe (:1419) — all three conditions', () => {
  test('502 + X-Portless: 1 + text/html is offline, and the body is drained', async () => {
    const before = upstream().pulled('/portless/offline');
    const failure = await runFail(client().jsonOrStatus({ path: '/portless/offline' }));
    expect(failure._tag).toBe('UpstreamOffline');
    if (failure._tag !== 'UpstreamOffline') return;
    expect(failure.reason).toBe('portless');
    expect(upstreamFailureProblem(failure)).toEqual({
      status: 502,
      message: Gateway.GATEWAY_PROBLEM_MESSAGES.upstreamUnavailable,
    });
    const pulled = upstream().pulled('/portless/offline') - before;
    console.log(`  [upstream] portless drain: upstream produced ${pulled} of ${ERROR_CHUNKS} chunks`);
    expect(pulled).toBeLessThan(ERROR_CHUNKS / 2);
  });

  test('X-Portless: 0 is an ordinary 502 to relay', async () => {
    const result = await run(client().jsonOrStatus({ path: '/portless/zero' }));
    expect(result).toEqual({ _tag: 'UpstreamStatus', status: 502 } satisfies UpstreamJson);
  });

  test('a JSON content type is an ordinary 502 to relay', async () => {
    const result = await run(client().jsonOrStatus({ path: '/portless/json' }));
    expect(result).toEqual({ _tag: 'UpstreamStatus', status: 502 } satisfies UpstreamJson);
  });

  test('the media type is compared case- and parameter-insensitively', () => {
    expect(mediaType('text/HTML; charset=utf-8')).toBe('text/html');
    expect(mediaType('  TEXT/HTML  ')).toBe('text/html');
    expect(mediaType(null)).toBe('');
  });
});

describe('transport failure and timeout (:1434)', () => {
  test('a refused connection is UpstreamOffline(transport)', async () => {
    // Port 1 on loopback: nothing listens, and nothing of ours is touched.
    const offline = make({ serviceUrl: 'http://127.0.0.1:1' });
    const failure = await runFail(offline.jsonOrStatus({ path: '/v1/games' }));
    expect(failure._tag).toBe('UpstreamOffline');
    if (failure._tag !== 'UpstreamOffline') return;
    expect(failure.reason).toBe('transport');
    expect(upstreamFailureProblem(failure).status).toBe(502);
  });

  test('the request timeout is TestClock-drivable — no ten-second wait', async () => {
    const stalled = make({
      serviceUrl: 'http://upstream.invalid',
      timeout: DEFAULT_UPSTREAM_TIMEOUT,
      // A transport that accepts the request and never answers.
      fetch: () => new Promise<Response>(() => undefined),
    });
    const failure = await Effect.runPromise(
      Effect.provide(
        Effect.fork(Effect.flip(stalled.jsonOrStatus({ path: '/v1/games' }))).pipe(
          Effect.flatMap((fiber) =>
            Effect.yieldNow().pipe(
              Effect.flatMap(() => TestClock.adjust(Duration.seconds(10))),
              Effect.flatMap(() => Fiber.join(fiber)),
            ),
          ),
        ),
        TestContext.TestContext,
      ),
    );
    expect(failure._tag).toBe('UpstreamOffline');
    if (failure._tag !== 'UpstreamOffline') return;
    expect(failure.reason).toBe('timeout');
    expect(upstreamFailureProblem(failure).status).toBe(502);
  });
});

describe('openBinary (:1938)', () => {
  test('only PROXY_RESPONSE_HEADERS pass, in declaration order', async () => {
    const headers = await run(
      Effect.scoped(
        client()
          .openBinary({ path: '/frames/0.png' })
          .pipe(Effect.map((response) => response.headers)),
      ),
    );
    expect(headers.map(([name]) => name)).toEqual([
      'Content-Type',
      'Content-Length',
      'Cache-Control',
      'ETag',
      'Last-Modified',
    ]);
    expect(Object.fromEntries(headers)['Content-Type']).toBe('image/png');
    expect(Object.fromEntries(headers)['Cache-Control']).toBe('public, max-age=2');
    expect(Object.fromEntries(headers)['ETag']).toBe('"frame"');
    // Everything else is dropped, `Set-Cookie` above all.
    expect(headers.map(([name]) => name.toLowerCase())).not.toContain('set-cookie');
    expect(headers.map(([name]) => name.toLowerCase())).not.toContain('x-custom');
    expect(PROXY_RESPONSE_HEADERS.length).toBe(5);
  });

  test('the body streams through unchanged and Accept is */*', async () => {
    const bytes = await run(
      Effect.scoped(
        client()
          .openBinary({ path: '/frames/0.png' })
          .pipe(
            Effect.flatMap((response) => Stream.runCollect(response.stream)),
            Effect.map((chunks) =>
              Chunk.toReadonlyArray(chunks).flatMap((chunk) => Array.from(chunk)),
            ),
          ),
      ),
    );
    expect(bytes).toEqual([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]);
    expect(lastRequest().headers['accept']).toBe('*/*');
  });

  test('a non-2xx binary response exposes its status and a 64 KiB drain', async () => {
    const before = upstream().pulled('/frames/500.png');
    const drained = await run(
      Effect.scoped(
        client()
          .openBinary({ path: '/frames/500.png' })
          .pipe(
            Effect.tap((response) => Effect.sync(() => expect(response.status).toBe(500))),
            Effect.flatMap((response) => response.drainError),
          ),
      ),
    );
    expect(drained).toBeGreaterThan(0);
    expect(drained).toBeLessThanOrEqual(MAX_PROXY_ERROR_BYTES + JSON_CHUNK_BYTES);
    const pulled = upstream().pulled('/frames/500.png') - before;
    console.log(`  [upstream] binary 500 drain: read ${drained} bytes, upstream produced ${pulled} chunks`);
    expect(pulled).toBeLessThan(ERROR_CHUNKS / 2);
  });

  test('closing the scope cancels the reader, so an endless upstream stops', async () => {
    const before = upstream().cancels('/endless');
    const taken = await run(
      Effect.scoped(
        client()
          .openBinary({ path: '/endless' })
          .pipe(
            Effect.flatMap((response) => Stream.runCollect(Stream.take(response.stream, 2))),
            Effect.map(Chunk.size),
          ),
      ),
    );
    expect(taken).toBe(2);
    await Bun.sleep(250);
    expect(upstream().cancels('/endless') - before).toBeGreaterThanOrEqual(1);
  }, 30_000);

  test('a disconnect mid-stream fails the stream after the partial chunks', async () => {
    const failure = await runFail(
      Effect.scoped(
        client()
          .openBinary({ path: '/truncated' })
          .pipe(Effect.flatMap((response) => Stream.runCollect(response.stream))),
      ),
    );
    expect(failure._tag).toBe('UpstreamBodyError');
    if (failure._tag !== 'UpstreamBodyError') return;
    expect(failure.reason).toBe('read');
    // Python lets the read error escape to the do_GET catch-all: a 500, and
    // pointedly not the 502 that would let a route serve disk data instead.
    expect(upstreamFailureProblem(failure)).toEqual({
      status: 500,
      message: Gateway.GATEWAY_PROBLEM_MESSAGES.internalError,
    });
  });
});

/** A transport that records what it was handed and answers with `response()`. */
const responder =
  (seen: Array<{ url: string; init: RequestInit }>, response: () => Response): FetchLike =>
  (url, init) => {
    seen.push({ url, init });
    return Promise.resolve(response());
  };

describe('the injected-transport layer', () => {
  test('the live layer builds a manual-redirect GET with only the fixed headers', async () => {
    const seen: Array<{ url: string; init: RequestInit }> = [];
    const layer = layerTest({
      serviceUrl: 'http://upstream.test',
      fetch: responder(seen, () => new Response('{}')),
    });
    const result = await Effect.runPromise(
      Effect.provide(
        UpstreamClient.pipe(
          Effect.flatMap((service) => service.jsonOrStatus({ path: '/v1/games' })),
        ),
        layer,
      ),
    );
    expect(result.status).toBe(200);
    const call = seen[0];
    expect(call?.url).toBe('http://upstream.test/v1/games');
    expect(call?.init.method).toBe('GET');
    expect(call?.init.redirect).toBe('manual');
    expect(call?.init.headers).toEqual({
      Accept: 'application/json',
      'Accept-Encoding': 'identity',
    });
    expect(call?.init.signal).toBeDefined();
  });

  test('layerLive provides the tag without an injected transport', () => {
    const layer = layerLive({ serviceUrl: 'http://upstream.test' });
    expect(typeof layer).toBe('object');
    expect(UpstreamClient.key).toBe('@arena/harness/gateway/UpstreamClient');
  });

  test('Content-Length is parsed with Python int(), and an unparseable one is ignored', async () => {
    const oversized = make({
      serviceUrl: 'http://upstream.test',
      fetch: () =>
        Promise.resolve(
          new Response('{}', { headers: { 'content-length': '8_388_609' } }),
        ),
    });
    const failure = await runFail(oversized.jsonOrStatus({ path: '/v1/games' }));
    expect(failure._tag).toBe('UpstreamJsonTooLarge');
    if (failure._tag === 'UpstreamJsonTooLarge') expect(failure.source).toBe('content-length');

    const garbled = make({
      serviceUrl: 'http://upstream.test',
      fetch: () =>
        Promise.resolve(new Response('{}', { headers: { 'content-length': 'not-a-number' } })),
    });
    const result = await run(garbled.jsonOrStatus({ path: '/v1/games' }));
    expect(isUpstreamBody(result) ? new TextDecoder().decode(result.body) : '').toBe('{}');
  });
});

/** `parsePythonInt` flattened to a comparable value. */
const pythonInt = (text: string): bigint | null =>
  Option.getOrNull(parsePythonInt(text));

describe('parsePythonInt (trap B4)', () => {
  test('accepts what Python accepts', () => {
    expect(pythonInt('0')).toBe(0n);
    expect(pythonInt('5_0')).toBe(50n);
    expect(pythonInt('+3')).toBe(3n);
    expect(pythonInt(' 5 \n')).toBe(5n);
    expect(pythonInt('-7')).toBe(-7n);
    expect(pythonInt('007')).toBe(7n);
    // Unicode decimal digits: Arabic-Indic and fullwidth.
    expect(pythonInt('٣')).toBe(3n);
    expect(pythonInt('１２')).toBe(12n);
    // Unbounded, like Python's int.
    expect(pythonInt('9007199254740993')).toBe(9007199254740993n);
  });

  test('rejects what Python rejects', () => {
    expect(pythonInt('')).toBeNull();
    expect(pythonInt('abc')).toBeNull();
    expect(pythonInt('1__2')).toBeNull();
    expect(pythonInt('_1')).toBeNull();
    expect(pythonInt('1_')).toBeNull();
    expect(pythonInt('1.0')).toBeNull();
    expect(pythonInt('0x10')).toBeNull();
    expect(pythonInt('1e3')).toBeNull();
  });
});
