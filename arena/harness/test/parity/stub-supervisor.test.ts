/**
 * Self-tests for the parity rig's upstream stub — one describe per mode.
 *
 * A fixture that is wrong is worse than a missing test: it makes the rig agree
 * with itself about something that never happened.  So each mode is checked
 * against the property the rig will *rely* on, not against the code above it:
 *
 * - `ok-json` really does emit bytes no canonical writer could have produced,
 *   so "the spaces survived" is a usable relay assertion downstream;
 * - `portless` really does send `text/html` with no parameter, because the
 *   probe compares media types and two of three conditions is an ordinary 502;
 * - `hang` really does outlast a full second, because `--upstream-timeout-s 1`
 *   is the thing it exists to let the rig measure;
 * - `oversize-9mib` really is past the 8 MiB cap, and really is chunked.
 *
 * Every stub binds `127.0.0.1:0` and no test spawns a process, so this file
 * cannot touch a running local stack and has no orphan to leave behind.  Each
 * test owns and closes its own stub; the shared-port checks at the bottom are
 * the only place two live at once.
 */
// oxlint-disable effecttsgo/global-fetch -- these tests assert what a stub puts on a socket, and an `HttpClient` layer between the assertion and the wire is exactly the thing that must not be in the way
// oxlint-disable effecttsgo/global-console -- the measured numbers (bytes past the cap, milliseconds held) are the evidence a reader of the run needs
import { describe, expect, test } from 'bun:test';
import {
  BINARY_ETAG,
  BINARY_LAST_MODIFIED,
  DROPPED_LEAK_HEADER,
  DROPPED_SET_COOKIE,
  makeStub,
  METHOD_ALLOW_HEADER,
  METHOD_NOT_ALLOWED_BODY,
  MP4_BYTES,
  NOT_FOUND_BODY,
  OK_JSON_CACHE_CONTROL,
  OK_JSON_ETAG,
  okJsonBodyFor,
  OVERSIZE_CHUNKS,
  OVERSIZE_TOTAL_BYTES,
  PNG_BYTES,
  PORTLESS_BODY,
  PORTLESS_CONTENT_TYPE,
  REDIRECT_LOCATION,
  STUB_LIVE_GAME_ID,
  STUB_MODES,
  type StubHandle,
  type StubMode,
} from './stub-supervisor.ts';

const MiB = 1024 * 1024;
/** `MAX_PROXY_JSON_BYTES` (`src/gateway/constants.ts`), re-spelled locally. */
const MAX_PROXY_JSON_BYTES = 8 * MiB;

const GAMES = '/v1/games';
const STATUS = `${GAMES}/${STUB_LIVE_GAME_ID}/status`;

/**
 * A stub, a request, and a guaranteed close — the shape every test below wants.
 *
 * `Promise.allSettled` rather than a `catch`: the `hang` leg's fetch is
 * *supposed* to reject, and its rejection is the assertion.
 */
const withStub = async <A>(
  mode: StubMode,
  body: (stub: StubHandle) => Promise<A>,
): Promise<A> => {
  const stub = makeStub(mode);
  const [outcome] = await Promise.allSettled([body(stub)]);
  await stub.close();
  if (outcome === undefined || outcome.status === 'rejected') {
    throw outcome === undefined ? new Error('no outcome') : outcome.reason;
  }
  return outcome.value;
};

/** The gateway's own upstream `Accept` for a JSON leg (`UPSTREAM_ACCEPT_JSON`). */
const JSON_REQUEST: RequestInit = { headers: { accept: 'application/json' } };

const isRecord = (value: unknown): value is Readonly<Record<string, unknown>> =>
  typeof value === 'object' && value !== null && !Array.isArray(value);

/** `toEqual` on two `Uint8Array`s whose buffer types differ; the bytes are the claim. */
const bytesOf = (value: Uint8Array): ReadonlyArray<number> => Array.from(value);

// ---------------------------------------------------------------------------
// ok-json
// ---------------------------------------------------------------------------

describe('ok-json — canonical-looking payloads with deliberately non-canonical bytes', () => {
  test('the index body is exactly okJsonBodyFor, spaces and all', async () => {
    const body = await withStub('ok-json', async (stub) => {
      const response = await fetch(`${stub.origin}${GAMES}`, JSON_REQUEST);
      expect(response.status).toBe(200);
      expect(response.headers.get('content-type')).toBe('application/json');
      return response.text();
    });

    expect(body).toBe(okJsonBodyFor(GAMES));
    // The whole point: a serializer could not have produced these bytes.
    expect(body).toContain(',  "games"');
    expect(body).toContain('"current_turn": 7.0');
    expect(body).not.toBe(JSON.stringify(JSON.parse(body)));
    // ...and it is still a valid index for the route that re-serializes it.
    const parsed: unknown = JSON.parse(body);
    expect(parsed).toMatchObject({ schema_version: 1 });
    console.log(`  [ok-json] index body: ${body}`);
  });

  test('every JSON route gets its own unsorted, over-spaced body', async () => {
    const bodies = await withStub('ok-json', async (stub) => {
      const targets = [
        STATUS,
        `${GAMES}/${STUB_LIVE_GAME_ID}/result`,
        `${GAMES}/${STUB_LIVE_GAME_ID}/watch.json`,
        `${GAMES}/${STUB_LIVE_GAME_ID}/replay.json?after_turn=1&limit=2`,
        `${GAMES}/${STUB_LIVE_GAME_ID}`,
      ];
      return Promise.all(
        targets.map(async (target) => {
          const response = await fetch(`${stub.origin}${target}`, JSON_REQUEST);
          expect(response.status).toBe(200);
          const text = await response.text();
          expect(text).toBe(okJsonBodyFor(target));
          return text;
        }),
      );
    });

    bodies.forEach((body) => {
      expect(JSON.parse(body)).toBeDefined();
      expect(body).not.toBe(JSON.stringify(JSON.parse(body)));
    });
    // The bare id aliases `/status` upstream, and so does its body.
    expect(bodies.at(-1)).toBe(bodies[0]);
  });

  test('a trailing slash picks the same body but is recorded verbatim', async () => {
    const { requests, bodies } = await withStub('ok-json', async (stub) => {
      const slashed = await fetch(`${stub.origin}${STATUS}/`, JSON_REQUEST);
      const bare = await fetch(`${stub.origin}${STATUS}`, JSON_REQUEST);
      return { requests: stub.requests(), bodies: [await slashed.text(), await bare.text()] };
    });

    expect(bodies[0]).toBe(bodies[1]);
    expect(requests.map((entry) => entry.target)).toEqual([`${STATUS}/`, STATUS]);
  });

  test('the request log keeps method, target and forwarded headers', async () => {
    const requests = await withStub('ok-json', async (stub) => {
      await fetch(`${stub.origin}${GAMES}?ignored=1`, JSON_REQUEST);
      return stub.requests();
    });

    expect(requests).toHaveLength(1);
    const [entry] = requests;
    expect(entry?.method).toBe('GET');
    expect(entry?.target).toBe(`${GAMES}?ignored=1`);
    expect(entry?.headers['accept']).toBe('application/json');
    expect(entry?.headers['host']).toBeDefined();
    expect(entry?.receivedAt).toBeGreaterThan(0);
  });

  test('it sends both allowlisted and droppable headers, so the relay can be judged', async () => {
    const headers = await withStub('ok-json', async (stub) => {
      const response = await fetch(`${stub.origin}${STATUS}`, JSON_REQUEST);
      await response.text();
      return response.headers;
    });

    expect(headers.get('etag')).toBe(OK_JSON_ETAG);
    expect(headers.get('cache-control')).toBe(OK_JSON_CACHE_CONTROL);
    expect(headers.get('set-cookie')).toBe(DROPPED_SET_COOKIE);
    expect(headers.get(DROPPED_LEAK_HEADER)).toBe('leak-me');
  });

  test('okJsonBodyFor escapes a hostile target instead of breaking its own JSON', () => {
    const body = okJsonBodyFor('/v1/oddity/"quoted"');
    expect(JSON.parse(body)).toEqual({ stub: 'ok-json', target: '/v1/oddity/"quoted"' });
  });
});

// ---------------------------------------------------------------------------
// not-found-404 / method-405 — the two fallback statuses
// ---------------------------------------------------------------------------

describe('not-found-404 — the disk-fallback branch', () => {
  test('every target is 404 with the same JSON body', async () => {
    const seen = await withStub('not-found-404', async (stub) => {
      const responses = await Promise.all(
        [GAMES, STATUS, '/anything/else'].map((target) => fetch(`${stub.origin}${target}`)),
      );
      return Promise.all(
        responses.map(async (response) => ({
          status: response.status,
          type: response.headers.get('content-type'),
          body: await response.text(),
        })),
      );
    });

    seen.forEach((entry) => {
      expect(entry.status).toBe(404);
      expect(entry.type).toBe('application/json');
      expect(entry.body).toBe(NOT_FOUND_BODY);
    });
  });
});

describe('method-405 — the other disk-fallback status', () => {
  test('405 with an Allow header on every target', async () => {
    const { status, allow, body } = await withStub('method-405', async (stub) => {
      const response = await fetch(`${stub.origin}${STATUS}`);
      return {
        status: response.status,
        allow: response.headers.get('allow'),
        body: await response.text(),
      };
    });

    expect(status).toBe(405);
    expect(allow).toBe(METHOD_ALLOW_HEADER);
    expect(body).toBe(METHOD_NOT_ALLOWED_BODY);
  });
});

// ---------------------------------------------------------------------------
// redirect-302
// ---------------------------------------------------------------------------

describe('redirect-302 — the redirect a gateway must refuse', () => {
  test('302 + Location, and a client that does not follow leaves one request', async () => {
    const { status, location, requests } = await withStub('redirect-302', async (stub) => {
      const response = await fetch(`${stub.origin}${GAMES}`, { redirect: 'manual' });
      await response.text();
      return {
        status: response.status,
        location: response.headers.get('location'),
        requests: stub.requests(),
      };
    });

    expect(status).toBe(302);
    expect(location).toBe(REDIRECT_LOCATION);
    expect(requests).toHaveLength(1);
  });

  test('a follower loops here — which is exactly how the rig would see the bug', async () => {
    const requests = await withStub('redirect-302', async (stub) => {
      const [outcome] = await Promise.allSettled([
        fetch(`${stub.origin}${GAMES}`, { redirect: 'follow' }),
      ]);
      // Every target is a 302, so following never terminates on a 2xx: either
      // the client gives up (rejects) or it is still going. Both leave a trail.
      expect(outcome?.status).toBe('rejected');
      return stub.requests();
    });

    expect(requests.length).toBeGreaterThan(1);
    console.log(`  [redirect-302] a following client made ${requests.length} requests`);
  });
});

// ---------------------------------------------------------------------------
// portless
// ---------------------------------------------------------------------------

describe('portless — all three conditions of the offline probe', () => {
  test('502 + X-Portless: 1 + a Content-Type of exactly text/html', async () => {
    const seen = await withStub('portless', async (stub) => {
      const response = await fetch(`${stub.origin}${GAMES}`);
      return {
        status: response.status,
        portless: response.headers.get('x-portless'),
        type: response.headers.get('content-type'),
        body: await response.text(),
      };
    });

    expect(seen.status).toBe(502);
    expect(seen.portless).toBe('1');
    // Exactly this: no charset, no parameter, nothing to normalize away.
    expect(seen.type).toBe(PORTLESS_CONTENT_TYPE);
    expect(seen.type).not.toContain(';');
    expect(seen.body).toBe(PORTLESS_BODY);
  });
});

// ---------------------------------------------------------------------------
// oversize-9mib
// ---------------------------------------------------------------------------

describe('oversize-9mib — past the 8 MiB cap, and chunked', () => {
  test('9 MiB of valid JSON with no Content-Length', async () => {
    const seen = await withStub('oversize-9mib', async (stub) => {
      const response = await fetch(`${stub.origin}${GAMES}`, JSON_REQUEST);
      const bytes = new Uint8Array(await response.arrayBuffer());
      return {
        status: response.status,
        type: response.headers.get('content-type'),
        declared: response.headers.get('content-length'),
        bytes,
        pulled: stub.chunksPulled(),
      };
    });

    expect(seen.status).toBe(200);
    expect(seen.type).toBe('application/json');
    // Streamed, so the cap has to fire on bytes read rather than on a header.
    expect(seen.declared).toBeNull();
    expect(seen.bytes.byteLength).toBe(OVERSIZE_TOTAL_BYTES);
    expect(seen.bytes.byteLength).toBeGreaterThan(MAX_PROXY_JSON_BYTES);
    expect(seen.pulled).toBe(OVERSIZE_CHUNKS);
    const document: unknown = JSON.parse(new TextDecoder().decode(seen.bytes));
    const items = isRecord(document) ? document['items'] : undefined;
    expect(Array.isArray(items)).toBe(true);
    console.log(
      `  [oversize] ${seen.bytes.byteLength} bytes (cap ${MAX_PROXY_JSON_BYTES}) in ${seen.pulled} chunks`,
    );
  });

  test('a client that stops early stops the upstream — the drain is observable', async () => {
    const pulled = await withStub('oversize-9mib', async (stub) => {
      const response = await fetch(`${stub.origin}${GAMES}`, JSON_REQUEST);
      const reader = response.body?.getReader();
      expect(reader).toBeDefined();
      await reader?.read();
      await reader?.cancel();
      // The producer pauses 1ms per chunk, so a cancelled reader cannot have
      // let the whole 9 MiB out of the door.
      return stub.chunksPulled();
    });

    expect(pulled).toBeLessThan(OVERSIZE_CHUNKS);
    console.log(`  [oversize] cancelled after ${pulled} of ${OVERSIZE_CHUNKS} chunks`);
  });
});

// ---------------------------------------------------------------------------
// hang
// ---------------------------------------------------------------------------

describe('hang — accepted, read, never answered', () => {
  /** `--upstream-timeout-s 1`, the value the rig configures. */
  const TIMEOUT_MS = 1000;

  test('outlasts a one-second timeout, having read the request', async () => {
    const seen = await withStub('hang', async (stub) => {
      const startedAt = performance.now();
      const [outcome] = await Promise.allSettled([
        fetch(`${stub.origin}${GAMES}`, {
          ...JSON_REQUEST,
          signal: AbortSignal.timeout(TIMEOUT_MS * 1.5),
        }),
      ]);
      return { outcome, elapsed: performance.now() - startedAt, requests: stub.requests() };
    });

    expect(seen.outcome?.status).toBe('rejected');
    expect(seen.elapsed).toBeGreaterThan(TIMEOUT_MS);
    // The request was accepted and parsed: this is a read timeout, not a
    // refused connection — the two fail different code paths in the gateway.
    expect(seen.requests).toHaveLength(1);
    expect(seen.requests[0]?.target).toBe(GAMES);
    console.log(`  [hang] held ${seen.elapsed.toFixed(0)}ms without answering`);
  });

  test('close() returns promptly even with a request still held', async () => {
    const stub = makeStub('hang');
    const inflight = Promise.allSettled([
      fetch(`${stub.origin}${GAMES}`, { signal: AbortSignal.timeout(5000) }),
    ]);
    // Give the server time to accept and park the request.
    await Bun.sleep(50);
    expect(stub.requests()).toHaveLength(1);

    const startedAt = performance.now();
    await stub.close();
    const elapsed = performance.now() - startedAt;

    expect(elapsed).toBeLessThan(1000);
    await inflight;
    console.log(`  [hang] close() with one held request: ${elapsed.toFixed(0)}ms`);
  });
});

// ---------------------------------------------------------------------------
// binary-ok
// ---------------------------------------------------------------------------

describe('binary-ok — mp4 bytes and the conditional headers', () => {
  test('mp4-ish bytes with ETag and Last-Modified', async () => {
    const seen = await withStub('binary-ok', async (stub) => {
      const response = await fetch(`${stub.origin}${GAMES}/${STUB_LIVE_GAME_ID}/video.mp4`, {
        headers: { accept: '*/*' },
      });
      return {
        status: response.status,
        type: response.headers.get('content-type'),
        length: response.headers.get('content-length'),
        etag: response.headers.get('etag'),
        lastModified: response.headers.get('last-modified'),
        cookie: response.headers.get('set-cookie'),
        bytes: new Uint8Array(await response.arrayBuffer()),
        accept: stub.requests()[0]?.headers['accept'],
      };
    });

    expect(seen.status).toBe(200);
    expect(seen.type).toBe('video/mp4');
    expect(seen.length).toBe(String(MP4_BYTES.byteLength));
    expect(seen.etag).toBe(BINARY_ETAG);
    expect(seen.lastModified).toBe(BINARY_LAST_MODIFIED);
    expect(bytesOf(seen.bytes)).toEqual(bytesOf(MP4_BYTES));
    // `ftyp` at offset 4 — a truncating or transcoding relay loses this.
    expect(new TextDecoder().decode(seen.bytes.subarray(4, 8))).toBe('ftyp');
    // Sent so the allowlist has something to drop.
    expect(seen.cookie).toBe(DROPPED_SET_COOKIE);
    // The binary leg's forwarded Accept (`UPSTREAM_ACCEPT_BINARY`).
    expect(seen.accept).toBe('*/*');
  });

  test('a frame path gets PNG bytes with the same conditional headers', async () => {
    const seen = await withStub('binary-ok', async (stub) => {
      const response = await fetch(
        `${stub.origin}${GAMES}/${STUB_LIVE_GAME_ID}/frames/latest.png`,
        { headers: { accept: '*/*' } },
      );
      return {
        type: response.headers.get('content-type'),
        etag: response.headers.get('etag'),
        bytes: new Uint8Array(await response.arrayBuffer()),
      };
    });

    expect(seen.type).toBe('image/png');
    expect(seen.etag).toBe(BINARY_ETAG);
    expect(bytesOf(seen.bytes)).toEqual(bytesOf(PNG_BYTES));
  });

  test('If-None-Match is passed through and answered 304 with no body', async () => {
    const seen = await withStub('binary-ok', async (stub) => {
      const response = await fetch(`${stub.origin}${GAMES}/${STUB_LIVE_GAME_ID}/video.mp4`, {
        headers: { accept: '*/*', 'if-none-match': BINARY_ETAG },
      });
      const bytes = new Uint8Array(await response.arrayBuffer());
      return {
        status: response.status,
        etag: response.headers.get('etag'),
        lastModified: response.headers.get('last-modified'),
        length: bytes.byteLength,
        forwarded: stub.requests()[0]?.headers['if-none-match'],
      };
    });

    expect(seen.status).toBe(304);
    expect(seen.etag).toBe(BINARY_ETAG);
    expect(seen.lastModified).toBe(BINARY_LAST_MODIFIED);
    expect(seen.length).toBe(0);
    // The evidence the rig actually wants: the gateway forwarded the header.
    expect(seen.forwarded).toBe(BINARY_ETAG);
  });

  test('a stale If-None-Match still gets the full body', async () => {
    const seen = await withStub('binary-ok', async (stub) => {
      const response = await fetch(`${stub.origin}${GAMES}/${STUB_LIVE_GAME_ID}/video.mp4`, {
        headers: { 'if-none-match': '"stale"' },
      });
      return { status: response.status, length: (await response.arrayBuffer()).byteLength };
    });

    expect(seen.status).toBe(200);
    expect(seen.length).toBe(MP4_BYTES.byteLength);
  });
});

// ---------------------------------------------------------------------------
// The handle itself
// ---------------------------------------------------------------------------

describe('the handle — one instance per mode, and it releases', () => {
  test('every mode starts, binds its own ephemeral port, and closes', async () => {
    const stubs = STUB_MODES.map((mode) => makeStub(mode));
    const ports = stubs.map((stub) => stub.port);

    expect(new Set(ports).size).toBe(STUB_MODES.length);
    ports.forEach((port) => {
      expect(port).toBeGreaterThan(0);
    });
    expect(stubs.map((stub) => stub.mode)).toEqual([...STUB_MODES]);
    stubs.forEach((stub) => {
      expect(stub.origin).toBe(`http://127.0.0.1:${stub.port}`);
      expect(stub.requests()).toEqual([]);
    });

    await Promise.all(stubs.map((stub) => stub.close()));
  });

  test('close() is idempotent', async () => {
    const stub = makeStub('ok-json');
    await stub.close();
    await stub.close();
    expect(stub.requests()).toEqual([]);
  });

  test('requests() hands back a snapshot, not the live log', async () => {
    const [before, after] = await withStub('ok-json', async (stub) => {
      const snapshot = stub.requests();
      await fetch(`${stub.origin}${GAMES}`);
      return [snapshot, stub.requests()] as const;
    });

    expect(before).toHaveLength(0);
    expect(after).toHaveLength(1);
  });

  test('await using releases it too', async () => {
    const captured = await (async (): Promise<number> => {
      await using stub = makeStub('ok-json');
      const response = await fetch(`${stub.origin}${GAMES}`);
      await response.text();
      return stub.port;
    })();

    const [outcome] = await Promise.allSettled([
      fetch(`http://127.0.0.1:${captured}${GAMES}`, { signal: AbortSignal.timeout(1000) }),
    ]);
    expect(outcome?.status).toBe('rejected');
  });
});
