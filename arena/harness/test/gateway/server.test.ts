/** Raw HTTP parity plus startup publication and resource-ordering coverage. */
import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { Gateway } from '@arena/wire';
import {
  makeWideEvent,
  Observability,
  ObservabilityNoop,
  ObservabilityTest,
  sealWideEvent,
  TelemetryCapture,
  telemetryConfigLayer,
} from '@arena/telemetry';
import { HelpDoc, ValidationError } from '@effect/cli';
import { Headers } from '@effect/platform';
import { NodeContext, NodeHttpServer } from '@effect/platform-node';
import { Cause, Console, Data, Effect, Either, Exit, FiberId, Layer, Option, Ref } from 'effect';
import { mkdirSync, mkdtempSync, rmSync, statSync } from 'node:fs';
import { createServer } from 'node:http';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  GATEWAY_CLI_ERROR_EXIT_CODE,
  DEFAULT_REPO_ROOT,
} from 'src/gateway/cli.ts';
import { GatewayConfig, gatewayConfigLayer } from 'src/gateway/config.ts';
import {
  describeStartupError,
  GATEWAY_TELEMETRY_DIR_ENV,
  gatewayTeardown,
  isValidationError,
  reportStartupFailure,
  telemetryLayer,
} from 'src/gateway/main.ts';
import {
  bodySignal,
  gatewayApp,
  GATEWAY_REQUEST_EVENT,
  serveGateway,
  splitTarget,
  stdlibErrorPage,
  UNSUPPORTED_METHOD_EXPLAIN,
  unsupportedMethod,
  unsupportedMethodMessage,
  STDLIB_ERROR_CONTENT_TYPE,
  type GatewayHandle,
} from 'src/gateway/server.ts';
import type { RequestBodySignal } from 'src/gateway/http/dispatch.ts';
import { ReplayDerivationUnavailable } from './support/derivation.ts';
import {
  GatewayIdentity,
  makeGatewayIdentity,
} from 'src/gateway/http/routes/health.ts';
import {
  ReadyFile,
  ReadyFileIoError,
  ReadyFileLocked,
  type ReadyLineSink,
  layer as readyFileLayer,
} from 'src/gateway/services/ready-file.ts';
import { layer as runsRepositoryLayer } from 'src/gateway/services/runs.ts';
import { layerLive as upstreamClientLayer } from 'src/gateway/services/upstream.ts';

const HOST = '127.0.0.1';
const EXCHANGE_TIMEOUT = '5 seconds';
const GAME_ID = 'game_gggggggggggggggggggggggg';

// ---------------------------------------------------------------------------
// Temporary roots
// ---------------------------------------------------------------------------

const roots = mkdtempSync(join(tmpdir(), 'arena-gateway-server-'));
const runsRoot = join(roots, 'runs');
const cacheRoot = join(roots, 'cache', 'nested');
const readyDirectory = join(roots, 'ready');

beforeAll(() => {
  mkdirSync(runsRoot, { recursive: true });
  mkdirSync(readyDirectory, { recursive: true });
});

afterAll(async () => {
  // `stop(true)` closes in-flight connections too; awaited so the suite cannot
  // finish with the stub still accepting.
  await upstreamStub.stop(true);
  rmSync(roots, { recursive: true, force: true });
});

// ---------------------------------------------------------------------------
// The upstream: a stub that answers with the portless-offline signature
// ---------------------------------------------------------------------------

/**
 * Every route except `/health` opens the upstream first, so the suite needs one
 * that is reliably "down".
 *
 * A *released ephemeral port* is the obvious trick and it is wrong: the gateway
 * under test binds `port 0` moments later and the kernel will sometimes hand it
 * the very same number, at which point the gateway proxies to itself and the
 * request hangs until the client's timeout.  (It did, once, which is why this
 * comment exists.)
 *
 * So the stub is a real server that answers the exact portless signature
 * `_open_upstream` looks for (`:1419`): status **502**, `X-Portless: 1`, and a
 * media type of exactly `text/html`.  All three are required — the dossier's
 * trap X1 — and together they are the *only* 502 that means "the supervisor is
 * not running".  It also records what it was asked for, so a test can assert
 * that a refusal never reached the network.
 */
const upstreamRequests: Array<string> = [];

const upstreamStub = Bun.serve({
  port: 0,
  hostname: HOST,
  fetch: (request) => {
    upstreamRequests.push(new URL(request.url).pathname);
    return new Response('<html>portless</html>', {
      status: 502,
      headers: { 'X-Portless': '1', 'Content-Type': 'text/html; charset=utf-8' },
    });
  },
});

const OFFLINE_UPSTREAM = `http://${HOST}:${upstreamStub.port ?? 0}`;

// ---------------------------------------------------------------------------
// Raw HTTP/1.1 client — `fetch` cannot send a GET body, a bare HEAD, a
// fragment, an absolute-form target or an unmapped verb.
// ---------------------------------------------------------------------------

class WireError extends Data.TaggedError('WireError')<{ readonly cause: unknown }> {}

/**
 * A response is complete when its `Content-Length` worth of body has arrived —
 * not when the socket closes.
 *
 * Measured, and the reason this client is not the spike's: Bun does **not**
 * promptly close a connection whose response came from a handler that made an
 * outbound `fetch`, even with `Connection: close` on the request.  A
 * close-terminated client blocks for ~12 seconds on `//v1/games` (which is the
 * only case in the table that reaches upstream) and instantly on `/health`,
 * which is exactly the shape of a flaky suite.  Completing on the advertised
 * length is also what a correct HTTP/1.1 client does.
 *
 * `expectBody: false` is HEAD: `Content-Length` advertises the GET body that
 * the adapter stripped, so waiting for those bytes would wait forever.
 */
const exchangeBytes = (
  port: number,
  payload: string,
  expectBody: boolean,
): Promise<Buffer> =>
  new Promise<Buffer>((resolve, reject) => {
    const chunks: Array<Uint8Array> = [];
    const finished = (): Buffer | null => {
      const buffer = Buffer.concat(chunks);
      const head = buffer.toString('binary');
      const at = head.indexOf('\r\n\r\n');
      if (at === -1) return null;
      if (!expectBody) return buffer;
      const length = /\r\ncontent-length:\s*(\d+)/i.exec(head.slice(0, at + 2));
      if (length === null) return null;
      const expected = Number.parseInt(length[1] ?? '0', 10);
      return buffer.length - (at + 4) >= expected ? buffer : null;
    };
    Bun.connect({
      hostname: HOST,
      port,
      socket: {
        open: (socket) => {
          socket.write(payload);
        },
        data: (socket, received) => {
          chunks.push(new Uint8Array(received));
          const complete = finished();
          if (complete !== null) {
            socket.end();
            // `resolve` is idempotent, so the `close` below is harmless.
            resolve(complete);
          }
        },
        close: () => {
          resolve(Buffer.concat(chunks));
        },
        error: (_socket, cause) => {
          reject(cause);
        },
      },
    }).then(undefined, reject);
  });

interface WireResponse {
  readonly status: number;
  readonly reason: string;
  readonly headers: ReadonlyMap<string, string>;
  readonly body: string;
  readonly bodyBytes: Uint8Array;
}

const parseWire = (raw: Buffer): WireResponse => {
  const text = raw.toString('binary');
  const separator = text.indexOf('\r\n\r\n');
  const head = separator === -1 ? text : text.slice(0, separator);
  const bodyBytes =
    separator === -1 ? new Uint8Array() : new Uint8Array(raw.subarray(separator + 4));
  const [statusLine = '', ...headerLines] = head.split('\r\n');
  const [, statusCode = '0', ...reasonWords] = statusLine.split(' ');
  return {
    status: Number.parseInt(statusCode, 10),
    reason: reasonWords.join(' '),
    headers: new Map(
      headerLines
        .filter((line) => line.includes(':'))
        .map((line): readonly [string, string] => [
          line.slice(0, line.indexOf(':')).trim().toLowerCase(),
          line.slice(line.indexOf(':') + 1).trim(),
        ]),
    ),
    body: new TextDecoder().decode(bodyBytes),
    bodyBytes,
  };
};

/** Send a hand-rolled request so the verb, the target and the framing are ours. */
const wire = (
  port: number,
  options: {
    readonly method: string;
    readonly target: string;
    readonly headers?: ReadonlyArray<readonly [string, string]> | undefined;
    readonly body?: string | undefined;
  },
): Effect.Effect<WireResponse> => {
  const lines = [
    `${options.method} ${options.target} HTTP/1.1`,
    `Host: ${HOST}:${port}`,
    'Connection: close',
    ...(options.headers ?? []).map(([name, value]) => `${name}: ${value}`),
  ];
  const payload = `${lines.join('\r\n')}\r\n\r\n${options.body ?? ''}`;
  return Effect.tryPromise({
    try: () => exchangeBytes(port, payload, options.method !== 'HEAD'),
    catch: (cause) => new WireError({ cause }),
  }).pipe(Effect.timeout(EXCHANGE_TIMEOUT), Effect.orDie, Effect.map(parseWire));
};

/** 'open' if something still accepts on `port`, 'refused' otherwise. */
const probeConnect = (port: number): Effect.Effect<'open' | 'refused'> =>
  Effect.tryPromise(() =>
    Bun.connect({
      hostname: HOST,
      port,
      socket: {
        open: (socket) => {
          socket.end();
        },
        data: () => {},
        close: () => {},
        error: () => {},
      },
    }),
  ).pipe(
    Effect.timeout(EXCHANGE_TIMEOUT),
    Effect.match({ onSuccess: () => 'open' as const, onFailure: () => 'refused' as const }),
  );

// ---------------------------------------------------------------------------
// The ready sink that probes, which is the ordering assertion itself
// ---------------------------------------------------------------------------

/** What the sink observed at the instant the ready record became visible. */
interface ReadyProbe {
  readonly line: string;
  readonly status: number;
  readonly body: string;
}

/** Read one string field out of a JSON object text without an unchecked cast. */
const stringField = (text: string, key: string): string => {
  const value: unknown = JSON.parse(text);
  if (typeof value !== 'object' || value === null) return '';
  const found = Object.entries(value).find(([name]) => name === key);
  return typeof found?.[1] === 'string' ? found[1] : '';
};

/**
 * A `ReadyLineSink` that answers the question the ordering rule exists for:
 * *at the moment a reader can see this record, does the URL in it work?*
 *
 * The Python suite asks it the same way (`urlopen(observed[0]["url"] +
 * "/health")`).  Doing it from the sink rather than from the test body removes
 * the only interesting timing gap: there is no `await` between publication and
 * probe that a scheduler could fill.
 */
const probingSink = (into: Array<ReadyProbe>): ReadyLineSink => (line) =>
  Effect.promise(async () => {
    const response = await fetch(`${stringField(line, 'url')}/health`);
    into.push({ line, status: response.status, body: await response.text() });
  });

/** Node listens before `serve` attaches a handler, but emits no fallback body. */
const probingUnservedSink = (into: Array<ReadyProbe>): ReadyLineSink => (line) =>
  Effect.promise(async () => {
    try {
      const response = await fetch(`${stringField(line, 'url')}/health`, {
        signal: AbortSignal.timeout(50),
      });
      into.push({ line, status: response.status, body: await response.text() });
    } catch {
      into.push({ line, status: 0, body: '' });
    }
  });

// ---------------------------------------------------------------------------
// The layer stack under test
// ---------------------------------------------------------------------------

interface HarnessOptions {
  readonly readyFile: string;
  readonly sink: ReadyLineSink;
  readonly readyLayer?: Layer.Layer<ReadyFile>;
  readonly port?: bigint;
}

const configArguments = (readyFile: string, port = 0n) => ({
  host: HOST,
  port,
  serviceUrl: OFFLINE_UPSTREAM,
  runsRoot,
  cacheRoot,
  repoRoot: DEFAULT_REPO_ROOT,
  readyFile,
  upstreamTimeoutSeconds: 2,
  viewerPublicUrl: Option.none<string>(),
});

/**
 * The production stack with three substitutions, each named because it is a
 * deliberate reduction in coverage rather than a convenience:
 *
 * - the derivation bridge is `ReplayDerivationUnavailable` — no `python3` is
 *   spawned, because this suite is about the socket edge and
 *   `test/gateway/derivation.test.ts` owns the bridge;
 * - the observability backend is the *capturing* one, so the wide events are
 *   assertable and nothing is written to disk;
 * - the ready sink is {@link probingSink} instead of stdout.
 */
const harnessLayer = (options: HarnessOptions) =>
  Layer.mergeAll(
    runsRepositoryLayer(runsRoot),
    upstreamClientLayer({ serviceUrl: OFFLINE_UPSTREAM, timeout: '2 seconds' }),
    options.readyLayer ?? readyFileLayer({ path: options.readyFile, sink: options.sink }),
    ReplayDerivationUnavailable,
    ObservabilityTest.pipe(
      Layer.provide(
        telemetryConfigLayer({
          service: 'arena-gateway-test',
          environment: 'test',
          ndjsonDir: join(roots, 'telemetry'),
        }),
      ),
      Layer.orDie,
    ),
    gatewayConfigLayer(configArguments(options.readyFile, options.port)).pipe(
      Layer.provide(NodeContext.layer),
      Layer.orDie,
    ),
    NodeContext.layer,
  );

/** Start a gateway, hand it to `use`, and tear the whole scope down after. */
const withGateway = <A>(
  options: HarnessOptions,
  use: (handle: GatewayHandle) => Effect.Effect<A, never, TelemetryCapture>,
): Promise<A> =>
  Effect.runPromise(
    Effect.scoped(Effect.flatMap(serveGateway, use)).pipe(
      Effect.provide(harnessLayer(options)),
      Effect.orDie,
    ),
  );

const readyPath = (name: string): string => join(readyDirectory, `${name}.json`);

// ---------------------------------------------------------------------------
// 1. Startup order
// ---------------------------------------------------------------------------

describe('startup: the ready record is published only after serve() attaches the app', () => {
  test('the URL in the record answers /health at the instant the record exists', async () => {
    const probes: Array<ReadyProbe> = [];
    const observed = await withGateway(
      { readyFile: readyPath('ordering'), sink: probingSink(probes) },
      (handle) => Effect.succeed({ url: handle.url, port: handle.port }),
    );

    expect(probes).toHaveLength(1);
    const probe = probes[0];
    // Not 404: the built-in handler is gone by the time anyone can read this.
    expect(probe?.status).toBe(200);
    expect(probe?.body).toContain('"ok":true');
    expect(probe?.body).toContain(`"port":${observed.port}`);
    // The line is the canonical one-line spelling of the same payload.
    expect(stringField(probe?.line ?? '', 'url')).toBe(observed.url);
    expect(observed.url).toBe(`http://${HOST}:${observed.port}`);
  });

  test('the guard has teeth: publishing before serve() exposes no gateway response', async () => {
    // Node has bound and is accepting, but no request listener exists until
    // `serve(app)`. Publishing in that window gives a client a URL that hangs.
    const probes: Array<ReadyProbe> = [];
    const misordered = Effect.gen(function* () {
      const config = yield* GatewayConfig;
      const server = yield* NodeHttpServer.make(createServer, { port: 0, host: HOST });
      const address = server.address;
      const port = address._tag === 'TcpAddress' ? address.port : -1;
      const identity = makeGatewayIdentity({ config, boundPort: port, pid: process.pid });
      const readyFile = yield* ReadyFile;
      yield* readyFile.publish(identity.payload);
      yield* server.serve(Effect.provideService(gatewayApp, GatewayIdentity, identity));
      return port;
    });

    await Effect.runPromise(
      Effect.scoped(misordered).pipe(
        Effect.provide(
          harnessLayer({ readyFile: readyPath('misordered'), sink: probingUnservedSink(probes) }),
        ),
        Effect.asVoid,
        Effect.orDie,
      ),
    );

    expect(probes).toEqual([{ line: probes[0]?.line ?? '', status: 0, body: '' }]);
  });

  test('shutdown closes the listener before ready cleanup runs', async () => {
    const observed: Array<string> = [];
    const orderingLayer = Layer.succeed(ReadyFile, {
      publish: (payload) =>
        Effect.gen(function* () {
          const port = payload['port'];
          yield* Effect.addFinalizer(() =>
            typeof port === 'bigint'
              ? Effect.flatMap(probeConnect(Number(port)), (state) =>
                  Effect.sync(() => observed.push(`ready-cleanup:${state}`)),
                )
              : Effect.sync(() => observed.push('ready-cleanup:invalid-port')),
          );
          return {
            path: Option.none(),
            lockPath: Option.none(),
            bytes: Option.none(),
            line: '',
          };
        }),
    });

    await withGateway(
      {
        readyFile: readyPath('shutdown-order'),
        sink: probingSink([]),
        readyLayer: orderingLayer,
      },
      () => Effect.void,
    );

    expect(observed).toEqual(['ready-cleanup:refused']);
  });

  test('a refused bind is a typed startup failure and publishes nothing', async () => {
    const blocker = Bun.serve({
      hostname: HOST,
      port: 0,
      fetch: () => new Response('occupied'),
    });
    const path = readyPath('bind-failure');
    try {
      const outcome = await Effect.runPromise(
        Effect.either(
          Effect.scoped(serveGateway).pipe(
            Effect.provide(
              harnessLayer({
                readyFile: path,
                sink: probingSink([]),
                port: BigInt(blocker.port ?? 0),
              }),
            ),
          ),
        ),
      );
      expect(Either.isLeft(outcome)).toBe(true);
      if (Either.isLeft(outcome)) expect(outcome.left._tag).toBe('ServeError');
      expect(() => statSync(path)).toThrow();
    } finally {
      await blocker.stop(true);
    }
  });

  test('cache_root is created before anything is served, parents included', async () => {
    await withGateway({ readyFile: readyPath('cache'), sink: probingSink([]) }, () =>
      Effect.void,
    );
    expect(statSync(cacheRoot).isDirectory()).toBe(true);
  });

  test('the ready file is 0600 and is removed, with its port, when the scope closes', async () => {
    const path = readyPath('lifecycle');
    const probes: Array<ReadyProbe> = [];
    const observed = await withGateway({ readyFile: path, sink: probingSink(probes) }, (handle) =>
      Effect.sync(() => ({
        port: handle.port,
        mode: statSync(path).mode & 0o777,
        exists: Option.isSome(handle.ready.path),
      })),
    );

    expect(observed.exists).toBe(true);
    expect(observed.mode).toBe(0o600);
    // `finally: server_close(); _remove_owned_ready_file(...)` (`:2164-2166`).
    expect(() => statSync(path)).toThrow();
    expect(await Effect.runPromise(probeConnect(observed.port))).toBe('refused');
    // The `.lock` companion is deliberately NOT unlinked (`:2169` releases it).
    expect(statSync(`${path}.lock`).isFile()).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// 2. The request line
// ---------------------------------------------------------------------------

interface Case {
  readonly name: string;
  readonly method?: string;
  readonly target: string;
  readonly headers?: ReadonlyArray<readonly [string, string]>;
  readonly body?: string;
  readonly status: number;
  readonly check: (response: WireResponse) => void;
}

const jsonError = (message: string) => (response: WireResponse) => {
  expect(response.body).toBe(`{"error":${JSON.stringify(message)}}`);
  expect(response.headers.get('content-type')).toBe(Gateway.GATEWAY_PROBLEM_CONTENT_TYPE);
  expect(response.headers.get('cache-control')).toBe(Gateway.GATEWAY_PROBLEM_CACHE_CONTROL);
  expect(response.headers.get('x-content-type-options')).toBe('nosniff');
  expect(response.headers.get('referrer-policy')).toBe('no-referrer');
};

const REQUEST_CASES: ReadonlyArray<Case> = [
  {
    name: '/health is the identity, canonically spelled',
    target: '/health',
    status: 200,
    check: (response) => {
      expect(response.body).toContain('"ok":true');
      expect(response.body).toContain(`"kind":${JSON.stringify(Gateway.GATEWAY_KIND)}`);
      expect(response.headers.get('content-type')).toBe(Gateway.GATEWAY_PROBLEM_CONTENT_TYPE);
      expect(response.headers.get('cache-control')).toBe('no-store');
      expect(response.headers.get('x-content-type-options')).toBe('nosniff');
      expect(response.headers.get('referrer-policy')).toBe('no-referrer');
    },
  },
  {
    name: 'literal dot segments remain in the raw target and do not route',
    target: '/v1/games/../health',
    status: 404,
    check: jsonError(Gateway.GATEWAY_PROBLEM_MESSAGES.notFound),
  },
  {
    name: 'percent-encoded dot segments remain in the raw target and do not route',
    target: '/v1/games/%2e%2e/health',
    status: 404,
    check: jsonError(Gateway.GATEWAY_PROBLEM_MESSAGES.notFound),
  },
  {
    name: 'a bare trailing ? is no query at all',
    target: '/health?',
    status: 200,
    check: (response) => expect(response.body).toContain('"ok":true'),
  },
  {
    name: 'a fragment is dropped before routing',
    target: '/health#frag',
    status: 200,
    check: (response) => expect(response.body).toContain('"ok":true'),
  },
  {
    name: 'a fragment hides a would-be query, exactly as urlsplit reports it',
    target: '/health#a?b=c',
    status: 200,
    check: (response) => expect(response.body).toContain('"ok":true'),
  },
  {
    name: 'an absolute-form target is accepted and its netloc ignored',
    target: 'http://evil.invalid/health',
    status: 200,
    check: (response) => expect(response.body).toContain('"ok":true'),
  },
  {
    name: '/health rejects any query with its own message',
    target: '/health?token=private',
    status: 400,
    check: jsonError(Gateway.GATEWAY_PROBLEM_MESSAGES.healthQuery),
  },
  {
    name: '//v1/games collapses to /v1/games and serves the index',
    target: '//v1/games',
    status: 200,
    check: (response) => {
      expect(response.body).toBe('{"games":[],"schema_version":1}');
    },
  },
  {
    name: '/v1/games rejects any query with its own message',
    target: '/v1/games?x=1',
    status: 400,
    check: jsonError(Gateway.GATEWAY_PROBLEM_MESSAGES.gamesIndexQuery),
  },
  {
    name: '/v1/games/ (trailing slash) is not the index route',
    target: '/v1/games/',
    status: 404,
    check: jsonError(Gateway.GATEWAY_PROBLEM_MESSAGES.notFound),
  },
  {
    name: '%2F is never decoded into a path separator',
    target: `/v1/games/${GAME_ID}%2Fstatus`,
    status: 404,
    check: jsonError(Gateway.GATEWAY_PROBLEM_MESSAGES.notFound),
  },
  {
    name: 'the id check precedes the query check: a short id is 404, not 400',
    target: '/v1/games/short/status?x=1',
    status: 404,
    check: jsonError(Gateway.GATEWAY_PROBLEM_MESSAGES.notFound),
  },
  {
    name: 'the query check precedes the final 404: an unroutable suffix with a query is 400',
    target: `/v1/games/${GAME_ID}/nonsense?x=1`,
    status: 400,
    check: jsonError(Gateway.GATEWAY_PROBLEM_MESSAGES.viewerRouteQuery),
  },
  {
    name: 'a leading-zero frame index never routes',
    target: `/v1/games/${GAME_ID}/frames/007.png`,
    status: 404,
    check: jsonError(Gateway.GATEWAY_PROBLEM_MESSAGES.notFound),
  },
  {
    name: 'a GET body is refused before the path is even looked at',
    target: '/v1/games/unroutable-nonsense',
    headers: [['Content-Length', '13']],
    body: '{"hello":"x"}',
    status: 400,
    check: jsonError(Gateway.GATEWAY_PROBLEM_MESSAGES.getRequestBody),
  },
  {
    name: 'a chunked GET is a body, and it is ours to refuse',
    target: '/health',
    headers: [['Transfer-Encoding', 'chunked']],
    body: '0\r\n\r\n',
    status: 400,
    check: jsonError(Gateway.GATEWAY_PROBLEM_MESSAGES.getRequestBody),
  },
  {
    name: 'Content-Length: 0 is not a body',
    target: '/health',
    headers: [['Content-Length', '0']],
    status: 200,
    check: (response) => expect(response.body).toContain('"ok":true'),
  },
  {
    // DIVERGENCE, measured: Python answers `400 invalid Content-Length` from
    // `_reject_body` (`:1390`); Node's parser refuses the message before any
    // Effect code runs and writes its own bodiless `400 Bad Request`. Same
    // status, different body, and nothing this layer can reach. The *decision*
    // is still pinned — see the `bodySignal` table below.
    name: "an unparseable Content-Length never reaches us: Node's own 400",
    target: '/health',
    headers: [['Content-Length', 'abc']],
    status: 400,
    check: (response) => {
      expect(response.body).toBe('');
      expect(response.headers.has('x-content-type-options')).toBe(false);
    },
  },
  {
    // Node's parser rejects this transfer-coding on Darwin but delivers it on
    // Linux. Both paths answer 400; Linux reaches `_reject_body` like Python.
    name: 'Transfer-Encoding: identity is rejected before serving',
    target: '/health',
    headers: [['Transfer-Encoding', 'identity']],
    status: 400,
    check: (response) => {
      if (process.platform === 'linux') {
        expect(response.body).toBe('{"error":"GET request bodies are not accepted"}');
        expect(response.headers.get('x-content-type-options')).toBe('nosniff');
      } else {
        expect(response.body).toBe('');
        expect(response.headers.has('x-content-type-options')).toBe(false);
      }
    },
  },
];

describe('the request line, spoken down a raw socket', () => {
  REQUEST_CASES.forEach((one, index) => {
    test(one.name, async () => {
      const response = await withGateway(
        { readyFile: readyPath(`case-${index}`), sink: probingSink([]) },
        (handle) =>
          wire(handle.port, {
            method: one.method ?? 'GET',
            target: one.target,
            headers: one.headers,
            body: one.body,
          }),
      );
      expect(response.status).toBe(one.status);
      one.check(response);
    });
  });

  test('a raw cross-game A/../B target is rejected before any upstream request', async () => {
    const gameA = 'game_AAAAAAAAAAAAAAAAAAAAAAAA';
    const gameB = 'game_BBBBBBBBBBBBBBBBBBBBBBBB';
    const before = upstreamRequests.length;
    const response = await withGateway(
      { readyFile: readyPath('cross-game-dotdot'), sink: probingSink([]) },
      (handle) =>
        wire(handle.port, {
          method: 'GET',
          target: `/v1/games/${gameA}/../${gameB}/replay.json`,
        }),
    );
    expect(response.status).toBe(404);
    expect(response.body).toBe(`{"error":"${Gateway.GATEWAY_PROBLEM_MESSAGES.notFound}"}`);
    expect(upstreamRequests.length).toBe(before);
  });
});

// ---------------------------------------------------------------------------
// 3. Methods
// ---------------------------------------------------------------------------

describe('methods: the stdlib answers the unmapped ones, we answer the rest', () => {
  const MAPPED: ReadonlyArray<string> = ['POST', 'PUT', 'PATCH', 'DELETE', 'OPTIONS'];

  MAPPED.forEach((method) => {
    test(`${method} on a private route is the JSON 405 with Allow: GET`, async () => {
      const response = await withGateway(
        { readyFile: readyPath(`method-${method}`), sink: probingSink([]) },
        (handle) => wire(handle.port, { method, target: `/v1/games/${GAME_ID}/join` }),
      );
      expect(response.status).toBe(405);
      expect(response.headers.get('allow')).toBe(Gateway.GATEWAY_METHOD_NOT_ALLOWED_ALLOW);
      expect(response.body).toBe(
        `{"error":"${Gateway.GATEWAY_PROBLEM_MESSAGES.methodNotAllowed}"}`,
      );
    });
  });

  test('HEAD keeps the headers and the Content-Length, and loses only the body', async () => {
    const response = await withGateway(
      { readyFile: readyPath('method-HEAD'), sink: probingSink([]) },
      (handle) => wire(handle.port, { method: 'HEAD', target: '/health' }),
    );
    expect(response.status).toBe(405);
    expect(response.headers.get('allow')).toBe('GET');
    expect(response.body).toBe('');
    // 30 = len(b'{"error":"method not allowed"}'), computed from the GET body
    // the adapter then stripped (dossier §7.2).
    expect(response.headers.get('content-length')).toBe('30');
  });

  const UNMAPPED: ReadonlyArray<string> = ['TRACE'];

  UNMAPPED.forEach((method) => {
    test(`${method} is the stdlib HTML 501, with none of our headers`, async () => {
      const response = await withGateway(
        { readyFile: readyPath(`method-${method}`), sink: probingSink([]) },
        (handle) => wire(handle.port, { method, target: '/health' }),
      );
      expect(response.status).toBe(501);
      expect(response.headers.get('content-type')).toBe(STDLIB_ERROR_CONTENT_TYPE);
      expect(response.body).toBe(
        stdlibErrorPage(501, unsupportedMethodMessage(method), UNSUPPORTED_METHOD_EXPLAIN),
      );
      // The three headers `send_error` never writes, because it never reaches
      // `_send_headers` (`:1335`).
      expect(response.headers.has('x-content-type-options')).toBe(false);
      expect(response.headers.has('referrer-policy')).toBe(false);
      expect(response.headers.has('cache-control')).toBe(false);
      // DIVERGENCE, measured: Python's reason phrase carries the verb
      // (`501 Unsupported method ('TRACE')`). Node writes the canonical phrase
      // for the status whatever the `Response.statusText` says, so the wire
      // says `Not Implemented` — while the value we built still carries the
      // right text, asserted below.
      expect(response.reason).toBe('Not Implemented');
    });
  });

  test('CONNECT is intercepted by Node before the request handler', async () => {
    // Node emits CONNECT on its separate `connect` event. The Effect adapter
    // intentionally installs only request/upgrade handlers, so no response is
    // available to gateway code at this layer.
    const response = await withGateway(
      { readyFile: readyPath('method-CONNECT'), sink: probingSink([]) },
      (handle) => wire(handle.port, { method: 'CONNECT', target: '/health' }),
    );
    expect(response.status).toBe(0);
    expect(response.body).toBe('');
  });

  test('the response value carries the reason phrase Node then discards', () => {
    expect(unsupportedMethod('TRACE').statusText).toBe("Unsupported method ('TRACE')");
    expect(unsupportedMethod('TRACE').status).toBe(501);
  });

  test("an invented verb never reaches us at all — Node's parser writes 400", async () => {
    // DIVERGENCE, measured: Python answers `501 Unsupported method ('FOO')`.
    // Node's parser rejects the method token before the request event and emits
    // its own 400, so there is nothing for the Effect app to answer with.
    const response = await withGateway(
      { readyFile: readyPath('method-FOO'), sink: probingSink([]) },
      (handle) => wire(handle.port, { method: 'FOO', target: '/health' }),
    );
    expect(response.status).toBe(400);
    expect(response.body).toBe('');
  });

  test("the 501 page is byte-for-byte CPython 3.14's, 483 bytes for TRACE", () => {
    const page = stdlibErrorPage(
      501,
      unsupportedMethodMessage('TRACE'),
      UNSUPPORTED_METHOD_EXPLAIN,
    );
    expect(new TextEncoder().encode(page).byteLength).toBe(483);
    expect(page).toContain("<p>Message: Unsupported method ('TRACE').</p>");
    expect(page).toContain(
      '<p>Error code explanation: 501 - Server does not support this operation.</p>',
    );
  });

  test('the message is HTML-escaped, and quotes are left alone', () => {
    expect(stdlibErrorPage(400, '<script>&', 'x')).toContain(
      '<p>Message: &lt;script&gt;&amp;.</p>',
    );
    expect(unsupportedMethod('FOO').status).toBe(501);
    expect(unsupportedMethod('FOO').headers['connection']).toBe('close');
  });
});

// ---------------------------------------------------------------------------
// 4. Telemetry
// ---------------------------------------------------------------------------

describe('telemetry: exactly one wide event per request, refusals included', () => {
  /**
   * `evlog`'s emitted event is a flat record — `toEvlogContext` spreads the
   * sealed fields next to `event`, `eventId` and `durationMs` — so a reader
   * looks the annotations up by their own key and not under a `fields` object.
   */
  const field = (event: Readonly<Record<string, unknown>>, key: string): unknown => event[key];

  test('a served request, a refused one and an unmapped verb each produce one event', async () => {
    const events = await withGateway(
      { readyFile: readyPath('telemetry'), sink: probingSink([]) },
      (handle) =>
        Effect.gen(function* () {
          const capture = yield* TelemetryCapture;
          // Drain the event the sink's own /health probe produced.
          yield* capture.takeEvents;
          yield* wire(handle.port, { method: 'GET', target: '/health' });
          yield* wire(handle.port, { method: 'GET', target: '/health?x=1' });
          yield* wire(handle.port, { method: 'TRACE', target: '/health' });
          return yield* capture.takeEvents;
        }),
    );

    expect(events).toHaveLength(3);
    expect(events.map((event) => field(event, 'event'))).toEqual([
      GATEWAY_REQUEST_EVENT,
      GATEWAY_REQUEST_EVENT,
      GATEWAY_REQUEST_EVENT,
    ]);
    expect(events.map((event) => field(event, 'http.status_code'))).toEqual([200, 400, 501]);
    expect(events.map((event) => field(event, 'outcome'))).toEqual([
      'success',
      'success',
      'success',
    ]);
    expect(field(events[0] ?? {}, 'gateway.route')).toBe('Health');
    expect(field(events[2] ?? {}, 'gateway.route')).toBe('UnsupportedMethod');
    // A rejected query never reaches a route, so no route is claimed.
    expect(field(events[1] ?? {}, 'gateway.route')).toBeUndefined();
    // The path is recorded; the query's *value* deliberately is not.
    expect(field(events[1] ?? {}, 'http.path')).toBe('/health');
    expect(field(events[1] ?? {}, 'http.query')).toBe(true);
    expect(field(events[1] ?? {}, 'http.method')).toBe('GET');
  });

  test('the outcome is `success` even for a 400, because the request was served', async () => {
    // `outcome` is the *effect's* exit, not the HTTP status: `respondGateway`
    // turns every refusal into a response, so the app succeeds. The status is
    // the field that says the client was refused, which is why both are
    // recorded.
    const events = await withGateway(
      { readyFile: readyPath('telemetry-outcome'), sink: probingSink([]) },
      (handle) =>
        Effect.gen(function* () {
          const capture = yield* TelemetryCapture;
          yield* capture.takeEvents;
          yield* wire(handle.port, { method: 'POST', target: '/health' });
          return yield* capture.takeEvents;
        }),
    );
    expect(events).toHaveLength(1);
    expect(field(events[0] ?? {}, 'http.status_code')).toBe(405);
    expect(field(events[0] ?? {}, 'outcome')).toBe('success');
  });
});

// ---------------------------------------------------------------------------
// 5. Upstream-first, and refusals that never reach the network
// ---------------------------------------------------------------------------

describe('a refusal is a promise that nothing was proxied', () => {
  test('only the routes that route reach upstream', async () => {
    const seen = await withGateway(
      { readyFile: readyPath('proxied'), sink: probingSink([]) },
      (handle) =>
        Effect.sync(() => upstreamRequests.length).pipe(
          Effect.flatMap((before) =>
            Effect.gen(function* () {
              // Four refusals, one per ordering rule, none of which may open a
              // socket: a bad id, a query on a non-viewer route, a body, and a
              // verb.
              yield* wire(handle.port, { method: 'GET', target: '/v1/games/short/status' });
              yield* wire(handle.port, {
                method: 'GET',
                target: `/v1/games/${GAME_ID}/status?token=private`,
              });
              yield* wire(handle.port, {
                method: 'GET',
                target: `/v1/games/${GAME_ID}/status`,
                headers: [['Content-Length', '13']],
                body: '{"hello":"x"}',
              });
              yield* wire(handle.port, { method: 'POST', target: `/v1/games/${GAME_ID}/status` });
              const quiet = upstreamRequests.length - before;
              // And one that does route, to prove the counter moves at all.
              yield* wire(handle.port, { method: 'GET', target: '/v1/games' });
              return { quiet, noisy: upstreamRequests.length - before };
            }),
          ),
        ),
    );
    expect(seen.quiet).toBe(0);
    expect(seen.noisy).toBe(1);
    expect(upstreamRequests.at(-1)).toBe('/v1/games');
  });
});

// ---------------------------------------------------------------------------
// 6. Pure helpers the socket tests exercise only indirectly
// ---------------------------------------------------------------------------

describe('splitTarget reproduces urlsplit on a request target', () => {
  const CASES: ReadonlyArray<readonly [string, string, string]> = [
    ['/health', '/health', ''],
    ['/health?', '/health', ''],
    ['/health?x=1&y=2', '/health', 'x=1&y=2'],
    ['/health#frag', '/health', ''],
    ['/health#a?b=c', '/health', ''],
    ['/health?a=1#frag', '/health', 'a=1'],
    ['//v1/games', '//v1/games', ''],
    ['/v1/games/x%2Fy', '/v1/games/x%2Fy', ''],
  ];

  CASES.forEach(([target, path, query]) => {
    test(`${target} → path ${path}, query ${JSON.stringify(query)}`, () => {
      expect(splitTarget(target)).toEqual({ path, query });
    });
  });
});

describe('bodySignal is _reject_body over headers', () => {
  const CASES: ReadonlyArray<readonly [RequestBodySignal, Record<string, string>]> = [
    ['absent', {}],
    ['absent', { 'content-length': '0' }],
    ['absent', { 'content-length': ' 0 ' }],
    ['present', { 'content-length': '1' }],
    ['present', { 'content-length': '+3' }],
    ['present', { 'content-length': '5_0' }],
    ['present', { 'content-length': '-1' }],
    ['present', { 'transfer-encoding': 'identity' }],
    ['present', { 'transfer-encoding': 'chunked', 'content-length': '0' }],
    ['invalid-content-length', { 'content-length': 'abc' }],
    ['invalid-content-length', { 'content-length': '' }],
    ['invalid-content-length', { 'content-length': 'abc', 'transfer-encoding': 'chunked' }],
  ];

  CASES.forEach(([signal, headers]) => {
    test(`${signal} ← ${JSON.stringify(headers)}`, () => {
      expect(bodySignal(Headers.fromInput(headers))).toBe(signal);
    });
  });
});

// ---------------------------------------------------------------------------
// 7. main.ts — the one error → exit-code site
// ---------------------------------------------------------------------------

describe('main: three return statements, one Teardown', () => {
  const codeOf = <E, A>(exit: Exit.Exit<E, A>): number => {
    const codes: Array<number> = [];
    gatewayTeardown(exit, (code) => codes.push(code));
    return codes.length === 1 ? (codes[0] ?? -1) : -1;
  };

  test('success exits 0 (`:2209`)', () => {
    expect(codeOf(Exit.succeed(undefined))).toBe(0);
  });

  test('KeyboardInterrupt exits 0 — not 1, not 130 (`:2204`)', () => {
    expect(codeOf(Exit.failCause(Cause.interrupt(FiberId.none)))).toBe(0);
  });

  test('a typed failure exits 2 (`:2207`)', () => {
    expect(codeOf(Exit.fail(new Error('boom')))).toBe(GATEWAY_CLI_ERROR_EXIT_CODE);
  });

  test('a defect exits 2 as well — a refused bind arrives that way', () => {
    expect(codeOf(Exit.die(new Error('address already in use')))).toBe(
      GATEWAY_CLI_ERROR_EXIT_CODE,
    );
  });

  test('a failure that is *also* interrupted still exits 2', () => {
    expect(
      codeOf(
        Exit.failCause(Cause.sequential(Cause.fail(new Error('boom')), Cause.interrupt(FiberId.none))),
      ),
    ).toBe(GATEWAY_CLI_ERROR_EXIT_CODE);
  });
});

describe('main: the single stderr line', () => {
  /** Capture `Console.error` without touching the real stderr. */
  const captured = (cause: Cause.Cause<unknown>): Promise<ReadonlyArray<string>> =>
    Effect.runPromise(
      Effect.gen(function* () {
        const lines = yield* Ref.make<ReadonlyArray<string>>([]);
        const base = yield* Console.consoleWith(Effect.succeed);
        yield* Console.withConsole(reportStartupFailure(cause), {
          ...base,
          error: (...args: ReadonlyArray<unknown>) =>
            Ref.update(lines, (all) => [...all, args.map((arg) => String(arg)).join(' ')]),
        });
        return yield* Ref.get(lines);
      }),
    );

  test('a typed failure is one line, prefixed `error: `', async () => {
    expect(await captured(Cause.fail(new Error('host must be a loopback address')))).toEqual([
      'error: host must be a loopback address',
    ]);
  });

  test('a ValidationError is not reported twice — @effect/cli already printed usage', async () => {
    // The tags are `MissingValue`/`MissingFlag`/`InvalidValue`/…: none of them
    // says "validation", which is why the guard is `@effect/cli`'s own and not
    // a tag-name test. A `bun run main.ts --runs-root x` with the three
    // required flags missing printed `error: [object Object]` under the usage
    // block until it was.
    const missing = ValidationError.missingValue(HelpDoc.p('--service-url'));
    expect(isValidationError(missing)).toBe(true);
    expect(await captured(Cause.fail(missing))).toEqual([]);
    expect(isValidationError(new Error('boom'))).toBe(false);
    expect(isValidationError(null)).toBe(false);
  });

  test('an interrupted program reports nothing: it succeeded', async () => {
    expect(await captured(Cause.interrupt(FiberId.none))).toEqual([]);
    expect(await captured(Cause.empty)).toEqual([]);
  });

  test('a defect is reported in full, because nothing else will', async () => {
    const lines = await captured(Cause.die(new Error('address already in use')));
    expect(lines).toHaveLength(1);
    expect(lines[0]).toContain('address already in use');
    expect(lines[0]?.startsWith('error: ')).toBe(false);
  });

  test("an error's public text is its message, or its own rendering", () => {
    // Tier 1: the verbatim CPython `ValueError` text.
    expect(describeStartupError(new Error('service URL has an invalid port'))).toBe(
      'service URL has an invalid port',
    );
    // Tier 2: a `Data.TaggedError` has no message, so its scalars stand in —
    // `error: ReadyFileLocked` alone does not name the file to go look at.
    const locked = new ReadyFileLocked({ lockPath: '/tmp/r.json.lock', errno: 35 });
    expect(describeStartupError(locked)).toBe(
      'ReadyFileLocked: lockPath=/tmp/r.json.lock errno=35',
    );
    // The private detail is not appended: `cause` is an object, and objects are
    // skipped rather than rendered `[object Object]`.
    expect(
      describeStartupError(
        new ReadyFileIoError({ operation: 'write', path: '/tmp/x', cause: new Error('secret') }),
      ),
    ).toBe('ReadyFileIoError: operation=write path=/tmp/x');
    // Tier 3.
    expect(describeStartupError('plain')).toBe('plain');
    expect(describeStartupError(new Error('')).length).toBeGreaterThan(0);
  });
});

describe('main: telemetry is opt-in', () => {
  test('with no corpus directory configured the backend records nothing', async () => {
    const recorded = await Effect.runPromise(
      Effect.flatMap(makeWideEvent('probe'), sealWideEvent).pipe(
        Effect.flatMap((sealed) =>
          Effect.flatMap(Observability, (backend) => backend.record(sealed)),
        ),
        Effect.provide(telemetryLayer),
        Effect.orDie,
      ),
    );
    expect(Option.isNone(recorded)).toBe(true);
    // And the layer it degrades to is the documented one.
    expect(ObservabilityNoop).toBeDefined();
    expect(process.env[GATEWAY_TELEMETRY_DIR_ENV]).toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// 8. Nothing outlives the suite
// ---------------------------------------------------------------------------

describe('ownership', () => {
  test('every gateway this suite started has released its port', async () => {
    const probes: Array<ReadyProbe> = [];
    const port = await withGateway(
      { readyFile: readyPath('ownership'), sink: probingSink(probes) },
      (handle) => Effect.succeed(handle.port),
    );
    expect(await Effect.runPromise(probeConnect(port))).toBe('refused');
  });
});
