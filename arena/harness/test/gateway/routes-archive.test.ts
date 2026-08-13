/** Archive route fallback, relay, streaming, local-file security, and exact body coverage. */
import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { Gateway } from '@arena/wire';
import { HttpServerResponse } from '@effect/platform';
import { Effect, Either, Exit, Layer, Scope } from 'effect';
import {
  mkdirSync,
  mkdtempSync,
  readFileSync,
  realpathSync,
  rmSync,
  symlinkSync,
  writeFileSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { GATEWAY_GET_METHOD } from 'src/gateway/constants';
import { type GatewayError } from 'src/gateway/errors';
import {
  dispatch,
  type ArchiveBinaryRoute,
  type ArchiveJsonRoute,
  type RouteDecision,
} from 'src/gateway/http/dispatch';
import { respondGateway } from 'src/gateway/http/respond';
import { gatewayErrorFromUpstream } from 'src/gateway/errors';
import { boundedJsonResponse } from 'src/gateway/http/json';
import {
  archiveBinaryRoute,
  archiveJsonRoute,
  archiveRouteOptions,
  type ArchiveRouteOptions,
} from 'src/gateway/http/routes/archive';
import { layer as runsLayer, RunsRepository } from 'src/gateway/services/runs';
import {
  layerLive as upstreamLayer,
  UpstreamBodyError,
  UpstreamClient,
  UpstreamJsonTooLarge,
  UpstreamOffline,
  UpstreamRedirect,
} from 'src/gateway/services/upstream';

const REPO_ROOT = resolve(import.meta.dir, '../../../..');
const FIXTURES = join(REPO_ROOT, 'arena/wire/test/fixtures/runs');

const decoder = new TextDecoder();
const encoder = new TextEncoder();

// ---------------------------------------------------------------------------
// The fixture tree
// ---------------------------------------------------------------------------

/** Terminal, with frames, autosaves and a video: the fully viewable archive. */
const ARCHIVED = 'game_ieTomdES08hpUmFRFzCOAVMo';
/** Terminal, with frames but **no** `saves/`: `_safe_archive_directory` refuses. */
const NO_SAVES = 'game_archiveWithoutSaves1';
/** Terminal, with `saves/` but no `watch_frames/`. */
const NO_FRAMES = 'game_archiveWithoutFrames';
/** Non-terminal: `_terminal_archive` refuses it even with upstream down. */
const LIVE = 'game_liveRunNotTerminal01';
/** No such directory at all. */
const ABSENT = 'game_noSuchRunOnDisk00001';
/** Terminal, with an autosave far larger than the bounded prefix read. */
const BIG_PPM = 'game_archiveWithBigPpm001';

/** The autosave header `_archive_ppm_players` scans (`test_replay_gateway.py:375`). */
const PPM_ONE = [
  'P3',
  '# playerno:0:color:(  0, 103, 165):name:"AgentPlace1"',
  '# playerno:2:color:(255,  20, 147):name:"Blackbeard"',
  '1 1',
  '255',
  '0 0 0',
  '',
].join('\n');

const PPM_TWO = [
  'P3',
  '# playerno:0:color:(  0, 103, 165):name:"AgentPlace1"',
  '# playerno:1:color:( 10,  20,  30):name:"AgentPlace2"',
  '1 1',
  '255',
  '0 0 0',
  '',
].join('\n');

/**
 * A real autosave is megabytes of pixels; the header the scan cares about is
 * the first few logical lines. This fixture puts a player row after a single
 * comment larger than the old 512 KiB byte prefix, proving the reader bounds
 * by Python's 513 complete lines rather than by bytes.
 */
const PPM_BIG = [
  'P3',
  `# ${'padding'.repeat(80_000)}`,
  '# playerno:0:color:(  0, 103, 165):name:"AgentPlace1"',
  '# playerno:2:color:(255,  20, 147):name:"Blackbeard"',
  '4 4',
  '255',
  ...Array.from({ length: 200_000 }, () => '0 0 0'),
  '',
].join('\n');

const FRAME_ZERO = new Uint8Array([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0x30]);
const FRAME_ONE = new Uint8Array([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0x31]);
const FRAME_SEVEN = new Uint8Array([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0x37]);
const VIDEO_BYTES = encoder.encode('archive-video');
const SECRET = '{"owner_token":"must-not-leak"}';

const readFixture = (kind: string, name: string): Record<string, unknown> => {
  const parsed: unknown = JSON.parse(readFileSync(join(FIXTURES, kind, `${name}.json`), 'utf8'));
  if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) {
    throw new Error(`fixture ${kind}/${name}.json is not a JSON object`);
  }
  return { ...parsed };
};

const writeJson = (path: string, value: unknown): void => {
  writeFileSync(path, `${JSON.stringify(value)}\n`, 'utf8');
};

interface RunSpec {
  readonly id: string;
  readonly manifest: string;
  readonly report: string;
  readonly frames?: ReadonlyArray<readonly [string, Uint8Array]>;
  readonly saves?: ReadonlyArray<readonly [string, string]>;
  readonly video?: boolean;
}

const writeRun = (root: string, spec: RunSpec): void => {
  const directory = join(root, spec.id);
  mkdirSync(directory, { recursive: true });
  writeJson(join(directory, 'manifest.json'), {
    ...readFixture('manifest', spec.manifest),
    game_id: spec.id,
  });
  const report = readFixture('report', spec.report);
  const manifest = report['manifest'];
  writeJson(join(directory, 'report.json'), {
    ...report,
    manifest: {
      ...(typeof manifest === 'object' && manifest !== null && !Array.isArray(manifest)
        ? manifest
        : {}),
      game_id: spec.id,
    },
  });
  writeFileSync(join(directory, 'auth.json'), SECRET, 'utf8');
  if (spec.frames !== undefined) {
    mkdirSync(join(directory, 'watch_frames'), { recursive: true });
    spec.frames.forEach(([name, bytes]) =>
      writeFileSync(join(directory, 'watch_frames', name), bytes),
    );
  }
  if (spec.saves !== undefined) {
    mkdirSync(join(directory, 'saves'), { recursive: true });
    spec.saves.forEach(([name, text]) => writeFileSync(join(directory, 'saves', name), text, 'utf8'));
  }
  if (spec.video === true) {
    writeFileSync(join(directory, 'game.mp4'), VIDEO_BYTES);
  }
};

const buildTree = (): string => {
  // `realpath` because macOS's `/var/folders/...` is itself a symlink and the
  // CPython oracle is handed this path directly.
  const root = realpathSync(mkdtempSync(join(tmpdir(), 'arena-archive-')));
  writeRun(root, {
    id: ARCHIVED,
    manifest: 'completed-strategic-v1-multiplayer',
    report: 'completed-two-seats-full-score',
    frames: [
      ['000000.png', FRAME_ZERO],
      ['000001.png', FRAME_ONE],
      ['000007.png', FRAME_SEVEN],
      // Empty: invisible to `_archive_regular_files`, so it is neither listed
      // nor servable, and `latest.png` stays 000007.
      ['000003.png', new Uint8Array(0)],
    ],
    saves: [
      ['turn-0001-M-test.map.ppm', PPM_ONE],
      ['turn-0002-M-test.map.ppm', PPM_TWO],
      // Not an `ARCHIVE_PPM_RE` match: it must not shift the pairing.
      ['turn-0001-auto.sav', 'not a ppm'],
    ],
    video: true,
  });
  // The X10 hazard: a frame that is a link to a secret.
  symlinkSync(
    join(root, ARCHIVED, 'auth.json'),
    join(root, ARCHIVED, 'watch_frames', '000002.png'),
  );
  writeRun(root, {
    id: NO_SAVES,
    manifest: 'completed-strategic-v1-multiplayer',
    report: 'completed-two-seats-full-score',
    frames: [['000000.png', FRAME_ZERO]],
  });
  writeRun(root, {
    id: NO_FRAMES,
    manifest: 'completed-strategic-v1-multiplayer',
    report: 'completed-two-seats-full-score',
    saves: [['turn-0001-M-test.map.ppm', PPM_ONE]],
  });
  writeRun(root, {
    id: BIG_PPM,
    manifest: 'completed-strategic-v1-multiplayer',
    report: 'completed-two-seats-full-score',
    frames: [['000000.png', FRAME_ZERO]],
    saves: [['turn-0009-M-test.map.ppm', PPM_BIG]],
  });
  writeRun(root, {
    id: LIVE,
    manifest: 'running-v2-multiplayer',
    report: 'completed-two-seats-full-score',
    frames: [['000000.png', FRAME_ZERO]],
    saves: [['turn-0001-M-test.map.ppm', PPM_ONE]],
    video: true,
  });
  return root;
};

// ---------------------------------------------------------------------------
// The CPython oracle
// ---------------------------------------------------------------------------

const ORACLE = `
import json, os, sys
sys.path.insert(0, os.environ["ARENA_REPO_ROOT"])
from pathlib import Path
from agent_eval.replay_gateway import (
    GatewayProblem, _archive_frames, _archive_watch, _canonical, _terminal_archive,
)

runs = Path(os.environ["ARENA_RUNS_ROOT"])
base = os.environ["ARENA_BASE"]
absolute = os.environ["ARENA_ABSOLUTE"] == "1"


def view(game_id, kind):
    try:
        archive = _terminal_archive(runs, game_id)
        value = (
            _archive_frames(archive, base) if kind == "frames"
            else _archive_watch(archive, base, absolute_watch=absolute)
        )
    except GatewayProblem as exc:
        return {"status": int(exc.status), "body": str(exc)}
    return {"status": 200, "body": _canonical(value).decode("utf-8")}


sys.stdout.write(json.dumps({
    f"{game_id}:{kind}": view(game_id, kind)
    for game_id, kind in json.loads(os.environ["ARENA_PROBES"])
}))
`;

interface OracleView {
  readonly status: number;
  readonly body: string;
}

type OracleResult = Record<string, OracleView>;

/** One probe's answer, or a loud failure — never `undefined` in an assertion. */
const oracleView = (oracle: OracleResult, gameId: string, kind: string): OracleView => {
  const view = oracle[`${gameId}:${kind}`];
  if (view === undefined) throw new Error(`oracle produced no ${gameId}:${kind}`);
  return view;
};

const runOracle = (
  root: string,
  options: ArchiveRouteOptions,
  probes: ReadonlyArray<readonly [string, 'watch' | 'frames']>,
): OracleResult => {
  const result = Bun.spawnSync(['python3', '-c', ORACLE], {
    stdout: 'pipe',
    stderr: 'pipe',
    env: {
      ...process.env,
      ARENA_REPO_ROOT: REPO_ROOT,
      ARENA_RUNS_ROOT: root,
      ARENA_BASE: options.base,
      ARENA_ABSOLUTE: options.absoluteWatch ? '1' : '0',
      ARENA_PROBES: JSON.stringify(probes),
    },
  });
  if (result.exitCode !== 0) {
    throw new Error(`oracle failed: ${result.stderr.toString()}`);
  }
  // The one assertion in this file: a subprocess's stdout is untyped by
  // construction, and this is the boundary where it becomes typed.
  return JSON.parse(result.stdout.toString()) as OracleResult;
};

// ---------------------------------------------------------------------------
// The stub upstream
// ---------------------------------------------------------------------------

/** What the stub answers next.  Every test sets it explicitly. */
type UpstreamMode =
  | 'binary-stream'
  | 'binary-headers'
  | 'binary-bare'
  | 'json-body'
  | 'not-found'
  | 'method-not-allowed'
  | 'server-error'
  | 'redirect'
  | 'portless';

const STREAM_CHUNK_BYTES = 256 * 1024;
const STREAM_CHUNKS = 40;
const ERROR_CHUNK_BYTES = 8 * 1024;
/** 2 MiB of error body: only a bounded drain keeps this off the heap. */
const ERROR_CHUNKS = 256;

interface Stub {
  readonly url: string;
  readonly setMode: (mode: UpstreamMode) => void;
  readonly targets: () => ReadonlyArray<string>;
  readonly accepts: () => ReadonlyArray<string>;
  readonly produced: () => number;
  readonly cancels: () => number;
  readonly reset: () => void;
  readonly stop: () => void;
}

const startStub = (): Stub => {
  const state = {
    mode: 'not-found' as UpstreamMode,
    produced: 0,
    cancels: 0,
    targets: [] as string[],
    accepts: [] as string[],
  };

  const counted = (chunk: Uint8Array, count: number, delayMs: number): ReadableStream<Uint8Array> => {
    const cursor = { sent: 0 };
    return new ReadableStream<Uint8Array>({
      async pull(controller) {
        if (cursor.sent >= count) {
          controller.close();
          return;
        }
        cursor.sent += 1;
        state.produced += 1;
        controller.enqueue(chunk.slice());
        // Without the delay a loopback socket swallows the whole body into its
        // buffers before the client reads a byte, and the counter stops being
        // an observation of the client's pace.
        if (delayMs > 0) await Bun.sleep(delayMs);
      },
      cancel() {
        state.cancels += 1;
      },
    });
  };

  const frameChunk = new Uint8Array(STREAM_CHUNK_BYTES).fill(0x5a);
  const errorChunk = new Uint8Array(ERROR_CHUNK_BYTES).fill(0x45);
  // Deliberately not canonical: byte parity means these exact bytes.
  const jsonBody = encoder.encode('{"zeta":1,  "alpha":[2,3] ,"png_url":"http://up/x.png"}');

  const answers: { readonly [M in UpstreamMode]: () => Response } = {
    'binary-stream': () =>
      new Response(counted(frameChunk, STREAM_CHUNKS, 3), {
        headers: { 'content-type': 'image/png' },
      }),
    'binary-headers': () =>
      // A fixed body, not a stream: Bun answers a streamed body with chunked
      // framing and drops `Content-Length`, and this case is about relaying the
      // header the upstream actually sent.
      new Response(frameChunk.slice(), {
        headers: {
          'content-type': 'image/png',
          'cache-control': 'public, max-age=2',
          etag: '"frame"',
          'last-modified': 'Wed, 12 Aug 2026 09:25:16 GMT',
          'set-cookie': 'session=secret',
          'x-custom': 'leak-me',
          vary: 'origin',
        },
      }),
    // No Content-Type at all: `_stream_upstream`'s binary default has to fire.
    'binary-bare': () =>
      new Response(new Uint8Array([1, 2, 3, 4, 5]), { headers: { 'content-length': '5' } }),
    'json-body': () =>
      new Response(jsonBody, {
        headers: {
          'content-type': 'application/json',
          etag: '"watch"',
          'cache-control': 'public, max-age=60',
        },
      }),
    'not-found': () => new Response(counted(errorChunk, ERROR_CHUNKS, 1), { status: 404 }),
    'method-not-allowed': () => new Response('{"error":"nope"}', { status: 405 }),
    'server-error': () =>
      new Response(counted(errorChunk, ERROR_CHUNKS, 1), {
        status: 500,
        headers: { 'content-type': 'text/plain' },
      }),
    redirect: () => new Response(null, { status: 302, headers: { location: '/elsewhere' } }),
    // All three portless conditions, with a mixed-case media type and a
    // parameter so the normalization has to happen.
    portless: () =>
      new Response('<html>gone</html>', {
        status: 502,
        headers: { 'content-type': 'text/HTML; charset=utf-8', 'x-portless': '1' },
      }),
  };

  const server = Bun.serve({
    port: 0,
    idleTimeout: 0,
    fetch(request) {
      const url = new URL(request.url);
      state.targets.push(`${url.pathname}${url.search}`);
      state.accepts.push(request.headers.get('accept') ?? '');
      return answers[state.mode]();
    },
  });

  return {
    url: server.url.origin,
    setMode: (mode) => {
      state.mode = mode;
    },
    targets: () => state.targets,
    accepts: () => state.accepts,
    produced: () => state.produced,
    cancels: () => state.cancels,
    reset: () => {
      state.produced = 0;
      state.cancels = 0;
      state.targets.length = 0;
      state.accepts.length = 0;
    },
    stop: () => {
      void server.stop(true);
    },
  };
};

// ---------------------------------------------------------------------------
// The rig
// ---------------------------------------------------------------------------

const fixture: { root: string | null; stub: Stub | null } = { root: null, stub: null };

const runsRoot = (): string => {
  const root = fixture.root;
  if (root === null) throw new Error('runs root not built');
  return root;
};

const stub = (): Stub => {
  const current = fixture.stub;
  if (current === null) throw new Error('stub upstream not started');
  return current;
};

const OPTIONS: ArchiveRouteOptions = archiveRouteOptions('http://127.0.0.1:9', null);
const ABSOLUTE_OPTIONS: ArchiveRouteOptions = archiveRouteOptions(
  'http://127.0.0.1:9',
  'https://freeciv.localhost',
);

/** A port nothing listens on: the transport half of "the upstream is gone". */
const DEAD_UPSTREAM = 'http://127.0.0.1:1';

type RouteEffect = Effect.Effect<
  HttpServerResponse.HttpServerResponse,
  GatewayError,
  RunsRepository | Scope.Scope | UpstreamClient
>;

const isBinaryRoute = (decision: RouteDecision): decision is ArchiveBinaryRoute =>
  decision._tag === 'FramePng' ||
  decision._tag === 'LatestFramePng' ||
  decision._tag === 'VideoMp4';

const isArchiveJsonRoute = (decision: RouteDecision): decision is ArchiveJsonRoute =>
  decision._tag === 'ArchiveStatus' ||
  decision._tag === 'ArchiveResult' ||
  decision._tag === 'ArchiveWatch' ||
  decision._tag === 'ArchiveFrames';

/**
 * `do_GET` for the routes this module owns: dispatch, then the handler, then
 * the one response site.  A dispatch refusal is a `Left` and takes the same
 * rendering path, which is how the routing assertions below get real bodies.
 */
const handle = (target: string, options: ArchiveRouteOptions = OPTIONS): RouteEffect => {
  const split = target.indexOf('?');
  const path = split < 0 ? target : target.slice(0, split);
  const query = split < 0 ? '' : target.slice(split + 1);
  return Either.match(dispatch(GATEWAY_GET_METHOD, path, query, 'absent'), {
    onLeft: (problem): RouteEffect => Effect.fail(problem),
    onRight: (decision) => {
      if (isBinaryRoute(decision)) return archiveBinaryRoute(decision);
      if (isArchiveJsonRoute(decision)) return archiveJsonRoute(decision, options);
      throw new Error(`route ${decision._tag} is not owned by this module`);
    },
  });
};

interface Opened {
  readonly web: Response;
  readonly close: () => Promise<void>;
}

/**
 * Run a route with a scope the caller closes — the shape
 * `@effect/platform` gives a streamed body, whose request scope outlives the
 * handler (`HttpApp.unsafeEjectStreamScope`).  Closing early would cancel the
 * upstream reader and the open file descriptor before a byte was read.
 */
const open = (target: string, serviceUrl: string, options?: ArchiveRouteOptions): Promise<Opened> =>
  Effect.runPromise(Scope.make()).then(async (scope) => {
    const response = await Effect.runPromise(
      Scope.extend(
        Effect.provide(
          respondGateway(handle(target, options)),
          Layer.merge(runsLayer(runsRoot()), upstreamLayer({ serviceUrl })),
        ),
        scope,
      ),
    );
    return {
      web: HttpServerResponse.toWeb(response),
      close: () => Effect.runPromise(Scope.close(scope, Exit.void)),
    };
  });

interface Served {
  readonly status: number;
  readonly headers: Headers;
  readonly bytes: Uint8Array;
  readonly text: string;
}

const serve = async (
  target: string,
  serviceUrl: string = stub().url,
  options?: ArchiveRouteOptions,
): Promise<Served> => {
  const opened = await open(target, serviceUrl, options);
  const bytes = new Uint8Array(await opened.web.arrayBuffer());
  await opened.close();
  return {
    status: opened.web.status,
    headers: opened.web.headers,
    bytes,
    text: decoder.decode(bytes),
  };
};

const problemOf = (name: Gateway.GatewayProblemName): { status: number; body: string } => {
  const message = Gateway.GATEWAY_PROBLEM_MESSAGES[name];
  return {
    status: Gateway.GATEWAY_PROBLEM_STATUS[message],
    body: `{"error":${JSON.stringify(message)}}`,
  };
};

beforeAll(() => {
  fixture.root = buildTree();
  fixture.stub = startStub();
});

afterAll(() => {
  fixture.stub?.stop();
  const root = fixture.root;
  if (root !== null) rmSync(root, { recursive: true, force: true });
});

// ---------------------------------------------------------------------------

describe('the pure pieces', () => {
  test('archiveRouteOptions derives base and absoluteWatch from the one field', () => {
    expect(archiveRouteOptions('http://127.0.0.1:9', null)).toEqual({
      base: 'http://127.0.0.1:9',
      absoluteWatch: false,
    });
    expect(archiveRouteOptions('http://127.0.0.1:9', 'https://viewer')).toEqual({
      base: 'https://viewer',
      absoluteWatch: true,
    });
    // Python's `or` vs `is not None`: the empty string disagrees with itself.
    expect(archiveRouteOptions('http://127.0.0.1:9', '')).toEqual({
      base: 'http://127.0.0.1:9',
      absoluteWatch: true,
    });
  });

  test('every upstream failure maps to a gateway error with the wire message', () => {
    const mapped = [
      new UpstreamOffline({ reason: 'portless', url: 'u', cause: null }),
      new UpstreamOffline({ reason: 'timeout', url: 'u', cause: null }),
      new UpstreamOffline({ reason: 'transport', url: 'u', cause: null }),
      new UpstreamJsonTooLarge({ source: 'body', capBytes: 1, bytesRead: 2, bytesRetained: 1, url: 'u' }),
      new UpstreamRedirect({ status: 302, url: 'u' }),
      new UpstreamRedirect({ status: 503, url: 'u' }),
      new UpstreamBodyError({ reason: 'read', url: 'u', cause: new Error('private path') }),
    ].map(gatewayErrorFromUpstream);

    expect(mapped.map((error) => [error._tag, error.status, error.message])).toEqual([
      ['UpstreamUnavailable', 502, Gateway.GATEWAY_PROBLEM_MESSAGES.upstreamUnavailable],
      ['UpstreamUnavailable', 502, Gateway.GATEWAY_PROBLEM_MESSAGES.upstreamUnavailable],
      ['UpstreamUnavailable', 502, Gateway.GATEWAY_PROBLEM_MESSAGES.upstreamUnavailable],
      ['UpstreamTooLarge', 502, Gateway.GATEWAY_PROBLEM_MESSAGES.upstreamJsonTooLarge],
      ['UpstreamHttpError', 502, Gateway.GATEWAY_PROBLEM_MESSAGES.upstreamRedirect],
      ['UpstreamHttpError', 503, Gateway.upstreamReturnedHttp(503)],
      ['InternalError', 500, Gateway.GATEWAY_PROBLEM_MESSAGES.internalError],
    ]);
    // A timeout is a transport failure, not a third kind of offline.
    expect(mapped[1]?._tag === 'UpstreamUnavailable' ? mapped[1].reason : null).toBe('transport');
  });

  test('boundedJson canonicalizes, then measures, then answers', () => {
    const small = boundedJsonResponse({ b: 1n, a: 'é' });
    expect(Either.isRight(small)).toBe(true);
    if (Either.isRight(small)) {
      const web = HttpServerResponse.toWeb(small.right);
      expect(web.status).toBe(200);
      expect(web.headers.get('content-type')).toBe(Gateway.GATEWAY_PROBLEM_CONTENT_TYPE);
      expect(web.headers.get('cache-control')).toBe(Gateway.GATEWAY_PROBLEM_CACHE_CONTROL);
      // `{"a":"é","b":1}` — sorted keys, `(",", ":")` separators,
      // `ensure_ascii=False`, so the é is two UTF-8 bytes and not `\u00e9`.
      expect(web.headers.get('content-length')).toBe('16');
    }

    // 8 MiB + change: a 503, and a *different* message from the upstream cap.
    const huge = boundedJsonResponse({ blob: 'x'.repeat(8 * 1024 * 1024 + 1) });
    expect(Either.isLeft(huge)).toBe(true);
    if (Either.isLeft(huge)) {
      expect([huge.left._tag, huge.left.status, huge.left.message]).toEqual([
        'ArchiveUnavailable',
        503,
        Gateway.GATEWAY_PROBLEM_MESSAGES.archiveJsonTooLarge,
      ]);
    }

    // A value CPython's `json.dumps` would raise on is a defect, not a problem.
    const unencodable = boundedJsonResponse({ lone: '\ud800' });
    expect(Either.isLeft(unencodable) ? unencodable.left._tag : null).toBe('InternalError');
  });
});

// ---------------------------------------------------------------------------

describe('binary routes: the streaming leg (:1463-1489)', () => {
  test('a 10 MiB frame reaches the client while the upstream is still sending it', async () => {
    stub().reset();
    stub().setMode('binary-stream');

    const opened = await open(`/v1/games/${ARCHIVED}/frames/0.png`, stub().url);
    expect(opened.web.status).toBe(200);
    const body = opened.web.body;
    if (body === null) throw new Error('a proxied 2xx must have a body');
    const reader = body.getReader();

    const first = await reader.read();
    const producedAtFirstChunk = stub().produced();
    expect(first.done).toBe(false);

    const drain = async (total: number): Promise<number> => {
      const next = await reader.read();
      return next.done ? total : drain(total + (next.value?.byteLength ?? 0));
    };
    const total = await drain(first.value?.byteLength ?? 0);
    await opened.close();

    // The whole body arrived...
    expect(total).toBe(STREAM_CHUNK_BYTES * STREAM_CHUNKS);
    // ...but the first chunk was in the client's hands long before the upstream
    // had produced the last one. A buffering proxy would report all 40 here.
    expect(producedAtFirstChunk).toBeLessThanOrEqual(STREAM_CHUNKS / 2);
    expect(producedAtFirstChunk).toBeGreaterThan(0);
    console.log(
      `  [archive] first chunk delivered after ${String(producedAtFirstChunk)}/${String(STREAM_CHUNKS)} upstream chunks`,
    );
  }, 30_000);

  test('the header subset is relayed and everything else is dropped', async () => {
    stub().reset();
    stub().setMode('binary-headers');
    const served = await serve(`/v1/games/${ARCHIVED}/frames/1.png`);

    expect(served.status).toBe(200);
    expect(served.headers.get('content-type')).toBe('image/png');
    expect(served.headers.get('content-length')).toBe(String(STREAM_CHUNK_BYTES));
    expect(served.headers.get('cache-control')).toBe('public, max-age=2');
    expect(served.headers.get('etag')).toBe('"frame"');
    expect(served.headers.get('last-modified')).toBe('Wed, 12 Aug 2026 09:25:16 GMT');
    // The security pair is unconditional, proxied bodies included.
    expect(served.headers.get('x-content-type-options')).toBe('nosniff');
    expect(served.headers.get('referrer-policy')).toBe('no-referrer');
    // Everything outside PROXY_RESPONSE_HEADERS is gone.
    expect(served.headers.get('set-cookie')).toBeNull();
    expect(served.headers.get('x-custom')).toBeNull();
    expect(served.headers.get('vary')).toBeNull();
    // Never CORS, on any route.
    expect(served.headers.get('access-control-allow-origin')).toBeNull();
    // The upstream request is a fixed header set with the binary Accept.
    expect(stub().accepts()).toEqual(['*/*']);
    expect(stub().targets()).toEqual([`/v1/games/${ARCHIVED}/frames/1.png`]);
  });

  test('a proxied 2xx with no upstream Cache-Control carries none at all (trap B6)', async () => {
    stub().reset();
    stub().setMode('binary-bare');
    const served = await serve(`/v1/games/${ARCHIVED}/video.mp4`);

    expect(served.status).toBe(200);
    expect(Array.from(served.bytes)).toEqual([1, 2, 3, 4, 5]);
    // `_send_headers` only defaults to no-store when the mapping is empty *and*
    // the status is >= 400; a proxied 2xx is neither.
    expect(served.headers.get('cache-control')).toBeNull();
    // No upstream Content-Type: `_stream_upstream`'s binary default.
    expect(served.headers.get('content-type')).toBe('application/octet-stream');
    expect(served.headers.get('content-length')).toBe('5');
  });
});

describe('binary routes: the non-2xx arms (trap B5)', () => {
  test('a 500 on a frame is a JSON problem, not an image, and does not reach disk', async () => {
    stub().reset();
    stub().setMode('server-error');
    const served = await serve(`/v1/games/${ARCHIVED}/frames/0.png`);

    expect(served.status).toBe(500);
    expect(served.text).toBe(`{"error":${JSON.stringify(Gateway.upstreamReturnedHttp(500))}}`);
    expect(served.headers.get('content-type')).toBe(Gateway.GATEWAY_PROBLEM_CONTENT_TYPE);
    expect(served.headers.get('cache-control')).toBe(Gateway.GATEWAY_PROBLEM_CACHE_CONTROL);
    // The archived PNG exists and was deliberately not served.
    expect(served.bytes).not.toEqual(FRAME_ZERO);
    // Drained 64 KiB of a 2 MiB error body, and stopped.
    const produced = stub().produced();
    expect(produced).toBeGreaterThanOrEqual(8);
    expect(produced).toBeLessThan(ERROR_CHUNKS / 4);
    console.log(
      `  [archive] drained ${String(produced * ERROR_CHUNK_BYTES)} bytes of a ${String(ERROR_CHUNKS * ERROR_CHUNK_BYTES)}-byte error body`,
    );
  }, 20_000);

  test('a redirect is refused and never followed', async () => {
    stub().reset();
    stub().setMode('redirect');
    const served = await serve(`/v1/games/${ARCHIVED}/frames/latest.png`);

    expect(served.status).toBe(502);
    expect(served.text).toBe(problemOf('upstreamRedirect').body);
    // Exactly one upstream request: the Location was not fetched.
    expect(stub().targets()).toEqual([`/v1/games/${ARCHIVED}/frames/latest.png`]);
  });
});

describe('binary routes: the disk fallback (:1956-1963)', () => {
  test('a 404 serves the archived frame with the immutable cache header, undrained', async () => {
    stub().reset();
    stub().setMode('not-found');
    const served = await serve(`/v1/games/${ARCHIVED}/frames/0.png`);

    expect(served.status).toBe(200);
    expect(Array.from(served.bytes)).toEqual(Array.from(FRAME_ZERO));
    expect(served.headers.get('content-type')).toBe(Gateway.ARCHIVE_FRAME_CONTENT_TYPE);
    expect(served.headers.get('content-length')).toBe(String(FRAME_ZERO.byteLength));
    expect(served.headers.get('cache-control')).toBe(Gateway.ARCHIVE_BINARY_CACHE_CONTROL);
    expect(served.headers.get('x-content-type-options')).toBe('nosniff');
    expect(served.headers.get('referrer-policy')).toBe('no-referrer');
    // The 404 arm closes the socket without draining it (`with response:`).
    expect(stub().produced()).toBeLessThanOrEqual(1);
  });

  test('a 405 falls back too, and latest.png is the highest index', async () => {
    stub().reset();
    stub().setMode('method-not-allowed');
    const served = await serve(`/v1/games/${ARCHIVED}/frames/latest.png`);
    expect(served.status).toBe(200);
    expect(Array.from(served.bytes)).toEqual(Array.from(FRAME_SEVEN));
  });

  test('offline — both the portless probe and a dead socket — serves the archive', async () => {
    stub().reset();
    stub().setMode('portless');
    const probed = await serve(`/v1/games/${ARCHIVED}/video.mp4`);
    expect(probed.status).toBe(200);
    expect(probed.text).toBe('archive-video');
    expect(probed.headers.get('content-type')).toBe(Gateway.ARCHIVE_VIDEO_CONTENT_TYPE);
    expect(probed.headers.get('cache-control')).toBe(Gateway.ARCHIVE_BINARY_CACHE_CONTROL);

    const dead = await serve(`/v1/games/${ARCHIVED}/frames/1.png`, DEAD_UPSTREAM);
    expect(dead.status).toBe(200);
    expect(Array.from(dead.bytes)).toEqual(Array.from(FRAME_ONE));
  });

  test('a frame that is a symlink to a secret is a 404, never the secret', async () => {
    stub().reset();
    stub().setMode('not-found');
    const served = await serve(`/v1/games/${ARCHIVED}/frames/2.png`);
    expect(served.status).toBe(404);
    expect(served.text).toBe(problemOf('mapFrameDoesNotExist').body);
    expect(served.text).not.toContain('must-not-leak');
  });

  test('an empty frame file is invisible, and an unknown index is its own 404', async () => {
    stub().reset();
    stub().setMode('not-found');
    expect((await serve(`/v1/games/${ARCHIVED}/frames/3.png`)).text).toBe(
      problemOf('mapFrameDoesNotExist').body,
    );
    expect((await serve(`/v1/games/${ARCHIVED}/frames/999999.png`)).text).toBe(
      problemOf('mapFrameDoesNotExist').body,
    );
  });

  test('an archive with no frames, no video, or no archive at all names its own 404', async () => {
    stub().reset();
    stub().setMode('not-found');
    const noFrames = await serve(`/v1/games/${NO_FRAMES}/frames/0.png`);
    expect([noFrames.status, noFrames.text]).toEqual([404, problemOf('archiveDataNotFound').body]);

    const noVideo = await serve(`/v1/games/${NO_SAVES}/video.mp4`);
    expect([noVideo.status, noVideo.text]).toEqual([404, problemOf('replayVideoNotFound').body]);

    const live = await serve(`/v1/games/${LIVE}/frames/0.png`);
    expect([live.status, live.text]).toEqual([
      404,
      problemOf('terminalArchiveNotFound').body,
    ]);

    const absent = await serve(`/v1/games/${ABSENT}/frames/0.png`);
    expect([absent.status, absent.text]).toEqual([404, problemOf('gameNotFound').body]);
  });
});

// ---------------------------------------------------------------------------

describe('watch.json and frames: upstream first (:1887-1911)', () => {
  test('a 2xx body is relayed byte for byte under gateway headers', async () => {
    stub().reset();
    stub().setMode('json-body');
    const served = await serve(`/v1/games/${ARCHIVED}/watch.json`);

    expect(served.status).toBe(200);
    // Not canonicalized, not reordered, not rewritten.
    expect(served.text).toBe('{"zeta":1,  "alpha":[2,3] ,"png_url":"http://up/x.png"}');
    expect(served.headers.get('content-type')).toBe(Gateway.GATEWAY_PROBLEM_CONTENT_TYPE);
    // The JSON path forces no-store and drops the upstream's caching headers —
    // they survive only on the binary path.
    expect(served.headers.get('cache-control')).toBe(Gateway.GATEWAY_PROBLEM_CACHE_CONTROL);
    expect(served.headers.get('etag')).toBeNull();
    expect(stub().accepts()).toEqual(['application/json']);
  });

  test('the dispatched path is forwarded verbatim, trailing slash and all (trap B1)', async () => {
    stub().reset();
    stub().setMode('json-body');
    await serve(`/v1/games/${ARCHIVED}/frames`);
    await serve(`//v1/games/${ARCHIVED}/watch.json`);
    expect(stub().targets()).toEqual([
      `/v1/games/${ARCHIVED}/frames`,
      // The stdlib collapses `//` before the gateway sees it.
      `/v1/games/${ARCHIVED}/watch.json`,
    ]);
  });

  test('a 500 is relayed with its own status and never masked by disk data', async () => {
    stub().reset();
    stub().setMode('server-error');
    const served = await serve(`/v1/games/${ARCHIVED}/watch.json`);
    expect(served.status).toBe(500);
    expect(served.text).toBe(`{"error":${JSON.stringify(Gateway.upstreamReturnedHttp(500))}}`);
    expect(served.text).not.toContain('timeline');
  }, 20_000);

  test('a redirect is a 502 on the JSON family too', async () => {
    stub().reset();
    stub().setMode('redirect');
    const served = await serve(`/v1/games/${ARCHIVED}/frames`);
    expect([served.status, served.text]).toEqual([502, problemOf('upstreamRedirect').body]);
  });
});

describe('watch.json and frames: the disk projection, against CPython', () => {
  test('a 404 builds the archive body byte for byte the way the oracle does', async () => {
    stub().reset();
    stub().setMode('not-found');
    const oracle = runOracle(runsRoot(), OPTIONS, [
      [ARCHIVED, 'watch'],
      [ARCHIVED, 'frames'],
    ]);

    const frames = await serve(`/v1/games/${ARCHIVED}/frames`);
    expect(frames.status).toBe(200);
    expect(frames.text).toBe(oracleView(oracle, ARCHIVED, 'frames').body);

    const watch = await serve(`/v1/games/${ARCHIVED}/watch.json`);
    expect(watch.status).toBe(200);
    expect(watch.text).toBe(oracleView(oracle, ARCHIVED, 'watch').body);

    // Not just equal — equal to something that says what it should say.
    expect(watch.text).toContain('"source_name":"turn-0001-M-test.map.ppm"');
    expect(watch.text).toContain('"player_name":"AgentPlace1"');
    // The dynamic faction: a PPM row with no matching place.
    expect(watch.text).toContain('"controller_label":"Freeciv dynamic faction"');
    expect(watch.text).toContain('"seat_id":"dynamic-player-2"');
    // Frame 7 has no autosave at its position: null turn, PNG name as source.
    expect(watch.text).toContain('"source_name":"000007.png"');
    expect(watch.text).toContain('"video":{"available":true');
    expect(watch.text).not.toContain('must-not-leak');
    expect(watch.text).not.toContain(runsRoot());
    // Gateway-built JSON is always no-store, success and failure alike.
    expect(watch.headers.get('cache-control')).toBe(Gateway.GATEWAY_PROBLEM_CACHE_CONTROL);
    expect(watch.headers.get('content-type')).toBe(Gateway.GATEWAY_PROBLEM_CONTENT_TYPE);
  });

  test('a player row after a >512 KiB line is read within the 513-line bound', async () => {
    stub().reset();
    stub().setMode('not-found');
    const oracle = runOracle(runsRoot(), OPTIONS, [[BIG_PPM, 'frames']]);
    const frames = await serve(`/v1/games/${BIG_PPM}/frames`);

    expect(frames.status).toBe(200);
    expect(frames.text).toBe(oracleView(oracle, BIG_PPM, 'frames').body);
    // The header was found and the pixels were not published.
    expect(frames.text).toContain('"player_name":"AgentPlace1"');
    expect(frames.text).toContain('"turn":9');
    expect(frames.bytes.byteLength).toBeLessThan(2000);
  });

  test('the viewer origin moves every URL, and the oracle agrees', async () => {
    stub().reset();
    stub().setMode('not-found');
    const oracle = runOracle(runsRoot(), ABSOLUTE_OPTIONS, [[ARCHIVED, 'watch']]);
    const watch = await serve(`/v1/games/${ARCHIVED}/watch.json`, stub().url, ABSOLUTE_OPTIONS);
    expect(watch.text).toBe(oracleView(oracle, ARCHIVED, 'watch').body);
    expect(watch.text).toContain('"watch_url":"https://freeciv.localhost/watch/');
  });

  test('offline is the identical arm, and a live run is not exposed as an archive', async () => {
    stub().reset();
    stub().setMode('portless');
    const oracle = runOracle(runsRoot(), OPTIONS, [
      [ARCHIVED, 'watch'],
      [LIVE, 'watch'],
      [NO_SAVES, 'frames'],
    ]);

    const offline = await serve(`/v1/games/${ARCHIVED}/watch.json`);
    expect(offline.text).toBe(oracleView(oracle, ARCHIVED, 'watch').body);

    const dead = await serve(`/v1/games/${ARCHIVED}/watch.json`, DEAD_UPSTREAM);
    expect(dead.text).toBe(offline.text);

    // A non-terminal run with upstream down: 404, not a partial status.
    const live = await serve(`/v1/games/${LIVE}/watch.json`);
    expect([live.status, live.text]).toEqual([404, problemOf('terminalArchiveNotFound').body]);
    expect(oracleView(oracle, LIVE, 'watch').status).toBe(404);

    // Frames but no `saves/`: `_safe_archive_directory` refuses both.
    const noSaves = await serve(`/v1/games/${NO_SAVES}/frames`);
    expect([noSaves.status, noSaves.text]).toEqual([404, problemOf('archiveDataNotFound').body]);
    expect(oracleView(oracle, NO_SAVES, 'frames').status).toBe(404);
  });
});

// ---------------------------------------------------------------------------

describe('what never reaches this module', () => {
  test('a leading-zero frame name is a 404 that opens no socket', async () => {
    stub().reset();
    stub().setMode('binary-headers');
    const padded = await serve(`/v1/games/${ARCHIVED}/frames/007.png`);
    const doubled = await serve(`/v1/games/${ARCHIVED}/frames/00.png`);
    const negative = await serve(`/v1/games/${ARCHIVED}/frames/-1.png`);

    expect([padded.status, doubled.status, negative.status]).toEqual([404, 404, 404]);
    expect(padded.text).toBe(problemOf('notFound').body);
    expect(stub().targets()).toEqual([]);
    // `0.png` is the one unpadded spelling that does route.
    stub().setMode('not-found');
    expect((await serve(`/v1/games/${ARCHIVED}/frames/0.png`)).status).toBe(200);
    expect(stub().targets()).toEqual([`/v1/games/${ARCHIVED}/frames/0.png`]);
  });

  test('a query on an archive route is a 400 about query parameters, before any I/O', async () => {
    stub().reset();
    stub().setMode('binary-headers');
    const query = problemOf('viewerRouteQuery');
    const targets = [
      `/v1/games/${ARCHIVED}/watch.json?token=private`,
      `/v1/games/${ARCHIVED}/frames?x=1`,
      `/v1/games/${ARCHIVED}/frames/0.png?x=1`,
      `/v1/games/${ARCHIVED}/video.mp4?x=1`,
      // The gate precedes the final 404, so a route that does not exist gets
      // the query message anyway (trap B2).
      `/v1/games/${ARCHIVED}/nonsense?x=1`,
    ];
    const served = await Promise.all(targets.map((target) => serve(target)));
    expect(served.map((response) => [response.status, response.text])).toEqual(
      targets.map(() => [400, query.body]),
    );
    expect(served[0]?.headers.get('cache-control')).toBe(Gateway.GATEWAY_PROBLEM_CACHE_CONTROL);
    expect(stub().targets()).toEqual([]);

    // A bare `?` is not a query at all.
    stub().setMode('not-found');
    expect((await serve(`/v1/games/${ARCHIVED}/frames/0.png?`)).status).toBe(200);
  });

  test('a bad game id is a 404 before the query check and before any I/O', async () => {
    stub().reset();
    stub().setMode('binary-headers');
    const short = await serve('/v1/games/short/watch.json?x=1');
    expect([short.status, short.text]).toEqual([404, problemOf('notFound').body]);
    // `%2F` is never decoded, so it cannot become a path separator.
    const encoded = await serve(`/v1/games/${ARCHIVED}%2Fstatus/frames/0.png`);
    expect(encoded.status).toBe(404);
    expect(stub().targets()).toEqual([]);
  });
});
