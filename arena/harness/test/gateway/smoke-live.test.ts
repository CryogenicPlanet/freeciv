/**
 * Live smoke parity — the two gateways as **real processes**, side by side.
 *
 * Every other file under `test/gateway/` exercises the port's modules in
 * process: `dispatch` as a function, a route as an `Effect`, `_archive_watch`
 * against a CPython oracle invoked with `python3 -c`.  That is the right way
 * to pin a projection, and it is structurally unable to catch the class of bug
 * this file exists for — the ones that only exist once there is a socket, an
 * argv, a ready file and an operating system in the way:
 *
 * - a flag the TypeScript CLI spells differently, so the two servers are not
 *   actually configured the same way;
 * - a ready file that appears before the socket answers, so a supervisor races
 *   the first request;
 * - a request Bun's HTTP parser rejects *before* the handler runs, where the
 *   port has no opportunity to produce Python's body at all;
 * - a body that differs by four bytes because a number crossed a process
 *   boundary and came back a float.
 *
 * All four are below, and three of them were found here.
 *
 * ## The rig
 *
 * One fixture `runs_root` built from `arena/wire/test/fixtures/runs`, shared by
 * both servers — the disk is not a variable.  Two phases, each spawning a
 * *fresh pair* of processes with the same argv modulo `--cache-root` and
 * `--ready-file` (which must differ: a shared cache would let one process's
 * derivation answer for the other, and a shared ready file is refused by the
 * `flock` the ready-file service takes):
 *
 * | Phase | `--service-url` | What it proves |
 * |---|---|---|
 * | `offline` | `http://127.0.0.1:1` — a port nothing listens on | the whole disk-fallback matrix, end to end |
 * | `upstream` | a recording `Bun.serve` stub | relay, forwarded targets, the failure branches |
 *
 * Requests go over a **raw socket**, not `fetch`: parity needs request targets
 * `fetch` normalizes away (`//v1/games`, `…/status/`), methods it refuses to
 * pair with a body (`GET` with `Content-Length`), and verbs it will not send
 * (`TRACE`).  The client sends `Connection: close` so both HTTP/1.0 Python and
 * HTTP/1.1 Bun end the response the same way — at EOF.
 *
 * ## What is compared, and what is deliberately not
 *
 * Compared: **status**, **body bytes**, and the header subset
 * {@link COMPARED_HEADERS}.  Excluded, per the behavior dossier §15: `Server`
 * and `Date` (stdlib vs Bun), `Connection` (HTTP/1.0 has no keep-alive),
 * header *order* and header *case* (both are insignificant in HTTP, and Python
 * emits `Allow` where Bun emits `allow`).
 *
 * Two response fields cannot be equal by construction, because the two
 * processes are two processes: every URL a body contains carries the gateway's
 * own port, and `/health` additionally reports `identity`, `pid`, `port`,
 * `cache_root` and `upstream_service_url`.  {@link normalize} rewrites the
 * origins; `/health` is compared field-wise with exactly that set excluded and
 * *asserted* to be the only difference, which is a stronger claim than
 * skipping the route.
 *
 * ## Processes
 *
 * `beforeAll` spawns, probes and **stops** both pairs; `afterAll` is only a
 * safety net and a `rm -rf`.  Doing the teardown inside `beforeAll` is what
 * lets {@link ORPHAN_CHECK} be a real test: by the time it runs, every pid this
 * file created has already been waited on, so `ps -p` is asked a question with
 * a right answer.  The user's running stack is never contacted — the only
 * upstreams are a dead port and a stub this file owns.
 */
import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { Gateway } from '@arena/wire';
import {
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  realpathSync,
  rmSync,
  writeFileSync,
} from 'node:fs';
import { connect } from 'node:net';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';

/**
 * `flock` through `bun:ffi` against `libSystem.B.dylib`, and the `python3` that
 * owns the oracle, are both darwin facts here.  Everything else in this file is
 * portable; the tag is on the whole suite because a partial run would report a
 * parity result the platform cannot support.
 */
const DARWIN = process.platform === 'darwin';

const REPO_ROOT = resolve(import.meta.dir, '../../../..');
const HARNESS_ROOT = resolve(import.meta.dir, '../..');
const FIXTURES = join(REPO_ROOT, 'arena/wire/test/fixtures/runs');

const encoder = new TextEncoder();

// ---------------------------------------------------------------------------
// The fixture runs_root
// ---------------------------------------------------------------------------

/** Terminal, with frames, an autosave and a video: the fully viewable archive. */
const ARCHIVED = 'game_ieTomdES08hpUmFRFzCOAVMo';
/** Non-terminal, with a replay tail: the `interrupted` relabeling in the index. */
const LIVE = 'game_liveRunNotTerminal01';
/** No directory at all — the routes must reach upstream and then 404. */
const ABSENT = 'game_noSuchRunOnDisk00001';
/** Well-formed id the stub answers 500 for. */
const UPSTREAM_500 = 'game_upstream500relay0001';
/** Well-formed id the stub answers 302 for. */
const UPSTREAM_REDIRECT = 'game_upstreamRedirect00001';
/** Well-formed id the stub answers with the portless-offline signature. */
const UPSTREAM_PORTLESS = 'game_upstreamPortless00001';

/** The autosave header `_archive_ppm_players` scans. */
const PPM = [
  'P3',
  '# playerno:0:color:(  0, 103, 165):name:"AgentPlace1"',
  '# playerno:2:color:(255,  20, 147):name:"Blackbeard"',
  '1 1',
  '255',
  '0 0 0',
  '',
].join('\n');

const FRAME_ZERO = new Uint8Array([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0x30]);
const VIDEO = encoder.encode('archive-video');
const SECRET = '{"owner_token":"must-not-leak"}';

const readFixture = (kind: string, name: string): Record<string, unknown> => {
  const parsed: unknown = JSON.parse(readFileSync(join(FIXTURES, kind, `${name}.json`), 'utf8'));
  if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) {
    throw new Error(`fixture ${kind}/${name}.json is not a JSON object`);
  }
  return { ...parsed };
};

const writeJson = (path: string, value: unknown): void =>
  writeFileSync(path, `${JSON.stringify(value)}\n`, 'utf8');

interface RunSpec {
  readonly id: string;
  readonly manifest: string;
  readonly frames?: boolean;
  readonly saves?: boolean;
  readonly video?: boolean;
  /** `replay.jsonl` contents; its tail is what `_as_interrupted` recovers. */
  readonly replay?: string;
}

const writeRun = (root: string, spec: RunSpec): void => {
  const directory = join(root, spec.id);
  mkdirSync(directory, { recursive: true });
  writeJson(join(directory, 'manifest.json'), {
    ...readFixture('manifest', spec.manifest),
    game_id: spec.id,
  });
  const report = readFixture('report', 'completed-two-seats-full-score');
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
  // A secret in the run directory: nothing this file requests may echo it.
  writeFileSync(join(directory, 'auth.json'), SECRET, 'utf8');
  if (spec.frames === true) {
    mkdirSync(join(directory, 'watch_frames'), { recursive: true });
    writeFileSync(join(directory, 'watch_frames', '000000.png'), FRAME_ZERO);
  }
  if (spec.saves === true) {
    mkdirSync(join(directory, 'saves'), { recursive: true });
    writeFileSync(join(directory, 'saves', 'turn-0001-M-test.map.ppm'), PPM, 'utf8');
  }
  if (spec.video === true) writeFileSync(join(directory, 'game.mp4'), VIDEO);
  if (spec.replay !== undefined) {
    writeFileSync(join(directory, 'replay.jsonl'), spec.replay, 'utf8');
  }
};

/**
 * `realpath` because macOS's `/var/folders/…` is itself a symlink and both
 * servers resolve `--runs-root`; an unresolved path would make `/health`
 * disagree with itself for reasons that have nothing to do with the port.
 */
const buildRunsRoot = (): string => {
  const root = realpathSync(mkdtempSync(join(tmpdir(), 'arena-smoke-runs-')));
  writeRun(root, {
    id: ARCHIVED,
    manifest: 'completed-strategic-v1-multiplayer',
    frames: true,
    saves: true,
    video: true,
  });
  writeRun(root, {
    id: LIVE,
    manifest: 'running-v2-multiplayer',
    frames: true,
    saves: true,
    // The last line is torn, exactly as `test_games_offline_fallback…` writes
    // it: the backwards scan must walk past it to turn 44.
    replay: '{"schema_version":1,"turn":41}\n{"schema_version":1,"turn":44}\n{"schema_version":1,"tu',
  });
  return root;
};

// ---------------------------------------------------------------------------
// The raw HTTP client
// ---------------------------------------------------------------------------

interface RawResponse {
  readonly status: number;
  readonly reason: string;
  readonly headers: ReadonlyMap<string, string>;
  readonly body: Buffer;
}

const CRLF2 = Buffer.from('\r\n\r\n');

const dechunk = (input: Buffer): Buffer => {
  const parts: Buffer[] = [];
  const walk = (offset: number): void => {
    const end = input.indexOf('\r\n', offset);
    if (end < 0) return;
    const size = Number.parseInt(input.toString('latin1', offset, end), 16);
    if (!Number.isFinite(size) || size <= 0) return;
    parts.push(input.subarray(end + 2, end + 2 + size));
    walk(end + 2 + size + 2);
  };
  walk(0);
  return Buffer.concat(parts);
};

const parseResponse = (all: Buffer): RawResponse => {
  const split = all.indexOf(CRLF2);
  if (split < 0) throw new Error(`no header terminator in ${JSON.stringify(all.toString('latin1'))}`);
  const [statusLine, ...headerLines] = all.toString('latin1', 0, split).split('\r\n');
  const match = /^HTTP\/[0-9.]+ (\d{3}) ?(.*)$/.exec(statusLine ?? '');
  if (match === null) throw new Error(`bad status line: ${String(statusLine)}`);
  const headers = new Map(
    headerLines.flatMap((line): ReadonlyArray<readonly [string, string]> => {
      const at = line.indexOf(':');
      return at < 0 ? [] : [[line.slice(0, at).toLowerCase().trim(), line.slice(at + 1).trim()]];
    }),
  );
  const rest = all.subarray(split + CRLF2.length);
  return {
    status: Number(match[1]),
    reason: match[2] ?? '',
    headers,
    body: headers.get('transfer-encoding') === 'chunked' ? dechunk(rest) : rest,
  };
};

interface RawRequest {
  readonly method: string;
  /** The literal request target — never normalized, never re-encoded. */
  readonly target: string;
  readonly headers?: ReadonlyArray<readonly [string, string]>;
  readonly body?: Uint8Array;
  /** Send this exact request line instead of `{method} {target} HTTP/1.1`. */
  readonly rawRequestLine?: string;
}

const REQUEST_TIMEOUT_MS = 30_000;

/**
 * One request, one connection, read to EOF.
 *
 * `ECONNRESET` is **not** an error here whenever bytes have already arrived:
 * a server that answers a malformed request and then resets the connection —
 * which is what Bun does for a `Content-Length` its parser refuses, and what
 * CPython does after `send_error` with `Connection: close` — delivers a
 * complete response followed by an RST.  Rejecting on the RST would throw away
 * the very response the divergence tests are about.
 */
const request = (origin: string, spec: RawRequest): Promise<RawResponse> => {
  const url = new URL(origin);
  return new Promise((settle, reject) => {
    const chunks: Buffer[] = [];
    const finish = (): void => {
      if (chunks.length === 0) {
        reject(new Error(`empty response: ${spec.method} ${spec.target}`));
        return;
      }
      settle(parseResponse(Buffer.concat(chunks)));
    };
    const socket = connect({ host: url.hostname, port: Number(url.port) }, () => {
      socket.write(
        [
          spec.rawRequestLine ?? `${spec.method} ${spec.target} HTTP/1.1`,
          `Host: ${url.hostname}:${url.port}`,
          'Connection: close',
          ...(spec.headers ?? []).map(([name, value]) => `${name}: ${value}`),
          '',
          '',
        ].join('\r\n'),
      );
      if (spec.body !== undefined) socket.write(spec.body);
    });
    socket.on('data', (chunk: Buffer) => chunks.push(chunk));
    socket.on('error', (cause: Error) => {
      if (chunks.length === 0) reject(new Error(`${spec.method} ${spec.target}: ${cause.message}`));
    });
    socket.setTimeout(REQUEST_TIMEOUT_MS, () => {
      socket.destroy();
      reject(new Error(`timeout: ${spec.method} ${spec.target}`));
    });
    socket.on('close', finish);
  });
};

// ---------------------------------------------------------------------------
// The recording stub upstream
// ---------------------------------------------------------------------------

interface Stub {
  readonly origin: string;
  /** Every forwarded `pathname + search`, in arrival order. */
  readonly targets: () => ReadonlyArray<string>;
  /** Every forwarded `Accept`, in arrival order. */
  readonly accepts: () => ReadonlyArray<string>;
  /** Header names the *gateway* sent upstream, lower-cased, for the last call. */
  readonly lastHeaders: () => ReadonlyArray<string>;
  readonly stop: () => Promise<void>;
}

const startStub = (): Stub => {
  const seen = { targets: [] as string[], accepts: [] as string[], headers: [] as string[] };
  const json = (body: string, status = 200, extra: Record<string, string> = {}): Response =>
    new Response(body, { status, headers: { 'content-type': 'application/json', ...extra } });

  const server = Bun.serve({
    port: 0,
    idleTimeout: 0,
    fetch: (incoming) => {
      const url = new URL(incoming.url);
      seen.targets.push(`${url.pathname}${url.search}`);
      seen.accepts.push(incoming.headers.get('accept') ?? '');
      seen.headers = Array.from(incoming.headers.keys(), (name) => name.toLowerCase()).toSorted();
      if (url.pathname === '/v1/games') {
        return json('{"schema_version":1,"games":[{"game_id":"game_upstreamLiveRow000001","state":"running"}]}');
      }
      if (url.pathname.includes(UPSTREAM_500)) {
        return json('{"upstream":"boom"}', 500);
      }
      if (url.pathname.includes(UPSTREAM_REDIRECT)) {
        return new Response(null, { status: 302, headers: { location: '/elsewhere' } });
      }
      if (url.pathname.includes(UPSTREAM_PORTLESS)) {
        // All three conditions of the portless probe, with a mixed-case media
        // type and a parameter so the normalization has to happen.
        return new Response('<html>gone</html>', {
          status: 502,
          headers: { 'content-type': 'text/HTML; charset=utf-8', 'x-portless': '1' },
        });
      }
      if (url.pathname.endsWith('/watch.json')) {
        // Deliberately not canonical: byte relay means these exact bytes.
        return json('{"zeta":1,  "alpha":[2,3] ,"n":1.5}', 200, {
          etag: '"watch"',
          'cache-control': 'public, max-age=60',
          'set-cookie': 'session=secret',
          'x-custom': 'leak-me',
        });
      }
      if (url.pathname.endsWith('/status')) return json('{"upstream":"status"}');
      if (url.pathname.endsWith('.png')) {
        return new Response(new Uint8Array([1, 2, 3, 4, 5]), {
          headers: {
            'content-type': 'image/png',
            'content-length': '5',
            etag: '"frame"',
            'set-cookie': 'session=secret',
          },
        });
      }
      return json('{"error":"nope"}', 404);
    },
  });

  return {
    origin: server.url.origin,
    targets: () => seen.targets,
    accepts: () => seen.accepts,
    lastHeaders: () => seen.headers,
    stop: () => server.stop(true),
  };
};

// ---------------------------------------------------------------------------
// Spawning a gateway
// ---------------------------------------------------------------------------

interface Gw {
  readonly kind: 'python' | 'typescript';
  readonly process: Bun.Subprocess;
  readonly pid: number;
  readonly url: string;
  readonly readyFile: string;
  readonly readyRecord: Readonly<Record<string, unknown>>;
  /** Everything the process printed on stdout, read after it exits. */
  readonly stdout: () => Promise<string>;
  readonly stderr: () => Promise<string>;
}

const READY_TIMEOUT_MS = 40_000;

const readReady = (path: string): Readonly<Record<string, unknown>> | null => {
  if (!existsSync(path)) return null;
  const text = readFileSync(path, 'utf8');
  if (text.trim() === '') return null;
  const parsed: unknown = JSON.parse(text);
  return typeof parsed === 'object' && parsed !== null && !Array.isArray(parsed)
    ? (parsed as Record<string, unknown>)
    : null;
};

const awaitReady = async (
  child: Bun.Subprocess,
  path: string,
): Promise<Readonly<Record<string, unknown>>> => {
  const deadline = Date.now() + READY_TIMEOUT_MS;
  const poll = async (): Promise<Readonly<Record<string, unknown>>> => {
    const record = readReady(path);
    if (record !== null && typeof record['url'] === 'string') return record;
    if (child.exitCode !== null) {
      throw new Error(`gateway exited ${String(child.exitCode)} before publishing ${path}`);
    }
    if (Date.now() > deadline) throw new Error(`ready file never appeared: ${path}`);
    await Bun.sleep(40);
    return poll();
  };
  return poll();
};

interface SpawnSpec {
  readonly runsRoot: string;
  readonly serviceUrl: string;
  readonly scratch: string;
  readonly slot: string;
}

/**
 * The eight flags `local_stack.py:540-548` passes, spelled identically for both
 * implementations.  `--cache-root` and `--ready-file` are the only two that
 * differ, and both *must*: the derivation cache is keyed by game and turn, so a
 * shared one would let whichever process ran first answer for the other, and
 * the ready file is held under an exclusive `flock` for the process lifetime.
 */
const flagsFor = (spec: SpawnSpec, slot: string): ReadonlyArray<string> => [
  '--host', '127.0.0.1',
  '--port', '0',
  '--service-url', spec.serviceUrl,
  '--runs-root', spec.runsRoot,
  '--cache-root', join(spec.scratch, `cache-${slot}`),
  '--repo-root', REPO_ROOT,
  '--ready-file', join(spec.scratch, `${slot}.ready.json`),
];

/**
 * Every child this file has started, registered **before** it is waited on.
 *
 * The first version of this rig registered a process only once it had published
 * a ready file and answered every leg, which meant a single throwing leg left
 * two gateways running past the end of the suite — observed, with `ps`.  The
 * registry is written at `Bun.spawn` and read by `afterAll`, so there is no
 * window in which a live process is unknown to the teardown.
 */
const spawned: Bun.Subprocess[] = [];

const spawnGateway = async (
  kind: Gw['kind'],
  spec: SpawnSpec,
): Promise<Gw> => {
  const slot = `${spec.slot}-${kind === 'python' ? 'py' : 'ts'}`;
  const readyFile = join(spec.scratch, `${slot}.ready.json`);
  const argv =
    kind === 'python'
      ? ['python3', '-m', 'agent_eval.replay_gateway', ...flagsFor(spec, slot)]
      : ['bun', 'src/gateway/main.ts', ...flagsFor(spec, slot)];
  const child = Bun.spawn(argv, {
    cwd: kind === 'python' ? REPO_ROOT : HARNESS_ROOT,
    stdout: 'pipe',
    stderr: 'pipe',
    // The gateways must not inherit this test's telemetry configuration: the
    // Python has none, and a corpus directory appearing beside one of them is
    // a filesystem difference the parity claim does not want.
    env: { ...process.env, ARENA_GATEWAY_TELEMETRY_DIR: undefined },
  });
  spawned.push(child);
  const record = await awaitReady(child, readyFile);
  const url = record['url'];
  if (typeof url !== 'string') throw new Error(`ready record has no url: ${readyFile}`);
  return {
    kind,
    process: child,
    pid: child.pid,
    url,
    readyFile,
    readyRecord: record,
    // `Bun.Subprocess`'s streams are typed as a union over every `stdio`
    // option; `'pipe'` is the one asked for above, and it is a stream.
    stdout: () => new Response(child.stdout as ReadableStream<Uint8Array>).text(),
    stderr: () => new Response(child.stderr as ReadableStream<Uint8Array>).text(),
  };
};

// ---------------------------------------------------------------------------
// The legs
// ---------------------------------------------------------------------------

interface Leg extends RawRequest {
  readonly name: string;
  /** Skip in the `offline` phase (needs a live upstream to mean anything). */
  readonly upstreamOnly?: boolean;
}

const get = (name: string, target: string): Leg => ({ name, method: 'GET', target });

const LEGS: ReadonlyArray<Leg> = [
  // --- served locally, never proxied -------------------------------------
  get('health', '/health'),
  get('health-fragment', '/health#frag'),
  get('health-query-400', '/health?x=1'),
  // --- the index, all four branches --------------------------------------
  get('games-index', '/v1/games'),
  get('games-index-leading-slashes', '//v1/games'),
  get('games-index-query-400', '/v1/games?x=1'),
  get('games-index-trailing-slash-404', '/v1/games/'),
  // --- the archive JSON family -------------------------------------------
  get('status', `/v1/games/${ARCHIVED}/status`),
  get('bare-id-alias', `/v1/games/${ARCHIVED}`),
  get('status-trailing-slash', `/v1/games/${ARCHIVED}/status/`),
  get('status-leading-slashes', `//v1/games/${ARCHIVED}/status`),
  get('result', `/v1/games/${ARCHIVED}/result`),
  get('watch-json', `/v1/games/${ARCHIVED}/watch.json`),
  get('frames-index', `/v1/games/${ARCHIVED}/frames`),
  // --- binaries -----------------------------------------------------------
  get('frame-0-png', `/v1/games/${ARCHIVED}/frames/0.png`),
  get('frame-latest-png', `/v1/games/${ARCHIVED}/frames/latest.png`),
  get('frame-007-png-404', `/v1/games/${ARCHIVED}/frames/007.png`),
  get('video-mp4', `/v1/games/${ARCHIVED}/video.mp4`),
  // --- the three derivations ---------------------------------------------
  get('replay-limit5-cold', `/v1/games/${ARCHIVED}/replay.json?limit=5`),
  get('replay-limit5-warm', `/v1/games/${ARCHIVED}/replay.json?limit=5`),
  get('replay-python-int-literal', `/v1/games/${ARCHIVED}/replay.json?limit=5_0&after_turn=+1`),
  get('replay-limit-251-400', `/v1/games/${ARCHIVED}/replay.json?limit=251`),
  get('board-turn1', `/v1/games/${ARCHIVED}/board.json?turn=1`),
  get('board-missing-turn-400', `/v1/games/${ARCHIVED}/board.json`),
  get('events', `/v1/games/${ARCHIVED}/events.json`),
  get('events-query-400', `/v1/games/${ARCHIVED}/events.json?turn=1`),
  // --- dispatch refusals --------------------------------------------------
  get('absent-id', `/v1/games/${ABSENT}/status`),
  get('live-run-status', `/v1/games/${LIVE}/status`),
  get('short-id-404', '/v1/games/short/status'),
  get('short-id-query-404', '/v1/games/short/status?x=1'),
  get('nonsense-suffix-query-400', `/v1/games/${ARCHIVED}/nonsense?x=1`),
  get('percent-encoded-separator-404', `/v1/games/${ARCHIVED}%2Fstatus`),
  get('private-route-404', `/v1/games/${ARCHIVED}/join`),
  // --- methods ------------------------------------------------------------
  { name: 'post-405', method: 'POST', target: `/v1/games/${ARCHIVED}/status` },
  { name: 'put-405', method: 'PUT', target: '/health' },
  { name: 'options-405', method: 'OPTIONS', target: '/health' },
  { name: 'head-405', method: 'HEAD', target: '/health' },
  { name: 'trace-501', method: 'TRACE', target: '/health' },
  // --- request bodies -----------------------------------------------------
  {
    name: 'get-with-body-400',
    method: 'GET',
    target: `/v1/games/${ARCHIVED}/status`,
    headers: [['Content-Length', '12']],
    body: encoder.encode('private body'),
  },
  { name: 'get-content-length-zero', method: 'GET', target: '/health', headers: [['Content-Length', '0']] },
  { name: 'get-bad-content-length', method: 'GET', target: '/health', headers: [['Content-Length', 'abc']] },
  { name: 'get-transfer-encoding', method: 'GET', target: '/health', headers: [['Transfer-Encoding', 'identity']] },
  // --- credentials are never forwarded ------------------------------------
  {
    name: 'credentials-not-forwarded',
    method: 'GET',
    target: `/v1/games/${ARCHIVED}/watch.json`,
    headers: [
      ['Authorization', 'Bearer secret'],
      ['Cookie', 'session=secret'],
      ['X-Forwarded-Host', 'evil.invalid'],
      ['Accept-Encoding', 'gzip'],
      ['Range', 'bytes=0-1'],
    ],
  },
  // --- upstream failure branches -----------------------------------------
  { ...get('upstream-500-relay', `/v1/games/${UPSTREAM_500}/status`), upstreamOnly: true },
  { ...get('upstream-500-on-png', `/v1/games/${UPSTREAM_500}/frames/0.png`), upstreamOnly: true },
  { ...get('upstream-redirect-502', `/v1/games/${UPSTREAM_REDIRECT}/status`), upstreamOnly: true },
  { ...get('upstream-portless-offline', `/v1/games/${UPSTREAM_PORTLESS}/status`), upstreamOnly: true },
  // --- the pre-router, stdlib-owned rejections ----------------------------
  {
    name: 'space-in-request-target',
    method: 'GET',
    target: '/v1/games?a= b',
    rawRequestLine: 'GET /v1/games?a= b HTTP/1.1',
  },
  {
    name: 'absolute-form-target',
    method: 'GET',
    target: 'http://evil.invalid/health',
  },
];

const legByName = (name: string): Leg => {
  const leg = LEGS.find((candidate) => candidate.name === name);
  if (leg === undefined) throw new Error(`no leg named ${name}`);
  return leg;
};

// ---------------------------------------------------------------------------
// Comparison
// ---------------------------------------------------------------------------

/** The header subset parity is asserted on. */
const COMPARED_HEADERS = [
  'content-type',
  'cache-control',
  'allow',
  'x-content-type-options',
  'referrer-policy',
] as const;

/**
 * The `/health` fields that are properties of *this process* rather than of the
 * port.  Asserted to be exactly the set that differs, so a sixth field drifting
 * is a failure and not a silent exemption.
 */
const PROCESS_SCOPED_HEALTH_FIELDS = ['cache_root', 'identity', 'pid', 'port', 'url'] as const;

const ORIGIN_PLACEHOLDER = 'http://gateway.invalid';

/** A `/health` body as a field map — the one place a body is read structurally. */
const healthFields = (body: Buffer): Record<string, unknown> =>
  JSON.parse(body.toString('utf8')) as Record<string, unknown>;

/** Rewrite a gateway's own origin out of a body so the two are comparable. */
const normalize = (body: Buffer, origin: string): string =>
  body.toString('latin1').split(origin).join(ORIGIN_PLACEHOLDER);

interface Compared {
  readonly leg: string;
  readonly py: RawResponse;
  readonly ts: RawResponse;
  readonly statusEqual: boolean;
  readonly bodyEqual: boolean;
  readonly headersEqual: boolean;
  /** Forwarded upstream targets, per side, for the requests this leg caused. */
  readonly pyTargets: ReadonlyArray<string>;
  readonly tsTargets: ReadonlyArray<string>;
}

const headerSubset = (response: RawResponse): ReadonlyArray<readonly [string, string | null]> =>
  COMPARED_HEADERS.map((name) => [name, response.headers.get(name) ?? null] as const);

const sameHeaders = (a: RawResponse, b: RawResponse): boolean =>
  COMPARED_HEADERS.every((name) => (a.headers.get(name) ?? null) === (b.headers.get(name) ?? null));

interface PhaseResult {
  readonly phase: string;
  readonly serviceUrl: string;
  readonly py: Gw;
  readonly ts: Gw;
  readonly compared: ReadonlyMap<string, Compared>;
  readonly pyExit: number | null;
  readonly tsExit: number | null;
  readonly pyStdout: string;
  readonly tsStdout: string;
  readonly pyStderr: string;
  readonly tsStderr: string;
  readonly pyReadyRemoved: boolean;
  readonly tsReadyRemoved: boolean;
  readonly table: ReadonlyArray<string>;
}

/**
 * Run every applicable leg against both servers, one side at a time, slicing
 * the stub's recording between the two so each side's forwarded targets are
 * attributable without inventing a per-client marker.
 */
const compareAll = async (
  phase: string,
  py: Gw,
  ts: Gw,
  stub: Stub | null,
): Promise<ReadonlyMap<string, Compared>> => {
  const legs = LEGS.filter((leg) => phase === 'upstream' || leg.upstreamOnly !== true);
  const results = new Map<string, Compared>();
  await legs.reduce(async (previous, leg) => {
    await previous;
    const before = stub?.targets().length ?? 0;
    const pyResponse = await request(py.url, leg);
    const middle = stub?.targets().length ?? 0;
    const tsResponse = await request(ts.url, leg);
    const after = stub?.targets().length ?? 0;
    results.set(leg.name, {
      leg: leg.name,
      py: pyResponse,
      ts: tsResponse,
      statusEqual: pyResponse.status === tsResponse.status,
      bodyEqual: normalize(pyResponse.body, py.url) === normalize(tsResponse.body, ts.url),
      headersEqual: sameHeaders(pyResponse, tsResponse),
      pyTargets: stub === null ? [] : stub.targets().slice(before, middle),
      tsTargets: stub === null ? [] : stub.targets().slice(middle, after),
    });
  }, Promise.resolve());
  return results;
};

/** Stop a gateway the way `local_stack` does, and wait for it. */
const stopGateway = async (gateway: Gw): Promise<number | null> => {
  gateway.process.kill('SIGINT');
  await gateway.process.exited;
  return gateway.process.exitCode;
};

const renderTable = (phase: string, compared: ReadonlyMap<string, Compared>): ReadonlyArray<string> =>
  Array.from(compared.values(), (row) => {
    const verdict = row.statusEqual && row.bodyEqual && row.headersEqual ? 'MATCH' : 'DIFF ';
    return `${verdict} ${phase.padEnd(8)} ${row.leg.padEnd(30)} py=${row.py.status} ts=${row.ts.status} body=${row.bodyEqual} headers=${row.headersEqual}`;
  });

// ---------------------------------------------------------------------------
// The rig
// ---------------------------------------------------------------------------

interface Rig {
  readonly runsRoot: string;
  readonly scratch: string;
  readonly stub: Stub;
  readonly offline: PhaseResult;
  readonly upstream: PhaseResult;
  readonly stubHeadersSeen: ReadonlyArray<string>;
}

const state: { rig: Rig | null; roots: string[]; stub: Stub | null } = {
  rig: null,
  roots: [],
  stub: null,
};

const rig = (): Rig => {
  const value = state.rig;
  if (value === null) throw new Error('rig not built');
  return value;
};

const phase = (name: 'offline' | 'upstream'): PhaseResult => rig()[name];

const row = (name: 'offline' | 'upstream', leg: string): Compared => {
  const found = phase(name).compared.get(leg);
  if (found === undefined) throw new Error(`no result for ${name}:${leg}`);
  return found;
};

const runPhase = async (
  name: 'offline' | 'upstream',
  serviceUrl: string,
  runsRoot: string,
  scratch: string,
  stub: Stub | null,
): Promise<PhaseResult> => {
  const spec: SpawnSpec = { runsRoot, serviceUrl, scratch, slot: name };
  const py = await spawnGateway('python', spec);
  const ts = await spawnGateway('typescript', spec);
  const compared = await compareAll(name, py, ts, stub);
  const pyExit = await stopGateway(py);
  const tsExit = await stopGateway(ts);
  return {
    phase: name,
    serviceUrl,
    py,
    ts,
    compared,
    pyExit,
    tsExit,
    pyStdout: await py.stdout(),
    tsStdout: await ts.stdout(),
    pyStderr: await py.stderr(),
    tsStderr: await ts.stderr(),
    pyReadyRemoved: !existsSync(py.readyFile),
    tsReadyRemoved: !existsSync(ts.readyFile),
    table: renderTable(name, compared),
  };
};

/** Ample: two `bun` starts, two CPython starts and ~100 requests, some of
 * which spawn a `python3 -m agent_eval.replay_derive_cli` on each side. */
const RIG_TIMEOUT_MS = 600_000;

beforeAll(async () => {
  if (!DARWIN) return;
  const runsRoot = buildRunsRoot();
  const scratch = realpathSync(mkdtempSync(join(tmpdir(), 'arena-smoke-rt-')));
  state.roots = [runsRoot, scratch];
  const stub = startStub();
  state.stub = stub;
  const offline = await runPhase('offline', 'http://127.0.0.1:1', runsRoot, scratch, null);
  const upstream = await runPhase('upstream', stub.origin, runsRoot, scratch, stub);
  const stubHeadersSeen = stub.lastHeaders();
  state.rig = { runsRoot, scratch, stub, offline, upstream, stubHeadersSeen };
  // oxlint-disable-next-line effecttsgo/global-console -- the leg-by-leg table
  // is this file's deliverable and has to reach a terminal, not a Logger.
  console.log(['', ...offline.table, ...upstream.table, ''].join('\n'));
}, RIG_TIMEOUT_MS);

/**
 * Unconditional teardown, driven by the {@link spawned} registry rather than by
 * a successfully-built rig — a `beforeAll` that throws halfway is exactly the
 * case that leaves processes behind, and it is the case a rig keyed on its own
 * success cannot clean up.  `runPhase` has normally already stopped both pairs
 * with `SIGINT`; this is the `SIGKILL` for anything it did not reach.
 */
afterAll(async () => {
  await Promise.all(
    spawned.map(async (child) => {
      if (child.exitCode === null) child.kill('SIGKILL');
      await child.exited;
    }),
  );
  await state.stub?.stop();
  state.roots.forEach((root) => rmSync(root, { recursive: true, force: true }));
});

// ---------------------------------------------------------------------------

const problem = (name: Gateway.GatewayProblemName): { status: number; body: string } => {
  const message = Gateway.GATEWAY_PROBLEM_MESSAGES[name];
  return {
    status: Gateway.GATEWAY_PROBLEM_STATUS[message],
    body: `{"error":${JSON.stringify(message)}}`,
  };
};

/**
 * Set `ARENA_REQUIRE_PARITY=1` to make a skipped rig a **failure** rather than
 * a silent pass.
 *
 * `describe.skipIf(!DARWIN)` is the right gate — `flock` through `bun:ffi`
 * against `libSystem.B.dylib` and a `python3` that owns the oracle are darwin
 * facts, and a partial run would report a parity result the platform cannot
 * support — but a gate that vanishes is indistinguishable from a gate that
 * passed: on a Linux CI box `bun test` would print `N pass, 0 fail` with this
 * file contributing zero assertions, and a green run would not be evidence of
 * anything.  So the skip is announced, loudly, and a job that means to *depend*
 * on the parity claim can set the flag and get an error instead.
 */
const REQUIRE_PARITY = process.env['ARENA_REQUIRE_PARITY'] !== undefined;

test('the live parity rig is not silently skipped', () => {
  if (!DARWIN) {
    // oxlint-disable-next-line effecttsgo/global-console -- a skipped oracle has
    // to reach a terminal; there is no Logger in a bun:test process.
    console.warn(
      `\n!! live smoke parity DID NOT RUN: platform is ${process.platform}, not darwin.\n` +
        '!! ~50 legs x 2 phases of Python-vs-TypeScript byte parity contributed ZERO\n' +
        '!! assertions to this run.  Set ARENA_REQUIRE_PARITY=1 to make this a failure.\n',
    );
  }
  expect({ platform: process.platform, ranParityRig: DARWIN || !REQUIRE_PARITY }).toEqual({
    platform: process.platform,
    ranParityRig: true,
  });
});

describe.skipIf(!DARWIN)('live smoke parity', () => {
  describe('the rig itself', () => {
    test('both implementations published a ready record and a working url', () => {
      (['offline', 'upstream'] as const).forEach((name) => {
        const result = phase(name);
        [result.py, result.ts].forEach((gateway) => {
          expect(gateway.readyRecord['ok']).toBe(true);
          expect(gateway.readyRecord['kind']).toBe('freeciv-replay-gateway');
          expect(gateway.readyRecord['schema_version']).toBe(1);
          expect(gateway.readyRecord['pid']).toBe(gateway.pid);
          expect(String(gateway.readyRecord['identity'])).toMatch(/^[0-9a-f]{20}$/);
        });
      });
    });

    test('the ready line on stdout is the record, canonically, and nothing else', () => {
      (['offline', 'upstream'] as const).forEach((name) => {
        const result = phase(name);
        ([[result.py, result.pyStdout], [result.ts, result.tsStdout]] as const).forEach(
          ([gateway, out]) => {
            const lines = out.split('\n').filter((line) => line !== '');
            expect(lines).toHaveLength(1);
            const parsed: unknown = JSON.parse(lines[0] ?? '');
            expect(parsed).toEqual(gateway.readyRecord);
          },
        );
      });
    });

    test('SIGINT is a clean exit and removes the ready file, on both', () => {
      (['offline', 'upstream'] as const).forEach((name) => {
        const result = phase(name);
        expect({ phase: name, py: result.pyExit, ts: result.tsExit }).toEqual({
          phase: name,
          py: 0,
          ts: 0,
        });
        expect({ phase: name, py: result.pyReadyRemoved, ts: result.tsReadyRemoved }).toEqual({
          phase: name,
          py: true,
          ts: true,
        });
        expect(result.pyStderr).toBe('');
        expect(result.tsStderr).toBe('');
      });
    });

    test('every process this file spawned is gone', () => {
      const pids = (['offline', 'upstream'] as const).flatMap((name) => {
        const result = phase(name);
        return [result.py.pid, result.ts.pid];
      });
      const alive = pids.filter((pid) => {
        const probe = Bun.spawnSync(['ps', '-p', String(pid), '-o', 'pid=,comm=']);
        return probe.stdout.toString().trim() !== '';
      });
      expect({ pids: pids.length, alive }).toEqual({ pids: 4, alive: [] });
    });
  });

  // -------------------------------------------------------------------------

  describe('status parity', () => {
    test.each(
      LEGS.filter(
        (leg) =>
          // Bun's HTTP parser answers these before any handler; they are the
          // two documented status divergences below.
          leg.name !== 'space-in-request-target',
      ).map((leg) => leg.name),
    )('%s', (name) => {
      const applicable = (['offline', 'upstream'] as const).filter(
        (candidate) => candidate === 'upstream' || legByName(name).upstreamOnly !== true,
      );
      applicable.forEach((candidate) => {
        const result = row(candidate, name);
        expect({
          phase: candidate,
          leg: name,
          status: result.ts.status,
        }).toEqual({ phase: candidate, leg: name, status: result.py.status });
      });
    });
  });

  describe('header-subset parity', () => {
    test.each(
      LEGS.filter(
        (leg) =>
          leg.name !== 'space-in-request-target' &&
          leg.name !== 'get-bad-content-length' &&
          leg.name !== 'get-transfer-encoding',
      ).map((leg) => leg.name),
    )('%s', (name) => {
      const applicable = (['offline', 'upstream'] as const).filter(
        (candidate) => candidate === 'upstream' || legByName(name).upstreamOnly !== true,
      );
      applicable.forEach((candidate) => {
        const result = row(candidate, name);
        expect({ phase: candidate, headers: headerSubset(result.ts) }).toEqual({
          phase: candidate,
          headers: headerSubset(result.py),
        });
      });
    });
  });

  describe('body byte parity', () => {
    /** The three legs whose bodies cannot be equal, each for a named reason. */
    const EXEMPT = new Set([
      // process-scoped fields; asserted field-wise below instead
      'health',
      'health-fragment',
      'absolute-form-target',
      'get-content-length-zero',
      // Bun's adapter strips HEAD bodies (spike law); headers still compared
      'head-405',
      // Bun's parser answers before the handler; documented below
      'get-bad-content-length',
      'get-transfer-encoding',
      'space-in-request-target',
    ]);

    test.each(LEGS.filter((leg) => !EXEMPT.has(leg.name)).map((leg) => leg.name))('%s', (name) => {
      const applicable = (['offline', 'upstream'] as const).filter(
        (candidate) => candidate === 'upstream' || legByName(name).upstreamOnly !== true,
      );
      applicable.forEach((candidate) => {
        const result = row(candidate, name);
        expect({
          phase: candidate,
          leg: name,
          body: normalize(result.ts.body, phase(candidate).ts.url),
        }).toEqual({
          phase: candidate,
          leg: name,
          body: normalize(result.py.body, phase(candidate).py.url),
        });
      });
    });

    test('/health differs in exactly the five process-scoped fields', () => {
      (['offline', 'upstream'] as const).forEach((candidate) => {
        const result = row(candidate, 'health');
        const pyBody = healthFields(result.py.body);
        const tsBody = healthFields(result.ts.body);
        expect(Object.keys(tsBody).toSorted()).toEqual(Object.keys(pyBody).toSorted());
        const differing = Object.keys(pyBody)
          .filter((key) => JSON.stringify(pyBody[key]) !== JSON.stringify(tsBody[key]))
          .toSorted();
        // `upstream_service_url` is identical: both were pointed at the same
        // upstream, which is the point of "identical flags".
        expect(differing).toEqual([...PROCESS_SCOPED_HEALTH_FIELDS].toSorted());
      });
    });
  });

  // -------------------------------------------------------------------------

  describe('the disk-fallback matrix, offline', () => {
    test('the games index relabels the orphaned run as interrupted, identically', () => {
      const result = row('offline', 'games-index');
      const parsed = JSON.parse(result.py.body.toString('utf8')) as {
        games: ReadonlyArray<Record<string, unknown>>;
      };
      const live = parsed.games.find((game) => game['game_id'] === LIVE);
      expect(live?.['state']).toBe('interrupted');
      // 44 is the last *parseable* line: the scan walks past the torn tail.
      expect(live?.['current_turn']).toBe(44);
      expect(result.bodyEqual).toBe(true);
    });

    test('a run directory secret never reaches a body, on either side', () => {
      const bodies = Array.from(phase('offline').compared.values()).flatMap((entry) => [
        entry.py.body.toString('latin1'),
        entry.ts.body.toString('latin1'),
      ]);
      expect(bodies.filter((body) => body.includes('must-not-leak'))).toEqual([]);
      expect(bodies.filter((body) => body.includes('owner_token'))).toEqual([]);
    });

    test('a non-terminal run is not exposed as a terminal archive', () => {
      // The run is on disk and its manifest reads fine, so `_read_manifest`
      // succeeds and `_terminal_archive` is the thing that refuses it.
      const live = row('offline', 'live-run-status');
      expect(live.py.status).toBe(problem('terminalArchiveNotFound').status);
      expect(live.py.body.toString('utf8')).toBe(problem('terminalArchiveNotFound').body);
      expect(live.bodyEqual).toBe(true);
      // No directory at all is a *different* 404, from `_read_manifest` itself.
      const absent = row('offline', 'absent-id');
      expect(absent.py.status).toBe(problem('gameNotFound').status);
      expect(absent.py.body.toString('utf8')).toBe(problem('gameNotFound').body);
      expect(absent.bodyEqual).toBe(true);
    });
  });

  describe('the upstream client', () => {
    test('both forward the same targets, with Python int() query normalization', () => {
      const cases: ReadonlyArray<readonly [string, ReadonlyArray<string>]> = [
        ['status', [`/v1/games/${ARCHIVED}/status`]],
        // B1: the bare form proxies without `/status`…
        ['bare-id-alias', [`/v1/games/${ARCHIVED}`]],
        // …and the trailing slash survives into the forwarded target.
        ['status-trailing-slash', [`/v1/games/${ARCHIVED}/status/`]],
        // The stdlib collapses `//` before the gateway sees it.
        ['status-leading-slashes', [`/v1/games/${ARCHIVED}/status`]],
        // B4 + P7: `5_0` is 50 to Python's int(), `+1` is 1, and the query is
        // re-encoded as after_turn-then-limit with both materialized.
        ['replay-python-int-literal', [`/v1/games/${ARCHIVED}/replay.json?after_turn=1&limit=50`]],
        ['board-turn1', [`/v1/games/${ARCHIVED}/board.json?turn=1`]],
        ['events', [`/v1/games/${ARCHIVED}/events.json`]],
        ['frame-0-png', [`/v1/games/${ARCHIVED}/frames/0.png`]],
        // Never proxied at all.
        ['health', []],
        ['short-id-404', []],
        ['frame-007-png-404', []],
        ['nonsense-suffix-query-400', []],
        ['get-with-body-400', []],
        ['events-query-400', []],
        ['replay-limit-251-400', []],
        // A redirect is refused without a second request.
        ['upstream-redirect-502', [`/v1/games/${UPSTREAM_REDIRECT}/status`]],
      ];
      cases.forEach(([name, expected]) => {
        const result = row('upstream', name);
        expect({ leg: name, py: result.pyTargets, ts: result.tsTargets }).toEqual({
          leg: name,
          py: expected,
          ts: expected,
        });
      });
    });

    test('binary routes ask for */* and JSON routes for application/json', () => {
      // The stub records in arrival order; the two frame requests (py then ts)
      // are the only `*/*` in the pair for that leg.
      const frames = row('upstream', 'frame-0-png');
      expect(frames.pyTargets).toHaveLength(1);
      expect(frames.tsTargets).toHaveLength(1);
      const accepts = rig().stub.accepts();
      expect(accepts).toContain('*/*');
      expect(accepts).toContain('application/json');
    });

    test('no inbound header is forwarded, on either side', () => {
      const result = row('upstream', 'credentials-not-forwarded');
      expect(result.pyTargets).toEqual([`/v1/games/${ARCHIVED}/watch.json`]);
      expect(result.tsTargets).toEqual([`/v1/games/${ARCHIVED}/watch.json`]);
      const forwarded = rig().stubHeadersSeen;
      ['authorization', 'cookie', 'x-forwarded-host', 'range'].forEach((name) =>
        expect(forwarded).not.toContain(name),
      );
      // The upstream body is relayed verbatim, whitespace and all.
      expect(result.py.body.toString('utf8')).toBe('{"zeta":1,  "alpha":[2,3] ,"n":1.5}');
      expect(result.bodyEqual).toBe(true);
    });

    test('an upstream 500 is relayed with its own status and never masked by disk', () => {
      const result = row('upstream', 'upstream-500-relay');
      expect(result.py.status).toBe(500);
      expect(result.py.body.toString('utf8')).toBe(
        `{"error":${JSON.stringify(Gateway.upstreamReturnedHttp(500))}}`,
      );
      expect(result.bodyEqual).toBe(true);
      expect(result.headersEqual).toBe(true);
    });

    test('a 500 on a binary route is a JSON problem, not an image (trap B5)', () => {
      const result = row('upstream', 'upstream-500-on-png');
      expect(result.py.headers.get('content-type')).toBe('application/json; charset=utf-8');
      expect(result.ts.headers.get('content-type')).toBe('application/json; charset=utf-8');
      expect(result.bodyEqual).toBe(true);
    });

    test('a redirect is 502 and is never followed', () => {
      const result = row('upstream', 'upstream-redirect-502');
      expect(result.py.status).toBe(problem('upstreamRedirect').status);
      expect(result.py.body.toString('utf8')).toBe(problem('upstreamRedirect').body);
      expect(result.bodyEqual).toBe(true);
    });

    test('the portless-502 signature is the offline path, not a relay', () => {
      const result = row('upstream', 'upstream-portless-offline');
      // A relay would be 502; falling through to disk with no such run is the
      // 404 `_read_manifest` raises. That the answer is 404 *is* the proof the
      // probe fired, on both implementations.
      expect(result.py.status).toBe(problem('gameNotFound').status);
      expect(result.py.body.toString('utf8')).toBe(problem('gameNotFound').body);
      expect(result.bodyEqual).toBe(true);
      expect(result.headersEqual).toBe(true);
    });
  });

  // -------------------------------------------------------------------------
  // Known divergences, each pinned to its exact shape
  // -------------------------------------------------------------------------

  describe('known divergences', () => {
    test('HEAD: headers match including Content-Length; the adapter strips the body', () => {
      const result = row('offline', 'head-405');
      expect(result.py.body.toString('utf8')).toBe('{"error":"method not allowed"}');
      expect(result.ts.body).toHaveLength(0);
      // The header the stripping must not disturb.
      expect(result.py.headers.get('content-length')).toBe('30');
      expect(result.ts.headers.get('content-length')).toBe('30');
      expect(result.headersEqual).toBe(true);
    });

    test('an unparseable Content-Length is answered by Bun, before any handler', () => {
      const result = row('offline', 'get-bad-content-length');
      // Python reaches `_reject_body` and produces its own problem.
      expect(result.py.status).toBe(problem('invalidContentLength').status);
      expect(result.py.body.toString('utf8')).toBe(problem('invalidContentLength').body);
      // Bun's HTTP parser refuses the framing: same status, bare response.
      expect(result.ts.status).toBe(400);
      expect(result.ts.body).toHaveLength(0);
      expect(headerSubset(result.ts)).toEqual(COMPARED_HEADERS.map((name) => [name, null]));
    });

    test('a Transfer-Encoding on a GET is likewise answered by Bun', () => {
      const result = row('offline', 'get-transfer-encoding');
      expect(result.py.status).toBe(problem('getRequestBody').status);
      expect(result.py.body.toString('utf8')).toBe(problem('getRequestBody').body);
      expect(result.ts.status).toBe(400);
      expect(result.ts.body).toHaveLength(0);
    });

    test('a GET with a *well-framed* body does reach the handler and matches', () => {
      const result = row('offline', 'get-with-body-400');
      expect(result.py.body.toString('utf8')).toBe(problem('getRequestBody').body);
      expect(result.bodyEqual).toBe(true);
      expect(result.headersEqual).toBe(true);
    });

    test('a space in the request target: CPython 400-HTML, Bun 505', () => {
      const result = row('offline', 'space-in-request-target');
      // CPython's `parse_request` cannot split it and sends the stdlib page.
      expect(result.py.status).toBe(400);
      expect(result.py.reason).toBe("Bad request syntax ('GET /v1/games?a= b HTTP/1.1')");
      expect(result.py.headers.get('content-type')).toBe('text/html;charset=utf-8');
      // Bun reads the trailing `b HTTP/1.1` as the version and refuses that.
      expect(result.ts.status).toBe(505);
      expect(result.ts.body).toHaveLength(0);
    });

    /**
     * The defect this file was written to find, now closed.
     *
     * `replay.json` and `board.json` relay the derivation's document
     * unprojected, and it used to arrive through `Schema.parseJson` — i.e.
     * `JSON.parse` — so every JSON integer became a JS `number` and wire's
     * canonical writer spelled a `number` as a Python **float**.  Python's
     * `{"next_after_turn":0,"schema_version":1}` came back
     * `{"next_after_turn":0.0,"schema_version":1.0}`: four bytes, two routes,
     * a different digest, and a `schema_version` wire's own decoders reject.
     * `events.json` escaped it only because `publicEvents` rebuilt its numeric
     * fields as `bigint`.
     *
     * The bridge's stdout is now read by `src/gateway/python-json.ts`, which
     * yields `bigint` for an integer literal and `number` for a float one, so
     * the three legs are ordinary members of the body-parity table above and
     * this test only pins the *shape* of what used to be wrong.
     */
    test('replay.json carries Python integer spelling, not floats', () => {
      (['replay-limit5-cold', 'replay-python-int-literal'] as const).forEach((name) => {
        const result = row('offline', name);
        const tsText = result.ts.body.toString('utf8');
        const pyText = result.py.body.toString('utf8');
        expect({ leg: name, body: tsText }).toEqual({ leg: name, body: pyText });
        expect(tsText).toContain('"schema_version":1');
        expect(tsText).not.toContain('"schema_version":1.0');
        expect(tsText).not.toContain('"next_after_turn":0.0');
      });
    });
  });
});
