/**
 * Cross-request parity for cold/warm/re-cold caches, corrupt entries,
 * concurrent derivation, live fixture mutation, upstream flapping, restart,
 * and ready-file contention/takeover.
 *
 * The committed tree is copied before mutation. Every gateway and stub binds
 * port 0 in private scratch space, and boot rejects `.agent-eval`, so no live
 * stack or fixture can be modified.
 */

import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import {
  chmodSync,
  cpSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  readdirSync,
  renameSync,
  rmSync,
  statSync,
  symlinkSync,
  writeFileSync,
} from 'node:fs';
import { createHash } from 'node:crypto';
import { createServer, type Socket } from 'node:net';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { type JsonObject } from '@arena/wire';
import {
  parseJsonObjectFromText,
  pidFromStackRecord,
  pipedStreamText,
  portFromListenAddress,
} from './json-boundary.ts';
import {
  bootGatewayPair,
  HARNESS_ROOT,
  killAllBooted,
  PARITY_PLATFORM_SUPPORTED,
  PARITY_REQUIRED,
  paritySkipWarning,
  PYTHON_LAUNCHER,
  PYTHON_ROOT,
  registerBooted,
  readyFileFor,
  REPO_ROOT,
  TYPESCRIPT_LAUNCHER,
  unwrapPair,
  type ByImpl,
  type GatewayPair,
  type Impl,
} from './boot.ts';
import { PARITY_RUNS_ROOT } from './fixtures/scenarios.ts';
import { VALID_GAME_ID } from './fixtures/request-cases.ts';
import { makeStub, type StubHandle } from './stub-supervisor.ts';
import { bodyLatin1, isWireResponse, wireRequest, type WireOutcome } from './wire-client.ts';

/** Linux and Darwin have the native locking support needed by the full rig. */
const PLATFORM_SUPPORTED = PARITY_PLATFORM_SUPPORTED;

/** Unconditional: unsupported-platform skips must be visible or required to fail. */
test('the state and cache fuzz is not silently skipped', () => {
  if (!PLATFORM_SUPPORTED) {
    // oxlint-disable-next-line effecttsgo/global-console -- skipped parity must reach the terminal
    console.warn(
      paritySkipWarning(
        'the state and cache fuzz',
        'every ready-file, cache-root and runs-root mutation probe',
      ),
    );
  }
  expect({
    platform: process.platform,
    ranTheFuzz: PLATFORM_SUPPORTED || !PARITY_REQUIRED,
  }).toEqual({
    platform: process.platform,
    ranTheFuzz: true,
  });
});

/** Pinned on both sides so no archive body carries the answering port. */
const VIEWER_PUBLIC_URL = 'http://viewer.parity.invalid';

/** Every phase together spawns eight gateways and derives ~30 times. */
const SETUP_TIMEOUT_MS = 300_000;

const INTERRUPTED_GAME_ID = 'game_parity_interrupted_03';
const NOWIN_GAME_ID = 'game_parity_terminal_nowin_02';
const TORN_GAME_ID = 'game_parity_torn_tail_08';
const HUSK_GAME_ID = 'game_parity_lobby_husk_04';
const ADDED_GAME_ID = 'game_parity_added_midrun_09';

// ---------------------------------------------------------------------------
// Comparison
// ---------------------------------------------------------------------------

/** The header subset `diff.test.ts` compares, spelled the same way. */
const COMPARED_HEADERS: ReadonlyArray<string> = [
  'content-type',
  'cache-control',
  'allow',
  'x-content-type-options',
  'referrer-policy',
  'etag',
  'last-modified',
  'content-length',
];

/** Binary legs now compare the full matrix header set, framing included. */
const BINARY_ROUTE_ASPECTS: ReadonlyArray<string> = COMPARED_HEADERS;

const digest = (text: string): string =>
  createHash('sha256').update(Buffer.from(text, 'latin1')).digest('hex').slice(0, 16);

/** One side's comparable fingerprint.  `null` fields mean "there was no response". */
interface LegFingerprint {
  readonly tag: string;
  readonly status: number | null;
  readonly reason: string | null;
  readonly framing: string | null;
  readonly bodySha: string | null;
  readonly bodyLen: number | null;
  readonly headers: Readonly<Record<string, string | null>>;
}

const legFingerprintOf = (outcome: WireOutcome): LegFingerprint =>
  isWireResponse(outcome)
    ? {
        tag: 'Response',
        status: outcome.status,
        reason: outcome.reasonLine,
        framing: outcome.completedBy,
        bodySha: digest(bodyLatin1(outcome)),
        bodyLen: bodyLatin1(outcome).length,
        headers: Object.fromEntries(
          COMPARED_HEADERS.map((name) => [name, outcome.headers.get(name)]),
        ),
      }
    : {
        tag: outcome._tag,
        status: null,
        reason: null,
        framing: null,
        bodySha: null,
        bodyLen: null,
        headers: {},
      };

/** One request, sent to both gateways, reduced to what parity asserts. */
interface Leg {
  readonly name: string;
  readonly target: string;
  readonly python: LegFingerprint;
  readonly typescript: LegFingerprint;
  readonly diffs: ReadonlyArray<string>;
}

const diffsOf = (
  python: LegFingerprint,
  typescript: LegFingerprint,
  headers: ReadonlyArray<string>,
): ReadonlyArray<string> => [
  ...(python.tag === typescript.tag ? [] : [`outcome ${python.tag} vs ${typescript.tag}`]),
  ...(python.status === typescript.status
    ? []
    : [`status ${String(python.status)} vs ${String(typescript.status)}`]),
  ...(python.reason === typescript.reason
    ? []
    : [`reason ${String(python.reason)} vs ${String(typescript.reason)}`]),
  ...headers.flatMap((name) =>
    (python.headers[name] ?? null) === (typescript.headers[name] ?? null)
      ? []
      : [`header:${name} ${String(python.headers[name])} vs ${String(typescript.headers[name])}`],
  ),
  ...(python.bodySha === typescript.bodySha
    ? []
    : [`body ${String(python.bodySha)} vs ${String(typescript.bodySha)}`]),
];

const LEG_TIMEOUT_MS = 60_000;

/** Send one target to both sides and reduce it to a {@link Leg}. */
const runLeg = async (
  pair: GatewayPair,
  name: string,
  target: string,
  headers: ReadonlyArray<string> = COMPARED_HEADERS,
): Promise<Leg> => {
  const request = { method: 'GET', target, timeoutMs: LEG_TIMEOUT_MS } as const;
  const [python, typescript] = await Promise.all([
    wireRequest(pair.python.origin, request),
    wireRequest(pair.typescript.origin, request),
  ]);
  const left = legFingerprintOf(python);
  const right = legFingerprintOf(typescript);
  return { name, target, python: left, typescript: right, diffs: diffsOf(left, right, headers) };
};

/** `count` identical requests in flight at once, per side. */
const runBurst = async (
  pair: GatewayPair,
  name: string,
  target: string,
  count: number,
): Promise<ReadonlyArray<Leg>> => {
  const request = { method: 'GET', target, timeoutMs: LEG_TIMEOUT_MS } as const;
  const indices = Array.from({ length: count }, (_unused, index) => index);
  const [pythons, typescripts] = await Promise.all([
    Promise.all(indices.map(() => wireRequest(pair.python.origin, request))),
    Promise.all(indices.map(() => wireRequest(pair.typescript.origin, request))),
  ]);
  return indices.map((index) => {
    const left = legFingerprintOf(pythons[index] ?? { _tag: 'ClosedWithoutResponse', message: 'missing' });
    const right = legFingerprintOf(typescripts[index] ?? { _tag: 'ClosedWithoutResponse', message: 'missing' });
    return {
      name: `${name}#${String(index)}`,
      target,
      python: left,
      typescript: right,
      diffs: diffsOf(left, right, COMPARED_HEADERS),
    };
  });
};

/** Run legs one after another — the order *is* the meaning in every phase here. */
const runSequence = (
  pair: GatewayPair,
  legs: ReadonlyArray<readonly [string, string]>,
  headers: ReadonlyArray<string> = COMPARED_HEADERS,
): Promise<ReadonlyArray<Leg>> =>
  legs.reduce<Promise<ReadonlyArray<Leg>>>(
    async (previous, [name, target]) => [
      ...(await previous),
      await runLeg(pair, name, target, headers),
    ],
    Promise.resolve([]),
  );

const bodyShas = (legs: ReadonlyArray<Leg>, side: 'python' | 'typescript'): ReadonlyArray<string> =>
  legs.map((leg) => String(leg[side].bodySha));

const allDiffs = (legs: ReadonlyArray<Leg>): ReadonlyArray<string> =>
  legs.flatMap((leg) => leg.diffs.map((one) => `${leg.name}: ${one}`));

// ---------------------------------------------------------------------------
// The fixture copy
// ---------------------------------------------------------------------------

/** A writable copy of the committed tree.  Symlink fixture preserved. */
const cloneFixtures = (destination: string): string => {
  mkdirSync(destination, { recursive: true });
  cpSync(PARITY_RUNS_ROOT, destination, { recursive: true, verbatimSymlinks: true });
  return destination;
};

const manifestPath = (runsRoot: string, gameId: string): string =>
  join(runsRoot, gameId, 'manifest.json');

const readJson = (path: string): JsonObject =>
  parseJsonObjectFromText(readFileSync(path, 'utf8')) ?? {};

const writeJson = (path: string, value: JsonObject): void =>
  writeFileSync(path, `${JSON.stringify(value, null, 2)}\n`);

const cacheTree = (root: string) => {
  const walk = (directory: string, prefix: string): ReadonlyArray<readonly [string, string]> =>
    readdirSync(directory, { withFileTypes: true }).flatMap((entry) => {
      const full = join(directory, entry.name);
      const key = prefix === '' ? entry.name : `${prefix}/${entry.name}`;
      return entry.isDirectory()
        ? walk(full, key)
        : [[key, digest(readFileSync(full, 'latin1'))] as const];
    });
  return Object.fromEntries(
    statSync(root, { throwIfNoEntry: false }) === undefined ? [] : walk(root, ''),
  ) satisfies Readonly<Record<string, string>>;
};

// ---------------------------------------------------------------------------
// The flapping upstream
// ---------------------------------------------------------------------------

/** `up` answers 404 (the disk-fallback branch); `reset` resets every connect. */
type FlapMode = 'up' | 'reset';

interface FlapUpstream {
  readonly origin: string;
  readonly setMode: (mode: FlapMode) => void;
  readonly close: () => Promise<void>;
}

const FLAP_404_BODY = '{ "error": "no such game",  "stub": "flap-404" }';

/**
 * A stable-port upstream: `reset` reaches `UpstreamUnavailable` without the
 * self-proxy race caused by releasing and reusing an ephemeral port.
 */
interface FlapServerState {
  mode: FlapMode;
}

const makeFlapUpstream = (): Promise<FlapUpstream> => {
  const flapState: FlapServerState = { mode: 'up' };
  const server = createServer((socket: Socket) => {
    if (flapState.mode === 'reset') {
      socket.resetAndDestroy();
      return;
    }
    socket.on('error', () => undefined);
    socket.on('data', () =>
      socket.end(
        `HTTP/1.1 404 Not Found\r\nContent-Type: application/json\r\nContent-Length: ${String(
          Buffer.byteLength(FLAP_404_BODY),
        )}\r\nConnection: close\r\n\r\n${FLAP_404_BODY}`,
      ),
    );
  });
  return new Promise<FlapUpstream>((resolve) => {
    server.listen(0, '127.0.0.1', () => {
      const address = server.address();
      const port = portFromListenAddress(address);
      resolve({
        origin: `http://127.0.0.1:${String(port)}`,
        setMode: (mode) => {
          flapState.mode = mode;
        },
        close: () => new Promise<void>((done) => server.close(() => done())),
      });
    });
  });
};

// ---------------------------------------------------------------------------
// Raw single-gateway spawns, for the ready-file legs
// ---------------------------------------------------------------------------

/** Raw spawn result for ready files that may predate the child. */
interface SpawnOutcome {
  readonly exitCode: number | null;
  readonly signal: string | null;
  readonly stderrEmpty: boolean;
  readonly readyExists: boolean;
  readonly readyPid: number | null;
}

const readyPidOf = (path: string): number | null => {
  const stats = statSync(path, { throwIfNoEntry: false });
  if (stats === undefined || !stats.isFile()) return null;
  const parsed = parseJsonObjectFromText(readFileSync(path, 'utf8'));
  return parsed === null ? null : pidFromStackRecord(parsed);
};

const spawnGateway = (
  impl: Impl,
  serviceUrl: string,
  runsRoot: string,
  readyFile: string,
  cacheRoot: string,
): Bun.Subprocess =>
  // Registered at the spawn, never later: `killAllBooted()` in this file's
  // `afterAll` is then a total safety net over these four gateways too.
  registerBooted(
    Bun.spawn(
      [
        ...(impl === 'python' ? PYTHON_LAUNCHER : TYPESCRIPT_LAUNCHER),
        '--host', '127.0.0.1',
        '--port', '0',
        '--service-url', serviceUrl,
        '--runs-root', runsRoot,
        '--cache-root', cacheRoot,
        '--repo-root', REPO_ROOT,
        '--ready-file', readyFile,
        '--upstream-timeout-s', '1',
        '--viewer-public-url', VIEWER_PUBLIC_URL,
      ],
      {
        cwd: impl === 'python' ? REPO_ROOT : HARNESS_ROOT,
        stdout: 'pipe',
        stderr: 'pipe',
        env: {
          ...process.env,
          ARENA_GATEWAY_TELEMETRY_DIR: undefined,
          PYTHONPATH: PYTHON_ROOT,
        },
      },
    ),
  );

/** Ceiling for a gateway expected to remain live; refusals exit earlier. */
const LIVE_PROBE_MS = 3_000;

/** Spawn, wait up to `waitMs`, SIGINT if still alive, and report. */
const spawnAndSettle = async (
  impl: Impl,
  serviceUrl: string,
  runsRoot: string,
  readyFile: string,
  cacheRoot: string,
  waitMs: number,
): Promise<SpawnOutcome> => {
  const child = spawnGateway(impl, serviceUrl, runsRoot, readyFile, cacheRoot);
  const raced = await Promise.race([
    child.exited.then(() => 'exited' as const),
    Bun.sleep(waitMs).then(() => 'alive' as const),
  ]);
  const readyExists = existsSync(readyFile);
  const readyPid = readyPidOf(readyFile);
  if (raced === 'alive') {
    child.kill('SIGINT');
    await child.exited;
  }
  return {
    exitCode: child.exitCode,
    signal: child.signalCode,
    stderrEmpty: (await pipedStreamText(child.stderr)).trim() === '',
    readyExists,
    readyPid,
  };
};

// ---------------------------------------------------------------------------
// What the phases produce
// ---------------------------------------------------------------------------

interface DerivationRoute {
  readonly name: string;
  readonly target: string;
}

/** Every route whose answer is a derivation subprocess, plus its neighbours. */
const DERIVATION_ROUTES: ReadonlyArray<DerivationRoute> = [
  { name: 'replay-default', target: `/v1/games/${VALID_GAME_ID}/replay.json` },
  { name: 'replay-limit-5', target: `/v1/games/${VALID_GAME_ID}/replay.json?limit=5` },
  { name: 'replay-after-1', target: `/v1/games/${VALID_GAME_ID}/replay.json?after_turn=1` },
  { name: 'board-turn-1', target: `/v1/games/${VALID_GAME_ID}/board.json?turn=1` },
  { name: 'board-turn-2', target: `/v1/games/${VALID_GAME_ID}/board.json?turn=2` },
  { name: 'events', target: `/v1/games/${VALID_GAME_ID}/events.json` },
  { name: 'replay-nowin', target: `/v1/games/${NOWIN_GAME_ID}/replay.json` },
  { name: 'events-nowin', target: `/v1/games/${NOWIN_GAME_ID}/events.json` },
  { name: 'replay-interrupted', target: `/v1/games/${INTERRUPTED_GAME_ID}/replay.json` },
];

interface Report {
  /** route → the four legs, in order: cold, warm, warm again, re-cold. */
  readonly derivation: ReadonlyMap<string, ReadonlyArray<Leg>>;
  readonly cacheTreesMatch: ReadonlyArray<readonly [string, ReadonlyArray<string>]>;
  readonly poison: ReadonlyArray<Leg>;
  readonly bursts: ReadonlyArray<readonly [string, ReadonlyArray<Leg>]>;
  readonly mixedConcurrent: ReadonlyArray<Leg>;
  readonly mixedSerial: ReadonlyArray<Leg>;
  readonly mutation: ReadonlyArray<Leg>;
  readonly indexRoundTrip: { readonly python: boolean; readonly typescript: boolean };
  readonly flapUp1: ReadonlyArray<Leg>;
  readonly flapDown: ReadonlyArray<Leg>;
  readonly flapUp2: ReadonlyArray<Leg>;
  readonly offlineIndex: ReadonlyArray<Leg>;
  readonly rebootWarm: ReadonlyArray<Leg>;
  readonly rebootBefore: ReadonlyArray<Leg>;
  readonly cacheSurvivedRestart: ReadonlyArray<string>;
  readonly lockContention: ByImpl<SpawnOutcome>;
  readonly staleTakeover: ByImpl<SpawnOutcome>;
  readonly stalePids: ByImpl<number | null>;
  readonly teardown: ReadonlyArray<JsonObject>;
}

interface HuntState {
  report: Report | null;
  scratches: ReadonlyArray<string>;
}

const state: HuntState = { report: null, scratches: [] };

const report = (): Report => {
  const built = state.report;
  if (built === null) throw new Error('the hunt did not run');
  return built;
};

// ---------------------------------------------------------------------------
// The run
// ---------------------------------------------------------------------------

const mainRoutes = (): ReadonlyArray<readonly [string, string]> => [
  ['index', '/v1/games'],
  ['status', `/v1/games/${VALID_GAME_ID}/status`],
  ['frames', `/v1/games/${VALID_GAME_ID}/frames`],
  ['replay', `/v1/games/${VALID_GAME_ID}/replay.json?limit=5`],
  ['board', `/v1/games/${VALID_GAME_ID}/board.json?turn=1`],
  ['events', `/v1/games/${VALID_GAME_ID}/events.json`],
];

beforeAll(async () => {
  if (!PLATFORM_SUPPORTED) return;
  const scratch = mkdtempSync(join(tmpdir(), 'arena-hunt-state-'));
  state.scratches = [scratch];
  const runsRoot = cloneFixtures(join(scratch, 'runs'));
  const stub: StubHandle = makeStub('not-found-404');
  const teardown: Array<JsonObject> = [];

  const pair = unwrapPair(
    await bootGatewayPair({
      runsRoot,
      serviceUrl: stub.origin,
      scenario: 'hunt',
      scratch: join(scratch, 'gateways'),
      viewerPublicUrl: VIEWER_PUBLIC_URL,
      upstreamTimeoutSeconds: 1,
    }),
  );

  // --- cold / warm / warm again / re-cold --------------------------------
  await pair.freshCaches();
  const derivation = new Map<string, ReadonlyArray<Leg>>();
  await DERIVATION_ROUTES.reduce<Promise<void>>(async (previous, route) => {
    await previous;
    const three = await runSequence(pair, [
      [`${route.name}:cold`, route.target],
      [`${route.name}:warm`, route.target],
      [`${route.name}:warm2`, route.target],
    ]);
    derivation.set(route.name, three);
  }, Promise.resolve());
  /** Names present on either side whose contents are not identical. */
  const treeMismatches = (): ReadonlyArray<string> => {
    const left = cacheTree(pair.python.cacheRoot);
    const right = cacheTree(pair.typescript.cacheRoot);
    return Array.from(new Set([...Object.keys(left), ...Object.keys(right)]))
      .sort()
      .flatMap((name) => (left[name] === right[name] ? [] : [`${name} differs`]));
  };
  const treeAfterWarm = ['after-warm', treeMismatches()] as const;

  await pair.freshCaches();
  await DERIVATION_ROUTES.reduce<Promise<void>>(async (previous, route) => {
    await previous;
    const recold = await runLeg(pair, `${route.name}:recold`, route.target);
    derivation.set(route.name, [...(derivation.get(route.name) ?? []), recold]);
  }, Promise.resolve());

  const cacheTreesMatch: ReadonlyArray<readonly [string, ReadonlyArray<string>]> = [
    ['after-recold', treeMismatches()],
    treeAfterWarm,
  ];

  // --- cache poisoning ----------------------------------------------------
  const turnCacheFiles = (root: string): ReadonlyArray<string> => {
    const directory = join(root, VALID_GAME_ID);
    return statSync(directory, { throwIfNoEntry: false }) === undefined
      ? []
      : readdirSync(directory)
          .filter((name) => name.startsWith('turn-'))
          .map((name) => join(directory, name));
  };
  const bothCaches = (mutate: (root: string) => void): void => {
    [pair.python.cacheRoot, pair.typescript.cacheRoot].forEach(mutate);
  };
  const REPLAY_5 = `/v1/games/${VALID_GAME_ID}/replay.json?limit=5`;
  const BOARD_1 = `/v1/games/${VALID_GAME_ID}/board.json?turn=1`;
  const EVENTS = `/v1/games/${VALID_GAME_ID}/events.json`;

  bothCaches((root) => turnCacheFiles(root).forEach((path) => writeFileSync(path, '{ not json')));
  const poisonGarbage = await runSequence(pair, [
    ['poison-garbage-replay', REPLAY_5],
    ['poison-garbage-board', BOARD_1],
  ]);
  bothCaches((root) => turnCacheFiles(root).forEach((path) => writeFileSync(path, '{"turn": 99999}')));
  const poisonWrongSchema = await runSequence(pair, [
    ['poison-wrong-shape-replay', REPLAY_5],
    ['poison-wrong-shape-board', BOARD_1],
  ]);
  bothCaches((root) => turnCacheFiles(root).forEach((path) => writeFileSync(path, '')));
  const poisonEmpty = await runSequence(pair, [['poison-empty-replay', REPLAY_5]]);
  bothCaches((root) => {
    const path = join(root, VALID_GAME_ID, 'events.json');
    if (statSync(path, { throwIfNoEntry: false }) !== undefined) {
      writeFileSync(path, '{"events": "poisoned"}');
    }
  });
  const poisonEvents = await runSequence(pair, [['poison-events-cache', EVENTS]]);
  bothCaches((root) => rmSync(root, { recursive: true, force: true }));
  const poisonRootGone = await runSequence(pair, [
    ['poison-cache-root-removed-replay', REPLAY_5],
    ['poison-cache-root-removed-events', EVENTS],
  ]);
  await pair.freshCaches();
  bothCaches((root) => chmodSync(root, 0o500));
  const poisonReadOnly = await runSequence(pair, [
    ['poison-read-only-replay', REPLAY_5],
    ['poison-read-only-board', BOARD_1],
    ['poison-read-only-events', EVENTS],
  ]);
  bothCaches((root) => chmodSync(root, 0o755));
  await pair.freshCaches();
  const poisonReseed = await runSequence(pair, [['poison-reseed-replay', REPLAY_5]]);
  const cacheNames = turnCacheFiles(pair.python.cacheRoot).map((path) =>
    path.slice(path.lastIndexOf('/') + 1),
  );
  bothCaches((root) =>
    cacheNames.forEach((name) => {
      const path = join(root, VALID_GAME_ID, name);
      rmSync(path, { force: true });
      mkdirSync(path, { recursive: true });
    }),
  );
  const poisonDirectory = await runSequence(pair, [['poison-cache-file-is-a-directory', REPLAY_5]]);
  bothCaches((root) =>
    cacheNames.forEach((name) =>
      rmSync(join(root, VALID_GAME_ID, name), { recursive: true, force: true }),
    ),
  );

  // --- concurrency --------------------------------------------------------
  await pair.freshCaches();
  const burstBoard = await runBurst(pair, 'burst-board-cold', BOARD_1, 8);
  await pair.freshCaches();
  const burstReplay = await runBurst(pair, 'burst-replay-cold', `/v1/games/${VALID_GAME_ID}/replay.json`, 8);
  await pair.freshCaches();
  const burstEvents = await runBurst(pair, 'burst-events-cold', EVENTS, 8);

  await pair.freshCaches();
  const mixedTargets = mainRoutes();
  const mixedConcurrent = await Promise.all(
    mixedTargets.map(([name, target]) => runLeg(pair, `mixed:${name}`, target)),
  );
  await pair.freshCaches();
  const mixedSerial = await runSequence(
    pair,
    mixedTargets.map(([name, target]) => [`serial:${name}`, target] as const),
  );

  // --- the tree rewritten under a warm gateway ---------------------------
  const INDEX = '/v1/games';
  const baselineIndex = await runLeg(pair, 'mutate:index-baseline', INDEX);
  const validManifestText = readFileSync(manifestPath(runsRoot, VALID_GAME_ID), 'utf8');
  const validManifest = readJson(manifestPath(runsRoot, VALID_GAME_ID));

  writeFileSync(manifestPath(runsRoot, VALID_GAME_ID), validManifestText.slice(0, 400));
  const truncated = await runSequence(pair, [
    ['mutate:index-truncated-manifest', INDEX],
    ['mutate:status-truncated-manifest', `/v1/games/${VALID_GAME_ID}/status`],
    ['mutate:replay-truncated-manifest', REPLAY_5],
    ['mutate:events-truncated-manifest', EVENTS],
    ['mutate:frames-truncated-manifest', `/v1/games/${VALID_GAME_ID}/frames`],
  ]);
  writeFileSync(manifestPath(runsRoot, VALID_GAME_ID), validManifestText);
  const restoredIndex = await runLeg(pair, 'mutate:index-restored', INDEX);

  writeJson(manifestPath(runsRoot, VALID_GAME_ID), {
    ...validManifest,
    game_id: `${VALID_GAME_ID}_moved`,
  });
  const idMismatch = await runSequence(pair, [
    ['mutate:index-id-mismatch', INDEX],
    ['mutate:status-id-mismatch', `/v1/games/${VALID_GAME_ID}/status`],
    ['mutate:status-claimed-id', `/v1/games/${VALID_GAME_ID}_moved/status`],
    ['mutate:replay-id-mismatch', REPLAY_5],
  ]);
  writeJson(manifestPath(runsRoot, VALID_GAME_ID), validManifest);

  cpSync(join(runsRoot, INTERRUPTED_GAME_ID), join(runsRoot, ADDED_GAME_ID), { recursive: true });
  writeJson(manifestPath(runsRoot, ADDED_GAME_ID), {
    ...readJson(manifestPath(runsRoot, ADDED_GAME_ID)),
    game_id: ADDED_GAME_ID,
  });
  const appeared = await runSequence(pair, [
    ['mutate:index-new-run', INDEX],
    ['mutate:status-new-run', `/v1/games/${ADDED_GAME_ID}/status`],
  ]);
  rmSync(join(runsRoot, ADDED_GAME_ID), { recursive: true, force: true });
  const vanished = await runSequence(pair, [
    ['mutate:index-run-removed', INDEX],
    ['mutate:status-run-removed', `/v1/games/${ADDED_GAME_ID}/status`],
  ]);

  renameSync(join(runsRoot, NOWIN_GAME_ID), join(runsRoot, `${NOWIN_GAME_ID}_real`));
  symlinkSync(join(runsRoot, `${NOWIN_GAME_ID}_real`), join(runsRoot, NOWIN_GAME_ID));
  const symlinked = await runSequence(pair, [
    ['mutate:index-symlinked-run', INDEX],
    ['mutate:status-symlinked-run', `/v1/games/${NOWIN_GAME_ID}/status`],
  ]);
  rmSync(join(runsRoot, NOWIN_GAME_ID), { force: true });
  renameSync(join(runsRoot, `${NOWIN_GAME_ID}_real`), join(runsRoot, NOWIN_GAME_ID));

  const framePath = join(runsRoot, VALID_GAME_ID, 'watch_frames', '000000.png');
  const frameBytes = readFileSync(framePath);
  writeFileSync(framePath, Buffer.concat([frameBytes, Buffer.from('PARITYFUZZ')]));
  const frameMutated = await runSequence(
    pair,
    [
      ['mutate:frame-zero-rewritten', `/v1/games/${VALID_GAME_ID}/frames/0.png`],
      ['mutate:frames-after-rewrite', `/v1/games/${VALID_GAME_ID}/frames`],
    ],
    BINARY_ROUTE_ASPECTS,
  );
  writeFileSync(framePath, frameBytes);
  rmSync(join(runsRoot, VALID_GAME_ID, 'watch_frames'), { recursive: true, force: true });
  const framesGone = await runSequence(pair, [
    ['mutate:frames-directory-removed', `/v1/games/${VALID_GAME_ID}/frames`],
    ['mutate:frame-latest-directory-removed', `/v1/games/${VALID_GAME_ID}/frames/latest.png`],
  ]);
  cpSync(
    join(PARITY_RUNS_ROOT, VALID_GAME_ID, 'watch_frames'),
    join(runsRoot, VALID_GAME_ID, 'watch_frames'),
    { recursive: true },
  );

  const savePath = join(runsRoot, VALID_GAME_ID, 'saves', 'turn-0001-auto.sav.gz');
  const saveBytes = readFileSync(savePath);
  writeFileSync(savePath, Buffer.concat([saveBytes, Buffer.from([0x00, 0x01, 0x02])]));
  const staleCache = await runSequence(pair, [['mutate:replay-stale-cache', REPLAY_5]]);
  await pair.freshCaches();
  const coldOnCorrupt = await runSequence(pair, [
    ['mutate:replay-cold-on-corrupt-save', REPLAY_5],
    ['mutate:board-cold-on-corrupt-save', BOARD_1],
  ]);
  writeFileSync(savePath, saveBytes);
  await pair.freshCaches();

  renameSync(join(runsRoot, VALID_GAME_ID, 'saves'), join(runsRoot, VALID_GAME_ID, 'saves_gone'));
  const noSaves = await runSequence(pair, [
    ['mutate:replay-no-saves', REPLAY_5],
    ['mutate:board-no-saves', BOARD_1],
    ['mutate:events-no-saves', EVENTS],
  ]);
  renameSync(join(runsRoot, VALID_GAME_ID, 'saves_gone'), join(runsRoot, VALID_GAME_ID, 'saves'));

  renameSync(runsRoot, `${runsRoot}-parked`);
  const noRunsRoot = await runSequence(pair, [
    ['mutate:index-no-runs-root', INDEX],
    ['mutate:status-no-runs-root', `/v1/games/${VALID_GAME_ID}/status`],
    ['mutate:replay-no-runs-root', REPLAY_5],
  ]);
  renameSync(`${runsRoot}-parked`, runsRoot);
  const finalIndex = await runLeg(pair, 'mutate:index-final', INDEX);

  teardown.push({ phase: 'main', ...(await pair.stop()) });
  pair.cleanup();
  await stub.close();

  // --- the flapping upstream ---------------------------------------------
  const flapRunsRoot = cloneFixtures(join(scratch, 'runs-flap'));
  const upstream = await makeFlapUpstream();
  const flapPair = unwrapPair(
    await bootGatewayPair({
      runsRoot: flapRunsRoot,
      serviceUrl: upstream.origin,
      scenario: 'hunt-flap',
      scratch: join(scratch, 'gateways-flap'),
      viewerPublicUrl: VIEWER_PUBLIC_URL,
      upstreamTimeoutSeconds: 1,
    }),
  );
  const sweep = (): Promise<ReadonlyArray<Leg>> =>
    runSequence(flapPair, mainRoutes(), COMPARED_HEADERS);
  upstream.setMode('up');
  const flapUp1 = await sweep();
  upstream.setMode('reset');
  const flapDown = await sweep();
  upstream.setMode('up');
  const flapUp2 = await sweep();

  upstream.setMode('reset');
  const tornReplay = join(flapRunsRoot, TORN_GAME_ID, 'replay.jsonl');
  // The fixture's final line is deliberately torn, so every append starts with
  // a newline: without it the new row would be welded onto the torn one.
  const tornText = `${readFileSync(tornReplay, 'utf8')}\n`;
  const offlineBaseline = await runLeg(flapPair, 'offline:index-baseline', INDEX);
  const offlineLegs = await [
    ['offline:index-tail-grew', `{"schema_version":1,"game_id":"${TORN_GAME_ID}","turn":9}\n`],
    ['offline:index-negative-turn', `{"schema_version":1,"game_id":"${TORN_GAME_ID}","turn":-3}\n`],
    ['offline:index-float-turn', `{"schema_version":1,"game_id":"${TORN_GAME_ID}","turn":3.0}\n`],
    ['offline:index-string-turn', `{"schema_version":1,"game_id":"${TORN_GAME_ID}","turn":"7"}\n`],
    ['offline:index-array-tail', '["not","a","dict"]\n'],
    ['offline:index-empty-replay', ''],
  ].reduce<Promise<ReadonlyArray<Leg>>>(async (previous, [name, tail]) => {
    const soFar = await previous;
    writeFileSync(tornReplay, tail === '' ? '' : `${tornText}${String(tail)}`);
    return [...soFar, await runLeg(flapPair, String(name), INDEX)];
  }, Promise.resolve([]));
  writeFileSync(tornReplay, tornText);
  const offlineRestored = await runLeg(flapPair, 'offline:index-restored', INDEX);
  const huskReplay = join(flapRunsRoot, HUSK_GAME_ID, 'replay.jsonl');
  writeFileSync(huskReplay, `{"schema_version":1,"game_id":"${HUSK_GAME_ID}","turn":5}\n`);
  const huskWoke = await runLeg(flapPair, 'offline:index-husk-woke-up', INDEX);
  writeFileSync(huskReplay, '');

  teardown.push({ phase: 'flap', ...(await flapPair.stop()) });
  flapPair.cleanup();
  await upstream.close();

  // --- reboot over the same scratch, and the ready-file legs --------------
  const rebootScratch = join(scratch, 'gateways-reboot');
  mkdirSync(rebootScratch, { recursive: true });
  const rebootRunsRoot = cloneFixtures(join(scratch, 'runs-reboot'));
  const rebootStub = makeStub('not-found-404');
  const rebootSpec = {
    runsRoot: rebootRunsRoot,
    serviceUrl: rebootStub.origin,
    scenario: 'hunt-reboot',
    scratch: rebootScratch,
    viewerPublicUrl: VIEWER_PUBLIC_URL,
    upstreamTimeoutSeconds: 1,
  } as const;

  const firstBoot = unwrapPair(await bootGatewayPair(rebootSpec));
  await firstBoot.freshCaches();
  await runSequence(firstBoot, mainRoutes());
  const rebootBefore = await runSequence(firstBoot, mainRoutes());
  const firstStop = await firstBoot.stop();
  teardown.push({ phase: 'reboot-first', ...firstStop });
  const cacheSurvivedRestart = readdirSync(
    join(rebootScratch, 'cache-hunt-reboot-py', VALID_GAME_ID),
  ).sort();

  const secondBoot = unwrapPair(await bootGatewayPair(rebootSpec));
  const rebootWarm = await runSequence(secondBoot, mainRoutes());

  // The incumbent holds its ready file; a second gateway must refuse it.
  const lockContention = {
    python: await spawnAndSettle(
      'python',
      rebootStub.origin,
      rebootRunsRoot,
      secondBoot.python.readyFile,
      join(scratch, 'cache-clash-py'),
      LIVE_PROBE_MS,
    ),
    typescript: await spawnAndSettle(
      'typescript',
      rebootStub.origin,
      rebootRunsRoot,
      secondBoot.typescript.readyFile,
      join(scratch, 'cache-clash-ts'),
      LIVE_PROBE_MS,
    ),
  } satisfies ByImpl<SpawnOutcome>;

  // SIGKILL leaves the record behind with no lock holder; the next gateway
  // must take it over rather than refuse it.
  secondBoot.both.forEach((gateway) => gateway.process.kill('SIGKILL'));
  await Promise.all(secondBoot.both.map((gateway) => gateway.process.exited));
  const stalePids = {
    python: readyPidOf(readyFileFor(rebootScratch, 'hunt-reboot', 'python')),
    typescript: readyPidOf(readyFileFor(rebootScratch, 'hunt-reboot', 'typescript')),
  } satisfies ByImpl<number | null>;
  const staleTakeover = {
    python: await spawnAndSettle(
      'python',
      rebootStub.origin,
      rebootRunsRoot,
      readyFileFor(rebootScratch, 'hunt-reboot', 'python'),
      join(scratch, 'cache-stale-py'),
      LIVE_PROBE_MS,
    ),
    typescript: await spawnAndSettle(
      'typescript',
      rebootStub.origin,
      rebootRunsRoot,
      readyFileFor(rebootScratch, 'hunt-reboot', 'typescript'),
      join(scratch, 'cache-stale-ts'),
      LIVE_PROBE_MS,
    ),
  } satisfies ByImpl<SpawnOutcome>;
  await rebootStub.close();

  state.report = {
    derivation,
    cacheTreesMatch,
    poison: [
      ...poisonGarbage,
      ...poisonWrongSchema,
      ...poisonEmpty,
      ...poisonEvents,
      ...poisonRootGone,
      ...poisonReadOnly,
      ...poisonReseed,
      ...poisonDirectory,
    ],
    bursts: [
      ['board', burstBoard],
      ['replay', burstReplay],
      ['events', burstEvents],
    ],
    mixedConcurrent,
    mixedSerial,
    mutation: [
      baselineIndex,
      ...truncated,
      restoredIndex,
      ...idMismatch,
      ...appeared,
      ...vanished,
      ...symlinked,
      ...frameMutated,
      ...framesGone,
      ...staleCache,
      ...coldOnCorrupt,
      ...noSaves,
      ...noRunsRoot,
      finalIndex,
    ],
    indexRoundTrip: {
      python:
        baselineIndex.python.bodySha === restoredIndex.python.bodySha &&
        baselineIndex.python.bodySha === finalIndex.python.bodySha,
      typescript:
        baselineIndex.typescript.bodySha === restoredIndex.typescript.bodySha &&
        baselineIndex.typescript.bodySha === finalIndex.typescript.bodySha,
    },
    flapUp1,
    flapDown,
    flapUp2,
    offlineIndex: [offlineBaseline, ...offlineLegs, offlineRestored, huskWoke],
    rebootWarm,
    rebootBefore,
    cacheSurvivedRestart,
    lockContention,
    staleTakeover,
    stalePids,
    teardown,
  };
}, SETUP_TIMEOUT_MS);

afterAll(async () => {
  await killAllBooted();
  state.scratches.forEach((path) => rmSync(path, { recursive: true, force: true }));
});

// ---------------------------------------------------------------------------
// The pins
// ---------------------------------------------------------------------------

describe.if(PLATFORM_SUPPORTED)('state and cache fuzz', () => {
  describe('a warm answer is the cold answer', () => {
    DERIVATION_ROUTES.forEach((route) => {
      test(`${route.name} — cold, warm, warm again and re-cold are one body on both sides`, () => {
        const legs = report().derivation.get(route.name) ?? [];
        expect(allDiffs(legs)).toEqual([]);
        expect(new Set(bodyShas(legs, 'python')).size).toBe(1);
        expect(new Set(bodyShas(legs, 'typescript')).size).toBe(1);
        expect(legs.length).toBe(4);
      });
    });

    test('both caches hold the same files with the same contents', () => {
      report().cacheTreesMatch.forEach(([label, problems]) => {
        expect({ label, problems }).toEqual({ label, problems: [] });
      });
    });
  });

  describe('a corrupt cache is re-derived, never trusted and never fatal', () => {
    test('every poisoned-cache leg agrees on both sides', () => {
      expect(allDiffs(report().poison)).toEqual([]);
    });

    test('garbage, wrong-shape, empty and absent caches all answer 200 again', () => {
      const recovered = report()
        .poison.filter((leg) => !leg.name.startsWith('poison-read-only'))
        .map((leg) => ({ leg: leg.name, python: leg.python.status, typescript: leg.typescript.status }));
      recovered.forEach((row) => {
        expect(row).toEqual({ leg: row.leg, python: 200, typescript: 200 });
      });
    });

    test('a read-only cache root is a 503 on both sides, not a crash and not a 500', () => {
      report()
        .poison.filter((leg) => leg.name.startsWith('poison-read-only'))
        .forEach((leg) => {
          expect({ leg: leg.name, python: leg.python.status, typescript: leg.typescript.status }).toEqual(
            { leg: leg.name, python: 503, typescript: 503 },
          );
        });
    });
  });

  describe('concurrency', () => {
    test('eight identical cold derivations answer one body, on both sides', () => {
      report().bursts.forEach(([name, legs]) => {
        expect({
          name,
          diffs: allDiffs(legs),
          python: new Set(bodyShas(legs, 'python')).size,
          typescript: new Set(bodyShas(legs, 'typescript')).size,
        }).toEqual({ name, diffs: [], python: 1, typescript: 1 });
      });
    });

    test('six different derivations in flight at once answer what they answer serially', () => {
      const concurrent = report().mixedConcurrent;
      const serial = report().mixedSerial;
      expect(allDiffs(concurrent)).toEqual([]);
      expect(allDiffs(serial)).toEqual([]);
      expect(bodyShas(concurrent, 'python')).toEqual(bodyShas(serial, 'python'));
      expect(bodyShas(concurrent, 'typescript')).toEqual(bodyShas(serial, 'typescript'));
    });
  });

  describe('the fixture tree rewritten under a running gateway', () => {
    test('every mutation leg agrees on both sides', () => {
      expect(allDiffs(report().mutation)).toEqual([]);
    });

    test('nothing is held in memory: the index round-trips byte for byte', () => {
      expect(report().indexRoundTrip).toEqual({ python: true, typescript: true });
    });

    test('a truncated manifest is a 503 on every route that reads it', () => {
      report()
        .mutation.filter((leg) => leg.name.includes('truncated-manifest') && !leg.name.includes('index'))
        .forEach((leg) => {
          expect({ leg: leg.name, python: leg.python.status, typescript: leg.typescript.status }).toEqual(
            { leg: leg.name, python: 503, typescript: 503 },
          );
        });
    });

    /** A rewritten save under the same filename must invalidate its warm answer. */
    test('a savegame rewritten behind its own cache key is not served from cache', () => {
      const stale = report().mutation.find((leg) => leg.name === 'mutate:replay-stale-cache');
      const cold = report().mutation.find((leg) => leg.name === 'mutate:replay-cold-on-corrupt-save');
      const warm = report().derivation.get('replay-limit-5')?.[1];
      expect({
        movedPython: stale?.python.bodySha !== warm?.python.bodySha,
        movedTypescript: stale?.typescript.bodySha !== warm?.typescript.bodySha,
        matchesColdPython: stale?.python.bodySha === cold?.python.bodySha,
        matchesColdTypescript: stale?.typescript.bodySha === cold?.typescript.bodySha,
      }).toEqual({
        movedPython: true,
        movedTypescript: true,
        matchesColdPython: true,
        matchesColdTypescript: true,
      });
    });
  });

  describe('an upstream that flaps up → down → up', () => {
    test('every leg agrees on both sides, in every mode', () => {
      expect(allDiffs(report().flapUp1)).toEqual([]);
      expect(allDiffs(report().flapDown)).toEqual([]);
      expect(allDiffs(report().flapUp2)).toEqual([]);
    });

    test('nothing sticks: the second up sweep is the first, byte for byte', () => {
      expect(bodyShas(report().flapUp2, 'python')).toEqual(bodyShas(report().flapUp1, 'python'));
      expect(bodyShas(report().flapUp2, 'typescript')).toEqual(
        bodyShas(report().flapUp1, 'typescript'),
      );
    });

    /** `UpstreamUnavailable` relabels orphans and drops husks unlike fallback 404. */
    test('the down sweep takes the relabeling index path on both sides', () => {
      const up = report().flapUp1[0];
      const down = report().flapDown[0];
      expect({
        pythonDiffers: up?.python.bodySha !== down?.python.bodySha,
        typescriptDiffers: up?.typescript.bodySha !== down?.typescript.bodySha,
        agreeUp: up?.python.bodySha === up?.typescript.bodySha,
        agreeDown: down?.python.bodySha === down?.typescript.bodySha,
      }).toEqual({ pythonDiffers: true, typescriptDiffers: true, agreeUp: true, agreeDown: true });
    });

    test('the replay tail decides the relabeled index identically on both sides', () => {
      expect(allDiffs(report().offlineIndex)).toEqual([]);
    });

    test('a turn that is not a positive int drops the row, and a woken husk adds one', () => {
      const byName = new Map(report().offlineIndex.map((leg) => [leg.name, leg] as const));
      const baseline = byName.get('offline:index-baseline');
      const dropped = ['negative-turn', 'float-turn', 'string-turn', 'array-tail', 'empty-replay'].map(
        (suffix) => byName.get(`offline:index-${suffix}`)?.python.bodySha,
      );
      expect(new Set(dropped).size).toBe(1);
      expect(dropped[0]).not.toBe(baseline?.python.bodySha);
      expect(byName.get('offline:index-restored')?.python.bodySha).toBe(baseline?.python.bodySha);
      expect(byName.get('offline:index-husk-woke-up')?.python.bodySha).not.toBe(
        baseline?.python.bodySha,
      );
    });
  });

  describe('a derivation cache that outlives its gateway', () => {
    test('the cache root survives a clean SIGINT with its entries intact', () => {
      expect(report().cacheSurvivedRestart).toEqual([
        'events.json',
        'turn-0000000001-b2eb2020b2b2.json',
        'turn-0000000002-2c30a7a00247.json',
      ]);
    });

    test('the first answer after a restart is the last answer before it', () => {
      expect(allDiffs(report().rebootWarm)).toEqual([]);
      expect(bodyShas(report().rebootWarm, 'python')).toEqual(bodyShas(report().rebootBefore, 'python'));
      expect(bodyShas(report().rebootWarm, 'typescript')).toEqual(
        bodyShas(report().rebootBefore, 'typescript'),
      );
    });
  });

  describe('the ready file is one publisher at a time', () => {
    test('a second gateway onto a held ready file exits 2 on both implementations', () => {
      const { lockContention } = report();
      expect({
        python: { exit: lockContention.python.exitCode, quiet: lockContention.python.stderrEmpty },
        typescript: {
          exit: lockContention.typescript.exitCode,
          quiet: lockContention.typescript.stderrEmpty,
        },
      }).toEqual({
        python: { exit: 2, quiet: false },
        typescript: { exit: 2, quiet: false },
      });
    });

    test('the incumbent’s record is left intact by the refusal', () => {
      const { lockContention, stalePids } = report();
      expect({
        python: lockContention.python.readyExists,
        typescript: lockContention.typescript.readyExists,
      }).toEqual({ python: true, typescript: true });
      expect(stalePids.python).not.toBeNull();
      expect(stalePids.typescript).not.toBeNull();
    });

    test('a stale record left by a SIGKILLed gateway is taken over, not refused', () => {
      const { staleTakeover, stalePids } = report();
      expect({
        python: { signal: staleTakeover.python.signal, ready: staleTakeover.python.readyExists },
        typescript: {
          signal: staleTakeover.typescript.signal,
          ready: staleTakeover.typescript.readyExists,
        },
      }).toEqual({
        python: { signal: null, ready: true },
        typescript: { signal: null, ready: true },
      });
      expect(staleTakeover.python.readyPid).not.toBe(stalePids.python);
      expect(staleTakeover.typescript.readyPid).not.toBe(stalePids.typescript);
    });
  });

  test('every gateway this file spawned exited cleanly and left no orphan', () => {
    report().teardown.forEach((stop) => {
      expect({ phase: stop['phase'], exit: stop['exitCodes'], orphans: stop['orphans'] }).toEqual({
        phase: stop['phase'],
        exit: { python: 0, typescript: 0 },
        orphans: [],
      });
    });
  });
});
