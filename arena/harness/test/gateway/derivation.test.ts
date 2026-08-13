/** Derivation semaphore, bridge transport, classification, and real-loader coverage. */
import { afterAll, describe, expect, test } from 'bun:test';
import {
  chmodSync,
  copyFileSync,
  mkdirSync,
  mkdtempSync,
  readdirSync,
  rmSync,
  statSync,
  writeFileSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { CANON_UTF8, canonicalBytes } from '@arena/wire';
import { Deferred, Duration, Effect, Either, Fiber, Layer, Ref } from 'effect';
import {
  DERIVE_CLI_MODULE,
  type DerivationError,
  type DerivationRequest,
  DerivationUnavailable,
  derivationArgv,
  derivationCacheDirectory,
  layerFromRunner,
  pythonDerivationRunner,
  ReplayDerivation,
  ReplayDerivationPython,
} from 'src/gateway/services/derivation';
import {
  type DerivationFixture,
  derivationFixture,
  derivationRequestKey,
  ReplayDerivationFixture,
  ReplayDerivationUnavailable,
} from './support/derivation.ts';

// ---------------------------------------------------------------------------
// Rig
// ---------------------------------------------------------------------------

const REPO_ROOT = resolve(import.meta.dir, '../../../..');

/**
 * Throwaway cache roots, owned by this file and removed in `afterAll`.  Kept
 * in a closure rather than a module-level array so nothing else can reach the
 * list and so the only writes this suite makes are provably inside it.
 */
const scratch = ((): {
  readonly directory: (label: string) => string;
  readonly cleanup: () => void;
} => {
  const paths: Array<string> = [];
  return {
    directory: (label) => {
      const path = mkdtempSync(join(tmpdir(), `derivation-${label}-`));
      paths.push(path);
      return path;
    },
    cleanup: () => {
      paths.forEach((path) => {
        rmSync(path, { recursive: true, force: true });
      });
    },
  };
})();

const scratchDirectory = (label: string): string => scratch.directory(label);

afterAll(() => {
  scratch.cleanup();
});

const provide = <A, E>(
  effect: Effect.Effect<A, E, ReplayDerivation>,
  layer: Layer.Layer<ReplayDerivation>,
): Promise<A> => Effect.runPromise(Effect.provide(effect, layer));

const attempt = <A, E>(
  effect: Effect.Effect<A, E, ReplayDerivation>,
  layer: Layer.Layer<ReplayDerivation>,
): Promise<{ readonly _tag: 'Left'; readonly left: E } | { readonly _tag: 'Right' }> =>
  Effect.runPromise(
    Effect.provide(
      Effect.match(effect, {
        onFailure: (left) => ({ _tag: 'Left', left }) as const,
        onSuccess: () => ({ _tag: 'Right' }) as const,
      }),
      layer,
    ),
  );

// ---------------------------------------------------------------------------
// Request keys
// ---------------------------------------------------------------------------

const GAME_ID = 'game_derivationtestidentifier';

describe('request keys', () => {
  test('each operation keys on exactly the arguments the loader takes', () => {
    expect(
      derivationRequestKey({
        operation: 'replay',
        gameId: GAME_ID,
        places: [],
        afterTurn: 12,
        limit: 7,
        complete: true,
      }),
    ).toBe(`replay:${GAME_ID}:12:7:true`);
    expect(
      derivationRequestKey({ operation: 'board', gameId: GAME_ID, places: [], turn: 3 }),
    ).toBe(`board:${GAME_ID}:3`);
    expect(
      derivationRequestKey({
        operation: 'events',
        gameId: GAME_ID,
        places: [],
        complete: false,
      }),
    ).toBe(`events:${GAME_ID}:false`);
  });
});

// ---------------------------------------------------------------------------
// Fixture layer: the logic
// ---------------------------------------------------------------------------

describe('fixture layer', () => {
  const layer = ReplayDerivationFixture(
    derivationFixture({
      [`replay:${GAME_ID}:0:250:true`]: { available: true, snapshots: [] },
      [`board:${GAME_ID}:1`]: { turn: 1 },
      [`events:${GAME_ID}:false`]: { events: [], complete: false },
    }),
  );

  test('each method routes to its own key', async () => {
    expect(
      await provide(
        Effect.flatMap(ReplayDerivation, (service) =>
          service.replay({ gameId: GAME_ID, afterTurn: 0, limit: 250, complete: true }),
        ),
        layer,
      ),
    ).toEqual({ available: true, snapshots: [] });
    expect(
      await provide(
        Effect.flatMap(ReplayDerivation, (service) => service.board({ gameId: GAME_ID, turn: 1 })),
        layer,
      ),
    ).toEqual({ turn: 1 });
    expect(
      await provide(
        Effect.flatMap(ReplayDerivation, (service) =>
          service.events({ gameId: GAME_ID, complete: false }),
        ),
        layer,
      ),
    ).toEqual({ events: [], complete: false });
  });

  test('a missing fixture fails the way a missing artifact fails', async () => {
    const outcome = await attempt(
      Effect.flatMap(ReplayDerivation, (service) => service.board({ gameId: GAME_ID, turn: 99 })),
      layer,
    );
    expect(outcome._tag).toBe('Left');
    if (outcome._tag === 'Left') {
      expect(outcome.left._tag).toBe('DerivationArtifactsMissing');
      expect(outcome.left.operation).toBe('board');
    }
  });

  test('the request reaching the runner is fully defaulted and complete', async () => {
    const seen: Array<DerivationRequest> = [];
    const recording = layerFromRunner((request) => {
      seen.push(request);
      return Effect.succeed({ ok: true });
    });
    await provide(
      Effect.flatMap(ReplayDerivation, (service) =>
        Effect.all([
          service.events({ gameId: GAME_ID, complete: true }),
          service.replay({
            gameId: GAME_ID,
            places: [{ place: 1, seat_id: 'seat-1' }],
            afterTurn: 4,
            limit: 9,
            complete: false,
          }),
        ]),
      ),
      recording,
    );
    // `places` omitted becomes `[]` — the loaders' own default (`()`), not
    // `undefined`, which Python would reject as "must be a sequence".
    expect(seen[0]).toEqual({
      operation: 'events',
      gameId: GAME_ID,
      places: [],
      complete: true,
    });
    // ...and when supplied it reaches the runner untouched: the events cache
    // keys on a digest of exactly these rows.
    expect(seen[1]).toEqual({
      operation: 'replay',
      gameId: GAME_ID,
      places: [{ place: 1, seat_id: 'seat-1' }],
      afterTurn: 4,
      limit: 9,
      complete: false,
    });
  });
});

// ---------------------------------------------------------------------------
// The semaphore — the property Python never tests
// ---------------------------------------------------------------------------

describe('single flight (replay_lock)', () => {
  test('a second derivation cannot start while the first is in flight', async () => {
    const program = Effect.gen(function* () {
      const firstStarted = yield* Deferred.make<void>();
      const gate = yield* Deferred.make<void>();
      const secondStarted = yield* Ref.make(false);

      const entries: DerivationFixture = new Map([
        [
          `board:${GAME_ID}:1`,
          Effect.zipRight(
            Deferred.succeed(firstStarted, undefined),
            Effect.as(Deferred.await(gate), { turn: 1 }),
          ),
        ],
        [
          `board:${GAME_ID}:2`,
          Effect.as(Ref.set(secondStarted, true), { turn: 2 }),
        ],
      ]);

      const service = yield* Effect.provide(ReplayDerivation, ReplayDerivationFixture(entries));

      const first = yield* Effect.fork(service.board({ gameId: GAME_ID, turn: 1 }));
      yield* Deferred.await(firstStarted);
      const second = yield* Effect.fork(service.board({ gameId: GAME_ID, turn: 2 }));

      // The second derivation is runnable and its fixture is instant; if the
      // semaphore were missing it would have set the flag by now.
      yield* Effect.sleep(Duration.millis(60));
      const startedWhileHeld = yield* Ref.get(secondStarted);

      yield* Deferred.succeed(gate, undefined);
      const firstValue = yield* Fiber.join(first);
      const secondValue = yield* Fiber.join(second);
      const startedAfterRelease = yield* Ref.get(secondStarted);

      return { startedWhileHeld, startedAfterRelease, firstValue, secondValue };
    });

    const result = await Effect.runPromise(program);
    expect(result.startedWhileHeld).toBe(false);
    expect(result.startedAfterRelease).toBe(true);
    expect(result.firstValue).toEqual({ turn: 1 });
    expect(result.secondValue).toEqual({ turn: 2 });
  });

  test('the lock spans all three operations, not one per method', async () => {
    const program = Effect.gen(function* () {
      const active = yield* Ref.make(0);
      const peak = yield* Ref.make(0);

      const busy = <A extends { readonly [key: string]: number }>(
        value: A,
      ): Effect.Effect<A> =>
        Effect.acquireUseRelease(
          Effect.flatMap(
            Ref.updateAndGet(active, (count) => count + 1),
            (count) => Effect.as(Ref.update(peak, (seen) => Math.max(seen, count)), count),
          ),
          () => Effect.as(Effect.sleep(Duration.millis(15)), value),
          () => Ref.update(active, (count) => count - 1),
        );

      const entries: DerivationFixture = new Map([
        [`replay:${GAME_ID}:0:250:true`, busy({ kind: 1 })],
        [`board:${GAME_ID}:1`, busy({ kind: 2 })],
        [`events:${GAME_ID}:true`, busy({ kind: 3 })],
      ]);

      const service = yield* Effect.provide(ReplayDerivation, ReplayDerivationFixture(entries));
      const values = yield* Effect.all(
        [
          service.replay({ gameId: GAME_ID, afterTurn: 0, limit: 250, complete: true }),
          service.board({ gameId: GAME_ID, turn: 1 }),
          service.events({ gameId: GAME_ID, complete: true }),
        ],
        { concurrency: 'unbounded' },
      );
      return { peak: yield* Ref.get(peak), values };
    });

    const result = await Effect.runPromise(program);
    expect(result.peak).toBe(1);
    expect(result.values).toEqual([{ kind: 1 }, { kind: 2 }, { kind: 3 }]);
  });
});

// ---------------------------------------------------------------------------
// The unavailable layer
// ---------------------------------------------------------------------------

describe('unavailable layer', () => {
  test('all three derivations are typed 503s', async () => {
    const calls: ReadonlyArray<
      readonly ['replay' | 'board' | 'events', Effect.Effect<unknown, DerivationError, ReplayDerivation>]
    > = [
      [
        'replay',
        Effect.flatMap(ReplayDerivation, (service) =>
          service.replay({ gameId: GAME_ID, afterTurn: 0, limit: 1, complete: false }),
        ),
      ],
      ['board', Effect.flatMap(ReplayDerivation, (service) => service.board({ gameId: GAME_ID, turn: 1 }))],
      [
        'events',
        Effect.flatMap(ReplayDerivation, (service) =>
          service.events({ gameId: GAME_ID, complete: false }),
        ),
      ],
    ];
    const failures = await Promise.all(
      calls.map(async ([operation, effect]) => ({
        operation,
        outcome: await attempt(effect, ReplayDerivationUnavailable),
      })),
    );
    failures.forEach(({ operation, outcome }) => {
      expect(outcome._tag).toBe('Left');
      if (outcome._tag === 'Left') {
        expect(outcome.left._tag).toBe('DerivationUnavailable');
        expect(outcome.left.operation).toBe(operation);
      }
    });
  });
});

// ---------------------------------------------------------------------------
// Python process transport
// ---------------------------------------------------------------------------

const transportRequest = (places: DerivationRequest['places'] = []): DerivationRequest => ({
  operation: 'board',
  gameId: GAME_ID,
  places,
  turn: 1,
});

const transportOptions = (python: string, timeout?: Duration.DurationInput) => ({
  repoRoot: REPO_ROOT,
  runsRoot: '/runs',
  cacheRoot: '/cache',
  python,
  ...(timeout === undefined ? {} : { timeout }),
});

const executableFixture = (source: string): string => {
  const directory = scratchDirectory('transport');
  const path = join(directory, 'python-fixture');
  writeFileSync(path, `#!/usr/bin/env python3\n${source}\n`, 'utf8');
  chmodSync(path, 0o700);
  return path;
};

describe('python bridge transport failures stay typed', () => {
  test('spawn failure is DerivationUnavailable, never a defect', async () => {
    const failure = await Effect.runPromise(
      Effect.flip(
        pythonDerivationRunner(transportOptions('/definitely/missing/arena-python'))(
          transportRequest(),
        ),
      ),
    );
    expect(failure).toBeInstanceOf(DerivationUnavailable);
    expect(failure.detail).toContain('spawn failed');
  });

  test('a closed child stdin is DerivationUnavailable, never a defect', async () => {
    const python = executableFixture('import os, time\nos.close(0)\ntime.sleep(0.2)');
    const padding = 'x'.repeat(1024);
    const places = Array.from({ length: 20_000 }, (_, place) => ({ place, padding }));
    const failure = await Effect.runPromise(
      Effect.flip(
        pythonDerivationRunner(transportOptions(python, '5 seconds'))(
          transportRequest(places),
        ),
      ),
    );
    expect(failure).toBeInstanceOf(DerivationUnavailable);
    expect(failure.detail).toContain('stdin write failed');
  });

  test('timeout is opt-in: omitted waits, configured timeout fails with a typed value', async () => {
    const python = executableFixture(
      'import sys, time\nsys.stdin.buffer.read()\ntime.sleep(0.08)\nsys.stdout.write("{}")',
    );
    const withoutTimeout = pythonDerivationRunner(transportOptions(python))(transportRequest());
    expect(await Effect.runPromise(withoutTimeout)).toEqual({});

    const failure = await Effect.runPromise(
      Effect.flip(
        pythonDerivationRunner(transportOptions(python, '10 millis'))(transportRequest()),
      ),
    );
    expect(failure).toBeInstanceOf(DerivationUnavailable);
    expect(failure.detail).toBe('derivation timed out');
  });
});

// ---------------------------------------------------------------------------
// The Python bridge, against a real run directory
// ---------------------------------------------------------------------------

const FIXTURE_GAME_ID = 'game_mEUltpqtzauPGfjI9IlhWJ5x';
const FIXTURE_TURN = 52;
const FIXTURE_SAVE = join(
  REPO_ROOT,
  'agent_eval',
  'tests',
  'fixtures',
  'incidents',
  FIXTURE_GAME_ID,
  'saves',
  'turn-0052-auto.sav.gz',
);

interface RunFixture {
  readonly gameId: string;
  readonly turn: number;
}

/**
 * Build the smallest real run from a committed save. The bridge differential
 * must run in a clean checkout rather than depending on a developer's ignored
 * `.agent-eval/runs` directory.
 */
const makeCommittedRun = (): { readonly fixture: RunFixture; readonly runsRoot: string } => {
  const runsRoot = scratchDirectory('committed-run');
  const saves = join(runsRoot, FIXTURE_GAME_ID, 'saves');
  mkdirSync(saves, { recursive: true });
  copyFileSync(FIXTURE_SAVE, join(saves, 'turn-0052-auto.sav.gz'));
  return { fixture: { gameId: FIXTURE_GAME_ID, turn: FIXTURE_TURN }, runsRoot };
};

const COMMITTED_RUN = makeCommittedRun();
const RUN: RunFixture = COMMITTED_RUN.fixture;

const DIRECT_PY = [
  'import json, os, sys',
  'from agent_eval.save_replay import board_from_autosave, replay_from_autosaves',
  'from agent_eval.game_events import events_from_autosaves',
  'op = os.environ["DERIVE_OP"]',
  'runs = os.environ["DERIVE_RUNS"]',
  'game = os.environ["DERIVE_GAME"]',
  'cache = os.environ["DERIVE_CACHE"]',
  'places = json.loads(os.environ["DERIVE_PLACES"])',
  'complete = os.environ["DERIVE_COMPLETE"] == "1"',
  'if op == "replay":',
  '    value = replay_from_autosaves(',
  '        runs, game, places, after_turn=int(os.environ["DERIVE_AFTER"]),',
  '        limit=int(os.environ["DERIVE_LIMIT"]), cache_root=cache, complete=complete)',
  'elif op == "board":',
  '    value = board_from_autosave(',
  '        runs, game, places, turn=int(os.environ["DERIVE_TURN"]), cache_root=cache)',
  'else:',
  '    value = events_from_autosaves(runs, game, places, cache_root=cache, complete=complete)',
  'sys.stdout.write(json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False))',
].join('\n');

interface DirectRequest {
  readonly operation: 'replay' | 'board' | 'events';
  readonly gameId: string;
  readonly turn: number;
  readonly cacheRoot: string;
}

/** The oracle: the loaders called straight, canonicalized exactly as the gateway does. */
const directDerivation = (request: DirectRequest): Uint8Array<ArrayBuffer> => {
  const child = Bun.spawnSync(['python3', '-c', DIRECT_PY], {
    cwd: REPO_ROOT,
    env: {
      ...process.env,
      DERIVE_OP: request.operation,
      DERIVE_RUNS: COMMITTED_RUN.runsRoot,
      DERIVE_GAME: request.gameId,
      DERIVE_CACHE: request.cacheRoot,
      DERIVE_PLACES: '[]',
      DERIVE_COMPLETE: '1',
      DERIVE_AFTER: '0',
      DERIVE_LIMIT: '250',
      DERIVE_TURN: String(request.turn),
    },
    stdout: 'pipe',
    stderr: 'pipe',
  });
  expect(child.stderr.toString()).toBe('');
  expect(child.exitCode).toBe(0);
  // Copied rather than viewed: `spawnSync` hands back a `Buffer`, whose
  // backing store is typed `ArrayBufferLike` and will not compare against the
  // `ArrayBuffer`-backed bytes read off a live stream.
  return Uint8Array.from(child.stdout);
};

test('the committed bridge fixture is present', () => {
  expect(statSync(FIXTURE_SAVE).isFile()).toBe(true);
});

describe('python bridge', () => {
  const run: RunFixture = RUN;

  test('argv is exactly the bridge CLI surface', () => {
    const options = { repoRoot: REPO_ROOT, runsRoot: COMMITTED_RUN.runsRoot, cacheRoot: '/cache' };
    expect(
      derivationArgv(options, {
        operation: 'replay',
        gameId: run.gameId,
        places: [],
        afterTurn: 2,
        limit: 3,
        complete: true,
      }),
    ).toEqual([
      'python3',
      '-m',
      DERIVE_CLI_MODULE,
      '--op',
      'replay',
      '--runs-root',
      COMMITTED_RUN.runsRoot,
      '--game-id',
      run.gameId,
      '--cache-root',
      '/cache',
      '--after-turn',
      '2',
      '--limit',
      '3',
      '--complete',
    ]);
    expect(
      derivationArgv(options, {
        operation: 'events',
        gameId: run.gameId,
        places: [],
        complete: false,
      }).includes('--complete'),
    ).toBe(false);
    expect(
      derivationArgv(options, { operation: 'board', gameId: run.gameId, places: [], turn: 5 }),
    ).toEqual([
      'python3',
      '-m',
      DERIVE_CLI_MODULE,
      '--op',
      'board',
      '--runs-root',
      COMMITTED_RUN.runsRoot,
      '--game-id',
      run.gameId,
      '--cache-root',
      '/cache',
      '--turn',
      '5',
    ]);
  });

  test(
    'the service returns what the loaders return, cached into a throwaway root',
    async () => {
      const serviceCache = scratchDirectory('service');
      const oracleCache = scratchDirectory('oracle');
      const layer = ReplayDerivationPython({
        repoRoot: REPO_ROOT,
        runsRoot: COMMITTED_RUN.runsRoot,
        cacheRoot: serviceCache,
        timeout: '90 seconds',
      });

      const values = await provide(
        Effect.flatMap(ReplayDerivation, (service) =>
          Effect.all([
            service.replay({ gameId: run.gameId, afterTurn: 0, limit: 250, complete: true }),
            service.board({ gameId: run.gameId, turn: run.turn }),
            service.events({ gameId: run.gameId, complete: true }),
          ]),
        ),
        layer,
      );

      const oracle = (['replay', 'board', 'events'] as const).map((operation) =>
        directDerivation({
          operation,
          gameId: run.gameId,
          turn: run.turn,
          cacheRoot: oracleCache,
        }),
      );

      // Byte parity, not `JSON.parse` equality.  The bridge prints
      // `json.dumps(sort_keys=True, separators=(",", ":"), ensure_ascii=False)`
      // — `_canonical` exactly — so re-canonicalizing what the service read has
      // to reproduce those bytes.  Comparing parsed documents is what the
      // previous version of this test did, and it cannot see the defect it
      // needs to see: `JSON.parse` on both sides agrees that `1` and `1.0` are
      // the same value, which is how `replay.json` shipped every Python `int`
      // as a float.
      expect(
        values.map((value) =>
          Either.getOrThrowWith(
            canonicalBytes(value, CANON_UTF8),
            (error) => new Error(`derivation is not canonicalizable: ${error._tag}`),
          ),
        ),
      ).toEqual(oracle);

      // The cache landed where Python puts it, and nowhere near the run.
      expect(readdirSync(derivationCacheDirectory(serviceCache, run.gameId))).toContain(
        'events.json',
      );
    },
    180_000,
  );

  test(
    'a missing board turn is the 404 classification, not a crash',
    async () => {
      const layer = ReplayDerivationPython({
        repoRoot: REPO_ROOT,
        runsRoot: COMMITTED_RUN.runsRoot,
        cacheRoot: scratchDirectory('missing'),
        timeout: '90 seconds',
      });
      const outcome = await attempt(
        Effect.flatMap(ReplayDerivation, (service) =>
          service.board({ gameId: run.gameId, turn: 999_999 }),
        ),
        layer,
      );
      expect(outcome._tag).toBe('Left');
      if (outcome._tag === 'Left') {
        expect(outcome.left._tag).toBe('DerivationArtifactsMissing');
        expect(outcome.left.operation).toBe('board');
      }
    },
    120_000,
  );

  test(
    'a rejected game id is the 503 classification, and its text stays private',
    async () => {
      const layer = ReplayDerivationPython({
        repoRoot: REPO_ROOT,
        runsRoot: COMMITTED_RUN.runsRoot,
        cacheRoot: scratchDirectory('rejected'),
        timeout: '90 seconds',
      });
      const outcome = await attempt(
        Effect.flatMap(ReplayDerivation, (service) =>
          service.events({ gameId: 'short', complete: false }),
        ),
        layer,
      );
      expect(outcome._tag).toBe('Left');
      if (outcome._tag === 'Left') {
        expect(outcome.left._tag).toBe('DerivationUnavailable');
        expect(outcome.left.operation).toBe('events');
        expect(outcome.left.detail).toContain('Invalid game id');
      }
    },
    120_000,
  );
});

// ---------------------------------------------------------------------------
// Additivity
// ---------------------------------------------------------------------------

describe('additivity', () => {
  test('no existing agent_eval module imports the interim bridge', () => {
    const child = Bun.spawnSync(
      [
        'grep',
        '-rn',
        '--include=*.py',
        '--exclude=replay_derive_cli.py',
        'replay_derive_cli',
        join(REPO_ROOT, 'agent_eval'),
      ],
      { stdout: 'pipe', stderr: 'pipe' },
    );
    // grep exits 1 with no output when nothing matches — that is the pass.
    expect(child.stdout.toString()).toBe('');
    expect(child.exitCode).toBe(1);
  });
});
