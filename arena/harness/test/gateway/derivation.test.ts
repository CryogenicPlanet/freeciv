/**
 * `ReplayDerivation`: the mutex, the error classification, and the interim
 * Python bridge.
 *
 * Three things are pinned here that the Python suite never pins:
 *
 * 1. **Single flight.** `replay_lock` has no concurrency test in
 *    `agent_eval/tests/test_replay_gateway.py` at all (dossier §9, U-C3), yet
 *    it is the reason the savegame parsers do not corrupt each other's cache.
 *    A gated fixture proves the second derivation cannot start while the first
 *    is in flight.
 * 2. **The classification.** `FileNotFoundError` → 404, everything else → 503,
 *    with the per-operation message from `@arena/wire`.
 * 3. **The bridge is faithful.** `python3 -m agent_eval.replay_derive_cli`
 *    returns byte-identical JSON to calling `save_replay` / `game_events`
 *    directly, against a real run directory, read-only, cached into a
 *    throwaway directory rather than the real cache.
 *
 * Every process this file starts is its own; the user's stack is never
 * contacted, and the only writes are inside `mkdtemp` directories removed in
 * `afterAll`.
 */
import { afterAll, describe, expect, test } from 'bun:test';
import { mkdtempSync, readdirSync, rmSync, statSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { CANON_UTF8, canonicalBytes } from '@arena/wire';
import { GATEWAY_PROBLEM_MESSAGES } from '@arena/wire/gateway';
import { Deferred, Duration, Effect, Either, Fiber, Layer, Ref } from 'effect';
import {
  DERIVATION_EVENTS_CACHE_NAME,
  DERIVE_CLI_MODULE,
  DerivationArtifactsMissing,
  type DerivationError,
  type DerivationFixture,
  type DerivationRequest,
  DerivationUnavailable,
  derivationArgv,
  derivationCacheDirectory,
  derivationFixture,
  derivationProblem,
  derivationRequestKey,
  derivationTurnCacheName,
  layerFromRunner,
  ReplayDerivation,
  ReplayDerivationFixture,
  ReplayDerivationPython,
  ReplayDerivationUnavailable,
} from 'src/gateway/services/derivation';

// ---------------------------------------------------------------------------
// Rig
// ---------------------------------------------------------------------------

const REPO_ROOT = resolve(import.meta.dir, '../../../..');
const RUNS_ROOT = join(REPO_ROOT, '.agent-eval', 'runs');

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
// Request keys and problem mapping
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

describe('problem mapping', () => {
  const missing = (operation: 'replay' | 'board' | 'events'): DerivationError =>
    new DerivationArtifactsMissing({ operation, gameId: GAME_ID, detail: 'private path' });
  const unavailable = (operation: 'replay' | 'board' | 'events'): DerivationError =>
    new DerivationUnavailable({ operation, gameId: GAME_ID, detail: 'private corrupt details' });

  test('FileNotFoundError is the route 404, per operation', () => {
    expect(derivationProblem(missing('replay'))).toEqual({
      status: 404,
      message: GATEWAY_PROBLEM_MESSAGES.replayArtifactsNotFound,
    });
    expect(derivationProblem(missing('board'))).toEqual({
      status: 404,
      message: GATEWAY_PROBLEM_MESSAGES.boardSnapshotNotFound,
    });
    expect(derivationProblem(missing('events'))).toEqual({
      status: 404,
      message: GATEWAY_PROBLEM_MESSAGES.eventArtifactsNotFound,
    });
  });

  test('everything else is the route 503, per operation', () => {
    expect(derivationProblem(unavailable('replay'))).toEqual({
      status: 503,
      message: GATEWAY_PROBLEM_MESSAGES.replayTelemetryUnavailable,
    });
    expect(derivationProblem(unavailable('board'))).toEqual({
      status: 503,
      message: GATEWAY_PROBLEM_MESSAGES.boardSnapshotUnavailable,
    });
    expect(derivationProblem(unavailable('events'))).toEqual({
      status: 503,
      message: GATEWAY_PROBLEM_MESSAGES.eventsUnavailable,
    });
  });

  test('the private detail never appears in the public problem', () => {
    const rendered = JSON.stringify([
      derivationProblem(missing('events')),
      derivationProblem(unavailable('events')),
    ]);
    expect(rendered).not.toContain('private');
  });
});

// ---------------------------------------------------------------------------
// Cache layout — mirrored from Python, checked against Python
// ---------------------------------------------------------------------------

describe('cache layout', () => {
  test('the per-game directory and the events file match `_cache_directory`', () => {
    expect(derivationCacheDirectory('/cache', GAME_ID)).toBe(`/cache/${GAME_ID}`);
    expect(derivationCacheDirectory('/cache/', GAME_ID)).toBe(`/cache/${GAME_ID}`);
    expect(DERIVATION_EVENTS_CACHE_NAME).toBe('events.json');
  });

  test('the per-turn name is byte-identical to `save_replay._cache_path`', () => {
    const cases: ReadonlyArray<readonly [number, string]> = [
      [0, 'turn-0000-auto.sav.gz'],
      [1, 'turn-0001-auto.sav.gz'],
      [4321, 'turn-4321-final.sav'],
      [7, 'turn-0007-M-bc--tuZ1Pall.map.ppm'],
    ];
    const source = [
      'import json, os, sys',
      'from pathlib import Path',
      'from agent_eval.save_replay import _cache_path',
      'cases = json.loads(os.environ["CACHE_CASES"])',
      'sys.stdout.write(json.dumps([',
      '  _cache_path(Path("/cache/game"), int(turn), name).name for turn, name in cases',
      ']))',
    ].join('\n');
    const child = Bun.spawnSync(['python3', '-c', source], {
      cwd: REPO_ROOT,
      env: { ...process.env, CACHE_CASES: JSON.stringify(cases) },
      stdout: 'pipe',
      stderr: 'pipe',
    });
    expect(child.stderr.toString()).toBe('');
    expect(child.exitCode).toBe(0);
    expect(JSON.parse(child.stdout.toString())).toEqual(
      cases.map(([turn, name]) => derivationTurnCacheName(turn, name)),
    );
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
      expect(derivationProblem(outcome.left)).toEqual({
        status: 404,
        message: GATEWAY_PROBLEM_MESSAGES.boardSnapshotNotFound,
      });
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
      readonly [string, Effect.Effect<unknown, DerivationError, ReplayDerivation>]
    > = [
      [
        GATEWAY_PROBLEM_MESSAGES.replayTelemetryUnavailable,
        Effect.flatMap(ReplayDerivation, (service) =>
          service.replay({ gameId: GAME_ID, afterTurn: 0, limit: 1, complete: false }),
        ),
      ],
      [
        GATEWAY_PROBLEM_MESSAGES.boardSnapshotUnavailable,
        Effect.flatMap(ReplayDerivation, (service) => service.board({ gameId: GAME_ID, turn: 1 })),
      ],
      [
        GATEWAY_PROBLEM_MESSAGES.eventsUnavailable,
        Effect.flatMap(ReplayDerivation, (service) =>
          service.events({ gameId: GAME_ID, complete: false }),
        ),
      ],
    ];
    const problems = await Promise.all(
      calls.map(async ([message, effect]) => {
        const outcome = await attempt(effect, ReplayDerivationUnavailable);
        return { message, outcome };
      }),
    );
    problems.forEach(({ message, outcome }) => {
      expect(outcome._tag).toBe('Left');
      if (outcome._tag === 'Left') {
        expect(derivationProblem(outcome.left)).toEqual({ status: 503, message });
      }
    });
  });
});

// ---------------------------------------------------------------------------
// The Python bridge, against a real run directory
// ---------------------------------------------------------------------------

const SAVE_NAME_RE = /^turn-(\d{4,})-.*\.sav(\.gz|\.bz2|\.xz|\.zst)?$/;

interface RunFixture {
  readonly gameId: string;
  readonly turn: number;
}

/**
 * The smallest run on this machine that has savegames — smallest so the smoke
 * test stays fast, and chosen by scanning rather than by hard-coded id so the
 * suite is not pinned to one developer's `.agent-eval`.
 */
const discoverRun = (): RunFixture | undefined => {
  const candidates = readdirSync(RUNS_ROOT, { withFileTypes: true })
    .filter((entry) => entry.isDirectory())
    .flatMap((entry) => {
      const saves = join(RUNS_ROOT, entry.name, 'saves');
      const names = statSync(saves, { throwIfNoEntry: false })?.isDirectory() === true
        ? readdirSync(saves)
        : [];
      const turns = names.flatMap((name) => {
        const match = SAVE_NAME_RE.exec(name);
        return match?.[1] === undefined ? [] : [Number(match[1])];
      });
      return turns.length === 0
        ? []
        : [{ gameId: entry.name, turn: Math.min(...turns), count: turns.length }];
    })
    .toSorted((left, right) => left.count - right.count || left.gameId.localeCompare(right.gameId));
  const chosen = candidates[0];
  return chosen === undefined ? undefined : { gameId: chosen.gameId, turn: chosen.turn };
};

const runsRootExists = statSync(RUNS_ROOT, { throwIfNoEntry: false })?.isDirectory() === true;
const RUN = runsRootExists ? discoverRun() : undefined;

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
      DERIVE_RUNS: RUNS_ROOT,
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

/**
 * Set `ARENA_REQUIRE_PARITY=1` to make a skipped bridge differential a
 * **failure** rather than a silent pass.
 *
 * `discoverRun()` scans `<repo>/.agent-eval/runs/*&#47;saves` for a real
 * `turn-NNNN-*.sav*`, and a fresh clone has none — so on a clean checkout the
 * whole `python bridge` block, which is this file's only evidence that the
 * bridge is *faithful*, disappears while `bun test` still prints `N pass, 0
 * fail`.  The degraded case is otherwise indistinguishable from the healthy
 * one, so it is announced.
 */
const REQUIRE_PARITY = process.env['ARENA_REQUIRE_PARITY'] !== undefined;

test('the python bridge differential is not silently skipped', () => {
  if (RUN === undefined) {
    // oxlint-disable-next-line effecttsgo/global-console -- a skipped oracle has
    // to reach a terminal; there is no Logger in a bun:test process.
    console.warn(
      `\n!! python bridge differential DID NOT RUN: no savegame under ${RUNS_ROOT}.\n` +
        '!! The claim "the bridge is faithful" contributed ZERO assertions to this run.\n' +
        '!! Set ARENA_REQUIRE_PARITY=1 to make this a failure.\n',
    );
  }
  expect({ ranBridgeDifferential: RUN !== undefined || !REQUIRE_PARITY }).toEqual({
    ranBridgeDifferential: true,
  });
});

describe.skipIf(RUN === undefined)('python bridge', () => {
  const run: RunFixture = RUN ?? { gameId: GAME_ID, turn: 1 };

  test('argv is exactly the bridge CLI surface', () => {
    const options = { repoRoot: REPO_ROOT, runsRoot: RUNS_ROOT, cacheRoot: '/cache' };
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
      RUNS_ROOT,
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
      RUNS_ROOT,
      '--game-id',
      run.gameId,
      '--cache-root',
      '/cache',
      '--turn',
      '5',
    ]);
  });

  test(
    'the CLI is byte-identical to calling save_replay/game_events directly',
    async () => {
      const cliCache = scratchDirectory('cli');
      const directCache = scratchDirectory('direct');
      const requests: ReadonlyArray<DirectRequest> = [
        { operation: 'replay', gameId: run.gameId, turn: run.turn, cacheRoot: cliCache },
        { operation: 'board', gameId: run.gameId, turn: run.turn, cacheRoot: cliCache },
        { operation: 'events', gameId: run.gameId, turn: run.turn, cacheRoot: cliCache },
      ];
      const options = { repoRoot: REPO_ROOT, runsRoot: RUNS_ROOT, cacheRoot: cliCache };
      await Promise.all(
        requests.map(async (request) => {
          const argv = derivationArgv(
            options,
            request.operation === 'replay'
              ? {
                  operation: 'replay',
                  gameId: request.gameId,
                  places: [],
                  afterTurn: 0,
                  limit: 250,
                  complete: true,
                }
              : request.operation === 'board'
                ? {
                    operation: 'board',
                    gameId: request.gameId,
                    places: [],
                    turn: request.turn,
                  }
                : {
                    operation: 'events',
                    gameId: request.gameId,
                    places: [],
                    complete: true,
                  },
          );
          const child = Bun.spawn([...argv], {
            cwd: REPO_ROOT,
            stdin: 'pipe',
            stdout: 'pipe',
            stderr: 'pipe',
          });
          await Promise.resolve(child.stdin.write('[]'));
          await child.stdin.end();
          const stdout = new Uint8Array(await new Response(child.stdout).arrayBuffer());
          const stderr = await new Response(child.stderr).text();
          expect(stderr).toBe('');
          expect(await child.exited).toBe(0);
          expect(stdout.byteLength).toBeGreaterThan(0);
          expect(stdout).toEqual(
            directDerivation({ ...request, cacheRoot: directCache }),
          );
        }),
      );
    },
    120_000,
  );

  test(
    'the service returns what the loaders return, cached into a throwaway root',
    async () => {
      const serviceCache = scratchDirectory('service');
      const oracleCache = scratchDirectory('oracle');
      const layer = ReplayDerivationPython({
        repoRoot: REPO_ROOT,
        runsRoot: RUNS_ROOT,
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
        DERIVATION_EVENTS_CACHE_NAME,
      );
    },
    180_000,
  );

  test(
    'a missing board turn is the 404 classification, not a crash',
    async () => {
      const layer = ReplayDerivationPython({
        repoRoot: REPO_ROOT,
        runsRoot: RUNS_ROOT,
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
        expect(derivationProblem(outcome.left)).toEqual({
          status: 404,
          message: GATEWAY_PROBLEM_MESSAGES.boardSnapshotNotFound,
        });
      }
    },
    120_000,
  );

  test(
    'a rejected game id is the 503 classification, and its text stays private',
    async () => {
      const layer = ReplayDerivationPython({
        repoRoot: REPO_ROOT,
        runsRoot: RUNS_ROOT,
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
        expect(derivationProblem(outcome.left)).toEqual({
          status: 503,
          message: GATEWAY_PROBLEM_MESSAGES.eventsUnavailable,
        });
        expect(JSON.stringify(derivationProblem(outcome.left))).not.toContain('Invalid game id');
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
