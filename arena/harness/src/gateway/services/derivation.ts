/**
 * Three savegame derivations behind one process-wide semaphore. The interim bridge invokes
 * `python3 -m agent_eval.replay_derive_cli`, sends public places on stdin, parses Python-number JSON,
 * caps output, kills children on every exit, and classifies missing artifacts separately from
 * unavailable loaders. Replace the runner, not this service contract, when parsers become native.
 */

import type { CanonRecord, JsonObject } from '@arena/wire';
import { Context, Data, Duration, Effect, Either, Layer } from 'effect';
import { resolve } from 'node:path';
import { parsePythonJsonObject } from '../python-json.ts';

// ---------------------------------------------------------------------------
// Requests
// ---------------------------------------------------------------------------

/** The three derivations, named the way the routes that need them are named. */
export const DERIVATION_OPERATIONS = ['replay', 'board', 'events'] as const;

/** One of `replay` / `board` / `events`. */
export type DerivationOperation = (typeof DERIVATION_OPERATIONS)[number];

/**
 * `resolved_places` — the loaders' third positional argument.
 *
 * The gateway passes the *public* projection of the manifest's places
 * (`_public_places`, applied at `:1699` and peers), never the raw rows.  This
 * service takes them as opaque JSON: projecting them is the archive module's
 * job, and the events cache keys on a digest of exactly these rows
 * (`game_events._places_digest`), so re-shaping them here would silently
 * invalidate a cache the Python gateway shares.
 */
export type ResolvedPlaces = ReadonlyArray<JsonObject>;

/**
 * `replay_from_autosaves(runs_root, game_id, places, *, after_turn, limit, cache_root, complete)`.
 *
 * These values are `bigint` because CPython's `int()` is unbounded and the
 * loader echoes `after_turn` in `next_after_turn`. This also keeps the bridge
 * argv in exact decimal notation beyond JavaScript's safe-integer range.
 */
export interface ReplayDerivationInput {
  readonly gameId: string;
  /** Defaults to `[]`, which is the loader's own default (`()`). */
  readonly places?: ResolvedPlaces;
  readonly afterTurn: bigint;
  readonly limit: bigint;
  /** `state in TERMINAL_STATES`, computed from the manifest — never from upstream (`:1700`). */
  readonly complete: boolean;
}

/** `board_from_autosave(runs_root, game_id, places, *, turn, cache_root)`. */
export interface BoardDerivationInput {
  readonly gameId: string;
  readonly places?: ResolvedPlaces;
  /** `bigint`, for the reason {@link ReplayDerivationInput} carries. */
  readonly turn: bigint;
}

/** `events_from_autosaves(runs_root, game_id, places, *, cache_root, complete)`. */
export interface EventsDerivationInput {
  readonly gameId: string;
  readonly places?: ResolvedPlaces;
  readonly complete: boolean;
}

/** A fully-defaulted derivation request, as the runner sees it. */
export type DerivationRequest =
  | ({ readonly operation: 'replay'; readonly places: ResolvedPlaces } & Omit<
      ReplayDerivationInput,
      'places'
    >)
  | ({ readonly operation: 'board'; readonly places: ResolvedPlaces } & Omit<
      BoardDerivationInput,
      'places'
    >)
  | ({ readonly operation: 'events'; readonly places: ResolvedPlaces } & Omit<
      EventsDerivationInput,
      'places'
    >);

const NO_PLACES: ResolvedPlaces = [];

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/**
 * The loader raised `FileNotFoundError`: this game has no artifacts to derive
 * from.  Renders as the route's 404.
 */
export class DerivationArtifactsMissing extends Data.TaggedError('DerivationArtifactsMissing')<{
  readonly operation: DerivationOperation;
  readonly gameId: string;
  /** Diagnostic only. **Never** put this in a response body. */
  readonly detail: string;
}> {}

/**
 * The loader raised anything else public, returned a non-mapping, or could not
 * be reached at all.  Renders as the route's 503.
 */
export class DerivationUnavailable extends Data.TaggedError('DerivationUnavailable')<{
  readonly operation: DerivationOperation;
  readonly gameId: string;
  /** Diagnostic only. **Never** put this in a response body. */
  readonly detail: string;
}> {}

/** Every way a derivation can fail. Both are public-safe *classifications*. */
export type DerivationError = DerivationArtifactsMissing | DerivationUnavailable;

// ---------------------------------------------------------------------------
// The cache layout, mirrored
// ---------------------------------------------------------------------------

/**
 * `<cache_root>/<game_id>` — `save_replay._cache_directory` (`:791-806`).
 *
 * The gateway creates `cache_root` and passes it through; the database cache
 * mirror uses this helper to share the same per-game namespace.
 */
export const derivationCacheDirectory = (cacheRoot: string, gameId: string): string =>
  `${cacheRoot.replace(/\/+$/, '')}/${gameId}`;

// ---------------------------------------------------------------------------
// The service
// ---------------------------------------------------------------------------

/** The three derivations, each already serialized against the others. */
export interface ReplayDerivationService {
  readonly replay: (
    input: ReplayDerivationInput,
  ) => Effect.Effect<CanonRecord, DerivationError>;
  readonly board: (input: BoardDerivationInput) => Effect.Effect<CanonRecord, DerivationError>;
  readonly events: (input: EventsDerivationInput) => Effect.Effect<CanonRecord, DerivationError>;
}

/** The service tag. */
export class ReplayDerivation extends Context.Tag('@arena/harness/gateway/ReplayDerivation')<
  ReplayDerivation,
  ReplayDerivationService
>() {}

/** A backend: one request in, one JSON document or a classified failure out. */
export type DerivationRunner = (
  request: DerivationRequest,
) => Effect.Effect<CanonRecord, DerivationError>;

/**
 * Wrap a runner in the single mutex.
 *
 * The semaphore is created *here*, once per layer, so every method of one
 * service instance shares it — which is what makes this `replay_lock` rather
 * than three unrelated locks.  Only the runner call is inside it; building the
 * request is not.
 */
export const makeReplayDerivation = (
  run: DerivationRunner,
): Effect.Effect<ReplayDerivationService> =>
  Effect.map(Effect.makeSemaphore(1), (lock): ReplayDerivationService => {
    const serialized = (request: DerivationRequest): Effect.Effect<CanonRecord, DerivationError> =>
      lock.withPermits(1)(Effect.suspend(() => run(request)));
    return {
      replay: (input) =>
        serialized({
          operation: 'replay',
          gameId: input.gameId,
          places: input.places ?? NO_PLACES,
          afterTurn: input.afterTurn,
          limit: input.limit,
          complete: input.complete,
        }),
      board: (input) =>
        serialized({
          operation: 'board',
          gameId: input.gameId,
          places: input.places ?? NO_PLACES,
          turn: input.turn,
        }),
      events: (input) =>
        serialized({
          operation: 'events',
          gameId: input.gameId,
          places: input.places ?? NO_PLACES,
          complete: input.complete,
        }),
    };
  });

/** Build a `ReplayDerivation` layer from any runner. */
export const layerFromRunner = (run: DerivationRunner): Layer.Layer<ReplayDerivation> =>
  Layer.effect(ReplayDerivation, makeReplayDerivation(run));

// ---------------------------------------------------------------------------
// Layer: python
// ---------------------------------------------------------------------------

/** How to reach the interim Python bridge. */
export interface PythonDerivationOptions {
  /** The Freeciv checkout; archived Python is loaded from `arena/archive/`. */
  readonly repoRoot: string;
  /** `--runs-root`. Read-only to the loaders. */
  readonly runsRoot: string;
  /** `--cache-root`. Must not be inside the run's `saves/` (the loaders refuse). */
  readonly cacheRoot: string;
  /** Defaults to `python3`. */
  readonly python?: string;
  /** Optional wall-clock budget. Omitted means no timeout, matching Python. */
  readonly timeout?: Duration.DurationInput;
  /** Cap on the JSON the bridge may return. Defaults to 64 MiB. */
  readonly maxOutputBytes?: number;
}

/** `python3 -m agent_eval.replay_derive_cli`. */
export const DERIVE_CLI_MODULE = 'agent_eval.replay_derive_cli';

/** Exit codes the bridge uses; see its docstring. */
export const DERIVE_EXIT = {
  ok: 0,
  usage: 2,
  notFound: 3,
  unavailable: 4,
} as const;

const DEFAULT_MAX_OUTPUT_BYTES = 64 * 1024 * 1024;
const STDERR_CAP_BYTES = 8 * 1024;

/** The child's argv, in a fixed order so a recording test can assert on it. */
export const derivationArgv = (
  options: PythonDerivationOptions,
  request: DerivationRequest,
): ReadonlyArray<string> => {
  const base = [
    options.python ?? 'python3',
    '-m',
    DERIVE_CLI_MODULE,
    '--op',
    request.operation,
    '--runs-root',
    options.runsRoot,
    '--game-id',
    request.gameId,
    '--cache-root',
    options.cacheRoot,
  ];
  return request.operation === 'replay'
    ? [
        ...base,
        '--after-turn',
        String(request.afterTurn),
        '--limit',
        String(request.limit),
        ...(request.complete ? ['--complete'] : []),
      ]
    : request.operation === 'board'
      ? [...base, '--turn', String(request.turn)]
      : [...base, ...(request.complete ? ['--complete'] : [])];
};

interface CollectState {
  readonly parts: ReadonlyArray<Uint8Array>;
  readonly bytes: number;
  readonly done: boolean;
  readonly truncated: boolean;
}

const EMPTY_COLLECT: CollectState = { parts: [], bytes: 0, done: false, truncated: false };

const joinChunks = (parts: ReadonlyArray<Uint8Array>, total: number): Uint8Array =>
  parts.reduce(
    (accumulator, part) => {
      accumulator.buffer.set(part, accumulator.offset);
      return { buffer: accumulator.buffer, offset: accumulator.offset + part.byteLength };
    },
    { buffer: new Uint8Array(total), offset: 0 },
  ).buffer;

/** Collected output, and whether the cap cut it short. */
interface Collected {
  readonly text: string;
  readonly truncated: boolean;
}

/**
 * Read a child stream with a hard ceiling on retention, cancelling the reader
 * the moment the cap is crossed — the same bounded-read discipline the
 * upstream client uses, so a runaway bridge cannot exhaust the gateway.
 */
const collectCapped = (
  stream: ReadableStream<Uint8Array>,
  capBytes: number,
  request: DerivationRequest,
  channel: 'stdout' | 'stderr',
): Effect.Effect<Collected, DerivationUnavailable> =>
  Effect.acquireUseRelease(
    Effect.sync(() => stream.getReader()),
    (reader) =>
      Effect.iterate(EMPTY_COLLECT, {
        while: (state) => !state.done && !state.truncated,
        body: (state) =>
          Effect.map(
            Effect.tryPromise({
              try: () => reader.read(),
              catch: (cause) =>
                new DerivationUnavailable({
                  operation: request.operation,
                  gameId: request.gameId,
                  detail: `${channel} read failed: ${String(cause)}`,
                }),
            }),
            (result): CollectState =>
              result.done || result.value === undefined
                ? { ...state, done: true }
                : {
                    parts: [...state.parts, result.value],
                    bytes: state.bytes + result.value.byteLength,
                    done: false,
                    truncated: state.bytes + result.value.byteLength > capBytes,
                  },
          ),
      }),
    (reader) => Effect.ignore(Effect.tryPromise(() => reader.cancel())),
  ).pipe(
    Effect.map((state) => ({
      text: new TextDecoder().decode(joinChunks(state.parts, state.bytes)),
      truncated: state.truncated,
    })),
  );

interface ChildOutcome {
  readonly exitCode: number;
  readonly stdout: Collected;
  readonly stderr: Collected;
}

const spawnBridge = (
  options: PythonDerivationOptions,
  request: DerivationRequest,
): Effect.Effect<ChildOutcome, DerivationUnavailable> =>
  Effect.scoped(
    Effect.gen(function* () {
      const child = yield* Effect.acquireRelease(
        Effect.try({
          try: () =>
            Bun.spawn([...derivationArgv(options, request)], {
              cwd: options.repoRoot,
              env: {
                ...process.env,
                PYTHONPATH: resolve(options.repoRoot, 'arena/archive'),
              },
              stdin: 'pipe',
              stdout: 'pipe',
              stderr: 'pipe',
            }),
          catch: (cause) =>
            new DerivationUnavailable({
              operation: request.operation,
              gameId: request.gameId,
              detail: `spawn failed: ${String(cause)}`,
            }),
        }),
        // Runs on success, failure, timeout and interruption: no orphans.
        //
        // `Effect.try`, not `Effect.sync`: `Effect.ignore` only handles the
        // *error* channel, so a `kill()` that threw inside an `Effect.sync`
        // would be a defect, escape the `ignore`, and poison the scope's exit
        // — turning a derivation timeout into a 500 for a request that was
        // already being torn down.  Same rule as `./upstream.ts`'s reader
        // cancel and `./ready-file.ts`'s `closeQuietly`.
        (spawned) => Effect.ignore(Effect.try(() => spawned.kill())),
      );

      // `resolved_places` travels on stdin, not in argv. The sink is finalized
      // on success, transport failure and interruption so the child can never
      // remain parked waiting for EOF.
      yield* Effect.acquireUseRelease(
        Effect.succeed(child.stdin),
        (stdin) =>
          Effect.tryPromise({
            try: async () => {
              await Promise.resolve(stdin.write(JSON.stringify(request.places)));
              await Promise.resolve(stdin.end());
            },
            catch: (cause) =>
              new DerivationUnavailable({
                operation: request.operation,
                gameId: request.gameId,
                detail: `stdin write failed: ${String(cause)}`,
              }),
          }),
        (stdin) => Effect.ignore(Effect.tryPromise(() => Promise.resolve(stdin.end()))),
      );

      const [stdout, stderr] = yield* Effect.all(
        [
          collectCapped(
            child.stdout,
            options.maxOutputBytes ?? DEFAULT_MAX_OUTPUT_BYTES,
            request,
            'stdout',
          ),
          collectCapped(child.stderr, STDERR_CAP_BYTES, request, 'stderr'),
        ],
        { concurrency: 2 },
      );

      const exitCode = yield* Effect.tryPromise({
        try: () => child.exited,
        catch: (cause) =>
          new DerivationUnavailable({
            operation: request.operation,
            gameId: request.gameId,
            detail: `wait failed: ${String(cause)}`,
          }),
      });

      return { exitCode, stdout, stderr };
    }),
  );

const parseBridgeOutput = (
  request: DerivationRequest,
  outcome: ChildOutcome,
): Effect.Effect<CanonRecord, DerivationError> => {
  const unavailable = (detail: string): Effect.Effect<CanonRecord, DerivationError> =>
    Effect.fail(
      new DerivationUnavailable({
        operation: request.operation,
        gameId: request.gameId,
        detail,
      }),
    );
  if (outcome.exitCode === DERIVE_EXIT.notFound) {
    return Effect.fail(
      new DerivationArtifactsMissing({
        operation: request.operation,
        gameId: request.gameId,
        detail: outcome.stderr.text.trim(),
      }),
    );
  }
  if (outcome.exitCode !== DERIVE_EXIT.ok) {
    return unavailable(
      `derive exited ${String(outcome.exitCode)}: ${outcome.stderr.text.trim()}`,
    );
  }
  if (outcome.stdout.truncated) {
    return unavailable('derivation output exceeded the configured cap');
  }
  // `parsePythonJsonObject`, not `JSON.parse`: the bridge printed
  // `json.dumps(loader_result)`, and Python never round-tripped that document
  // at all — it handed the loader's `dict` straight to `_bounded_json`.  A
  // reader that collapsed `1` and `1.0` would make `replay.json` and
  // `board.json` differ from CPython in every integer they carry (`../python-json.ts`).
  //
  // A non-object is the bridge's own `isinstance(value, Mapping)` guard seen
  // from the other side; Python already refuses to print one, so reaching here
  // means something else wrote to stdout.
  return Either.match(parsePythonJsonObject(outcome.stdout.text), {
    onLeft: (issue) => unavailable(`derivation output is not a JSON mapping: ${issue.message}`),
    onRight: (value) => Effect.succeed(value),
  });
};

/** The runner behind {@link ReplayDerivationPython}, exposed for tests. */
export const pythonDerivationRunner =
  (options: PythonDerivationOptions): DerivationRunner =>
  (request) => {
    const derive = spawnBridge(options, request).pipe(
      Effect.flatMap((outcome) => parseBridgeOutput(request, outcome)),
    );
    return options.timeout === undefined
      ? derive
      : derive.pipe(
          Effect.timeoutFail({
            duration: Duration.decode(options.timeout),
            onTimeout: () =>
              new DerivationUnavailable({
                operation: request.operation,
                gameId: request.gameId,
                detail: 'derivation timed out',
              }),
          }),
        );
  };

/**
 * The interim layer: each derivation is one `python3 -m
 * agent_eval.replay_derive_cli` process, JSON over stdout.
 *
 * **Replaced in phase 4**, when the savegame parsers are ported and this layer
 * becomes a native implementation with the same tag and the same semaphore.
 * Until then the bridge is what makes the Python and TypeScript gateways
 * byte-comparable on the replay/board/events routes: they call the *same*
 * functions with the *same* cache discipline.
 */
export const ReplayDerivationPython = (
  options: PythonDerivationOptions,
): Layer.Layer<ReplayDerivation> => layerFromRunner(pythonDerivationRunner(options));
