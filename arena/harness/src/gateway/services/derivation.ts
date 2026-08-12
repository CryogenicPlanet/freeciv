/**
 * `ReplayDerivation` — the three savegame derivations behind one mutex.
 *
 * The Python gateway calls three loaders and nothing else touches a savegame:
 * `replay_from_autosaves`, `board_from_autosave`, `events_from_autosaves`
 * (`agent_eval/replay_gateway.py:262-320`).  Every call is wrapped in
 * `with server.replay_lock:` (`:1701`, `:1775`, `:1857`) — a single
 * process-wide `RLock` (`:1298`), **not** a per-game one, because the parsers
 * write into a shared per-game cache directory and are not concurrency-safe.
 * Nothing else is inside the lock: manifest reads, the archive projection and
 * all serialization happen outside it.
 *
 * This module is that shape in Effect: a service with three methods, a single
 * `Effect.Semaphore(1)` created per layer, and `withPermits(1)` around **the
 * loader invocation only**.  Reentrancy (the `R` in `RLock`) is never
 * exercised in Python — no loader calls back into the gateway — so a plain
 * semaphore is faithful.
 *
 * ## Errors are values, and they carry the route's classification
 *
 * The routes make exactly two classes out of a loader failure
 * (`:1712-1723`, `:1784-1795`, `:1866-1877`):
 *
 * - `FileNotFoundError` → **404**, message per operation.
 * - `OSError` / `UnicodeError` / `JSONDecodeError` / `ValueError` (which
 *   subsumes `SaveReplayError`), *or a result that is not a `Mapping`* →
 *   **503**, message per operation.
 *
 * So this service fails with {@link DerivationArtifactsMissing} or
 * {@link DerivationUnavailable}, and {@link derivationProblem} turns either
 * into the `{status, message}` the one response site renders.  The messages
 * and their statuses come from `@arena/wire`; none are re-declared here.
 * Neither error's `detail` is ever public — Python's rule is that loader
 * exception text never reaches a body
 * (`test_events_loader_failures_stay_public`), and `derivationProblem` is the
 * only sanctioned way to render one.
 *
 * ## The document is canonical, not `JSON.parse` output
 *
 * The three methods answer with a `CanonRecord` — `@arena/wire`'s model, where
 * a Python `int` is a `bigint` and a Python `float` is a `number` — because
 * `replay.json` and `board.json` relay the loader's document *whole*.  In
 * CPython there is no round trip at all: `_bounded_json` is handed the loader's
 * `dict`.  Here the document crosses a process boundary as text, so it is read
 * back by `../python-json.ts` rather than by `JSON.parse`, which would collapse
 * `1` and `1.0` and re-emit every integer in a replay page as a float.
 *
 * ## Three layers
 *
 * - {@link ReplayDerivationPython} — spawns `python3 -m
 *   agent_eval.replay_derive_cli`, an **explicit interim bridge replaced in
 *   phase 4** (see that module's docstring).  It exists so the gateway port
 *   can be finished and byte-compared before the savegame parsers are ported.
 * - {@link ReplayDerivationUnavailable} — every derivation is a typed 503.
 *   The honest layer for a gateway built without a Python checkout.
 * - {@link ReplayDerivationFixture} — a map, for tests.
 *
 * @module
 */

import { createHash } from 'node:crypto';
import type { CanonRecord, JsonObject } from '@arena/wire';
import {
  GATEWAY_PROBLEM_MESSAGES,
  GATEWAY_PROBLEM_STATUS,
  type GatewayProblemMessage,
  type GatewayProblemResponse,
} from '@arena/wire/gateway';
import { Context, Data, Duration, Effect, Either, Layer } from 'effect';
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

/** `replay_from_autosaves(runs_root, game_id, places, *, after_turn, limit, cache_root, complete)`. */
export interface ReplayDerivationInput {
  readonly gameId: string;
  /** Defaults to `[]`, which is the loader's own default (`()`). */
  readonly places?: ResolvedPlaces;
  readonly afterTurn: number;
  readonly limit: number;
  /** `state in TERMINAL_STATES`, computed from the manifest — never from upstream (`:1700`). */
  readonly complete: boolean;
}

/** `board_from_autosave(runs_root, game_id, places, *, turn, cache_root)`. */
export interface BoardDerivationInput {
  readonly gameId: string;
  readonly places?: ResolvedPlaces;
  readonly turn: number;
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

/**
 * A stable string identifying a request, for fixture maps and for logs.
 *
 * `places` are deliberately **not** part of the key: a fixture keyed by the
 * manifest's place projection would be unreadable, and the tests that care
 * about places assert on the recorded request instead.
 */
export const derivationRequestKey = (request: DerivationRequest): string =>
  request.operation === 'replay'
    ? `replay:${request.gameId}:${String(request.afterTurn)}:${String(request.limit)}:${String(request.complete)}`
    : request.operation === 'board'
      ? `board:${request.gameId}:${String(request.turn)}`
      : `events:${request.gameId}:${String(request.complete)}`;

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

const NOT_FOUND_MESSAGE: { readonly [K in DerivationOperation]: GatewayProblemMessage } = {
  replay: GATEWAY_PROBLEM_MESSAGES.replayArtifactsNotFound,
  board: GATEWAY_PROBLEM_MESSAGES.boardSnapshotNotFound,
  events: GATEWAY_PROBLEM_MESSAGES.eventArtifactsNotFound,
};

const UNAVAILABLE_MESSAGE: { readonly [K in DerivationOperation]: GatewayProblemMessage } = {
  replay: GATEWAY_PROBLEM_MESSAGES.replayTelemetryUnavailable,
  board: GATEWAY_PROBLEM_MESSAGES.boardSnapshotUnavailable,
  events: GATEWAY_PROBLEM_MESSAGES.eventsUnavailable,
};

/**
 * The public `{status, message}` for a derivation failure — the only thing a
 * route may take from one.  Statuses come from wire's table rather than being
 * spelled again, so a message that moves between statuses moves here too.
 */
export const derivationProblem = (error: DerivationError): GatewayProblemResponse => {
  const message =
    error._tag === 'DerivationArtifactsMissing'
      ? NOT_FOUND_MESSAGE[error.operation]
      : UNAVAILABLE_MESSAGE[error.operation];
  return { status: GATEWAY_PROBLEM_STATUS[message], message };
};

// ---------------------------------------------------------------------------
// The cache layout, mirrored
// ---------------------------------------------------------------------------

/**
 * `<cache_root>/<game_id>` — `save_replay._cache_directory` (`:791-806`).
 *
 * The gateway itself never reads or writes inside `cache_root`; it creates it
 * once at startup and passes it through.  These helpers exist so a port can
 * *assert* on the layout (and so a test can prove it is caching where Python
 * caches) without reimplementing the discipline.
 */
export const derivationCacheDirectory = (cacheRoot: string, gameId: string): string =>
  `${cacheRoot.replace(/\/+$/, '')}/${gameId}`;

/**
 * `turn-{turn:010d}-{sha256(source_name)[:12]}.json` —
 * `save_replay._cache_path` (`:836-838`).  The digest is over the save's
 * *file name*, UTF-8, not its contents.
 */
export const derivationTurnCacheName = (turn: number, sourceName: string): string => {
  const digest = createHash('sha256').update(sourceName, 'utf8').digest('hex').slice(0, 12);
  return `turn-${String(turn).padStart(10, '0')}-${digest}.json`;
};

/** `events.json` — `game_events._events_cache_path` (`:913`). */
export const DERIVATION_EVENTS_CACHE_NAME = 'events.json';

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
// Layer: unavailable
// ---------------------------------------------------------------------------

const UNAVAILABLE_DETAIL = 'no derivation backend is configured';

/**
 * Every derivation is a typed 503.
 *
 * This is the correct answer — not a crash and not a 500 — for a gateway with
 * no way to reach the savegame parsers: the Python routes reach the same 503
 * whenever the loader cannot produce a mapping, and the viewer already renders
 * it.
 */
export const ReplayDerivationUnavailable: Layer.Layer<ReplayDerivation> = layerFromRunner(
  (request) =>
    Effect.fail(
      new DerivationUnavailable({
        operation: request.operation,
        gameId: request.gameId,
        detail: UNAVAILABLE_DETAIL,
      }),
    ),
);

// ---------------------------------------------------------------------------
// Layer: fixture
// ---------------------------------------------------------------------------

/**
 * A fixture map: {@link derivationRequestKey} to the effect that answers it.
 *
 * Entries are *effects*, not values, so a test can gate one on a latch and
 * prove the semaphore actually serializes — which is the one property of this
 * service the Python suite never tests (it has no concurrency test for
 * `replay_lock` at all).
 */
export type DerivationFixture = ReadonlyMap<
  string,
  Effect.Effect<CanonRecord, DerivationError>
>;

/** A fixture map from plain success values. */
export const derivationFixture = (
  entries: Readonly<Record<string, CanonRecord>>,
): DerivationFixture =>
  new Map(
    Object.entries(entries).map(([key, value]) => [key, Effect.succeed(value)] as const),
  );

/**
 * Answer from a map.  A key with no entry fails the way a loader with no
 * artifacts fails — `FileNotFoundError`, the route's 404 — so a test that
 * forgets a fixture sees the realistic error rather than a hang.
 */
export const ReplayDerivationFixture = (
  entries: DerivationFixture,
): Layer.Layer<ReplayDerivation> =>
  layerFromRunner((request) => {
    const entry = entries.get(derivationRequestKey(request));
    return entry ?? Effect.fail(
      new DerivationArtifactsMissing({
        operation: request.operation,
        gameId: request.gameId,
        detail: `no fixture for ${derivationRequestKey(request)}`,
      }),
    );
  });

// ---------------------------------------------------------------------------
// Layer: python
// ---------------------------------------------------------------------------

/** How to reach the interim Python bridge. */
export interface PythonDerivationOptions {
  /** The checkout containing `agent_eval/` — the child's working directory. */
  readonly repoRoot: string;
  /** `--runs-root`. Read-only to the loaders. */
  readonly runsRoot: string;
  /** `--cache-root`. Must not be inside the run's `saves/` (the loaders refuse). */
  readonly cacheRoot: string;
  /** Defaults to `python3`. */
  readonly python?: string;
  /** Wall-clock budget for one derivation. Defaults to 120 seconds. */
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
const DEFAULT_TIMEOUT: Duration.DurationInput = '120 seconds';
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
): Effect.Effect<Collected> =>
  Effect.acquireUseRelease(
    Effect.sync(() => stream.getReader()),
    (reader) =>
      Effect.iterate(EMPTY_COLLECT, {
        while: (state) => !state.done && !state.truncated,
        body: (state) =>
          Effect.map(
            Effect.orElseSucceed(
              Effect.tryPromise(() => reader.read()),
              () => ({ done: true, value: undefined }) as const,
            ),
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

      // `resolved_places` travels on stdin, not in argv: it is the loaders'
      // third positional argument and can be arbitrarily large.
      yield* Effect.ignore(
        Effect.tryPromise(async () => {
          await Promise.resolve(child.stdin.write(JSON.stringify(request.places)));
          await child.stdin.end();
        }),
      );

      const [stdout, stderr] = yield* Effect.all(
        [
          collectCapped(child.stdout, options.maxOutputBytes ?? DEFAULT_MAX_OUTPUT_BYTES),
          collectCapped(child.stderr, STDERR_CAP_BYTES),
        ],
        { concurrency: 2 },
      );

      const exitCode = yield* Effect.orElseSucceed(
        Effect.tryPromise(() => child.exited),
        () => -1,
      );

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
  (request) =>
    spawnBridge(options, request).pipe(
      Effect.flatMap((outcome) => parseBridgeOutput(request, outcome)),
      Effect.timeoutFail({
        duration: Duration.decode(options.timeout ?? DEFAULT_TIMEOUT),
        onTimeout: () =>
          new DerivationUnavailable({
            operation: request.operation,
            gameId: request.gameId,
            detail: 'derivation timed out',
          }),
      }),
    );

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
