/** Gateway CLI composition and scoped startup/teardown. */

import {
  type Observability,
  ObservabilityLive,
  ObservabilityNoop,
  telemetryConfigLayer,
} from '@arena/telemetry';
import { type CliApp, Command, ValidationError } from '@effect/cli';
import type { FileSystem, Path } from '@effect/platform';
import type { Teardown } from '@effect/platform/Runtime';
import { NodeContext, NodeRuntime } from '@effect/platform-node';
import { Cause, Console, Data, Effect, Either, Exit, Layer, Option, Predicate } from 'effect';
import {
  GATEWAY_BACKEND_FLAG,
  GATEWAY_CLI_ERROR_EXIT_CODE,
  GATEWAY_CLI_NAME,
  type GatewayCliArgs,
  type GatewayCommandArgs,
  gatewayCommand,
} from './cli.ts';
import {
  GatewayConfig,
  type GatewayConfigError,
  type GatewayConfigValues,
  type PostgresBackend,
  gatewayConfigLayer,
} from './config.ts';
import { type GatewayServeError, runGatewayForever } from './server.ts';
import { type ReplayDerivation, ReplayDerivationPython } from './services/derivation.ts';
import { type ReadyFile, layer as readyFileLayer, stdoutSink } from './services/ready-file.ts';
import {
  RunsRepository,
  type RunsRepositoryApi,
  layer as runsRepositoryLayer,
} from './services/runs.ts';
import { type UpstreamClient, layerLive as upstreamClientLayer } from './services/upstream.ts';

// ---------------------------------------------------------------------------
// Telemetry
// ---------------------------------------------------------------------------

/**
 * Where the wide-event corpus is written, when it is written at all.
 *
 * An environment variable and **not** a flag: `_parser()` has exactly nine
 * options (`:2174-2185`) and `local_stack.py:540-548` spells eight of them, so
 * a tenth would change the surface the parity rig compares.  Unset — the
 * default, and what every existing caller does — selects `ObservabilityNoop`,
 * which records nothing, touches no global state and creates no files.  That
 * matters in a way a "harmless" default would not: the Python gateway writes
 * nothing but the ready file, and a corpus directory appearing next to a run is
 * a filesystem difference a diff-based rig would report.
 */
export const GATEWAY_TELEMETRY_DIR_ENV = 'ARENA_GATEWAY_TELEMETRY_DIR';

/** `service` on every wide event this process emits. */
export const GATEWAY_TELEMETRY_SERVICE = 'arena-gateway';

/** `environment` on every wide event this process emits. */
export const GATEWAY_TELEMETRY_ENVIRONMENT = 'production';

/**
 * Telemetry must never be the reason a gateway fails to start.
 *
 * `ObservabilityLive` can fail with a `TelemetryInitError` — an unwritable
 * corpus directory, an `evlog` that refuses its configuration.  Python has no
 * telemetry at all, so a gateway that refused to serve because its *optional*
 * observability backend could not initialize would be strictly worse than the
 * thing it replaces.  The layer degrades to the no-op and says so on the
 * logger, which is the same discipline `withWideEvent` applies to a dropped
 * event.
 */
export const telemetryLayer: Layer.Layer<Observability> = Layer.unwrapEffect(
  Effect.map(
    // The telemetry sink is an ambient operator switch read once at startup,
    // not harness configuration: a `Config` here would have to be provided
    // before the layer that decides whether telemetry exists at all.
    // oxlint-disable-next-line effecttsgo/process-env-in-effect
    Effect.sync(() => Option.fromNullable(process.env[GATEWAY_TELEMETRY_DIR_ENV])),
    Option.match({
      onNone: (): Layer.Layer<Observability> => ObservabilityNoop,
      onSome: (ndjsonDir): Layer.Layer<Observability> =>
        ObservabilityLive.pipe(
          Layer.provide(
            telemetryConfigLayer({
              service: GATEWAY_TELEMETRY_SERVICE,
              environment: GATEWAY_TELEMETRY_ENVIRONMENT,
              ndjsonDir,
            }),
          ),
          Layer.catchAll((error) =>
            Layer.merge(
              Layer.effectDiscard(
                Effect.logWarning('gateway telemetry disabled').pipe(
                  Effect.annotateLogs({ reason: error._tag, ndjsonDir }),
                ),
              ),
              ObservabilityNoop,
            ),
          ),
        ),
    }),
  ),
);

// ---------------------------------------------------------------------------
// The stack
// ---------------------------------------------------------------------------

/** The four services that need a validated configuration to exist at all. */
export type GatewayConfiguredServices =
  | ReadyFile
  | ReplayDerivation
  | RunsRepository
  | UpstreamClient;

/**
 * The two services whose *implementation* `--backend` chooses.
 *
 * The other two do not have a second implementation and never will:
 * `UpstreamClient` talks to the freeciv service and `ReadyFile` writes a file
 * the local stack polls, neither of which knows what a run archive is.
 */
export type GatewayArchiveServices = ReplayDerivation | RunsRepository;

/**
 * `--backend postgres` was asked for, and this build cannot answer.
 *
 * It carries a `message` for the same reason `GatewayConfigError` does: it is
 * printed at the one exit-2 site, and a bare tag would tell an operator
 * nothing.  It carries **no URL** — not even a host — because the flag that
 * produced it may have held a password (`config.ts`'s {@link PostgresBackend}).
 */
export class GatewayBackendUnavailable extends Data.TaggedError('GatewayBackendUnavailable')<{
  readonly message: string;
}> {}

/**
 * What a gateway is told when `import('@arena/db')` itself fails.
 *
 * The package is a workspace dependency, so this is not a routine failure — it
 * is a broken install, and the remedy belongs in the message rather than in a
 * stack trace nobody can act on.
 */
export const POSTGRES_BACKEND_UNAVAILABLE: string =
  `${GATEWAY_BACKEND_FLAG} postgres could not load @arena/db: run \`bun install\` at the repository root`;

/** The Postgres half of the gateway, loaded only when it is asked for. */
type ArenaDb = typeof import('@arena/db');

/**
 * `import('@arena/db')`, as an Effect.
 *
 * **This is the one dynamic import in the gateway, and it is the point.**  A
 * static import would put `@effect/sql-pg` — and through it the `pg` driver and
 * its `net`/`tls` sockets — into the module graph of *every* gateway, including
 * the filesystem one that `local_stack.py` spawns and the parity rig compares.
 * The Python gateway loads no database driver, and neither does this one until
 * an operator types `--backend postgres`.  `test/gateway/cli.test.ts` proves it
 * with a module-resolution probe rather than with this paragraph.
 *
 * `Effect.tryPromise` is the designated interop boundary for it: a dynamic
 * `import()` is a promise, and this is the only place the gateway makes one.
 */
const loadArenaDb: Effect.Effect<ArenaDb, GatewayBackendUnavailable> = Effect.tryPromise({
  try: () => import('@arena/db'),
  catch: () => new GatewayBackendUnavailable({ message: POSTGRES_BACKEND_UNAVAILABLE }),
});

/** The credential-free stand-in for a target that could not even be parsed. */
export const UNUSABLE_DATABASE_URL = 'an unusable database url';

/** What a failed connection is called when the driver's reason is not repeatable. */
export const DATABASE_CONNECT_FAILED = 'could not open a connection';

/**
 * The database named by `--database-url` cannot be used.
 *
 * It carries `host:port/database` and a short problem, and **never** the URL:
 * `Client.describeTarget` drops userinfo by construction, and the driver's own
 * message is deliberately not forwarded — a driver that echoed its
 * configuration into an error would put a password on stderr, and this error is
 * printed at the one exit-2 site.
 */
export class DatabaseUnavailable extends Data.TaggedError('DatabaseUnavailable')<{
  readonly target: string;
  readonly problem: string;
}> {}

/**
 * How many pooled connections one gateway may hold.
 *
 * `@arena/db`'s `databaseConfig` leaves this unset, which means the `pg`
 * driver's default of **10 per process** — a sensible default for one service
 * and the wrong one for a machine running a matrix of them.  The arithmetic
 * that fixes the number: the parity rig boots one gateway per scenario, nine
 * scenarios, and can run two matrices at once (`ts-fs` against `ts-pg`), so the
 * default is 9 × 10 × 2 = **180 connections against a `max_connections` of
 * 100** — measured as a rig failure, not predicted.  At four, the same
 * arrangement asks for 72 and every gateway still has three spare connections
 * for the concurrent reads a single request makes.
 *
 * A gateway that wants more is a gateway serving more than one request at a
 * time per scenario, which the rig does not do and a developer's browser does
 * not either.  Raising it is a one-line change with an arithmetic obligation.
 */
export const GATEWAY_MAX_DB_CONNECTIONS = 4;

/** `application_name` on every session this gateway opens. */
export const GATEWAY_DB_APPLICATION_NAME = 'arena-replay-gateway';

/**
 * One line of stderr for a repository read that failed on the Postgres path.
 *
 * The gateway's response bytes are the contract, and they say only
 * `{"error":"game manifest is unavailable"}` — a 503 whose *cause* is a
 * `SqlError` that nothing had ever printed: `http/respond.ts` logs unhandled
 * causes, and an `ArchiveUnavailable` is handled, carrying the driver's error
 * silently in its `cause`.  A whole class of defect (a cold pool, a dropped
 * connection, a mistyped column) was therefore invisible from outside the
 * process.  This is that line, and it is deliberately small:
 *
 * - **stderr, never stdout.**  The ready record is the only thing on stdout and
 *   `local_stack.py` parses it; a log line there would break the handshake.
 * - **No response change.**  It is a `tapError`, so the error value, the status
 *   and every byte of the body are what they were.
 * - **Only when there is something to report.**  A 404 is an answer, not a
 *   defect, and a 503 the content produced carries no `cause`; see
 *   {@link failureCause}.
 * - **No URL, no credentials, no stack.**  Only the failure's tag and its
 *   `cause`'s scalar fields, rendered by the same {@link describeStartupError}
 *   the exit-2 site uses.
 * - **Filesystem gateways never reach it.**  It is installed by
 *   {@link postgresArchiveServices} and by nothing else.
 */
export const describeRepositoryFailure = <Failure>(
  operation: string,
  gameId: string,
  error: Failure,
): string => `pg-backend: ${operation} ${gameId} failed: ${describeStartupError(error)}`;

/**
 * The failure's `cause`, when it has one.
 *
 * This is the discriminator the log line turns on, and it is the repository's
 * own: a 503 the *content* produced — a manifest ingest recorded as `unusable`,
 * because it was not UTF-8, was over 8 MiB, or was not an object — is built
 * with no `cause`, because there is nothing behind it to name.  A 503 the
 * *infrastructure* produced carries the `SqlError` that caused it.  Only the
 * second kind is a defect report; the first is an answer, and the parity
 * corpus contains a fixture that produces it on every run.
 */
const failureCause = <Failure>(error: Failure): Option.Option<string> =>
  Predicate.isRecord(error) && 'cause' in error
    ? Option.map(Option.fromNullable(error.cause), describeStartupError)
    : Option.none();

/**
 * The failure's own text, plus whatever its `cause` can say for itself.
 *
 * One level deep on purpose, and it is the level that matters: the public text
 * of every one of these is `game manifest is unavailable`, and the `cause` is
 * what distinguishes a `SqlError` — "the database is gone", "the pool is
 * cold", "a column was mistyped" — from a stored verdict the read path merely
 * relayed.  Rendering it with the same {@link describeStartupError} the exit-2
 * site uses keeps the scalar-only, no-stack, no-URL discipline in one function.
 */
const failureLine = <Failure>(
  operation: string,
  gameId: string,
  error: Failure,
  cause: string,
): string => `${describeRepositoryFailure(operation, gameId, error)} (${cause})`;

/**
 * {@link failureLine}, attached to one repository read — and to nothing that
 * answers normally.
 *
 * A 404 is not a report: `game not found`, `map frame does not exist` and
 * `terminal archive not found` are answers the filesystem gateway gives too,
 * dozens of times in one parity run, and a gateway that narrated them would
 * both bury the one line that matters and put bytes on a stream the rig
 * asserts is empty.  {@link failureCause} is the filter.
 */
const logged = <A, E, R>(
  operation: string,
  gameId: string,
  effect: Effect.Effect<A, E, R>,
): Effect.Effect<A, E, R> =>
  Effect.tapError(effect, (error) =>
    Option.match(failureCause(error), {
      onNone: () => Effect.void,
      onSome: (cause) => Console.error(failureLine(operation, gameId, error, cause)),
    }),
  );

/**
 * The Postgres repository, with every failing read reported on stderr.
 *
 * ## Logging only — nothing here serializes, and nothing here should
 *
 * This decorator once held a per-game semaphore, because `terminalArchive`
 * used to *reconcile a directory* — staging the archive's saves and frames out
 * of `bytea` so the python bridge could read real files — and two requests for
 * one cold game raced (measured: a 24-way burst answered `200` **2** times and
 * `503` **22** times in round 0, then `200` 24/24 forever after).
 *
 * There is no directory to reconcile any more.  A `TerminalArchive.runRoot` is
 * `<--runs-root>/<game-id>` in *both* backends, so the binaries were never
 * moved and never have to be put back: `frameFile` and `videoFile` are pure
 * disk reads, and the database contributes the two documents and the replay
 * tail.  A lock over that would only be a queue on a `SELECT`.
 *
 * ## The logging
 *
 * The five members below are the ones whose error channel is not `never`; the
 * index reads swallow their failures by contract (`diskGamesIndex` answers `[]`
 * on an unreadable root, filesystem or database), so there is nothing there to
 * report that would not also be a lie about what the response contains.
 */
export const withFailureLog = (repository: RunsRepositoryApi): RunsRepositoryApi => ({
  ...repository,
  readManifest: (gameId) => logged('readManifest', gameId, repository.readManifest(gameId)),
  decodeManifest: (gameId) => logged('decodeManifest', gameId, repository.decodeManifest(gameId)),
  terminalArchive: (gameId) =>
    logged('terminalArchive', gameId, repository.terminalArchive(gameId)),
  frameFile: (archive, index) =>
    logged('frameFile', archive.gameId, repository.frameFile(archive, index)),
  videoFile: (archive) => logged('videoFile', archive.gameId, repository.videoFile(archive)),
});

/**
 * The derivation bridge, built the same way for every backend.
 *
 * Spelled once and called twice so that "the pg arm uses the *same* bridge as
 * the fs arm" is a property the compiler keeps rather than a claim two call
 * sites happen to satisfy.  It is replaced, not rewired, when the savegame
 * parsers are ported.
 */
const derivationServices = (config: GatewayConfigValues): Layer.Layer<ReplayDerivation> =>
  ReplayDerivationPython({
    repoRoot: config.repoRoot,
    runsRoot: config.runsRoot,
    cacheRoot: config.cacheRoot,
  });

/**
 * The archive services, out of Postgres.
 *
 * ## Only *one* of the two services changes
 *
 * `ReplayDerivation` is the **same layer, with the same arguments, as the
 * filesystem arm builds** — one `python3 -m agent_eval.replay_derive_cli` per
 * derivation, pointed at the real `--runs-root` and the real `--cache-root`.
 * It used to be `ReplayDerivationPg`, which staged savegames out of `bytea`
 * into a materialization root and mirrored the loaders' cache into a table,
 * because a pg gateway had no run directory to give the bridge.  It has one
 * now: `<--runs-root>/<game-id>`, resolved by the same `realpath`-with-fallback
 * both backends use, because the savegames were never in the database — the
 * frozen schema stores no artifacts and no paths, and the binaries are
 * ephemeral and regenerable.
 *
 * That is the whole of why `--materialize-root`, the materializer and the cache
 * mirror are gone, and it makes the parity claim *stronger* rather than weaker:
 * `TerminalArchive.runRoot` is now the same string on both TypeScript legs, so
 * it stops being the one field the differential oracle has to exclude.
 *
 * ## What the database is still asked for
 *
 * {@link RunsRepository}, and only it: the two documents, the replay tail and
 * the games index.  One {@link ArenaDb.Client.DatabaseLive} under a
 * `DatabaseConfig` built from the redacted URL.  The two errors that stack can
 * produce — an unusable URL and a refused connection — become one
 * {@link DatabaseUnavailable}, which is exit 2, because a gateway that cannot
 * reach its storage has nothing to serve.  Read failures *after* startup are a
 * different question and stay where the repository puts them: a read of one
 * named artifact answers 503, and the two index reads answer empty, because
 * that is what an unreadable `runs_root` does to the filesystem backend.
 *
 * The `mapError` is scoped to the repository's own stack for the same reason
 * the derivation layer is built outside it: the python bridge cannot fail to
 * *construct*, and folding it into a `DatabaseUnavailable` conversion would
 * claim otherwise.
 */
const postgresArchiveServices = (
  config: GatewayConfigValues,
  backend: PostgresBackend,
  db: ArenaDb,
): Layer.Layer<GatewayArchiveServices, DatabaseUnavailable, FileSystem.FileSystem | Path.Path> => {
  const target = Either.match(db.Client.describeTarget(backend.databaseUrl), {
    onLeft: () => UNUSABLE_DATABASE_URL,
    onRight: (reached) => `${reached.host}:${String(reached.port)}/${reached.database}`,
  });
  const repository = Layer.effect(
    RunsRepository,
    Effect.map(RunsRepository, withFailureLog),
  ).pipe(
    Layer.provide(db.RunsRepositoryPg.layer(config.runsRoot)),
    Layer.provide(db.Client.DatabaseLive),
    Layer.provide(
      db.Client.databaseConfigLayer({
        ...db.Client.databaseConfig(backend.databaseUrl),
        applicationName: GATEWAY_DB_APPLICATION_NAME,
        maxConnections: Option.some(GATEWAY_MAX_DB_CONNECTIONS),
      }),
    ),
    Layer.mapError(
      (cause) =>
        new DatabaseUnavailable({
          target,
          problem:
            cause._tag === 'DatabaseUrlInvalid'
              ? db.Client.describeDatabaseUrlInvalid(cause)
              : DATABASE_CONNECT_FAILED,
        }),
    ),
  );
  return Layer.merge(repository, derivationServices(config));
};

/**
 * The archive services for the selected backend.
 *
 * `config.backend === undefined` is `--backend fs` — and, far more often, an
 * argv that never mentioned a backend at all.  That arm is the *original*
 * expression, unchanged and unwrapped: the filesystem gateway builds the
 * layers it always built, and neither `@arena/db` nor a database driver is
 * loaded, resolved or reachable on the way there.
 *
 * {@link derivationServices} is the interim bridge: one `python3 -m
 * agent_eval.replay_derive_cli` per derivation, which is what makes the two
 * gateways byte-comparable on `replay.json`/`board.json`/`events.json` — they
 * call the *same* loaders, over the *same* `--runs-root`, with the *same*
 * cache discipline.  **Both arms build it identically**, which is why it is
 * hoisted out of the branch: after `--materialize-root` died, the storage
 * choice reaches exactly one of the two services.
 */
export const archiveServices = (
  config: GatewayConfigValues,
): Layer.Layer<
  GatewayArchiveServices,
  GatewayBackendUnavailable | DatabaseUnavailable,
  FileSystem.FileSystem | Path.Path
> => {
  const backend = config.backend;
  return backend === undefined
    ? Layer.merge(runsRepositoryLayer(config.runsRoot), derivationServices(config))
    : Layer.unwrapEffect(
        Effect.map(loadArenaDb, (db) => postgresArchiveServices(config, backend, db)),
      );
};

/**
 * Everything downstream of a validated configuration.
 *
 * Only {@link archiveServices} branches; the upstream client and the ready
 * file are built from the same values whichever backend is selected.
 */
export const configuredServices: Layer.Layer<
  GatewayConfiguredServices,
  GatewayBackendUnavailable | DatabaseUnavailable,
  GatewayConfig | FileSystem.FileSystem | Path.Path
> = Layer.unwrapEffect(
  Effect.map(GatewayConfig, (config) =>
    Layer.mergeAll(
      archiveServices(config),
      upstreamClientLayer({
        serviceUrl: config.upstreamServiceUrl,
        timeout: `${config.upstreamTimeoutSeconds} seconds`,
      }),
      readyFileLayer({ path: config.readyFile, sink: stdoutSink }),
    ),
  ),
);

/**
 * argv → every service {@link runGatewayForever} asks for.
 *
 * `provideMerge` keeps {@link GatewayConfig} in the output because the server
 * reads `host`, `port` and `cacheRoot` back out of it; the `FileSystem` the
 * config resolution needs stays in the *requirements* and is satisfied once, at
 * the very top, by `NodeContext.layer`.
 */
export const gatewayLayer = (
  args: GatewayCliArgs,
): Layer.Layer<
  GatewayConfig | GatewayConfiguredServices | Observability,
  GatewayConfigError | GatewayBackendUnavailable | DatabaseUnavailable,
  FileSystem.FileSystem | Path.Path
> =>
  Layer.merge(configuredServices, telemetryLayer).pipe(
    Layer.provideMerge(gatewayConfigLayer(args)),
  );

/**
 * The command with its handler: serve until interrupted, with the stack built
 * from the very argv that named it.
 *
 * `Effect.scoped` is the `finally` at `:2163` — the socket, the ready record
 * and the lock are all registered on this one scope and unwind in the mirror
 * order when the fiber is interrupted.
 */
export type GatewayStartupError =
  | GatewayConfigError
  | GatewayServeError
  | GatewayBackendUnavailable
  | DatabaseUnavailable;

export const gatewayServeCommand: Command.Command<
  typeof GATEWAY_CLI_NAME,
  FileSystem.FileSystem | Path.Path,
  GatewayStartupError,
  GatewayCommandArgs
> = Command.withHandler(gatewayCommand, (args) =>
  Effect.scoped(runGatewayForever).pipe(Effect.provide(gatewayLayer(args))),
);

/**
 * The version `--version` reports.
 *
 * argparse declares none — `_parser()` adds no `--version` action — so this is
 * `@effect/cli`'s built-in answering with `@arena/harness`'s package version.
 * A built-in cannot change how a *valid* invocation parses, which is the bar
 * `cli.ts` sets for accepting `@effect/cli`'s extra flags.
 */
export const GATEWAY_CLI_VERSION = '0.1.0';

/** argv → the program; help, usage and validation handled by `@effect/cli`. */
export const runGatewayCli: (
  argv: ReadonlyArray<string>,
) => Effect.Effect<
  void,
  GatewayStartupError | ValidationError.ValidationError,
  CliApp.CliApp.Environment
> = Command.run(gatewayServeCommand, {
  name: GATEWAY_CLI_NAME,
  version: GATEWAY_CLI_VERSION,
});

// ---------------------------------------------------------------------------
// The one error → exit-code site
// ---------------------------------------------------------------------------

/**
 * `@effect/cli`'s own guard, and it has to be its own: the eleven tags are
 * `MissingValue`, `MissingFlag`, `InvalidValue`, `CommandMismatch`,
 * `HelpRequested` and friends — none of them contains the word "validation", so
 * a hand-rolled tag test silently misses every one and prints
 * `error: [object Object]` under the usage block `@effect/cli` already wrote.
 * (Observed, before this line existed.)
 */
export const isValidationError = <Cause>(cause: Cause): boolean =>
  ValidationError.isValidationError(cause);

/**
 * An error's public text, falling back to whatever it can say about itself.
 *
 * Three tiers, because the failures reaching here have three shapes:
 *
 * 1. `GatewayConfigError` carries the verbatim CPython `ValueError` text as its
 *    `message` — `gateway host must be a loopback address` — and that is the
 *    line, unchanged.
 * 2. A `Data.TaggedError` has an **empty** `message`, so the tag alone is all a
 *    naive renderer produces: `error: ReadyFileLocked` says less than Python's
 *    `error: [Errno 35] Resource temporarily unavailable`.  Its scalar payload
 *    is appended (`lockPath=… errno=35`), which names the file an operator has
 *    to go look at.  Nested values — `cause` above all — are skipped: they are
 *    the private detail this line must not carry, and `[object Object]` would
 *    not help anyone anyway.
 * 3. Anything else is rendered as-is.
 */
export const describeStartupError = <Cause>(cause: Cause): string => {
  if (cause instanceof Error && cause.message !== '') return cause.message;
  if (!Predicate.isRecord(cause) && !Array.isArray(cause)) return String(cause);
  const detail = Object.entries(cause)
    .flatMap(([key, value]: readonly [string, unknown]): ReadonlyArray<string> =>
      key !== '_tag' &&
      (Predicate.isString(value) || Predicate.isNumber(value) || Predicate.isBoolean(value))
        ? [`${key}=${String(value)}`]
        : [],
    )
    .join(' ');
  const label = errorLabel(cause);
  return detail === '' ? label : `${label}: ${detail}`;
};

/**
 * What to call an object that has no usable `message`.
 *
 * Its `_tag` when it has one, its class name when it is an `Error`, and
 * otherwise nothing it can be blamed for.  Deliberately not `String(error)`:
 * for a plain object that is `[object Object]`, and the type-aware lint is
 * right to refuse it.
 */
const errorLabel = <ErrorValue extends object>(error: ErrorValue): string =>
  '_tag' in error && Predicate.isString(error._tag)
    ? error._tag
    : error instanceof Error
      ? error.name
      : 'error';

/**
 * `print(f"error: {exc}", file=sys.stderr)` (`:2206`).
 *
 * The *text* after `error: ` is deliberately not parity material: Python prints
 * an exception's `str()`, nothing in the repo reads it (`local_stack.py`'s
 * `_wait_http` greps the ready record for `"ok":true`, on stdout and over
 * HTTP), and reproducing CPython's `OSError` spelling would mean inventing
 * errno strings for failures Node words differently. What *is* parity material
 * is the stream, the `error: ` prefix and the exit code — all three are here.
 *
 * A `ValidationError` is already on stderr with a usage block by the time it
 * reaches this function — argparse behaves identically, printing usage and
 * exiting 2 — so it is not reported twice.  An interrupted program reports
 * nothing at all: it succeeded.
 */
export const reportStartupFailure = (cause: Cause.Cause<unknown>): Effect.Effect<void> =>
  Cause.isInterruptedOnly(cause)
    ? Effect.void
    : Option.match(Cause.failureOption(cause), {
        onNone: () => Console.error(Cause.pretty(cause)),
        onSome: (error) =>
          isValidationError(error)
            ? Effect.void
            : Console.error(`error: ${describeStartupError(error)}`),
      });

/**
 * `main`'s three return statements, as one total function over the `Exit`.
 *
 * - success → **0** (`:2209`)
 * - interrupted → **0** (`KeyboardInterrupt`, `:2204-2205`)
 * - anything else → **2** (`OSError | ValueError`, `:2206-2207`; argparse's own
 *   exit code for a bad command line is also 2)
 *
 * A defect lands in the third arm on purpose: a refused bind arrives that way
 * (`NodeHttpServer.make` declares no error channel), and in Python it is an
 * `OSError` — exit 2 either way.
 */
export const gatewayTeardown: Teardown = <E, A>(
  exit: Exit.Exit<E, A>,
  onExit: (code: number) => void,
): void => {
  onExit(
    Exit.isSuccess(exit) ? 0 : Cause.isInterruptedOnly(exit.cause) ? 0 : GATEWAY_CLI_ERROR_EXIT_CODE,
  );
};

/**
 * The process entry point, as a value a test can run without a process.
 *
 * The error channel is preserved on purpose: {@link gatewayTeardown} reads the
 * `Exit`, so a program that swallowed its failure here would always exit 0.
 */
export const main = (
  argv: ReadonlyArray<string>,
): Effect.Effect<void, GatewayStartupError | ValidationError.ValidationError> =>
  runGatewayCli(argv).pipe(
    Effect.tapErrorCause(reportStartupFailure),
    Effect.provide(NodeContext.layer),
  );

/**
 * `if __name__ == "__main__": raise SystemExit(main())` (`:2212-2213`).
 *
 * Guarded so that importing this module — which `test/gateway/server.test.ts`
 * does, for the layer stack and the teardown — does not start a gateway.
 * `disableErrorReporting` is on because {@link reportStartupFailure} is the
 * reporter: the default would print an Effect-flavoured stack trace *in
 * addition* to the `error: …` line, and `main` prints exactly one.
 */
if (import.meta.main) {
  NodeRuntime.runMain(main(process.argv), {
    disableErrorReporting: true,
    teardown: gatewayTeardown,
  });
}
