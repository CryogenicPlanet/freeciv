/**
 * `main` and the layer stack — `agent_eval/replay_gateway.py:2188-2213`.
 *
 * ```python
 * def main(argv=None):
 *     args = _parser().parse_args(argv)
 *     try:
 *         run_replay_gateway(..., on_ready=lambda value: print(_canonical(value).decode(), flush=True))
 *     except KeyboardInterrupt:
 *         return 0
 *     except (OSError, ValueError) as exc:
 *         print(f"error: {exc}", file=sys.stderr)
 *         return 2
 *     return 0
 * ```
 *
 * Four lines of Python and three obligations that a port loses one at a time:
 *
 * 1. **One error → exit-code site.**  Every construction-time refusal — a
 *    non-loopback host, a port outside `[0, 65535]`, a service URL carrying
 *    credentials, a non-positive timeout, a ready file another gateway already
 *    holds — reaches the *same* `except` and the *same* exit 2.  Here that is
 *    {@link gatewayTeardown}: one `Teardown` that reads the program's `Exit`
 *    and picks the code, so no branch of the program can pick its own.
 * 2. **Interruption is success.**  `KeyboardInterrupt` returns **0**, not 130
 *    and not 1.  `local_stack.py` stops the gateway with a signal on every
 *    clean shutdown, so a non-zero code there would make every normal run look
 *    like a failure.
 * 3. **The handshake line is the only thing on stdout.**  `log_message` is a
 *    no-op (`:1332`) and nothing else prints, which is why the ready line is
 *    written by the `ReadyFile` service's sink and never from here.
 *
 * ## The layer stack, and why it is built in two steps
 *
 * `gatewayConfigLayer` turns argv into a validated {@link GatewayConfig}; every
 * other service is parameterized *by* that configuration (`runs-root` for the
 * repository, the normalized service URL and the timeout for the upstream
 * client, `repo-root`/`cache-root` for the derivation bridge, `ready-file` for
 * the publisher).  So the stack is a `Layer.unwrapEffect` over the config tag
 * rather than a flat `mergeAll`: the second step cannot be written without the
 * first step's *values*, and expressing that as a dependency is what stops a
 * service from reaching for argv on its own.
 *
 * @module
 */

import {
  Observability,
  ObservabilityLive,
  ObservabilityNoop,
  telemetryConfigLayer,
} from '@arena/telemetry';
import { CliApp, Command, ValidationError } from '@effect/cli';
import type { FileSystem } from '@effect/platform';
import type { Teardown } from '@effect/platform/Runtime';
import { BunContext, BunRuntime } from '@effect/platform-bun';
import { Cause, Console, Effect, Exit, Layer, Option } from 'effect';
import {
  GATEWAY_CLI_ERROR_EXIT_CODE,
  GATEWAY_CLI_NAME,
  type GatewayCliArgs,
  gatewayCommand,
} from './cli.ts';
import { GatewayConfig, type GatewayConfigError, gatewayConfigLayer } from './config.ts';
import { type GatewayServeError, runGatewayForever } from './server.ts';
import { ReplayDerivation, ReplayDerivationPython } from './services/derivation.ts';
import { ReadyFile, layer as readyFileLayer, stdoutSink } from './services/ready-file.ts';
import { RunsRepository, layer as runsRepositoryLayer } from './services/runs.ts';
import { UpstreamClient, layerLive as upstreamClientLayer } from './services/upstream.ts';

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
 * Everything downstream of a validated configuration.
 *
 * `ReplayDerivationPython` is the interim bridge: one `python3 -m
 * agent_eval.replay_derive_cli` per derivation, which is what makes the two
 * gateways byte-comparable on `replay.json`/`board.json`/`events.json` — they
 * call the *same* loaders with the *same* cache discipline.  It is replaced,
 * not rewired, when the savegame parsers are ported.
 */
export const configuredServices: Layer.Layer<GatewayConfiguredServices, never, GatewayConfig> =
  Layer.unwrapEffect(
    Effect.map(GatewayConfig, (config) =>
      Layer.mergeAll(
        runsRepositoryLayer(config.runsRoot),
        upstreamClientLayer({
          serviceUrl: config.upstreamServiceUrl,
          timeout: `${config.upstreamTimeoutSeconds} seconds`,
        }),
        readyFileLayer({ path: config.readyFile, sink: stdoutSink }),
        ReplayDerivationPython({
          repoRoot: config.repoRoot,
          runsRoot: config.runsRoot,
          cacheRoot: config.cacheRoot,
        }),
      ),
    ),
  );

/**
 * argv → every service {@link runGatewayForever} asks for.
 *
 * `provideMerge` keeps {@link GatewayConfig} in the output because the server
 * reads `host`, `port` and `cacheRoot` back out of it; the `FileSystem` the
 * config resolution needs stays in the *requirements* and is satisfied once, at
 * the very top, by `BunContext.layer`.
 */
export const gatewayLayer = (
  args: GatewayCliArgs,
): Layer.Layer<
  GatewayConfig | GatewayConfiguredServices | Observability,
  GatewayConfigError,
  FileSystem.FileSystem
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
export type GatewayStartupError = GatewayConfigError | GatewayServeError;

export const gatewayServeCommand: Command.Command<
  typeof GATEWAY_CLI_NAME,
  FileSystem.FileSystem,
  GatewayStartupError,
  GatewayCliArgs
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
export const isValidationError: (error: unknown) => boolean =
  ValidationError.isValidationError;

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
export const describeStartupError = (error: unknown): string => {
  if (error instanceof Error && error.message !== '') return error.message;
  if (typeof error !== 'object' || error === null) return String(error);
  const detail = Object.entries(error)
    .flatMap(([key, value]: readonly [string, unknown]): ReadonlyArray<string> =>
      key !== '_tag' &&
      (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean')
        ? [`${key}=${String(value)}`]
        : [],
    )
    .join(' ');
  const label = errorLabel(error);
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
const errorLabel = (error: object): string =>
  '_tag' in error && typeof error._tag === 'string'
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
 * errno strings for failures Bun words differently.  What *is* parity material
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
 * (`BunHttpServer.make` declares no error channel), and in Python it is an
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
    Effect.provide(BunContext.layer),
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
  BunRuntime.runMain(main(process.argv), {
    disableErrorReporting: true,
    teardown: gatewayTeardown,
  });
}
