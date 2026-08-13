/**
 * `arena-ingest` — the command line over {@link Ingest.ingest}.
 *
 * The gateway never writes to the database; this command is the only writer, so
 * it is also the only place a connection string is accepted. Three rules shape
 * every line below.
 *
 * **1. The URL is `Redacted` from the moment it is parsed and is never
 * printed.** Unlike the gateway's `--service-url`, a database URL may carry
 * credentials. `Options.redacted` keeps it out of `@effect/cli`'s own error and
 * wizard rendering; {@link describeTarget} is the *only* shape of it anyone is
 * shown — host, port, database, no user, no password. That is also why the
 * failure path below never prints a `SqlError`'s `cause`: `pg` puts the
 * connection parameters into some of them.
 *
 * **2. Exit codes match the gateway's, so a script can treat the two the
 * same.** Success is 0, an interrupt is 0 (a `SIGINT` during a sweep is not a
 * failure — the sweep is left `aborted` and deletes nothing), and everything
 * else is 2, which is also `@effect/cli`'s exit code for a bad command line.
 *
 * **3. Migrations run by default.** A sweep against a database whose schema is
 * older than the ingester is not a partial success, it is a confusing failure;
 * `Migrator` records what it applied, so running them is idempotent and a
 * warm database pays one `SELECT`. `--skip-migrations` exists for the operator
 * who has a separate deploy step.
 *
 * ```
 * arena-ingest --runs-root .agent-eval/runs --database-url postgres://…/arena
 * arena-ingest --runs-root … --database-url … --game game_abc… --dry-run
 * ```
 *
 * @module
 */

import { Command, Options } from "@effect/cli"
import type { ValidationError } from "@effect/cli/ValidationError"
import { BunContext, BunRuntime } from "@effect/platform-bun"
import type { Teardown } from "@effect/platform/Runtime"
import * as Cause from "effect/Cause"
import * as Console from "effect/Console"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import type * as Redacted from "effect/Redacted"

import {
  DatabaseLive,
  databaseConfig,
  databaseConfigLayer,
  DatabaseUrlInvalid,
  describeDatabaseUrlInvalid,
  describeTarget
} from "./client.ts"
import * as Ingest from "./ingest.ts"
import * as Migrate from "./migrate.ts"

/** The command's name — one token, because `@effect/cli` matches on it. */
export const INGEST_CLI_NAME = "arena-ingest"

/** Reported by `--version`. */
export const INGEST_CLI_VERSION = "0.1.0"

/**
 * Everything the command can fail with: a bad command line, a bad URL, or the
 * sweep itself. Spelled once so the error channel never widens to `unknown` —
 * which would take every branch of {@link describeIngestError} with it.
 */
export type CliError = ValidationError | DatabaseUrlInvalid | Ingest.IngestError

/** Anything that is not success and not an interrupt (`@effect/cli` uses 2 too). */
export const INGEST_CLI_ERROR_EXIT_CODE = 2

// ---------------------------------------------------------------------------
// Flags
// ---------------------------------------------------------------------------

const runsRoot = Options.text("runs-root").pipe(
  Options.withDescription("The run archive to walk. Stored verbatim as the scope key on every row.")
)

const databaseUrl = Options.redacted("database-url").pipe(
  Options.withDescription("postgres:// connection string. Never logged, never echoed.")
)

const game = Options.text("game").pipe(
  Options.repeated,
  Options.withDescription(
    "Restrict the sweep to this game id; repeatable. A filtered sweep never deletes."
  )
)

const dryRun = Options.boolean("dry-run").pipe(
  Options.withDescription("Read, compare and report. Writes nothing, not even the sweep row.")
)

const skipMigrations = Options.boolean("skip-migrations").pipe(
  Options.withDescription("Do not apply the committed migrations before the sweep.")
)

const ppmHeadBytes = Options.integer("ppm-head-bytes").pipe(
  Options.withDefault(Ingest.DEFAULT_PPM_HEAD_BYTES),
  Options.withDescription(
    "Bytes of each *.map.ppm to store. The default dominates both readers' header caps."
  )
)

const maxBinaryRowBytes = Options.integer("max-binary-row-bytes").pipe(
  Options.withDefault(Ingest.DEFAULT_MAX_BINARY_ROW_BYTES),
  Options.withDescription("Refuse, rather than truncate, any single payload larger than this.")
)

/** Everything the command parsed. */
export interface IngestCliArgs {
  readonly runsRoot: string
  readonly databaseUrl: Redacted.Redacted
  readonly game: ReadonlyArray<string>
  readonly dryRun: boolean
  readonly skipMigrations: boolean
  readonly ppmHeadBytes: number
  readonly maxBinaryRowBytes: number
}

/** The parsed flags as {@link Ingest.IngestOptions}. */
export const ingestOptionsOf = (args: IngestCliArgs): Ingest.IngestOptions => ({
  ...Ingest.ingestOptions(args.runsRoot),
  gameIds: new Set(args.game),
  dryRun: args.dryRun,
  ppmHeadBytes: args.ppmHeadBytes,
  maxBinaryRowBytes: args.maxBinaryRowBytes
})

// ---------------------------------------------------------------------------
// The program
// ---------------------------------------------------------------------------

/**
 * Where the sweep is pointed, credential-free.
 *
 * A malformed URL is caught here rather than at connect time so the operator
 * gets the argparse-shaped one-liner instead of a driver stack trace — and so
 * that nothing has been written when it fails.
 */
const describeConnection = (
  url: Redacted.Redacted
): Effect.Effect<string, DatabaseUrlInvalid> =>
  Effect.map(
    describeTarget(url),
    (target) => `${target.host}:${target.port}/${target.database}`
  )

/**
 * The sweep, with the database layer built around it.
 *
 * The layer is built *inside* the handler, not around the command, because
 * `--dry-run` still needs a connection (it compares content hashes against the
 * stored ones) but a bad `--runs-root` must fail before one is opened.
 */
export const runIngest = (
  args: IngestCliArgs
): Effect.Effect<Ingest.IngestReport, Ingest.IngestError | DatabaseUrlInvalid> =>
  Effect.scoped(
    // The sweep is the whole program; this is where its layers belong.
    // oxlint-disable-next-line effecttsgo/strict-effect-provide
    Effect.provide(
      Effect.flatMap(
        args.skipMigrations ? Effect.void : Effect.asVoid(Effect.orDie(Migrate.run)),
        () => Ingest.ingest(ingestOptionsOf(args))
      ),
      Layer.provideMerge(
        Layer.merge(DatabaseLive, BunContext.layer),
        databaseConfigLayer(databaseConfig(args.databaseUrl))
      )
    )
  )

const handler = (args: IngestCliArgs): Effect.Effect<void, Ingest.IngestError | DatabaseUrlInvalid> =>
  Effect.gen(function*() {
    const connection = yield* describeConnection(args.databaseUrl)
    yield* Console.log(`${INGEST_CLI_NAME}: ${connection}`)
    const report = yield* runIngest(args)
    yield* Console.log(Ingest.describeReport(report))
  })

/** `arena-ingest`, as a `@effect/cli` command. */
export const ingestCommand = Command.make(
  INGEST_CLI_NAME,
  { runsRoot, databaseUrl, game, dryRun, skipMigrations, ppmHeadBytes, maxBinaryRowBytes },
  handler
)

/** The command as a runnable program over `argv`. */
export const runIngestCli = Command.run(ingestCommand, {
  name: INGEST_CLI_NAME,
  version: INGEST_CLI_VERSION
})

// ---------------------------------------------------------------------------
// The one error → exit-code site
// ---------------------------------------------------------------------------

/**
 * An error's public text.
 *
 * Every branch is deliberate about what it may say. A {@link DatabaseUrlInvalid}
 * describes the *shape* problem and never the URL; a {@link
 * Ingest.RunsRootUnreadable} names the directory, which is not a secret and is
 * the only thing an operator can act on; a `SqlError` is reduced to its
 * `message`, because its `cause` is a driver error object that can carry
 * connection parameters.
 */
export const describeIngestError = (error: unknown): string => {
  if (error instanceof DatabaseUrlInvalid) {
    return describeDatabaseUrlInvalid(error)
  }
  if (error instanceof Ingest.RunsRootUnreadable) {
    return `runs_root could not be listed: ${error.runsRoot}`
  }
  if (error instanceof Error && error.message !== "") {
    return error.message
  }
  return typeof error === "object" && error !== null && "_tag" in error &&
      typeof error._tag === "string"
    ? error._tag
    : "ingest failed"
}

/** One `error: …` line on stderr, and nothing at all for an interrupt. */
export const reportIngestFailure = (cause: Cause.Cause<unknown>): Effect.Effect<void> =>
  Cause.isInterruptedOnly(cause)
    ? Effect.void
    : Option.match(Cause.failureOption(cause), {
      onNone: () => Console.error(Cause.pretty(cause)),
      onSome: (error) => Console.error(`error: ${describeIngestError(error)}`)
    })

/**
 * Success → 0, interrupt → 0, anything else → 2.
 *
 * An interrupted sweep is not a failure: every run committed on its own, and the
 * sweep row is left `aborted`, which is exactly the state that forbids the next
 * sweep from treating a half-walk as a licence to delete.
 */
export const ingestTeardown: Teardown = <E, A>(
  exit: Exit.Exit<E, A>,
  onExit: (code: number) => void
): void => {
  onExit(
    Exit.isSuccess(exit)
      ? 0
      : Cause.isInterruptedOnly(exit.cause)
      ? 0
      : INGEST_CLI_ERROR_EXIT_CODE
  )
}

/** The process entry point, as a value a test can run without a process. */
export const main = (
  argv: ReadonlyArray<string>
): Effect.Effect<void, CliError> =>
  runIngestCli(argv).pipe(
    Effect.tapErrorCause(reportIngestFailure),
    // This is the entry point: the layer's scope is the process's.
    // oxlint-disable-next-line effecttsgo/strict-effect-provide
    Effect.provide(BunContext.layer)
  )

// Guarded so that importing this module — which the tests do, for the flag
// surface — does not start a sweep.
if (import.meta.main) {
  BunRuntime.runMain(main(process.argv), {
    disableErrorReporting: true,
    teardown: ingestTeardown
  })
}
