/**
 * The live Postgres client layer.
 *
 * `@effect/sql-pg`'s `PgClient.layer` is the *single* designated interop
 * boundary with the raw `pg` driver in this package: nothing above it ever
 * touches a driver promise, and nothing else in the repository constructs a
 * pool. Everything a caller needs to configure arrives through the
 * {@link DatabaseConfig} tag, so a gateway wires one `Layer.succeed` and never
 * threads a connection string through code.
 *
 * The connection URL is `Redacted` throughout, deliberately. Unlike the
 * gateway's `--service-url`, a database URL may carry credentials, so it must
 * never reach `/health`, a ready record, a log line or a startup-error string.
 * That is also why {@link DatabaseUrlInvalid} carries only a problem tag and
 * {@link DatabaseTarget} carries only host, port and database.
 */

import * as PgDrizzle from "@effect/sql-drizzle/Pg"
import * as PgClient from "@effect/sql-pg/PgClient"
import * as SqlClient from "@effect/sql/SqlClient"
import type { SqlError } from "@effect/sql/SqlError"
import * as Context from "effect/Context"
import * as Data from "effect/Data"
import type * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Either from "effect/Either"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Redacted from "effect/Redacted"

/** What is wrong with a database URL. Never the URL itself. */
export type DatabaseUrlProblem =
  /** Not parseable as a URL at all. */
  | "malformed"
  /** Not a `postgres:` / `postgresql:` URL. */
  | "scheme"
  /** No host — a socket path or a bare `postgres:///db` cannot be reached. */
  | "host"

const PROBLEM_MESSAGE: Record<DatabaseUrlProblem, string> = {
  malformed: "database url is not a valid URL",
  scheme: "database url must use the postgres:// or postgresql:// scheme",
  host: "database url must name a host"
}

/**
 * A database URL that cannot be used. The URL is not carried, and neither is
 * any fragment of it: this error is printed at the gateway's one exit-2 site.
 */
export class DatabaseUrlInvalid extends Data.TaggedError("DatabaseUrlInvalid")<{
  readonly problem: DatabaseUrlProblem
}> {}

/** Human-readable form of a {@link DatabaseUrlInvalid}, safe to print. */
export const describeDatabaseUrlInvalid = (error: DatabaseUrlInvalid): string =>
  PROBLEM_MESSAGE[error.problem]

/** The credential-free description of where a client is pointed. */
export interface DatabaseTarget {
  readonly host: string
  readonly port: number
  readonly database: string
}

/** Everything the live client needs, and nothing it does not. */
export interface DatabaseConfigValues {
  /** Redacted so it cannot be logged by accident. */
  readonly url: Redacted.Redacted
  /** Reported to Postgres as `application_name`. */
  readonly applicationName: string
  readonly maxConnections: Option.Option<number>
  readonly connectTimeout: Option.Option<Duration.DurationInput>
}

/** How a consumer supplies the connection. */
export class DatabaseConfig extends Context.Tag("@arena/db/DatabaseConfig")<
  DatabaseConfig,
  DatabaseConfigValues
>() {}

/** The default configuration for a URL: no pool tuning, arena application name. */
export const databaseConfig = (url: Redacted.Redacted): DatabaseConfigValues => ({
  url,
  applicationName: "arena-db",
  maxConnections: Option.none(),
  connectTimeout: Option.none()
})

/** `DatabaseConfig` as a layer, for the one place that knows the URL. */
export const databaseConfigLayer = (
  values: DatabaseConfigValues
): Layer.Layer<DatabaseConfig> => Layer.succeed(DatabaseConfig, values)

/**
 * Session settings pinned on every connection.
 *
 * `bytea_output = 'hex'` so an operator's `ALTER DATABASE … SET bytea_output =
 * 'escape'` cannot change how a PNG comes off the wire, and
 * `client_encoding = 'UTF8'` so a server-side default cannot re-encode a
 * document whose bytes are the contract. They travel in the startup packet's
 * `options` parameter, which `pg` forwards verbatim, rather than as a `SET`
 * statement — a `SET` would only apply to whichever pooled connection happened
 * to run it.
 */
export const PINNED_SESSION_OPTIONS = "-c bytea_output=hex -c client_encoding=UTF8"

const parseUrl = (raw: string): Option.Option<URL> =>
  URL.canParse(raw) ? Option.some(new URL(raw)) : Option.none()

const POSTGRES_PROTOCOLS: ReadonlySet<string> = new Set(["postgres:", "postgresql:"])

const validate = (raw: string): Either.Either<URL, DatabaseUrlInvalid> =>
  Option.match(parseUrl(raw), {
    onNone: () => Either.left(new DatabaseUrlInvalid({ problem: "malformed" })),
    onSome: (url) =>
      !POSTGRES_PROTOCOLS.has(url.protocol)
        ? Either.left(new DatabaseUrlInvalid({ problem: "scheme" }))
        : url.hostname === ""
        ? Either.left(new DatabaseUrlInvalid({ problem: "host" }))
        : Either.right(url)
  })

/**
 * Where a URL points, with the credentials dropped. This is the only shape of a
 * database URL that may be shown to anyone.
 */
export const describeTarget = (
  url: Redacted.Redacted
): Either.Either<DatabaseTarget, DatabaseUrlInvalid> =>
  Either.map(validate(Redacted.value(url)), (parsed) => ({
    host: parsed.hostname,
    port: parsed.port === "" ? 5432 : Number(parsed.port),
    database: parsed.pathname.replace(/^\//, "")
  }))

/**
 * The URL with {@link PINNED_SESSION_OPTIONS} folded into its `options` query
 * parameter, preserving anything the caller already put there. `pg` copies every
 * query parameter of a connection string into its config, and forwards `options`
 * in the startup packet.
 */
export const pinSessionOptions = (
  url: Redacted.Redacted
): Either.Either<Redacted.Redacted, DatabaseUrlInvalid> =>
  Either.map(validate(Redacted.value(url)), (parsed) => {
    const existing = parsed.searchParams.get("options")
    const pinned = existing === null || existing.trim() === ""
      ? PINNED_SESSION_OPTIONS
      // The caller's options go last so an explicit override still wins.
      : `${PINNED_SESSION_OPTIONS} ${existing}`
    const withOptions = new URL(parsed.href)
    withOptions.searchParams.set("options", pinned)
    return Redacted.make(withOptions.href)
  })

const clientConfig = (
  values: DatabaseConfigValues,
  url: Redacted.Redacted
): PgClient.PgClientConfig => ({
  url,
  applicationName: values.applicationName,
  maxConnections: Option.getOrUndefined(values.maxConnections),
  connectTimeout: Option.getOrUndefined(values.connectTimeout)
  // `transformQueryNames` / `transformResultNames` are deliberately unset. They
  // install a camelCase<->snake_case rewriter that, with `transformJson` on,
  // descends into JSON column values and rewrites their keys — silent corruption
  // of a payload whose bytes are the contract.
})

/**
 * The live `SqlClient` (and `PgClient`) over a real Postgres server.
 *
 * Both tags are published because `PgClient.layer` publishes both; depend on
 * `SqlClient` unless you specifically need `listen`/`notify`, so the same code
 * runs against the hermetic PGlite client in tests.
 */
export const PgClientLive: Layer.Layer<
  SqlClient.SqlClient | PgClient.PgClient,
  SqlError | DatabaseUrlInvalid,
  DatabaseConfig
> = Layer.unwrapEffect(
  Effect.gen(function*() {
    const values = yield* DatabaseConfig
    const url = yield* pinSessionOptions(values.url)
    return PgClient.layer(clientConfig(values, url))
  })
)

/**
 * The live client plus a drizzle handle over it.
 *
 * Note that `db.transaction(...)` throws under `@effect/sql-drizzle` — its
 * `pg-proxy` session has no transaction support. Use
 * `SqlClient.withTransaction`, which wraps every statement issued inside the
 * effect, drizzle's included, because the drizzle callback re-enters this same
 * client.
 */
export const DatabaseLive: Layer.Layer<
  PgDrizzle.PgDrizzle | SqlClient.SqlClient | PgClient.PgClient,
  SqlError | DatabaseUrlInvalid,
  DatabaseConfig
> = Layer.provideMerge(PgDrizzle.layer, PgClientLive)
