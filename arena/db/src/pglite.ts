/**
 * A `SqlClient` implementation backed by PGlite (in-process WASM Postgres).
 *
 * This is the hermetic test backend: the same `@effect/sql` / `@effect/sql-drizzle`
 * code that runs against `@effect/sql-pg` in production runs against this layer in
 * tests, without any live Postgres server. Only the driver calls themselves are an
 * interop boundary — everything above them stays in Effect.
 */

import * as Reactivity from "@effect/experimental/Reactivity"
import * as PgClient from "@effect/sql-pg/PgClient"
import * as Client from "@effect/sql/SqlClient"
import type { Connection } from "@effect/sql/SqlConnection"
import { SqlError } from "@effect/sql/SqlError"
import { PGlite } from "@electric-sql/pglite"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import type * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"

/**
 * Where a PGlite database lives.
 *
 * `memory` keeps everything in WASM memory and disappears with the process;
 * `directory` persists to a data directory on disk, so the database survives
 * `close` and can be reopened or inspected between runs.
 */
export type PgliteLocation =
  | { readonly _tag: "memory" }
  | { readonly _tag: "directory"; readonly path: string }

/** An in-memory PGlite database. */
export const memory: PgliteLocation = { _tag: "memory" }

/** A PGlite database persisted under `path`. */
export const directory = (path: string): PgliteLocation => ({ _tag: "directory", path })

/** Handle to a live PGlite instance, for tests that need the raw driver. */
export class Pglite extends Context.Tag("@arena/db/Pglite")<Pglite, PGlite>() {}

const dataDir = (location: PgliteLocation): string | undefined =>
  location._tag === "directory" ? location.path : undefined

const sqlFail = (message: string) => (cause: unknown): SqlError => new SqlError({ cause, message })

/**
 * PGlite's single-user Postgres bootstrap returns 99 as its internal success
 * sentinel. Bun 1.4 exposes that WASM return through `process.exitCode`, even
 * though PGlite handles it as success, so restore the caller's status once the
 * database is ready instead of making an otherwise-green process exit 99.
 */
const create = async (location: PgliteLocation): Promise<PGlite> => {
  const previousExitCode = process.exitCode ?? 0
  try {
    return await PGlite.create(dataDir(location))
  } finally {
    if (process.exitCode === 99) process.exitCode = previousExitCode
  }
}

/**
 * Acquire a PGlite instance as a scoped resource. The database is closed when the
 * scope closes, so a test's layer teardown is also the database teardown.
 */
export const make = (
  location: PgliteLocation
): Effect.Effect<PGlite, SqlError, Scope.Scope> =>
  Effect.acquireRelease(
    Effect.tryPromise({
      try: () => create(location),
      catch: sqlFail("Failed to start PGlite")
    }),
    (db) => Effect.ignoreLogged(Effect.promise(() => db.close()))
  )

// PGlite mutates nothing but types its params as a mutable array, so copy.
const mutable = (params: ReadonlyArray<unknown>): Array<unknown> => [...params]

const connection = (db: PGlite): Connection => {
  const run = <A>(promise: () => Promise<A>, message: string): Effect.Effect<A, SqlError> =>
    Effect.tryPromise({ try: promise, catch: sqlFail(message) })

  const rows = (
    sql: string,
    params: ReadonlyArray<unknown>,
    transformRows: (<A extends object>(row: ReadonlyArray<A>) => ReadonlyArray<A>) | undefined
  ): Effect.Effect<ReadonlyArray<object>, SqlError> =>
    run(() => db.query<object>(sql, mutable(params)), "Failed to execute statement").pipe(
      Effect.map((result) => (transformRows === undefined ? result.rows : transformRows(result.rows)))
    )

  return {
    execute: rows,
    executeUnprepared: rows,
    executeRaw: (sql, params) =>
      run(() => db.query(sql, mutable(params)), "Failed to execute raw statement"),
    executeValues: (sql, params) =>
      run(
        () => db.query<ReadonlyArray<unknown>>(sql, mutable(params), { rowMode: "array" }),
        "Failed to execute statement"
      ).pipe(Effect.map((result) => result.rows)),
    executeStream: (sql, params, transformRows) =>
      Stream.unwrap(Effect.map(rows(sql, params, transformRows), Stream.fromIterable))
  }
}

/**
 * Build a `SqlClient` over an already-running PGlite instance.
 *
 * The compiler is the real Postgres one from `@effect/sql-pg`, so statements are
 * compiled to the exact `$1`-style SQL that the production client emits, and
 * transaction control uses the default `BEGIN` / `SAVEPOINT` / `COMMIT` verbs.
 */
export const makeClient = (
  db: PGlite
): Effect.Effect<Client.SqlClient, never, Reactivity.Reactivity> =>
  Client.make({
    acquirer: Effect.succeed(connection(db)),
    compiler: PgClient.makeCompiler(),
    spanAttributes: [["db.system", "postgresql"], ["db.client", "pglite"]]
  })

/**
 * `SqlClient` layer backed by a single PGlite instance, which is also exposed as
 * `Pglite` for tests that need the raw driver.
 */
export const layer = (
  location: PgliteLocation
): Layer.Layer<Client.SqlClient | Pglite, SqlError> => {
  const pglite = Layer.scoped(Pglite, make(location))
  const client = Layer.effect(Client.SqlClient, Effect.flatMap(Pglite, makeClient)).pipe(
    Layer.provide(Reactivity.layer),
    Layer.provide(pglite)
  )
  // `pglite` is memoized by reference during layer building, so both the client
  // and the exposed handle observe the same database.
  return Layer.merge(client, pglite)
}
