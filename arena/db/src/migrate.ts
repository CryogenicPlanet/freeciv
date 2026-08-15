/**
 * Applying the committed drizzle-kit migrations through `@effect/sql`.
 *
 * Two things are worth stating, because both were found the hard way:
 *
 * 1. `@effect/sql/Migrator/FileSystem.fromFileSystem` matches only files named
 *    `<digits>_<name>.(js|ts)`. It **silently ignores** every `.sql` file
 *    drizzle-kit emits — no error, just zero migrations applied. So this module
 *    supplies the missing `Loader`, driven by drizzle-kit's `meta/_journal.json`.
 * 2. `@effect/sql-pg`'s `PgMigrator` demands the `PgClient` tag *and* a
 *    `CommandExecutor` (it shells out to `pg_dump`), which makes it unusable
 *    against PGlite. The generic `Migrator.make({})` needs only `SqlClient`, so
 *    the identical migrations apply to PGlite in the hermetic tests and to a
 *    live server in production. Schema drift between the two is then impossible
 *    by construction — which is exactly what the byte-parity differential legs
 *    depend on.
 */

import { FileSystem } from "@effect/platform/FileSystem"
import { Path } from "@effect/platform/Path"
import * as Migrator from "@effect/sql/Migrator"
import * as SqlClient from "@effect/sql/SqlClient"
import type { SqlError } from "@effect/sql/SqlError"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import { fileURLToPath } from "node:url"

/** The committed migrations folder — `drizzle/`, next to `src/`. */
export const migrationsDirectory: string = fileURLToPath(new URL("../drizzle", import.meta.url))

/** drizzle-kit's `meta/_journal.json`, narrowed to the fields the loader needs. */
const Journal = Schema.Struct({
  entries: Schema.Array(Schema.Struct({
    idx: Schema.Number,
    tag: Schema.String
  }))
})

const decodeJournal = Schema.decodeUnknown(Schema.parseJson(Journal))

const migrationError = (message: string): Migrator.MigrationError =>
  new Migrator.MigrationError({ reason: "failed", message })

/** drizzle-kit separates statements within one migration with this marker. */
const BREAKPOINT = "--> statement-breakpoint"

const statements = (source: string): ReadonlyArray<string> =>
  source
    .split(BREAKPOINT)
    .map((chunk) => chunk.trim())
    .filter((chunk) => chunk.length > 0)

const applyFile = (contents: string): Effect.Effect<void, never, SqlClient.SqlClient> =>
  Effect.flatMap(
    SqlClient.SqlClient,
    (client) =>
      Effect.forEach(statements(contents), (statement) => Effect.orDie(client.unsafe(statement)), {
        discard: true
      })
  )

/**
 * A `Migrator.Loader` over a drizzle-kit output folder.
 *
 * Migration ids come from the journal's `idx` offset by one, because
 * `@effect/sql` reads id 0 as "nothing applied".
 */
export const fromDrizzleFolder = (directory: string): Migrator.Loader<FileSystem | Path> =>
  Effect.gen(function*() {
    const fs = yield* FileSystem
    const path = yield* Path

    const journalPath = path.join(directory, "meta", "_journal.json")
    const raw = yield* Effect.mapError(
      fs.readFileString(journalPath),
      (error) => migrationError(`Failed to read ${journalPath}: ${error.message}`)
    )

    const journal = yield* Effect.mapError(
      decodeJournal(raw),
      (error) => migrationError(`Invalid drizzle journal at ${journalPath}: ${error.message}`)
    )

    return yield* Effect.forEach(journal.entries, (entry) => {
      const sqlPath = path.join(directory, `${entry.tag}.sql`)
      return Effect.map(
        Effect.mapError(
          fs.readFileString(sqlPath),
          (error) => migrationError(`Failed to read ${sqlPath}: ${error.message}`)
        ),
        // `load` is the `import()` slot, not the migration. The Migrator *runs*
        // it and then does `Effect.isEffect(_) ? _ : _.default`, so returning the
        // migration Effect directly makes it dereference `.default` on that
        // Effect's result and die with a TypeError.
        (contents): Migrator.ResolvedMigration => [
          entry.idx + 1,
          entry.tag,
          Effect.succeed(applyFile(contents))
        ]
      )
    })
  })

/**
 * Apply a drizzle-kit migrations folder against whatever `SqlClient` is in
 * context. Applied migrations are stamped in `effect_sql_migrations`, so this is
 * idempotent: a second run returns an empty array.
 */
export const runFrom = (
  directory: string
): Effect.Effect<
  ReadonlyArray<readonly [id: number, name: string]>,
  Migrator.MigrationError | SqlError,
  SqlClient.SqlClient | FileSystem | Path
> => Migrator.make({})({ loader: fromDrizzleFolder(directory) })

/** {@link runFrom} against this package's committed {@link migrationsDirectory}. */
export const run: Effect.Effect<
  ReadonlyArray<readonly [id: number, name: string]>,
  Migrator.MigrationError | SqlError,
  SqlClient.SqlClient | FileSystem | Path
> = Effect.suspend(() => runFrom(migrationsDirectory))
