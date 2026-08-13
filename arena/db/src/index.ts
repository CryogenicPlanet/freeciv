/**
 * `@arena/db` — the Postgres persistence layer behind the pg-backed gateway.
 *
 * The package is deliberately thin at the edges: schema definitions and Effect
 * `Layer`s live here, and every consumer talks to Postgres through `@effect/sql`
 * so that queries are Effects with typed errors rather than raw driver promises.
 *
 * ## This package and `@arena/harness` depend on each other, and say so
 *
 * The dependency is mutual and both directions are declared in `package.json`:
 *
 * - `@arena/harness` → `@arena/db`: `gateway/main.ts` does a *dynamic*
 *   `import('@arena/db')`, so a filesystem gateway never resolves a database
 *   driver at all.
 * - `@arena/db` → `@arena/harness`: `runs-repository-pg.ts` imports the gateway's
 *   pure projections — `diskGameRow`, `terminalArchiveView`, `archiveVictory`,
 *   `selectFramePng`, `gamesIndex`, `sortDiskGameRows` — and calls them with the
 *   same arguments the fs repository does. **That is the byte-parity contract**:
 *   re-implementing any of them here would be a second place for the same answer
 *   to live, and the two copies would drift. `ingest.ts` imports the four archive
 *   filenames and the tail window for the same reason.
 *
 * Both edges are spelled as paths into `../../harness/src/gateway/…` rather than
 * as `@arena/harness/…` because that package's `exports` map publishes only its
 * barrel, and the barrel deliberately re-exports no gateway internals: "a caller
 * that needs `dispatch` or `RunsRepository` should import the module that owns it
 * so the citation travels with the import". The cycle is real either way; what
 * matters is that the manifest states it, so nobody reasons from a module graph
 * the code does not keep.
 *
 * - `Schema` — the drizzle tables, with the byte-fidelity rules that shaped them.
 * - `Migrate` — applying the committed `drizzle/` migrations.
 * - `Client` — the live `@effect/sql-pg` layer, configured through a tag.
 * - `Pglite` — the hermetic in-process backend the tests run against.
 * - `Ingest` — the runs_root → Postgres sweep (idempotent, content-hash keyed).
 *
 * ## Two modules that used to be here, and why they are not
 *
 * `Materialize` and `DerivationCachePg` existed for one reason: a pg-backed
 * gateway had no run directory, so saves and frames were rebuilt into a
 * `--materialize-root` and the replay cache was mirrored into the database.
 * Schema v2 stores no artifacts and no paths, and every backend resolves a run
 * as `<--runs-root>/<game_id>` — so `TerminalArchive.runRoot` is a real
 * directory again, `http/routes/archive.ts`'s three reach-arounds work
 * unmodified, and the derivation cache goes back to the disk `cache_root` the
 * filesystem gateway already uses. Both modules are deleted rather than
 * emptied: a materializer that materializes nothing is a lock nobody takes.
 */

export * as Client from "./client.ts"
export * as Ingest from "./ingest.ts"
export * as Migrate from "./migrate.ts"
export * as Pglite from "./pglite.ts"
export * as RunsRepositoryPg from "./runs-repository-pg.ts"
export * as Schema from "./schema.ts"

/** Marker for the package's scaffold, exercised by the scaffold test. */
export const packageName: string = "@arena/db"
