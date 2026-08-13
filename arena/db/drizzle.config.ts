/**
 * drizzle-kit configuration.
 *
 * `bunx drizzle-kit generate` diffs `src/schema.ts` against the snapshot in
 * `drizzle/meta` and emits a numbered `.sql` file. It never touches a live
 * database, so no credentials are needed to generate — and the emitted `.sql`
 * is exactly what a DBA would review, which is why the migrations are committed
 * as SQL rather than as a schema dump.
 *
 * The same committed folder is applied to PGlite in the hermetic tests and to
 * live Postgres in production (`src/migrate.ts`), so schema drift between the
 * two environments is impossible by construction.
 */

import { defineConfig } from "drizzle-kit"

export default defineConfig({
  dialect: "postgresql",
  schema: "./src/schema.ts",
  out: "./drizzle"
})

// Two things worth knowing before editing this file:
//
//   * `tablesFilter` is honoured by `push`/`pull` only, never by `generate` —
//     every table exported from `schema` lands in the migration. Keep the schema
//     file free of anything that is not part of the arena schema.
//   * Never hand-edit an emitted `.sql`. `src/schema.ts`, `drizzle/meta/*` and
//     the `.sql` files must agree, or the next `generate` diffs against a
//     snapshot that never happened. Storage tuning and other things drizzle-kit
//     does not model belong in a `generate --custom` migration, which gets a
//     real snapshot of its own.
