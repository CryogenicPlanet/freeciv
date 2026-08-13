/**
 * The arena schema, exercised hermetically against PGlite.
 *
 * Four things are pinned here, and each of them is a byte-parity contract
 * rather than a database nicety:
 *
 * 1. The committed `drizzle/` migrations apply from an empty database, produce
 *    exactly the declared tables, enums and column storage, and are idempotent.
 * 2. `bytea` round-trips *exact* bytes — including the canonical JSON fixtures
 *    from `@arena/wire`, whose int-vs-float spelling (`1.0` vs `1`) is the whole
 *    reason payloads are not stored as `json`/`jsonb`/`text`.
 * 3. `byte_size` is an independent stored gate, not `octet_length(bytes)`.
 * 4. The primary keys really are the ingest idempotency keys, and the child rows
 *    cascade with the run.
 *
 * The same migrations and the same drizzle statements run against live Postgres,
 * so anything asserted here is asserted about production.
 */

import { BunContext } from "@effect/platform-bun"
import { FileSystem } from "@effect/platform/FileSystem"
import { Path } from "@effect/platform/Path"
import * as PgDrizzle from "@effect/sql-drizzle/Pg"
import * as SqlClient from "@effect/sql/SqlClient"
import { eq, getTableName } from "drizzle-orm"
import * as Effect from "effect/Effect"
import * as Either from "effect/Either"
import * as Layer from "effect/Layer"
import * as Redacted from "effect/Redacted"
import { describe, expect, it } from "bun:test"
import { fileURLToPath } from "node:url"

import * as Client from "../src/client.ts"
import * as Migrate from "../src/migrate.ts"
import * as Pglite from "../src/pglite.ts"
import {
  arenaTables,
  derivationCache,
  derivationWorkdirs,
  ingestSweeps,
  runDocuments,
  runFrames,
  runReplayTail,
  runs,
  runSaves,
  runVideos
} from "../src/schema.ts"

// ---------------------------------------------------------------- harness ---

const testLayer = Layer.mergeAll(
  Layer.provideMerge(PgDrizzle.layer, Pglite.layer(Pglite.memory)),
  BunContext.layer
)

type TestContext = Layer.Layer.Success<typeof testLayer>

// This *is* the entry point: one `run` per test, composing every layer at once,
// with the enclosing `Effect.scoped` closing the PGlite instance afterwards. Each
// test therefore gets a fresh, empty database, which is what makes "migrations
// apply from zero" a real assertion rather than a replay of an existing schema.
const run = <A, E>(effect: Effect.Effect<A, E, TestContext>): Promise<A> =>
  // oxlint-disable-next-line effecttsgo/strict-effect-provide
  Effect.runPromise(Effect.scoped(Effect.provide(effect, testLayer)))

/** A migrated, empty database. */
const migrated = Effect.gen(function*() {
  yield* Migrate.run
  return yield* PgDrizzle.PgDrizzle
})

const RUNS_ROOT = "/srv/arena/.agent-eval/runs"
const GAME_ID = "game_parity_terminal_valid_01"

/** One sweep plus one run, so child rows have a parent to hang off. */
const seedRun = (db: PgDrizzle.PgDrizzle["Type"], gameId: string, runsRoot: string = RUNS_ROOT) =>
  Effect.gen(function*() {
    const sweeps = yield* db.insert(ingestSweeps).values({ runsRoot, status: "running" })
      .returning({ sweepId: ingestSweeps.sweepId })
    const sweepId = sweeps[0]?.sweepId ?? 0
    yield* db.insert(runs).values({
      runsRoot,
      gameId,
      contentHash: new Uint8Array(32).fill(7),
      sweepId,
      framesDirOk: true,
      savesDirOk: true
    })
    return sweepId
  })

/** Every byte value, so nothing can be laundered by a text codec. */
const allBytes = new Uint8Array(Array.from({ length: 256 }, (_, i) => i))

/** Bytes a naive escape/encode round trip would mangle. */
const nastyBytes = new Uint8Array([0x00, 0x5c, 0x78, 0x22, 0x27, 0x0a, 0x0d, 0xff, 0xfe, 0xc3, 0x28])

const sameBytes = (actual: Uint8Array | undefined, expected: Uint8Array): void => {
  expect(actual?.length).toBe(expected.length)
  expect(Array.from(actual ?? new Uint8Array())).toEqual(Array.from(expected))
}

/** Ten canonical JSON documents from `@arena/wire`'s fixture corpus. */
const fixturesRoot = fileURLToPath(new URL("../../wire/test/fixtures/runs", import.meta.url))

const sampledFixtures = Effect.gen(function*() {
  const fs = yield* FileSystem
  const path = yield* Path
  const entries = yield* fs.readDirectory(fixturesRoot, { recursive: true })
  const sampled = entries.filter((entry) => entry.endsWith(".json")).toSorted().slice(0, 10)
  return yield* Effect.forEach(sampled, (name) =>
    Effect.map(
      fs.readFile(path.join(fixturesRoot, name)),
      (bytes) => ({ name, bytes } as const)
    ))
})

const isFailure = <A, E>(effect: Effect.Effect<A, E, TestContext>) =>
  Effect.map(Effect.either(effect), Either.isLeft)

// ------------------------------------------------------------- migrations ---

describe("committed drizzle migrations", () => {
  it("apply from an empty database and are idempotent", () =>
    run(Effect.gen(function*() {
      const applied = yield* Migrate.run
      const reapplied = yield* Migrate.run

      expect(applied.map(([, name]) => name)).toEqual([
        "0000_arena_schema",
        "0001_storage_external",
        "0002_byte_size_bigint_and_prunable_sweeps"
      ])
      // Migration ids are journal `idx + 1`: @effect/sql reads id 0 as "nothing
      // applied", so a zero-based id would replay forever.
      expect(applied.map(([id]) => id)).toEqual([1, 2, 3])
      expect(reapplied).toEqual([])
    })))

  it("create exactly the declared tables, and nothing else", () =>
    run(Effect.gen(function*() {
      yield* Migrate.run
      const client = yield* SqlClient.SqlClient
      const rows = yield* client.unsafe<{ table_name: string }>(
        `select table_name from information_schema.tables
         where table_schema = 'public' order by table_name`
      )
      const names = rows.map((row) => row.table_name)

      const declared: ReadonlyArray<string> = arenaTables.map(getTableName)
      expect(declared.length).toBe(9)

      // Every declared table exists...
      expect(names).toEqual(expect.arrayContaining([...declared]))
      // ...and the only extra is the Migrator's own bookkeeping table.
      expect(names.filter((name) => !declared.includes(name))).toEqual(["effect_sql_migrations"])
      // The phase-0 spike table is gone for good.
      expect(names).not.toContain("blobs")
    })))

  it("create the three status enums with their labels in order", () =>
    run(Effect.gen(function*() {
      yield* Migrate.run
      const client = yield* SqlClient.SqlClient
      const rows = yield* client.unsafe<{ typname: string; enumlabel: string }>(
        `select t.typname, e.enumlabel from pg_type t
         join pg_enum e on e.enumtypid = t.oid
         where t.typname in ('document_kind', 'document_status', 'save_kind')
         order by t.typname, e.enumsortorder`
      )
      const labels = (name: string) =>
        rows.filter((row) => row.typname === name).map((row) => row.enumlabel)

      expect(labels("document_kind")).toEqual(["manifest", "report", "victory"])
      expect(labels("document_status")).toEqual(["ok", "unusable"])
      expect(labels("save_kind")).toEqual(["autosave", "ppm", "other"])
    })))

  it("set EXTERNAL storage on the already-compressed payload columns only", () =>
    run(Effect.gen(function*() {
      yield* Migrate.run
      const client = yield* SqlClient.SqlClient
      const rows = yield* client.unsafe<{ relname: string; attname: string; attstorage: string }>(
        `select c.relname, a.attname, a.attstorage from pg_attribute a
         join pg_class c on c.oid = a.attrelid
         where c.relname in ('run_frames', 'run_videos', 'run_saves', 'run_documents')
           and a.attname in ('bytes', 'head_bytes')
         order by c.relname, a.attname`
      )
      const storage = (relname: string, attname: string) =>
        rows.find((row) => row.relname === relname && row.attname === attname)?.attstorage

      // 'e' = EXTERNAL: out of line, uncompressed. PNG, H.264 and gzip do not
      // compress again, so the LZ pass would be pure cost.
      expect(storage("run_frames", "bytes")).toBe("e")
      expect(storage("run_videos", "bytes")).toBe("e")
      expect(storage("run_saves", "head_bytes")).toBe("e")
      // 'x' = EXTENDED, the default: JSON text compresses roughly 4x.
      expect(storage("run_documents", "bytes")).toBe("x")
    })))
})

// ------------------------------------------------------------ byte parity ---

describe("bytea round-trips exact bytes", () => {
  it("preserves every byte value, the escape-hostile set, and an empty payload", () =>
    run(Effect.gen(function*() {
      const db = yield* migrated
      yield* seedRun(db, GAME_ID)

      yield* db.insert(runFrames).values([
        { runsRoot: RUNS_ROOT, gameId: GAME_ID, name: "all.png", frameIndex: 0, bytes: allBytes, byteSize: 256, sha256: nastyBytes },
        { runsRoot: RUNS_ROOT, gameId: GAME_ID, name: "nasty.png", frameIndex: 1, bytes: nastyBytes, byteSize: 11, sha256: nastyBytes },
        { runsRoot: RUNS_ROOT, gameId: GAME_ID, name: "empty.png", frameIndex: null, bytes: new Uint8Array(0), byteSize: 0, sha256: nastyBytes }
      ])

      const rows = yield* db.select().from(runFrames).orderBy(runFrames.name)
      expect(rows.map((row) => row.name)).toEqual(["all.png", "empty.png", "nasty.png"])
      sameBytes(rows[0]?.bytes, allBytes)
      sameBytes(rows[1]?.bytes, new Uint8Array(0))
      sameBytes(rows[2]?.bytes, nastyBytes)
      // A name that `decodeArchivePngName` cannot index is listed, not dropped.
      expect(rows[1]?.frameIndex).toBeNull()
    })))

  it("preserves @arena/wire's canonical JSON fixtures byte for byte", () =>
    run(Effect.gen(function*() {
      const db = yield* migrated
      const fixtures = yield* sampledFixtures
      expect(fixtures.length).toBe(10)

      yield* Effect.forEach(fixtures, (fixture, index) =>
        Effect.gen(function*() {
          const gameId = `game_fixture_${String(index).padStart(2, "0")}`
          yield* seedRun(db, gameId)
          yield* db.insert(runDocuments).values({
            runsRoot: RUNS_ROOT,
            gameId,
            kind: "manifest",
            status: "ok",
            bytes: fixture.bytes,
            byteSize: fixture.bytes.length,
            sha256: nastyBytes
          })
        }), { discard: true })

      const stored = yield* db.select().from(runDocuments).orderBy(runDocuments.gameId)
      expect(stored.length).toBe(10)

      const decoder = new TextDecoder("utf-8", { fatal: true })
      fixtures.forEach((fixture, index) => {
        const row = stored[index]
        sameBytes(row?.bytes, fixture.bytes)
        // The text is what the fs backend would have decoded: identical
        // whitespace, key order, and numeric spelling. Postgres normalises none
        // of it, which `json`/`jsonb` would have.
        expect(decoder.decode(row?.bytes ?? new Uint8Array())).toBe(decoder.decode(fixture.bytes))
      })
    })))

  it("keeps int-vs-float spelling that jsonb would normalise away", () =>
    run(Effect.gen(function*() {
      const db = yield* migrated
      yield* seedRun(db, GAME_ID)

      // `1.0` must not become `1`, `1e3` must not become `1000`, and the NUL
      // escape must survive: `victory.json`'s turn/year are copied out of this
      // spelling with no validation at all.
      const document = `{"turn": 753, "year": 1995.0, "scaled": 1e3, "nul": "a\\u0000b"}`
      const bytes = new TextEncoder().encode(document)

      yield* db.insert(runDocuments).values({
        runsRoot: RUNS_ROOT,
        gameId: GAME_ID,
        kind: "victory",
        status: "ok",
        bytes,
        byteSize: bytes.length,
        sha256: nastyBytes
      })

      const rows = yield* db.select().from(runDocuments)
      sameBytes(rows[0]?.bytes, bytes)
      expect(new TextDecoder().decode(rows[0]?.bytes ?? new Uint8Array())).toBe(document)
    })))

  it("stores byte_size as an independent gate, not octet_length(bytes)", () =>
    run(Effect.gen(function*() {
      const db = yield* migrated
      yield* seedRun(db, GAME_ID)

      // The fs reader takes the size from fstat and then `readFully` may come up
      // short, so `bytes.length` can be below `byte_size` — and it is `byte_size`
      // that decides the <=0 / >8MiB 503.
      const head = new Uint8Array(64).fill(0x50)
      yield* db.insert(runSaves).values({
        runsRoot: RUNS_ROOT,
        gameId: GAME_ID,
        name: "turn-0000000012-auto.map.ppm",
        kind: "ppm",
        saveTurn: 12,
        byteSize: 2_449_182,
        headBytes: head,
        isWhole: false,
        sha256: nastyBytes
      })

      const rows = yield* db.select().from(runSaves)
      expect(rows[0]?.byteSize).toBe(2_449_182)
      expect(rows[0]?.headBytes.length).toBe(64)
      expect(rows[0]?.isWhole).toBe(false)
      // int8 comes back as `bigint` under PGlite and as `string` under `pg`;
      // `{ mode: "number" }` pins both to a plain number, so the hermetic tests
      // predict live behaviour and nothing throws inside JSON.stringify.
      expect(typeof rows[0]?.byteSize).toBe("number")
      expect(typeof rows[0]?.saveTurn).toBe("number")
    })))

  it("stores the replay tail as a byte window, partial first line included", () =>
    run(Effect.gen(function*() {
      const db = yield* migrated
      yield* seedRun(db, GAME_ID)

      // A window cut mid-line is the normal case, not an error case.
      const tail = new TextEncoder().encode(`rn": 12}\n{"turn": 13}\n`)
      yield* db.insert(runReplayTail).values({
        runsRoot: RUNS_ROOT,
        gameId: GAME_ID,
        byteSize: 1_543_053,
        tailBytes: tail,
        sha256: nastyBytes
      })

      const rows = yield* db.select().from(runReplayTail)
      sameBytes(rows[0]?.tailBytes, tail)
      expect(rows[0]?.byteSize).toBe(1_543_053)
      expect(typeof rows[0]?.byteSize).toBe("number")
    })))
})

// -------------------------------------------------------- idempotency keys ---

describe("idempotency keys", () => {
  it("rejects a second row for the same (runs_root, game_id)", () =>
    run(Effect.gen(function*() {
      const db = yield* migrated
      const sweepId = yield* seedRun(db, GAME_ID)

      const duplicate = db.insert(runs).values({
        runsRoot: RUNS_ROOT,
        gameId: GAME_ID,
        contentHash: new Uint8Array(32).fill(9),
        sweepId,
        framesDirOk: false,
        savesDirOk: false
      })
      expect(yield* isFailure(duplicate)).toBe(true)

      // The same game id under a *different* logical root is a different run:
      // one database holds several roots, and `runsRoot` keeps its meaning.
      yield* db.insert(runs).values({
        runsRoot: "/other/root",
        gameId: GAME_ID,
        contentHash: new Uint8Array(32).fill(9),
        sweepId,
        framesDirOk: false,
        savesDirOk: false
      })
      expect((yield* db.select().from(runs)).length).toBe(2)
    })))

  it("allows two runs to share a content hash — the PK is the key, the hash is the detector", () =>
    run(Effect.gen(function*() {
      const db = yield* migrated
      const sweepId = yield* seedRun(db, GAME_ID)
      // Two runs with identical archives hash identically, by construction.
      yield* db.insert(runs).values({
        runsRoot: RUNS_ROOT,
        gameId: "game_parity_terminal_valid_02",
        contentHash: new Uint8Array(32).fill(7),
        sweepId,
        framesDirOk: true,
        savesDirOk: true
      })
      expect((yield* db.select().from(runs)).length).toBe(2)
    })))

  it("keys documents by kind, frames and saves by name, cache entries by entry name", () =>
    run(Effect.gen(function*() {
      const db = yield* migrated
      yield* seedRun(db, GAME_ID)
      const key = { runsRoot: RUNS_ROOT, gameId: GAME_ID } as const

      const document = { ...key, status: "ok", bytes: nastyBytes, byteSize: 11, sha256: nastyBytes } as const
      yield* db.insert(runDocuments).values({ ...document, kind: "manifest" })
      // A different kind is a different row...
      yield* db.insert(runDocuments).values({ ...document, kind: "report" })
      // ...the same kind is not.
      expect(yield* isFailure(db.insert(runDocuments).values({ ...document, kind: "manifest" }))).toBe(true)

      const frame = { ...key, bytes: nastyBytes, byteSize: 11, sha256: nastyBytes } as const
      yield* db.insert(runFrames).values({ ...frame, name: "000001.png", frameIndex: 1 })
      expect(yield* isFailure(db.insert(runFrames).values({ ...frame, name: "000001.png", frameIndex: 2 }))).toBe(true)

      const save = { ...key, kind: "autosave", byteSize: 11, headBytes: nastyBytes, isWhole: true, sha256: nastyBytes } as const
      yield* db.insert(runSaves).values({ ...save, name: "turn-0000000001-auto.sav.gz", saveTurn: 1 })
      expect(yield* isFailure(db.insert(runSaves).values({ ...save, name: "turn-0000000001-auto.sav.gz", saveTurn: 1 }))).toBe(true)

      // The video and the replay tail are one per run.
      yield* db.insert(runVideos).values({ ...key, bytes: nastyBytes, byteSize: 11, sha256: nastyBytes })
      expect(yield* isFailure(db.insert(runVideos).values({ ...key, bytes: allBytes, byteSize: 256, sha256: nastyBytes }))).toBe(true)

      // The derivation cache is namespaced by the resolved --cache-root, not by
      // runs_root, exactly as the on-disk cache is.
      const entry = { cacheKey: "/tmp/cache", gameId: GAME_ID, bytes: nastyBytes, byteSize: 11 } as const
      yield* db.insert(derivationCache).values({ ...entry, entryName: "events.json" })
      yield* db.insert(derivationCache).values({ ...entry, entryName: "turn-0000000001-abc123def456.json" })
      yield* db.insert(derivationCache).values({ ...entry, cacheKey: "/tmp/other", entryName: "events.json" })
      expect(yield* isFailure(db.insert(derivationCache).values({ ...entry, entryName: "events.json" }))).toBe(true)
      expect((yield* db.select().from(derivationCache)).length).toBe(3)

      yield* db.insert(derivationWorkdirs).values({ ...key, path: "/tmp/cache-saves/g", savesHash: nastyBytes })
      expect(
        yield* isFailure(db.insert(derivationWorkdirs).values({ ...key, path: "/elsewhere", savesHash: allBytes }))
      ).toBe(true)
    })))

  it("refuses a child row with no run, and cascades every child when the run goes", () =>
    run(Effect.gen(function*() {
      const db = yield* migrated

      const orphan = db.insert(runDocuments).values({
        runsRoot: RUNS_ROOT,
        gameId: "game_never_ingested_00",
        kind: "manifest",
        status: "ok",
        bytes: nastyBytes,
        byteSize: 11,
        sha256: nastyBytes
      })
      expect(yield* isFailure(orphan)).toBe(true)

      yield* seedRun(db, GAME_ID)
      const key = { runsRoot: RUNS_ROOT, gameId: GAME_ID } as const
      yield* db.insert(runDocuments).values({ ...key, kind: "manifest", status: "unusable", bytes: nastyBytes, byteSize: 11, sha256: nastyBytes })
      yield* db.insert(runFrames).values({ ...key, name: "000001.png", frameIndex: 1, bytes: nastyBytes, byteSize: 11, sha256: nastyBytes })
      yield* db.insert(runVideos).values({ ...key, bytes: nastyBytes, byteSize: 11, sha256: nastyBytes })
      yield* db.insert(runSaves).values({ ...key, name: "turn-0000000001-auto.sav.gz", kind: "autosave", saveTurn: 1, byteSize: 11, headBytes: nastyBytes, isWhole: true, sha256: nastyBytes })
      yield* db.insert(runReplayTail).values({ ...key, byteSize: 11, tailBytes: nastyBytes, sha256: nastyBytes })

      // Deletion detection is a hard delete — a soft `deleted_at` would add a
      // filter to every read, and a filter that can be forgotten is a byte
      // parity bug waiting to happen.
      yield* db.delete(runs).where(eq(runs.gameId, GAME_ID))

      expect((yield* db.select().from(runDocuments)).length).toBe(0)
      expect((yield* db.select().from(runFrames)).length).toBe(0)
      expect((yield* db.select().from(runVideos)).length).toBe(0)
      expect((yield* db.select().from(runSaves)).length).toBe(0)
      expect((yield* db.select().from(runReplayTail)).length).toBe(0)
    })))

  it("hands back the sweep identity as a number, not a bigint", () =>
    run(Effect.gen(function*() {
      const db = yield* migrated
      const first = yield* seedRun(db, GAME_ID)
      const rows = yield* db.select().from(ingestSweeps)

      expect(typeof first).toBe("number")
      expect(first).toBe(1)
      expect(typeof rows[0]?.sweepId).toBe("number")
      expect(rows[0]?.status).toBe("running")
      expect(rows[0]?.seenCount).toBe(0)
      expect(rows[0]?.finishedAt).toBeNull()
    })))
})

// ------------------------------------------------------------ live client ---

/**
 * The live `PgClient` layer, exercised only where it can be exercised without a
 * server: URL validation, the credential-free description, and the pinned
 * session options. Everything past that needs a live Postgres and belongs to the
 * opt-in differential leg — these assertions are about what may *never* reach a
 * log line, and about the two settings an operator must not be able to change
 * out from under a stored byte.
 */
const URL_WITH_CREDENTIALS = "postgres://arena:hunter2@127.0.0.1:5432/arena_wf_demo"

/** What a URL is rejected for, or `"accepted"`. */
const urlProblem = (raw: string): Client.DatabaseUrlProblem | "accepted" =>
  Either.match(Client.describeTarget(Redacted.make(raw)), {
    onLeft: (error) => error.problem,
    onRight: () => "accepted" as const
  })

describe("the live client's configuration", () => {
  it("describes where it points without carrying a credential", () => {
    const target = Client.describeTarget(Redacted.make(URL_WITH_CREDENTIALS))
    expect(Either.getOrNull(target)).toEqual({
      host: "127.0.0.1",
      port: 5432,
      database: "arena_wf_demo"
    })
    // The whole point: nothing that could be shown to anyone holds the password.
    expect(JSON.stringify(Either.getOrNull(target))).not.toContain("hunter2")
    // And a Redacted refuses to serialise itself even by accident, which is what
    // keeps it out of a structured log line.
    expect(JSON.stringify(Redacted.make(URL_WITH_CREDENTIALS))).toBe(`"<redacted>"`)
  })

  it("defaults the port to 5432 when the URL omits it", () => {
    const target = Client.describeTarget(Redacted.make("postgresql://db.internal/arena"))
    expect(Either.getOrNull(target)).toEqual({
      host: "db.internal",
      port: 5432,
      database: "arena"
    })
  })

  it("names the problem, and only the problem, for a URL it cannot use", () => {
    expect(urlProblem("not a url at all")).toBe("malformed")
    expect(urlProblem("mysql://host/arena")).toBe("scheme")
    // A socket-only URL cannot be reached, and `pg` would silently fall back to
    // a local socket rather than failing, so it is rejected here instead.
    expect(urlProblem("postgres:///arena")).toBe("host")
    expect(urlProblem(URL_WITH_CREDENTIALS)).toBe("accepted")

    const message = Either.match(Client.describeTarget(Redacted.make("mysql://host/arena")), {
      onLeft: Client.describeDatabaseUrlInvalid,
      onRight: () => ""
    })
    expect(message).toBe("database url must use the postgres:// or postgresql:// scheme")
    expect(message).not.toContain("mysql://host/arena")
  })

  it("pins bytea_output and client_encoding into the startup options", () => {
    const pinned = Client.pinSessionOptions(Redacted.make(URL_WITH_CREDENTIALS))
    const href = Either.map(pinned, Redacted.value)
    const options = Either.getOrNull(
      Either.map(href, (raw) => new URL(raw).searchParams.get("options"))
    )

    // A `SET` would only apply to whichever pooled connection ran it; these
    // travel in the startup packet instead, so every connection has them.
    expect(options).toBe("-c bytea_output=hex -c client_encoding=UTF8")
    // The rest of the URL — credentials included — survives untouched.
    expect(Either.getOrNull(href)).toContain("arena:hunter2@127.0.0.1:5432/arena_wf_demo")
  })

  it("keeps a caller's own options, and lets them win", () => {
    const pinned = Client.pinSessionOptions(
      Redacted.make(`${URL_WITH_CREDENTIALS}?options=-c%20statement_timeout%3D5000`)
    )
    const options = Either.getOrNull(
      Either.map(pinned, (url) => new URL(Redacted.value(url)).searchParams.get("options"))
    )

    // The caller's options go last, because Postgres applies `-c` left to right
    // and the last spelling of a setting wins.
    expect(options).toBe(`${Client.PINNED_SESSION_OPTIONS} -c statement_timeout=5000`)
  })

  it("fails with DatabaseUrlInvalid before it opens a socket", () =>
    // `mysql://` is rejected inside the layer's own effect, so nothing is
    // dialled — which is what makes this assertion hermetic. There is no
    // ambient database here at all: the only layer is the config.
    Effect.runPromise(Layer.build(Client.PgClientLive).pipe(
      // oxlint-disable-next-line effecttsgo/strict-effect-provide
      Effect.provide(Client.databaseConfigLayer(Client.databaseConfig(
        Redacted.make("mysql://127.0.0.1:5432/arena")
      ))),
      Effect.scoped,
      Effect.either,
      Effect.map((outcome) => {
        expect(Either.isLeft(outcome)).toBe(true)
        expect(Either.match(outcome, {
          // The error channel is `SqlError | DatabaseUrlInvalid`; a `SqlError`
          // here would mean the layer had gone as far as dialling, which is the
          // failure this test exists to rule out.
          onLeft: (error) =>
            error._tag === "DatabaseUrlInvalid" ? `${error._tag}:${error.problem}` : error._tag,
          onRight: () => "connected"
        })).toBe("DatabaseUrlInvalid:scheme")
      })
    )))
})
