/**
 * The `pg-divergence` hunt, hermetic.
 *
 * `runs-repository-pg.test.ts` is the differential over the eight parity
 * fixtures, and it is the right oracle for the repository. It cannot see two
 * things, and this file is those two things:
 *
 * 1. **The PPM head is not read through the repository at all.**
 *    `http/routes/archive.ts#frameSources` still lists `runRoot/saves` itself —
 *    one of the three places that route reaches around the repository and
 *    touches the run directory by path — and reads each `*.map.ppm`
 *    with its own `PPM_PREFIX_BYTES = 512 KiB` prefix. On the pg backend that
 *    `runRoot` is the *materialized* directory, whose PPMs are the ingester's
 *    `ppmHeadBytes` head — **131072** by default. Two constants, four times
 *    apart, on the same read: a header whose player comments end between them
 *    is visible to the fs gateway and invisible to the pg one. The parity
 *    corpus's PPMs are 207 bytes, so no fixture can reach the gap.
 *
 * 2. **The corpus has nothing on the size gates.** Its documents are kilobytes,
 *    so `byte_size` (the fstat gate) and `octet_length(bytes)` are never forced
 *    apart, and no fixture carries a NUL inside a JSON string or invalid UTF-8
 *    — the two things `text`/`jsonb` could not have stored.
 *
 * Everything here runs in PGlite, in process, over the committed migrations.
 * Nothing connects to a server and nothing is written outside `mktemp -d`.
 *
 * @module
 */

import { BunContext } from "@effect/platform-bun"
import { FileSystem } from "@effect/platform/FileSystem"
import { Path } from "@effect/platform/Path"
import * as PgDrizzle from "@effect/sql-drizzle/Pg"
import { and, eq } from "drizzle-orm"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as ManagedRuntime from "effect/ManagedRuntime"
import * as Option from "effect/Option"
import type { SqlError } from "@effect/sql/SqlError"
import { afterAll, describe, expect, it } from "bun:test"
import { readFileSync, rmSync, statSync, writeFileSync } from "node:fs"
import { join } from "node:path"
import { fileURLToPath } from "node:url"

import { archivePpmPlayers } from "../../harness/src/gateway/archive.ts"
import { PPM_PREFIX_BYTES } from "../../harness/src/gateway/http/routes/archive.ts"
import { makeMaterializer } from "../src/materialize.ts"
import * as Ingest from "../src/ingest.ts"
import * as Migrate from "../src/migrate.ts"
import * as Pglite from "../src/pglite.ts"
import { runDocuments, runSaves } from "../src/schema.ts"

const fixturesRoot = fileURLToPath(
  new URL("../../harness/test/parity/fixtures/runs", import.meta.url)
)

const TERMINAL_ID = "game_parity_terminal_valid_01"
const PPM_NAME = "turn-0001-M-bc--tuZ1Pall.map.ppm"

/** The second player's name, placed past the stored head by {@link longPpm}. */
const LATE_PLAYER = "PlayerPastTheHead"

// ---------------------------------------------------------------------------
// The two artifacts the parity corpus cannot express
// ---------------------------------------------------------------------------

/**
 * A PPM whose second `# playerno:` comment sits at byte `markerAtByte`.
 *
 * Both readers stop at 513 *lines* as well as at a byte cap, so the filler
 * lines are wide rather than numerous: a header pushed past 128 KiB with short
 * lines would hit the line cap first and prove nothing about the byte one.
 */
const longPpm = (markerAtByte: number): Buffer => {
  const head = ["P3", "# version:2", `# playerno:0:color:(  0, 103, 165):name:"AgentPlace1"`]
  const marker = `# playerno:1:color:(243, 132,   0):name:"${LATE_PLAYER}"`
  const filler = (index: number): string => `# pad ${String(index).padStart(6, "0")} ${"x".repeat(500)}`
  const grown = Array.from({ length: 400 }, (_, index) => index).reduce<{
    readonly lines: ReadonlyArray<string>
    readonly size: number
  }>(
    (state, index) => {
      const next = filler(index)
      return state.size + next.length + 1 > markerAtByte
        ? state
        : { lines: [...state.lines, next], size: state.size + next.length + 1 }
    },
    { lines: head, size: head.join("\n").length + 1 }
  )
  // No guard against the 513-line scan cap, because the construction cannot
  // reach it: 400 filler candidates plus three head lines is 403 at most, and
  // every filler is 500 characters wide precisely so the *byte* cap is what the
  // header crosses.
  return Buffer.from([...grown.lines, marker, "543 597", "255", "0 0 0", ""].join("\n"), "utf8")
}

/** A manifest holding a NUL inside a string and an invalid UTF-8 byte. */
const hostileManifest = (gameId: string): Buffer =>
  Buffer.concat([
    Buffer.from(
      `{"game_id":"${gameId}","state":"completed","created_at":1.7e9,` +
        `"finished_at":1.70000000e9,"benchmark_valid":true,"nul":"a\\u0000b","raw":"`,
      "utf8"
    ),
    Buffer.from([0xff, 0xfe, 0x80]),
    Buffer.from(`"}`, "utf8")
  ])

interface Scratch {
  readonly base: string
  readonly runsRoot: string
  readonly materializeRoot: string
}

const makeScratch = (markerAtByte: number): Scratch => {
  const base = Bun.spawnSync(["mktemp", "-d", "/tmp/arena-pg-hunt-XXXXXX"]).stdout.toString().trim()
  const runsRoot = join(base, "runs")
  const copy = Bun.spawnSync(["cp", "-R", fixturesRoot, runsRoot])
  // The one throw in this file, and deliberate: a corpus that did not copy makes
  // every assertion below meaningless, and there is no *value* a fixture builder
  // can return that a later expectation would read as "the environment broke".
  // This is the house rule's carve-out for a programmer/environment error, not a
  // failure mode of the code under test.
  if (copy.exitCode !== 0) throw new Error(`cp -R failed: ${copy.stderr.toString()}`)
  writeFileSync(join(runsRoot, TERMINAL_ID, "saves", PPM_NAME), longPpm(markerAtByte))
  writeFileSync(join(runsRoot, TERMINAL_ID, "manifest.json"), hostileManifest(TERMINAL_ID))
  return { base, runsRoot, materializeRoot: join(base, "cache-saves") }
}

// ---------------------------------------------------------------------------
// The read the *route* performs, on either backend
// ---------------------------------------------------------------------------

/**
 * `http/routes/archive.ts#readPpmPrefix`, which is module-private there.
 *
 * Transcribed rather than imported, and the transcription is the subject: at
 * most {@link PPM_PREFIX_BYTES}, decoded with replacement, cut back to the last
 * complete line **only when the file was longer than the prefix**. A
 * materialized PPM is shorter than the prefix, so this never trims it — which
 * is exactly how a head that ends mid-line reaches `archivePpmPlayers`.
 */
const ppmPrefixText = (path: string): string => {
  const size = statSync(path).size
  const length = Math.min(size, PPM_PREFIX_BYTES)
  const bytes = readFileSync(path).subarray(0, length)
  const text = new TextDecoder("utf-8").decode(bytes)
  const lastBreak = text.lastIndexOf("\n")
  return length >= size || lastBreak < 0 ? text : text.slice(0, lastBreak + 1)
}

const playerNames = (text: string): ReadonlyArray<string> =>
  archivePpmPlayers(text, []).map((player) => player.player_name)

// ---------------------------------------------------------------------------
// One PGlite per head size
// ---------------------------------------------------------------------------

const testLayer = Layer.mergeAll(
  Layer.provideMerge(PgDrizzle.layer, Pglite.layer(Pglite.memory)),
  BunContext.layer
)

type TestContext = Layer.Layer.Success<typeof testLayer>

interface Case {
  readonly scratch: Scratch
  readonly runtime: ManagedRuntime.ManagedRuntime<TestContext, SqlError>
  /** The materialized PPM the pg-backed route would read. */
  readonly materializedPpm: string
  readonly storedHead: { readonly byteSize: number; readonly headLength: number; readonly isWhole: boolean }
}

/** Ingest the scratch corpus at one `ppmHeadBytes`, then materialize the run. */
const buildCase = async (markerAtByte: number, ppmHeadBytes: number): Promise<Case> => {
  const scratch = makeScratch(markerAtByte)
  const runtime = ManagedRuntime.make(testLayer)
  const materializedPpm = await runtime.runPromise(
    Effect.orDie(
      Effect.gen(function*() {
        yield* Migrate.run
        yield* Ingest.ingest({ ...Ingest.ingestOptions(scratch.runsRoot), ppmHeadBytes })
        const db = yield* PgDrizzle.PgDrizzle
        const fileSystem = yield* FileSystem
        const path = yield* Path
        const materializer = yield* makeMaterializer(db, fileSystem, path, {
          runsRoot: Ingest.resolveRunsRoot(scratch.runsRoot),
          materializeRoot: scratch.materializeRoot
        })
        const runRoot = yield* materializer.ensureRunArchive(TERMINAL_ID)
        return path.join(runRoot, "saves", PPM_NAME)
      })
    )
  )
  const storedHead = await runtime.runPromise(
    Effect.orDie(
      Effect.gen(function*() {
        const db = yield* PgDrizzle.PgDrizzle
        const rows = yield* db
          .select()
          .from(runSaves)
          .where(and(eq(runSaves.gameId, TERMINAL_ID), eq(runSaves.name, PPM_NAME)))
        // `Effect.fromNullable`, not a `throw`: this runs *inside* `Effect.gen`,
        // where a throw is a defect the surrounding `Effect.orDie` could not
        // have converted and no error channel can carry.
        const row = yield* Effect.fromNullable(rows[0])
        return { byteSize: row.byteSize, headLength: row.headBytes.length, isWhole: row.isWhole }
      })
    )
  )
  return { scratch, runtime, materializedPpm, storedHead }
}

/** The marker sits at ~150 KiB: past the 128 KiB default, inside the 512 KiB prefix. */
const MARKER_AT = 150 * 1024

const cases: Promise<{
  readonly atDefault: Case
  readonly atPrefix: Case
}> = Promise.all([
  buildCase(MARKER_AT, Ingest.DEFAULT_PPM_HEAD_BYTES),
  buildCase(MARKER_AT, PPM_PREFIX_BYTES)
]).then(([atDefault, atPrefix]) => ({ atDefault, atPrefix }))

afterAll(async () => {
  // A fixture that never built has nothing to tear down, and teardown is not
  // where that failure gets reported — the tests already failed. `Option`
  // rather than a swallowed `catch`, so "there is nothing here" is a value.
  const built = await cases.then(Option.some, () => Option.none<Awaited<typeof cases>>())
  if (Option.isNone(built)) return
  const cleaned = [built.value.atDefault, built.value.atPrefix]
  await Promise.all(cleaned.map((one) => one.runtime.dispose()))
  cleaned.forEach((one) => {
    if (one.scratch.base.startsWith("/tmp/arena-pg-hunt-")) {
      rmSync(one.scratch.base, { recursive: true, force: true })
    }
  })
})

// ---------------------------------------------------------------------------

describe("the PPM head against the route's own prefix", () => {
  it("stores a head, not the file, and records the true size beside it", async () => {
    const { atDefault } = await cases
    expect(atDefault.storedHead.isWhole).toBe(false)
    expect(atDefault.storedHead.headLength).toBe(Ingest.DEFAULT_PPM_HEAD_BYTES)
    // The gate is the fstat size, never `octet_length(head_bytes)`.
    expect(atDefault.storedHead.byteSize).toBeGreaterThan(atDefault.storedHead.headLength)
  })

  it("agrees with the filesystem for every player inside the stored head", async () => {
    const { atDefault } = await cases
    const source = join(atDefault.scratch.runsRoot, TERMINAL_ID, "saves", PPM_NAME)
    expect(playerNames(ppmPrefixText(source))[0]).toBe("AgentPlace1")
    expect(playerNames(ppmPrefixText(atDefault.materializedPpm))[0]).toBe("AgentPlace1")
  })

  it(
    "loses a player the filesystem publishes when the header outruns the head " +
      "(DEFAULT_PPM_HEAD_BYTES < PPM_PREFIX_BYTES — raise the ingest default to close it)",
    async () => {
      const { atDefault } = await cases
      const source = join(atDefault.scratch.runsRoot, TERMINAL_ID, "saves", PPM_NAME)
      const fromDisk = playerNames(ppmPrefixText(source))
      const fromDatabase = playerNames(ppmPrefixText(atDefault.materializedPpm))
      expect(fromDisk).toContain(LATE_PLAYER)
      // The characterization, not the contract: this is the open divergence.
      expect(fromDatabase).not.toContain(LATE_PLAYER)
      expect(Ingest.DEFAULT_PPM_HEAD_BYTES).toBeLessThan(PPM_PREFIX_BYTES)
    }
  )

  it("agrees exactly when the head is ingested at the route's own prefix", async () => {
    const { atPrefix } = await cases
    const source = join(atPrefix.scratch.runsRoot, TERMINAL_ID, "saves", PPM_NAME)
    expect(playerNames(ppmPrefixText(atPrefix.materializedPpm))).toEqual(
      playerNames(ppmPrefixText(source))
    )
  })
})

describe("bytea holds what text and jsonb could not", () => {
  it("round-trips a NUL inside a JSON string and an invalid UTF-8 byte", async () => {
    const { atDefault } = await cases
    const onDisk = readFileSync(join(atDefault.scratch.runsRoot, TERMINAL_ID, "manifest.json"))
    const stored = await atDefault.runtime.runPromise(
      Effect.orDie(
        Effect.gen(function*() {
          const db = yield* PgDrizzle.PgDrizzle
          const rows = yield* db
            .select()
            .from(runDocuments)
            .where(and(eq(runDocuments.gameId, TERMINAL_ID), eq(runDocuments.kind, "manifest")))
          // As above: inside `Effect.gen`, absence is a failure value.
          return yield* Effect.fromNullable(rows[0])
        })
      )
    )
    expect(Buffer.from(stored.bytes).equals(onDisk)).toBe(true)
    expect(stored.byteSize).toBe(onDisk.length)
    // `unusable` because `decodeUtf8` is fatal and this document is not valid
    // UTF-8 — the fs backend's 503.  The bytes are stored **anyway**, which is
    // the whole reason the column is `bytea`: a `text` column could not have
    // held them, and the 503 would have become unreproducible.
    expect(stored.status).toBe("unusable")
  })
})
