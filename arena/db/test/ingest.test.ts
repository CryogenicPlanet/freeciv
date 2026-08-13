/**
 * The ingest pipeline, against the parity fixture corpus, on PGlite.
 *
 * The corpus is `arena/harness/test/parity/fixtures/runs` — the same eight
 * scenario classes the CPython↔TypeScript differential runs on, which is what
 * makes these assertions statements about parity rather than about a database:
 *
 * | fixture | what it pins here |
 * |---|---|
 * | `terminal_valid_01` | the full archive: three documents, frames, saves, a tail |
 * | `terminal_nowin_02` | a terminal run with no `victory.json` |
 * | `interrupted_03` | a manifest and a tail, nothing else |
 * | `lobby_husk_04` | a zero-byte `replay.jsonl` — the row exists and says `0` |
 * | `malformed_05` | a truncated manifest: stored verbatim, `status = 'unusable'` |
 * | `wrong_id_06` | a manifest whose `game_id` lies — ingested anyway, gated on read |
 * | `symlink_07` | a symlinked run directory — skipped, with a reason |
 * | `torn_tail_08` | a half-written final line, kept byte for byte |
 *
 * Four properties get their own describe block because each of them is a
 * contract rather than a behaviour: bytes survive the round trip untouched,
 * `byte_size` is an independent stored gate, a re-ingest of an unchanged root
 * writes *nothing*, and a changed file moves exactly its own rows.
 */

import { BunContext } from "@effect/platform-bun"
import * as PgDrizzle from "@effect/sql-drizzle/Pg"
import * as SqlClient from "@effect/sql/SqlClient"
import { and, eq, notInArray } from "drizzle-orm"
import * as Effect from "effect/Effect"
import * as Either from "effect/Either"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Redacted from "effect/Redacted"
import { beforeAll, describe, expect, it } from "bun:test"
// oxlint-disable-next-line effecttsgo/node-builtin-import
import { cpSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs"
// oxlint-disable-next-line effecttsgo/node-builtin-import
import { tmpdir } from "node:os"
// oxlint-disable-next-line effecttsgo/node-builtin-import
import { join } from "node:path"
import { fileURLToPath } from "node:url"

import * as Client from "../src/client.ts"
import * as Cli from "../src/ingest-cli.ts"
import * as Ingest from "../src/ingest.ts"
import * as Migrate from "../src/migrate.ts"
import * as Pglite from "../src/pglite.ts"
import {
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

/** One fresh, migrated, in-process database per test. */
const run = <A, E>(effect: Effect.Effect<A, E, TestContext>): Promise<A> =>
  // oxlint-disable-next-line effecttsgo/strict-effect-provide
  Effect.runPromise(Effect.scoped(Effect.provide(Effect.flatMap(Migrate.run, () => effect), testLayer)))

/** The parity corpus, read-only. Every mutation test works on a copy. */
const FIXTURES = fileURLToPath(
  new URL("../../harness/test/parity/fixtures/runs", import.meta.url)
)

const TERMINAL = "game_parity_terminal_valid_01"
const NOWIN = "game_parity_terminal_nowin_02"
const INTERRUPTED = "game_parity_interrupted_03"
const LOBBY = "game_parity_lobby_husk_04"
const MALFORMED = "game_parity_malformed_05"
const WRONG_ID = "game_parity_wrong_id_06"
const SYMLINK = "game_parity_symlink_07"
const TORN_TAIL = "game_parity_torn_tail_08"

/** The seven directories that are runs; the symlink is not one of them. */
const INGESTED_IDS: ReadonlyArray<string> = [
  TERMINAL,
  NOWIN,
  INTERRUPTED,
  LOBBY,
  MALFORMED,
  WRONG_ID,
  TORN_TAIL
]

/**
 * A writable copy of the corpus, for the tests that mutate it.
 *
 * `verbatimSymlinks` matters: `game_parity_symlink_07` is a *relative* symlink,
 * and dereferencing it while copying would turn the symlink scenario into an
 * ordinary run and quietly delete the test.
 */
const copyFixtures = (): string => {
  const root = join(mkdtempSync(join(tmpdir(), "arena-ingest-")), "runs")
  cpSync(FIXTURES, root, { recursive: true, verbatimSymlinks: true })
  return root
}

const discard = (root: string): void => rmSync(join(root, ".."), { recursive: true, force: true })

/** Ingest a root with the default options, plus any overrides. */
const sweep = (root: string, overrides: Partial<Ingest.IngestOptions> = {}) =>
  Ingest.ingest({ ...Ingest.ingestOptions(root), ...overrides })

/**
 * One field of one run's result, as a value — a missing run included.
 *
 * The absence is not thrown: a `throw` in a test body is reported as a crash
 * with a stack, where an expectation that reads `"no result for game_…"` next to
 * the value it was supposed to be says both which run went missing and which
 * assertion noticed.
 */
const fieldOf = <K extends keyof Ingest.RunResult>(
  report: Ingest.IngestReport,
  gameId: string,
  key: K
): Ingest.RunResult[K] | string =>
  Option.match(Option.fromNullable(report.runs.find((entry) => entry.gameId === gameId)), {
    onNone: () => `no result for ${gameId}`,
    onSome: (result) => result[key]
  })

const bytesOf = (path: string): Uint8Array => Uint8Array.from(readFileSync(path))

const sameBytes = (actual: Uint8Array | undefined, expected: Uint8Array): void => {
  expect(actual?.length).toBe(expected.length)
  expect(Array.from(actual ?? new Uint8Array())).toEqual(Array.from(expected))
}

/** `game_id:ingested_at` for every run row, so a rewrite is visible. */
const stamps = (
  rows: ReadonlyArray<{ readonly gameId: string; readonly ingestedAt: Date }>
): ReadonlyArray<string> =>
  rows.map((row) => `${row.gameId}:${row.ingestedAt.toISOString()}`).toSorted()

const documentsOf = (db: PgDrizzle.PgDrizzle["Type"], root: string, gameId: string) =>
  db
    .select()
    .from(runDocuments)
    .where(and(eq(runDocuments.runsRoot, root), eq(runDocuments.gameId, gameId)))

// --------------------------------------------------------- the eight cases ---

describe("the walk", () => {
  it("ingests seven runs and skips the symlinked directory with a reason", () =>
    run(Effect.gen(function*() {
      const report = yield* sweep(FIXTURES)

      expect(report.runs.map((entry) => entry.gameId).toSorted()).toEqual([...INGESTED_IDS].toSorted())
      expect(report.runs.every((entry) => entry.outcome === "inserted")).toBe(true)
      expect(report.seen).toBe(8)
      expect(report.deleted).toEqual([])

      // The symlink is the whole point of `game_parity_symlink_07`: the fs
      // backend 404s it at read time, and a row would have made it answerable.
      expect(report.skipped.map((skip) => ({ entry: skip.entry, reason: skip.reason }))).toEqual([
        { entry: SYMLINK, reason: "symlink" }
      ])
      expect(report.runsRoot).toBe(Ingest.resolveRunsRoot(FIXTURES))
    })))

  it("stores each fixture's documents with the status the fs backend would answer with", () =>
    run(Effect.gen(function*() {
      const db = yield* PgDrizzle.PgDrizzle
      yield* sweep(FIXTURES)
      const root = Ingest.resolveRunsRoot(FIXTURES)

      const kinds = (gameId: string) =>
        Effect.map(
          documentsOf(db, root, gameId),
          (rows) => rows.map((row) => `${row.kind}:${row.status}`).toSorted()
        )

      // The full archive: all three documents, all parseable.
      expect(yield* kinds(TERMINAL)).toEqual(["manifest:ok", "report:ok", "victory:ok"])
      // Terminal, but nobody won: no `victory.json` on disk, so no row — which
      // is exactly the absence `archiveVictory` reads as `Option.none`.
      expect(yield* kinds(NOWIN)).toEqual(["manifest:ok", "report:ok"])
      // Live runs never wrote a report.
      expect(yield* kinds(INTERRUPTED)).toEqual(["manifest:ok"])
      expect(yield* kinds(LOBBY)).toEqual(["manifest:ok"])
      expect(yield* kinds(TORN_TAIL)).toEqual(["manifest:ok"])
      // A truncated manifest: the bytes are stored and the row says `unusable`,
      // which is what keeps the 503 reproducible instead of merely asserted.
      expect(yield* kinds(MALFORMED)).toEqual(["manifest:unusable"])
      // The id mismatch is a *read-side* gate — `readManifest` re-runs it over
      // these bytes — so ingest stores the document like any other.
      expect(yield* kinds(WRONG_ID)).toEqual(["manifest:ok", "report:ok"])

      expect(fieldOf(yield* sweep(FIXTURES), MALFORMED, "unusableDocuments")).toBe(1)
    })))

  it("records the frames, saves, video and directory booleans per fixture", () =>
    run(Effect.gen(function*() {
      const db = yield* PgDrizzle.PgDrizzle
      yield* sweep(FIXTURES)
      const root = Ingest.resolveRunsRoot(FIXTURES)

      const frames = yield* db.select().from(runFrames).where(eq(runFrames.runsRoot, root))
      const saves = yield* db.select().from(runSaves).where(eq(runSaves.runsRoot, root))
      const videos = yield* db.select().from(runVideos).where(eq(runVideos.runsRoot, root))
      const stored = yield* db.select().from(runs).where(eq(runs.runsRoot, root))

      const named = (gameId: string) => frames.filter((row) => row.gameId === gameId)
      expect(named(TERMINAL).map((row) => row.name).toSorted()).toEqual([
        "000000.png",
        "000752.png"
      ])
      // `decodeArchivePngName` runs at ingest so the repository never parses a
      // file name on the read path.
      expect(named(TERMINAL).map((row) => row.frameIndex ?? -1).toSorted((a, b) => a - b))
        .toEqual([0, 752])
      expect(named(NOWIN).length).toBe(3)
      expect(named(INTERRUPTED)).toEqual([])

      const savesOf = (gameId: string) => saves.filter((row) => row.gameId === gameId)
      expect(savesOf(TERMINAL).map((row) => `${row.kind}:${String(row.saveTurn)}`).toSorted())
        .toEqual(["autosave:1", "autosave:2", "ppm:1", "ppm:2"])
      expect(savesOf(NOWIN).map((row) => row.kind)).toEqual(["ppm"])

      // No fixture ships a `game.mp4`; the absence has to be an absent row.
      expect(videos).toEqual([])

      const flags = (gameId: string) => {
        const row = stored.find((entry) => entry.gameId === gameId)
        return `${String(row?.framesDirOk)}/${String(row?.savesDirOk)}`
      }
      expect(flags(TERMINAL)).toBe("true/true")
      // `safeArchiveDirectory` failing is `archiveDataNotFound` on the read
      // path, and these two booleans are the only record of it.
      expect(flags(INTERRUPTED)).toBe("false/false")
    })))

  it("stores the replay tail as a byte window, including a zero-length file", () =>
    run(Effect.gen(function*() {
      const db = yield* PgDrizzle.PgDrizzle
      yield* sweep(FIXTURES)
      const root = Ingest.resolveRunsRoot(FIXTURES)
      const tails = yield* db.select().from(runReplayTail).where(eq(runReplayTail.runsRoot, root))
      const tailOf = (gameId: string) => tails.find((row) => row.gameId === gameId)

      // Every fixture but `wrong_id_06` ships a `replay.jsonl`.
      expect(tails.map((row) => row.gameId).toSorted()).toEqual(
        INGESTED_IDS.filter((id) => id !== WRONG_ID).toSorted()
      )

      const whole = bytesOf(join(FIXTURES, TORN_TAIL, "replay.jsonl"))
      sameBytes(tailOf(TORN_TAIL)?.tailBytes, whole)
      expect(tailOf(TORN_TAIL)?.byteSize).toBe(whole.length)
      // The fixture's last line is half-written; the window keeps it that way.
      expect(new TextDecoder().decode(whole).endsWith("\n")).toBe(false)

      // A lobby husk's `replay.jsonl` is empty: the row exists and says `0`,
      // which is the value `lastReplayTurn` reads as "no turn".
      expect(tailOf(LOBBY)?.byteSize).toBe(0)
      expect(tailOf(LOBBY)?.tailBytes.length).toBe(0)
      expect(tailOf(MALFORMED)?.byteSize).toBe(0)
    })))

  it("fills the advisory columns from @arena/wire's manifest schema, and never fails on a bad one", () =>
    run(Effect.gen(function*() {
      const db = yield* PgDrizzle.PgDrizzle
      const report = yield* sweep(FIXTURES)
      const root = Ingest.resolveRunsRoot(FIXTURES)
      const stored = yield* db.select().from(runs).where(eq(runs.runsRoot, root))
      const row = (gameId: string) => stored.find((entry) => entry.gameId === gameId)

      expect(row(TERMINAL)?.state).toBe("completed")
      // `created_at` is a float epoch second, deliberately not a timestamptz.
      expect(typeof row(TERMINAL)?.createdAt).toBe("number")
      // The column is the *manifest field*, copied verbatim — deliberately not
      // `archive.ts`'s `benchmarkValid`, which is
      // `state === 'completed' && manifest.benchmark_valid === true`. Anything
      // that filtered on this column would publish the wrong answer, which is
      // why it is advisory and why the read path never reads it.
      expect(row(TERMINAL)?.benchmarkValid).toBe(true)
      expect(row(MALFORMED)?.benchmarkValid).toBeNull()
      expect(fieldOf(report, TERMINAL, "manifestUndecodable")).toBe(false)

      // A manifest that is not even a JSON object leaves every advisory column
      // null and is reported — and the run is still ingested.
      expect(row(MALFORMED)?.state).toBeNull()
      expect(row(MALFORMED)?.createdAt).toBeNull()
      expect(row(MALFORMED)?.benchmarkValid).toBeNull()
      expect(fieldOf(report, MALFORMED, "outcome")).toBe("inserted")
    })))

  it("fails loudly on a runs_root it cannot list", () =>
    run(Effect.gen(function*() {
      const missing = join(tmpdir(), "arena-ingest-does-not-exist-3f9a")
      const failure = yield* Effect.either(sweep(missing))
      expect(Either.isLeft(failure)).toBe(true)
      expect(Either.getLeft(failure).pipe(Option.map((error) => error._tag))).toEqual(
        Option.some("RunsRootUnreadable")
      )
    })))
})

// ------------------------------------------------------------ byte fidelity ---

describe("byte fidelity", () => {
  it("round-trips every stored payload exactly", () =>
    run(Effect.gen(function*() {
      const db = yield* PgDrizzle.PgDrizzle
      yield* sweep(FIXTURES)
      const root = Ingest.resolveRunsRoot(FIXTURES)

      const documents = yield* documentsOf(db, root, TERMINAL)
      const document = (kind: string) => documents.find((row) => row.kind === kind)

      // `victory.json` is the one document nobody may pre-decode: `relayedJson`
      // guesses int-vs-float from its `turn`/`year` spelling, so the bytes have
      // to reach the read path unchanged.
      sameBytes(document("victory")?.bytes, bytesOf(join(FIXTURES, TERMINAL, "victory.json")))
      sameBytes(document("manifest")?.bytes, bytesOf(join(FIXTURES, TERMINAL, "manifest.json")))
      sameBytes(document("report")?.bytes, bytesOf(join(FIXTURES, TERMINAL, "report.json")))

      // A truncated, unparseable manifest is stored just as verbatim.
      const malformed = yield* documentsOf(db, root, MALFORMED)
      sameBytes(malformed[0]?.bytes, bytesOf(join(FIXTURES, MALFORMED, "manifest.json")))

      const frames = yield* db
        .select()
        .from(runFrames)
        .where(and(eq(runFrames.runsRoot, root), eq(runFrames.gameId, TERMINAL)))
      const png = frames.find((row) => row.name === "000752.png")
      sameBytes(png?.bytes, bytesOf(join(FIXTURES, TERMINAL, "watch_frames", "000752.png")))

      const saves = yield* db
        .select()
        .from(runSaves)
        .where(and(eq(runSaves.runsRoot, root), eq(runSaves.gameId, TERMINAL)))
      const autosave = saves.find((row) => row.name === "turn-0001-auto.sav.gz")
      sameBytes(
        autosave?.headBytes,
        bytesOf(join(FIXTURES, TERMINAL, "saves", "turn-0001-auto.sav.gz"))
      )
      // An autosave is stored whole, because `save_replay` decompresses it.
      expect(autosave?.isWhole).toBe(true)
    })))

  it("keeps byte_size as the fstat size when the stored bytes are only a head", () =>
    run(Effect.gen(function*() {
      const db = yield* PgDrizzle.PgDrizzle
      // 64 bytes is far below any real PPM header; the point is that the true
      // size still has to be recorded beside the truncated payload.
      yield* sweep(FIXTURES, { ppmHeadBytes: 64 })
      const root = Ingest.resolveRunsRoot(FIXTURES)
      const saves = yield* db
        .select()
        .from(runSaves)
        .where(and(eq(runSaves.runsRoot, root), eq(runSaves.gameId, TERMINAL)))

      const ppm = saves.find((row) => row.kind === "ppm")
      const source = bytesOf(join(FIXTURES, TERMINAL, "saves", "turn-0001-M-bc--tuZ1Pall.map.ppm"))
      expect(ppm?.byteSize).toBe(source.length)
      expect(ppm?.headBytes.length).toBe(64)
      expect(ppm?.isWhole).toBe(false)
      sameBytes(ppm?.headBytes, source.subarray(0, 64))

      // At the default the same file fits whole, and `is_whole` says so.
      const autosave = saves.find((row) => row.kind === "autosave")
      expect(autosave?.isWhole).toBe(true)
    })))

  it("stores the whole PPM when it fits under the head limit", () =>
    run(Effect.gen(function*() {
      const db = yield* PgDrizzle.PgDrizzle
      yield* sweep(FIXTURES)
      const root = Ingest.resolveRunsRoot(FIXTURES)
      const saves = yield* db
        .select()
        .from(runSaves)
        .where(and(eq(runSaves.runsRoot, root), eq(runSaves.gameId, NOWIN)))
      const ppm = saves[0]
      const source = bytesOf(join(FIXTURES, NOWIN, "saves", "turn-0001-M-bc--tuZ1Pall.map.ppm"))
      sameBytes(ppm?.headBytes, source)
      expect(ppm?.isWhole).toBe(true)
      expect(ppm?.byteSize).toBe(source.length)
    })))
})

// -------------------------------------------------------------- idempotence ---

describe("idempotence", () => {
  it("writes nothing at all on a second sweep of an unchanged root", () =>
    run(Effect.gen(function*() {
      const first = yield* sweep(FIXTURES)
      expect(Ingest.dataWrites(first.writes)).toBeGreaterThan(0)

      const second = yield* sweep(FIXTURES)

      expect(second.runs.every((entry) => entry.outcome === "unchanged")).toBe(true)
      expect(Ingest.dataWrites(second.writes)).toBe(0)
      // The exact proof: `pg_current_xact_id_if_assigned()` is NULL in every
      // per-run transaction, so not one of them wrote a tuple — a row count
      // cannot distinguish that from a rewrite of identical bytes.
      expect(second.transactionsWithWrites).toBe(0)
      // The first sweep did write, and its transactions say so.
      expect(first.transactionsWithWrites).toBe(INGESTED_IDS.length)
      // Sweep bookkeeping is not a data write, and always happens.
      expect(second.writes.sweeps).toBe(2)
    })))

  it("leaves ingested_at alone for a run whose bytes did not change", () =>
    run(Effect.gen(function*() {
      const db = yield* PgDrizzle.PgDrizzle
      const root = Ingest.resolveRunsRoot(FIXTURES)
      yield* sweep(FIXTURES)
      const before = yield* db.select().from(runs).where(eq(runs.runsRoot, root))
      yield* sweep(FIXTURES)
      const after = yield* db.select().from(runs).where(eq(runs.runsRoot, root))

      expect(stamps(after)).toEqual(stamps(before))
    })))

  it("reports what a dry run would do without opening a sweep or writing a row", () =>
    run(Effect.gen(function*() {
      const db = yield* PgDrizzle.PgDrizzle
      const report = yield* sweep(FIXTURES, { dryRun: true })

      expect(report.dryRun).toBe(true)
      expect(Option.isNone(report.sweepId)).toBe(true)
      expect(report.runs.every((entry) => entry.outcome === "inserted")).toBe(true)
      expect(Ingest.dataWrites(report.writes)).toBe(0)
      expect(report.writes.sweeps).toBe(0)

      const stored = yield* db.select().from(runs)
      expect(stored).toEqual([])

      // And the summary it prints names no connection string.
      const summary = Ingest.describeReport(report)
      expect(summary).toContain("(dry run)")
      expect(summary).toContain("7 inserted")
    })))
})

// ------------------------------------------------------------ change tracking ---

describe("a changed run", () => {
  const mutated = (change: (directory: string) => void) =>
    Effect.gen(function*() {
      const directory = copyFixtures()
      const first = yield* sweep(directory)
      expect(first.runs.length).toBe(INGESTED_IDS.length)
      yield* Effect.sync(() => change(directory))
      const second = yield* sweep(directory)
      return { root: directory, second } as const
    })

  it("rewrites exactly the manifest row when the manifest changes", () =>
    run(Effect.gen(function*() {
      const db = yield* PgDrizzle.PgDrizzle
      const manifestPath = (directory: string) => join(directory, TERMINAL, "manifest.json")
      const { root, second } = yield* mutated((directory) => {
        const text = readFileSync(manifestPath(directory), "utf8")
        // A real edit, and one the schema still accepts: `state` stays legal,
        // so the advisory columns move too.
        writeFileSync(manifestPath(directory), text.replace('"state": "completed"', '"state": "failed"'))
      })

      expect(fieldOf(second, TERMINAL, "outcome")).toBe("updated")
      expect(second.runs.filter((entry) => entry.outcome === "unchanged").length).toBe(
        INGESTED_IDS.length - 1
      )
      expect(second.transactionsWithWrites).toBe(1)
      expect(second.writes.runs).toBe(1)
      expect(second.writes.documents).toBe(1)
      // The rest of the run is byte-identical, so not one of its rows is touched
      // — which is what keeps a 838-frame run's TOAST chains still.
      expect(second.writes.frames).toBe(0)
      expect(second.writes.saves).toBe(0)
      expect(second.writes.replayTails).toBe(0)
      expect(second.writes.deletes).toBe(0)

      const resolved = Ingest.resolveRunsRoot(root)
      const documents = yield* documentsOf(db, resolved, TERMINAL)
      sameBytes(
        documents.find((row) => row.kind === "manifest")?.bytes,
        bytesOf(manifestPath(root))
      )
      const stored = yield* db
        .select()
        .from(runs)
        .where(and(eq(runs.runsRoot, resolved), eq(runs.gameId, TERMINAL)))
      expect(stored[0]?.state).toBe("failed")

      discard(root)
    })))

  it("rewrites exactly one frame row when one PNG changes", () =>
    run(Effect.gen(function*() {
      const db = yield* PgDrizzle.PgDrizzle
      const framePath = (directory: string) =>
        join(directory, TERMINAL, "watch_frames", "000000.png")
      const { root, second } = yield* mutated((directory) => {
        const original = readFileSync(framePath(directory))
        writeFileSync(framePath(directory), Buffer.concat([original, Buffer.from([0, 1, 2])]))
      })

      expect(second.writes.frames).toBe(1)
      expect(second.writes.documents).toBe(0)
      expect(second.writes.saves).toBe(0)

      const resolved = Ingest.resolveRunsRoot(root)
      const frames = yield* db
        .select()
        .from(runFrames)
        .where(and(eq(runFrames.runsRoot, resolved), eq(runFrames.gameId, TERMINAL)))
      sameBytes(frames.find((row) => row.name === "000000.png")?.bytes, bytesOf(framePath(root)))
      sameBytes(
        frames.find((row) => row.name === "000752.png")?.bytes,
        bytesOf(join(root, TERMINAL, "watch_frames", "000752.png"))
      )

      discard(root)
    })))

  it("drops a child row whose file disappeared, without touching its siblings", () =>
    run(Effect.gen(function*() {
      const db = yield* PgDrizzle.PgDrizzle
      const { root, second } = yield* mutated((directory) => {
        rmSync(join(directory, TERMINAL, "watch_frames", "000000.png"))
      })

      expect(second.writes.deletes).toBe(1)
      expect(second.writes.frames).toBe(0)

      const resolved = Ingest.resolveRunsRoot(root)
      const frames = yield* db
        .select()
        .from(runFrames)
        .where(and(eq(runFrames.runsRoot, resolved), eq(runFrames.gameId, TERMINAL)))
      expect(frames.map((row) => row.name)).toEqual(["000752.png"])

      discard(root)
    })))
})

// ---------------------------------------------------------------- deletion ---

describe("deletion", () => {
  it("removes a run that left the disk, cascading to its children", () =>
    run(Effect.gen(function*() {
      const db = yield* PgDrizzle.PgDrizzle
      const root = copyFixtures()
      yield* sweep(root)
      const resolved = Ingest.resolveRunsRoot(root)

      rmSync(join(root, TERMINAL), { recursive: true })
      const second = yield* sweep(root)

      expect(second.deleted).toEqual([TERMINAL])
      expect(second.writes.deletes).toBe(1)

      const remaining = yield* db.select().from(runs).where(eq(runs.runsRoot, resolved))
      expect(remaining.map((row) => row.gameId).toSorted()).toEqual(
        INGESTED_IDS.filter((id) => id !== TERMINAL).toSorted()
      )
      // ON DELETE CASCADE, not a soft flag: a filter that can be forgotten is a
      // byte-parity bug waiting to happen.
      const orphans = yield* db
        .select()
        .from(runFrames)
        .where(and(eq(runFrames.runsRoot, resolved), eq(runFrames.gameId, TERMINAL)))
      expect(orphans).toEqual([])
      const documents = yield* documentsOf(db, resolved, TERMINAL)
      expect(documents).toEqual([])

      discard(root)
    })))

  it("never deletes outside the ids a --game sweep was given", () =>
    run(Effect.gen(function*() {
      const db = yield* PgDrizzle.PgDrizzle
      const root = copyFixtures()
      yield* sweep(root)
      const resolved = Ingest.resolveRunsRoot(root)

      rmSync(join(root, TERMINAL), { recursive: true })
      const scoped = yield* sweep(root, { gameIds: new Set([NOWIN]) })

      expect(scoped.runs.map((entry) => entry.gameId)).toEqual([NOWIN])
      // The sweep never looked at `TERMINAL`, so its absence is not evidence.
      expect(scoped.deleted).toEqual([])
      const remaining = yield* db.select().from(runs).where(eq(runs.runsRoot, resolved))
      expect(remaining.length).toBe(INGESTED_IDS.length)

      // Naming the missing id is what licenses the delete — and it is reported.
      const named = yield* sweep(root, { gameIds: new Set([TERMINAL]) })
      expect(named.deleted).toEqual([TERMINAL])
      expect(named.skipped.map((skip) => skip.reason)).toEqual(["requestedButAbsent"])

      discard(root)
    })))

  it("keeps two runs_roots in one database independent", () =>
    run(Effect.gen(function*() {
      const db = yield* PgDrizzle.PgDrizzle
      const other = copyFixtures()
      yield* sweep(FIXTURES)
      yield* sweep(other)

      const resolvedOther = Ingest.resolveRunsRoot(other)
      rmSync(join(other, TERMINAL), { recursive: true })
      const second = yield* sweep(other)
      expect(second.deleted).toEqual([TERMINAL])

      // The corpus root is untouched: every statement is scoped by runs_root.
      const original = yield* db
        .select()
        .from(runs)
        .where(eq(runs.runsRoot, Ingest.resolveRunsRoot(FIXTURES)))
      expect(original.length).toBe(INGESTED_IDS.length)
      const trimmed = yield* db.select().from(runs).where(eq(runs.runsRoot, resolvedOther))
      expect(trimmed.length).toBe(INGESTED_IDS.length - 1)

      discard(other)
    })))
})

// ------------------------------------------------------------------ the CLI ---

describe("collectRun", () => {
  beforeAll(() => {
    // A guard against the corpus moving out from under these tests.
    expect(readFileSync(join(FIXTURES, TERMINAL, "victory.json"), "utf8")).toContain('"turn": 753')
  })

  it("is a pure read: two calls on the same directory hash identically", () => {
    const options = Ingest.ingestOptions(FIXTURES)
    const root = Ingest.resolveRunsRoot(FIXTURES)
    const left = Ingest.collectRun(root, TERMINAL, options)
    const right = Ingest.collectRun(root, TERMINAL, options)
    sameBytes(left.contentHash, right.contentHash)
    // …and a different PPM head is a different archive, so the hash moves.
    const shallow = Ingest.collectRun(root, TERMINAL, { ...options, ppmHeadBytes: 64 })
    expect(Array.from(shallow.contentHash)).not.toEqual(Array.from(left.contentHash))
  })

  it("hashes two identical archives to the same value", () => {
    const copy = copyFixtures()
    const left = Ingest.collectRun(Ingest.resolveRunsRoot(FIXTURES), TERMINAL, Ingest.ingestOptions(FIXTURES))
    const right = Ingest.collectRun(Ingest.resolveRunsRoot(copy), TERMINAL, Ingest.ingestOptions(copy))
    sameBytes(left.contentHash, right.contentHash)
    discard(copy)
  })
})

describe("the SQL surface", () => {
  it("scopes every write by runs_root, and never orders or filters on a served column", () =>
    run(Effect.gen(function*() {
      const client = yield* SqlClient.SqlClient
      yield* sweep(FIXTURES)
      // A cheap structural guard: the module must not have learned to sort or
      // project in SQL, because `sortDiskRows` and the `public*` coercions are
      // the only things allowed to decide what a response body contains.
      const source = readFileSync(fileURLToPath(new URL("../src/ingest.ts", import.meta.url)), "utf8")
      expect(source.toLowerCase()).not.toContain("order by")
      expect(source.toLowerCase()).not.toContain("group by")
      expect(source).not.toContain("::jsonb")

      // And the schema really did apply — a sanity check that this ran at all.
      const tables = yield* client.unsafe<{ count: number }>(
        `select count(*)::int as count from runs`
      )
      expect(tables[0]?.count).toBe(INGESTED_IDS.length)
    })))
})

// ------------------------------------------------------------------ refusal ---

describe("artifacts too large to store", () => {
  it("refuses an oversize payload instead of truncating it, and ingests the run anyway", () =>
    run(Effect.gen(function*() {
      const db = yield* PgDrizzle.PgDrizzle
      // Below the smallest fixture PNG (27023 bytes) and above every document,
      // so exactly the frames are refused.
      const report = yield* sweep(FIXTURES, { maxBinaryRowBytes: 4096 })

      // Truncation would serve wrong bytes under a right `byte_size`, which is
      // the one failure mode byte parity cannot survive. Refusal is reported.
      expect(report.skipped.map((skip) => skip.reason)).toContain("oversizedArtifact")
      expect(report.skipped.some((skip) => skip.entry === TERMINAL)).toBe(true)
      expect((yield* db.select().from(runFrames)).length).toBe(0)
      // …and the run itself is still ingested, documents and all.
      expect(fieldOf(report, TERMINAL, "outcome")).toBe("inserted")
      expect((yield* documentsOf(db, Ingest.resolveRunsRoot(FIXTURES), TERMINAL)).length).toBe(3)
    })))
})

// ------------------------------------------------------- deletion regressions ---

describe("the deletion predicate", () => {
  it("pins the drizzle behaviour that makes an empty exclusion list dangerous", () =>
    run(Effect.gen(function*() {
      const db = yield* PgDrizzle.PgDrizzle
      yield* sweep(FIXTURES)
      const root = Ingest.resolveRunsRoot(FIXTURES)

      // Not a test of ingest: a pin on `drizzle-orm`. `notInArray(column, [])`
      // compiles to the literal `true`, so a stale-run deletion written as
      // `where(and(eq(runs_root), notInArray(game_id, …)))` over an empty list
      // matches *every* row instead of none — it deletes the whole archive. The
      // deletion in `ingest.ts` must therefore name the stale ids with
      // `inArray`, and this row count is what makes a regression loud.
      const matched = yield* db.select({ gameId: runs.gameId }).from(runs).where(
        and(eq(runs.runsRoot, root), notInArray(runs.gameId, []))
      )
      expect(matched.length).toBe(INGESTED_IDS.length)
    })))

  it("leaves no sweep row behind when the root could not be listed", () =>
    run(Effect.gen(function*() {
      const db = yield* PgDrizzle.PgDrizzle
      const failure = yield* Effect.either(
        sweep(join(tmpdir(), "arena-ingest-unlistable-9c2f"))
      )
      expect(Either.isLeft(failure)).toBe(true)
      // The walk runs before the sweep is opened, so a root nobody could read
      // leaves no `running` row for a later reader to mistake for a live sweep.
      expect((yield* db.select().from(ingestSweeps)).length).toBe(0)
    })))

  it("marks the sweep complete and counts what it saw", () =>
    run(Effect.gen(function*() {
      const db = yield* PgDrizzle.PgDrizzle
      yield* sweep(FIXTURES)
      const sweeps = yield* db.select().from(ingestSweeps)
      expect(sweeps.length).toBe(1)
      expect(sweeps[0]?.status).toBe("complete")
      expect(sweeps[0]?.seenCount).toBe(INGESTED_IDS.length)
      expect(sweeps[0]?.finishedAt).not.toBeNull()
    })))
})

// ---------------------------------------------------------------------- CLI ---

describe("the command line", () => {
  it("maps its flags onto ingest options", () => {
    const options = Cli.ingestOptionsOf({
      runsRoot: "/srv/runs",
      databaseUrl: Redacted.make("postgres://user:secret@localhost:5432/arena"),
      game: [TERMINAL, NOWIN],
      dryRun: true,
      skipMigrations: false,
      ppmHeadBytes: 8192,
      maxBinaryRowBytes: 1024
    })
    expect(options.runsRoot).toBe("/srv/runs")
    expect([...options.gameIds].toSorted((left, right) => left.localeCompare(right)))
      .toEqual([TERMINAL, NOWIN].toSorted((left, right) => left.localeCompare(right)))
    expect(options.dryRun).toBe(true)
    expect(options.ppmHeadBytes).toBe(8192)
    expect(options.maxBinaryRowBytes).toBe(1024)
  })

  it("describes a bad database url without echoing anything the operator typed", () => {
    const message = Cli.describeIngestError(new Client.DatabaseUrlInvalid({ problem: "scheme" }))
    // The two scheme names in this sentence are the *legal* ones, not the
    // operator's string: a database URL may carry credentials, so no branch of
    // the error path may quote it, and `Redacted` keeps @effect/cli from
    // quoting it either.
    expect(message).toBe("database url must use the postgres:// or postgresql:// scheme")
    expect(message).not.toContain("secret")
    expect(message).not.toContain("localhost")
  })

  it("names the directory when the root cannot be listed", () => {
    const message = Cli.describeIngestError(
      new Ingest.RunsRootUnreadable({ runsRoot: "/srv/runs" })
    )
    expect(message).toContain("/srv/runs")
  })

  it("exits 0 on success and 2 on failure", () => {
    const codes: Array<number> = []
    Cli.ingestTeardown(Effect.runSyncExit(Effect.void), (code) => codes.push(code))
    Cli.ingestTeardown(
      Effect.runSyncExit(Effect.fail(new Ingest.RunsRootUnreadable({ runsRoot: "/x" }))),
      (code) => codes.push(code)
    )
    // The gateway's convention, so a script can treat the two the same.
    expect(codes).toEqual([0, Cli.INGEST_CLI_ERROR_EXIT_CODE])
  })
})
