/**
 * An adversarial hunt at the v2 ingest boundary: dirty data, moving disks, and
 * repetition.
 *
 * `test/ingest.test.ts` walks the eight parity fixtures and asserts the happy
 * contract. This file assumes the archive is hostile. It asks six questions that
 * only a *writer* has to answer, and each `describe` below is one of them:
 *
 * 1. **Is a sweep a fixpoint?** Ten sweeps in a row over an unchanged root must
 *    write nothing at all, ten times — not "almost nothing", and not "nothing
 *    the row counters noticed". The proof is
 *    `pg_current_xact_id_if_assigned()`, read inside each run's transaction.
 * 2. **What happens when the disk moves under the sweep?** Runs appear, vanish,
 *    come back, turn into symlinks, lose their replay, gain a save. Each one
 *    must move exactly its own rows.
 * 3. **What happens to a manifest that parses but lies?** Every value a typed
 *    column reads is attacker-shaped here: `1e400`, a string where a float
 *    belongs, ten thousand characters of state, U+0000, a lone surrogate,
 *    invalid UTF-8. None of them may fail a sweep, because none of them fails
 *    the fs backend — and the two that Postgres genuinely cannot hold are
 *    *declared*, not silently repaired.
 * 4. **What does a number do that a typed column cannot hold?** This is where
 *    the hunt found blood. D1–D11 were pins written against v1's blob mirror;
 *    v2 moved the risk from `bytea` into `integer` and `bigint`, so the block
 *    keeps the numbering and changes the subject — and adds D12/D13, the two
 *    driver hazards measured on this machine:
 *    - **D12**: `bigint({mode:"number"})` launders values past 2^53 **silently**
 *      (`9007199254740993` → `…992`, no error).
 *    - **D13**: `integer` **rounds** a non-integer (`5.5` → `6`) rather than
 *      refusing it; only out-of-range throws.
 *    Every guard against those lives at ingest, which is why they are pinned
 *    here as *stored* values rather than as unit tests of a predicate.
 * 5. **Does any of it survive volume?** A hundred runs in one root, swept twice.
 * 6. **What licenses a deletion?** v2 has no `runs_root` column, so pruning is
 *    opt-in — and this block pins what an unscoped `--prune` costs, rather than
 *    describing it in a comment.
 *
 * Every hermetic case runs on PGlite. One thing a single-process WASM database
 * cannot express is the *interleaving* of two sweeps of one root, so the licence
 * the delete phase asks for — `Ingest.isRunOnDisk` — is exported and tested
 * directly rather than left to be observed by winning a race.
 *
 * PGlite is not a perfect twin, and one difference is load-bearing here: its
 * parameter ceiling is a *signed* int16 (32767) where a real server's is
 * unsigned (65535), and crossing it is **silent** — zero rows, no error, and the
 * transaction's earlier statements lost with it. `ingest.ts` budgets to the
 * lower of the two, which is the only value at which these tests predict the
 * live server.
 */

import { BunContext } from "@effect/platform-bun"
import * as PgDrizzle from "@effect/sql-drizzle/Pg"
import * as SqlClient from "@effect/sql/SqlClient"
import { eq } from "drizzle-orm"
import * as Effect from "effect/Effect"
import * as Either from "effect/Either"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import { describe, expect, it } from "bun:test"
// oxlint-disable-next-line effecttsgo/node-builtin-import
import {
  mkdirSync,
  mkdtempSync,
  rmSync,
  symlinkSync,
  truncateSync,
  writeFileSync
  // oxlint-disable-next-line effecttsgo/node-builtin-import
} from "node:fs"
// oxlint-disable-next-line effecttsgo/node-builtin-import
import { tmpdir } from "node:os"
// oxlint-disable-next-line effecttsgo/node-builtin-import
import { join } from "node:path"

import type { DerivationRequest, DerivationRunner } from "../../harness/src/gateway/services/derivation.ts"
import * as Ingest from "../src/ingest.ts"
import * as Migrate from "../src/migrate.ts"
import * as Pglite from "../src/pglite.ts"
import { agentStats, boardState, games, playerTurns, seats, turns } from "../src/schema.ts"

// ---------------------------------------------------------------- harness ---

const testLayer = Layer.mergeAll(
  Layer.provideMerge(PgDrizzle.layer, Pglite.layer(Pglite.memory)),
  BunContext.layer
)

type TestContext = Layer.Layer.Success<typeof testLayer>

const run = <A, E>(effect: Effect.Effect<A, E, TestContext>): Promise<A> =>
  // oxlint-disable-next-line effecttsgo/strict-effect-provide
  Effect.runPromise(
    Effect.scoped(Effect.provide(Effect.flatMap(Migrate.run, () => effect), testLayer))
  )

const sweep = (
  root: string,
  overrides: Partial<Ingest.IngestOptions> = {}
): Effect.Effect<Ingest.IngestReport, Ingest.IngestError, TestContext> =>
  Ingest.ingest({ ...Ingest.ingestOptions(root), ...overrides })

/** A sweep that is allowed to fail, so a failure can be *asserted*. */
const attemptSweep = (
  root: string,
  overrides: Partial<Ingest.IngestOptions> = {}
) => Effect.either(sweep(root, overrides))

/** A pruning sweep — v2's delete phase is opt-in, so most tests must ask for it. */
const pruningSweep = (
  root: string,
  overrides: Partial<Ingest.IngestOptions> = {}
) => sweep(root, { ...overrides, prune: true })

const newRoot = (): string => {
  const root = join(mkdtempSync(join(tmpdir(), "arena-hunt-")), "runs")
  mkdirSync(root, { recursive: true })
  return root
}

const gid = (n: number): string => `game_hunt_${String(n).padStart(16, "0")}`

/**
 * A manifest with every field the gateway's projections read.
 *
 * Deliberately *not* schema-valid in every respect — several cases below make
 * the typed columns fail on purpose. What matters is that it is a JSON object,
 * which is what decides `manifest_status = 'ok'`.
 */
const manifestFor = (id: string, extra: Record<string, unknown> = {}): Record<string, unknown> => ({
  benchmark_valid: true,
  created_at: 1785660947.0197709,
  current_turn: 752,
  finished_at: 1785676381.883568,
  game_id: id,
  state: "completed",
  status: "completed",
  ...extra
})

interface RunSpec {
  readonly id: string
  readonly manifest?: string | Uint8Array
  readonly report?: string
  readonly victory?: string
  readonly replay?: string
  readonly saves?: ReadonlyArray<string>
}

const makeRun = (root: string, spec: RunSpec): string => {
  const dir = join(root, spec.id)
  mkdirSync(dir, { recursive: true })
  writeFileSync(
    join(dir, "manifest.json"),
    spec.manifest ?? JSON.stringify(manifestFor(spec.id))
  )
  if (spec.report !== undefined) {
    writeFileSync(join(dir, "report.json"), spec.report)
  }
  if (spec.victory !== undefined) {
    writeFileSync(join(dir, "victory.json"), spec.victory)
  }
  if (spec.replay !== undefined) {
    writeFileSync(join(dir, "replay.jsonl"), spec.replay)
  }
  if (spec.saves !== undefined) {
    mkdirSync(join(dir, "saves"), { recursive: true })
    spec.saves.forEach((name) => writeFileSync(join(dir, "saves", name), "savegame"))
  }
  return dir
}

/** A manifest-only run: the smallest thing the walk will accept. */
const plainRun = (root: string, id: string): string => makeRun(root, { id })

const hex = (bytes: Uint8Array): string =>
  Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("")

/** Replay lines for `turns` turns and `seats` seats, as the supervisor writes them. */
const replayOf = (turnCount: number, seatCount: number): string =>
  Array.from({ length: turnCount }, (_, index) => {
    const turn = index + 1
    const players = Array.from({ length: seatCount }, (_, seat) =>
      `{"seat_id": "place-${String(seat + 1)}", "player_id": ${String(seat)}, "score": ${
        String(turn)
      }, "population": ${String(turn * 1000)}}`)
    return `{"turn": ${String(turn)}, "year": ${String(-4000 + turn)}, "players": [${
      players.join(",")
    }]}`
  }).join("\n") + "\n"

/** Every domain table's rows, as one comparable list of strings. */
const snapshot = Effect.gen(function*() {
  const db = yield* PgDrizzle.PgDrizzle
  const [gameRows, seatRows, turnRows, playerRows, statRows, boardRows] = yield* Effect.all(
    [
      Effect.orDie(db.select().from(games)),
      Effect.orDie(db.select().from(seats)),
      Effect.orDie(db.select().from(turns)),
      Effect.orDie(db.select().from(playerTurns)),
      Effect.orDie(db.select().from(agentStats)),
      Effect.orDie(db.select().from(boardState))
    ],
    { concurrency: 1 }
  )
  return [
    ...gameRows.map((row) =>
      `game ${row.gameId} ${row.state} ${row.manifestStatus}/${row.reportStatus} ${
        hex(row.contentHash)
      }`
    ),
    ...seatRows.map((row) => `seat ${row.gameId} ${String(row.seatIndex)} ${String(row.kind)}`),
    ...turnRows.map((row) => `turn ${row.gameId} ${String(row.turn)} ${String(row.year)}`),
    ...playerRows.map((row) =>
      `player ${row.gameId} ${String(row.turn)} ${row.seatId} ${String(row.score)}`
    ),
    ...statRows.map((row) => `stat ${row.gameId} ${row.seatId} ${String(row.turns)}`),
    ...boardRows.map((row) => `board ${row.gameId} ${String(row.turn)}`)
  ].toSorted()
})

const storedIds = Effect.map(
  Effect.flatMap(PgDrizzle.PgDrizzle, (db) =>
    Effect.orDie(db.select({ gameId: games.gameId }).from(games))),
  (rows) => rows.map((row) => row.gameId).toSorted()
)

const written = (report: Ingest.IngestReport): number => Ingest.dataWrites(report.writes)

const asObject = (value: unknown): { readonly [key: string]: unknown } =>
  typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as { readonly [key: string]: unknown })
    : {}

const extrasManifestOf = (row: { readonly extras: unknown }) =>
  asObject(asObject(row.extras)["manifest"])

const extrasDerivedOf = (row: { readonly extras: unknown }) =>
  asObject(asObject(row.extras)["derived"])

// ==========================================================================
// 1. A sweep is a fixpoint
// ==========================================================================

describe("re-ingest is a fixpoint", () => {
  const fixpointRoot = (): string => {
    const root = newRoot()
    Array.from({ length: 5 }, (_, index) =>
      makeRun(root, {
        id: gid(index),
        report: JSON.stringify({
          seat_stats: { "place-1": { turns: 7, decisions: 7, mean_latency_ms: 1.5 } }
        }),
        victory: JSON.stringify({ turn: 753, year: 1995 }),
        replay: replayOf(4, 2),
        saves: ["turn-0001-auto.sav.gz"]
      }))
    return root
  }

  it("writes nothing on each of ten consecutive sweeps of an unchanged root", () =>
    run(Effect.gen(function*() {
      const root = fixpointRoot()
      const first = yield* sweep(root)
      expect(written(first)).toBeGreaterThan(0)

      const repeats = yield* Effect.forEach(
        Array.from({ length: 10 }),
        () => sweep(root),
        { concurrency: 1 }
      )
      // Ten separate proofs, not one: a counter that stayed level could be a
      // rewrite that happened to produce the same numbers, but a transaction
      // that never assigned an id provably never wrote a tuple.
      expect(repeats.map(written)).toEqual(Array.from({ length: 10 }, () => 0))
      expect(repeats.map((report) => report.transactionsWithWrites)).toEqual(
        Array.from({ length: 10 }, () => 0)
      )
    })), 60_000)

  it("reports every run unchanged on each of ten sweeps", () =>
    run(Effect.gen(function*() {
      const root = fixpointRoot()
      yield* sweep(root)
      const repeats = yield* Effect.forEach(
        Array.from({ length: 10 }),
        () => sweep(root),
        { concurrency: 1 }
      )
      expect(
        repeats.every((report) => report.runs.every((entry) => entry.outcome === "unchanged"))
      ).toBe(true)
    })), 60_000)

  it("leaves the stored rows identical after ten sweeps", () =>
    run(Effect.gen(function*() {
      const root = fixpointRoot()
      yield* sweep(root)
      const before = yield* snapshot
      yield* Effect.forEach(Array.from({ length: 10 }), () => sweep(root), { concurrency: 1 })
      expect(yield* snapshot).toEqual(before)
    })), 60_000)

  it("keeps ingested_at still across ten sweeps", () =>
    run(Effect.gen(function*() {
      const db = yield* PgDrizzle.PgDrizzle
      const root = fixpointRoot()
      yield* sweep(root)
      const stamped = yield* Effect.orDie(
        db.select({ at: games.ingestedAt }).from(games).where(eq(games.gameId, gid(0)))
      )
      yield* Effect.forEach(Array.from({ length: 10 }), () => sweep(root), { concurrency: 1 })
      const after = yield* Effect.orDie(
        db.select({ at: games.ingestedAt }).from(games).where(eq(games.gameId, gid(0)))
      )
      // `ingested_at` means "when this run's content last moved", which is what
      // the `IS DISTINCT FROM` guard on the upsert is for.
      expect(after[0]?.at).toEqual(stamped[0]?.at)
    })), 60_000)

  it("hashes a run identically on ten independent reads of the same directory", () => {
    const root = fixpointRoot()
    const options = Ingest.ingestOptions(root)
    const hashes = Array.from(
      { length: 10 },
      () => hex(Ingest.collectRun(Ingest.resolveRunsRoot(root), gid(0), options).contentHash)
    )
    expect(new Set(hashes).size).toBe(1)
  })

  it("still reports every run unchanged from a dry run after ten sweeps", () =>
    run(Effect.gen(function*() {
      const root = fixpointRoot()
      yield* Effect.forEach(Array.from({ length: 10 }), () => sweep(root), { concurrency: 1 })
      const dry = yield* sweep(root, { dryRun: true })
      expect(dry.runs.every((entry) => entry.outcome === "unchanged")).toBe(true)
      expect(written(dry)).toBe(0)
      expect(dry.deleted).toEqual([])
    })), 60_000)

  it("writes nothing when only a file's mtime moved", () =>
    run(Effect.gen(function*() {
      const root = fixpointRoot()
      yield* sweep(root)
      writeFileSync(join(root, gid(0), "manifest.json"), JSON.stringify(manifestFor(gid(0))))
      const again = yield* sweep(root)
      expect(written(again)).toBe(0)
      expect(again.transactionsWithWrites).toBe(0)
    })), 60_000)

  it("writes nothing when victory.json changes, because v2 stores no victory", () =>
    run(Effect.gen(function*() {
      const root = fixpointRoot()
      yield* sweep(root)
      writeFileSync(join(root, gid(0), "victory.json"), JSON.stringify({ turn: 999 }))
      const again = yield* sweep(root)
      // `/result` reads `victory.json` from disk in **both** backends, so it is
      // deliberately outside the content hash: a rewritten victory must not
      // rewrite a run's typed rows.
      expect(written(again)).toBe(0)
      expect(again.transactionsWithWrites).toBe(0)
    })), 60_000)
})

// ==========================================================================
// 2. The disk moves under the sweep
// ==========================================================================

describe("a root that moves between sweeps", () => {
  it("inserts an appearing run and touches nothing else", () =>
    run(Effect.gen(function*() {
      const root = newRoot()
      plainRun(root, gid(1))
      yield* sweep(root)
      plainRun(root, gid(2))
      const second = yield* sweep(root)

      expect(second.runs.filter((entry) => entry.outcome === "inserted").map((e) => e.gameId))
        .toEqual([gid(2)])
      expect(second.writes.games).toBe(1)
      expect(second.writes.deletes).toBe(0)
      expect(second.transactionsWithWrites).toBe(1)
    })))

  it("deletes a vanished run with --prune, and cascades to every child table", () =>
    run(Effect.gen(function*() {
      const root = newRoot()
      makeRun(root, {
        id: gid(1),
        replay: replayOf(3, 2),
        report: JSON.stringify({ seat_stats: { "place-1": { turns: 3 } } })
      })
      plainRun(root, gid(2))
      yield* sweep(root)
      rmSync(join(root, gid(1)), { recursive: true, force: true })
      const second = yield* pruningSweep(root)

      expect(second.deleted).toEqual([gid(1)])
      expect(yield* storedIds).toEqual([gid(2)])
      expect((yield* snapshot).some((line) => line.includes(gid(1)))).toBe(false)
    })))

  it("restores a run that came back, row for row", () =>
    run(Effect.gen(function*() {
      const root = newRoot()
      makeRun(root, { id: gid(1), replay: replayOf(3, 1) })
      yield* sweep(root)
      const before = yield* snapshot

      const saved = join(root, "..", "stash")
      mkdirSync(saved, { recursive: true })
      Bun.spawnSync(["cp", "-R", join(root, gid(1)), join(saved, gid(1))])
      rmSync(join(root, gid(1)), { recursive: true, force: true })
      expect((yield* pruningSweep(root)).deleted).toEqual([gid(1)])
      Bun.spawnSync(["cp", "-R", join(saved, gid(1)), join(root, gid(1))])
      const restored = yield* sweep(root)

      expect(restored.runs.map((entry) => entry.outcome)).toEqual(["inserted"])
      expect(yield* snapshot).toEqual(before)
    })))

  it("skips a run that became a symlink, and deletes it only under --prune", () =>
    run(Effect.gen(function*() {
      const root = newRoot()
      plainRun(root, gid(1))
      plainRun(root, gid(2))
      yield* sweep(root)
      rmSync(join(root, gid(1)), { recursive: true, force: true })
      symlinkSync(join(root, gid(2)), join(root, gid(1)))

      const kept = yield* sweep(root)
      expect(kept.skipped.map((skip) => skip.reason)).toEqual(["symlink"])
      expect(kept.deleted).toEqual([])
      expect(yield* storedIds).toEqual([gid(1), gid(2)])

      const pruned = yield* pruningSweep(root)
      expect(pruned.deleted).toEqual([gid(1)])
      expect(yield* storedIds).toEqual([gid(2)])
    })))

  it("deletes a run that became a plain file, under --prune", () =>
    run(Effect.gen(function*() {
      const root = newRoot()
      plainRun(root, gid(1))
      yield* sweep(root)
      rmSync(join(root, gid(1)), { recursive: true, force: true })
      writeFileSync(join(root, gid(1)), "not a run")
      const second = yield* pruningSweep(root)

      expect(second.deleted).toEqual([gid(1)])
      expect(second.skipped.map((skip) => skip.reason)).toEqual(["notADirectory"])
    })))

  it("keeps a husk row when the run directory is emptied, and drops every child", () =>
    run(Effect.gen(function*() {
      const db = yield* PgDrizzle.PgDrizzle
      const root = newRoot()
      makeRun(root, {
        id: gid(1),
        replay: replayOf(3, 2),
        report: JSON.stringify({ seat_stats: { "place-1": { turns: 3 } } })
      })
      yield* sweep(root)
      rmSync(join(root, gid(1)), { recursive: true, force: true })
      mkdirSync(join(root, gid(1)), { recursive: true })
      const second = yield* pruningSweep(root)

      // The entry is still a directory, so it is still a run; it simply has
      // nothing in it — which is the 404 the fs backend answers for the same
      // state, spelled `manifest_status = 'absent'`.
      expect(second.deleted).toEqual([])
      const rows = yield* Effect.orDie(db.select().from(games).where(eq(games.gameId, gid(1))))
      expect(rows[0]?.manifestStatus).toBe("absent")
      expect(rows[0]?.reportStatus).toBe("absent")
      expect(rows[0]?.state).toBe(Ingest.UNRECOGNIZED_STATE)
      expect(rows[0]?.extras).toBe(null)
      expect((yield* snapshot).filter((line) => !line.startsWith("game "))).toEqual([])
    })))

  it("moves exactly the games row when the manifest is rewritten", () =>
    run(Effect.gen(function*() {
      const root = newRoot()
      makeRun(root, { id: gid(1), replay: replayOf(3, 2) })
      yield* sweep(root)
      writeFileSync(
        join(root, gid(1), "manifest.json"),
        JSON.stringify(manifestFor(gid(1), { current_turn: 999 }))
      )
      const second = yield* sweep(root)

      expect(second.writes.games).toBe(1)
      expect(second.writes.turns).toBe(0)
      expect(second.writes.playerTurns).toBe(0)
      expect(second.writes.agentStats).toBe(0)
      expect(second.writes.deletes).toBe(0)
    })))

  it("moves exactly the turn rows when replay.jsonl is appended to", () =>
    run(Effect.gen(function*() {
      const root = newRoot()
      makeRun(root, { id: gid(1), replay: replayOf(2, 1) })
      yield* sweep(root)
      writeFileSync(join(root, gid(1), "replay.jsonl"), replayOf(3, 1))
      const second = yield* sweep(root)

      // The `games` row moves too, because `last_replay_turn` is one of its
      // columns; nothing else that did not change is rewritten.
      expect(second.writes.turns).toBe(1)
      expect(second.writes.playerTurns).toBe(1)
      expect(second.writes.seats).toBe(0)
      expect(second.writes.deletes).toBe(0)
    })))

  it("deletes the turn rows a shortened replay no longer has", () =>
    run(Effect.gen(function*() {
      const db = yield* PgDrizzle.PgDrizzle
      const root = newRoot()
      makeRun(root, { id: gid(1), replay: replayOf(4, 2) })
      yield* sweep(root)
      writeFileSync(join(root, gid(1), "replay.jsonl"), replayOf(2, 2))
      const second = yield* sweep(root)

      // Two `turns` rows go, and their four `player_turns` rows go with them
      // through the composite foreign key's cascade — which is why the counter
      // says 2 and the tables say 2 and 4.
      expect(second.writes.deletes).toBe(2)
      expect(second.writes.turns).toBe(0)
      const remainingTurns = yield* Effect.orDie(
        db.select().from(turns).where(eq(turns.gameId, gid(1)))
      )
      const remainingPlayers = yield* Effect.orDie(
        db.select().from(playerTurns).where(eq(playerTurns.gameId, gid(1)))
      )
      expect(remainingTurns.map((row) => row.turn).toSorted((a, b) => a - b)).toEqual([1, 2])
      expect(remainingPlayers.length).toBe(4)
    })))

  it("deletes nothing when the root stopped being listable", () =>
    run(Effect.gen(function*() {
      const root = newRoot()
      plainRun(root, gid(1))
      yield* sweep(root)
      const before = yield* snapshot
      rmSync(root, { recursive: true, force: true })

      const failed = yield* attemptSweep(root, { prune: true })
      expect(Either.isLeft(failed)).toBe(true)
      // The rows the unreadable root cannot vouch for are still there.
      expect(yield* snapshot).toEqual(before)
    })))
})

// ==========================================================================
// 3. Manifests that decode but lie
// ==========================================================================

describe("manifests that parse but violate every typed-column expectation", () => {
  const withManifest = (body: string | Uint8Array) =>
    Effect.gen(function*() {
      const db = yield* PgDrizzle.PgDrizzle
      const root = newRoot()
      makeRun(root, { id: gid(1), manifest: body })
      const report = yield* sweep(root)
      const rows = yield* Effect.orDie(
        db.select().from(games).where(eq(games.gameId, gid(1)))
      )
      return { report, row: rows[0], root }
    })

  it("stores a manifest whose created_at overflows to Infinity as unusable", () =>
    run(Effect.gen(function*() {
      const seen = yield* withManifest(`{"game_id":"${gid(1)}","created_at":1e400}`)
      // `decodeJsonValueFromString` refuses the non-finite literal, so the
      // document is `unusable` — the same 503 the fs backend answers.
      expect(seen.row?.manifestStatus).toBe("unusable")
      expect(seen.row?.createdAt).toBe(null)
      expect(seen.row?.extras).toBe(null)
    })))

  it("records a negative zero rather than pretending it survived", () =>
    run(Effect.gen(function*() {
      const seen = yield* withManifest(`{"game_id":"${gid(1)}","created_at":-0.0}`)
      expect(seen.row?.manifestStatus).toBe("ok")
      // `String(-0) === "0"` in the parameter binding and
      // `JSON.stringify(-0) === "0"` in extras: v2 cannot carry a `-0`, so the
      // loss is written down at the pointer where it happened.
      expect(seen.row?.createdAt).toBe(null)
      expect(extrasDerivedOf(seen.row ?? { extras: null })["manifest_negative_zero"])
        .toEqual(["/created_at"])
      expect(seen.report.runs[0]?.outcome).toBe("inserted")
    })))

  it("survives created_at spelled as a string, and demotes it", () =>
    run(Effect.gen(function*() {
      const seen = yield* withManifest(`{"game_id":"${gid(1)}","created_at":"yesterday"}`)
      expect(seen.row?.manifestStatus).toBe("ok")
      expect(seen.row?.createdAt).toBe(null)
      expect(extrasManifestOf(seen.row ?? { extras: null })["created_at"]).toBe("yesterday")
    })))

  it("survives state spelled as a number, with the sentinel and the raw value", () =>
    run(Effect.gen(function*() {
      const seen = yield* withManifest(`{"game_id":"${gid(1)}","state":17}`)
      // A domain query must read this as *unrecognized*, not as "the run was
      // invalid": the raw value is right there in extras.
      expect(seen.row?.state).toBe(Ingest.UNRECOGNIZED_STATE)
      expect(extrasManifestOf(seen.row ?? { extras: null })["state"]).toBe(17)
    })))

  it("survives ten thousand characters of state", () =>
    run(Effect.gen(function*() {
      const seen = yield* withManifest(
        JSON.stringify({ game_id: gid(1), state: "x".repeat(10_000) })
      )
      expect(seen.row?.state).toBe(Ingest.UNRECOGNIZED_STATE)
      expect(String(extrasManifestOf(seen.row ?? { extras: null })["state"]).length).toBe(10_000)
      expect(seen.report.runs[0]?.outcome).toBe("inserted")
    })))

  it("survives benchmark_valid spelled as a string", () =>
    run(Effect.gen(function*() {
      const seen = yield* withManifest(JSON.stringify({ game_id: gid(1), benchmark_valid: "true" }))
      expect(seen.row?.benchmarkValid).toBe(null)
      expect(extrasManifestOf(seen.row ?? { extras: null })["benchmark_valid"]).toBe("true")
    })))

  it("survives a manifest of nothing but nulls, and keeps every key", () =>
    run(Effect.gen(function*() {
      const body = { game_id: null, state: null, created_at: null, finished_at: null, config: null }
      const seen = yield* withManifest(JSON.stringify(body))
      expect(seen.row?.manifestStatus).toBe("ok")
      // Every key present-and-null is a demotion, because a `NULL` column means
      // "the manifest had no such key" and one gateway site can tell them apart.
      expect(extrasManifestOf(seen.row ?? { extras: null })).toEqual(body)
    })))

  it("stores a manifest whose game_id names another run", () =>
    run(Effect.gen(function*() {
      const seen = yield* withManifest(JSON.stringify(manifestFor(gid(99))))
      // The cross-check is a *read-side* gate, re-run over the reconstructed
      // document, so the ingester must not pre-empt it.
      expect(seen.row?.manifestStatus).toBe("ok")
      expect(extrasManifestOf(seen.row ?? { extras: null })["game_id"]).toBe(gid(99))
    })))

  it("resolves duplicate keys the way JSON.parse does, and stores one of them", () =>
    run(Effect.gen(function*() {
      const seen = yield* withManifest(
        `{"game_id":"${gid(1)}","state":"running","state":"completed"}`
      )
      // The fs backend parses the same bytes with the same parser, so "last one
      // wins" is parity, not a choice made here.
      expect(seen.row?.state).toBe("completed")
    })))

  it("refuses a manifest carrying U+0000 — loudly, as a declared divergence", () =>
    run(Effect.gen(function*() {
      const seen = yield* withManifest(`{"game_id":"${gid(1)}","objective":"a\\u0000b"}`)
      // `text` refuses U+0000 and `jsonb` refuses it too; there is no
      // bytes-shaped carrier in the schema and a silent strip would change
      // `resolved_places` and with it the derivation cache key. So the document
      // is `unusable` (the fs backend serves it — a recorded divergence) and the
      // sweep says which run and which pointer.
      expect(seen.row?.manifestStatus).toBe("unusable")
      expect(seen.row?.extras).toBe(null)
      const unstorable = seen.report.skipped.filter((skip) =>
        skip.reason === "documentUnstorable"
      )
      expect(unstorable.length).toBe(1)
      expect(Option.getOrElse(unstorable[0]?.detail ?? Option.none(), () => "")).toContain(
        "/objective"
      )
      expect(Ingest.describeReport(seen.report)).toContain("unstorable     1")
    })))

  it("refuses a manifest carrying a lone surrogate, the same way", () =>
    run(Effect.gen(function*() {
      const seen = yield* withManifest(`{"game_id":"${gid(1)}","objective":"a\\ud800b"}`)
      // Quieter than U+0000 and worse: `text` would *silently* rewrite it to
      // U+FFFD, which is a changed document nobody would notice.
      expect(seen.row?.manifestStatus).toBe("unusable")
      expect(seen.report.skipped.map((skip) => skip.reason)).toEqual(["documentUnstorable"])
    })))

  it("refuses a U+0000 hidden in a key, not only in a value", () =>
    run(Effect.gen(function*() {
      const seen = yield* withManifest(`{"game_id":"${gid(1)}","a\\u0000b":1}`)
      expect(seen.row?.manifestStatus).toBe("unusable")
    })))

  it("calls invalid UTF-8 unusable", () =>
    run(Effect.gen(function*() {
      const seen = yield* withManifest(Uint8Array.from([0x7b, 0xff, 0x7d]))
      expect(seen.row?.manifestStatus).toBe("unusable")
      expect(seen.row?.manifestByteSize).toBe(3)
    })))

  it("refuses a manifest one byte over the 8 MiB gate, on its fstat size", () =>
    run(Effect.gen(function*() {
      const body = `{"game_id":"${gid(1)}","pad":"${"x".repeat(8 * 1024 * 1024)}"}`
      const seen = yield* withManifest(body)
      expect(seen.row?.manifestStatus).toBe("unusable")
      expect(seen.row?.manifestByteSize).toBe(body.length)
    })), 30_000)

  it("treats a zero-byte manifest as unusable, with a row and a zero size", () =>
    run(Effect.gen(function*() {
      const seen = yield* withManifest("")
      expect(seen.row?.manifestStatus).toBe("unusable")
      expect(seen.row?.manifestByteSize).toBe(0)
    })))

  it("treats a JSON array as unusable", () =>
    run(Effect.gen(function*() {
      const seen = yield* withManifest("[1,2,3]")
      expect(seen.row?.manifestStatus).toBe("unusable")
    })))

  it("survives a manifest nested two thousand levels deep", () =>
    run(Effect.gen(function*() {
      const body = `{"game_id":"${gid(1)}","deep":${"[".repeat(2000)}1${"]".repeat(2000)}}`
      const seen = yield* withManifest(body)
      expect(seen.report.runs[0]?.outcome).toBe("inserted")
      expect(seen.row?.manifestStatus).toBe("ok")
      // Measured: `jsonb` holds 2 000 levels. The demoted key is stored whole.
      expect("deep" in extrasManifestOf(seen.row ?? { extras: null })).toBe(true)
    })))

  it("survives a report.json that is a bare number, and stores no report envelope", () =>
    run(Effect.gen(function*() {
      const db = yield* PgDrizzle.PgDrizzle
      const root = newRoot()
      makeRun(root, { id: gid(1), report: "42" })
      yield* sweep(root)
      const rows = yield* Effect.orDie(db.select().from(games).where(eq(games.gameId, gid(1))))
      expect(rows[0]?.reportStatus).toBe("unusable")
      expect("report" in asObject(rows[0]?.extras)).toBe(false)
    })))

  it("survives a victory.json that is not JSON at all, and stores nothing of it", () =>
    run(Effect.gen(function*() {
      const root = newRoot()
      makeRun(root, { id: gid(1), victory: "}{ not json" })
      const report = yield* sweep(root)
      // `/result` reads it from disk in both backends; ingest never opens it.
      expect(report.runs[0]?.outcome).toBe("inserted")
      expect(JSON.stringify(yield* snapshot)).not.toContain("victory")
    })))

  it("keeps a hostile run from costing the archive its other runs", () =>
    run(Effect.gen(function*() {
      const root = newRoot()
      makeRun(root, { id: gid(1), manifest: `{"game_id":"${gid(1)}","objective":"a\\u0000b"}` })
      plainRun(root, gid(2))
      plainRun(root, gid(3))
      const report = yield* sweep(root)
      expect(yield* storedIds).toEqual([gid(1), gid(2), gid(3)])
      expect(report.runs.length).toBe(3)
    })))
})

// ==========================================================================
// 4. Numbers a typed column cannot hold — the defects
// ==========================================================================

describe("values a typed column cannot hold cost one column, never the sweep", () => {
  /**
   * D1–D8 were written against v1's blob mirror, where the risk lived in
   * `run_saves.save_turn` and in stored `bytea`. v2 has no artifact tables and
   * six typed ones, so the numbering survives and the subject moves: the same
   * hostile values now aim at `current_turn`, `schema_version`,
   * `last_replay_turn`, `player_turns.population` and the two token counters.
   */

  const rowOf = (gameId: string) =>
    Effect.flatMap(PgDrizzle.PgDrizzle, (db) =>
      Effect.map(
        Effect.orDie(db.select().from(games).where(eq(games.gameId, gameId))),
        (rows) => rows[0]
      ))

  it("D1: a current_turn past 2^63 demotes the column and ingests the run", () =>
    run(Effect.gen(function*() {
      const root = newRoot()
      makeRun(root, {
        id: gid(1),
        manifest: `{"game_id":"${gid(1)}","state":"running","current_turn":99999999999999999999}`
      })
      const report = yield* sweep(root)
      const row = yield* rowOf(gid(1))

      // The worst failure shape would be an `INSERT` that throws and a run that
      // *disappears* — a missing game rather than a demoted field.
      expect(report.runs[0]?.outcome).toBe("inserted")
      expect(row?.currentTurn).toBe(null)
      expect(extrasManifestOf(row ?? { extras: null })["current_turn"]).toBe(1e20)
    })))

  it("D1: a last_replay_turn past int32 is answered from extras, not from the column", () =>
    run(Effect.gen(function*() {
      const root = newRoot()
      makeRun(root, { id: gid(1), replay: '{"turn": 99999999999999999999}\n' })
      yield* sweep(root)
      const row = yield* rowOf(gid(1))

      // `lastReplayTurn` must answer the same `bigint` the fs backend answers,
      // and `games.last_replay_turn` is an `integer` — hence the decimal-string
      // carrier, which is the only tagged encoding this schema allows.
      expect(row?.lastReplayTurn).toBe(null)
      expect(extrasDerivedOf(row ?? { extras: null })["last_replay_turn"]).toBe(
        "99999999999999999999"
      )
    })))

  it("D2: a replay turn past int32 stores no rows rather than a rounded one", () =>
    run(Effect.gen(function*() {
      const db = yield* PgDrizzle.PgDrizzle
      const root = newRoot()
      makeRun(root, {
        id: gid(1),
        replay: '{"turn": 1, "players": []}\n{"turn": 3000000000, "players": []}\n'
      })
      const report = yield* sweep(root)
      const rows = yield* Effect.orDie(db.select().from(turns).where(eq(turns.gameId, gid(1))))

      expect(rows.map((row) => row.turn)).toEqual([1])
      expect(report.skipped.map((skip) => skip.reason)).toContain("rowUnstorable")
    })))

  it("D12: a population past 2^53 is nulled rather than silently laundered", () =>
    run(Effect.gen(function*() {
      const db = yield* PgDrizzle.PgDrizzle
      const root = newRoot()
      makeRun(root, {
        id: gid(1),
        replay: '{"turn": 1, "players": [' +
          '{"seat_id": "place-1", "population": 9007199254740993},' +
          '{"seat_id": "place-2", "population": 9007199254740991}]}\n'
      })
      yield* sweep(root)
      const rows = yield* Effect.orDie(
        db.select().from(playerTurns).where(eq(playerTurns.gameId, gid(1)))
      )
      const populations = new Map(rows.map((row) => [row.seatId, row.population]))

      // Measured on this driver: `bigint({mode:"number"})` accepts
      // 9007199254740993 and stores 9007199254740992, with no error at any
      // layer. 2^53 - 1 is exactly representable and is stored exactly; the
      // value `Number` cannot hold is never written as a value it can.
      expect(populations.get("place-2")).toBe(Number.MAX_SAFE_INTEGER)
      expect(populations.get("place-1")).toBe(null)
    })))

  it("D12: token counters past 2^53 are nulled the same way", () =>
    run(Effect.gen(function*() {
      const db = yield* PgDrizzle.PgDrizzle
      const root = newRoot()
      makeRun(root, {
        id: gid(1),
        // Written as raw JSON text: a `JSON.stringify` of the literal would
        // have laundered the value before the file ever existed.
        report: '{"seat_stats":{"place-1":' +
          '{"input_tokens":9007199254740993,"output_tokens":12,"turns":3}}}'
      })
      yield* sweep(root)
      const rows = yield* Effect.orDie(
        db.select().from(agentStats).where(eq(agentStats.gameId, gid(1)))
      )
      expect(rows[0]?.inputTokens).toBe(null)
      expect(rows[0]?.outputTokens).toBe(12)
      // The *document* is a `JsonObject` by then — `decodeJsonValueFromString`
      // is `JSON.parse`, so the envelope carries the double the fs backend also
      // sees (2^53, one below the digits on disk). The guard is about the
      // column: `Number.isSafeInteger(2^53)` is false, so it is not written at
      // all rather than written as a number that is not the one recorded.
      const stats = asObject(
        asObject(asObject((yield* rowOf(gid(1)))?.extras)["report"])["seat_stats"]
      )
      expect(asObject(stats["place-1"])["input_tokens"]).toBe(Number("9007199254740992"))
    })))

  it("D13: a non-integer is demoted rather than rounded", () =>
    run(Effect.gen(function*() {
      const db = yield* PgDrizzle.PgDrizzle
      const root = newRoot()
      makeRun(root, {
        id: gid(1),
        manifest: `{"game_id":"${gid(1)}","current_turn":5.5,"schema_version":2.5}`,
        replay: '{"turn": 1, "year": 2.5, "players": [{"seat_id": "place-1", "cities": 5.5}]}\n'
      })
      yield* sweep(root)
      const row = yield* rowOf(gid(1))
      const players = yield* Effect.orDie(
        db.select().from(playerTurns).where(eq(playerTurns.gameId, gid(1)))
      )
      const turnRows = yield* Effect.orDie(db.select().from(turns).where(eq(turns.gameId, gid(1))))

      // Measured: an `integer` column *rounds* 5.5 to 6 and says nothing; only
      // an out-of-range value throws. Every one of these would have been a
      // quiet, wrong number.
      expect(row?.currentTurn).toBe(null)
      expect(row?.schemaVersion).toBe(null)
      expect(extrasManifestOf(row ?? { extras: null })["current_turn"]).toBe(5.5)
      expect(players[0]?.cities).toBe(null)
      expect(turnRows[0]?.year).toBe(null)
    })))

  it("D13: a replay turn spelled as a float records no turn at all", () =>
    run(Effect.gen(function*() {
      const db = yield* PgDrizzle.PgDrizzle
      const root = newRoot()
      makeRun(root, { id: gid(1), replay: '{"turn": 2.0, "players": []}\n' })
      yield* sweep(root)
      const rows = yield* Effect.orDie(db.select().from(turns).where(eq(turns.gameId, gid(1))))

      // `_last_replay_turn`'s `isinstance(turn, int)` is what makes
      // `{"turn": 2.0}` answer "no turn"; a line the gateway will not count is a
      // line this will not store.
      expect(rows).toEqual([])
      expect((yield* rowOf(gid(1)))?.lastReplayTurn).toBe(null)
    })))

  it("D3: a 3 GiB document is stored unusable, with the size that failed the gate", () =>
    run(Effect.gen(function*() {
      const root = newRoot()
      const dir = makeRun(root, { id: gid(1) })
      // A sparse file: 3 GiB of address space, a few blocks on disk. Only the
      // first 8 MiB is ever read; it is the *stored fstat size* that used to
      // overflow an `integer` column and take the sweep with it.
      writeFileSync(join(dir, "report.json"), "")
      truncateSync(join(dir, "report.json"), 3 * 1024 * 1024 * 1024)

      const report = yield* sweep(root)
      const row = yield* rowOf(gid(1))
      expect(report.runs[0]?.outcome).toBe("inserted")
      expect(row?.reportStatus).toBe("unusable")
      expect(row?.reportByteSize).toBe(3 * 1024 * 1024 * 1024)
    })), 60_000)

  it("D4/D11: a run the database refuses is one reported skip, not the sweep", () =>
    run(Effect.gen(function*() {
      const client = yield* SqlClient.SqlClient
      const root = newRoot()
      // A constraint the ingester knows nothing about, so the failure arrives
      // exactly as a driver error from inside one run's transaction.
      yield* Effect.orDie(
        client.unsafe(
          `alter table games add constraint hunt_no_poison check (game_id <> '${gid(2)}')`
        )
      )
      plainRun(root, gid(1))
      plainRun(root, gid(2))
      plainRun(root, gid(3))

      const report = yield* sweep(root)

      expect(yield* storedIds).toEqual([gid(1), gid(3)])
      expect(report.status).toBe("complete")
      expect(report.runs.map((entry) => entry.gameId).toSorted()).toEqual([gid(1), gid(3)])

      const refused = report.skipped.filter((skip) => skip.reason === "databaseRefused")
      expect(refused.length).toBe(1)
      expect(refused[0]?.entry).toBe(gid(2))
      // D11: the operator is told which run and which constraint. Neither is a
      // credential, and the `SqlError`'s `cause` — which can carry connection
      // parameters — is never printed.
      expect(Option.getOrElse(refused[0]?.detail ?? Option.none(), () => "")).toContain(
        "constraint=hunt_no_poison"
      )
      expect(Ingest.describeReport(report)).toContain("refused runs   1")
    })))

  it("D4: the refused run is not deleted — it was seen", () =>
    run(Effect.gen(function*() {
      const client = yield* SqlClient.SqlClient
      const root = newRoot()
      plainRun(root, gid(1))
      plainRun(root, gid(2))
      yield* sweep(root)

      // `not valid`, so the row already stored is left alone and only the
      // *update* the next sweep attempts is refused.
      yield* Effect.orDie(
        client.unsafe(
          `alter table games add constraint hunt_no_poison2 check (game_id <> '${gid(2)}') not valid`
        )
      )
      makeRun(root, { id: gid(2), report: JSON.stringify({ changed: true }) })
      const second = yield* pruningSweep(root)

      expect(second.deleted).toEqual([])
      expect(yield* storedIds).toEqual([gid(1), gid(2)])
    })))

  it("D5: no chunk of any table can spend more parameters than the ceiling", () => {
    // The regression this closes was silent and cost a whole run: the budget
    // said `player_turns` had 17 columns and the frozen schema says 18, so a
    // 1 923-row chunk asked for 34 614 parameters, PGlite wrote **zero** rows
    // and reported success. Counting off the table makes the two agree; this
    // asserts the arithmetic for every table rather than for the one that broke.
    const budgets = [seats, turns, playerTurns, agentStats, boardState].map((table) => {
      const columns = Ingest.columnCount(table)
      const chunk = Ingest.chunkRows(Array.from({ length: 100_000 }), columns)[0]?.length ?? 0
      return { columns, parameters: chunk * columns }
    })
    expect(budgets.every((budget) => budget.parameters <= Ingest.MAX_BIND_PARAMETERS)).toBe(true)
    expect(Ingest.columnCount(playerTurns)).toBe(18)
    // …and an empty input issues no statement at all, which is what the
    // zero-write proof needs.
    expect(Ingest.chunkRows([], 18)).toEqual([])
  })

  it("D5: 4 000 turns of two seats cross the bind-parameter ceiling and land whole", () =>
    run(Effect.gen(function*() {
      const db = yield* PgDrizzle.PgDrizzle
      const root = newRoot()
      makeRun(root, { id: gid(1), replay: replayOf(4000, 2) })
      // 17 columns per `player_turns` row against PGlite's *signed* int16
      // ceiling: 1 924 rows is one statement's capacity, and crossing it there
      // is silent. A 5 000-turn game is an ordinary run.
      const report = yield* sweep(root)
      expect(report.writes.turns).toBe(4000)
      expect(report.writes.playerTurns).toBe(8000)
      const stored = yield* Effect.orDie(
        db.select({ turn: playerTurns.turn }).from(playerTurns).where(
          eq(playerTurns.gameId, gid(1))
        )
      )
      expect(stored.length).toBe(8000)
    }), ), 180_000)

  it("D5: a chunked run is still a fixpoint on the next sweep", () =>
    run(Effect.gen(function*() {
      const root = newRoot()
      makeRun(root, { id: gid(1), replay: replayOf(4000, 2) })
      yield* sweep(root)
      const second = yield* sweep(root)
      expect(written(second)).toBe(0)
      expect(second.transactionsWithWrites).toBe(0)
    })), 240_000)

  it("D6: 250 boards in one game are inserted in chunks, and resume as a set difference", () =>
    run(Effect.gen(function*() {
      const db = yield* PgDrizzle.PgDrizzle
      const root = newRoot()
      const count = 250
      makeRun(root, {
        id: gid(1),
        replay: replayOf(count, 1),
        saves: Array.from(
          { length: count },
          (_, index) => `turn-${String(index + 1).padStart(4, "0")}-auto.sav.gz`
        )
      })
      const runner: DerivationRunner = (request: DerivationRequest) =>
        Effect.succeed({ turn: request.operation === "board" ? request.turn : 0n })
      const boards = Option.some({
        ...Ingest.boardOptions(join(root, "..", "derive-cache")),
        runner: Option.some(runner)
      })

      const first = yield* sweep(root, { boards })
      expect(first.writes.boards).toBe(count)
      expect(
        (yield* Effect.orDie(db.select().from(boardState).where(eq(boardState.gameId, gid(1)))))
          .length
      ).toBe(count)

      const second = yield* sweep(root, { boards })
      expect(second.writes.boards).toBe(0)
      expect(second.transactionsWithWrites).toBe(0)
    })), 180_000)

  it("D7: the hash encoding is injective over fields that contain its separators", () => {
    // The classic ambiguity: ["ab", "c"] and ["a", "bc"] have the same
    // concatenation and must not have the same encoding. An equal hash is the
    // licence `ingestRun` takes to write *nothing*, so a forgeable listing is a
    // forgeable "unchanged".
    expect(Ingest.encodeFields(["ab", "c"])).not.toBe(Ingest.encodeFields(["a", "bc"]))
    expect(Ingest.encodeFields(["x 1 ok", "y"])).not.toBe(Ingest.encodeFields(["x", "1 ok y"]))
    expect(Ingest.encodeFields(["a\nb"])).not.toBe(Ingest.encodeFields(["a", "b"]))
    expect(Ingest.encodeFields([])).toBe("")
  })

  it("D7: the hash covers the whole replay, not only the tail the gateway reads", () => {
    const rootA = newRoot()
    const rootB = newRoot()
    const head = '{"turn": 1, "players": [{"seat_id": "place-1", "score": 1}]}\n'
    const changed = '{"turn": 1, "players": [{"seat_id": "place-1", "score": 2}]}\n'
    const tail = replayOf(3, 1).split("\n").slice(1).join("\n")
    makeRun(rootA, { id: gid(1), replay: head + tail })
    makeRun(rootB, { id: gid(1), replay: changed + tail })

    const hashOf = (root: string): string =>
      hex(
        Ingest.collectRun(Ingest.resolveRunsRoot(root), gid(1), Ingest.ingestOptions(root))
          .contentHash
      )
    // v2 decodes every line into rows, so a change in the *middle* of a long
    // replay is a change to the stored rows and must move the hash — v1's
    // tail-window digest would not have seen it.
    expect(hashOf(rootA)).not.toBe(hashOf(rootB))
  })

  it("D7: a document's status is part of its hash entry", () => {
    const rootA = newRoot()
    const rootB = newRoot()
    makeRun(rootA, { id: gid(1), report: "{}" })
    makeRun(rootB, { id: gid(1), report: "[]" })
    const hashOf = (root: string): string =>
      hex(
        Ingest.collectRun(Ingest.resolveRunsRoot(root), gid(1), Ingest.ingestOptions(root))
          .contentHash
      )
    expect(hashOf(rootA)).not.toBe(hashOf(rootB))
  })

  it("D8: a sweep's residency is bounded by the largest run, not by the root", () =>
    run(Effect.gen(function*() {
      // `ingest` used to build `walked.gameIds.map(collectRun)` before it wrote
      // anything, so peak residency tracked the size of the *archive*: measured
      // out of test at 477 MB of resident growth for a 480 MiB root.
      // `collectRun` now runs inside each run's own step, so a run's bytes are
      // unreachable the moment the step ends. A dry run isolates the reader —
      // it collects every run exactly as a real sweep does and writes nothing,
      // so the database cannot be what holds the memory — and each replay here
      // is one enormous unparseable line, which produces no rows at all and so
      // measures the *reader* rather than the row builder.
      //
      // The measurement is steady-state on purpose. RSS never shrinks under
      // JSC, so a first sweep is dominated by allocator warm-up (measured: ~900
      // MB for four 4 MiB runs, then ~7 MB for sixteen of them once warm). What
      // this asserts is the shape that matters: a *whole extra sweep* of a
      // 64 MiB archive costs a fraction of the archive, which an archive-sized
      // residency cannot do.
      const root = newRoot()
      const perRun = 4 * 1024 * 1024
      const count = 16
      Array.from({ length: count }, (_, index) =>
        makeRun(root, { id: gid(index), replay: `${"x".repeat(perRun)}\n` }))

      const warm = yield* sweep(root, { dryRun: true })
      expect(warm.runs.length).toBe(count)

      Bun.gc(true)
      const before = process.memoryUsage().rss
      const report = yield* sweep(root, { dryRun: true })
      Bun.gc(true)
      const after = process.memoryUsage().rss

      expect(report.runs.length).toBe(count)
      expect(after - before).toBeLessThan((count * perRun) / 2)
    })), 240_000)

  it("D8: a replay past --max-replay-bytes is reported, not silently cut", () =>
    run(Effect.gen(function*() {
      const db = yield* PgDrizzle.PgDrizzle
      const root = newRoot()
      makeRun(root, { id: gid(1), replay: replayOf(20, 1) })
      const report = yield* sweep(root, { maxReplayBytes: 512 })

      // "Some turns are missing" must not be indistinguishable from "the game
      // had that many turns".
      expect(report.skipped.map((skip) => skip.reason)).toContain("replayTruncated")
      const rows = yield* Effect.orDie(db.select().from(turns).where(eq(turns.gameId, gid(1))))
      expect(rows.length).toBeGreaterThan(0)
      expect(rows.length).toBeLessThan(20)
      // …and the tail answer is still the *whole* file's, because that is what
      // the fs backend reads.
      const stored = yield* Effect.orDie(db.select().from(games).where(eq(games.gameId, gid(1))))
      expect(stored[0]?.lastReplayTurn).toBe(20)
    })))
})

// ==========================================================================
// 5. Volume
// ==========================================================================

describe("a hundred runs in one root", () => {
  const bigRoot = (): string => {
    const root = newRoot()
    Array.from({ length: 100 }, (_, index) =>
      makeRun(root, {
        id: gid(index),
        report: JSON.stringify({
          seat_stats: { "place-1": { turns: index, decisions: index, mean_latency_ms: 1.5 } }
        }),
        replay: replayOf(3, 2)
      }))
    return root
  }

  it("ingests all hundred and writes each table's rows exactly once", () =>
    run(Effect.gen(function*() {
      const root = bigRoot()
      const report = yield* sweep(root)
      expect(report.runs.length).toBe(100)
      expect(report.writes.games).toBe(100)
      expect(report.writes.turns).toBe(300)
      expect(report.writes.playerTurns).toBe(600)
      expect(report.writes.agentStats).toBe(100)
      expect(report.skipped).toEqual([])
    })), 240_000)

  it("writes nothing on the second sweep of a hundred runs", () =>
    run(Effect.gen(function*() {
      const root = bigRoot()
      yield* sweep(root)
      const second = yield* sweep(root)
      expect(written(second)).toBe(0)
      expect(second.transactionsWithWrites).toBe(0)
    })), 240_000)

  it("deletes exactly the fifty runs that left, and keeps the rest untouched", () =>
    run(Effect.gen(function*() {
      const root = bigRoot()
      yield* sweep(root)
      Array.from({ length: 50 }, (_, index) =>
        rmSync(join(root, gid(index)), { recursive: true, force: true }))
      const second = yield* pruningSweep(root)

      expect(second.deleted.length).toBe(50)
      expect((yield* storedIds).length).toBe(50)
      expect(second.writes.games).toBe(0)
      expect(second.writes.turns).toBe(0)
    })), 240_000)

  it("is order-independent: the same hundred runs hash the same in any walk", () => {
    const root = bigRoot()
    const scoped = Ingest.resolveRunsRoot(root)
    const options = Ingest.ingestOptions(root)
    const forward = Array.from({ length: 100 }, (_, index) =>
      hex(Ingest.collectRun(scoped, gid(index), options).contentHash))
    const backward = Array.from({ length: 100 }, (_, index) =>
      hex(Ingest.collectRun(scoped, gid(99 - index), options).contentHash)).toReversed()
    expect(backward).toEqual(forward)
  }, 120_000)
})

// ==========================================================================
// 6. What licenses a deletion
// ==========================================================================

describe("deletion is licensed by the disk, not by a captured seen-set", () => {
  /**
   * The race this closes needs two processes and was reproduced against live
   * Postgres: sweep A over a 40-run root, sweep B ingesting one new run 1.0 s
   * later, A then reporting `40 unchanged, 1 deleted` — both exiting `0`, and
   * B's run gone from every table while sitting on disk. A single-process
   * hermetic test cannot make a run appear between one sweep's `readdir` and its
   * own delete phase, which is exactly why the licence is exported: the
   * predicate the delete phase asks is testable even when the interleaving is
   * not.
   */

  it("D9: a run that is on the disk is never a deletion candidate", () => {
    const root = newRoot()
    plainRun(root, gid(1))
    expect(Ingest.isRunOnDisk(Ingest.resolveRunsRoot(root), gid(1))).toBe(true)
  })

  it("D9: the four things that make an entry not a run all read as absent", () => {
    const root = newRoot()
    const scoped = Ingest.resolveRunsRoot(root)
    plainRun(root, gid(1))

    expect(Ingest.isRunOnDisk(scoped, gid(2))).toBe(false)
    expect(Ingest.isRunOnDisk(scoped, "not-a-game-id")).toBe(false)
    symlinkSync(join(root, gid(1)), join(root, gid(3)))
    expect(Ingest.isRunOnDisk(scoped, gid(3))).toBe(false)
    writeFileSync(join(root, gid(4)), "x")
    expect(Ingest.isRunOnDisk(scoped, gid(4))).toBe(false)
  })

  it("D9: a run whose directory is gone is deleted, once --prune is asked for", () =>
    run(Effect.gen(function*() {
      const root = newRoot()
      plainRun(root, gid(1))
      plainRun(root, gid(2))
      yield* sweep(root)
      rmSync(join(root, gid(2)), { recursive: true, force: true })

      expect((yield* sweep(root)).deleted).toEqual([])
      const pruned = yield* pruningSweep(root)
      expect(pruned.deleted).toEqual([gid(2)])
      expect(yield* storedIds).toEqual([gid(1)])
    })))

  it("D10: an unscoped --prune of a second archive deletes the first archive's rows", () =>
    run(Effect.gen(function*() {
      const archiveA = newRoot()
      const archiveB = newRoot()
      plainRun(archiveA, gid(1))
      plainRun(archiveB, gid(2))
      yield* sweep(archiveA)
      const second = yield* pruningSweep(archiveB)

      // Stated rather than judged. The frozen schema has **no `runs_root`
      // column** — a `games` row is a game, not a directory — so "delete the ids
      // that are not on this disk" cannot be scoped. This is the entire reason
      // pruning is opt-in, and the reason `--game` exists.
      expect(second.deleted).toEqual([gid(1)])
      expect(yield* storedIds).toEqual([gid(2)])
    })))

  it("D10: --game makes a prune safe for a shared database", () =>
    run(Effect.gen(function*() {
      const archiveA = newRoot()
      const archiveB = newRoot()
      plainRun(archiveA, gid(1))
      plainRun(archiveB, gid(2))
      yield* sweep(archiveA)
      yield* sweep(archiveB)
      rmSync(join(archiveB, gid(2)), { recursive: true, force: true })
      const second = yield* pruningSweep(archiveB, { gameIds: new Set([gid(2)]) })

      expect(second.deleted).toEqual([gid(2)])
      expect(yield* storedIds).toEqual([gid(1)])
    })))

  it("D10: a dry run names what a prune would delete, and deletes none of it", () =>
    run(Effect.gen(function*() {
      const root = newRoot()
      plainRun(root, gid(1))
      plainRun(root, gid(2))
      yield* sweep(root)
      rmSync(join(root, gid(2)), { recursive: true, force: true })
      const dry = yield* sweep(root, { prune: true, dryRun: true })

      expect(dry.deleted).toEqual([gid(2)])
      expect(yield* storedIds).toEqual([gid(1), gid(2)])
    })))
})

// ==========================================================================
// 7. The board phase, under the same hostility
// ==========================================================================

describe("boards are domain data, and no run depends on them", () => {
  const failingRunner: DerivationRunner = () =>
    Effect.fail(
      new (class extends Error {
        readonly _tag = "DerivationUnavailable" as const
        readonly operation = "board" as const
        readonly gameId = "unknown"
        readonly detail = "python is not here"
      })() as never
    )

  it("keeps a run's documents when every board fails", () =>
    run(Effect.gen(function*() {
      const db = yield* PgDrizzle.PgDrizzle
      const root = newRoot()
      makeRun(root, {
        id: gid(1),
        replay: replayOf(2, 1),
        saves: ["turn-0001-auto.sav.gz", "turn-0002-auto.sav.gz"]
      })
      const report = yield* sweep(root, {
        boards: Option.some({
          ...Ingest.boardOptions(join(root, "..", "derive-cache")),
          runner: Option.some(failingRunner)
        })
      })

      // A board failure commits in its own transaction, after the documents:
      // it must never roll back a run's `games`/`turns`/`player_turns` rows.
      expect(report.runs[0]?.outcome).toBe("inserted")
      expect(report.writes.boards).toBe(0)
      expect(report.skipped.filter((skip) => skip.reason === "boardUnavailable").length).toBe(2)
      expect((yield* Effect.orDie(db.select().from(turns))).length).toBe(2)
      // Exit code stays 0: a missing board is not an ingest failure.
      expect(report.status).toBe("complete")
    })))

  it("attempts nothing for a game whose saves directory is a symlink", () =>
    run(Effect.gen(function*() {
      const root = newRoot()
      const other = newRoot()
      mkdirSync(join(other, "saves"), { recursive: true })
      writeFileSync(join(other, "saves", "turn-0001-auto.sav.gz"), "savegame")
      const dir = makeRun(root, { id: gid(1), replay: replayOf(1, 1) })
      symlinkSync(join(other, "saves"), join(dir, "saves"))

      const seen: Array<string> = []
      const runner: DerivationRunner = (request: DerivationRequest) => {
        seen.push(request.operation)
        return Effect.succeed({})
      }
      const report = yield* sweep(root, {
        boards: Option.some({
          ...Ingest.boardOptions(join(root, "..", "derive-cache")),
          runner: Option.some(runner)
        })
      })

      // `_safe_archive_directory` refuses a symlinked `saves/`, so the archive
      // cannot be made to derive from a directory outside the run.
      expect(seen).toEqual([])
      expect(report.writes.boards).toBe(0)
    })))

  it("ignores a save whose turn cannot be an integer column", () =>
    run(Effect.gen(function*() {
      const root = newRoot()
      makeRun(root, {
        id: gid(1),
        replay: replayOf(1, 1),
        saves: ["turn-0001-auto.sav.gz", "turn-99999999999999999999-auto.sav.gz"]
      })
      const seen: Array<string> = []
      const runner: DerivationRunner = (request: DerivationRequest) => {
        seen.push(request.operation === "board" ? String(request.turn) : request.operation)
        return Effect.succeed({})
      }
      const report = yield* sweep(root, {
        boards: Option.some({
          ...Ingest.boardOptions(join(root, "..", "derive-cache")),
          runner: Option.some(runner)
        })
      })

      // `BigInt` on the `\d+` capture is exact where `Number` is not:
      // `turn-9007199254740993-auto.sav` must not become …992 and collide.
      expect(seen).toEqual(["1"])
      expect(report.writes.boards).toBe(1)
    })))
})
