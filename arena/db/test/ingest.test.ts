/**
 * The v2 ingest pipeline, against the parity fixture corpus, on PGlite.
 *
 * v1 mirrored bytes and this file asserted that the bytes came back. v2 stores a
 * *model of a game*, so every assertion below is about a **column**, and the
 * corpus is still `arena/harness/test/parity/fixtures/runs` — the same eight
 * scenario classes the CPython↔TypeScript differential runs on, which is what
 * makes these statements about parity rather than about a database:
 *
 * | fixture | what it pins here |
 * |---|---|
 * | `terminal_valid_01` | the full archive: manifest + report + replay + saves |
 * | `terminal_nowin_02` | a terminal run with no `victory.json` (and none stored) |
 * | `interrupted_03` | a live run: manifest and replay, no report |
 * | `lobby_husk_04` | a zero-byte `replay.jsonl` — `last_replay_turn` is `NULL` |
 * | `malformed_05` | a truncated manifest: `manifest_status = 'unusable'`, no extras |
 * | `wrong_id_06` | a manifest whose `game_id` lies — ingested, gated on read |
 * | `symlink_07` | a symlinked run directory — no row at all, with a reason |
 * | `torn_tail_08` | a half-written final line — dropped, and the tail answer is 3 |
 *
 * Five properties get their own block because each is a *contract* rather than a
 * behaviour:
 *
 * 1. **The partition rule.** For a document with `status = 'ok'` every manifest
 *    key is stored exactly once — in its typed column or in `extras.manifest`,
 *    never both, never neither. That is what makes reconstruction total, and it
 *    is asserted key by key over every fixture rather than field by field.
 * 2. **Demotion.** A key takes its column only when the column holds the value
 *    losslessly *and* the value's type is the column's type. An explicit `null`
 *    is therefore always a demotion, because a column cannot tell it from an
 *    absent key and `untrustedFieldOr(manifest, 'state', 'status')` can.
 * 3. **The lifted config columns are write-only.** `name`/`ruleset`/`mode`/… are
 *    query projections of `config`; nothing reconstructs from them.
 * 4. **Idempotence.** A re-ingest of an unchanged root writes nothing, and the
 *    proof is `pg_current_xact_id_if_assigned()` rather than a row counter.
 * 5. **Boards are off the parity path.** They are skipped by default, keyed by
 *    set difference, and deliberately outside `content_hash`.
 */

import type { JsonObject, JsonValue } from "@arena/wire"
import { BunContext } from "@effect/platform-bun"
import * as PgDrizzle from "@effect/sql-drizzle/Pg"
import { eq } from "drizzle-orm"
import * as Effect from "effect/Effect"
import * as Either from "effect/Either"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Redacted from "effect/Redacted"
import { describe, expect, it } from "bun:test"
// oxlint-disable-next-line effecttsgo/node-builtin-import
import {
  cpSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  statSync,
  writeFileSync
  // oxlint-disable-next-line effecttsgo/node-builtin-import
} from "node:fs"
// oxlint-disable-next-line effecttsgo/node-builtin-import
import { tmpdir } from "node:os"
// oxlint-disable-next-line effecttsgo/node-builtin-import
import { join } from "node:path"
// oxlint-disable-next-line effecttsgo/node-builtin-import
import { fileURLToPath } from "node:url"

import type { DerivationRequest, DerivationRunner } from "../../harness/src/gateway/services/derivation.ts"
import * as Cli from "../src/ingest-cli.ts"
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

/** One fresh, migrated, in-process database per test. */
const run = <A, E>(effect: Effect.Effect<A, E, TestContext>): Promise<A> =>
  // oxlint-disable-next-line effecttsgo/strict-effect-provide
  Effect.runPromise(
    Effect.scoped(Effect.provide(Effect.flatMap(Migrate.run, () => effect), testLayer))
  )

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

/** The six fixtures whose manifest parses — the ones with typed columns. */
const OK_MANIFEST_IDS: ReadonlyArray<string> = [
  TERMINAL,
  NOWIN,
  INTERRUPTED,
  LOBBY,
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

/** Ingest a root with the default options, plus any overrides. */
const sweep = (
  root: string,
  overrides: Partial<Ingest.IngestOptions> = {}
): Effect.Effect<Ingest.IngestReport, Ingest.IngestError, TestContext> =>
  Ingest.ingest({ ...Ingest.ingestOptions(root), ...overrides })

/** The raw document on disk, as the fs backend parses it. */
const documentOf = (root: string, gameId: string, file: string): JsonObject =>
  JSON.parse(readFileSync(join(root, gameId, file), "utf8")) as JsonObject

const manifestOf = (gameId: string): JsonObject => documentOf(FIXTURES, gameId, "manifest.json")

const byteSizeOf = (root: string, gameId: string, file: string): number =>
  statSync(join(root, gameId, file)).size

/** A document field that really is a number — the shape a `float8` column takes. */
const numberField = (document: JsonObject, key: string): number | null => {
  const value = document[key]
  return typeof value === "number" ? value : null
}

/** A `record` narrowing that never casts blind — `{}` for anything else. */
const asObject = (value: unknown): { readonly [key: string]: unknown } =>
  typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as { readonly [key: string]: unknown })
    : {}

const extrasOf = (row: { readonly extras: unknown }): { readonly [key: string]: unknown } =>
  asObject(row.extras)

const extrasManifestOf = (
  row: { readonly extras: unknown }
): { readonly [key: string]: unknown } => asObject(extrasOf(row)["manifest"])

type GameRow = typeof games.$inferSelect

const gameRows = (
  db: PgDrizzle.PgDrizzle["Type"]
): Effect.Effect<ReadonlyArray<GameRow>, never, never> =>
  Effect.orDie(db.select().from(games))

const gameRow = (
  db: PgDrizzle.PgDrizzle["Type"],
  gameId: string
): Effect.Effect<GameRow, never, never> =>
  Effect.map(
    Effect.orDie(db.select().from(games).where(eq(games.gameId, gameId))),
    (rows) => {
      const row = rows[0]
      // A missing row is a test failure with a readable shape rather than a
      // `throw` with a stack: the expectations below compare fields.
      expect(rows.length).toBe(1)
      return row as GameRow
    }
  )

/** One run's result out of a report, without throwing when it is missing. */
const resultOf = (
  report: Ingest.IngestReport,
  gameId: string
): Ingest.RunResult | undefined => report.runs.find((entry) => entry.gameId === gameId)

const writes = (report: Ingest.IngestReport): number => Ingest.dataWrites(report.writes)

/** The manifest keys that have a typed column. `game_id` is deliberately not one. */
const COLUMN_KEYS: ReadonlyArray<string> = [
  "state",
  "schema_version",
  "created_at",
  "started_at",
  "finished_at",
  "current_turn",
  "benchmark_valid",
  "config"
]

/** The column a manifest key lands in, read off a stored row. */
const columnValue = (row: GameRow, key: string): unknown =>
  key === "state"
    ? row.state
    : key === "schema_version"
    ? row.schemaVersion
    : key === "created_at"
    ? row.createdAt
    : key === "started_at"
    ? row.startedAt
    : key === "finished_at"
    ? row.finishedAt
    : key === "current_turn"
    ? row.currentTurn
    : key === "benchmark_valid"
    ? row.benchmarkValid
    : key === "config"
    ? row.config
    : undefined

/**
 * The partition predicate of §1.1, as a list of violations.
 *
 * For every key of the document: it is either held by its column, or present in
 * `extras.manifest` — never both, never neither. `state` is the one column that
 * is `NOT NULL`, so "held" there means the raw value is one of the seven
 * spellings rather than the `'invalid'` sentinel.
 */
const partitionViolations = (
  manifest: JsonObject,
  row: GameRow
): ReadonlyArray<string> => {
  const extras = extrasManifestOf(row)
  return Object.keys(manifest).flatMap((key) => {
    const demoted = key in extras
    const held = COLUMN_KEYS.includes(key) &&
      (key === "state"
        ? Ingest.isStorableRunState(manifest[key])
        : columnValue(row, key) !== null)
    return demoted === held ? [`${key}: ${demoted ? "in both" : "in neither"}`] : []
  })
}

// --------------------------------------------------------- the eight cases ---

describe("the walk", () => {
  it("ingests seven runs and skips the symlinked directory with a reason", () =>
    run(Effect.gen(function*() {
      const report = yield* sweep(FIXTURES)

      expect(report.runs.map((entry) => entry.gameId).toSorted()).toEqual(
        [...INGESTED_IDS].toSorted()
      )
      expect(report.runs.every((entry) => entry.outcome === "inserted")).toBe(true)
      expect(report.seen).toBe(8)
      expect(report.deleted).toEqual([])
      expect(report.status).toBe("complete")

      // The symlink is the whole point of `game_parity_symlink_07`: the fs
      // backend 404s it at read time, and a row would have made it answerable.
      expect(report.skipped.map((skip) => ({ entry: skip.entry, reason: skip.reason }))).toEqual([
        { entry: SYMLINK, reason: "symlink" }
      ])
      expect(report.runsRoot).toBe(Ingest.resolveRunsRoot(FIXTURES))
    })))

  it("writes exactly the rows the corpus implies, and no others", () =>
    run(Effect.gen(function*() {
      const report = yield* sweep(FIXTURES)
      expect(report.writes).toEqual({
        games: 7,
        seats: 12,
        // 3 + 3 + 4 + 3 recorded turns; the two husks and the report-only run
        // contribute none.
        turns: 13,
        playerTurns: 26,
        agentStats: 4,
        boards: 0,
        deletes: 0
      })
    })))

  it("restricts a --game sweep to the ids it was given, and names the absent ones", () =>
    run(Effect.gen(function*() {
      const report = yield* sweep(FIXTURES, {
        gameIds: new Set([TERMINAL, "game_parity_not_here_99"])
      })
      expect(report.runs.map((entry) => entry.gameId)).toEqual([TERMINAL])
      expect(report.skipped.map((skip) => skip.reason)).toEqual(["requestedButAbsent"])
    })))
})

describe("the two document gates", () => {
  it("stores the status the fs backend would answer with, per document", () =>
    run(Effect.gen(function*() {
      const db = yield* PgDrizzle.PgDrizzle
      yield* sweep(FIXTURES)
      const statuses = (gameId: string) =>
        Effect.map(
          gameRow(db, gameId),
          (row) => `${row.manifestStatus}/${row.reportStatus}`
        )

      // Terminal runs wrote a report; live ones never did.
      expect(yield* statuses(TERMINAL)).toBe("ok/ok")
      expect(yield* statuses(NOWIN)).toBe("ok/ok")
      expect(yield* statuses(WRONG_ID)).toBe("ok/ok")
      expect(yield* statuses(INTERRUPTED)).toBe("ok/absent")
      expect(yield* statuses(LOBBY)).toBe("ok/absent")
      expect(yield* statuses(TORN_TAIL)).toBe("ok/absent")
      // A truncated manifest is the 503 the fs backend answers, recorded once at
      // ingest because there are no stored bytes to re-gate.
      expect(yield* statuses(MALFORMED)).toBe("unusable/absent")
    })))

  it("records the fstat size that the gate saw, and zero for an absent document", () =>
    run(Effect.gen(function*() {
      const db = yield* PgDrizzle.PgDrizzle
      yield* sweep(FIXTURES)

      const terminal = yield* gameRow(db, TERMINAL)
      expect(terminal.manifestByteSize).toBe(byteSizeOf(FIXTURES, TERMINAL, "manifest.json"))
      expect(terminal.reportByteSize).toBe(byteSizeOf(FIXTURES, TERMINAL, "report.json"))

      // `unusable` still carries its size: it is the number that failed the gate.
      const malformed = yield* gameRow(db, MALFORMED)
      expect(malformed.manifestByteSize).toBe(byteSizeOf(FIXTURES, MALFORMED, "manifest.json"))
      expect(malformed.reportByteSize).toBe(0)
    })))

  it("gives an unusable manifest the 'invalid' sentinel and no extras at all", () =>
    run(Effect.gen(function*() {
      const db = yield* PgDrizzle.PgDrizzle
      yield* sweep(FIXTURES)
      const row = yield* gameRow(db, MALFORMED)

      expect(row.state).toBe(Ingest.UNRECOGNIZED_STATE)
      expect(row.extras).toBe(null)
      // Every typed column is `NULL`: there is no document to fill them from.
      expect([
        row.schemaVersion,
        row.createdAt,
        row.startedAt,
        row.finishedAt,
        row.currentTurn,
        row.benchmarkValid,
        row.config,
        row.name,
        row.mode
      ]).toEqual([null, null, null, null, null, null, null, null, null])
    })))
})

describe("the games row", () => {
  it("lifts the lifecycle columns of a terminal run exactly", () =>
    run(Effect.gen(function*() {
      const db = yield* PgDrizzle.PgDrizzle
      yield* sweep(FIXTURES)
      const row = yield* gameRow(db, TERMINAL)
      const manifest = manifestOf(TERMINAL)

      expect(row.state).toBe("completed")
      expect(row.schemaVersion).toBe(1)
      // A `float8` round trip has to be exact to the last digit: this value is
      // the manifest's own spelling and `/status` serves it verbatim.
      expect(row.createdAt).toBe(numberField(manifest, "created_at"))
      expect(row.createdAt).toBe(1785660947.0197709)
      expect(row.startedAt).toBe(numberField(manifest, "started_at"))
      expect(row.finishedAt).toBe(numberField(manifest, "finished_at"))
      expect(row.currentTurn).toBe(752)
      expect(row.benchmarkValid).toBe(true)
    })))

  it("keeps benchmark_valid tri-state across the corpus", () =>
    run(Effect.gen(function*() {
      const db = yield* PgDrizzle.PgDrizzle
      yield* sweep(FIXTURES)
      const flags = yield* Effect.forEach(
        [TERMINAL, WRONG_ID, LOBBY],
        (gameId) => Effect.map(gameRow(db, gameId), (row) => row.benchmarkValid),
        { concurrency: 1 }
      )
      // `true`, `false` and a *present* `null` are three different answers on
      // `/status`, and the column keeps all three apart.
      expect(flags).toEqual([true, false, null])
    })))

  it("stores every state the corpus spells, and never invents one", () =>
    run(Effect.gen(function*() {
      const db = yield* PgDrizzle.PgDrizzle
      yield* sweep(FIXTURES)
      const rows = yield* gameRows(db)
      expect(
        rows.map((row) => `${row.gameId}=${row.state}`).toSorted()
      ).toEqual([
        `${INTERRUPTED}=running`,
        `${LOBBY}=lobby`,
        `${MALFORMED}=invalid`,
        `${NOWIN}=cancelled`,
        `${TERMINAL}=completed`,
        `${TORN_TAIL}=running`,
        `${WRONG_ID}=failed`
      ])
    })))

  it("answers last_replay_turn the way the fs tail read answers it", () =>
    run(Effect.gen(function*() {
      const db = yield* PgDrizzle.PgDrizzle
      yield* sweep(FIXTURES)
      const turnOf = (gameId: string) =>
        Effect.map(gameRow(db, gameId), (row) => row.lastReplayTurn)

      // The last *parseable* line of the tail, not `max(turns.turn)`.
      expect(yield* turnOf(TERMINAL)).toBe(3)
      expect(yield* turnOf(INTERRUPTED)).toBe(4)
      // The torn final line does not parse, so the answer is the line before it
      // — and the torn line contributes no `turns` row either.
      expect(yield* turnOf(TORN_TAIL)).toBe(3)
      // A zero-byte replay is `Option.none`, which is the lobby-husk fix: the
      // index drops such a row rather than relabelling it interrupted.
      expect(yield* turnOf(LOBBY)).toBe(null)
      expect(yield* turnOf(MALFORMED)).toBe(null)
      // No `replay.jsonl` at all.
      expect(yield* turnOf(WRONG_ID)).toBe(null)
    })))

  it("stores the manifest's game_id claim without pre-empting the read-side check", () =>
    run(Effect.gen(function*() {
      const db = yield* PgDrizzle.PgDrizzle
      yield* sweep(FIXTURES)
      const row = yield* gameRow(db, WRONG_ID)

      // The row's key is the *directory*; the claim is a demoted key, because
      // `readManifest` re-runs the cross-check over the reconstructed document
      // and answers 404 exactly as the fs backend does.
      expect(row.gameId).toBe(WRONG_ID)
      expect(extrasManifestOf(row)["game_id"]).toBe("game_parity_wrong_id_06_other")
    })))
})

describe("the partition rule", () => {
  it("stores every manifest key exactly once, over every fixture", () =>
    run(Effect.gen(function*() {
      const db = yield* PgDrizzle.PgDrizzle
      yield* sweep(FIXTURES)
      const violations = yield* Effect.forEach(
        OK_MANIFEST_IDS,
        (gameId) =>
          Effect.map(gameRow(db, gameId), (row) => ({
            gameId,
            problems: partitionViolations(manifestOf(gameId), row)
          })),
        { concurrency: 1 }
      )
      expect(violations.filter((entry) => entry.problems.length > 0)).toEqual([])
    })))

  it("keeps every demoted value verbatim", () =>
    run(Effect.gen(function*() {
      const db = yield* PgDrizzle.PgDrizzle
      yield* sweep(FIXTURES)
      yield* Effect.forEach(
        OK_MANIFEST_IDS,
        (gameId) =>
          Effect.map(gameRow(db, gameId), (row) => {
            const manifest = manifestOf(gameId)
            const extras = extrasManifestOf(row)
            Object.keys(extras).forEach((key) => {
              expect(extras[key]).toEqual(manifest[key])
            })
          }),
        { concurrency: 1 }
      )
    })))

  it("never columnizes game_id, and always demotes it", () =>
    run(Effect.gen(function*() {
      const db = yield* PgDrizzle.PgDrizzle
      yield* sweep(FIXTURES)
      const claims = yield* Effect.forEach(
        OK_MANIFEST_IDS,
        (gameId) => Effect.map(gameRow(db, gameId), (row) => extrasManifestOf(row)["game_id"]),
        { concurrency: 1 }
      )
      expect(claims).toEqual(OK_MANIFEST_IDS.map((gameId) => manifestOf(gameId)["game_id"]))
    })))

  it("demotes an explicit null rather than storing it as an absent column", () => {
    // The one behaviourally sensitive site in the port:
    // `untrustedFieldOr(manifest, 'state', 'status')` — a *present* `null`
    // `state` beats `status`, an absent one does not.
    const fields = Ingest.gameFields({ state: null, status: "running" })
    expect(fields.state).toBe(Ingest.UNRECOGNIZED_STATE)
    expect(fields.extrasManifest).toEqual({ state: null, status: "running" })

    // `config: null` is the same rule wearing a jsonb hat: the column *could*
    // hold a JSON null, and then nothing could tell it from an absent key.
    const withNullConfig = Ingest.gameFields({ config: null })
    expect(withNullConfig.config).toBe(null)
    expect(withNullConfig.extrasManifest).toEqual({ config: null })

    // …while a `config` that is merely not an object keeps its column.
    const withStringConfig = Ingest.gameFields({ config: "nope" })
    expect(withStringConfig.config).toBe("nope")
    expect(withStringConfig.extrasManifest).toEqual({})
  })

  it("demotes every value its column cannot hold, one trigger at a time", () => {
    const demoted = (manifest: JsonObject): ReadonlyArray<string> =>
      Object.keys(Ingest.gameFields(manifest).extrasManifest)

    expect(demoted({ state: 17 })).toEqual(["state"])
    expect(demoted({ state: "x".repeat(10_000) })).toEqual(["state"])
    expect(demoted({ benchmark_valid: "true" })).toEqual(["benchmark_valid"])
    expect(demoted({ created_at: "yesterday" })).toEqual(["created_at"])
    expect(demoted({ current_turn: 5.5 })).toEqual(["current_turn"])
    expect(demoted(JSON.parse('{"current_turn":99999999999999999999}') as JsonObject))
      .toEqual(["current_turn"])
    expect(demoted({ schema_version: 2147483648 })).toEqual(["schema_version"])
    // …and a value the column *can* hold is not demoted, including the falsy
    // ones a truthiness test would have dropped.
    expect(demoted({ current_turn: 0, benchmark_valid: false, created_at: 0 })).toEqual([])
  })
})

describe("the lifted config columns", () => {
  it("projects config for querying, and takes max_turns from config.turns", () =>
    run(Effect.gen(function*() {
      const db = yield* PgDrizzle.PgDrizzle
      yield* sweep(FIXTURES)
      const row = yield* gameRow(db, TERMINAL)

      expect(row.name).toBe("session-game_parity_t")
      expect(row.ruleset).toBe("classic")
      expect(row.mode).toBe("multiplayer")
      expect(row.timingMode).toBe("infinite")
      expect(row.objective).toBe("Maximize final Freeciv civilization score.")
      // The corpus spells the turn limit `turns`; `max_turns` is the newer name
      // and takes precedence when both are present.
      expect(row.maxTurns).toBe(5000)
      expect(Ingest.gameFields({ config: { max_turns: 7, turns: 9 } }).maxTurns).toBe(7)
    })))

  it("stores the whole config document in the jsonb column, verbatim", () =>
    run(Effect.gen(function*() {
      const db = yield* PgDrizzle.PgDrizzle
      yield* sweep(FIXTURES)
      const row = yield* gameRow(db, TERMINAL)
      // Key order is not part of the predicate — `jsonb` reorders keys and
      // `canonicalBytes` sorts them again on the way out — but every value is.
      expect(row.config).toEqual(manifestOf(TERMINAL)["config"] as JsonValue)
    })))

  it("leaves a lifted column NULL when the config value is not its type", () => {
    const fields = Ingest.gameFields({
      config: { name: 17, ruleset: null, mode: [], max_turns: "many" }
    })
    expect([fields.name, fields.ruleset, fields.mode, fields.maxTurns]).toEqual([
      null,
      null,
      null,
      null
    ])
    // …and the config itself is untouched, because the columns are a projection
    // of it rather than a partition of it.
    expect(fields.config).toEqual({ name: 17, ruleset: null, mode: [], max_turns: "many" })
  })
})

describe("seats", () => {
  it("stores the configured seat list, in configured order", () =>
    run(Effect.gen(function*() {
      const db = yield* PgDrizzle.PgDrizzle
      yield* sweep(FIXTURES)
      const rows = yield* Effect.orDie(
        db.select().from(seats).where(eq(seats.gameId, TERMINAL))
      )
      expect(rows.map((row) => `${row.seatIndex}:${row.seatId ?? "-"}`).toSorted()).toEqual([
        "0:place-1",
        "1:place-2"
      ])
    })))

  it("maps native_classic_ai and a native seat type to 'cpu', everything else to 'agent'", () =>
    run(Effect.gen(function*() {
      const db = yield* PgDrizzle.PgDrizzle
      yield* sweep(FIXTURES)
      const rows = yield* Effect.orDie(db.select().from(seats))
      const kinds = new Map(rows.map((row) => [`${row.gameId}/${row.seatId ?? "-"}`, row.kind]))

      // Two external agents.
      expect(kinds.get(`${TERMINAL}/place-1`)).toBe("agent")
      expect(kinds.get(`${TERMINAL}/place-2`)).toBe("agent")
      // `config.seats[1].type === 'native'` *and* a resolved place controlled by
      // `native_classic_ai` — either one alone is enough.
      expect(kinds.get(`${LOBBY}/place-2`)).toBe("cpu")
      expect(kinds.get(`${WRONG_ID}/place-2`)).toBe("cpu")
      expect(kinds.get(`${TORN_TAIL}/place-2`)).toBe("cpu")
    })))

  it("takes 'cpu' from the resolved place alone, with no native seat type", () => {
    const rows = Ingest.seatRows({
      config: { seats: [{ id: "place-1", type: "external" }] },
      resolved_places: [{ seat_id: "place-1", controller: "native_classic_ai" }]
    })
    expect(rows.map((row) => row.kind)).toEqual(["cpu"])
  })

  it("stores no seat kind at all when the seat list is not a list of objects", () => {
    expect(Ingest.seatRows({ config: { seats: "nope" } })).toEqual([])
    expect(Ingest.seatRows({ config: { seats: [42] } }).map((row) => row.kind)).toEqual([null])
  })

  it("keeps controller_label raw, and never bakes in the serving default", () =>
    run(Effect.gen(function*() {
      const db = yield* PgDrizzle.PgDrizzle
      yield* sweep(FIXTURES)
      const rows = yield* Effect.orDie(db.select().from(seats).where(eq(seats.gameId, LOBBY)))
      const labels = new Map(rows.map((row) => [row.seatId ?? "-", row.label]))

      expect(labels.get("place-1")).toBe("kimi-k3")
      // `publicPlaces` applies `orDefault(rawLabel, 'Freeciv Classic AI')` at
      // serve time; storing it here would apply it twice.
      expect(labels.get("place-2")).toBe(null)
      // A blank label is the same absence.
      expect(Ingest.seatRows({
        config: { seats: [{ id: "place-1", controller_label: "   " }] }
      })[0]?.label).toBe(null)
    })))

  it("stores controller_metadata as the seat's metadata", () =>
    run(Effect.gen(function*() {
      const db = yield* PgDrizzle.PgDrizzle
      yield* sweep(FIXTURES)
      const rows = yield* Effect.orDie(db.select().from(seats).where(eq(seats.gameId, TERMINAL)))
      expect(rows.map((row) => row.metadata)).toEqual([{}, {}])
    })))

  it("fingerprints the frozen field list — which the corpus proves is coarser than a controller", () =>
    run(Effect.gen(function*() {
      const db = yield* PgDrizzle.PgDrizzle
      yield* sweep(FIXTURES)
      const rows = yield* Effect.orDie(db.select().from(seats))
      const prints = new Map(rows.map((row) => [`${row.gameId}/${row.seatId ?? "-"}`, row.fingerprint]))

      // Stable across games, which is the point: it is the join key for
      // cross-game statistics.
      expect(prints.get(`${TERMINAL}/place-1`)).toBe(prints.get(`${INTERRUPTED}/place-1`) ?? null)
      // …and, stated rather than discovered: the frozen docstring's field list
      // is `(id, type, model, instructions, base_url, options)`, which excludes
      // `controller_label`. In this corpus every external seat has a `null`
      // model, so `pi-gpt-5.6-sol` and `GPT-5.5` — different controllers, with
      // different `controller_fingerprint`s of the supervisor's own — share one
      // fingerprint here. Reported upstream; pinned so a change is deliberate.
      expect(prints.get(`${TERMINAL}/place-1`)).toBe(prints.get(`${WRONG_ID}/place-1`) ?? null)
      expect(manifestOf(TERMINAL)).not.toEqual(manifestOf(WRONG_ID))
    })))

  it("moves the fingerprint when an identity field moves, and not when key order does", () => {
    const print = (seat: JsonObject): string | null =>
      Ingest.seatRows({ config: { seats: [seat] } })[0]?.fingerprint ?? null

    const base = { id: "place-1", type: "external", model: "m", options: { a: 1, b: 2 } }
    expect(print(base)).toBe(print({ options: { b: 2, a: 1 }, model: "m", type: "external", id: "place-1" }))
    expect(print(base)).not.toBe(print({ ...base, model: "other" }))
    // A label is not identity, by the frozen docstring's own field list.
    expect(print(base)).toBe(print({ ...base, controller_label: "anything" }))
  })
})

describe("turns and player_turns", () => {
  it("stores one turns row per recorded replay line, with its year", () =>
    run(Effect.gen(function*() {
      const db = yield* PgDrizzle.PgDrizzle
      yield* sweep(FIXTURES)
      const rows = yield* Effect.orDie(
        db.select().from(turns).where(eq(turns.gameId, INTERRUPTED))
      )
      expect(rows.map((row) => `${row.turn}:${String(row.year)}`).toSorted()).toEqual([
        "1:-4000",
        "2:-3950",
        "3:-3900",
        "4:-3850"
      ])
    })))

  it("drops a torn final line, and stores every line before it", () =>
    run(Effect.gen(function*() {
      const db = yield* PgDrizzle.PgDrizzle
      yield* sweep(FIXTURES)
      const rows = yield* Effect.orDie(
        db.select().from(turns).where(eq(turns.gameId, TORN_TAIL))
      )
      // The fourth line is a half-written write; it parses as nothing and must
      // not become a turn row, a player row, or the tail answer.
      expect(rows.map((row) => row.turn).toSorted((left, right) => left - right)).toEqual([1, 2, 3])
    })))

  it("stores no rows for a run whose replay is empty or absent", () =>
    run(Effect.gen(function*() {
      const db = yield* PgDrizzle.PgDrizzle
      yield* sweep(FIXTURES)
      const rows = yield* Effect.orDie(db.select().from(turns))
      expect(rows.filter((row) => row.gameId === LOBBY)).toEqual([])
      expect(rows.filter((row) => row.gameId === WRONG_ID)).toEqual([])
      expect(rows.filter((row) => row.gameId === MALFORMED)).toEqual([])
    })))

  it("decodes a player row into its typed columns", () =>
    run(Effect.gen(function*() {
      const db = yield* PgDrizzle.PgDrizzle
      yield* sweep(FIXTURES)
      const rows = yield* Effect.orDie(
        db.select().from(playerTurns).where(eq(playerTurns.gameId, TERMINAL))
      )
      const first = rows.find((row) => row.turn === 1 && row.seatId === "place-1")

      expect(first?.playerId).toBe(0)
      expect(first?.playerName).toBe("AgentPlace1")
      expect(first?.nation).toBe("Portuguese")
      expect(first?.government).toBe("Despotism")
      expect(first?.alive).toBe(true)
      expect(first?.score).toBe(0)
      expect(first?.units).toBe(5)
      expect(first?.gold).toBe(50)
      expect(first?.knownTechIds).toEqual([])
      expect(first?.research).toEqual({ tech_id: 63, name: "Pottery", bulbs: 0, cost: 28 })
    })))

  it("keys player rows by (turn, seat_id) and lets the last line win", () =>
    run(Effect.gen(function*() {
      const db = yield* PgDrizzle.PgDrizzle
      const root = copyFixtures()
      writeFileSync(
        join(root, TERMINAL, "replay.jsonl"),
        '{"turn": 1, "players": [{"seat_id": "place-1", "score": 1}]}\n' +
          '{"turn": 1, "players": [{"seat_id": "place-1", "score": 2}]}\n'
      )
      yield* sweep(root)
      const rows = yield* Effect.orDie(
        db.select().from(playerTurns).where(eq(playerTurns.gameId, TERMINAL))
      )
      expect(rows.length).toBe(1)
      expect(rows[0]?.score).toBe(2)
      rmSync(join(root, ".."), { recursive: true, force: true })
    })))

  it("drops a player row with no storable seat_id, and says so", () =>
    run(Effect.gen(function*() {
      const root = copyFixtures()
      writeFileSync(
        join(root, TERMINAL, "replay.jsonl"),
        '{"turn": 1, "players": [{"score": 1}, {"seat_id": "place-2", "score": 2}]}\n'
      )
      const report = yield* sweep(root)
      expect(resultOf(report, TERMINAL)?.writes.playerTurns).toBe(1)
      expect(report.skipped.map((skip) => skip.reason)).toContain("rowUnstorable")
      rmSync(join(root, ".."), { recursive: true, force: true })
    })))
})

describe("agent_stats and the report envelope", () => {
  it("projects report.seat_stats into rows", () =>
    run(Effect.gen(function*() {
      const db = yield* PgDrizzle.PgDrizzle
      yield* sweep(FIXTURES)
      const rows = yield* Effect.orDie(
        db.select().from(agentStats).where(eq(agentStats.gameId, TERMINAL))
      )
      const first = rows.find((row) => row.seatId === "place-1")
      expect(rows.length).toBe(2)
      expect(first?.turns).toBe(752)
      expect(first?.decisions).toBe(752)
      expect(first?.fallbacks).toBe(0)
      expect(first?.inputTokens).toBe(0)
      expect(first?.outputTokens).toBe(0)
      expect(first?.meanLatencyMs).toBe(17531.375)
    })))

  it("keeps the whole report document in extras.report, verbatim", () =>
    run(Effect.gen(function*() {
      const db = yield* PgDrizzle.PgDrizzle
      yield* sweep(FIXTURES)
      const row = yield* gameRow(db, NOWIN)
      // This is the entire report reconstruction: `agent_stats` is a projection
      // and `seat_stats` is not rebuilt from it — `mean_latency_ms`'s
      // int-vs-float spelling and its `latency_ms` sibling (both present in this
      // very fixture, which has a zero-decision seat) have no column at all.
      expect(extrasOf(row)["report"]).toEqual(documentOf(FIXTURES, NOWIN, "report.json"))
      const stats = asObject(asObject(extrasOf(row)["report"])["seat_stats"])
      expect(Object.keys(asObject(stats["place-2"]))).toContain("latency_ms")
    })))

  it("writes no report envelope when the report is absent", () =>
    run(Effect.gen(function*() {
      const db = yield* PgDrizzle.PgDrizzle
      yield* sweep(FIXTURES)
      const row = yield* gameRow(db, INTERRUPTED)
      expect("report" in extrasOf(row)).toBe(false)
      expect(Object.keys(extrasOf(row))).toEqual(["manifest"])
    })))

  it("stores no agent_stats for a seat_stats that is not a map of objects", () => {
    expect(Ingest.agentStatRows({ seat_stats: [1, 2] })).toEqual([])
    expect(Ingest.agentStatRows({ seat_stats: { "place-1": 7 } })).toEqual([])
    expect(Ingest.agentStatRows({}).length).toBe(0)
  })
})

describe("idempotence", () => {
  it("writes nothing at all on a second sweep of an unchanged root", () =>
    run(Effect.gen(function*() {
      yield* sweep(FIXTURES)
      const second = yield* sweep(FIXTURES)

      expect(writes(second)).toBe(0)
      // The proof, rather than the symptom: a transaction that never assigned an
      // id provably never wrote a tuple.
      expect(second.transactionsWithWrites).toBe(0)
      expect(second.runs.every((entry) => entry.outcome === "unchanged")).toBe(true)
    })))

  it("keeps ingested_at still for a run that did not change", () =>
    run(Effect.gen(function*() {
      const db = yield* PgDrizzle.PgDrizzle
      const root = copyFixtures()
      yield* sweep(root)
      const before = (yield* gameRow(db, TERMINAL)).ingestedAt
      yield* sweep(root)
      expect((yield* gameRow(db, TERMINAL)).ingestedAt).toEqual(before)
      rmSync(join(root, ".."), { recursive: true, force: true })
    })))

  it("moves exactly the changed run's rows when one manifest is rewritten", () =>
    run(Effect.gen(function*() {
      const root = copyFixtures()
      yield* sweep(root)
      const manifest = manifestOf(TERMINAL)
      writeFileSync(
        join(root, TERMINAL, "manifest.json"),
        JSON.stringify({ ...manifest, current_turn: 999 })
      )
      const second = yield* sweep(root)

      expect(second.runs.filter((entry) => entry.outcome === "updated").map((e) => e.gameId))
        .toEqual([TERMINAL])
      expect(second.writes.games).toBe(1)
      // Seats are rewritten only if they moved; the turns did not move at all.
      expect(second.writes.turns).toBe(0)
      expect(second.writes.playerTurns).toBe(0)
      expect(second.writes.deletes).toBe(0)
      expect(second.transactionsWithWrites).toBe(1)
      rmSync(join(root, ".."), { recursive: true, force: true })
    })))

  it("moves exactly the turn rows when a replay line is appended", () =>
    run(Effect.gen(function*() {
      const root = copyFixtures()
      yield* sweep(root)
      const replay = readFileSync(join(root, INTERRUPTED, "replay.jsonl"), "utf8")
      writeFileSync(
        join(root, INTERRUPTED, "replay.jsonl"),
        `${replay}{"turn": 5, "year": -3800, "players": [{"seat_id": "place-1"}]}\n`
      )
      const second = yield* sweep(root)

      expect(second.writes.games).toBe(1)
      expect(second.writes.turns).toBe(1)
      expect(second.writes.playerTurns).toBe(1)
      expect(second.writes.deletes).toBe(0)
      rmSync(join(root, ".."), { recursive: true, force: true })
    })))

  it("hashes a run identically on ten independent reads of the same directory", () => {
    const options = Ingest.ingestOptions(FIXTURES)
    const root = Ingest.resolveRunsRoot(FIXTURES)
    const hashes = Array.from(
      { length: 10 },
      () => Array.from(Ingest.collectRun(root, TERMINAL, options).contentHash).join(",")
    )
    expect(new Set(hashes).size).toBe(1)
  })

  it("reports what a dry run would do, and writes none of it", () =>
    run(Effect.gen(function*() {
      const db = yield* PgDrizzle.PgDrizzle
      const dry = yield* sweep(FIXTURES, { dryRun: true })

      expect(dry.dryRun).toBe(true)
      expect(dry.runs.length).toBe(7)
      expect(dry.runs.every((entry) => entry.outcome === "inserted")).toBe(true)
      expect(writes(dry)).toBe(0)
      expect((yield* gameRows(db)).length).toBe(0)

      yield* sweep(FIXTURES)
      const after = yield* sweep(FIXTURES, { dryRun: true })
      expect(after.runs.every((entry) => entry.outcome === "unchanged")).toBe(true)
    })))
})

describe("deletion is opt-in", () => {
  it("deletes nothing without --prune, however much of the root has gone", () =>
    run(Effect.gen(function*() {
      const db = yield* PgDrizzle.PgDrizzle
      const root = copyFixtures()
      yield* sweep(root)
      rmSync(join(root, TERMINAL), { recursive: true, force: true })
      const second = yield* sweep(root)

      // One database may serve more than one archive and the schema has no
      // `runs_root` column, so an unscoped delete phase is not the default.
      expect(second.deleted).toEqual([])
      expect((yield* gameRows(db)).length).toBe(7)
      rmSync(join(root, ".."), { recursive: true, force: true })
    })))

  it("deletes exactly the vanished ids with --prune, and cascades their children", () =>
    run(Effect.gen(function*() {
      const db = yield* PgDrizzle.PgDrizzle
      const root = copyFixtures()
      yield* sweep(root)
      rmSync(join(root, TERMINAL), { recursive: true, force: true })
      const second = yield* sweep(root, { prune: true })

      expect(second.deleted).toEqual([TERMINAL])
      expect(second.writes.deletes).toBe(1)
      expect((yield* gameRows(db)).map((row) => row.gameId)).not.toContain(TERMINAL)
      const orphans = yield* Effect.orDie(
        db.select().from(playerTurns).where(eq(playerTurns.gameId, TERMINAL))
      )
      expect(orphans).toEqual([])
      rmSync(join(root, ".."), { recursive: true, force: true })
    })))

  it("prunes only the ids a --game sweep was given", () =>
    run(Effect.gen(function*() {
      const db = yield* PgDrizzle.PgDrizzle
      const root = copyFixtures()
      yield* sweep(root)
      rmSync(join(root, TERMINAL), { recursive: true, force: true })
      rmSync(join(root, NOWIN), { recursive: true, force: true })
      const second = yield* sweep(root, { prune: true, gameIds: new Set([TERMINAL]) })

      expect(second.deleted).toEqual([TERMINAL])
      expect((yield* gameRows(db)).map((row) => row.gameId)).toContain(NOWIN)
      rmSync(join(root, ".."), { recursive: true, force: true })
    })))
})

// -------------------------------------------------------------- the boards ---

/** A runner that records its requests and answers a board shaped like the bridge's. */
const recordingRunner = (
  seen: Array<string>
): DerivationRunner =>
(request: DerivationRequest) => {
  seen.push(
    request.operation === "board"
      ? `board:${request.gameId}:${String(request.turn)}:${String(request.places.length)}`
      : `${request.operation}:${request.gameId}`
  )
  return Effect.succeed({
    turn: request.operation === "board" ? request.turn : 0n,
    tiles: [{ x: 0n, y: 0n, terrain: "grassland" }]
  })
}

const boardsWith = (
  root: string,
  runner: DerivationRunner,
  overrides: Partial<Ingest.BoardOptions> = {}
): Partial<Ingest.IngestOptions> => ({
  boards: Option.some({
    ...Ingest.boardOptions(join(root, "..", "derive-cache")),
    ...overrides,
    runner: Option.some(runner)
  })
})

describe("the board phase", () => {
  it("is off by default, so the parity sweep spawns nothing", () =>
    run(Effect.gen(function*() {
      const db = yield* PgDrizzle.PgDrizzle
      const report = yield* sweep(FIXTURES)
      expect(report.writes.boards).toBe(0)
      expect(yield* Effect.orDie(db.select().from(boardState))).toEqual([])
    })))

  it("derives exactly the turns that have both an autosave and a turns row", () =>
    run(Effect.gen(function*() {
      const db = yield* PgDrizzle.PgDrizzle
      const root = copyFixtures()
      const seen: Array<string> = []
      const report = yield* sweep(root, boardsWith(root, recordingRunner(seen)))

      // `terminal_valid_01` has autosaves for turns 1 and 2, and replay lines
      // for 1, 2 and 3: the intersection is {1, 2}. Turn 3 is never attempted
      // because no save exists, and no save exists for a turn with no line.
      expect(seen.toSorted()).toEqual([
        `board:${TERMINAL}:1:2`,
        `board:${TERMINAL}:2:2`
      ])
      expect(report.writes.boards).toBe(2)
      const rows = yield* Effect.orDie(db.select().from(boardState))
      expect(rows.map((row) => `${row.gameId}:${row.turn}`).toSorted()).toEqual([
        `${TERMINAL}:1`,
        `${TERMINAL}:2`
      ])
      expect(asObject(rows[0]?.board)["tiles"]).toEqual([{ x: 0, y: 0, terrain: "grassland" }])
      rmSync(join(root, ".."), { recursive: true, force: true })
    })))

  it("records a turn whose autosave has no replay line as a skip, not an error", () =>
    run(Effect.gen(function*() {
      const root = copyFixtures()
      writeFileSync(join(root, TERMINAL, "saves", "turn-0042-auto.sav.gz"), "not a save")
      const seen: Array<string> = []
      const report = yield* sweep(root, boardsWith(root, recordingRunner(seen)))

      // The composite FK to `turns(game_id, turn)` forbids the row, so the turn
      // is never even attempted — and it is retried on the next sweep.
      expect(seen.some((entry) => entry.includes(":42:"))).toBe(false)
      expect(
        report.skipped.filter((skip) => skip.reason === "boardTurnNotRecorded").map((skip) =>
          Option.getOrNull(skip.detail)
        )
      ).toEqual(["turn 42"])
      expect(report.status).toBe("complete")
      rmSync(join(root, ".."), { recursive: true, force: true })
    })))

  it("resumes: a fully boarded game spawns nothing and writes nothing", () =>
    run(Effect.gen(function*() {
      const root = copyFixtures()
      const first: Array<string> = []
      yield* sweep(root, boardsWith(root, recordingRunner(first)))
      const second: Array<string> = []
      const again = yield* sweep(root, boardsWith(root, recordingRunner(second)))

      expect(first.length).toBe(2)
      expect(second).toEqual([])
      expect(again.writes.boards).toBe(0)
      expect(again.transactionsWithWrites).toBe(0)
      rmSync(join(root, ".."), { recursive: true, force: true })
    })))

  it("fills in over several sweeps under --board-turn-limit", () =>
    run(Effect.gen(function*() {
      const root = copyFixtures()
      const first: Array<string> = []
      const one = yield* sweep(root, boardsWith(root, recordingRunner(first), { turnLimit: 1 }))
      expect(first).toEqual([`board:${TERMINAL}:1:2`])
      expect(one.writes.boards).toBe(1)

      const second: Array<string> = []
      const two = yield* sweep(root, boardsWith(root, recordingRunner(second), { turnLimit: 1 }))
      expect(second).toEqual([`board:${TERMINAL}:2:2`])
      expect(two.writes.boards).toBe(1)
      rmSync(join(root, ".."), { recursive: true, force: true })
    })))

  it("keeps a failed derivation to one reported skip, and the run's rows intact", () =>
    run(Effect.gen(function*() {
      const db = yield* PgDrizzle.PgDrizzle
      const root = copyFixtures()
      const failing: DerivationRunner = (request: DerivationRequest) =>
        request.operation === "board" && request.turn === 1n
          ? Effect.fail(
            new (class extends Error {
              readonly _tag = "DerivationArtifactsMissing" as const
              readonly operation = "board" as const
              readonly gameId = request.gameId
              readonly detail = "no artifacts"
            })() as never
          )
          : Effect.succeed({ turn: 2n })
      const report = yield* sweep(root, boardsWith(root, failing))

      expect(report.writes.boards).toBe(1)
      expect(report.skipped.some((skip) => skip.reason === "boardUnavailable")).toBe(true)
      // The documents committed in their own transaction, before the boards.
      expect((yield* gameRows(db)).length).toBe(7)
      rmSync(join(root, ".."), { recursive: true, force: true })
    })))

  it("keeps boards out of the content hash, so a new autosave rewrites no typed row", () =>
    run(Effect.gen(function*() {
      const root = copyFixtures()
      const seen: Array<string> = []
      yield* sweep(root, boardsWith(root, recordingRunner(seen)))
      writeFileSync(join(root, TERMINAL, "saves", "turn-0003-auto.sav.gz"), "new save")
      const second = yield* sweep(root, boardsWith(root, recordingRunner(seen)))

      // Turn 3 *does* have a replay line, so the new save is derived — and the
      // run's own rows still say `unchanged`, because a save is not a document.
      expect(second.runs.every((entry) => entry.outcome === "unchanged")).toBe(true)
      expect(second.writes.games).toBe(0)
      expect(second.writes.boards).toBe(1)
      rmSync(join(root, ".."), { recursive: true, force: true })
    })))

  it("refuses a cache root that nests with the archive, before writing anything", () =>
    run(Effect.gen(function*() {
      const db = yield* PgDrizzle.PgDrizzle
      const root = copyFixtures()
      const seen: Array<string> = []
      const outcome = yield* Effect.either(
        sweep(root, {
          boards: Option.some({
            ...Ingest.boardOptions(join(root, "derive-cache")),
            runner: Option.some(recordingRunner(seen))
          })
        })
      )

      expect(Either.isLeft(outcome)).toBe(true)
      expect(Either.isLeft(outcome) ? outcome.left._tag : "").toBe("BoardCacheRootInvalid")
      // Nothing was walked, so nothing was written.
      expect((yield* gameRows(db)).length).toBe(0)
      expect(seen).toEqual([])
      rmSync(join(root, ".."), { recursive: true, force: true })
    })))

  it("derives no boards during a dry run", () =>
    run(Effect.gen(function*() {
      const root = copyFixtures()
      const seen: Array<string> = []
      const dry = yield* sweep(root, {
        ...boardsWith(root, recordingRunner(seen)),
        dryRun: true
      })
      expect(seen).toEqual([])
      expect(dry.writes.boards).toBe(0)
      rmSync(join(root, ".."), { recursive: true, force: true })
    })))
})

// ---------------------------------------------------------------- the CLI ---

describe("the command line", () => {
  const args = (overrides: Partial<Cli.IngestCliArgs> = {}): Cli.IngestCliArgs => ({
    runsRoot: FIXTURES,
    databaseUrl: Redacted.make("postgres://localhost:5432/arena"),
    game: [],
    dryRun: false,
    skipMigrations: false,
    prune: false,
    skipBoards: true,
    cacheRoot: Option.none(),
    repoRoot: Ingest.PACKAGE_REPO_ROOT,
    boardTurnLimit: 0,
    boardConcurrency: 4,
    maxReplayBytes: Ingest.DEFAULT_MAX_REPLAY_BYTES,
    ...overrides
  })

  it("maps --skip-boards to no board phase at all", () =>
    Effect.runPromise(
      Effect.map(Cli.ingestOptionsOf(args()), (options) => {
        expect(Option.isNone(options.boards)).toBe(true)
        expect(options.runsRoot).toBe(FIXTURES)
        expect(options.prune).toBe(false)
      })
    ))

  it("refuses a board phase with no cache root, rather than inventing one", () =>
    Effect.runPromise(
      Effect.map(
        Effect.either(Cli.ingestOptionsOf(args({ skipBoards: false }))),
        (outcome) => {
          // A `mkdtemp` default would silently re-parse every savegame in the
          // archive on every sweep: `save_replay._load_cache` validates entries
          // against the source save's inode and mtime.
          expect(Either.isLeft(outcome)).toBe(true)
          expect(Either.isLeft(outcome) ? outcome.left.name : "").toBe("BoardCacheRootMissing")
        }
      )
    ))

  it("carries the board flags through to the phase's options", () =>
    Effect.runPromise(
      Effect.map(
        Cli.ingestOptionsOf(
          args({
            skipBoards: false,
            cacheRoot: Option.some("/tmp/arena-cache"),
            boardTurnLimit: 25,
            boardConcurrency: 2,
            game: [TERMINAL],
            prune: true,
            dryRun: true
          })
        ),
        (options) => {
          const boards = Option.getOrNull(options.boards)
          expect(boards?.cacheRoot).toBe("/tmp/arena-cache")
          expect(boards?.turnLimit).toBe(25)
          expect(boards?.concurrency).toBe(2)
          expect(boards?.repoRoot).toBe(Ingest.PACKAGE_REPO_ROOT)
          expect([...options.gameIds]).toEqual([TERMINAL])
          expect(options.prune).toBe(true)
          expect(options.dryRun).toBe(true)
        }
      )
    ))

  it("summarises a sweep without naming a connection", () =>
    run(Effect.gen(function*() {
      const report = yield* sweep(FIXTURES)
      const text = Ingest.describeReport(report)
      expect(text).toContain("entries seen   8")
      expect(text).toContain("7 inserted")
      expect(text).toContain("refused runs   0")
      expect(text).toContain("unstorable     0")
      expect(text).toContain(`${SYMLINK}: symlink`)
      expect(text).not.toContain("postgres://")
    })))
})

// ------------------------------------------------------- reading the disk ---

describe("what makes a directory a run", () => {
  it("is the same predicate the prune phase re-asks", () => {
    const root = Ingest.resolveRunsRoot(FIXTURES)
    expect(Ingest.isRunOnDisk(root, TERMINAL)).toBe(true)
    // A symlinked run may not point out of the archive.
    expect(Ingest.isRunOnDisk(root, SYMLINK)).toBe(false)
    expect(Ingest.isRunOnDisk(root, "not-a-game-id")).toBe(false)
    expect(Ingest.isRunOnDisk(root, "game_parity_absent_00000000")).toBe(false)
  })

  it("aborts the sweep, and deletes nothing, when the root cannot be listed", () =>
    run(Effect.gen(function*() {
      const db = yield* PgDrizzle.PgDrizzle
      yield* sweep(FIXTURES)
      const missing = join(mkdtempSync(join(tmpdir(), "arena-ingest-")), "gone")
      const outcome = yield* Effect.either(sweep(missing, { prune: true }))

      expect(Either.isLeft(outcome)).toBe(true)
      expect(Either.isLeft(outcome) ? outcome.left._tag : "").toBe("RunsRootUnreadable")
      // An ingester that reported success on a root it never read would licence
      // a deletion pass over an archive it cannot see.
      expect((yield* gameRows(db)).length).toBe(7)
    })))

  it("classifies a plain file and a nested directory by the fs backend's rules", () =>
    run(Effect.gen(function*() {
      const root = join(mkdtempSync(join(tmpdir(), "arena-ingest-")), "runs")
      mkdirSync(root, { recursive: true })
      writeFileSync(join(root, "game_parity_file_00000000001"), "not a run")
      writeFileSync(join(root, "README"), "not a game id")
      const report = yield* sweep(root)

      expect(report.runs).toEqual([])
      expect(report.skipped.map((skip) => `${skip.entry}:${skip.reason}`).toSorted()).toEqual([
        "README:notAGameId",
        "game_parity_file_00000000001:notADirectory"
      ])
      rmSync(join(root, ".."), { recursive: true, force: true })
    })))
})
