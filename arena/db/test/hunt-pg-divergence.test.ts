/**
 * Hunting for a divergence between the two backends on documents nobody would
 * write on purpose.
 *
 * The v1 version of this file hunted the materializer and the derivation cache
 * mirror. Both are gone (schema v2 stores no artifacts and no paths), and the
 * hunt moves to where v2 actually put the risk: **a manifest is no longer stored
 * as bytes**. It is taken apart into typed columns and an `extras` envelope at
 * ingest and put back together at serving time, so every column is a chance to
 * launder a value — and the corpus already contains values that do not fit.
 *
 * Each case below is a run directory the *filesystem* backend serves, plus the
 * `games` row the plan says ingest must write for it, **spelled out by hand**.
 * Hand-written rather than produced by an encoder on purpose: an encoder shared
 * with the reconstruction under test can be wrong in both directions at once and
 * still look green. The expected storage shape is the assertion.
 *
 * Two verdicts are possible, and the difference is the point:
 *
 * - **parity** — fs and pg answer the same thing, byte for byte. The demotion
 *   rule is what makes that true for `state: 17`, `current_turn: 5.5`,
 *   `current_turn: 99999999999999999999` and an explicit `null` that has to beat
 *   a `status` sibling.
 * - **declared divergence** — they answer differently, we know why, and the test
 *   pins the exact difference rather than waiving it. There are three, all
 *   inherent to Postgres rather than to this code: a `U+0000` or a lone
 *   surrogate anywhere in a document (`jsonb` and `text` both refuse them, and
 *   the frozen schema has no bytes-shaped carrier), and a `-0` in a `float8`
 *   column (the parameter binding spells it `0`, and `JSON.stringify(-0)` is
 *   `"0"`, so `extras` cannot rescue it either).
 *
 * If a case here ever flips from one verdict to the other, something changed
 * that the 3-way oracle would eventually have caught three layers away.
 */

import { BunContext } from "@effect/platform-bun"
import * as PgDrizzle from "@effect/sql-drizzle/Pg"
import * as Effect from "effect/Effect"
import * as Either from "effect/Either"
import * as Layer from "effect/Layer"
import * as ManagedRuntime from "effect/ManagedRuntime"
import * as Option from "effect/Option"
import { afterAll, describe, expect, it } from "bun:test"
import { createHash } from "node:crypto"
import { mkdirSync, rmSync, writeFileSync } from "node:fs"
import { join } from "node:path"

import { CANON_UTF8, type CanonValue, canonicalText, Gateway, type JsonObject } from "@arena/wire"

import type { Canonical } from "../../harness/src/gateway/public.ts"
import {
  makeRunsRepository,
  type RunsError,
  type RunsRepositoryApi
} from "../../harness/src/gateway/services/runs.ts"

import { gameFields } from "../src/ingest.ts"
import * as Migrate from "../src/migrate.ts"
import * as Pglite from "../src/pglite.ts"
import {
  type Database,
  type GameDocumentRow,
  makeRunsRepositoryPg,
  reconstructManifest
} from "../src/runs-repository-pg.ts"
import { games } from "../src/schema.ts"

// ---------------------------------------------------------------- the cases --

/** What a `games` row looks like when a hand writes it. */
interface StoredRow {
  readonly state?: "lobby" | "starting" | "running" | "completed" | "invalid" | "failed" | "cancelled"
  readonly schemaVersion?: number | null
  readonly createdAt?: number | null
  readonly startedAt?: number | null
  readonly finishedAt?: number | null
  readonly currentTurn?: number | null
  readonly lastReplayTurn?: number | null
  readonly benchmarkValid?: boolean | null
  readonly config?: unknown
  readonly manifestStatus?: "ok" | "unusable" | "absent"
  readonly reportStatus?: "ok" | "unusable" | "absent"
  readonly extras?: unknown
}

interface HuntCase {
  /** 20–80 characters of `[A-Za-z0-9_-]`, or `GAME_ID_RE` refuses it. */
  readonly id: string
  /** The exact bytes of `manifest.json` — spelling is the subject. */
  readonly manifest: string
  readonly report?: string
  readonly replay?: string
  readonly row: StoredRow
  /** Set when fs and pg are *expected* to differ, with the reason. */
  readonly divergence?: string
}

/** `config` as every case spells it: an object, so `diskGameRow` keeps the row. */
const CONFIG = `{"mode": "duel", "turns": 10, "places": 2}`
const CONFIG_VALUE = { mode: "duel", turns: 10, places: 2 }

const document = (fields: ReadonlyArray<string>): string => `{${fields.join(", ")}}`

const HUGE_STATE = "x".repeat(10000)

/** `extras` for a row whose only demoted keys are the ones listed. */
const extrasOf = (
  manifestExtras: Record<string, unknown>,
  derived: Record<string, unknown> = {}
): unknown => ({ manifest: manifestExtras, derived })

const CASES: ReadonlyArray<HuntCase> = [
  {
    // `state` is not a string at all. The column takes the `'invalid'` sentinel
    // and the raw value is demoted, so `manifestState` still sees `17` and still
    // publishes `publicText(17, 'unknown', 32)`.
    id: "hunt_state_is_a_number_01",
    manifest: document([
      `"game_id": "hunt_state_is_a_number_01"`,
      `"state": 17`,
      `"status": "running"`,
      `"created_at": 1.5`,
      `"config": ${CONFIG}`
    ]),
    row: {
      state: "invalid",
      createdAt: 1.5,
      config: CONFIG_VALUE,
      extras: extrasOf({
        game_id: "hunt_state_is_a_number_01",
        state: 17,
        status: "running"
      })
    }
  },
  {
    // 10 000 characters is fine for `text` (measured) and meaningless for the
    // enum, so it demotes; `publicText`'s 32-character truncation happens at
    // serving time, in both backends, from the same input.
    id: "hunt_state_is_a_huge_string_02",
    manifest: document([
      `"game_id": "hunt_state_is_a_huge_string_02"`,
      `"state": ${JSON.stringify(HUGE_STATE)}`,
      `"config": ${CONFIG}`
    ]),
    row: {
      state: "invalid",
      config: CONFIG_VALUE,
      extras: extrasOf({ game_id: "hunt_state_is_a_huge_string_02", state: HUGE_STATE })
    }
  },
  {
    // `"true"` is not a boolean. `diskGameRow` publishes `benchmark_valid: null`
    // for it — tri-state, not falsy — which only survives if the string is
    // demoted rather than coerced.
    id: "hunt_benchmark_valid_is_text_03",
    manifest: document([
      `"game_id": "hunt_benchmark_valid_is_text_03"`,
      `"state": "completed"`,
      `"benchmark_valid": "true"`,
      `"config": ${CONFIG}`
    ]),
    report: `{"manifest": {"game_id": "hunt_benchmark_valid_is_text_03"}, "score": {"final_turn": 3, "players": []}}`,
    row: {
      state: "completed",
      config: CONFIG_VALUE,
      reportStatus: "ok",
      extras: {
        manifest: {
          game_id: "hunt_benchmark_valid_is_text_03",
          state: "completed",
          benchmark_valid: "true"
        },
        report: {
          manifest: { game_id: "hunt_benchmark_valid_is_text_03" },
          score: { final_turn: 3, players: [] }
        },
        derived: {}
      }
    }
  },
  {
    id: "hunt_created_at_is_text_04",
    manifest: document([
      `"game_id": "hunt_created_at_is_text_04"`,
      `"state": "running"`,
      `"created_at": "yesterday"`,
      `"config": ${CONFIG}`
    ]),
    row: {
      state: "running",
      config: CONFIG_VALUE,
      extras: extrasOf({
        game_id: "hunt_created_at_is_text_04",
        state: "running",
        created_at: "yesterday"
      })
    }
  },
  {
    // R6: `integer` is int32 and the corpus already exceeds it. Without the
    // demotion the INSERT throws, the run gets no row, and the pg gateway 404s a
    // game the fs gateway serves — the worst failure shape, because it looks
    // like a missing game rather than a bad field.
    id: "hunt_current_turn_overflows_05",
    manifest: document([
      `"game_id": "hunt_current_turn_overflows_05"`,
      `"state": "running"`,
      `"current_turn": 99999999999999999999`,
      `"config": ${CONFIG}`
    ]),
    row: {
      state: "running",
      config: CONFIG_VALUE,
      extras: extrasOf({
        game_id: "hunt_current_turn_overflows_05",
        state: "running",
        current_turn: 1e20
      })
    }
  },
  {
    // Measured driver hazard #2: `integer` **rounds** a non-integer (5.5 → 6)
    // instead of refusing it. Nothing downstream would ever raise, so the guard
    // has to be at ingest and the value has to be demoted.
    id: "hunt_current_turn_is_frac_06",
    manifest: document([
      `"game_id": "hunt_current_turn_is_frac_06"`,
      `"state": "running"`,
      `"current_turn": 5.5`,
      `"config": ${CONFIG}`
    ]),
    row: {
      state: "running",
      config: CONFIG_VALUE,
      extras: extrasOf({
        game_id: "hunt_current_turn_is_frac_06",
        state: "running",
        current_turn: 5.5
      })
    }
  },
  {
    // Every value explicitly `null`. A column cannot tell an explicit `null`
    // from an absent key, so every one of them is demoted (R5) — and `config:
    // null` is why `diskGameRow` drops this row from the index in *both*
    // backends.
    id: "hunt_every_value_is_null_07",
    manifest: document([
      `"game_id": "hunt_every_value_is_null_07"`,
      `"state": null`,
      `"schema_version": null`,
      `"created_at": null`,
      `"started_at": null`,
      `"finished_at": null`,
      `"current_turn": null`,
      `"benchmark_valid": null`,
      `"config": null`
    ]),
    row: {
      state: "invalid",
      extras: extrasOf({
        game_id: "hunt_every_value_is_null_07",
        state: null,
        schema_version: null,
        created_at: null,
        started_at: null,
        finished_at: null,
        current_turn: null,
        benchmark_valid: null,
        config: null
      })
    }
  },
  {
    id: "hunt_config_is_absent_08",
    manifest: document([
      `"game_id": "hunt_config_is_absent_08"`,
      `"state": "running"`
    ]),
    row: {
      state: "running",
      extras: extrasOf({ game_id: "hunt_config_is_absent_08", state: "running" })
    }
  },
  {
    // jsonb holds a top-level string perfectly well (measured), so `config`
    // keeps its column — and `diskGameRow` still drops the row, because it wants
    // a *mapping*. The two facts are independent and both have to hold.
    id: "hunt_config_is_a_string_09",
    manifest: document([
      `"game_id": "hunt_config_is_a_string_09"`,
      `"state": "running"`,
      `"config": "not a mapping"`
    ]),
    row: {
      state: "running",
      config: "not a mapping",
      extras: extrasOf({ game_id: "hunt_config_is_a_string_09", state: "running" })
    }
  },
  {
    // R5's crux, and the whole reason `state` is stored verbatim as well as in
    // its column: `untrustedFieldOr(manifest, 'state', 'status')` lets a
    // *present* `null` beat `status`, and an *absent* key not. Reconstruct this
    // one from the column alone and the run becomes `completed` — terminal —
    // and the index row changes shape entirely.
    id: "hunt_null_state_beats_status_10",
    manifest: document([
      `"game_id": "hunt_null_state_beats_status_10"`,
      `"state": null`,
      `"status": "completed"`,
      `"config": ${CONFIG}`
    ]),
    row: {
      state: "invalid",
      config: CONFIG_VALUE,
      extras: extrasOf({
        game_id: "hunt_null_state_beats_status_10",
        state: null,
        status: "completed"
      })
    }
  },
  {
    // `_last_replay_turn` answers an unbounded Python `int`; the column is int32.
    // The overflow lives in `extras.derived` as a decimal **string**, and
    // `asInterrupted` has to receive the same `bigint` the fs backend read out
    // of the tail — it ends up in `current_turn` and in the row's summary text.
    id: "hunt_replay_turn_overflows_11",
    manifest: document([
      `"game_id": "hunt_replay_turn_overflows_11"`,
      `"state": "running"`,
      `"current_turn": 3`,
      `"config": ${CONFIG}`
    ]),
    replay: `{"turn": 1}\n{"turn": 99999999999999999999}\n`,
    row: {
      state: "running",
      currentTurn: 3,
      lastReplayTurn: null,
      config: CONFIG_VALUE,
      extras: {
        manifest: {
          game_id: "hunt_replay_turn_overflows_11",
          state: "running"
        },
        derived: { last_replay_turn: "99999999999999999999" }
      }
    }
  },
  {
    // §1.5: `report.seat_stats` is carried whole rather than rebuilt from
    // `agent_stats`, because `mean_latency_ms` is the integer `0` when a seat
    // made no decisions and a float otherwise — a spelling no column records.
    id: "hunt_report_zero_decisions_12",
    manifest: document([
      `"game_id": "hunt_report_zero_decisions_12"`,
      `"state": "completed"`,
      `"benchmark_valid": true`,
      `"config": ${CONFIG}`
    ]),
    report:
      `{"manifest": {"game_id": "hunt_report_zero_decisions_12"}, "score": {"final_turn": 7, "players": [{"seat_id": "place-1", "name": "A", "score": 12, "player_id": 0, "rank": 1, "metrics": {"cities": 2}}]}, "seat_stats": {"place-1": {"turns": 7, "decisions": 0, "mean_latency_ms": 0}}}`,
    row: {
      state: "completed",
      benchmarkValid: true,
      config: CONFIG_VALUE,
      reportStatus: "ok",
      extras: {
        manifest: {
          game_id: "hunt_report_zero_decisions_12",
          state: "completed"
        },
        report: {
          manifest: { game_id: "hunt_report_zero_decisions_12" },
          score: {
            final_turn: 7,
            players: [
              {
                seat_id: "place-1",
                name: "A",
                score: 12,
                player_id: 0,
                rank: 1,
                metrics: { cities: 2 }
              }
            ]
          },
          seat_stats: { "place-1": { turns: 7, decisions: 0, mean_latency_ms: 0 } }
        },
        derived: {}
      }
    }
  },
  {
    // R1, measured: `jsonb` errors on an escaped `U+0000` in a key or a value and
    // `text` errors on the byte. The frozen schema offers no bytes-shaped
    // carrier, and stripping the character would silently change
    // `resolved_places` and the derivation places digest — so the document is
    // refused at ingest and the run answers 503 where the fs backend answers
    // 200. Loud, recorded, and not a waiver.
    id: "hunt_manifest_carries_a_nul_13",
    manifest: document([
      `"game_id": "hunt_manifest_carries_a_nul_13"`,
      `"state": "running"`,
      `"error": "boom\\u0000truncated"`,
      `"config": ${CONFIG}`
    ]),
    row: { state: "invalid", manifestStatus: "unusable", extras: { derived: {} } },
    divergence: "postgres cannot store U+0000 in jsonb or text"
  },
  {
    // R2, the same class and quieter: `"\ud800"` is valid JSON text, `JSON.parse`
    // accepts it, `publicText` passes it through, and `jsonb` refuses it.
    id: "hunt_manifest_lone_surrogate_14",
    manifest: document([
      `"game_id": "hunt_manifest_lone_surrogate_14"`,
      `"state": "running"`,
      `"error": "\\ud800"`,
      `"config": ${CONFIG}`
    ]),
    row: { state: "invalid", manifestStatus: "unusable", extras: { derived: {} } },
    divergence: "postgres cannot store a lone surrogate in jsonb"
  },
  {
    // R3: `created_at: -0.0` reaches `publicNumber` as `-0` and canonicalizes to
    // `-0.0` on the fs leg. It cannot survive v2 in *either* place: the
    // parameter binding spells `String(-0)` as `"0"`, and demoting it does not
    // help because `JSON.stringify(-0)` is `"0"` too. Declared, and recorded in
    // `extras.derived.manifest_negative_zero` so a reader of the row can see
    // that a value was flattened rather than absent.
    id: "hunt_created_at_negative_zero15",
    manifest: document([
      `"game_id": "hunt_created_at_negative_zero15"`,
      `"state": "running"`,
      `"created_at": -0.0`,
      `"config": ${CONFIG}`
    ]),
    row: {
      state: "running",
      createdAt: null,
      config: CONFIG_VALUE,
      extras: {
        manifest: {
          game_id: "hunt_created_at_negative_zero15",
          state: "running",
          created_at: 0
        },
        derived: { manifest_negative_zero: ["/created_at"] }
      }
    },
    divergence: "postgres has no -0: the fs leg publishes -0.0 and the pg leg 0.0"
  }
]

// ------------------------------------------------------------------ fixture --

interface Scratch {
  readonly base: string
  readonly runsRoot: string
}

const makeScratch = (): Scratch => {
  const base = Bun.spawnSync(["mktemp", "-d", "/tmp/arena-pg-hunt-XXXXXX"]).stdout.toString().trim()
  const runsRoot = join(base, "runs")
  mkdirSync(runsRoot, { recursive: true })
  CASES.forEach((hunt) => {
    const runRoot = join(runsRoot, hunt.id)
    mkdirSync(runRoot, { recursive: true })
    writeFileSync(join(runRoot, "manifest.json"), hunt.manifest)
    if (hunt.report !== undefined) {
      writeFileSync(join(runRoot, "report.json"), hunt.report)
    }
    if (hunt.replay !== undefined) {
      writeFileSync(join(runRoot, "replay.jsonl"), hunt.replay)
    }
  })
  return { base, runsRoot }
}

const insertCase = (db: Database, hunt: HuntCase): Effect.Effect<unknown, never> =>
  Effect.orDie(db.insert(games).values({
    gameId: hunt.id,
    state: hunt.row.state ?? "invalid",
    schemaVersion: hunt.row.schemaVersion ?? null,
    createdAt: hunt.row.createdAt ?? null,
    startedAt: hunt.row.startedAt ?? null,
    finishedAt: hunt.row.finishedAt ?? null,
    currentTurn: hunt.row.currentTurn ?? null,
    lastReplayTurn: hunt.row.lastReplayTurn ?? null,
    benchmarkValid: hunt.row.benchmarkValid ?? null,
    config: hunt.row.config ?? null,
    manifestStatus: hunt.row.manifestStatus ?? "ok",
    reportStatus: hunt.row.reportStatus ?? "absent",
    contentHash: new Uint8Array(createHash("sha256").update(hunt.id).digest()),
    extras: hunt.row.extras ?? null
  }))

const testLayer = Layer.mergeAll(
  Layer.provideMerge(PgDrizzle.layer, Pglite.layer(Pglite.memory)),
  BunContext.layer
)

type TestContext = Layer.Layer.Success<typeof testLayer>

const runtime = ManagedRuntime.make(testLayer)

const run = <A, E>(effect: Effect.Effect<A, E, TestContext>): Promise<A> =>
  runtime.runPromise(effect)

interface Fixture {
  readonly fs: RunsRepositoryApi
  readonly pg: RunsRepositoryApi
  readonly scratch: Scratch
}

const fixture: Promise<Fixture> = (async () => {
  const scratch = makeScratch()
  return await run(Effect.orDie(Effect.gen(function*() {
    yield* Migrate.run
    const db = yield* PgDrizzle.PgDrizzle
    yield* Effect.forEach(CASES, (hunt) => insertCase(db, hunt))
    return {
      fs: makeRunsRepository(scratch.runsRoot),
      pg: makeRunsRepositoryPg(db, scratch.runsRoot),
      scratch
    }
  })))
})()

const withFixture = <A, E>(
  body: (fixture: Fixture) => Effect.Effect<A, E, TestContext>
): Promise<A> => fixture.then((value) => run(body(value)))

afterAll(async () => {
  await runtime.dispose()
  const built = await fixture.then(Option.some, () => Option.none<Fixture>())
  if (Option.isSome(built) && built.value.scratch.base.startsWith("/tmp/arena-pg-hunt-")) {
    rmSync(built.value.scratch.base, { recursive: true, force: true })
  }
})

// -------------------------------------------------------------- comparators --

const describeFailure = (error: RunsError): string => `${error._tag}:${error.problem}`

const settle = <A>(
  effect: Effect.Effect<A, RunsError>
): Effect.Effect<Either.Either<A, string>> =>
  Effect.map(Effect.either(effect), Either.mapLeft(describeFailure))

const canonical = (value: CanonValue): string =>
  Either.getOrElse(canonicalText(value, CANON_UTF8), (error) => `!canon:${error._tag}`)

const asCanon = (value: unknown): CanonValue => value as CanonValue

/** The index row a backend publishes for one id, as its canonical bytes. */
const indexRow = (
  repository: RunsRepositoryApi,
  gameId: string
): Effect.Effect<string> =>
  Effect.map(repository.diskGamesIndex(), (index) =>
    canonical(asCanon(index.games.filter((row) => row.game_id === gameId))))

const PARITY = CASES.filter((hunt) => hunt.divergence === undefined)
const DIVERGENT = CASES.filter((hunt) => hunt.divergence !== undefined)

/**
 * The two shapes a declared divergence takes, kept apart on purpose.
 *
 * `UNSTORABLE` is a document ingest **refused** — `manifest_status =
 * 'unusable'`, no reconstruction, 503 — so the run vanishes from the pg index.
 * The `-0` run is the other shape: it is stored, served, and *listed*, and only
 * one field's spelling differs. Collapsing the two would let a future bug drop a
 * whole game and still read as "declared".
 */
const UNSTORABLE = DIVERGENT.filter((hunt) => hunt.row.manifestStatus === "unusable")

const NEGATIVE_ZERO_ID = "hunt_created_at_negative_zero15"

// --------------------------------------------------------------------- tests --

describe("documents that survive the round trip", () => {
  it.each(PARITY.map((hunt) => [hunt.id, hunt] as const))(
    "%s: readManifest is identical",
    (_id, hunt) =>
      withFixture((f) =>
        Effect.gen(function*() {
          const fs = yield* settle(f.fs.readManifest(hunt.id))
          const pg = yield* settle(f.pg.readManifest(hunt.id))
          expect(pg).toEqual(fs)
          // …and the reconstruction is deep-equal to the bytes on disk, which is
          // the §0 predicate: same keys, same scalars, no `bigint` anywhere.
          expect(Either.getOrNull(pg)).toEqual(JSON.parse(hunt.manifest))
        })
      )
  )

  it.each(PARITY.map((hunt) => [hunt.id, hunt] as const))(
    "%s: the index row is byte-identical",
    (_id, hunt) =>
      withFixture((f) =>
        Effect.gen(function*() {
          expect(yield* indexRow(f.pg, hunt.id)).toBe(yield* indexRow(f.fs, hunt.id))
        })
      )
  )

  it.each(PARITY.map((hunt) => [hunt.id, hunt] as const))(
    "%s: terminalArchive is identical",
    (_id, hunt) =>
      withFixture((f) =>
        Effect.gen(function*() {
          const fs = yield* settle(f.fs.terminalArchive(hunt.id))
          const pg = yield* settle(f.pg.terminalArchive(hunt.id))
          expect(pg).toEqual(fs)
        })
      )
  )

  it("relabels an interrupted run whose replay turn does not fit an integer", () =>
    withFixture((f) =>
      Effect.gen(function*() {
        const gameId = "hunt_replay_turn_overflows_11"
        expect(Option.getOrNull(yield* f.pg.lastReplayTurn(gameId)))
          .toBe(99999999999999999999n)
        expect(Option.getOrNull(yield* f.fs.lastReplayTurn(gameId)))
          .toBe(99999999999999999999n)
        const live: ReadonlySet<string> = new Set()
        const fsRows = yield* f.fs.diskRowsWithInterrupted(live)
        const pgRows = yield* f.pg.diskRowsWithInterrupted(live)
        const only = (rows: readonly Canonical<Gateway.GameRow>[]): string =>
          canonical(asCanon(rows.filter((row) => row.game_id === gameId)))
        expect(only(pgRows)).toBe(only(fsRows))
        // The relabelled row quotes the *post-`max`* turn, which is the bigint
        // that never fitted the column.
        expect(only(pgRows)).toContain("99999999999999999999")
      })
    ))

  it("publishes the same whole index for every run it can serve", () =>
    withFixture((f) =>
      Effect.gen(function*() {
        const fs = yield* f.fs.diskGamesIndex()
        const pg = yield* f.pg.diskGamesIndex()
        const ids = (index: { readonly games: readonly { readonly game_id: string }[] }) =>
          index.games.map((row) => row.game_id).toSorted()
        // The only ids the pg index is missing are the ones ingest **refused**
        // — the two documents postgres cannot hold. Every other run, including
        // the `-0` one whose *value* diverges, still has a row: a declared
        // divergence in a field is not a licence to drop the game.
        expect(ids(pg)).toEqual(
          ids(fs).filter((gameId) => !UNSTORABLE.some((hunt) => hunt.id === gameId))
        )
        expect(ids(pg)).toContain(NEGATIVE_ZERO_ID)
      })
    ))
})

describe("divergences we declare rather than discover", () => {
  it.each(DIVERGENT.map((hunt) => [hunt.id, hunt] as const))(
    "%s: %o",
    (_id, hunt) =>
      withFixture((f) =>
        Effect.gen(function*() {
          const fs = yield* settle(f.fs.readManifest(hunt.id))
          const pg = yield* settle(f.pg.readManifest(hunt.id))
          // The fs backend serves the document; that is the whole problem.
          expect(Either.isRight(fs)).toBe(true)
          expect(pg).not.toEqual(fs)
        })
      )
  )

  it("answers 503, not a wrong document, for a manifest postgres cannot hold", () =>
    withFixture((f) =>
      Effect.gen(function*() {
        expect(yield* settle(f.pg.readManifest("hunt_manifest_carries_a_nul_13"))).toEqual(
          Either.left("ArchiveUnavailable:manifestUnavailable")
        )
        expect(yield* settle(f.pg.readManifest("hunt_manifest_lone_surrogate_14"))).toEqual(
          Either.left("ArchiveUnavailable:manifestUnavailable")
        )
      })
    ))

  it("flattens a negative zero to zero, and records that it did", () =>
    withFixture((f) =>
      Effect.gen(function*() {
        const gameId = NEGATIVE_ZERO_ID
        const fs = yield* f.fs.readManifest(gameId)
        const pg = yield* f.pg.readManifest(gameId)
        expect(Object.is(fs["created_at"], -0)).toBe(true)
        expect(Object.is(pg["created_at"], 0)).toBe(true)
        // The published difference, exactly: `-0.0` against `0.0`.
        expect(canonical(asCanon(fs["created_at"]))).toBe("-0.0")
        expect(canonical(asCanon(pg["created_at"]))).toBe("0.0")
      })
    ))
})

// -------------------------------------------- the seam between the two halves --

/**
 * The storage contract, checked against the **real** ingest rather than assumed.
 *
 * Everything above stores its rows by hand, which is what makes the expected
 * shape an assertion instead of a tautology — and which is also its blind spot:
 * a hand-written row proves reconstruction reads what the *plan* says ingest
 * writes, not what `src/ingest.ts` actually writes. This block closes the seam by
 * running `gameFields` — the partition itself — and reconstructing its output.
 *
 * The predicate is §0's, and no database is involved: reconstruction is pure, so
 * a demotion bug shows up here as a one-line diff instead of as a byte mismatch
 * three layers away.
 *
 * The `"state": "invalid"` case is sharp enough to have its own pin below.
 */
const ROUND_TRIP: ReadonlyArray<readonly [string, JsonObject]> = [
  ["a recognized state", { game_id: "g", state: "running", status: "running", config: CONFIG_VALUE }],
  ["no state at all, with a status sibling", { game_id: "g", status: "running" }],
  ["a present null state, which beats status", { game_id: "g", state: null, status: "running" }],
  ["a state that is not a string", { game_id: "g", state: 17, status: "running" }],
  ["every column at once", {
    game_id: "g",
    state: "completed",
    schema_version: 3,
    created_at: 1785660947.0197709,
    started_at: 1785660950.5,
    finished_at: 1785661000,
    current_turn: 12,
    benchmark_valid: true,
    config: CONFIG_VALUE,
    joined_agents: 2,
    resolved_places: []
  }],
  ["values no column can hold", {
    game_id: "g",
    state: HUGE_STATE,
    schema_version: 5.5,
    created_at: "yesterday",
    // `99999999999999999999` as the double `JSON.parse` answers for it — the
    // pinned hunt case, spelled so no lint rule has to guess whether the
    // precision loss was intended.
    current_turn: 1e20,
    benchmark_valid: "true",
    config: null
  }]
]

/** One manifest, through the real partition and back. */
const roundTrip = (manifest: JsonObject): JsonObject | null => {
  const fields = gameFields(manifest)
  const row: GameDocumentRow = {
    gameId: "g",
    state: fields.state,
    schemaVersion: fields.schemaVersion,
    createdAt: fields.createdAt,
    startedAt: fields.startedAt,
    finishedAt: fields.finishedAt,
    currentTurn: fields.currentTurn,
    lastReplayTurn: null,
    benchmarkValid: fields.benchmarkValid,
    config: fields.config,
    manifestStatus: "ok",
    reportStatus: "absent",
    extras: { manifest: fields.extrasManifest, derived: {} }
  }
  return Option.getOrNull(reconstructManifest(row))
}

/** The manifest whose `state` ingest currently loses. See the pin below. */
const SENTINEL_MANIFEST: JsonObject = {
  game_id: "g",
  state: "invalid",
  status: "invalid",
  error: "boom",
  config: CONFIG_VALUE
}

describe("the ingest contract reconstruction depends on", () => {
  it.each(ROUND_TRIP.map(([name, manifest]) => [name, manifest] as const))(
    "%s: gameFields → reconstructManifest is the document",
    (_name, manifest) => {
      expect(roundTrip(manifest)).toEqual(manifest)
    }
  )

  /**
   * **A known ingest defect, pinned rather than waived.**
   *
   * `'invalid'` is *both* one of the seven enum spellings and the sentinel
   * `games.state` carries for "the manifest said something unrecognized, or said
   * nothing at all". `gameFields` treats a literal `"state": "invalid"` as
   * columnizable (`isStorableRunState` is true for it), so the key is left out of
   * `extras.manifest` — and the resulting row, column `'invalid'` with no demoted
   * `state`, is **indistinguishable** from the row a manifest with no `state` key
   * writes.
   *
   * Reconstruction resolves the ambiguity by omitting the key, and must: emitting
   * the sentinel would invent a `state` that beats a `status` sibling at
   * `untrustedFieldOr(manifest, 'state', 'status')` for every manifest that
   * genuinely has none. So the obligation is ingest's — record the raw value
   * whenever the key is present, keeping the column as it is:
   *
   * ```ts
   * // src/ingest.ts, gameFields' columnKeys
   * ...(stateColumn && rawState !== UNRECOGNIZED_STATE ? ["state"] : []),
   * ```
   *
   * It was not hypothetical: five of the thirty-one runs in the live archive
   * spell `"state": "invalid"`. The one-liner landed (`rawState !==
   * UNRECOGNIZED_STATE` in `gameFields`' columnKeys), so the key now also
   * lands verbatim in `extras.manifest` and the round trip is total.
   */
  it("keeps a manifest that spells the sentinel itself", () => {
    expect(roundTrip(SENTINEL_MANIFEST)).toEqual(SENTINEL_MANIFEST)
  })

  it("…and the column still carries the sentinel for the enum's sake", () => {
    // The fix is the extras partition and nothing else: the row's state column
    // is unchanged, so enum-typed reads keep working.
    expect(gameFields(SENTINEL_MANIFEST).state).toBe("invalid")
  })
})
