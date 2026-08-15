/**
 * The arena v2 schema, exercised hermetically against PGlite.
 *
 * v2 stores a *model of a game*, not a mirror of a `runs_root`: typed columns
 * plus two jsonb envelopes, no artifact bytes, no filesystem paths. That change
 * moves the parity risk out of `bytea` and into the column types themselves, so
 * this file pins the column-level facts the reconstruction spec is built on:
 *
 * 1. The committed `drizzle/` migration applies from an empty database, is
 *    idempotent, and produces exactly the tables, enums, columns, keys and
 *    cascades that the frozen `src/schema.ts` declares — nothing more.
 * 2. What each column type can and cannot hold. `integer` is int32 and the
 *    corpus already exceeds it; `jsonb` and `text` refuse U+0000; `jsonb`
 *    refuses an unpaired surrogate while `text` silently *replaces* it; `jsonb`
 *    reorders keys and collapses duplicates. Every one of those is a demotion
 *    rule or a declared divergence upstream, and each is measured here rather
 *    than assumed.
 * 3. The keys really are the ingest idempotency keys, and the composite
 *    `turns` foreign key is what stops a board or a player row existing for a
 *    turn `replay.jsonl` never recorded.
 *
 * The same migration and the same drizzle statements run against live Postgres,
 * so anything asserted here is asserted about production.
 */

import { BunContext } from "@effect/platform-bun"
import * as PgDrizzle from "@effect/sql-drizzle/Pg"
import * as SqlClient from "@effect/sql/SqlClient"
import { eq, getTableColumns, getTableName, type Table } from "drizzle-orm"
import * as Effect from "effect/Effect"
import * as Either from "effect/Either"
import * as Layer from "effect/Layer"
import * as Redacted from "effect/Redacted"
import * as Schema from "effect/Schema"
import { describe, expect, it } from "bun:test"

import * as Client from "../src/client.ts"
import * as Migrate from "../src/migrate.ts"
import * as Pglite from "../src/pglite.ts"
import {
  agentStats,
  arenaTables,
  boardState,
  games,
  playerTurns,
  seats,
  turns
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

const isFailure = <A, E>(effect: Effect.Effect<A, E, TestContext>) =>
  Effect.map(Effect.either(effect), Either.isLeft)

const GAME_ID = "game_parity_terminal_valid_01"

/**
 * The three NOT NULL columns a `games` row cannot be written without. Every
 * other column is nullable by design: a `NULL` column means "the manifest had
 * no such key", which is the reconstruction spec's absent-vs-null rule.
 */
const gameRow = (gameId: string) =>
  ({
    gameId,
    state: "invalid",
    manifestStatus: "absent",
    reportStatus: "absent",
    contentHash: new Uint8Array(32).fill(7)
  }) as const

const seedGame = (db: PgDrizzle.PgDrizzle["Type"], gameId: string = GAME_ID) =>
  db.insert(games).values(gameRow(gameId))

/** A `games` row plus the `turns` row that `board_state` and `player_turns` hang off. */
const seedTurn = (db: PgDrizzle.PgDrizzle["Type"], gameId: string, turn: number) =>
  Effect.gen(function*() {
    yield* seedGame(db, gameId)
    yield* db.insert(turns).values({ gameId, turn, year: -3000 })
  })

/** Every byte value, so nothing can be laundered by a text codec. */
const allBytes = new Uint8Array(Array.from({ length: 256 }, (_, i) => i))

/**
 * How `src/schema.ts` spells a table's columns. `getSQLType` produces exactly
 * what Postgres' `format_type` produces (`double precision`, `timestamp with
 * time zone`, the enum's own name), so the declaration and the database are
 * directly comparable without a translation table that could itself be wrong.
 */
const columnsInSchema = (table: Table): Array<string> =>
  Object.values(getTableColumns(table)).map((column) =>
    `${column.name} ${column.getSQLType()}${column.notNull ? " not null" : ""}`
  )

/**
 * A jsonb column reads back as `unknown`, which is the honest type: nothing
 * about the column promises an object. Decoding rather than asserting keeps the
 * test's own narrowing as strict as the ingest code's will have to be.
 */
const asJsonObject = Schema.decodeUnknownSync(
  Schema.Record({ key: Schema.String, value: Schema.Unknown })
)

const asNumberObject = Schema.decodeUnknownSync(
  Schema.Record({ key: Schema.String, value: Schema.Number })
)

// ------------------------------------------------------------- migrations ---

describe("committed drizzle migrations", () => {
  it("apply from an empty database and are idempotent", () =>
    run(Effect.gen(function*() {
      const applied = yield* Migrate.run
      const reapplied = yield* Migrate.run

      // A single baseline: v1's three migrations described a blob mirror whose
      // tables no longer exist, and no v1 -> v2 data migration exists or is
      // wanted — a v1 database is re-ingested from disk in one sweep.
      expect(applied.map(([, name]) => name)).toEqual(["0000_arena_v2"])
      // Migration ids are journal `idx + 1`: @effect/sql reads id 0 as "nothing
      // applied", so a zero-based id would replay forever.
      expect(applied.map(([id]) => id)).toEqual([1])
      expect(reapplied).toEqual([])
    })))

  it("create exactly the six declared tables, and no table from any earlier design", () =>
    run(Effect.gen(function*() {
      yield* Migrate.run
      const client = yield* SqlClient.SqlClient
      const rows = yield* client.unsafe<{ table_name: string }>(
        `select table_name from information_schema.tables
         where table_schema = 'public' order by table_name`
      )
      const names = rows.map((row) => row.table_name)

      const declared: ReadonlyArray<string> = arenaTables.map(getTableName)
      expect(declared).toEqual([
        "games",
        "seats",
        "turns",
        "board_state",
        "player_turns",
        "agent_stats"
      ])

      // Every declared table exists...
      expect(names).toEqual(expect.arrayContaining([...declared]))
      // ...and the only extra is the Migrator's own bookkeeping table.
      expect(names.filter((name) => !declared.includes(name))).toEqual(["effect_sql_migrations"])

      // The v1 blob mirror and the stale intermediate design are gone for good.
      // A leftover here would mean the drizzle folder was not regenerated from
      // the frozen schema, which is the one way the migration can lie.
      const buried = [
        "blobs",
        "runs",
        "run_documents",
        "run_frames",
        "run_videos",
        "run_saves",
        "run_replay_tail",
        "ingest_sweeps",
        "derivation_cache",
        "derivation_workdirs",
        "game_documents",
        "game_places",
        "game_score_players",
        "game_victory_nodes",
        "game_invalid_reasons"
      ]
      expect(names.filter((name) => buried.includes(name))).toEqual([])
    })))

  it("create the three enums with their labels in order, and no others", () =>
    run(Effect.gen(function*() {
      yield* Migrate.run
      const client = yield* SqlClient.SqlClient
      const rows = yield* client.unsafe<{ typname: string; enumlabel: string }>(
        `select t.typname, e.enumlabel from pg_type t
         join pg_enum e on e.enumtypid = t.oid
         join pg_namespace n on n.oid = t.typnamespace
         where n.nspname = 'public'
         order by t.typname, e.enumsortorder`
      )
      const labels = (name: string) =>
        rows.filter((row) => row.typname === name).map((row) => row.enumlabel)

      // The seven states the supervisor actually writes. `interrupted` and
      // `unknown` are derived at serving time from live/terminal context and are
      // deliberately not storable; `invalid` doubles as the sentinel for a
      // manifest whose `state` is not one of the seven.
      expect(labels("run_state")).toEqual([
        "lobby",
        "starting",
        "running",
        "completed",
        "invalid",
        "failed",
        "cancelled"
      ])
      // `absent` -> 404, `unusable` -> 503, `ok` -> the typed columns are real.
      // v1's `document_status` had no `absent` label, because v1 stored a row
      // only for a document it had read.
      expect(labels("document_status")).toEqual(["ok", "unusable", "absent"])
      // Two kinds and no third: an LLM agent, or the game's built-in AI.
      expect(labels("seat_kind")).toEqual(["agent", "cpu"])

      const declared = ["document_status", "run_state", "seat_kind"]
      expect([...new Set(rows.map((row) => row.typname))].toSorted()).toEqual(declared)
    })))

  it("match src/schema.ts column for column — name, type and nullability", () =>
    run(Effect.gen(function*() {
      yield* Migrate.run
      const client = yield* SqlClient.SqlClient
      // `format_type` spells a column the way drizzle's `getSQLType` does
      // (`double precision`, `timestamp with time zone`, the enum's own name),
      // so the two sides are directly comparable without a translation table
      // that could itself be wrong.
      const rows = yield* client.unsafe<{
        relname: string
        attname: string
        sqltype: string
        attnotnull: boolean
      }>(
        `select c.relname, a.attname, format_type(a.atttypid, a.atttypmod) as sqltype, a.attnotnull
         from pg_attribute a
         join pg_class c on c.oid = a.attrelid
         join pg_namespace n on n.oid = c.relnamespace
         where n.nspname = 'public' and c.relkind = 'r' and a.attnum > 0 and not a.attisdropped
         order by c.relname, a.attnum`
      )

      const inDatabase = (table: string) =>
        rows
          .filter((row) => row.relname === table)
          .map((row) => `${row.attname} ${row.sqltype}${row.attnotnull ? " not null" : ""}`)

      arenaTables.forEach((table) => {
        expect(inDatabase(getTableName(table))).toEqual(columnsInSchema(table))
      })

      // And the frozen shape itself, written out once, so an unauthorised edit
      // to `schema.ts` fails here instead of passing a comparison with itself.
      expect(inDatabase("games")).toEqual([
        "game_id text not null",
        "state run_state not null",
        "schema_version integer",
        "created_at double precision",
        "started_at double precision",
        "finished_at double precision",
        "current_turn integer",
        "last_replay_turn integer",
        "benchmark_valid boolean",
        "name text",
        "ruleset text",
        "mode text",
        "timing_mode text",
        "max_turns integer",
        "objective text",
        "config jsonb",
        "manifest_status document_status not null",
        "manifest_byte_size bigint",
        "report_status document_status not null",
        "report_byte_size bigint",
        "content_hash bytea not null",
        "ingested_at timestamp with time zone not null",
        "extras jsonb"
      ])
      expect(inDatabase("seats")).toEqual([
        "game_id text not null",
        "seat_index integer not null",
        "seat_id text",
        "kind seat_kind",
        "label text",
        "fingerprint text",
        "metadata jsonb"
      ])
      expect(inDatabase("turns")).toEqual([
        "game_id text not null",
        "turn integer not null",
        "year integer"
      ])
      expect(inDatabase("board_state")).toEqual([
        "game_id text not null",
        "turn integer not null",
        "board jsonb not null"
      ])
      expect(inDatabase("player_turns")).toEqual([
        "game_id text not null",
        "turn integer not null",
        "seat_id text not null",
        "player_id integer",
        "player_name text",
        "nation text",
        "government text",
        "alive boolean",
        "score double precision",
        "cities integer",
        "citizens integer",
        "population bigint",
        "units integer",
        "gold double precision",
        "culture double precision",
        "future_techs integer",
        "known_tech_ids jsonb",
        "research jsonb"
      ])
      expect(inDatabase("agent_stats")).toEqual([
        "game_id text not null",
        "seat_id text not null",
        "controller_fingerprint text",
        "turns integer",
        "decisions integer",
        "fallbacks integer",
        "input_tokens bigint",
        "output_tokens bigint",
        "mean_latency_ms double precision"
      ])
    })))

  it("declare no array column anywhere — every list is jsonb", () =>
    run(Effect.gen(function*() {
      yield* Migrate.run
      const client = yield* SqlClient.SqlClient
      const rows = yield* client.unsafe<{ relname: string; attname: string; sqltype: string }>(
        `select c.relname, a.attname, format_type(a.atttypid, a.atttypmod) as sqltype
         from pg_attribute a
         join pg_class c on c.oid = a.attrelid
         join pg_namespace n on n.oid = c.relnamespace
         where n.nspname = 'public' and c.relkind = 'r' and a.attnum > 0 and not a.attisdropped
           and (a.attndims > 0 or format_type(a.atttypid, a.atttypmod) like '%[]')`
      )
      // `invalid_reasons`, `joined_agents` and `known_tech_ids` are all lists in
      // the source documents, and none of them is a `text[]`: a Postgres array
      // cannot hold a heterogeneous or nested JSON list, and the manifest's
      // lists are untrusted. They live in jsonb, verbatim.
      expect(rows).toEqual([])
    })))

  it("key each table by its ingest identity, and cascade every child from the game", () =>
    run(Effect.gen(function*() {
      yield* Migrate.run
      const client = yield* SqlClient.SqlClient

      const keys = yield* client.unsafe<{ relname: string; contype: string; definition: string }>(
        `select c.relname, con.contype, pg_get_constraintdef(con.oid) as definition
         from pg_constraint con
         join pg_class c on c.oid = con.conrelid
         join pg_namespace n on n.oid = c.relnamespace
         where n.nspname = 'public' and con.contype in ('p', 'f')
         order by c.relname, con.contype, con.conname`
      )
      const shape = (relname: string, contype: string) =>
        keys.filter((row) => row.relname === relname && row.contype === contype)
          .map((row) => row.definition)

      expect(shape("games", "p")).toEqual(["PRIMARY KEY (game_id)"])
      expect(shape("seats", "p")).toEqual(["PRIMARY KEY (game_id, seat_index)"])
      expect(shape("turns", "p")).toEqual(["PRIMARY KEY (game_id, turn)"])
      expect(shape("board_state", "p")).toEqual(["PRIMARY KEY (game_id, turn)"])
      expect(shape("player_turns", "p")).toEqual(["PRIMARY KEY (game_id, turn, seat_id)"])
      expect(shape("agent_stats", "p")).toEqual(["PRIMARY KEY (game_id, seat_id)"])

      // `seats`, `turns` and `agent_stats` hang off the game; `board_state` and
      // `player_turns` hang off a *turn*, which is what makes "a turn the replay
      // never recorded" unstorable rather than silently orphaned.
      expect(shape("seats", "f")).toEqual([
        "FOREIGN KEY (game_id) REFERENCES games(game_id) ON DELETE CASCADE"
      ])
      expect(shape("turns", "f")).toEqual([
        "FOREIGN KEY (game_id) REFERENCES games(game_id) ON DELETE CASCADE"
      ])
      expect(shape("agent_stats", "f")).toEqual([
        "FOREIGN KEY (game_id) REFERENCES games(game_id) ON DELETE CASCADE"
      ])
      expect(shape("board_state", "f")).toEqual([
        "FOREIGN KEY (game_id, turn) REFERENCES turns(game_id, turn) ON DELETE CASCADE"
      ])
      expect(shape("player_turns", "f")).toEqual([
        "FOREIGN KEY (game_id, turn) REFERENCES turns(game_id, turn) ON DELETE CASCADE"
      ])
      // No CHECK constraint models a document rule: the gates are `document_status`
      // values decided once at ingest, not something the database re-derives.
      expect(keys.filter((row) => row.contype === "c")).toEqual([])
    })))
})

// -------------------------------------------------------------- the enums ---

describe("enum columns", () => {
  it("store the seven run states, including the 'invalid' sentinel, and refuse the derived ones", () =>
    run(Effect.gen(function*() {
      const db = yield* migrated

      // `invalid` is written for two different reasons — a genuinely invalid run,
      // and a manifest whose `state` is not one of the seven (17, a 10 000-char
      // string, `null`, absent). The raw value then lives in `extras.manifest.state`,
      // so a domain query reads the pair, never the column alone.
      yield* db.insert(games).values({ ...gameRow("g_lobby"), state: "lobby" })
      yield* db.insert(games).values({ ...gameRow("g_done"), state: "completed" })
      yield* db.insert(games).values({
        ...gameRow("g_sentinel"),
        state: "invalid",
        extras: { manifest: { state: 17 } }
      })

      const rows = yield* db.select().from(games).orderBy(games.gameId)
      expect(rows.map((row) => row.state)).toEqual(["completed", "lobby", "invalid"])
      expect(rows[2]?.extras).toEqual({ manifest: { state: 17 } })

      // `interrupted` and `unknown` are serving-time labels derived from live and
      // terminal context. Storing one would freeze a judgement that depends on
      // which games are running *now*.
      const client = yield* SqlClient.SqlClient
      expect(yield* isFailure(client.unsafe(`update games set state = 'interrupted'`))).toBe(true)
      expect(yield* isFailure(client.unsafe(`update games set state = 'unknown'`))).toBe(true)
    })))

  it("store a seat kind of agent or cpu, nullable, and refuse the controller spellings", () =>
    run(Effect.gen(function*() {
      const db = yield* migrated
      yield* seedGame(db)

      yield* db.insert(seats).values([
        { gameId: GAME_ID, seatIndex: 0, seatId: "place-1", kind: "agent", label: "claude-opus-5" },
        { gameId: GAME_ID, seatIndex: 1, seatId: "place-2", kind: "cpu", label: null },
        // NULL is "undeterminable" — the manifest was not `ok`, or `config.seats`
        // was not an array of objects. It is not a third kind.
        { gameId: GAME_ID, seatIndex: 2, seatId: "place-3", kind: null }
      ])

      const rows = yield* db.select().from(seats).orderBy(seats.seatIndex)
      expect(rows.map((row) => row.kind)).toEqual(["agent", "cpu", null])
      // `label` is the raw `controller_label`, never `publicPlaces`' default: the
      // projection applies `orDefault('Freeciv Classic AI')` at serve time and
      // would otherwise apply it twice.
      expect(rows[1]?.label).toBeNull()

      // The wire spellings a seat carries — `native`, `native_classic_ai`,
      // `external` — are not seat kinds. They stay verbatim in `config` and in
      // `extras.manifest.resolved_places`; `kind` is the decoded fact.
      const client = yield* SqlClient.SqlClient
      const refuses = (value: string) =>
        isFailure(client.unsafe(`update seats set kind = '${value}' where seat_index = 0`))
      expect(yield* refuses("native")).toBe(true)
      expect(yield* refuses("native_classic_ai")).toBe(true)
      expect(yield* refuses("external")).toBe(true)
      expect(yield* refuses("Agent")).toBe(true)
    })))

  it("gate each document independently — absent is 404, unusable is 503", () =>
    run(Effect.gen(function*() {
      const db = yield* migrated

      // The common shape of a live run: a manifest that parsed, no report yet.
      yield* db.insert(games).values({
        ...gameRow("g_live"),
        state: "running",
        manifestStatus: "ok",
        manifestByteSize: 4_096,
        reportStatus: "absent",
        reportByteSize: null
      })
      // A 3 GiB manifest: `unusable`, carrying the fstat size that failed the
      // gate, not the number of bytes anyone read.
      yield* db.insert(games).values({
        ...gameRow("g_huge"),
        manifestStatus: "unusable",
        manifestByteSize: 3_221_225_472,
        reportStatus: "absent"
      })

      const rows = yield* db.select().from(games).orderBy(games.gameId)
      expect(rows.map((row) => [row.manifestStatus, row.reportStatus])).toEqual([
        ["unusable", "absent"],
        ["ok", "absent"]
      ])
      expect(rows[0]?.manifestByteSize).toBe(3_221_225_472)
      // int8 comes back as `bigint` under PGlite and as `string` under `pg`;
      // `{ mode: "number" }` pins both to a plain number, so the hermetic tests
      // predict live behaviour and nothing throws inside JSON.stringify.
      expect(typeof rows[0]?.manifestByteSize).toBe("number")
      expect(rows[1]?.reportByteSize).toBeNull()
    })))
})

// ----------------------------------------------------------- what jsonb is ---

describe("jsonb columns", () => {
  it("round-trip every JSON shape a config or an extras envelope can hold", () =>
    run(Effect.gen(function*() {
      const db = yield* migrated

      const config = {
        mode: "benchmark",
        turns: 200,
        seats: [
          { id: "place-1", type: "external", model: "claude-opus-5", options: { temperature: 0.7 } },
          { id: "place-2", type: "native" }
        ],
        action_timeout_s: 12.5,
        nested: { deep: { deeper: [null, true, false, "", 0, -1, 1e-7] } }
      }
      const extras = {
        manifest: { game_id: "g_json", error: null, invalid_reasons: ["a", "b"], returncode: -9 },
        report: { score: { final_turn: 0, players: [] } },
        derived: { last_replay_turn: "99999999999999999999", manifest_negative_zero: ["/created_at"] }
      }

      yield* db.insert(games).values({ ...gameRow("g_json"), config, extras })
      const rows = yield* db.select().from(games)
      expect(rows[0]?.config).toEqual(config)
      expect(rows[0]?.extras).toEqual(extras)

      // A non-object `config` stores fine, which matters because the manifest is
      // untrusted: `diskGameRow` drops a run whose config is not an object, and
      // that judgement belongs to the projection, not to the column.
      yield* db.insert(games).values({ ...gameRow("g_str"), config: "not an object" })
      yield* db.insert(games).values({ ...gameRow("g_arr"), config: [1, 2, 3] })
      yield* db.insert(games).values({ ...gameRow("g_num"), config: 7 })
      yield* db.insert(games).values({ ...gameRow("g_bool"), config: false })
      const odd = yield* db.select({ gameId: games.gameId, config: games.config }).from(games)
        .where(eq(games.state, "invalid"))
      const configOf = (gameId: string) => odd.find((row) => row.gameId === gameId)?.config
      expect(configOf("g_str")).toBe("not an object")
      expect(configOf("g_arr")).toEqual([1, 2, 3])
      expect(configOf("g_num")).toBe(7)
      expect(configOf("g_bool")).toBe(false)
    })))

  it("does not preserve key order, and collapses duplicate keys to the last one", () =>
    run(Effect.gen(function*() {
      const db = yield* migrated
      yield* db.insert(games).values({
        ...gameRow("g_order"),
        extras: { zebra: 1, a: 2, mm: 3, b: 4 }
      })

      const rows = yield* db.select({ extras: games.extras }).from(games)
      // Measured: jsonb sorts keys by (length, bytes). This is safe *only*
      // because `canonicalBytes` sorts keys by code point before hashing, so no
      // response body ever depends on a manifest's key order. It is pinned here
      // because a future "store the document as text" idea would depend on it.
      expect(Object.keys(asJsonObject(rows[0]?.extras))).toEqual(["a", "b", "mm", "zebra"])

      const client = yield* SqlClient.SqlClient
      const duplicated = yield* client.unsafe<{ t: string }>(
        `select '{"a": 1, "a": 2}'::jsonb::text as t`
      )
      // JSON.parse keeps the last duplicate too, so ingest and Postgres agree —
      // but only because the value reaches jsonb having already been parsed.
      expect(duplicated[0]?.t).toBe(`{"a": 2}`)
    })))

  it("preserves doubles exactly, and loses only the int-vs-float spelling", () =>
    run(Effect.gen(function*() {
      const db = yield* migrated
      yield* db.insert(games).values({
        ...gameRow("g_num"),
        config: {
          epoch: 1785660947.0197709,
          tenth: 0.1,
          denormal: 1e-320,
          max: 1.7976931348623157e308,
          whole: 1.0
        }
      })

      const rows = yield* db.select({ config: games.config }).from(games)
      const value = asNumberObject(rows[0]?.config)
      expect(value.epoch).toBe(1785660947.0197709)
      expect(value.tenth).toBe(0.1)
      expect(value.denormal).toBe(1e-320)
      expect(value.max).toBe(1.7976931348623157e308)

      // `1.0` arrives as the JS number 1 and leaves as the JS number 1 — the
      // float spelling was already gone before Postgres saw it, because the
      // manifest is decoded with `JSON.parse` under `Schema.JsonNumber`. That is
      // why manifest and report reconstruct as *plain numbers*: the fs backend's
      // `readManifest` answers a `JsonObject` with no bigints in it either.
      expect(value.whole).toBe(1)
      expect(Number.isInteger(value.whole)).toBe(true)

      // Postgres itself keeps the trailing zero in the stored text, so the
      // information is not destroyed by jsonb — it is destroyed by JSON.parse,
      // on both legs equally.
      const client = yield* SqlClient.SqlClient
      const spelled = yield* client.unsafe<{ t: string }>(
        `select '{"whole": 1.0, "exp": 1e2}'::jsonb::text as t`
      )
      expect(spelled[0]?.t).toBe(`{"exp": 100, "whole": 1.0}`)
    })))

  it("refuses U+0000 and an unpaired surrogate — the two documents a run may legally carry", () =>
    run(Effect.gen(function*() {
      const db = yield* migrated
      yield* seedGame(db)
      const config = (value: unknown) =>
        db.update(games).set({ config: value }).where(eq(games.gameId, GAME_ID))
      const ruleset = (value: string) =>
        db.update(games).set({ ruleset: value }).where(eq(games.gameId, GAME_ID))

      // R1: a manifest may legally contain U+0000 (the JSON escape \u0000) and
      // `JSON.parse` accepts it — and jsonb cannot store it. There is no
      // bytes-shaped carrier in the frozen schema, so ingest must make a
      // decision about such a run; it may not strip the NUL, because that
      // silently changes `resolved_places` and the derivation places digest.
      expect(yield* isFailure(config({ a: "x\u0000y" }))).toBe(true)
      expect(yield* isFailure(config({ "k\u0000": 1 }))).toBe(true)
      // ...and `text` refuses it just as hard, so the lifted config columns are
      // not an escape hatch either.
      expect(yield* isFailure(ruleset("x\u0000y"))).toBe(true)

      // R2: an unpaired surrogate is the same class, and jsonb refuses it too.
      expect(yield* isFailure(config({ a: "\ud800" }))).toBe(true)

      // But `text` does *not* refuse it: the driver encodes the lone surrogate as
      // U+FFFD and the value comes back silently corrupted. That is worse than
      // the jsonb error, and it is the reason the lifted columns are write-only
      // query projections that no response body may read.
      yield* ruleset("\ud800")
      const rows = yield* db.select({ ruleset: games.ruleset }).from(games)
      expect(rows[0]?.ruleset).not.toBe("\ud800")
      expect(rows[0]?.ruleset).toBe("�")
    })))

  it("stores a board and a per-turn research object under the composite turn key", () =>
    run(Effect.gen(function*() {
      const db = yield* migrated
      yield* seedTurn(db, GAME_ID, 12)

      const board = { tiles: [[0, 1], [2, 3]], units: [{ id: 7, x: 1, y: 2 }], cities: [] }
      yield* db.insert(boardState).values({ gameId: GAME_ID, turn: 12, board })
      yield* db.insert(playerTurns).values({
        gameId: GAME_ID,
        turn: 12,
        seatId: "place-1",
        knownTechIds: [1, 2, 3, 55],
        research: { researching: "Bronze Working", bulbs: 42 }
      })

      const boards = yield* db.select().from(boardState)
      expect(boards[0]?.board).toEqual(board)
      const players = yield* db.select().from(playerTurns)
      expect(players[0]?.knownTechIds).toEqual([1, 2, 3, 55])
      expect(players[0]?.research).toEqual({ researching: "Bronze Working", bulbs: 42 })

      // `board` is the only NOT NULL jsonb: there are no sentinel board rows. A
      // turn whose autosave is missing gets no row and is retried next sweep.
      const client = yield* SqlClient.SqlClient
      expect(
        yield* isFailure(client.unsafe(
          `insert into board_state (game_id, turn, board) values ('${GAME_ID}', 12, null)`
        ))
      ).toBe(true)
    })))
})

// ------------------------------------------------------- numeric behaviour ---

describe("numeric columns", () => {
  it("refuse an integer past int32 — the demotion rule's reason for existing", () =>
    run(Effect.gen(function*() {
      const db = yield* migrated
      const client = yield* SqlClient.SqlClient

      yield* db.insert(games).values({
        ...gameRow("g_edge"),
        currentTurn: 2_147_483_647,
        schemaVersion: -2_147_483_648,
        lastReplayTurn: 2_147_483_647
      })
      const rows = yield* db.select().from(games)
      expect(rows[0]?.currentTurn).toBe(2_147_483_647)
      expect(rows[0]?.schemaVersion).toBe(-2_147_483_648)

      // `current_turn: 99999999999999999999` is a real corpus case. Without the
      // demotion rule the INSERT throws, the run gets no row at all, and the pg
      // gateway 404s a game the fs gateway serves 200 — the worst failure shape,
      // because it looks like a missing game rather than a bad field.
      const refuses = (column: string, value: string) =>
        isFailure(client.unsafe(`update games set ${column} = ${value} where game_id = 'g_edge'`))
      expect(yield* refuses("current_turn", "99999999999999999999")).toBe(true)
      expect(yield* refuses("current_turn", "2147483648")).toBe(true)
      expect(yield* refuses("schema_version", "-2147483649")).toBe(true)
      expect(yield* refuses("last_replay_turn", "4294967296")).toBe(true)
      expect(yield* refuses("max_turns", "2147483648")).toBe(true)
      // A non-integer is *not* refused: Postgres rounds it. `current_turn: 5.5`
      // would be stored as 6 and served as 6 while the fs backend serves 5.5, and
      // nothing would have failed anywhere. So the demotion guard cannot be "let
      // the INSERT decide" — it is `Number.isInteger(v) && int32 range`, applied
      // in TypeScript before the value is ever bound.
      expect(yield* refuses("current_turn", "5.5")).toBe(false)
      const rounded = yield* db.select({ currentTurn: games.currentTurn }).from(games)
      expect(rounded[0]?.currentTurn).toBe(6)
    })))

  it("hand back bigint columns as numbers — silently rounding one past 2^53", () =>
    run(Effect.gen(function*() {
      const db = yield* migrated
      yield* seedTurn(db, GAME_ID, 1)
      const client = yield* SqlClient.SqlClient

      yield* db.insert(playerTurns).values({
        gameId: GAME_ID,
        turn: 1,
        seatId: "place-1",
        population: 3_221_225_472
      })
      // A population past 2^53 can be *written* — int8 holds it happily — and
      // `{ mode: "number" }` then coerces it on the way out.
      yield* client.unsafe(
        `insert into player_turns (game_id, turn, seat_id, population)
         values ('${GAME_ID}', 1, 'place-2', 9007199254740993)`
      )

      const rows = yield* db.select().from(playerTurns).orderBy(playerTurns.seatId)
      expect(rows[0]?.population).toBe(3_221_225_472)
      expect(typeof rows[0]?.population).toBe("number")

      // Measured, and worse than an error: the value comes back as a `number`,
      // not a `bigint`, so 9007199254740993 reads as 9007199254740992 with no
      // failure anywhere. `integer` at least *refuses* what it cannot hold; a
      // `bigint(mode: "number")` column launders it. Every write to `population`,
      // `input_tokens`, `output_tokens` and the two `*_byte_size` columns needs
      // its own `Number.isSafeInteger` guard at ingest, because the database
      // will not raise one.
      expect(typeof rows[1]?.population).toBe("number")
      expect(String(rows[1]?.population)).toBe("9007199254740992")
      expect(Number.isSafeInteger(rows[1]?.population)).toBe(false)

      // The raw driver is where the precision still exists, which is what makes
      // this a mapping decision rather than a storage limit.
      const raw = yield* client.unsafe<{ population: unknown }>(
        `select population from player_turns where seat_id = 'place-2'`
      )
      expect(String(raw[0]?.population)).toBe("9007199254740993")
    })))

  it("flatten negative zero, in every column a timestamp can reach", () =>
    run(Effect.gen(function*() {
      const db = yield* migrated
      yield* db.insert(games).values({
        ...gameRow("g_zero"),
        createdAt: -0,
        startedAt: 0,
        finishedAt: 1785660947.0197709
      })

      const rows = yield* db.select().from(games)
      // R3: `created_at: -0.0` canonicalises to `-0.0` on the fs leg and cannot
      // survive the round trip here — the parameter binding spells it "0" long
      // before Postgres sees it. Ingest detects it with `Object.is(v, -0)` and
      // records it in `extras.derived`; this is a declared divergence, not a bug
      // the storage layer can fix.
      expect(rows[0]?.createdAt).toBe(0)
      expect(Object.is(rows[0]?.createdAt, -0)).toBe(false)
      expect(rows[0]?.startedAt).toBe(0)
      // Doubles that are not zero survive to the last bit, which is what lets
      // `created_at` be served verbatim.
      expect(rows[0]?.finishedAt).toBe(1785660947.0197709)

      const client = yield* SqlClient.SqlClient
      const stored = yield* client.unsafe<{ t: string }>(
        `select created_at::text as t from games where game_id = 'g_zero'`
      )
      expect(stored[0]?.t).toBe("0")
    })))
})

// ----------------------------------------------- bookkeeping and cascades ---

describe("ingest bookkeeping", () => {
  it("round-trips the content hash byte for byte and stamps ingested_at", () =>
    run(Effect.gen(function*() {
      const db = yield* migrated
      const before = yield* Effect.clockWith((clock) => clock.currentTimeMillis)
      yield* db.insert(games).values({ ...gameRow(GAME_ID), contentHash: allBytes })

      const rows = yield* db.select().from(games)
      const hash = rows[0]?.contentHash
      // bytea is the *only* binary column in v2, and it carries a hash, never a
      // payload. It still has to be exact: the hash is the idempotence detector,
      // and a laundering text codec would make an unchanged sweep rewrite rows.
      expect(hash?.length).toBe(256)
      expect(Array.from(hash ?? new Uint8Array())).toEqual(Array.from(allBytes))

      const ingestedAt = rows[0]?.ingestedAt
      expect(ingestedAt).toBeInstanceOf(Date)
      expect(ingestedAt instanceof Date && ingestedAt.getTime() >= before - 1_000).toBe(true)
    })))

  it("keys a game by its directory name, so a second ingest updates rather than duplicates", () =>
    run(Effect.gen(function*() {
      const db = yield* migrated
      yield* seedGame(db)

      // The manifest's own `game_id` is a *claim* and lives in
      // `extras.manifest.game_id`; the primary key is the directory. The two can
      // disagree, and the read path re-runs the cross-check to answer 404.
      expect(yield* isFailure(seedGame(db, GAME_ID))).toBe(true)

      // Two games with identical archives hash identically, by construction, so
      // the hash may never be a key.
      yield* db.insert(games).values(gameRow("game_parity_terminal_valid_02"))
      expect((yield* db.select().from(games)).length).toBe(2)
    })))

  it("refuses an orphan child, and cascades all five children when the game goes", () =>
    run(Effect.gen(function*() {
      const db = yield* migrated

      expect(
        yield* isFailure(db.insert(seats).values({ gameId: "game_never_ingested_00", seatIndex: 0 }))
      ).toBe(true)
      expect(
        yield* isFailure(db.insert(turns).values({ gameId: "game_never_ingested_00", turn: 1 }))
      ).toBe(true)

      yield* seedTurn(db, GAME_ID, 1)
      yield* db.insert(seats).values({ gameId: GAME_ID, seatIndex: 0, seatId: "place-1", kind: "agent" })
      yield* db.insert(boardState).values({ gameId: GAME_ID, turn: 1, board: { tiles: [] } })
      yield* db.insert(playerTurns).values({ gameId: GAME_ID, turn: 1, seatId: "place-1", score: 12.5 })
      yield* db.insert(agentStats).values({
        gameId: GAME_ID,
        seatId: "place-1",
        decisions: 0,
        meanLatencyMs: 0
      })

      // Deletion detection is a hard delete — a soft `deleted_at` would add a
      // filter to every read, and a filter that can be forgotten is a byte
      // parity bug waiting to happen.
      yield* db.delete(games).where(eq(games.gameId, GAME_ID))

      expect((yield* db.select().from(seats)).length).toBe(0)
      expect((yield* db.select().from(turns)).length).toBe(0)
      expect((yield* db.select().from(boardState)).length).toBe(0)
      expect((yield* db.select().from(playerTurns)).length).toBe(0)
      expect((yield* db.select().from(agentStats)).length).toBe(0)
    })))

  it("refuses a board or a player row for a turn replay.jsonl never recorded", () =>
    run(Effect.gen(function*() {
      const db = yield* migrated
      yield* seedTurn(db, GAME_ID, 1)

      // A turn with an autosave on disk but no replay line cannot be stored. The
      // board sweep treats that as a recorded skip, not an error: `candidates`
      // is the intersection of the saves directory with the `turns` rows.
      expect(
        yield* isFailure(db.insert(boardState).values({ gameId: GAME_ID, turn: 2, board: {} }))
      ).toBe(true)
      expect(
        yield* isFailure(
          db.insert(playerTurns).values({ gameId: GAME_ID, turn: 2, seatId: "place-1" })
        )
      ).toBe(true)

      // Dropping one turn takes its board and its player rows with it, and leaves
      // the rest of the game alone.
      yield* db.insert(turns).values({ gameId: GAME_ID, turn: 2, year: -2950 })
      yield* db.insert(boardState).values([
        { gameId: GAME_ID, turn: 1, board: { t: 1 } },
        { gameId: GAME_ID, turn: 2, board: { t: 2 } }
      ])
      yield* db.delete(turns).where(eq(turns.turn, 2))

      const boards = yield* db.select().from(boardState)
      expect(boards.map((row) => row.turn)).toEqual([1])
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
