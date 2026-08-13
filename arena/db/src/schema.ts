/**
 * Arena schema v2 — a model of Freeciv games, not a mirror of a runs_root.
 *
 * A game run on disk is described by three JSON files, all written by the
 * supervisor: `manifest.json` (the run's lifecycle record — state, timestamps,
 * config, who joined) and `report.json` (end-of-run agent statistics).
 * `replay.jsonl` holds one line per turn with
 * every player's metrics. This schema decodes those into typed tables and
 * stores no artifacts and no filesystem paths: frames/video/saves are
 * ephemeral — resolved from the gateway's own --runs-root while they exist on
 * disk, regenerable from game state after that. bytea appears only in the
 * content-hash column.
 *
 * victory.json is deliberately not stored (most games do not end cleanly
 * yet); the /result route reads it from disk in both backends. Outcome
 * columns return with one migration when that changes.
 *
 * Parity note: timestamps stay double-precision epoch seconds — the
 * manifest's own spelling, served verbatim.
 */

import {
  bigint,
  boolean,
  customType,
  doublePrecision,
  foreignKey,
  integer,
  jsonb,
  pgEnum,
  pgTable,
  primaryKey,
  text,
  timestamp
} from "drizzle-orm/pg-core"

/** bytea, used exclusively for the sha256 ingest content hash. */
export const hashBytes = customType<{ data: Uint8Array; driverData: Uint8Array }>({
  dataType: () => "bytea",
  toDriver: (value) => value,
  fromDriver: (value) => value
})

/**
 * Run lifecycle, exactly the states the supervisor writes. (`interrupted` and
 * `unknown` are derived at serving time from live/terminal context and are
 * deliberately not storable.)
 */
export const runState = pgEnum("run_state", [
  "lobby",
  "starting",
  "running",
  "completed",
  "invalid",
  "failed",
  "cancelled"
])

/**
 * Whether a source document was readable at ingest.
 * absent → 404, unusable → 503, ok → its data is in the typed columns.
 */
export const documentStatus = pgEnum("document_status", ["ok", "unusable", "absent"])

/** Who controls a seat: an LLM agent, or the game's built-in AI. */
export const seatKind = pgEnum("seat_kind", ["agent", "cpu"])

/** One row per game: identity, lifecycle, setup, and ingest bookkeeping. */
export const games = pgTable("games", {
  gameId: text("game_id").primaryKey(),

  // ---- lifecycle ----
  state: runState("state").notNull(),
  schemaVersion: integer("schema_version"),
  /** Epoch seconds as doubles — the manifest's own spelling, served verbatim. */
  createdAt: doublePrecision("created_at"),
  startedAt: doublePrecision("started_at"),
  finishedAt: doublePrecision("finished_at"),
  currentTurn: integer("current_turn"),
  /**
   * Highest turn recorded in replay.jsonl. Kept as a column (not inferred from
   * player_turns) because the API's interrupted-game relabeling needs it even
   * for games whose replay lines failed to parse into rows, and it must match
   * the fs backend's tail-window read exactly.
   */
  lastReplayTurn: integer("last_replay_turn"),
  /** Served in the games index; false when the run broke benchmark rules. */
  benchmarkValid: boolean("benchmark_valid"),

  // ---- game setup, lifted from config for querying ----
  name: text("name"),
  ruleset: text("ruleset"),
  mode: text("mode"),
  timingMode: text("timing_mode"),
  maxTurns: integer("max_turns"),
  objective: text("objective"),
  /** The full config document; the columns above are its queryable projection. */
  config: jsonb("config"),

  // ---- source-document readability gates ----
  // 404/503 semantics per document: absent → 404, unusable (unreadable /
  // oversize / not JSON) → 503. Sizes are the fstat sizes the gates check.
  manifestStatus: documentStatus("manifest_status").notNull(),
  manifestByteSize: bigint("manifest_byte_size", { mode: "number" }),
  reportStatus: documentStatus("report_status").notNull(),
  reportByteSize: bigint("report_byte_size", { mode: "number" }),

  // ---- ingest metadata: "was this game already ingested" ----
  contentHash: hashBytes("content_hash").notNull(),
  ingestedAt: timestamp("ingested_at", { withTimezone: true, mode: "date" })
    .notNull()
    .defaultNow(),

  /**
   * Everything from the manifest that has no typed column (error/returncode/
   * start_count/invalid_reasons/…). Operational detail, kept for completeness,
   * never queried by the API.
   */
  extras: jsonb("extras")
})

/** One row per seat: who controlled it. */
export const seats = pgTable("seats", {
  gameId: text("game_id").notNull()
    .references(() => games.gameId, { onDelete: "cascade" }),
  /**
   * Position in the game's configured seat list (which chair — seat ids are
   * literally "place-1", "place-2"). Identity, not a result: standings live in
   * player_turns.
   */
  seatIndex: integer("seat_index").notNull(),
  seatId: text("seat_id"),
  kind: seatKind("kind"),
  /** Display/model name, e.g. "claude-code-claude-opus-5". */
  label: text("label"),
  /**
   * sha256 over the seat's identity config (id, type, model, instructions,
   * base_url, options) — "the same agent setup" across games, the join key
   * for cross-game win rates.
   */
  fingerprint: text("fingerprint"),
  metadata: jsonb("metadata")
}, (table) => [primaryKey({ columns: [table.gameId, table.seatIndex] })])

/** One row per recorded turn (replay.jsonl line). */
export const turns = pgTable("turns", {
  gameId: text("game_id").notNull()
    .references(() => games.gameId, { onDelete: "cascade" }),
  turn: integer("turn").notNull(),
  year: integer("year")
}, (table) => [primaryKey({ columns: [table.gameId, table.turn] })])

/**
 * The board at a given turn, joined 1:1 onto `turns`.
 *
 * The projection `board_from_autosave` derives (tile grid, units, cities) —
 * structured data, filled at ingest from that turn's autosave while it exists
 * on disk. With this table the database is self-sufficient for board
 * scrubbing, frame regeneration, and replay; the raw savegames remain the
 * lossless archive (disk today, object storage when hosted).
 */
export const boardState = pgTable("board_state", {
  gameId: text("game_id").notNull(),
  turn: integer("turn").notNull(),
  board: jsonb("board").notNull()
}, (table) => [
  primaryKey({ columns: [table.gameId, table.turn] }),
  foreignKey({
    columns: [table.gameId, table.turn],
    foreignColumns: [turns.gameId, turns.turn]
  }).onDelete("cascade")
])

/**
 * Per-agent, per-turn snapshot — the game's time series. Final standings are
 * a query, not a table: player_turns at `games.last_replay_turn`.
 */
export const playerTurns = pgTable("player_turns", {
  gameId: text("game_id").notNull(),
  turn: integer("turn").notNull(),
  seatId: text("seat_id").notNull(),
  playerId: integer("player_id"),
  playerName: text("player_name"),
  nation: text("nation"),
  government: text("government"),
  alive: boolean("alive"),
  score: doublePrecision("score"),
  cities: integer("cities"),
  citizens: integer("citizens"),
  population: bigint("population", { mode: "number" }),
  units: integer("units"),
  gold: doublePrecision("gold"),
  culture: doublePrecision("culture"),
  futureTechs: integer("future_techs"),
  knownTechIds: jsonb("known_tech_ids"),
  research: jsonb("research")
}, (table) => [
  primaryKey({ columns: [table.gameId, table.turn, table.seatId] }),
  foreignKey({
    columns: [table.gameId, table.turn],
    foreignColumns: [turns.gameId, turns.turn]
  }).onDelete("cascade")
])

/** Per-seat agent performance for the whole run (from report.json). */
export const agentStats = pgTable("agent_stats", {
  gameId: text("game_id").notNull()
    .references(() => games.gameId, { onDelete: "cascade" }),
  seatId: text("seat_id").notNull(),
  controllerFingerprint: text("controller_fingerprint"),
  turns: integer("turns"),
  decisions: integer("decisions"),
  fallbacks: integer("fallbacks"),
  inputTokens: bigint("input_tokens", { mode: "number" }),
  outputTokens: bigint("output_tokens", { mode: "number" }),
  meanLatencyMs: doublePrecision("mean_latency_ms")
}, (table) => [primaryKey({ columns: [table.gameId, table.seatId] })])

/** Every table, in dependency order, for schema-walking tests. */
export const arenaTables = [
  games,
  seats,
  turns,
  boardState,
  playerTurns,
  agentStats
] as const
