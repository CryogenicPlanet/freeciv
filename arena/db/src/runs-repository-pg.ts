/**
 * `RunsRepository`, backed by the v2 domain schema instead of by a blob mirror.
 *
 * This module satisfies the *same* `Context.Tag` the filesystem backend
 * satisfies (`@arena/harness/gateway/RunsRepository`), member for member, and
 * the contract it keeps is **byte parity**: for every fixture and every method,
 * the value this repository answers with must be indistinguishable from the
 * value `makeRunsRepository(runs_root)` answers with.
 * `test/runs-repository-pg.test.ts` runs both over one corpus and asserts
 * exactly that, with the fs implementation as the oracle — `runRoot` included,
 * which v1 had to exclude.
 *
 * ## Two sources, one answer
 *
 * v1 stored the bytes of every document and re-ran the fs reader over them.
 * v2 stores *rows*: `manifest.json` and `report.json` are decomposed into typed
 * columns plus an `extras` envelope at ingest, and this module puts them back
 * together (§1 of the build plan). Everything that is **not** a document —
 * `victory.json`, `watch_frames/`, `game.mp4` — is read from
 * `<runs-root>/<game_id>` **by the filesystem backend's own code**, which is
 * what lets `TerminalArchive.runRoot` be a real run directory again and deletes
 * the materializer.
 *
 * ```
 * readManifest / decodeManifest   DB  (reconstruction, §1.2)
 * terminalArchive                 DB  for manifest + report, disk for victory.json + runRoot
 * lastReplayTurn                  DB  (games.last_replay_turn, +extras.derived for int32 overflow)
 * diskGamesIndex                  DB  (+ the disk reads terminal enrichment implies)
 * diskRowsWithInterrupted         DB  (the same pure relabelling functions)
 * frameFile / videoFile           disk — delegated to `makeRunsRepository`, not re-implemented
 * ```
 *
 * ## Reconstruction, and the two columns it may not read
 *
 * A key of an `ok` document is stored **exactly once**: in its typed column when
 * the column can hold it losslessly, otherwise verbatim in `extras.manifest`
 * (the *demotion* rule). So reconstruction is "every demoted key, plus every
 * non-`NULL` column whose key was not demoted", and the three stored shapes mean
 * exactly what the plan says they mean:
 *
 * | stored | reconstructed |
 * |---|---|
 * | column `NULL`, key absent from `extras.manifest` | the key is **omitted** — the document had none |
 * | column `NULL`, key present in `extras.manifest` | `extras.manifest[key]`, verbatim (explicit `null`, wrong type, out of range) |
 * | column non-`NULL` | the column, as a plain JSON `number` / `string` / `boolean` |
 *
 * Two columns are **write-only projections** and are never read here:
 *
 * - `games.game_id` is the *directory name*; the manifest's `game_id` is a
 *   *claim*, and fixture `game_parity_wrong_id_06` exists because they can
 *   differ. The claim lives in `extras.manifest.game_id`, and {@link
 *   makeRunsRepositoryPg}'s `readManifest` re-runs the cross-check over the
 *   reconstructed document exactly as the fs one does. Reconstructing it from
 *   the primary key would serve a run the fs backend 404s.
 * - `name` / `ruleset` / `mode` / `timing_mode` / `max_turns` / `objective` are
 *   lifted from `config` for querying and have not been through `publicText`.
 *   The projections read `config` — the jsonb — and a pg-side answer built from
 *   a lifted column would be silently, differently truncated.
 *
 * `games.state` is a third: it is `NOT NULL` over a seven-value enum with an
 * `'invalid'` sentinel, so it cannot distinguish *absent* from a manifest that
 * literally said `"invalid"`. {@link reconstructManifest} therefore prefers
 * `extras.manifest.state` whenever ingest recorded it — **ingest must record it
 * whenever the key is present**, which is the one deviation this module asks of
 * the plan's §1.2 table (see `RECONSTRUCTION` below) — and falls back to the
 * column only when it is not the sentinel. That closes `manifestState`'s
 * `untrustedFieldOr(manifest, 'state', 'status')` case, the single site where a
 * *present* `null` beats `status` and an *absent* key does not.
 *
 * ## No `bigint` in a reconstructed document
 *
 * `readManifest` answers a `JsonObject`, and the fs backend produces it with
 * `JSON.parse` under `Schema.JsonNumber`: **every number is a JS `number`.**
 * `Gateway.decodeManifest` depends on it (`WireInt = BigIntFromNumber` decodes
 * *from* a number), the declared return type has no `bigint` member, and the
 * deep-equality oracle would fail on one. The only `bigint` in this module is
 * {@link RunsRepositoryApi.lastReplayTurn}'s answer, whose value came from
 * `parsePythonJson` and whose `int`-ness is what refuses `{"turn": 2.0}`.
 *
 * ## The four SQL temptations, still refused
 *
 * No `ORDER BY` (Postgres orders `text` by collation; `sortDiskGameRows` orders
 * by code point and breaks `created_at` ties with float arithmetic in which
 * `null` and `0.0` are the same key); no `WHERE state = …` for `terminalOnly`
 * (the option applies to the *projected* state, which came through
 * `manifestState → publicText → runState`, not to the stored enum); no `WHERE
 * benchmark_valid` (the archive's `benchmarkValid` is `state === 'completed' &&
 * manifest.benchmark_valid === true`); no `LIMIT`. Sorting, filtering and
 * capping happen in TypeScript, over projected rows.
 *
 * ## Failure classification
 *
 * `manifest_status` / `report_status` are `_read_archive_json`'s verdict,
 * recorded once at ingest: `absent` → 404, `unusable` → 503, `ok` → the row.
 * The size gate is **not** re-applied — with no stored bytes there is nothing to
 * re-gate, and the gate is a pure function of `(fstat size, regular, bytes)`
 * evaluated with the same constants. A `SqlError` has no filesystem twin and is
 * classified the way the fs backend classifies its own storage failures, which
 * is deliberately not uniform: `diskGamesIndex` and `diskRowsWithInterrupted`
 * are declared `E = never` because an unreadable `runs_root` there produces an
 * *empty listing*, so a `SqlError` logs a warning and answers empty; a read of
 * one named artifact answers 503 `manifestUnavailable`, the port's existing
 * "this archive cannot be turned into a response" text, with the `SqlError` as a
 * log-only `cause`.
 *
 * @module
 */

import * as PgDrizzle from "@effect/sql-drizzle/Pg"
import type { SqlError } from "@effect/sql/SqlError"
import { eq, inArray } from "drizzle-orm"
import * as Effect from "effect/Effect"
import * as Either from "effect/Either"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import { closeSync, fstatSync, openSync, readSync, realpathSync } from "node:fs"
import { join } from "node:path"

import {
  decodeJsonValueFromString,
  Gateway,
  isGameId,
  isTerminalRunState,
  type JsonArray,
  type JsonObject,
  type JsonValue,
  type WireDecodeError
} from "@arena/wire"

import {
  archiveVictory,
  diskGameRow,
  diskRowsWithInterrupted as relabelDiskRows,
  gamesIndex,
  interruptedCandidates,
  manifestState,
  sortDiskGameRows,
  terminalArchiveView
} from "../../harness/src/gateway/archive.ts"
import {
  ArchiveUnavailable,
  type ArchiveUnavailableProblem,
  NOT_FOUND_PROBLEMS,
  NotFound,
  type NotFoundProblem
} from "../../harness/src/gateway/errors.ts"
import type { Canonical, Untrusted } from "../../harness/src/gateway/public.ts"
import { untrustedField } from "../../harness/src/gateway/public.ts"
import {
  type DiskGamesIndexOptions,
  makeRunsRepository,
  O_CLOEXEC,
  O_RDONLY,
  RunsRepository,
  type RunsRepositoryApi,
  type RunsError,
  type TerminalArchive,
  VICTORY_FILE
} from "../../harness/src/gateway/services/runs.ts"

import { games } from "./schema.ts"

/** The drizzle handle every query below runs on. */
export type Database = PgDrizzle.PgDrizzle["Type"]

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

const NOT_FOUND_PROBLEM_SET: ReadonlySet<string> = new Set<string>(NOT_FOUND_PROBLEMS)

const isNotFoundProblem = (problem: string): problem is NotFoundProblem =>
  NOT_FOUND_PROBLEM_SET.has(problem)

/**
 * A problem name from `archive.ts` as the 404 it is — `services/runs.ts#asNotFound`.
 *
 * Every name `terminalArchiveView` can return is a `NotFoundProblem`; the
 * fallback is unreachable and still right if it ever is not, because `notFound`
 * is the same 404.
 */
const asNotFound = (problem: Gateway.GatewayProblemName): NotFound =>
  new NotFound({ problem: isNotFoundProblem(problem) ? problem : "notFound" })

const gone: RunsError = new NotFound({ problem: "gameNotFound" })

/**
 * A database failure on a single-artifact read.
 *
 * 503 is the honest status — the artifact may well exist and simply cannot be
 * served — and `manifestUnavailable` is the message the port already owns for
 * exactly that. The `SqlError` travels as `cause`, which `http/respond.ts` logs
 * and never renders.
 */
const unreachable = (problem: ArchiveUnavailableProblem) => (cause: SqlError): RunsError =>
  new ArchiveUnavailable({ problem, cause })

/** The two `_read_archive_json` labels, and the pair of failures each names. */
const ARCHIVE_JSON_PROBLEMS: {
  readonly [L in Gateway.ArchiveJsonLabel]: {
    readonly missing: NotFoundProblem
    readonly unusable: ArchiveUnavailableProblem
  }
} = {
  "game manifest": { missing: "manifestNotFound", unusable: "manifestUnavailable" },
  "game report": { missing: "reportNotFound", unusable: "reportUnavailable" }
}

// ---------------------------------------------------------------------------
// JSON, as it comes back out of jsonb
// ---------------------------------------------------------------------------

/**
 * Whether a driver value is spellable as JSON — recursively, and refusing the
 * three things a `JsonValue` cannot be.
 *
 * The driver hands `jsonb` back as `JSON.parse`'s output, so this is a
 * *narrowing* rather than a validation in practice. It is written out anyway
 * because the alternative is a cast: `NaN`/`Infinity` (which no JSON document
 * and no `jsonb` can hold), a `Date` a future column type might produce, and an
 * `undefined` from a misspelled projection all fail here instead of reaching the
 * canonical writer, which would refuse them one layer further from the cause.
 */
const isJsonValue = (value: unknown): value is JsonValue =>
  value === null ||
  typeof value === "string" ||
  typeof value === "boolean" ||
  (typeof value === "number" && Number.isFinite(value)) ||
  (Array.isArray(value) && value.every(isJsonValue)) ||
  (typeof value === "object" && value !== null && Object.values(value).every(isJsonValue))

// `Array.isArray` does not narrow a `ReadonlyArray` member out of a union in its
// false branch; a declared guard does.
const isJsonArray = (value: JsonValue): value is JsonArray => Array.isArray(value)

const asJsonObject = (value: unknown): Option.Option<JsonObject> =>
  isJsonValue(value) && value !== null && typeof value === "object" && !isJsonArray(value)
    ? Option.some(value)
    : Option.none()

// ---------------------------------------------------------------------------
// The stored row, and the documents it reconstructs into
// ---------------------------------------------------------------------------

/** `extras`' three reserved keys — our own envelope, not the document's. */
const EXTRAS_MANIFEST = "manifest"
const EXTRAS_REPORT = "report"
const EXTRAS_DERIVED = "derived"

/** `extras.derived.last_replay_turn` — the decimal string of an int32 overflow. */
const DERIVED_LAST_REPLAY_TURN = "last_replay_turn"

/** `games.state`'s sentinel for "not one of the seven spellings" (§1.2). */
const INVALID_STATE = "invalid"

/**
 * The `games` columns reconstruction reads. Every other column is either
 * ingest bookkeeping or a write-only query projection.
 *
 * Declared as an interface rather than inferred from the drizzle projection so
 * that {@link reconstructManifest} and {@link reconstructReport} are pure,
 * testable without a database, and cannot silently start reading a column the
 * plan forbids.
 */
export interface GameDocumentRow {
  readonly gameId: string
  /** Write-only projection **except** as {@link reconstructManifest}'s fallback. */
  readonly state: string
  readonly schemaVersion: number | null
  readonly createdAt: number | null
  readonly startedAt: number | null
  readonly finishedAt: number | null
  readonly currentTurn: number | null
  readonly lastReplayTurn: number | null
  readonly benchmarkValid: boolean | null
  readonly config: unknown
  readonly manifestStatus: "ok" | "unusable" | "absent"
  readonly reportStatus: "ok" | "unusable" | "absent"
  readonly extras: unknown
}

/**
 * One `extras` section, or `Option.none` when the envelope is not one.
 *
 * A missing section is an **empty** section: a manifest all of whose keys took
 * a column demotes nothing, and ingest is not required to write an empty object
 * for it. An `extras` that is not an object, or a section that is not an object,
 * is an ingest defect and answers `Option.none` — which the callers turn into
 * the 503 the row deserves rather than into a silently short document.
 */
const extrasSection = (extras: unknown, section: string): Option.Option<JsonObject> => {
  if (extras === null || extras === undefined) {
    return Option.some({})
  }
  return Option.flatMap(asJsonObject(extras), (envelope) =>
    Object.hasOwn(envelope, section) ? asJsonObject(envelope[section]) : Option.some({}))
}

type Entries = ReadonlyArray<readonly [string, JsonValue]>

/** A typed column as its manifest key, or nothing when the column is `NULL`. */
const columnEntry = (key: string, value: JsonValue | null): Entries =>
  value === null ? [] : [[key, value]]

/**
 * `state`, from the only two places that can hold it.
 *
 * `extras.manifest.state` first: it is verbatim, so it round-trips a `17`, a
 * `null` and a 10 000-character string alike. The column is the fallback for a
 * row written by an ingest that columnized a recognized spelling without also
 * recording it — and the sentinel is *not* a fallback, because `'invalid'` there
 * means either "the manifest said something unrecognized" (in which case
 * `extras.manifest.state` holds it and this branch is not reached) or "the
 * manifest had no `state` at all", and emitting the sentinel for the second
 * would invent a key `manifestState` then prefers over `status`.
 */
const stateEntries = (row: GameDocumentRow, demoted: JsonObject): Entries =>
  Object.hasOwn(demoted, "state") || row.state === INVALID_STATE ? [] : [["state", row.state]]

/**
 * `config`, which is a whole jsonb rather than a scalar.
 *
 * `NULL` means either absent or explicitly `null`, and the second lives in
 * `extras.manifest.config` — so a `NULL` here contributes nothing and the
 * demoted key (if any) is what reconstruction emits.
 */
const configEntries = (config: unknown): Entries =>
  config === null || config === undefined || !isJsonValue(config) ? [] : [["config", config]]

/**
 * `manifest.json`, put back together (§1.2).
 *
 * Demoted keys win over columns unconditionally. Under the storage rule the two
 * sets are disjoint, so the filter changes nothing on a well-formed row; it is
 * there because a row that violates the rule must still answer *one* value, and
 * the verbatim one is the one that can be right for every input.
 *
 * `Option.none` is an unreadable envelope, never an absent document — the
 * document's own absence is `manifest_status`.
 */
export const reconstructManifest = (row: GameDocumentRow): Option.Option<JsonObject> =>
  Option.map(extrasSection(row.extras, EXTRAS_MANIFEST), (demoted) => {
    const columns: Entries = [
      ...stateEntries(row, demoted),
      ...columnEntry("schema_version", row.schemaVersion),
      ...columnEntry("created_at", row.createdAt),
      ...columnEntry("started_at", row.startedAt),
      ...columnEntry("finished_at", row.finishedAt),
      ...columnEntry("current_turn", row.currentTurn),
      ...columnEntry("benchmark_valid", row.benchmarkValid),
      ...configEntries(row.config)
    ]
    return Object.fromEntries([
      ...columns.filter(([key]) => !Object.hasOwn(demoted, key)),
      ...Object.entries(demoted)
    ])
  })

/**
 * `report.json`, which has no typed columns at all (§1.5).
 *
 * `agent_stats` is a queryable projection of `report.seat_stats`, in exactly the
 * relationship `games.mode` has to `config`, and rebuilding `seat_stats` from it
 * cannot reproduce `mean_latency_ms`'s int-vs-float spelling or its
 * `latency_ms` accumulator sibling. So the document is carried whole in
 * `extras.report` and this is the entire reconstruction.
 */
export const reconstructReport = (row: GameDocumentRow): Option.Option<JsonObject> =>
  Option.flatMap(
    asJsonObject(row.extras),
    (envelope) =>
      Object.hasOwn(envelope, EXTRAS_REPORT)
        ? asJsonObject(envelope[EXTRAS_REPORT])
        : Option.none()
  )

/**
 * `_last_replay_turn`'s answer, as ingest recorded it.
 *
 * The tail's `turn` is an unbounded Python `int` read through `parsePythonJson`,
 * and `games.last_replay_turn` is an `integer`; a value that does not fit is
 * demoted to `extras.derived.last_replay_turn` as a **decimal string**, which is
 * the only tagged encoding this schema admits and lives in the only section that
 * is allowed to carry one. The answer is a `bigint` either way, because that is
 * what `asInterrupted` compares against `current_turn`.
 */
const DECIMAL_RE = /^-?[0-9]+$/

export const reconstructLastReplayTurn = (row: GameDocumentRow): Option.Option<bigint> => {
  const derived = Option.flatMap(
    extrasSection(row.extras, EXTRAS_DERIVED),
    (section) => Option.fromNullable(section[DERIVED_LAST_REPLAY_TURN])
  )
  return Option.match(derived, {
    onNone: () =>
      row.lastReplayTurn === null
        ? Option.none<bigint>()
        : Option.some(BigInt(row.lastReplayTurn)),
    onSome: (value) =>
      typeof value === "string" && DECIMAL_RE.test(value)
        ? Option.some(BigInt(value))
        : Option.none<bigint>()
  })
}

// ---------------------------------------------------------------------------
// The disk half: victory.json and the run directory
//
// `services/runs.ts` owns the originals and exports neither `resolveStrictly`
// nor its `Path.read_text` reader, so both are transcribed here with their
// citations. `frameFile`/`videoFile` are *not* transcribed — they are the fs
// repository's own methods, called on a repository built over the same root.
// ---------------------------------------------------------------------------

const attempt = <A>(thunk: () => A): Option.Option<A> => Option.getRight(Either.try(thunk))

/**
 * `Path.resolve()` with `strict=False` — `services/runs.ts#resolveStrictly`.
 *
 * Resolve as far as the filesystem allows and keep the remaining components, so
 * a run directory that does not exist still resolves to a path under the root.
 */
const resolveStrictly = (root: string, name: string): string =>
  Option.getOrElse(attempt(() => realpathSync(join(root, name))), () => join(root, name))

/**
 * `read(2)` until `length` bytes are in hand or the file ends — a single
 * `readSync` may come up short, and Python's `stream.read()` does not.
 */
const readFully = (fd: number, length: number): Uint8Array => {
  const buffer = new Uint8Array(length)
  const step = (offset: number): number => {
    if (offset >= length) {
      return offset
    }
    const read = readSync(fd, buffer, offset, length - offset, offset)
    return read <= 0 ? offset : step(offset + read)
  }
  return buffer.subarray(0, step(0))
}

/**
 * `Path.read_text` — `services/runs.ts#readWholeFile`.
 *
 * No `O_NOFOLLOW`, no size ceiling, and every failure collapses to "no record".
 * `readFileSync` would be shorter and would *block* on a fifo, where the
 * descriptor-and-`fstat` shape reads zero bytes and answers nothing — the same
 * answer Python gives, without the hang.
 */
const readWholeFile = (path: string): Option.Option<Uint8Array> =>
  Option.flatMap(attempt(() => openSync(path, O_RDONLY | O_CLOEXEC)), (fd) => {
    const bytes = Option.flatMap(
      attempt(() => fstatSync(fd).size),
      (size) => attempt(() => readFully(fd, size))
    )
    Option.getOrElse(attempt(() => closeSync(fd)), () => undefined)
    return bytes
  })

const UTF8 = new TextDecoder("utf-8", { fatal: true })

/** `bytes.decode("utf-8")` — strict, so a `UnicodeError` stays a failure. */
const decodeUtf8 = (bytes: Uint8Array): Option.Option<string> =>
  Option.getRight(Either.try(() => UTF8.decode(bytes)))

/** `json.loads`, narrowed to what `JSON.parse` accepts. */
const parseJson = (text: string): Option.Option<JsonValue> =>
  Option.getRight(decodeJsonValueFromString(text))

/**
 * `_archive_victory`'s file read (`:684`), from the run directory on disk.
 *
 * `victory.json` is deliberately not stored: most games do not end cleanly yet,
 * and the schema says so. It is read here with `Path.read_text` semantics —
 * follows symlinks, no size ceiling, every failure collapses to "no record" —
 * and handed to `archiveVictory` **unparsed by anything else**, because
 * `relayedJson`'s int-vs-float guess must see the same input the fs backend
 * feeds it.
 */
const victoryRecord = (runRoot: string): Option.Option<Canonical<Gateway.MatchVictory>> =>
  archiveVictory(
    Option.flatMap(Option.flatMap(readWholeFile(join(runRoot, VICTORY_FILE)), decodeUtf8), parseJson)
  )

/** The `state in TERMINAL_STATES` gate of `_terminal_archive` (`:778`). */
const isTerminalManifest = (manifest: Untrusted): boolean =>
  isTerminalRunState(manifestState(manifest))

// ---------------------------------------------------------------------------
// Sorting
// ---------------------------------------------------------------------------

/**
 * `created_at` as the float Python sorts on (`services/runs.ts#createdAt`).
 *
 * `Canonical<GameRow>` widens the field to `number | bigint | null` even though
 * `_public_number` can only ever produce a float; anything but a `number` sorts
 * as `0.0`.
 */
const createdAt = (row: Canonical<Gateway.GameRow>): number =>
  typeof row.created_at === "number" ? row.created_at : 0

/**
 * `_disk_games_index`' ordering (`:1274`), through `archive.ts`'s own comparator.
 *
 * `sortDiskGameRows` is constrained to `created_at: number | null` and therefore
 * cannot take a `Canonical<GameRow>` directly. Projecting through
 * {@link createdAt} first removes the `null` case entirely, and on a `null`-free
 * key the two comparators are the same function: equal keys fall to
 * `compareCodePoints(right, left)` on the id, unequal keys to `right - left`.
 */
const sortDiskRows = (
  rows: readonly Canonical<Gateway.GameRow>[]
): readonly Canonical<Gateway.GameRow>[] =>
  sortDiskGameRows(
    rows.map((row) => ({ created_at: createdAt(row), game_id: row.game_id, row }))
  ).map((entry) => entry.row)

// ---------------------------------------------------------------------------
// The service
// ---------------------------------------------------------------------------

/** Exactly the columns reconstruction is allowed to see. */
const GAME_COLUMNS = {
  gameId: games.gameId,
  state: games.state,
  schemaVersion: games.schemaVersion,
  createdAt: games.createdAt,
  startedAt: games.startedAt,
  finishedAt: games.finishedAt,
  currentTurn: games.currentTurn,
  lastReplayTurn: games.lastReplayTurn,
  benchmarkValid: games.benchmarkValid,
  config: games.config,
  manifestStatus: games.manifestStatus,
  reportStatus: games.reportStatus,
  extras: games.extras
} as const

/**
 * `_read_archive_json`'s verdict (`:573`), as ingest recorded it.
 *
 * `absent` is the `open` failing — a **404**. `unusable` is a file that opened
 * and was not a non-empty regular file of at most 8 MiB of UTF-8 spelling a
 * JSON *object* — a **503**. `ok` is the document, and an envelope that cannot
 * be reassembled is the same 503, because a half-reconstructed document is a
 * wrong answer rather than a missing one.
 */
const documentJson = (
  status: "ok" | "unusable" | "absent",
  document: Option.Option<JsonObject>,
  label: Gateway.ArchiveJsonLabel
): Either.Either<JsonObject, RunsError> =>
  status === "absent"
    ? Either.left(new NotFound({ problem: ARCHIVE_JSON_PROBLEMS[label].missing }))
    : Either.fromOption(
      status === "ok" ? document : Option.none<JsonObject>(),
      () => new ArchiveUnavailable({ problem: ARCHIVE_JSON_PROBLEMS[label].unusable })
    )

/**
 * Build the repository over one `runs_root`.
 *
 * `db` is a value, not a context requirement, so every method comes out with
 * `R = never` — which is what lets this satisfy an interface written for a
 * backend that needs no environment at all. The root is resolved by the
 * filesystem repository this one delegates its binaries to, so the two cannot
 * disagree about which directory they mean.
 */
export const makeRunsRepositoryPg = (db: Database, runsRoot: string): RunsRepositoryApi => {
  const disk = makeRunsRepository(runsRoot)
  const root = disk.runsRoot
  const runRootOf = (gameId: string): string => resolveStrictly(root, gameId)

  // ---- rows ----------------------------------------------------------------

  const loadGame = (gameId: string): Effect.Effect<Option.Option<GameDocumentRow>, SqlError> =>
    Effect.map(
      db.select(GAME_COLUMNS).from(games).where(eq(games.gameId, gameId)),
      (rows) => Option.fromNullable(rows[0])
    )

  const loadGames = (): Effect.Effect<ReadonlyArray<GameDocumentRow>, SqlError> =>
    db.select(GAME_COLUMNS).from(games)

  // ---- documents -----------------------------------------------------------

  /**
   * `_read_manifest` (`:557`) over a stored row.
   *
   * The 404s in the order Python raises them: an id failing `GAME_ID_RE`; a run
   * that is not there at all — which is where the symlinked, escaping and
   * non-directory runs land, because ingest writes no row for one; a manifest
   * that could not be opened; and a manifest whose `game_id` disagrees with the
   * directory it was found in. That last check runs over the *reconstructed*
   * claim, never over the primary key.
   */
  const manifestOf = (
    gameId: string,
    row: Option.Option<GameDocumentRow>
  ): Either.Either<JsonObject, RunsError> =>
    !isGameId(gameId) || Option.isNone(row)
      ? Either.left(gone)
      : Either.flatMap(
        documentJson(row.value.manifestStatus, reconstructManifest(row.value), "game manifest"),
        (manifest) =>
          untrustedField(manifest, "game_id") === gameId ? Either.right(manifest) : Either.left(gone)
      )

  const readManifest = (gameId: string): Effect.Effect<JsonObject, RunsError> =>
    isGameId(gameId)
      ? Effect.flatMap(
        Effect.mapError(loadGame(gameId), unreachable("manifestUnavailable")),
        (row) => manifestOf(gameId, row)
      )
      : Effect.fail(gone)

  /**
   * `_terminal_archive` (`:773`) over a stored row — the pure half.
   *
   * The order is Python's: manifest, then the state gate, then `report.json`,
   * and only then `victory.json`. A live run therefore answers `terminal
   * archive not found` with the report never looked at and the disk never
   * touched.
   */
  const archiveOf = (
    gameId: string,
    row: Option.Option<GameDocumentRow>
  ): Either.Either<TerminalArchive, RunsError> =>
    Either.flatMap(manifestOf(gameId, row), (manifest) => {
      if (!isGameId(gameId) || Option.isNone(row)) {
        // Unreachable: `manifestOf` refuses both before it answers a manifest.
        return Either.left(gone)
      }
      const runRoot = runRootOf(gameId)
      return Either.flatMap(
        isTerminalManifest(manifest)
          ? documentJson(row.value.reportStatus, reconstructReport(row.value), "game report")
          : Either.left(new NotFound({ problem: "terminalArchiveNotFound" })),
        (report) =>
          Either.mapBoth(
            terminalArchiveView(gameId, manifest, report, victoryRecord(runRoot)),
            {
              onLeft: asNotFound,
              onRight: (view): TerminalArchive => ({ ...view, runRoot })
            }
          )
      )
    })

  const terminalArchive = (gameId: string): Effect.Effect<TerminalArchive, RunsError> =>
    Effect.flatMap(
      Effect.mapError(loadGame(gameId), unreachable("manifestUnavailable")),
      (row) => archiveOf(gameId, row)
    )

  // ---- the index -----------------------------------------------------------

  /**
   * `_disk_games_index` (`:1242`).
   *
   * Every per-run failure is swallowed, including the ones raised inside
   * `_terminal_archive` — a terminal run whose report is missing or corrupt
   * loses its row entirely rather than appearing without a leaderboard. A
   * `SqlError` is the `readdir` failure: a logged warning and an empty index.
   */
  const diskGameRows = (
    indexOptions?: DiskGamesIndexOptions
  ): Effect.Effect<readonly Canonical<Gateway.GameRow>[]> =>
    Effect.map(
      Effect.orElseSucceed(
        Effect.tapError(loadGames(), (cause) =>
          Effect.logWarning("disk games index is empty: the archive database is unreachable").pipe(
            Effect.annotateLogs({ cause: String(cause) })
          )),
        (): ReadonlyArray<GameDocumentRow> => []
      ),
      (rows) =>
        sortDiskRows(
          rows
            .filter((row) => isGameId(row.gameId))
            .flatMap((row): readonly Canonical<Gateway.GameRow>[] => {
              const manifest = manifestOf(row.gameId, Option.some(row))
              if (Either.isLeft(manifest)) {
                return []
              }
              return Option.match(diskGameRow(manifest.right), {
                onNone: (): readonly Canonical<Gateway.GameRow>[] => [],
                onSome: (indexRow) => {
                  if (!isTerminalRunState(indexRow.state)) {
                    return indexOptions?.terminalOnly === true ? [] : [indexRow]
                  }
                  return Either.match(archiveOf(row.gameId, Option.some(row)), {
                    onLeft: (): readonly Canonical<Gateway.GameRow>[] => [],
                    onRight: (found) => [
                      // `Object.assign`, not a spread: a spread would drop the
                      // `CanonRecord` half of `Canonical`.
                      Object.assign({}, indexRow, {
                        leaderboard: found.leaderboard,
                        outcome: found.outcome
                      })
                    ]
                  })
                }
              })
            })
        )
    )

  // ---- the replay tail -----------------------------------------------------

  const lastReplayTurn = (gameId: string): Effect.Effect<Option.Option<bigint>> =>
    Effect.map(
      Effect.orElseSucceed(loadGame(gameId), () => Option.none<GameDocumentRow>()),
      Option.flatMap(reconstructLastReplayTurn)
    )

  /**
   * `_disk_rows_with_interrupted` (`:1582`) — `archive.ts`'s relabelling with the
   * one input only this layer can supply.
   *
   * `interruptedCandidates` names exactly the rows that need a turn, and they
   * are fetched in **one** `game_id = ANY($1)` batch rather than N reads. The
   * relabelling itself is `archive.ts`'s function, called with its own
   * arguments; nothing about it is expressed in SQL. `asInterrupted` still
   * quotes the post-`max` turn and still drops a row whose answer is
   * `Option.none` — the lobby husk that must stay hidden.
   */
  const diskRowsWithInterrupted = (
    liveIds: ReadonlySet<string>
  ): Effect.Effect<readonly Canonical<Gateway.GameRow>[]> =>
    Effect.flatMap(diskGameRows(), (rows) => {
      const candidates = interruptedCandidates(rows, liveIds)
      const turns = candidates.length === 0
        ? Effect.succeed<ReadonlyArray<GameDocumentRow>>([])
        : Effect.orElseSucceed(
          db.select(GAME_COLUMNS).from(games).where(inArray(games.gameId, [...candidates])),
          (): ReadonlyArray<GameDocumentRow> => []
        )
      return Effect.map(turns, (found) =>
        relabelDiskRows(
          rows,
          liveIds,
          new Map(
            found.flatMap((row) =>
              Option.match(reconstructLastReplayTurn(row), {
                onNone: (): ReadonlyArray<readonly [string, bigint]> => [],
                onSome: (turn) => [[row.gameId, turn] as const]
              })
            )
          )
        ))
    })

  return {
    runsRoot: root,
    readManifest,
    decodeManifest: (gameId): Effect.Effect<Gateway.Manifest, RunsError | WireDecodeError> =>
      Effect.flatMap(readManifest(gameId), (manifest) => Gateway.decodeManifest(manifest)),
    terminalArchive,
    lastReplayTurn,
    diskGamesIndex: (indexOptions) => Effect.map(diskGameRows(indexOptions), gamesIndex),
    diskRowsWithInterrupted,
    // Binaries are ephemeral and regenerable, so they were never stored: these
    // are the *filesystem* repository's own methods, and they read
    // `archive.runRoot` — which §4 makes a real run directory in both backends.
    // Delegation rather than transcription is the whole point: a containment
    // check that exists twice is a containment check that drifts.
    frameFile: disk.frameFile,
    videoFile: disk.videoFile
  }
}

/**
 * The pg `RunsRepository`, over one `runs_root`.
 *
 * The root is the gateway's own `--runs-root` — the same string the filesystem
 * backend is given — because §4 resolves every run directory as
 * `<runs-root>/<game_id>` in both backends. There is no second root, no
 * materialized mirror and no `--materialize-root`.
 */
export const layer = (
  runsRoot: string
): Layer.Layer<RunsRepository, never, PgDrizzle.PgDrizzle> =>
  Layer.effect(
    RunsRepository,
    Effect.map(PgDrizzle.PgDrizzle, (db) => makeRunsRepositoryPg(db, runsRoot))
  )
