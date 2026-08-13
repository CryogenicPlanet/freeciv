/**
 * `RunsRepository`, backed by Postgres instead of by `runs_root`.
 *
 * This module satisfies the *same* `Context.Tag` the filesystem backend
 * satisfies (`@arena/harness/gateway/RunsRepository`), member for member, and
 * the contract it keeps is **byte parity**: for every fixture and every method,
 * the value this repository answers with must be indistinguishable from the
 * value `makeRunsRepository(runs_root)` answers with. `test/runs-repository-pg.test.ts`
 * runs both and asserts exactly that, with the fs implementation as the oracle.
 *
 * ## How parity is achieved: the database contributes bytes, nothing else
 *
 * Every projection — `diskGameRow`, `terminalArchiveView`, `archiveVictory`,
 * `selectFramePng`, `interruptedCandidates`, `diskRowsWithInterrupted`,
 * `gamesIndex`, `sortDiskGameRows` — is **imported from `@arena/harness`'s
 * `gateway/archive.ts` and called with the same arguments**. Not one of them is
 * re-implemented here. What changes is only where the bytes came from:
 * `readFully(fd, …)` becomes a `SELECT` of a `bytea`, and the very next line is
 * the identical `decodeUtf8 → decodeJsonValueFromString → asJsonObject`
 * pipeline the fs reader runs.
 *
 * Four temptations are therefore refused on purpose, and each one is a place
 * where SQL and the fs backend's TypeScript disagree about the same question:
 *
 * - **no `ORDER BY`** anywhere. Postgres orders `text` by collation;
 *   `sortDiskGameRows` orders by *code point* and breaks `created_at` ties with
 *   float arithmetic in which `null` and `0.0` are the same key. Sorting is done
 *   in TypeScript, over the projected rows.
 * - **no `WHERE state = …`** for `terminalOnly`. The option is applied against
 *   the *projected* row's state, which came out of `manifestState` →
 *   `publicText` → `runState`; the stored `state` column is an advisory copy
 *   that can lag a re-ingest.
 * - **no `WHERE benchmark_valid`**. The archive's `benchmarkValid` is
 *   `state === 'completed' && manifest.benchmark_valid === true`, which is not
 *   the manifest field.
 * - **no `LIMIT`**. `diskGamesIndex` has none; the cap is
 *   `interruptedCandidates`' filter, applied after projection.
 *
 * `frame_index` is not read either, even though the column exists: the index is
 * re-derived from the stored *name* with `Gateway.decodeArchivePngName`, the
 * same function the fs listing uses. A class-B column that reached a response
 * body would be a defect, and this is the one that could have.
 *
 * ## The three failure states, and where they come from
 *
 * | fs | pg | answer |
 * |---|---|---|
 * | run directory missing / symlinked / not a directory | no `runs` row | 404 `game not found` |
 * | `open(manifest.json)` failed | no `run_documents` row | 404 `game manifest not found` |
 * | not regular / size 0 / > 8 MiB / bad UTF-8 / not an object | `status = 'unusable'`, or the same gates re-applied to the stored bytes | 503 `game manifest is unavailable` |
 *
 * The size gates are re-applied here rather than trusted from ingest, because
 * `byte_size` is a *gate*, not metadata: it holds the fstat size, which a short
 * `readFully` can leave above `octet_length(bytes)`, and it is what decides the
 * `<= 0` / `> 8 MiB` 503.
 *
 * A `SqlError` is the one failure with no filesystem twin. It is classified the
 * way the fs backend classifies its own storage failures, which is not uniform
 * and should not be: `diskGamesIndex` and `lastReplayTurn` are declared
 * `E = never` in `services/runs.ts` because an unreadable `runs_root` there
 * produces an *empty listing*, not an error, and the index page still renders.
 * A `SqlError` on those two paths therefore answers empty as well, so the pg
 * gateway degrades exactly where the fs gateway degrades. A read of one named
 * artifact has no such fallback — an empty answer would be a lie about that
 * artifact — so it answers 503. There is no wire message for "the database is down", and `@arena/wire` is
 * read-only here, so the 503 carries `manifestUnavailable`, the port's existing
 * "this archive cannot be turned into a response" text, with the `SqlError` as a
 * log-only `cause`.
 *
 * ## The one thing that is not byte-identical: `TerminalArchive.runRoot`
 *
 * `runRoot` is documented in `services/runs.ts` as "the resolved run directory,
 * for the binary routes and nothing else", and no payload is derived from it —
 * but `http/routes/archive.ts` still reaches around the repository three times:
 * it lists `runRoot/watch_frames` and `runRoot/saves` with its own `readdirSync`
 * and opens each `*.map.ppm` by path. Until
 * that route takes a `BinaryArtifact` from the repository, a pg-backed
 * `runRoot` has to be a directory that really holds those files, so it points at
 * the *materialized* archive (`./materialize.ts`) rather than at `runs_root`.
 * The bytes underneath are the stored bytes, so every listing and every response
 * body is unchanged; only the path string differs, and the differential test
 * asserts equality on everything except that field.
 *
 * {@link RunsRepositoryPgApi.frameBytes} and {@link RunsRepositoryPgApi.videoBytes}
 * are the shape the refactor wants — bytes straight out of `bytea`, no file
 * anywhere. When `frameFile`/`videoFile` become `BinaryArtifact`, they become
 * one-line wrappers over these two and the staging directory disappears.
 */

import * as PgDrizzle from "@effect/sql-drizzle/Pg"
import type { SqlError } from "@effect/sql/SqlError"
import { and, eq, inArray } from "drizzle-orm"
import type { PgColumn } from "drizzle-orm/pg-core"
import * as Effect from "effect/Effect"
import * as Either from "effect/Either"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"

import {
  decodeJsonValueFromString,
  type FrameIndex,
  Gateway,
  isGameId,
  isTerminalRunState,
  type JsonArray,
  type JsonObject,
  type JsonValue,
  type WireDecodeError
} from "@arena/wire"

import {
  type ArchivePng,
  archiveVictory,
  diskGameRow,
  diskRowsWithInterrupted as relabelDiskRows,
  gamesIndex,
  interruptedCandidates,
  manifestState,
  selectFramePng,
  sortDiskGameRows,
  terminalArchiveView
} from "../../harness/src/gateway/archive.ts"
import { MAX_PROXY_JSON_BYTES } from "../../harness/src/gateway/constants.ts"
import {
  ArchiveUnavailable,
  type ArchiveUnavailableProblem,
  NOT_FOUND_PROBLEMS,
  NotFound,
  type NotFoundProblem
} from "../../harness/src/gateway/errors.ts"
import type { Canonical, Untrusted } from "../../harness/src/gateway/public.ts"
import { untrustedField } from "../../harness/src/gateway/public.ts"
import { parsePythonJson } from "../../harness/src/gateway/python-json.ts"
import {
  archiveBytes,
  type DiskGamesIndexOptions,
  REPLAY_TAIL_BYTES,
  RunsRepository,
  type RunsRepositoryApi,
  type RunsError,
  type TerminalArchive
} from "../../harness/src/gateway/services/runs.ts"

import { type Database, Materializer, type RunArchiveMaterializer } from "./materialize.ts"
import { runDocuments, runFrames, runReplayTail, runs, runVideos } from "./schema.ts"

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

const NOT_FOUND_PROBLEM_SET: ReadonlySet<string> = new Set<string>(NOT_FOUND_PROBLEMS)

const isNotFoundProblem = (problem: string): problem is NotFoundProblem =>
  NOT_FOUND_PROBLEM_SET.has(problem)

/**
 * A problem name from `archive.ts` as the 404 it is — `services/runs.ts#asNotFound`.
 *
 * Every name `terminalArchiveView` or `selectFramePng` can return is a
 * `NotFoundProblem`; the fallback is unreachable and still right if it ever is
 * not, because `notFound` is the same 404.
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
// The readers, byte for byte
// ---------------------------------------------------------------------------

const UTF8 = new TextDecoder("utf-8", { fatal: true })

/** `bytes.decode("utf-8")` — strict, so a `UnicodeError` stays a failure. */
const decodeUtf8 = (bytes: Uint8Array): Option.Option<string> =>
  Option.getRight(Either.try(() => UTF8.decode(bytes)))

/** Latin-1: the lossless byte↔code-unit mapping (`services/runs.ts#decodeLatin1`). */
const decodeLatin1 = (bytes: Uint8Array): string =>
  Array.from(bytes, (byte) => String.fromCharCode(byte)).join("")

const latin1Bytes = (line: string): Uint8Array =>
  Uint8Array.from(line, (character) => character.charCodeAt(0))

/** `json.loads`, narrowed to what `JSON.parse` accepts. */
const parseJson = (text: string): Option.Option<JsonValue> =>
  Option.getRight(decodeJsonValueFromString(text))

const isJsonArray = (value: JsonValue): value is JsonArray => Array.isArray(value)

const asJsonObject = (value: JsonValue): Option.Option<JsonObject> =>
  value !== null && typeof value === "object" && !isJsonArray(value)
    ? Option.some(value)
    : Option.none()

/** One `run_documents` row, as everything below needs it. */
interface DocumentRow {
  readonly status: "ok" | "unusable"
  readonly bytes: Uint8Array
  readonly byteSize: number
}

/** The three documents of one run, by kind. Absent key = absent file. */
type DocumentSet = ReadonlyMap<Gateway.ArchiveJsonLabel | "victory", DocumentRow>

/**
 * `_read_archive_json` (`:573`), over a stored row.
 *
 * The row's absence is the `open` failing — a **404**. A row that ingest marked
 * `unusable`, or whose stored fstat size fails the `<= 0` / `> 8 MiB` gate, or
 * whose bytes are not strict UTF-8, or which does not parse as a JSON *object*,
 * is a **503**. That is the same split, in the same order, that the fs reader
 * makes on a descriptor.
 */
const documentJson = (
  row: DocumentRow | undefined,
  label: Gateway.ArchiveJsonLabel
): Either.Either<JsonObject, RunsError> =>
  row === undefined
    ? Either.left(new NotFound({ problem: ARCHIVE_JSON_PROBLEMS[label].missing }))
    : Either.fromOption(
      row.status !== "ok" || row.byteSize <= 0 || row.byteSize > MAX_PROXY_JSON_BYTES
        ? Option.none<JsonObject>()
        : Option.flatMap(
          decodeUtf8(row.bytes),
          (text) => Option.flatMap(parseJson(text), asJsonObject)
        ),
      () => new ArchiveUnavailable({ problem: ARCHIVE_JSON_PROBLEMS[label].unusable })
    )

/**
 * `_archive_victory`'s file read (`:684`), over a stored row.
 *
 * `Path.read_text`: no `O_NOFOLLOW`, no size ceiling, and every failure —
 * missing, unreadable, unparseable — collapses to "no record" without a trace.
 * So there is no gate here beyond "the bytes decode and parse", and the value is
 * handed to the same `archiveVictory`, **unparsed by anything else**. Storing a
 * decoded `Gateway.VictoryRecord` instead would feed `relayedJson`'s int-vs-float
 * guess a different input and change the published `outcome.victory`.
 */
const victoryRecord = (
  row: DocumentRow | undefined
): Option.Option<Canonical<Gateway.MatchVictory>> =>
  archiveVictory(
    row === undefined || row.status !== "ok"
      ? Option.none()
      : Option.flatMap(decodeUtf8(row.bytes), parseJson)
  )

/** The `state in TERMINAL_STATES` gate of `_terminal_archive` (`:778`). */
const isTerminalManifest = (manifest: Untrusted): boolean =>
  isTerminalRunState(manifestState(manifest))

// ---------------------------------------------------------------------------
// The replay tail
// ---------------------------------------------------------------------------

/**
 * `bytes.splitlines()` — `\n`, `\r` and `\r\n`, and nothing else.
 *
 * Transcribed from `services/runs.ts`, which owns the original and does not
 * export it; the differential test is what pins the two to each other, fixture
 * `game_parity_torn_tail_08` in particular. Splitting in Latin-1 rather than
 * UTF-8 is load-bearing: the stored window starts at an arbitrary byte offset,
 * so its first line is routinely a torn multi-byte sequence that a UTF-8 decode
 * would reject outright.
 */
const splitLines = (bytes: Uint8Array): ReadonlyArray<string> =>
  decodeLatin1(bytes).split(/\r\n|\n|\r/)

/** `bytes.strip()` — ASCII whitespace only, which is what `if not raw.strip()` tests. */
const BLANK_LINE_RE = /^[ \t\v\f]*$/

/**
 * `_last_replay_turn` (`:1178`) over the stored window.
 *
 * Scans backwards, skipping blank and unparseable lines, and **stops at the
 * first line that parses**: a parseable row without a positive integer `turn`
 * answers "no turn" rather than continuing, which is what hides a lobby husk
 * from the index instead of resurrecting an older turn.
 *
 * `parsePythonJson`, not `JSON.parse`: CPython's `isinstance(turn, int)` refuses
 * `{"turn": 2.0}`, and only a reader that keeps `2` and `2.0` apart can refuse
 * it too.
 */
const tailTurn = (tail: Uint8Array): Option.Option<bigint> => {
  const parsed = splitLines(tail)
    .filter((line) => !BLANK_LINE_RE.test(line))
    .toReversed()
    .map((line) =>
      Option.flatMap(decodeUtf8(latin1Bytes(line)), (text) => Option.getRight(parsePythonJson(text)))
    )
    .find(Option.isSome)
  if (parsed === undefined) {
    return Option.none()
  }
  const turn = untrustedField(parsed.value, "turn")
  return typeof turn === "bigint" && turn > 0n ? Option.some(turn) : Option.none()
}

/**
 * The stored window as the fs reader would have read it.
 *
 * `byte_size` is the **whole** file's size, so `size <= 0` is `Option.none`
 * before anything is decoded, exactly as `fstat` decides it there. The window
 * itself is `[max(0, size - 65536), size)` and is stored that way; the slice
 * below is a belt-and-braces re-application for a row an over-eager ingester
 * stored whole.
 */
const replayTurnOf = (byteSize: number, tailBytes: Uint8Array): Option.Option<bigint> =>
  byteSize <= 0
    ? Option.none()
    : tailTurn(
      tailBytes.length > REPLAY_TAIL_BYTES
        ? tailBytes.subarray(tailBytes.length - REPLAY_TAIL_BYTES)
        : tailBytes
    )

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
 * cannot take a `Canonical<GameRow>` directly — which is why `services/runs.ts`
 * inlines a copy. Projecting through {@link createdAt} first removes the `null`
 * case entirely, and on a `null`-free key the two comparators are the same
 * function: equal keys fall to `compareCodePoints(right, left)` on the id, and
 * unequal keys to `right - left`. So the comparator is imported, not copied.
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

/**
 * The `RunsRepository` surface plus the two byte-returning reads the gateway
 * cannot type yet.
 */
export interface RunsRepositoryPgApi extends RunsRepositoryApi {
  /** `_archive_frame_path`'s answer as **bytes**, straight out of `bytea`. */
  readonly frameBytes: (
    archive: TerminalArchive,
    index: Option.Option<FrameIndex>
  ) => Effect.Effect<Uint8Array, RunsError>
  /** `_archive_video_path`'s answer as **bytes**. */
  readonly videoBytes: (archive: TerminalArchive) => Effect.Effect<Uint8Array, RunsError>
}

const documentSet = (
  rows: ReadonlyArray<{ kind: "manifest" | "report" | "victory"; status: "ok" | "unusable"; bytes: Uint8Array; byteSize: number }>
): DocumentSet =>
  new Map(
    rows.map((row) => [
      row.kind === "manifest" ? "game manifest" : row.kind === "report" ? "game report" : "victory",
      { status: row.status, bytes: row.bytes, byteSize: row.byteSize }
    ] as const)
  )

/** One run's rows: whether it exists at all, and its three documents. */
interface RunRecord {
  readonly present: boolean
  readonly documents: DocumentSet
}

/**
 * Build the repository over one logical `runs_root`.
 *
 * `db` and `materializer` are values, not context requirements, so every method
 * comes out with `R = never` — which is what lets this satisfy an interface
 * written for a backend that needs no environment at all. The root is the
 * materializer's, not a second parameter: the two must agree, and the cheapest
 * way to guarantee agreement is to have one of them.
 */
export const makeRunsRepositoryPg = (
  db: Database,
  materializer: RunArchiveMaterializer
): RunsRepositoryPgApi => {
  const root = materializer.options.runsRoot

  /**
   * `(runs_root, game_id) = ($1, $2)` — the composite key every table carries.
   *
   * The two columns are passed individually because drizzle brands a column
   * with its table's name, so one generic over "a table with these two columns"
   * cannot be written without a cast.
   */
  const scoped = (runsRootColumn: PgColumn, gameIdColumn: PgColumn, gameId: string) =>
    and(eq(runsRootColumn, root), eq(gameIdColumn, gameId))

  // ---- the run's documents -------------------------------------------------

  const loadRun = (gameId: string): Effect.Effect<RunRecord, SqlError> =>
    Effect.map(
      Effect.all([
        db.select({ gameId: runs.gameId }).from(runs).where(scoped(runs.runsRoot, runs.gameId, gameId)),
        db
          .select({
            kind: runDocuments.kind,
            status: runDocuments.status,
            bytes: runDocuments.bytes,
            byteSize: runDocuments.byteSize
          })
          .from(runDocuments)
          .where(scoped(runDocuments.runsRoot, runDocuments.gameId, gameId))
      ]),
      ([present, documents]) => ({
        present: present.length > 0,
        documents: documentSet(documents)
      })
    )

  /** Every run under the root, with every document, in one pair of queries. */
  const loadRoot = (): Effect.Effect<ReadonlyMap<string, RunRecord>, SqlError> =>
    Effect.map(
      Effect.all([
        db.select({ gameId: runs.gameId }).from(runs).where(eq(runs.runsRoot, root)),
        db
          .select({
            gameId: runDocuments.gameId,
            kind: runDocuments.kind,
            status: runDocuments.status,
            bytes: runDocuments.bytes,
            byteSize: runDocuments.byteSize
          })
          .from(runDocuments)
          .where(eq(runDocuments.runsRoot, root))
      ]),
      ([games, documents]) => {
        // Grouped once rather than filtered per run: the index reads every
        // document under the root, and a filter inside the map would make that
        // quadratic in the number of runs.
        const byGame = documents.reduce(
          (grouped, row) =>
            grouped.set(row.gameId, [...(grouped.get(row.gameId) ?? []), row]),
          new Map<string, ReadonlyArray<typeof documents[number]>>()
        )
        return new Map(
          games.map((game) => [
            game.gameId,
            { present: true, documents: documentSet(byGame.get(game.gameId) ?? []) }
          ] as const)
        )
      }
    )

  /**
   * `_read_manifest` (`:557`) over a loaded record.
   *
   * The 404s in the order Python raises them: an id failing `GAME_ID_RE`; a run
   * that is not there at all (which is where the symlink, parent and directory
   * refusals land, because the ingester never wrote a row for one); a manifest
   * that could not be opened; and a manifest whose `game_id` disagrees with the
   * path it was found at.
   */
  const manifestOf = (gameId: string, record: RunRecord): Either.Either<JsonObject, RunsError> =>
    !isGameId(gameId) || !record.present
      ? Either.left(gone)
      : Either.flatMap(
        documentJson(record.documents.get("game manifest"), "game manifest"),
        (value) => untrustedField(value, "game_id") === gameId ? Either.right(value) : Either.left(gone)
      )

  const readManifest = (gameId: string): Effect.Effect<JsonObject, RunsError> =>
    isGameId(gameId)
      ? Effect.flatMap(
        Effect.mapError(loadRun(gameId), unreachable("manifestUnavailable")),
        (record) => manifestOf(gameId, record)
      )
      : Effect.fail(gone)

  /**
   * `_terminal_archive` (`:773`) over a loaded record — the pure half.
   *
   * Note the order, which is Python's: manifest, then the state gate, then
   * `report.json`, and only then `victory.json`. A live run therefore answers
   * `terminal archive not found` without the report ever being looked at.
   */
  const archiveOf = (
    gameId: string,
    record: RunRecord,
    runRoot: string
  ): Either.Either<TerminalArchive, RunsError> =>
    Either.flatMap(manifestOf(gameId, record), (manifest) => {
      if (!isGameId(gameId)) {
        // Unreachable: `manifestOf` has already validated the id.
        return Either.left(gone)
      }
      return Either.flatMap(
        isTerminalManifest(manifest)
          ? documentJson(record.documents.get("game report"), "game report")
          : Either.left(new NotFound({ problem: "terminalArchiveNotFound" })),
        (report) =>
          Either.mapBoth(
            terminalArchiveView(
              gameId,
              manifest,
              report,
              victoryRecord(record.documents.get("victory"))
            ),
            {
              onLeft: asNotFound,
              onRight: (view): TerminalArchive => ({ ...view, runRoot })
            }
          )
      )
    })

  const terminalArchive = (gameId: string): Effect.Effect<TerminalArchive, RunsError> =>
    Effect.flatMap(
      Effect.mapError(loadRun(gameId), unreachable("manifestUnavailable")),
      (record) =>
        Effect.flatMap(
          // Built first, materialized second: a live run never touches the disk.
          archiveOf(gameId, record, materializer.runRoot(gameId)),
          (archive) =>
            Effect.mapBoth(materializer.ensureRunArchive(gameId), {
              onFailure: (cause): RunsError =>
                new ArchiveUnavailable({ problem: "manifestUnavailable", cause }),
              onSuccess: (runRoot): TerminalArchive => ({ ...archive, runRoot })
            })
        )
    )

  // ---- the index -----------------------------------------------------------

  /**
   * `_disk_games_index` (`:1242`).
   *
   * Every per-run failure is swallowed, including the ones raised inside
   * `_terminal_archive` — a terminal run whose report is missing or corrupt
   * loses its row entirely rather than appearing without a leaderboard. A
   * `SqlError` is the `readdir` failure: a logged warning and an empty index.
   *
   * The archive used for enrichment here is built **without** materializing:
   * only `leaderboard` and `outcome` are read from it, and an index request must
   * not write a byte to disk.
   */
  const diskGameRows = (
    indexOptions?: DiskGamesIndexOptions
  ): Effect.Effect<readonly Canonical<Gateway.GameRow>[]> =>
    Effect.map(
      Effect.orElseSucceed(
        Effect.tapError(loadRoot(), (cause) =>
          Effect.logWarning("disk games index is empty: the archive database is unreachable").pipe(
            Effect.annotateLogs({ cause: String(cause) })
          )),
        (): ReadonlyMap<string, RunRecord> => new Map()
      ),
      (records) =>
        sortDiskRows(
          [...records.entries()]
            .filter(([gameId]) => isGameId(gameId))
            .flatMap(([gameId, record]): readonly Canonical<Gateway.GameRow>[] => {
              const manifest = manifestOf(gameId, record)
              if (Either.isLeft(manifest)) {
                return []
              }
              return Option.match(diskGameRow(manifest.right), {
                onNone: (): readonly Canonical<Gateway.GameRow>[] => [],
                onSome: (row) => {
                  if (!isTerminalRunState(row.state)) {
                    return indexOptions?.terminalOnly === true ? [] : [row]
                  }
                  return Either.match(
                    archiveOf(gameId, record, materializer.runRoot(gameId)),
                    {
                      onLeft: (): readonly Canonical<Gateway.GameRow>[] => [],
                      onRight: (found) => [
                        // `Object.assign`, not a spread: a spread would drop the
                        // `CanonRecord` half of `Canonical`.
                        Object.assign({}, row, {
                          leaderboard: found.leaderboard,
                          outcome: found.outcome
                        })
                      ]
                    }
                  )
                }
              })
            })
        )
    )

  // ---- the replay tail -----------------------------------------------------

  const lastReplayTurn = (gameId: string): Effect.Effect<Option.Option<bigint>> =>
    Effect.map(
      Effect.orElseSucceed(
        db
          .select({ byteSize: runReplayTail.byteSize, tailBytes: runReplayTail.tailBytes })
          .from(runReplayTail)
          .where(scoped(runReplayTail.runsRoot, runReplayTail.gameId, gameId)),
        () => []
      ),
      (rows) =>
        Option.match(Option.fromNullable(rows[0]), {
          onNone: () => Option.none<bigint>(),
          onSome: (row) => replayTurnOf(row.byteSize, row.tailBytes)
        })
    )

  /**
   * `_disk_rows_with_interrupted` (`:1582`) — `archive.ts`'s relabelling with the
   * one input only this layer can supply.
   *
   * `interruptedCandidates` names exactly the rows that need a tail, and they are
   * fetched in **one** `game_id = ANY($2)` batch rather than N opens. The
   * relabelling itself is `archive.ts`'s function, called with its own
   * arguments; nothing about it is expressed in SQL.
   */
  const diskRowsWithInterrupted = (
    liveIds: ReadonlySet<string>
  ): Effect.Effect<readonly Canonical<Gateway.GameRow>[]> =>
    Effect.flatMap(diskGameRows(), (rows) => {
      const candidates = interruptedCandidates(rows, liveIds)
      const tails = candidates.length === 0
        ? Effect.succeed([] as ReadonlyArray<{ gameId: string; byteSize: number; tailBytes: Uint8Array }>)
        : Effect.orElseSucceed(
          db
            .select({
              gameId: runReplayTail.gameId,
              byteSize: runReplayTail.byteSize,
              tailBytes: runReplayTail.tailBytes
            })
            .from(runReplayTail)
            .where(
              and(
                eq(runReplayTail.runsRoot, root),
                inArray(runReplayTail.gameId, [...candidates])
              )
            ),
          () => []
        )
      return Effect.map(tails, (found) =>
        relabelDiskRows(
          rows,
          liveIds,
          new Map(
            found.flatMap((row) =>
              Option.match(replayTurnOf(row.byteSize, row.tailBytes), {
                onNone: (): ReadonlyArray<readonly [string, bigint]> => [],
                onSome: (turn) => [[row.gameId, turn] as const]
              })
            )
          )
        ))
    })

  // ---- binaries ------------------------------------------------------------

  /**
   * `_archive_frame_path`'s listing plus `archive.ts`'s choice.
   *
   * `frames_dir_ok` is `safeArchiveDirectory(watch_frames)` evaluated at ingest —
   * the containment check has no meaning in a database, so its *answer* is
   * stored and the same 404 comes out. The index of each PNG is re-derived from
   * its name with `Gateway.decodeArchivePngName`, not read from `frame_index`:
   * that column is advisory, and this is the one place it could have leaked into
   * a response.
   */
  const selectFrameName = (
    archive: TerminalArchive,
    index: Option.Option<FrameIndex>
  ): Effect.Effect<string, RunsError> =>
    Effect.flatMap(
      Effect.mapError(
        Effect.all([
          db
            .select({ framesDirOk: runs.framesDirOk })
            .from(runs)
            .where(scoped(runs.runsRoot, runs.gameId, archive.gameId)),
          db
            .select({ name: runFrames.name })
            .from(runFrames)
            .where(scoped(runFrames.runsRoot, runFrames.gameId, archive.gameId))
        ]),
        unreachable("manifestUnavailable")
      ),
      ([run, frames]) => {
        if (run[0]?.framesDirOk !== true) {
          return Either.left<RunsError>(new NotFound({ problem: "archiveDataNotFound" }))
        }
        const pngs: readonly ArchivePng[] = frames.flatMap((row) =>
          Either.match(Gateway.decodeArchivePngName(row.name), {
            onLeft: (): ReadonlyArray<ArchivePng> => [],
            onRight: (found) => [{ index: found, name: row.name }]
          })
        )
        return Either.mapBoth(selectFramePng(pngs, index), {
          onLeft: asNotFound,
          onRight: (png) => png.name
        })
      }
    )

  const frameBytes = (
    archive: TerminalArchive,
    index: Option.Option<FrameIndex>
  ): Effect.Effect<Uint8Array, RunsError> =>
    Effect.flatMap(selectFrameName(archive, index), (name) =>
      Effect.flatMap(
        Effect.mapError(
          db
            .select({ bytes: runFrames.bytes })
            .from(runFrames)
            .where(and(scoped(runFrames.runsRoot, runFrames.gameId, archive.gameId), eq(runFrames.name, name))),
          unreachable("manifestUnavailable")
        ),
        (rows) =>
          Option.match(Option.fromNullable(rows[0]), {
            onNone: (): Effect.Effect<Uint8Array, RunsError> =>
              // The row was listed and is gone: a concurrent re-ingest. The fs
              // backend answers the same 404 when a frame is unlinked between
              // the listing and the open.
              Effect.fail(new NotFound({ problem: "archiveFileNotFound" })),
            onSome: (row) => Effect.succeed(row.bytes)
          })
      ))

  /** `_archive_video_path` (`:1022`) — `game.mp4`: present, and non-empty. */
  const videoRow = (
    archive: TerminalArchive
  ): Effect.Effect<Uint8Array, RunsError> =>
    Effect.flatMap(
      Effect.mapError(
        db
          .select({ bytes: runVideos.bytes, byteSize: runVideos.byteSize })
          .from(runVideos)
          .where(scoped(runVideos.runsRoot, runVideos.gameId, archive.gameId)),
        unreachable("manifestUnavailable")
      ),
      (rows) => {
        const row = rows[0]
        return row === undefined || row.byteSize <= 0
          ? Either.left<RunsError>(new NotFound({ problem: "replayVideoNotFound" }))
          : Either.right(row.bytes)
      }
    )


  return {
    runsRoot: root,
    readManifest,
    decodeManifest: (gameId): Effect.Effect<Gateway.Manifest, RunsError | WireDecodeError> =>
      Effect.flatMap(readManifest(gameId), (manifest) => Gateway.decodeManifest(manifest)),
    terminalArchive,
    lastReplayTurn,
    diskGamesIndex: (indexOptions) => Effect.map(diskGameRows(indexOptions), gamesIndex),
    diskRowsWithInterrupted,
    frameBytes,
    videoBytes: videoRow,
    // `RunsRepositoryApi` takes a `BinaryArtifact`: bytes straight out of
    // `bytea`, no staged file anywhere. `archive.ts#sendArtifact` streams them
    // with the same framing the fs path gets. Staging (`stagedPath`) remains
    // only for the savegame archive the python bridge reads.
    frameFile: (archive, index) => Effect.map(frameBytes(archive, index), archiveBytes),
    videoFile: (archive) => Effect.map(videoRow(archive), archiveBytes)
  }
}

/**
 * The pg `RunsRepository`, over the materializer's logical `runs_root`.
 *
 * It takes no options, and that is the point: both roots come from
 * {@link Materializer}, which is also where the per-game lock that keeps
 * `terminalArchive`'s reconciliation single-flight lives. A lock is only a lock
 * if the derivation bridge holds the same one, so the materializer is provided
 * once and shared rather than constructed here (`materialize.ts`'s docstring).
 */
export const layer: Layer.Layer<RunsRepository, never, PgDrizzle.PgDrizzle | Materializer> =
  Layer.effect(
    RunsRepository,
    Effect.gen(function*() {
      const db = yield* PgDrizzle.PgDrizzle
      const materializer = yield* Materializer
      return makeRunsRepositoryPg(db, materializer)
    })
  )
