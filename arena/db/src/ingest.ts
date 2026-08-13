/**
 * The ingest pipeline — a `runs_root` on disk becomes rows in Postgres.
 *
 * The gateway never writes. This module is the only writer, and its job is
 * narrow and unglamorous: walk a `runs_root`, read every artifact the replay
 * gateway can ever serve, and store the bytes **verbatim** beside the handful
 * of typed columns an operator may query. Everything that shapes a response
 * body stays on the read side, in TypeScript, over the re-parsed bytes.
 *
 * ## What ingest decides, and what it deliberately does not
 *
 * Postgres has no symlinks and no `st_mode`, so four filesystem rules the fs
 * backend applies at *read* time have to be applied here instead. They are the
 * only judgements this module makes:
 *
 * | fs rule (read time) | here | observable result |
 * |---|---|---|
 * | `GAME_ID_RE` fails → 404 `gameNotFound` | entry skipped, no row | identical 404 |
 * | run directory is a symlink → 404 | skipped (`game_parity_symlink_07`) | identical 404 |
 * | resolved parent is not `runs_root` → 404 | skipped | identical 404 |
 * | not a directory → 404 | skipped | identical 404 |
 * | `archiveRegularFiles` drops symlink / non-regular / empty | those entries are not ingested | identical listing |
 * | `safeArchiveDirectory` fails → 404 `archiveDataNotFound` | `runs.frames_dir_ok` / `saves_dir_ok` | identical 404 |
 *
 * Every *other* gate stays on the read side. A manifest whose `game_id`
 * disagrees with its directory (`game_parity_wrong_id_06`) is ingested exactly
 * as it stands, because the pg `readManifest` re-runs that cross-check over the
 * stored bytes; a manifest that is not a JSON object
 * (`game_parity_malformed_05`) is ingested with `status = 'unusable'` and its
 * bytes intact, because the 503 has to stay *reproducible* rather than merely
 * asserted. Ingest is dumb on purpose: the more it decides, the more of the
 * gateway's behaviour lives in two places.
 *
 * ## Two asymmetries that are load-bearing
 *
 * `victory.json` and `replay.jsonl` are read **following symlinks**, with no
 * size ceiling and no `O_NOFOLLOW` — deliberately unlike every other read here,
 * because `readWholeFile` and `makeLastReplayTurn` in the fs backend omit it
 * too. An ingester that "hardened" them would silently change two answers.
 *
 * ## Idempotence
 *
 * `runs.content_hash` is sha256 over the canonical listing of the run's
 * artifacts — `(category, name, byte_size, status, sha256(bytes))` sorted, plus
 * the two directory booleans. Every field is length-prefixed
 * ({@link encodeFields}), because `name` is a filename off a disk this process
 * does not control and `saves/` is listed with a match-everything pattern: with
 * a separator-joined encoding, a file called
 * `x 1 whole <sha256 of x>\nsave y` reproduces the listing of a directory
 * holding `x` and `y`, and since an equal hash means "commit nothing", a
 * forged archive re-ingests as *unchanged* over a real one. A re-ingest reads
 * that one column, and when it
 * matches, the run's transaction issues **no statement that writes**: not an
 * `UPDATE`, not a no-op `INSERT ... ON CONFLICT`, not even a `SELECT ... FOR
 * UPDATE` (which would take a row lock, and a row lock is recorded in the
 * tuple's `xmax`, which assigns a transaction id). That is what makes
 * {@link IngestReport.transactionsWithWrites} — read from
 * `pg_current_xact_id_if_assigned()` inside each run's transaction — an exact
 * proof rather than a row count that happened to stay level.
 *
 * When the hash *does* differ, every child statement still carries its own
 * `WHERE ... IS DISTINCT FROM excluded....`, so changing one frame in a run of
 * 838 rewrites one row and leaves 837 TOAST chains alone.
 *
 * ## Deletion
 *
 * A sweep deletes the runs it did not see, by id, cascading to every child
 * table. A sweep that aborted — an unreadable `runs_root` — deletes nothing:
 * an unreadable root makes the fs backend answer with an empty index for one
 * request, and it must not make the pg backend drop the archive. A sweep
 * restricted with `--game` deletes only within the ids it was given, for the
 * same reason.
 *
 * Two refinements, both of which are about evidence rather than bookkeeping:
 *
 * - Deletion is **not** stamped by `sweep_id`, as the obvious design would have
 *   it. Stamping an unchanged run *is a write*, and one write per unchanged run
 *   is exactly what the idempotence contract forbids. `sweep_id` records the
 *   sweep that last **changed** the run; the delete is driven by ids.
 * - The delete phase re-reads the disk. The walk happens at the start of a
 *   sweep and the delete at the end, and in between another process — an
 *   `--game` sweep, most obviously — may ingest a run this one never saw.
 *   Measured, before the recheck: sweep A over a 40-run root, sweep B ingesting
 *   one new run 1.0 s later, and A then reporting `40 unchanged, 1 deleted` —
 *   both processes exiting `0`, and the run gone from every table while sitting
 *   on disk. So {@link staleRuns} runs *inside* the delete transaction and
 *   removes only ids that are still absent from the filesystem when it looks.
 *   That is a stronger licence than any lock, because it is the same evidence
 *   the walk itself used.
 *
 * ## Interop boundaries
 *
 * Three, all declared:
 *
 * 1. the `node:fs` section below (`Either.try` via {@link attempt}, mirroring
 *    `arena/harness/src/gateway/services/runs.ts`, which needs `open(2)` with a
 *    raw flag word, `fstat` on the descriptor it already holds, and positional
 *    reads — none of which `@effect/platform`'s `FileSystem` exposes);
 * 2. {@link decodeUtf8}, one `Either.try` around `TextDecoder.decode` with
 *    `fatal: true`, which is the *only* way that decoder reports invalid UTF-8
 *    and which has to stay reachable because it decides a 503;
 * 3. the SQL driver layer in `./client.ts`.
 *
 * Nothing else here throws or catches. `BigInt(...)` in {@link nameTurn} is not
 * a fourth: its argument is a `\d+` capture, on which it is total.
 *
 * @module
 */

import {
  decodeJsonValueFromString,
  Gateway,
  isGameId,
  type JsonObject,
  type JsonValue
} from "@arena/wire"
import * as PgDrizzle from "@effect/sql-drizzle/Pg"
import * as SqlClient from "@effect/sql/SqlClient"
import type { SqlError } from "@effect/sql/SqlError"
import { and, eq, inArray, notInArray, sql } from "drizzle-orm"
import type { PgColumn } from "drizzle-orm/pg-core"
import * as Arr from "effect/Array"
import * as Data from "effect/Data"
import * as Effect from "effect/Effect"
import * as Either from "effect/Either"
import * as Option from "effect/Option"
import { createHash } from "node:crypto"
// The two node: imports below are this module's declared filesystem interop
// boundary. `@effect/platform`'s `FileSystem` cannot express any of what the
// walk needs — `lstat` (there is no symlink probe at all), `open(2)` with a raw
// flag word for `O_NOFOLLOW`, `fstat` on the descriptor already held, or a
// positional read for the 64 KiB tail — and re-opening a path to stat it is the
// exact race the symlink checks exist to close.
// oxlint-disable-next-line effecttsgo/node-builtin-import
import {
  closeSync,
  fstatSync,
  lstatSync,
  openSync,
  readdirSync,
  readSync,
  realpathSync,
  statSync
  // oxlint-disable-next-line effecttsgo/node-builtin-import
} from "node:fs"
// oxlint-disable-next-line effecttsgo/node-builtin-import
import { dirname, join, resolve as resolvePath } from "node:path"

import { MAX_PROXY_JSON_BYTES } from "../../harness/src/gateway/constants.ts"
import {
  MANIFEST_FILE,
  REPLAY_JSONL_FILE,
  REPLAY_TAIL_BYTES,
  REPORT_FILE,
  VICTORY_FILE
} from "../../harness/src/gateway/services/runs.ts"

import {
  ingestSweeps,
  runDocuments,
  runFrames,
  runReplayTail,
  runs,
  runSaves,
  runVideos
} from "./schema.ts"

// ---------------------------------------------------------------------------
// Constants
//
// Imported, not redeclared. The writer and the reader have to agree on these
// exactly, and the only way to guarantee that is for there to be one of each.
// `runs-repository-pg.ts` already imports the same module, so this costs the
// package nothing it was not already paying.
// ---------------------------------------------------------------------------

/**
 * `manifest.json`, `report.json`, `victory.json`, `replay.jsonl`, and the
 * `[max(0, size - 65536), size)` tail window `_last_replay_turn` reads — all
 * five as the *reader* spells them (`services/runs.ts`), re-exported so a
 * caller of this package gets one spelling rather than two that must be kept
 * in step by hand.
 */
export { MANIFEST_FILE, REPLAY_JSONL_FILE, REPLAY_TAIL_BYTES, REPORT_FILE, VICTORY_FILE }

/**
 * `MAX_PROXY_JSON_BYTES` (`gateway/constants.ts`, `replay_gateway.py:55`).
 *
 * Not a cap on what may be stored: it is the *gate* that decides
 * `status = 'unusable'`, applied to the fstat size. It doubles as the read cap
 * for a document that has already failed the gate — those bytes can never be
 * parsed by the read path, so reading more of them would be pure cost.
 */
export const MAX_DOCUMENT_BYTES: number = MAX_PROXY_JSON_BYTES

/**
 * How much of a `*.map.ppm` is stored.
 *
 * 128 KiB is chosen so that no lemma is required: it strictly dominates both
 * readers' hard caps. `save_replay._ppm_players` breaks at `consumed > 64 KiB`
 * with `readline(4096)` steps, and `archivePpmPlayers` stops at the first line
 * after index 0 that does not begin with `#` within 513 lines — measured at
 * byte 214 on the corpus, 3254 bytes for an all-comment worst case.
 */
export const DEFAULT_PPM_HEAD_BYTES = 131072

/**
 * The largest single row payload ingest will store.
 *
 * `bytea` tops out at 1 GB, but the pg read path buffers a whole row where the
 * fs path streams 64 KiB at a time. Anything larger is refused, reported as an
 * oversized artifact, and left out of the run — loudly, not silently.
 */
export const DEFAULT_MAX_BINARY_ROW_BYTES = 64 * 1024 * 1024

/** darwin `O_RDONLY` / `O_NOFOLLOW` / `O_CLOEXEC` (`services/runs.ts`). */
const O_RDONLY = 0x0000_0000
const O_NOFOLLOW = 0x0000_0100
const O_CLOEXEC = 0x0100_0000

/**
 * `SAVE_NAME_RE` — `agent_eval/save_replay.py:35`. The autosaves the python
 * bridge discovers, and the only saves it ever decompresses.
 */
export const SAVE_NAME_RE: RegExp = /^turn-(\d+)-(?:auto|final)\.sav(?:\.gz|\.bz2|\.xz|\.zst)?$/

// ---------------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------------

/** Everything a sweep needs. Build one with {@link ingestOptions}. */
export interface IngestOptions {
  /** The `runs_root` to walk. Resolved with {@link resolveRunsRoot} on entry. */
  readonly runsRoot: string
  /** Restrict the sweep to these ids. Empty means "every run under the root". */
  readonly gameIds: ReadonlySet<string>
  /** Read and compare, write nothing — not even the sweep row. */
  readonly dryRun: boolean
  /** How much of a PPM to store. See {@link DEFAULT_PPM_HEAD_BYTES}. */
  readonly ppmHeadBytes: number
  /** The refusal threshold for a single stored payload. */
  readonly maxBinaryRowBytes: number
}

/** Default options for a root: whole sweep, real writes, 128 KiB PPM heads. */
export const ingestOptions = (runsRoot: string): IngestOptions => ({
  runsRoot,
  gameIds: new Set<string>(),
  dryRun: false,
  ppmHeadBytes: DEFAULT_PPM_HEAD_BYTES,
  maxBinaryRowBytes: DEFAULT_MAX_BINARY_ROW_BYTES
})

/**
 * The `runs_root` could not be listed.
 *
 * The sweep is marked `aborted` and nothing is deleted. This is the one place
 * ingest is *less* tolerant than the gateway, on purpose: the gateway answers
 * an unreadable root with an empty index for one request, while an ingester
 * that reported success on a root it never read would licence a deletion pass
 * over an archive it cannot see.
 */
export class RunsRootUnreadable extends Data.TaggedError("RunsRootUnreadable")<{
  readonly runsRoot: string
}> {}

/**
 * Everything {@link ingest} can fail with — the walk refusing, or the database.
 *
 * Named so a caller (the CLI, above all) can spell the channel once instead of
 * transcribing the union and drifting from it.
 */
export type IngestError = SqlError | RunsRootUnreadable

// ---------------------------------------------------------------------------
// Report
// ---------------------------------------------------------------------------

/** Why a directory entry produced no run. */
export type SkipReason =
  /** The name fails `GAME_ID_RE`. */
  | "notAGameId"
  /** The entry is a symlink — a run may not point out of `runs_root`. */
  | "symlink"
  /** The entry is not a directory. */
  | "notADirectory"
  /** It resolves outside `runs_root`. */
  | "escapesRunsRoot"
  /** An artifact was larger than {@link IngestOptions.maxBinaryRowBytes}. */
  | "oversizedArtifact"
  /** An artifact was listed but could not be read. */
  | "unreadableArtifact"
  /** `--game` named an id that is not a run under the root. */
  | "requestedButAbsent"
  /**
   * The database refused this run's rows. Its transaction rolled back, the run
   * kept whatever it had before, and **the sweep carried on** — the fs backend
   * answers per run, so one hostile run may not keep the rest of an archive out
   * of the database.
   */
  | "databaseRefused"

/** One directory entry, or one artifact, that did not make it into the run. */
export interface IngestSkip {
  /** The directory entry, which is a game id only when `reason` says so. */
  readonly entry: string
  readonly reason: SkipReason
  /** The artifact name, for the two artifact-scoped reasons. */
  readonly detail: Option.Option<string>
}

/**
 * Rows actually written, by table.
 *
 * "Actually" is exact: every statement returns the rows it changed, and the
 * `IS DISTINCT FROM` guards mean an unchanged row inside a changed run does not
 * count. {@link dataWrites} is the number the idempotence contract is about;
 * `sweeps` is bookkeeping and is always non-zero on a real sweep.
 */
export interface WriteCounters {
  readonly runs: number
  readonly documents: number
  readonly replayTails: number
  readonly frames: number
  readonly videos: number
  readonly saves: number
  /** Rows removed: stale children, plus whole runs deleted by the sweep. */
  readonly deletes: number
  /** `ingest_sweeps` rows opened and closed. Never part of {@link dataWrites}. */
  readonly sweeps: number
}

/** No rows written. */
export const NO_WRITES: WriteCounters = {
  runs: 0,
  documents: 0,
  replayTails: 0,
  frames: 0,
  videos: 0,
  saves: 0,
  deletes: 0,
  sweeps: 0
}

/** Add two counter sets. */
export const addWrites = (left: WriteCounters, right: WriteCounters): WriteCounters => ({
  runs: left.runs + right.runs,
  documents: left.documents + right.documents,
  replayTails: left.replayTails + right.replayTails,
  frames: left.frames + right.frames,
  videos: left.videos + right.videos,
  saves: left.saves + right.saves,
  deletes: left.deletes + right.deletes,
  sweeps: left.sweeps + right.sweeps
})

const sumWrites = (parts: ReadonlyArray<WriteCounters>): WriteCounters =>
  parts.reduce(addWrites, NO_WRITES)

/**
 * Every row written except the sweep bookkeeping — the number that must be `0`
 * when nothing under `runs_root` changed.
 */
export const dataWrites = (counters: WriteCounters): number =>
  counters.runs +
  counters.documents +
  counters.replayTails +
  counters.frames +
  counters.videos +
  counters.saves +
  counters.deletes

/** What one run's transaction did. */
export type RunOutcome = "inserted" | "updated" | "unchanged"

/** One run, as the sweep saw it. */
export interface RunResult {
  readonly gameId: string
  readonly outcome: RunOutcome
  /** Documents stored with `status = 'unusable'`, i.e. a reproducible 503. */
  readonly unusableDocuments: number
  /** `Gateway.decodeManifest` failed — advisory columns stay null. Never fatal. */
  readonly manifestUndecodable: boolean
  readonly writes: WriteCounters
}

/** The outcome of one sweep. */
export interface IngestReport {
  /** The resolved root, exactly as it is stored on every row. */
  readonly runsRoot: string
  /** `Option.none` for a dry run, which opens no sweep. */
  readonly sweepId: Option.Option<number>
  readonly dryRun: boolean
  readonly status: "complete" | "aborted"
  /** Directory entries examined. */
  readonly seen: number
  readonly runs: readonly RunResult[]
  readonly deleted: readonly string[]
  readonly skipped: readonly IngestSkip[]
  readonly writes: WriteCounters
  /**
   * Per-run transactions that assigned a transaction id, from
   * `pg_current_xact_id_if_assigned()` read inside each one. Zero is the exact
   * proof that a re-ingest of an unchanged root wrote nothing at all.
   */
  readonly transactionsWithWrites: number
}

const count = (results: readonly RunResult[], outcome: RunOutcome): number =>
  results.filter((result) => result.outcome === outcome).length

const skipLine = (skip: IngestSkip): string =>
  `    ${skip.entry}: ${skip.reason}${
    Option.match(skip.detail, { onNone: () => "", onSome: (detail) => ` (${detail})` })
  }`

/** The CLI's summary. Contains no connection string, by construction. */
export const describeReport = (report: IngestReport): string => {
  const writes = report.writes
  const lines: ReadonlyArray<string> = [
    `runs_root      ${report.runsRoot}`,
    `sweep          ${
      Option.match(report.sweepId, { onNone: () => "(dry run)", onSome: String })
    } — ${report.status}`,
    `entries seen   ${report.seen}`,
    `runs           ${count(report.runs, "inserted")} inserted, ${
      count(report.runs, "updated")
    } updated, ${count(report.runs, "unchanged")} unchanged, ${report.deleted.length} deleted`,
    `rows written   ${dataWrites(writes)} (runs ${writes.runs}, documents ${writes.documents}, ` +
    `tails ${writes.replayTails}, frames ${writes.frames}, videos ${writes.videos}, ` +
    `saves ${writes.saves}, deletes ${writes.deletes})`,
    `writing txns   ${report.transactionsWithWrites}`,
    // Called out on its own line rather than left to be counted out of the skip
    // list: a run the database refused is the one skip an operator has to act on.
    `refused runs   ${
      report.skipped.filter((skip) => skip.reason === "databaseRefused").length
    }`,
    `skipped        ${report.skipped.length}`,
    ...report.skipped.map(skipLine)
  ]
  return lines.join("\n")
}

// ---------------------------------------------------------------------------
// Filesystem interop — the only place this module reaches for `node:fs`.
//
// Transcribed from `arena/harness/src/gateway/services/runs.ts` so that the two
// walks agree byte for byte, including the failure conventions: every `OSError`
// is `false` / absent, never an exception.
// ---------------------------------------------------------------------------

/** A `node:fs` call that threw. Never escapes this module. */
class FsFailure extends Data.TaggedError("FsFailure")<{
  readonly operation: string
  readonly path: string
  readonly cause: unknown
}> {}

const attempt = <A>(
  operation: string,
  path: string,
  thunk: () => A
): Either.Either<A, FsFailure> =>
  Either.try({ try: thunk, catch: (cause) => new FsFailure({ operation, path, cause }) })

/** `Path.is_symlink()` — `False` on any `OSError`. */
const isSymlink = (path: string): boolean =>
  Either.match(attempt("lstat", path, () => lstatSync(path)), {
    onLeft: () => false,
    onRight: (info) => info.isSymbolicLink()
  })

/** `Path.is_dir()` — follows symlinks, `False` on any `OSError`. */
const isDirectory = (path: string): boolean =>
  Either.match(attempt("stat", path, () => statSync(path)), {
    onLeft: () => false,
    onRight: (info) => info.isDirectory()
  })

/** `Path.resolve()` with `strict=False`. */
const resolveStrictly = (root: string, name: string): string =>
  Either.getOrElse(
    attempt("realpath", join(root, name), () => realpathSync(join(root, name))),
    () => join(root, name)
  )

interface FileInfo {
  readonly size: number
  readonly regular: boolean
}

const statFd = (fd: number): Either.Either<FileInfo, FsFailure> =>
  Either.map(
    attempt("fstat", `fd:${String(fd)}`, () => fstatSync(fd)),
    (info) => ({ size: info.size, regular: info.isFile() })
  )

/**
 * `read(2)` until `length` bytes are in hand or the file ends — a single
 * `readSync` may come up short, and Python's `stream.read()` does not. The
 * result may therefore be shorter than `length`, which is precisely why the
 * fstat size is stored separately from `octet_length(bytes)`.
 */
const readFully = (
  fd: number,
  position: number,
  length: number
): Either.Either<Uint8Array, FsFailure> =>
  attempt("read", `fd:${String(fd)}`, () => {
    const buffer = new Uint8Array(length)
    const step = (offset: number): number => {
      if (offset >= length) {
        return offset
      }
      const read = readSync(fd, buffer, offset, length - offset, position + offset)
      return read <= 0 ? offset : step(offset + read)
    }
    return buffer.subarray(0, step(0))
  })

/** What one `open` + `fstat` + `read` produced. */
interface ReadFile {
  /** The fstat size — the gate, and the true on-disk length. */
  readonly size: number
  readonly regular: boolean
  /** What `read(2)` returned, capped at the caller's limit. */
  readonly bytes: Uint8Array
}

/**
 * Open, fstat, and read at most `cap` bytes from the start.
 *
 * `flags` is the caller's, because the asymmetry matters: `manifest.json`,
 * `report.json`, the frames, the video and the saves are opened with
 * `O_NOFOLLOW`, while `victory.json` and `replay.jsonl` are not.
 */
const readHead = (
  path: string,
  flags: number,
  cap: number
): Either.Either<ReadFile, FsFailure> =>
  Either.flatMap(attempt("open", path, () => openSync(path, flags)), (fd) => {
    const info = statFd(fd)
    const result = Either.flatMap(info, (stat) =>
      Either.map(
        readFully(fd, 0, Math.max(0, Math.min(stat.size, cap))),
        (bytes): ReadFile => ({ size: stat.size, regular: stat.regular, bytes })
      ))
    Either.getOrElse(attempt("close", path, () => closeSync(fd)), () => undefined)
    return result
  })

/** The 64 KiB byte window at the end of a file, with the whole file's size. */
const readTail = (path: string): Either.Either<ReadFile, FsFailure> =>
  Either.flatMap(
    attempt("open", path, () => openSync(path, O_RDONLY | O_CLOEXEC)),
    (fd) => {
      const result = Either.flatMap(statFd(fd), (stat) => {
        const start = Math.max(0, stat.size - REPLAY_TAIL_BYTES)
        return Either.map(
          readFully(fd, start, Math.max(0, stat.size - start)),
          (bytes): ReadFile => ({ size: stat.size, regular: stat.regular, bytes })
        )
      })
      Either.getOrElse(attempt("close", path, () => closeSync(fd)), () => undefined)
      return result
    }
  )

/**
 * `_safe_archive_directory` (`replay_gateway.py:871`) — `watch_frames/` and
 * `saves/` must exist, not be a symlink, be a directory, and resolve to a
 * direct child of the run root.
 */
const safeArchiveDirectory = (root: string, name: string): Option.Option<string> => {
  const info = attempt("lstat", join(root, name), () => lstatSync(join(root, name)))
  if (Either.isLeft(info) || info.right.isSymbolicLink() || !info.right.isDirectory()) {
    return Option.none()
  }
  const resolved = resolveStrictly(root, name)
  return dirname(resolved) === root ? Option.some(resolved) : Option.none()
}

/**
 * `_archive_regular_files` (`replay_gateway.py:885`) — names matching
 * `pattern`, each a non-empty regular file that is not a symlink. An unreadable
 * directory is an empty listing, not a failure.
 */
const archiveRegularFiles = (directory: string, pattern: RegExp): ReadonlyArray<string> =>
  Either.getOrElse(
    attempt("readdir", directory, () => readdirSync(directory)),
    (): ReadonlyArray<string> => []
  ).filter((name) => {
    if (!pattern.test(name)) {
      return false
    }
    const info = attempt("lstat", join(directory, name), () => lstatSync(join(directory, name)))
    return (
      Either.isRight(info) &&
      info.right.isFile() &&
      !info.right.isSymbolicLink() &&
      info.right.size > 0
    )
  })

/**
 * Matches every name, so {@link archiveRegularFiles} yields every regular,
 * non-symlink, non-empty entry. `saves/` is listed this way because the python
 * bridge globs the whole directory, unlike `watch_frames/`, whose listing the
 * gateway filters with `ARCHIVE_PNG_RE`.
 */
const ANY_NAME = /^/

// ---------------------------------------------------------------------------
// Hashing and JSON
// ---------------------------------------------------------------------------

const sha256 = (bytes: Uint8Array): Uint8Array =>
  Uint8Array.from(createHash("sha256").update(bytes).digest())

const hex = (bytes: Uint8Array): string =>
  Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("")

const UTF8 = new TextDecoder("utf-8", { fatal: true })

/** `bytes.decode("utf-8")` — strict, so a `UnicodeError` stays a failure. */
const decodeUtf8 = (bytes: Uint8Array): Option.Option<string> =>
  Option.getRight(Either.try(() => UTF8.decode(bytes)))

const isJsonArray = (value: JsonValue): value is ReadonlyArray<JsonValue> => Array.isArray(value)

const asJsonObject = (value: JsonValue): Option.Option<JsonObject> =>
  value !== null && typeof value === "object" && !isJsonArray(value)
    ? Option.some(value)
    : Option.none()

/**
 * `_read_archive_json`'s verdict (`replay_gateway.py:573`), as a status.
 *
 * `ok` exactly when the fs backend would have parsed the document: a non-empty
 * regular file of at most 8 MiB whose bytes decode as UTF-8 and parse as a JSON
 * *object*. Anything else is `unusable`, which the read path turns into the
 * same 503 the fs backend raises.
 */
const documentVerdict = (file: ReadFile): "ok" | "unusable" =>
  !file.regular || file.size <= 0 || file.size > MAX_DOCUMENT_BYTES
    ? "unusable"
    : Option.match(
      Option.flatMap(
        Option.flatMap(decodeUtf8(file.bytes), (text) =>
          Option.getRight(decodeJsonValueFromString(text))),
        asJsonObject
      ),
      { onNone: (): "ok" | "unusable" => "unusable", onSome: () => "ok" }
    )

// ---------------------------------------------------------------------------
// The artifacts of one run
// ---------------------------------------------------------------------------

/** One of the three archive JSON documents, verbatim. */
export interface StoredDocument {
  readonly kind: "manifest" | "report" | "victory"
  readonly status: "ok" | "unusable"
  readonly bytes: Uint8Array
  readonly byteSize: number
  readonly sha256: Uint8Array
}

/** The `replay.jsonl` byte window. */
export interface StoredTail {
  /** The **whole** file's size; `<= 0` is what makes `lastReplayTurn` answer none. */
  readonly byteSize: number
  readonly tailBytes: Uint8Array
  readonly sha256: Uint8Array
}

/** One `watch_frames/*.png`. */
export interface StoredFrame {
  readonly name: string
  readonly frameIndex: Option.Option<number>
  readonly bytes: Uint8Array
  readonly byteSize: number
  readonly sha256: Uint8Array
}

/** `game.mp4`. */
export interface StoredVideo {
  readonly bytes: Uint8Array
  readonly byteSize: number
  readonly sha256: Uint8Array
}

/** One entry of `saves/`. */
export interface StoredSave {
  readonly name: string
  readonly kind: "autosave" | "ppm" | "other"
  readonly saveTurn: Option.Option<number>
  /** The true on-disk size, even when {@link headBytes} is only a head. */
  readonly byteSize: number
  readonly headBytes: Uint8Array
  readonly isWhole: boolean
  readonly sha256: Uint8Array
}

/** The class-B advisory columns, read through `@arena/wire`'s manifest schema. */
export interface RunAdvisory {
  readonly state: string | null
  readonly createdAt: number | null
  readonly finishedAt: number | null
  readonly benchmarkValid: boolean | null
  /** The manifest failed the strict schema. Advisory only, never fatal. */
  readonly undecodable: boolean
}

const NO_ADVISORY: RunAdvisory = {
  state: null,
  createdAt: null,
  finishedAt: null,
  benchmarkValid: null,
  undecodable: false
}

/** Everything one run directory holds, ready to be written. */
export interface RunArtifacts {
  readonly gameId: string
  readonly framesDirOk: boolean
  readonly savesDirOk: boolean
  readonly documents: readonly StoredDocument[]
  readonly replayTail: Option.Option<StoredTail>
  readonly frames: readonly StoredFrame[]
  readonly video: Option.Option<StoredVideo>
  readonly saves: readonly StoredSave[]
  readonly advisory: RunAdvisory
  readonly contentHash: Uint8Array
  /** Artifacts refused or unreadable. Reported, never fatal. */
  readonly skips: readonly IngestSkip[]
}

/**
 * `manifest.json` / `report.json`.
 *
 * `open` failing is the *absence* of a row — the fs backend answers 404 for it
 * — while a file that opens and fails a gate is a row with
 * `status = 'unusable'`, which is the 503. Only those two shapes exist, which
 * is why the read side needs no third case.
 */
const readDocument = (
  runRoot: string,
  file: string,
  kind: "manifest" | "report"
): Option.Option<StoredDocument> =>
  Option.map(
    Option.getRight(
      readHead(join(runRoot, file), O_RDONLY | O_NOFOLLOW | O_CLOEXEC, MAX_DOCUMENT_BYTES)
    ),
    (read): StoredDocument => ({
      kind,
      status: documentVerdict(read),
      bytes: read.bytes,
      byteSize: read.size,
      sha256: sha256(read.bytes)
    })
  )

/**
 * `victory.json` — `Path.read_text`: follows symlinks, no `O_NOFOLLOW`, no size
 * ceiling, and every failure collapses to "no record" without a trace.
 *
 * It is stored with `status = 'ok'` unconditionally, because the read path has
 * no third answer for it: `archiveVictory` takes an `Option` and treats a
 * missing, unreadable or unparseable file identically. The only refusal is the
 * row-size ceiling, which a one-line file written by `bridge.lua` cannot reach.
 */
const readVictory = (
  runRoot: string,
  maxBytes: number
): Either.Either<Option.Option<StoredDocument>, IngestSkip> =>
  Option.match(
    Option.getRight(readHead(join(runRoot, VICTORY_FILE), O_RDONLY | O_CLOEXEC, maxBytes)),
    {
      onNone: () => Either.right(Option.none()),
      onSome: (read) =>
        read.size > maxBytes
          ? Either.left({
            entry: VICTORY_FILE,
            reason: "oversizedArtifact" as const,
            detail: Option.some(`${VICTORY_FILE} is ${read.size} bytes`)
          })
          : Either.right(
            Option.some({
              kind: "victory" as const,
              status: "ok" as const,
              bytes: read.bytes,
              byteSize: read.size,
              sha256: sha256(read.bytes)
            })
          )
    }
  )

/**
 * The `replay.jsonl` window. No `O_NOFOLLOW` — `makeLastReplayTurn` follows a
 * symlinked replay too, and hardening it here would change an answer.
 *
 * A file of size `0` still gets a row: `byte_size <= 0` is how the read path
 * says "no turn", and storing the row keeps a later append detectable by the
 * content hash.
 */
const readReplayTail = (runRoot: string): Option.Option<StoredTail> =>
  Option.map(
    Option.getRight(readTail(join(runRoot, REPLAY_JSONL_FILE))),
    (read): StoredTail => ({
      byteSize: read.size,
      tailBytes: read.bytes,
      sha256: sha256(read.bytes)
    })
  )

/** One artifact skip, for the two artifact-scoped reasons. */
const artifactSkip = (
  name: string,
  reason: "oversizedArtifact" | "unreadableArtifact",
  detail: string
): IngestSkip => ({ entry: name, reason, detail: Option.some(detail) })

/**
 * `watch_frames/*.png`.
 *
 * The listing rule is `archiveRegularFiles(dir, ARCHIVE_PNG_RE)` verbatim, so
 * a symlinked, empty or non-regular frame is absent here exactly as it is
 * absent from the fs listing. `frame_index` is `decodeArchivePngName`'s result;
 * a name that matches the pattern but fails to decode is unreachable
 * (`FrameIndex` is any non-negative integer and the pattern admits six digits),
 * and if it ever happens the row carries a null index, which the repository
 * drops from the listing exactly as `listFramePngs` drops it.
 */
const readFrames = (
  directory: string,
  maxBytes: number
): { readonly frames: readonly StoredFrame[]; readonly skips: readonly IngestSkip[] } => {
  const results = archiveRegularFiles(directory, Gateway.ARCHIVE_PNG_RE).map((name) =>
    Either.match(readHead(join(directory, name), O_RDONLY | O_NOFOLLOW | O_CLOEXEC, maxBytes), {
      onLeft: (): Either.Either<StoredFrame, IngestSkip> =>
        Either.left(artifactSkip(name, "unreadableArtifact", `watch_frames/${name}`)),
      onRight: (read) =>
        read.size > maxBytes
          ? Either.left(
            artifactSkip(name, "oversizedArtifact", `watch_frames/${name} is ${read.size} bytes`)
          )
          : Either.right({
            name,
            frameIndex: Option.getRight(Gateway.decodeArchivePngName(name)),
            bytes: read.bytes,
            byteSize: read.size,
            sha256: sha256(read.bytes)
          })
    })
  )
  return { frames: Arr.getRights(results), skips: Arr.getLefts(results) }
}

/** `game.mp4` — non-symlink, regular, non-empty, per `_archive_video_path`. */
const readVideo = (
  runRoot: string,
  maxBytes: number
): Either.Either<Option.Option<StoredVideo>, IngestSkip> => {
  const name = Gateway.ARCHIVE_VIDEO_FILE
  const path = join(runRoot, name)
  const info = attempt("lstat", path, () => lstatSync(path))
  if (
    Either.isLeft(info) ||
    info.right.isSymbolicLink() ||
    !info.right.isFile() ||
    info.right.size <= 0
  ) {
    return Either.right(Option.none())
  }
  if (info.right.size > maxBytes) {
    return Either.left(artifactSkip(name, "oversizedArtifact", `${name} is ${info.right.size} bytes`))
  }
  return Either.match(readHead(path, O_RDONLY | O_NOFOLLOW | O_CLOEXEC, maxBytes), {
    onLeft: (): Either.Either<Option.Option<StoredVideo>, IngestSkip> =>
      Either.left(artifactSkip(name, "unreadableArtifact", name)),
    onRight: (read) =>
      Either.right(
        Option.some({ bytes: read.bytes, byteSize: read.size, sha256: sha256(read.bytes) })
      )
  })
}

/**
 * `SAVE_NAME_RE` / `ARCHIVE_PPM_RE` group 1, as a turn.
 *
 * Both patterns capture `(\d+)` — *unbounded*, because a filename is whatever
 * a filesystem allowed someone to create. `Number` is therefore not a decoder
 * here but a hazard: `turn-99999999999999999999-auto.sav.gz` yields `1e20`,
 * which `bigint` refuses and which used to fail the run's `INSERT` and, before
 * per-run isolation, the whole sweep; `turn-9007199254740993-auto.sav.gz`
 * yields `9007199254740992`, which is worse, because it is stored silently.
 *
 * `save_turn` is class-B — advisory, indexed, never on a response path — so the
 * right answer for a turn no `number` can hold exactly is `NULL`. `BigInt` is
 * total on a `\d+` capture (leading zeros included), so the comparison below is
 * exact rather than a round trip through the float that is the problem.
 */
const nameTurn = (name: string, pattern: RegExp): Option.Option<number> =>
  Option.filterMap(Option.fromNullable(pattern.exec(name)?.[1]), (digits) => {
    const exact = BigInt(digits)
    return exact <= BigInt(Number.MAX_SAFE_INTEGER)
      ? Option.some(Number(exact))
      : Option.none()
  })

const saveClassification = (
  name: string
): { readonly kind: "autosave" | "ppm" | "other"; readonly turn: Option.Option<number> } =>
  SAVE_NAME_RE.test(name)
    ? { kind: "autosave", turn: nameTurn(name, SAVE_NAME_RE) }
    : Gateway.ARCHIVE_PPM_RE.test(name)
    ? { kind: "ppm", turn: nameTurn(name, Gateway.ARCHIVE_PPM_RE) }
    : { kind: "other", turn: Option.none() }

/**
 * `saves/*`.
 *
 * An autosave is stored whole, because `save_replay` decompresses it. A PPM is
 * stored as its first `ppmHeadBytes`, because no reader ever looks past its
 * header — and `is_whole` records which of the two happened, so a materialised
 * PPM's deliberate shortness is never mistaken for the real length. Everything
 * else in the directory is stored whole under `kind = 'other'`: nothing reads
 * it today, and a materialised workdir that silently lost a file would be a
 * different directory from the one the fs backend hands the bridge.
 */
const readSaves = (
  directory: string,
  ppmHeadBytes: number,
  maxBytes: number
): { readonly saves: readonly StoredSave[]; readonly skips: readonly IngestSkip[] } => {
  const results = archiveRegularFiles(directory, ANY_NAME).map((name) => {
    const classified = saveClassification(name)
    const cap = classified.kind === "ppm" ? Math.min(ppmHeadBytes, maxBytes) : maxBytes
    return Either.match(readHead(join(directory, name), O_RDONLY | O_NOFOLLOW | O_CLOEXEC, cap), {
      onLeft: (): Either.Either<StoredSave, IngestSkip> =>
        Either.left(artifactSkip(name, "unreadableArtifact", `saves/${name}`)),
      onRight: (read) =>
        classified.kind !== "ppm" && read.size > maxBytes
          ? Either.left(
            artifactSkip(name, "oversizedArtifact", `saves/${name} is ${read.size} bytes`)
          )
          : Either.right({
            name,
            kind: classified.kind,
            saveTurn: classified.turn,
            byteSize: read.size,
            headBytes: read.bytes,
            isWhole: read.bytes.length === read.size,
            sha256: sha256(read.bytes)
          })
    })
  })
  return { saves: Arr.getRights(results), skips: Arr.getLefts(results) }
}

/**
 * The class-B columns, through `@arena/wire`'s strict manifest schema.
 *
 * This is the *only* use of the schema in the ingester, and it touches nothing
 * a response body can reach: a manifest the schema rejects is still stored
 * verbatim and still serves, because the gateway's projections read a dict
 * through `publicText`/`publicInt` and coerce whatever they find. The
 * rejection is reported so an operator learns about it, and that is all.
 */
const readAdvisory = (documents: readonly StoredDocument[]): RunAdvisory => {
  const manifest = documents.find((document) => document.kind === "manifest")
  if (manifest === undefined || manifest.status !== "ok") {
    return NO_ADVISORY
  }
  const parsed = Option.flatMap(decodeUtf8(manifest.bytes), (text) =>
    Option.getRight(decodeJsonValueFromString(text)))
  return Option.match(parsed, {
    onNone: () => NO_ADVISORY,
    onSome: (value) =>
      Either.match(Gateway.decodeManifest(value), {
        onLeft: (): RunAdvisory => ({ ...NO_ADVISORY, undecodable: true }),
        onRight: (decoded) => ({
          state: String(decoded.state),
          createdAt: decoded.created_at,
          finishedAt: decoded.finished_at,
          benchmarkValid: decoded.benchmark_valid,
          undecodable: false
        })
      })
  })
}

/**
 * `content_hash` — sha256 over the canonical listing.
 *
 * Every byte, size, status, name or presence change moves it; two runs with
 * identical archives hash identically. `byte_size` is in the listing beside the
 * digest because a short `read(2)` can leave the stored bytes shorter than the
 * file, and that difference has to be a *change*.
 */
interface HashEntry {
  readonly category: string
  readonly name: string
  readonly byteSize: number
  readonly status: string
  readonly digest: Uint8Array
}

const hashEntry = (
  category: string,
  name: string,
  byteSize: number,
  status: string,
  digest: Uint8Array
): HashEntry => ({ category, name, byteSize, status, digest })

/**
 * Fields as one unambiguous string: `<length>:<field>` per field, concatenated.
 *
 * The length is in the same units the field is measured in (UTF-16 code units),
 * so the encoding is injective by construction — a reader takes the prefix,
 * consumes exactly that many units, and starts again. Nothing a field contains
 * can imitate a separator, because there is none.
 *
 * This is what makes {@link contentHash} an idempotency key rather than a
 * suggestion. With the separator-joined encoding it replaces, a `saves/` holding
 * `x` (1 byte) and `y` (1 byte) hashed *identically* to a `saves/` holding the
 * single file `x 1 whole <sha256 of x>\nsave y` with `y`'s content — and an
 * equal hash is precisely the licence {@link ingestRun} takes to write nothing
 * at all, so the forgery would have kept the real archive out of the database
 * with both processes exiting `0`.
 */
export const encodeFields = (fields: ReadonlyArray<string>): string =>
  fields.map((field) => `${String(field.length)}:${field}`).join("")

const contentHash = (
  framesDirOk: boolean,
  savesDirOk: boolean,
  entries: readonly HashEntry[]
): Uint8Array => {
  const lines = entries
    .map((entry) =>
      `${
        encodeFields([
          entry.category,
          entry.name,
          String(entry.byteSize),
          entry.status,
          hex(entry.digest)
        ])
      }\n`
    )
    .toSorted()
  // The header is fixed-shape and precedes every record, and no field of a
  // record can reach back past it, so it needs no prefixing of its own.
  const header = `frames_dir_ok ${String(framesDirOk)}\nsaves_dir_ok ${String(savesDirOk)}\n`
  return sha256(new TextEncoder().encode(header + lines.join("")))
}

/** The file each document kind was read from — its stable name in the listing. */
const DOCUMENT_FILE: Record<StoredDocument["kind"], string> = {
  manifest: MANIFEST_FILE,
  report: REPORT_FILE,
  victory: VICTORY_FILE
}

/**
 * Read one run directory. Never fails: an artifact that cannot be read is a
 * reported skip and an absent row, which is what the fs backend's 404 for the
 * same file already looks like from a client's side.
 */
export const collectRun = (
  runsRoot: string,
  gameId: string,
  options: IngestOptions
): RunArtifacts => {
  const runRoot = resolveStrictly(runsRoot, gameId)
  const framesDirectory = safeArchiveDirectory(runRoot, Gateway.ARCHIVE_FRAMES_DIRECTORY)
  const savesDirectory = safeArchiveDirectory(runRoot, Gateway.ARCHIVE_SAVES_DIRECTORY)

  const victory = readVictory(runRoot, options.maxBinaryRowBytes)
  const video = readVideo(runRoot, options.maxBinaryRowBytes)
  const frames = Option.match(framesDirectory, {
    onNone: () => ({ frames: [] as readonly StoredFrame[], skips: [] as readonly IngestSkip[] }),
    onSome: (directory) => readFrames(directory, options.maxBinaryRowBytes)
  })
  const saves = Option.match(savesDirectory, {
    onNone: () => ({ saves: [] as readonly StoredSave[], skips: [] as readonly IngestSkip[] }),
    onSome: (directory) =>
      readSaves(directory, options.ppmHeadBytes, options.maxBinaryRowBytes)
  })

  const documents: readonly StoredDocument[] = [
    ...Option.toArray(readDocument(runRoot, MANIFEST_FILE, "manifest")),
    ...Option.toArray(readDocument(runRoot, REPORT_FILE, "report")),
    ...Option.toArray(Either.getOrElse(victory, () => Option.none<StoredDocument>()))
  ]
  const replayTail = readReplayTail(runRoot)
  const framesDirOk = Option.isSome(framesDirectory)
  const savesDirOk = Option.isSome(savesDirectory)
  const storedVideo = Either.getOrElse(video, () => Option.none<StoredVideo>())

  return {
    gameId,
    framesDirOk,
    savesDirOk,
    documents,
    replayTail,
    frames: frames.frames,
    video: storedVideo,
    saves: saves.saves,
    advisory: readAdvisory(documents),
    contentHash: contentHash(framesDirOk, savesDirOk, [
      ...documents.map((document) =>
        hashEntry(
          "document",
          DOCUMENT_FILE[document.kind],
          document.byteSize,
          document.status,
          document.sha256
        )
      ),
      ...Option.toArray(replayTail).map((tail) =>
        hashEntry("replay-tail", REPLAY_JSONL_FILE, tail.byteSize, "ok", tail.sha256)
      ),
      ...frames.frames.map((frame) =>
        hashEntry("frame", frame.name, frame.byteSize, "ok", frame.sha256)
      ),
      ...Option.toArray(storedVideo).map((found) =>
        hashEntry("video", Gateway.ARCHIVE_VIDEO_FILE, found.byteSize, "ok", found.sha256)
      ),
      ...saves.saves.map((save) =>
        hashEntry(
          "save",
          save.name,
          save.byteSize,
          save.isWhole ? "whole" : "head",
          save.sha256
        )
      )
    ]),
    // An artifact skip is reported against the run it belongs to; the artifact
    // itself is named in `detail`.
    skips: [
      ...Option.toArray(Either.getLeft(victory)),
      ...Option.toArray(Either.getLeft(video)),
      ...frames.skips,
      ...saves.skips
    ].map((skip): IngestSkip => ({ ...skip, entry: gameId }))
  }
}

/**
 * The `runs_root` as the repository resolves it (`replay_gateway.py:196`,
 * `Path(...).expanduser().resolve()`): the logical scope key stored on every
 * row. It has to be resolved identically on both sides or the gateway's
 * `WHERE runs_root = $1` finds nothing.
 */
export const resolveRunsRoot = (runsRoot: string): string =>
  Either.getOrElse(
    attempt("realpath", runsRoot, () => realpathSync(resolvePath(runsRoot))),
    () => resolvePath(runsRoot)
  )

/**
 * Is this directory entry a run? The four filesystem judgements the fs backend
 * makes at *read* time, in the order it makes them.
 *
 * Shared with {@link isRunOnDisk} deliberately: the delete phase re-asks this
 * exact question at the end of a sweep, and a second copy of the rule that
 * drifted would decide to delete a run the walk would have kept.
 */
const classifyEntry = (
  runsRoot: string,
  entry: string
): Either.Either<string, IngestSkip> => {
  if (!isGameId(entry)) {
    return Either.left({ entry, reason: "notAGameId", detail: Option.none() })
  }
  if (isSymlink(join(runsRoot, entry))) {
    return Either.left({ entry, reason: "symlink", detail: Option.none() })
  }
  const runRoot = resolveStrictly(runsRoot, entry)
  if (dirname(runRoot) !== runsRoot) {
    return Either.left({ entry, reason: "escapesRunsRoot", detail: Option.none() })
  }
  return isDirectory(runRoot)
    ? Either.right(entry)
    : Either.left({ entry, reason: "notADirectory", detail: Option.none() })
}

/**
 * Is `gameId` a run under `runsRoot` **now**?
 *
 * Asked in the delete transaction, about ids the walk did not see. A run that is
 * on the disk at that moment was ingested by somebody else while this sweep was
 * reading, and deleting it — which is what a seen-set captured at readdir time
 * does — silently destroys another process's work.
 *
 * Exported because it is the whole of the delete phase's licence, and a licence
 * that can only be observed by winning a race is a licence nothing tests.
 */
export const isRunOnDisk = (runsRoot: string, gameId: string): boolean =>
  Either.isRight(classifyEntry(runsRoot, gameId))

/** The directory entries that are runs, and the ones that are not, with reasons. */
const walk = (
  runsRoot: string,
  filter: ReadonlySet<string>
): Either.Either<
  { readonly gameIds: readonly string[]; readonly skipped: readonly IngestSkip[] },
  RunsRootUnreadable
> =>
  Either.match(attempt("readdir", runsRoot, () => readdirSync(runsRoot)), {
    onLeft: () => Either.left(new RunsRootUnreadable({ runsRoot })),
    onRight: (entries) => {
      const considered = filter.size === 0
        ? entries
        : entries.filter((entry) => filter.has(entry))
      const classified = considered.map((entry) => classifyEntry(runsRoot, entry))
      const gameIds = Arr.getRights(classified)
      const absent = Array.from(filter)
        .filter((id) => !considered.includes(id))
        .map((entry): IngestSkip => ({
          entry,
          reason: "requestedButAbsent",
          detail: Option.none()
        }))
      return Either.right({ gameIds, skipped: [...Arr.getLefts(classified), ...absent] })
    }
  })

// ---------------------------------------------------------------------------
// Writing
// ---------------------------------------------------------------------------

type Database = PgDrizzle.PgDrizzle["Type"]

const rowsWritten = (rows: ReadonlyArray<unknown>): number => rows.length

/**
 * The parameter budget for one statement — the **smaller** of the two ceilings
 * this code actually runs against, and both were measured rather than assumed.
 *
 * A multi-row `INSERT` spends one parameter per column per row, so the ceiling
 * is a row count: `run_saves` has nine columns and `run_frames` seven. It is not
 * theoretical. `saves/` holds an autosave **and** a `*.map.ppm` per turn, the
 * corpus already carries runs with 1506 saves, and a long game walks straight
 * into it.
 *
 * - **Live Postgres**: the extended protocol's `Bind` message counts parameters
 *   in an unsigned int16 — 65535 — and refuses the statement with an error past
 *   it. Measured: 7281 saves in one statement succeed, 7282 fail, and before
 *   per-run isolation the failure took the whole sweep with it.
 * - **PGlite 0.5.4**: the ceiling is a *signed* int16, 32767, and crossing it is
 *   **silent** — no error, no rows, and the surrounding transaction's earlier
 *   statements are lost too. Measured exactly: 3640 nine-column rows (32760
 *   parameters) insert; 3641 (32769) return zero rows with `Exit.Success`.
 *
 * The lower bound is therefore the one to honour: it is the only value at which
 * the hermetic tests predict the live server, which is the whole reason the
 * schema pins `bigint` to `{ mode: "number" }` as well. A budget that respected
 * only Postgres' 65535 would pass every hermetic test by writing nothing.
 */
const MAX_BIND_PARAMETERS = 32767

/**
 * Room left for the parameters a statement spends outside its `VALUES` list.
 * Generous on purpose: the exact number is a drizzle rendering detail, and the
 * cost of a spare slot is nothing while the cost of one too few is a failed run.
 */
const RESERVED_BIND_PARAMETERS = 64

/**
 * `rows` split so no chunk can exceed {@link MAX_BIND_PARAMETERS}.
 *
 * `Arr.chunksOf` on an empty array is `[]`, not `[[]]`, so a caller that has
 * nothing to write issues no statement — which is what the idempotence contract
 * needs, since an empty `INSERT` is still a statement.
 */
const chunkRows = <A>(
  rows: ReadonlyArray<A>,
  columnsPerRow: number
): ReadonlyArray<ReadonlyArray<A>> =>
  Arr.chunksOf(
    rows,
    Math.max(1, Math.floor((MAX_BIND_PARAMETERS - RESERVED_BIND_PARAMETERS) / columnsPerRow))
  )

/**
 * `name <> ALL($1::text[])` — the deletion predicate as **one** parameter.
 *
 * `notInArray(column, names)` renders `name not in ($1, $2, …)`, which spends a
 * parameter per surviving name and so has the same int16 ceiling the inserts
 * do, one directory listing away. The array form spends exactly one however
 * many names there are. The explicit `::text[]` is required: without it the
 * planner sees an untyped parameter on the right of `ALL` and refuses.
 *
 * An empty `names` makes the predicate true for every row, which is the wanted
 * answer — nothing is expected, so everything stored is stale.
 */
const nameNotAmong = (column: PgColumn, names: ReadonlyArray<string>) =>
  sql`${column} <> all(${sql.param([...names])}::text[])`

/** Columns per `run_frames` row, which is what its chunk size is derived from. */
const FRAME_COLUMNS = 7

/** Columns per `run_saves` row. */
const SAVE_COLUMNS = 9

const chunkedRowsWritten = (chunks: ReadonlyArray<ReadonlyArray<unknown>>): number =>
  chunks.reduce((total, rows) => total + rows.length, 0)

/**
 * The run row.
 *
 * The `IS DISTINCT FROM` guard is belt and braces — this statement is only
 * reached when the hash already differs — but it also means `ingested_at` does
 * not move for a run whose content did not, which keeps the column meaning
 * "when these bytes arrived".
 */
const upsertRun = (
  db: Database,
  runsRoot: string,
  sweepId: number,
  artifacts: RunArtifacts
): Effect.Effect<number, SqlError> =>
  Effect.map(
    db
      .insert(runs)
      .values({
        runsRoot,
        gameId: artifacts.gameId,
        contentHash: artifacts.contentHash,
        sweepId,
        framesDirOk: artifacts.framesDirOk,
        savesDirOk: artifacts.savesDirOk,
        state: artifacts.advisory.state,
        createdAt: artifacts.advisory.createdAt,
        finishedAt: artifacts.advisory.finishedAt,
        benchmarkValid: artifacts.advisory.benchmarkValid
      })
      .onConflictDoUpdate({
        target: [runs.runsRoot, runs.gameId],
        set: {
          contentHash: sql`excluded.content_hash`,
          ingestedAt: sql`now()`,
          sweepId: sql`excluded.sweep_id`,
          framesDirOk: sql`excluded.frames_dir_ok`,
          savesDirOk: sql`excluded.saves_dir_ok`,
          state: sql`excluded.state`,
          createdAt: sql`excluded.created_at`,
          finishedAt: sql`excluded.finished_at`,
          benchmarkValid: sql`excluded.benchmark_valid`
        },
        setWhere: sql`${runs.contentHash} is distinct from excluded.content_hash`
      })
      .returning({ gameId: runs.gameId }),
    rowsWritten
  )

/**
 * `WHERE (runs_root, game_id) = ($1, $2)`, spelled per table.
 *
 * Written out five times rather than made generic: drizzle types a column by
 * its table, and the one-line duplication costs less than the type gymnastics
 * that would unify them. Every statement in this module carries this predicate,
 * so nothing can ever touch a row belonging to another root.
 */
const scopeDocuments = (runsRoot: string, gameId: string) =>
  and(eq(runDocuments.runsRoot, runsRoot), eq(runDocuments.gameId, gameId))

const scopeReplayTail = (runsRoot: string, gameId: string) =>
  and(eq(runReplayTail.runsRoot, runsRoot), eq(runReplayTail.gameId, gameId))

const scopeFrames = (runsRoot: string, gameId: string) =>
  and(eq(runFrames.runsRoot, runsRoot), eq(runFrames.gameId, gameId))

const scopeVideos = (runsRoot: string, gameId: string) =>
  and(eq(runVideos.runsRoot, runsRoot), eq(runVideos.gameId, gameId))

const scopeSaves = (runsRoot: string, gameId: string) =>
  and(eq(runSaves.runsRoot, runsRoot), eq(runSaves.gameId, gameId))

const writeDocuments = (
  db: Database,
  runsRoot: string,
  artifacts: RunArtifacts
): Effect.Effect<WriteCounters, SqlError> =>
  Effect.gen(function*() {
    const kinds = artifacts.documents.map((document) => document.kind)
    const removed = yield* db
      .delete(runDocuments)
      .where(
        and(scopeDocuments(runsRoot, artifacts.gameId), notInArray(runDocuments.kind, kinds))
      )
      .returning({ kind: runDocuments.kind })
    if (artifacts.documents.length === 0) {
      return { ...NO_WRITES, deletes: rowsWritten(removed) }
    }
    const written = yield* db
      .insert(runDocuments)
      .values(
        artifacts.documents.map((document) => ({
          runsRoot,
          gameId: artifacts.gameId,
          kind: document.kind,
          status: document.status,
          bytes: document.bytes,
          byteSize: document.byteSize,
          sha256: document.sha256
        }))
      )
      .onConflictDoUpdate({
        target: [runDocuments.runsRoot, runDocuments.gameId, runDocuments.kind],
        set: {
          status: sql`excluded.status`,
          bytes: sql`excluded.bytes`,
          byteSize: sql`excluded.byte_size`,
          sha256: sql`excluded.sha256`
        },
        setWhere: sql`${runDocuments.sha256} is distinct from excluded.sha256
          or ${runDocuments.byteSize} is distinct from excluded.byte_size
          or ${runDocuments.status} is distinct from excluded.status`
      })
      .returning({ kind: runDocuments.kind })
    return { ...NO_WRITES, documents: rowsWritten(written), deletes: rowsWritten(removed) }
  })

const writeReplayTail = (
  db: Database,
  runsRoot: string,
  artifacts: RunArtifacts
): Effect.Effect<WriteCounters, SqlError> =>
  Option.match(artifacts.replayTail, {
    onNone: () =>
      Effect.map(
        db
          .delete(runReplayTail)
          .where(scopeReplayTail(runsRoot, artifacts.gameId))
          .returning({ gameId: runReplayTail.gameId }),
        (removed): WriteCounters => ({ ...NO_WRITES, deletes: rowsWritten(removed) })
      ),
    onSome: (tail) =>
      Effect.map(
        db
          .insert(runReplayTail)
          .values({
            runsRoot,
            gameId: artifacts.gameId,
            byteSize: tail.byteSize,
            tailBytes: tail.tailBytes,
            sha256: tail.sha256
          })
          .onConflictDoUpdate({
            target: [runReplayTail.runsRoot, runReplayTail.gameId],
            set: {
              byteSize: sql`excluded.byte_size`,
              tailBytes: sql`excluded.tail_bytes`,
              sha256: sql`excluded.sha256`
            },
            setWhere: sql`${runReplayTail.sha256} is distinct from excluded.sha256
              or ${runReplayTail.byteSize} is distinct from excluded.byte_size`
          })
          .returning({ gameId: runReplayTail.gameId }),
        (written): WriteCounters => ({ ...NO_WRITES, replayTails: rowsWritten(written) })
      )
  })

const writeFrames = (
  db: Database,
  runsRoot: string,
  artifacts: RunArtifacts
): Effect.Effect<WriteCounters, SqlError> =>
  Effect.gen(function*() {
    const names = artifacts.frames.map((frame) => frame.name)
    const removed = yield* db
      .delete(runFrames)
      .where(and(scopeFrames(runsRoot, artifacts.gameId), nameNotAmong(runFrames.name, names)))
      .returning({ name: runFrames.name })
    // Seven columns per row — see MAX_BIND_PARAMETERS.
    const written = yield* Effect.forEach(
      chunkRows(artifacts.frames, FRAME_COLUMNS),
      (chunk) =>
        db
          .insert(runFrames)
          .values(
            chunk.map((frame) => ({
              runsRoot,
              gameId: artifacts.gameId,
              name: frame.name,
              frameIndex: Option.getOrNull(frame.frameIndex),
              bytes: frame.bytes,
              byteSize: frame.byteSize,
              sha256: frame.sha256
            }))
          )
          .onConflictDoUpdate({
            target: [runFrames.runsRoot, runFrames.gameId, runFrames.name],
            set: {
              frameIndex: sql`excluded.frame_index`,
              bytes: sql`excluded.bytes`,
              byteSize: sql`excluded.byte_size`,
              sha256: sql`excluded.sha256`
            },
            setWhere: sql`${runFrames.sha256} is distinct from excluded.sha256
              or ${runFrames.byteSize} is distinct from excluded.byte_size
              or ${runFrames.frameIndex} is distinct from excluded.frame_index`
          })
          .returning({ name: runFrames.name }),
      { concurrency: 1 }
    )
    return { ...NO_WRITES, frames: chunkedRowsWritten(written), deletes: rowsWritten(removed) }
  })

const writeVideo = (
  db: Database,
  runsRoot: string,
  artifacts: RunArtifacts
): Effect.Effect<WriteCounters, SqlError> =>
  Option.match(artifacts.video, {
    onNone: () =>
      Effect.map(
        db
          .delete(runVideos)
          .where(scopeVideos(runsRoot, artifacts.gameId))
          .returning({ gameId: runVideos.gameId }),
        (removed): WriteCounters => ({ ...NO_WRITES, deletes: rowsWritten(removed) })
      ),
    onSome: (video) =>
      Effect.map(
        db
          .insert(runVideos)
          .values({
            runsRoot,
            gameId: artifacts.gameId,
            bytes: video.bytes,
            byteSize: video.byteSize,
            sha256: video.sha256
          })
          .onConflictDoUpdate({
            target: [runVideos.runsRoot, runVideos.gameId],
            set: {
              bytes: sql`excluded.bytes`,
              byteSize: sql`excluded.byte_size`,
              sha256: sql`excluded.sha256`
            },
            setWhere: sql`${runVideos.sha256} is distinct from excluded.sha256
              or ${runVideos.byteSize} is distinct from excluded.byte_size`
          })
          .returning({ gameId: runVideos.gameId }),
        (written): WriteCounters => ({ ...NO_WRITES, videos: rowsWritten(written) })
      )
  })

const writeSaves = (
  db: Database,
  runsRoot: string,
  artifacts: RunArtifacts
): Effect.Effect<WriteCounters, SqlError> =>
  Effect.gen(function*() {
    const names = artifacts.saves.map((save) => save.name)
    const removed = yield* db
      .delete(runSaves)
      .where(and(scopeSaves(runsRoot, artifacts.gameId), nameNotAmong(runSaves.name, names)))
      .returning({ name: runSaves.name })
    // Nine columns per row — the tightest ceiling in the schema.
    const written = yield* Effect.forEach(
      chunkRows(artifacts.saves, SAVE_COLUMNS),
      (chunk) =>
        db
          .insert(runSaves)
          .values(
            chunk.map((save) => ({
              runsRoot,
              gameId: artifacts.gameId,
              name: save.name,
              kind: save.kind,
              saveTurn: Option.getOrNull(save.saveTurn),
              byteSize: save.byteSize,
              headBytes: save.headBytes,
              isWhole: save.isWhole,
              sha256: save.sha256
            }))
          )
          .onConflictDoUpdate({
            target: [runSaves.runsRoot, runSaves.gameId, runSaves.name],
            set: {
              kind: sql`excluded.kind`,
              saveTurn: sql`excluded.save_turn`,
              byteSize: sql`excluded.byte_size`,
              headBytes: sql`excluded.head_bytes`,
              isWhole: sql`excluded.is_whole`,
              sha256: sql`excluded.sha256`
            },
            setWhere: sql`${runSaves.sha256} is distinct from excluded.sha256
              or ${runSaves.byteSize} is distinct from excluded.byte_size
              or ${runSaves.isWhole} is distinct from excluded.is_whole
              or ${runSaves.saveTurn} is distinct from excluded.save_turn
              or ${runSaves.kind} is distinct from excluded.kind`
          })
          .returning({ name: runSaves.name }),
      { concurrency: 1 }
    )
    return { ...NO_WRITES, saves: chunkedRowsWritten(written), deletes: rowsWritten(removed) }
  })

/** The stored hash for a run, if it has ever been ingested. */
const storedHash = (
  db: Database,
  runsRoot: string,
  gameId: string
): Effect.Effect<Option.Option<Uint8Array>, SqlError> =>
  Effect.map(
    db
      .select({ contentHash: runs.contentHash })
      .from(runs)
      .where(and(eq(runs.runsRoot, runsRoot), eq(runs.gameId, gameId))),
    (rows) => Option.map(Option.fromNullable(rows[0]), (row) => row.contentHash)
  )

const sameBytes = (left: Uint8Array, right: Uint8Array): boolean =>
  left.length === right.length && left.every((byte, index) => byte === right[index])

/**
 * `pg_current_xact_id_if_assigned()` — `NULL` until the transaction writes.
 *
 * Reading it is itself free: the `_if_assigned` variant never allocates an id.
 * This is the hermetic proof behind {@link IngestReport.transactionsWithWrites}.
 */
const transactionWrote = (
  client: SqlClient.SqlClient
): Effect.Effect<boolean, SqlError> =>
  Effect.map(
    client.unsafe<{ xid: string | null }>("select pg_current_xact_id_if_assigned() as xid"),
    (rows) => (rows[0]?.xid ?? null) !== null
  )

interface RunWrite {
  readonly result: RunResult
  readonly wroteInTransaction: boolean
}

/**
 * One run, in one transaction.
 *
 * The hash comparison is a plain `SELECT`. The dossier's `FOR UPDATE` is
 * deliberately absent: a row lock is recorded in the tuple's `xmax`, so it
 * assigns a transaction id and would destroy the zero-write proof — while
 * buying nothing, because the upsert that follows is idempotent by content and
 * atomic under `ON CONFLICT`.
 */
const ingestRun = (
  db: Database,
  client: SqlClient.SqlClient,
  runsRoot: string,
  sweepId: number,
  artifacts: RunArtifacts
): Effect.Effect<RunWrite, SqlError> =>
  client.withTransaction(
    Effect.gen(function*() {
      const existing = yield* storedHash(db, runsRoot, artifacts.gameId)
      const unchanged = Option.match(existing, {
        onNone: () => false,
        onSome: (hash) => sameBytes(hash, artifacts.contentHash)
      })
      const unusableDocuments =
        artifacts.documents.filter((document) => document.status === "unusable").length

      if (unchanged) {
        return {
          result: {
            gameId: artifacts.gameId,
            outcome: "unchanged" as const,
            unusableDocuments,
            manifestUndecodable: artifacts.advisory.undecodable,
            writes: NO_WRITES
          },
          wroteInTransaction: yield* transactionWrote(client)
        }
      }

      const runWrites = yield* upsertRun(db, runsRoot, sweepId, artifacts)
      const children = yield* Effect.all(
        [
          writeDocuments(db, runsRoot, artifacts),
          writeReplayTail(db, runsRoot, artifacts),
          writeFrames(db, runsRoot, artifacts),
          writeVideo(db, runsRoot, artifacts),
          writeSaves(db, runsRoot, artifacts)
        ],
        { concurrency: 1 }
      )
      return {
        result: {
          gameId: artifacts.gameId,
          outcome: Option.isNone(existing) ? ("inserted" as const) : ("updated" as const),
          unusableDocuments,
          manifestUndecodable: artifacts.advisory.undecodable,
          writes: addWrites({ ...NO_WRITES, runs: runWrites }, sumWrites(children))
        },
        wroteInTransaction: yield* transactionWrote(client)
      }
    })
  )

// ---------------------------------------------------------------------------
// Sweeps
// ---------------------------------------------------------------------------

/**
 * A record `object` — the narrowing {@link describeSqlProblem} needs to read a
 * driver error without an unchecked cast.
 */
const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null

const readStringField = (source: unknown, key: string): Option.Option<string> =>
  isRecord(source) && typeof source[key] === "string" && source[key] !== ""
    ? Option.some(source[key])
    : Option.none()

/**
 * The fields of a Postgres `ErrorResponse` that may be repeated to an operator.
 *
 * All four are catalogue identifiers or a five-character SQLSTATE, set by the
 * *server*. None can hold a connection parameter, and none holds a row value —
 * unlike `detail` and `message`, which are deliberately not read.
 */
const SQL_SERVER_FIELDS: ReadonlyArray<string> = ["code", "table", "column", "constraint"]

/**
 * What may be said about a `SqlError` in a report line.
 *
 * `ingest-cli.ts` never prints a `SqlError`'s `cause`, because `pg` puts the
 * connection parameters on some of them. That is right, and on its own it left
 * every data-driven failure undiagnosable: one line reading `error: Failed to
 * execute QueryPromise`, with no table, no column, no value and no game id. The
 * game id is the call site's and is not a secret; these four fields are the
 * server's own and are not either.
 */
/**
 * How far down `cause` to look, and why there is a limit at all.
 *
 * The server's fields are not at a fixed depth: `pg` puts them on the
 * `DatabaseError` that *is* `SqlError.cause`, while PGlite wraps that error once
 * more, so the same fields sit one level further down. Four links reaches both
 * with room to spare and cannot loop on a cyclic `cause`.
 */
const MAX_CAUSE_DEPTH = 4

const causeChain = (error: unknown, depth: number): ReadonlyArray<unknown> =>
  depth <= 0 || !isRecord(error) ? [] : [error, ...causeChain(error["cause"], depth - 1)]

const problemFields = (link: unknown): ReadonlyArray<string> =>
  SQL_SERVER_FIELDS.flatMap((key) =>
    Option.match(readStringField(link, key), {
      onNone: (): ReadonlyArray<string> => [],
      onSome: (value) => [`${key}=${value}`]
    })
  )

export const describeSqlProblem = (error: unknown): string =>
  Option.match(
    Arr.findFirst(
      causeChain(error, MAX_CAUSE_DEPTH).map(problemFields),
      (fields) => fields.length > 0
    ),
    {
      onNone: () => "the database refused this run's rows",
      onSome: (fields) => `the database refused this run's rows (${fields.join(" ")})`
    }
  )

/**
 * How long a sweep may say `running` before it is presumed dead.
 *
 * A `kill -9` mid-sweep leaves the row saying `running` and nothing reconciles
 * it, so without this the word means "at some point since install". It cannot be
 * shorter than a real sweep of a real archive, and it need not be tight: nothing
 * *decides* anything from `status` — deletion is licensed by a fresh look at the
 * disk — so this only keeps the ledger readable.
 */
const ABANDONED_SWEEP = "24 hours"

/**
 * Close out this root's sweeps that have been `running` implausibly long.
 *
 * Scoped to one `runs_root` so a sweep of one archive never touches another's
 * ledger, and bounded by the *server's* clock rather than the client's, which is
 * the clock every other timestamp in this table was written by.
 */
const reconcileAbandonedSweeps = (
  db: Database,
  runsRoot: string
): Effect.Effect<number, SqlError> =>
  Effect.map(
    db
      .update(ingestSweeps)
      .set({ status: "aborted", finishedAt: sql`now()` })
      .where(
        and(
          eq(ingestSweeps.runsRoot, runsRoot),
          eq(ingestSweeps.status, "running"),
          sql`${ingestSweeps.startedAt} < now() - interval '${sql.raw(ABANDONED_SWEEP)}'`
        )
      )
      .returning({ sweepId: ingestSweeps.sweepId }),
    rowsWritten
  )

const openSweep = (db: Database, runsRoot: string): Effect.Effect<number, SqlError> =>
  Effect.map(
    db
      .insert(ingestSweeps)
      .values({ runsRoot, status: "running" })
      .returning({ sweepId: ingestSweeps.sweepId }),
    (rows) => rows[0]?.sweepId ?? 0
  )

const closeSweep = (
  db: Database,
  sweepId: number,
  status: "complete" | "aborted",
  seenCount: number
): Effect.Effect<number, SqlError> =>
  Effect.map(
    db
      .update(ingestSweeps)
      .set({ status, seenCount, finishedAt: sql`now()` })
      .where(eq(ingestSweeps.sweepId, sweepId))
      .returning({ sweepId: ingestSweeps.sweepId }),
    rowsWritten
  )

/**
 * The runs under this root that are stored, that the sweep did not see, **and
 * that are not on the disk now**.
 *
 * The third clause is the one that matters. The walk happens at the start of a
 * sweep and this runs at the end, and a captured seen-set says nothing about the
 * window in between: measured on a live server, a 40-run sweep A plus an
 * `--game` sweep B that ingested one new run 1.0 s in ended with A reporting
 * `40 unchanged, 1 deleted`, both processes exiting `0`, and B's run gone from
 * every table while sitting on disk. Re-asking the filesystem costs one `lstat`
 * per *candidate* — a set that is empty on every healthy sweep — and it is the
 * same evidence, from the same source, that put the row there.
 *
 * Scoped to `--game`'s ids when there are any: a sweep that only looked at two
 * runs may only delete those two, and a run it never examined is not evidence
 * of anything.
 */
const staleRuns = (
  db: Database,
  runsRoot: string,
  seen: readonly string[],
  filter: ReadonlySet<string>
): Effect.Effect<readonly string[], SqlError> =>
  Effect.map(
    db
      .select({ gameId: runs.gameId })
      .from(runs)
      .where(eq(runs.runsRoot, runsRoot)),
    (rows) =>
      rows
        .map((row) => row.gameId)
        .filter((gameId) =>
          !seen.includes(gameId) &&
          (filter.size === 0 || filter.has(gameId)) &&
          !isRunOnDisk(runsRoot, gameId)
        )
  )

const deleteRuns = (
  db: Database,
  runsRoot: string,
  gameIds: readonly string[]
): Effect.Effect<number, SqlError> =>
  gameIds.length === 0
    ? Effect.succeed(0)
    : Effect.map(
      db
        .delete(runs)
        // `inArray`, emphatically not `notInArray(…, [])`: drizzle renders an
        // empty `notInArray` as the literal `true`, which would have deleted
        // every run under the root the moment one run went stale.
        .where(and(eq(runs.runsRoot, runsRoot), inArray(runs.gameId, [...gameIds])))
        .returning({ gameId: runs.gameId }),
      rowsWritten
    )

/** One run's pass through the sweep: what it wrote, and what it could not. */
interface RunPass {
  /** `Option.none()` exactly when the database refused the run. */
  readonly write: Option.Option<RunWrite>
  readonly skips: readonly IngestSkip[]
}

/**
 * Read one run and write it, as one step of the sweep.
 *
 * Two properties, both structural:
 *
 * 1. **Bounded residency.** `collectRun` is called *inside* `Effect.suspend`, so
 *    the run's bytes are read when this step runs and are unreachable the moment
 *    it finishes. Building the whole root's artifacts up front — which is what a
 *    `gameIds.map(collectRun)` before the loop does — makes peak memory linear
 *    in the *archive*: measured at 477 MB of resident growth for a 480 MiB root,
 *    and the real corpus is 845 MB at the default PPM head. Collect, write,
 *    drop costs nothing and bounds it by the largest single run.
 * 2. **Isolation.** A `SqlError` from one run's transaction becomes that run's
 *    reported skip and nothing more. Without this, one refused row aborted the
 *    `Effect.forEach` and every run *after* it in `readdir` order — an arbitrary
 *    subset, chosen by the directory's hash order — never reached the database
 *    at all. The fs backend answers per run; so does this.
 */
const ingestOne = (
  db: Database,
  client: SqlClient.SqlClient,
  runsRoot: string,
  sweepId: number,
  gameId: string,
  options: IngestOptions
): Effect.Effect<RunPass> =>
  Effect.suspend(() => {
    const artifacts = collectRun(runsRoot, gameId, options)
    return Effect.map(
      Effect.either(ingestRun(db, client, runsRoot, sweepId, artifacts)),
      Either.match({
        onLeft: (error): RunPass => ({
          write: Option.none(),
          skips: [
            ...artifacts.skips,
            {
              entry: gameId,
              reason: "databaseRefused" as const,
              detail: Option.some(describeSqlProblem(error))
            }
          ]
        }),
        onRight: (write): RunPass => ({ write: Option.some(write), skips: artifacts.skips })
      })
    )
  })

/** The same step for `--dry-run`: read, compare, write nothing. */
const inspectOne = (
  db: Database,
  runsRoot: string,
  gameId: string,
  options: IngestOptions
): Effect.Effect<{ readonly result: RunResult; readonly skips: readonly IngestSkip[] }, SqlError> =>
  Effect.suspend(() => {
    const artifacts = collectRun(runsRoot, gameId, options)
    return Effect.map(storedHash(db, runsRoot, gameId), (existing) => ({
      result: {
        gameId,
        outcome: Option.match(existing, {
          onNone: (): RunOutcome => "inserted",
          onSome: (hash) => (sameBytes(hash, artifacts.contentHash) ? "unchanged" : "updated")
        }),
        unusableDocuments:
          artifacts.documents.filter((document) => document.status === "unusable").length,
        manifestUndecodable: artifacts.advisory.undecodable,
        writes: NO_WRITES
      },
      skips: artifacts.skips
    }))
  })

/**
 * Walk a `runs_root` and reconcile it into the database.
 *
 * The whole pipeline: close out any abandoned sweep of this root, open a sweep,
 * classify every directory entry, read and write each run in its own
 * transaction, delete the runs that are neither seen nor on the disk, close the
 * sweep. A dry run does all of the reading and none of the writing — it does not
 * even open a sweep row — and reports exactly what would have changed.
 *
 * A run the database refused is a reported skip, not the end of the sweep, and
 * it is *not* a licence to delete anything: it was seen, so it stays out of the
 * stale set whatever happened to its rows.
 */
export const ingest = (
  options: IngestOptions
): Effect.Effect<
  IngestReport,
  IngestError,
  PgDrizzle.PgDrizzle | SqlClient.SqlClient
> =>
  Effect.gen(function*() {
    const db = yield* PgDrizzle.PgDrizzle
    const client = yield* SqlClient.SqlClient
    const runsRoot = resolveRunsRoot(options.runsRoot)

    const walked = yield* walk(runsRoot, options.gameIds)
    const seen = walked.gameIds.length + walked.skipped.length

    if (options.dryRun) {
      const inspected = yield* Effect.forEach(
        walked.gameIds,
        (gameId) => inspectOne(db, runsRoot, gameId, options),
        { concurrency: 1 }
      )
      const stale = yield* staleRuns(db, runsRoot, walked.gameIds, options.gameIds)
      return {
        runsRoot,
        sweepId: Option.none(),
        dryRun: true,
        status: "complete" as const,
        seen,
        runs: inspected.map((entry) => entry.result),
        deleted: stale,
        skipped: [...walked.skipped, ...inspected.flatMap((entry) => entry.skips)],
        writes: NO_WRITES,
        transactionsWithWrites: 0
      }
    }

    const reconciled = yield* reconcileAbandonedSweeps(db, runsRoot)
    const sweepId = yield* openSweep(db, runsRoot)
    // Anything that fails after the sweep is open leaves it `aborted`, never
    // `running`. That is bookkeeping, not a safety property: the ledger licenses
    // nothing, because the delete phase re-reads the disk.
    const swept = <A>(effect: Effect.Effect<A, SqlError>): Effect.Effect<A, SqlError> =>
      Effect.tapError(effect, () => Effect.ignore(closeSweep(db, sweepId, "aborted", 0)))

    const passes = yield* Effect.forEach(
      walked.gameIds,
      (gameId) => ingestOne(db, client, runsRoot, sweepId, gameId, options),
      { concurrency: 1 }
    )
    const written = passes.flatMap((pass) => Option.toArray(pass.write))

    // The `SELECT` that finds the stale ids, the filesystem recheck that
    // confirms them and the `DELETE` that removes them are one transaction, so
    // no row can be inserted between the decision and the deletion.
    const removal = yield* swept(
      client.withTransaction(
        Effect.flatMap(
          staleRuns(db, runsRoot, walked.gameIds, options.gameIds),
          (stale) =>
            Effect.map(deleteRuns(db, runsRoot, stale), (deleted) => ({ stale, deleted }))
        )
      )
    )
    const closed = yield* closeSweep(db, sweepId, "complete", walked.gameIds.length)

    return {
      runsRoot,
      sweepId: Option.some(sweepId),
      dryRun: false,
      status: "complete" as const,
      seen,
      runs: written.map((entry) => entry.result),
      deleted: removal.stale,
      skipped: [...walked.skipped, ...passes.flatMap((pass) => pass.skips)],
      writes: addWrites(
        sumWrites(written.map((entry) => entry.result.writes)),
        { ...NO_WRITES, deletes: removal.deleted, sweeps: 1 + closed + reconciled }
      ),
      transactionsWithWrites: written.filter((entry) => entry.wroteInTransaction).length
    }
  })
