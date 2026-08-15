/**
 * The ingest pipeline v2 — a `runs_root` on disk becomes **domain rows**.
 *
 * v1 mirrored bytes: every artifact of every run was stored verbatim and the
 * read path re-parsed them. The frozen v2 schema stores a *model of a game*
 * (`games`/`seats`/`turns`/`board_state`/`player_turns`/`agent_stats`), no
 * artifacts and no filesystem paths, so this module is where the decoding
 * happens — once, at write time, with every judgement written down.
 *
 * ## The storage rule, in two sentences
 *
 * **Partition.** For a document with `status = 'ok'` every key is stored
 * exactly once: in its typed column, or in `extras.manifest`. Never both.
 *
 * **Demotion.** A key takes its typed column only when the column can hold the
 * value losslessly *and* the value's type is the column's type. Otherwise the
 * key is demoted whole into `extras.manifest`, verbatim, and its column is
 * `NULL`. An explicit `null` is therefore always a demotion — a column cannot
 * distinguish "absent" from "present and null", and one site in the gateway can
 * (`untrustedFieldOr(manifest, 'state', 'status')`: a *present* `null` `state`
 * beats `status`, an absent one does not).
 *
 * The consequence, which is the whole reconstruction contract:
 *
 * | stored shape | the document had |
 * |---|---|
 * | column `NULL`, key absent from `extras.manifest` | no such key |
 * | column `NULL`, key present in `extras.manifest` | that key, un-columnable |
 * | column non-`NULL` | that key, and the column is its value |
 *
 * ## What ingest decides, and what it deliberately does not
 *
 * Postgres has no symlinks and no `st_mode`, so the filesystem rules the fs
 * backend applies at *read* time are applied here instead: `GAME_ID_RE` fails,
 * the run directory is a symlink, it resolves outside `runs_root`, or it is not
 * a directory ⇒ **no `games` row** ⇒ the pg gateway's 404 `gameNotFound` is the
 * fs gateway's 404. Likewise `_read_archive_json`'s verdict becomes
 * `manifest_status` / `report_status`: `open` failed ⇒ `absent` ⇒ 404; opened
 * but not a non-empty regular file of at most 8 MiB whose bytes decode as UTF-8
 * and parse as a JSON *object* ⇒ `unusable` ⇒ 503.
 *
 * Every *other* gate stays on the read side. A manifest whose `game_id`
 * disagrees with its directory is ingested exactly as it stands, because the pg
 * `readManifest` re-runs that cross-check over the *reconstructed* document.
 *
 * ## The one place v2 is less capable than v1, and it is loud
 *
 * `text` silently turns a lone surrogate into U+FFFD and refuses U+0000;
 * `jsonb` refuses both. v1 survived a manifest carrying either because it
 * stored bytes. v2 cannot: there is no bytes-shaped carrier in the schema, and
 * a silent repair would change `resolved_places` and with it the derivation
 * cache key. So the decided policy is to be **honest rather than green**: a
 * document containing U+0000 or a lone surrogate in any string — key or value —
 * is `status = 'unusable'`, exactly as if it had failed the UTF-8 gate, and the
 * run is reported with a {@link SkipReason} of `documentUnstorable`. That is a
 * declared divergence (the fs gateway serves such a manifest), recorded here
 * rather than discovered later as a waiver request.
 *
 * ## Idempotence
 *
 * `games.content_hash` is sha256 over a canonical listing of what this module
 * actually decodes — the two documents (fstat size, status, digest) and
 * `replay.jsonl` (whole-file size, digest of the bytes read). Every field is
 * length-prefixed ({@link encodeFields}), because a filename or a status is
 * attacker-shaped and a separator-joined encoding lets one field imitate a
 * whole record; an equal hash is the licence to write *nothing*, so a forgeable
 * listing is a forgeable "unchanged".
 *
 * Boards are **not** in the hash: they are resumable by set difference (§ board
 * phase), and folding a new autosave into the hash would rewrite every typed
 * row of a run whose only change was an autosave.
 *
 * When the hash matches, the run's transaction issues no statement that writes
 * — not an `UPDATE`, not a no-op `INSERT ... ON CONFLICT`, not a `SELECT ...
 * FOR UPDATE` (a row lock is recorded in `xmax`, which assigns a transaction
 * id). That is what makes {@link IngestReport.transactionsWithWrites}, read
 * from `pg_current_xact_id_if_assigned()` inside each transaction, a proof
 * rather than a row count that happened to stay level.
 *
 * ## Deletion is opt-in now
 *
 * v1 scoped every row by `runs_root`. The frozen schema has no such column — a
 * `games` row is a game, not a directory — so "delete the ids that are not on
 * disk" would delete another archive's rows from a shared database. Pruning is
 * therefore `--prune`, keeping v1's licence: {@link isRunOnDisk} is re-checked
 * *inside* the delete transaction, so a run another process ingested while this
 * sweep was reading survives.
 *
 * ## Interop boundaries
 *
 * Three, all declared: the `node:fs` section below (mirroring
 * `gateway/services/runs.ts`, which needs `open(2)` with a raw flag word,
 * `fstat` on the descriptor it holds, and positional reads); {@link decodeUtf8},
 * one `Either.try` around a `fatal: true` `TextDecoder`, which is the only way
 * that decoder reports invalid UTF-8 and which decides a 503; and the SQL
 * driver layer in `./client.ts`. The board phase spawns `python3` through the
 * gateway's own bridge — that boundary belongs to `services/derivation.ts` and
 * is used, not re-implemented.
 *
 * @module
 */

import {
  CANON_ASCII,
  type CanonRecord,
  type CanonValue,
  canonicalText,
  findLoneSurrogate,
  Gateway,
  isGameId,
  isJsonObject,
  type JsonObject,
  type JsonValue
} from "@arena/wire"
import * as PgDrizzle from "@effect/sql-drizzle/Pg"
import * as SqlClient from "@effect/sql/SqlClient"
import type { SqlError } from "@effect/sql/SqlError"
import { and, eq, getTableColumns, inArray, sql, type Table } from "drizzle-orm"
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
import { basename, dirname, join, resolve as resolvePath } from "node:path"
// oxlint-disable-next-line effecttsgo/node-builtin-import
import { fileURLToPath } from "node:url"

import { MAX_PROXY_JSON_BYTES } from "../../harness/src/gateway/constants.ts"
import { derivationPlaces } from "../../harness/src/gateway/http/routes/replay.ts"
import {
  parsePythonJson,
  parsePythonJsonObject
} from "../../harness/src/gateway/python-json.ts"
import {
  type DerivationError,
  type DerivationRunner,
  pythonDerivationRunner
} from "../../harness/src/gateway/services/derivation.ts"
import {
  MANIFEST_FILE,
  REPLAY_JSONL_FILE,
  REPLAY_TAIL_BYTES,
  REPORT_FILE
} from "../../harness/src/gateway/services/runs.ts"

import {
  agentStats,
  boardState,
  games,
  playerTurns,
  runState,
  seats,
  turns
} from "./schema.ts"

// ---------------------------------------------------------------------------
// Constants
//
// Imported, not redeclared. The writer and the reader have to agree on these
// exactly, and the only way to guarantee that is for there to be one of each.
// ---------------------------------------------------------------------------

/** `manifest.json`, `report.json`, `replay.jsonl` and the 64 KiB tail window. */
export { MANIFEST_FILE, REPLAY_JSONL_FILE, REPLAY_TAIL_BYTES, REPORT_FILE }

/**
 * `MAX_PROXY_JSON_BYTES` (`gateway/constants.ts`, `replay_gateway.py:55`).
 *
 * The *gate* that decides `status = 'unusable'`, applied to the fstat size. It
 * doubles as the read cap for a document that has already failed the gate:
 * those bytes can never be parsed by the read path, so reading more is cost.
 */
export const MAX_DOCUMENT_BYTES: number = MAX_PROXY_JSON_BYTES

/**
 * How much of a `replay.jsonl` is decoded into `turns` / `player_turns`.
 *
 * The fs backend never reads more than the 64 KiB tail, so this cap changes no
 * response: it bounds the *domain* rows only. A file past it is decoded up to
 * the cap, its final (possibly torn) line dropped, and the run reported with a
 * `replayTruncated` skip — loudly, because "some turns are missing" must not be
 * indistinguishable from "the game had that many turns".
 */
export const DEFAULT_MAX_REPLAY_BYTES = 256 * 1024 * 1024

/** darwin `O_RDONLY` / `O_NOFOLLOW` / `O_CLOEXEC` (`services/runs.ts`). */
const O_RDONLY = 0x0000_0000
const O_NOFOLLOW = 0x0000_0100
const O_CLOEXEC = 0x0100_0000

/**
 * `SAVE_NAME_RE` — `agent_eval/save_replay.py:35`. The autosaves the python
 * bridge discovers, and therefore the only turns a board can be derived for.
 */
export const SAVE_NAME_RE: RegExp = /^turn-(\d+)-(?:auto|final)\.sav(?:\.gz|\.bz2|\.xz|\.zst)?$/

/** One of the seven lifecycle states `games.state` can hold. */
export type StorableRunState = (typeof runState.enumValues)[number]

/** The seven storable lifecycle states, taken from the frozen enum itself. */
export const RUN_STATE_VALUES: ReadonlyArray<StorableRunState> = runState.enumValues

/** Is this exactly one of the seven spellings the column accepts? */
export const isStorableRunState = (value: JsonValue | undefined): value is StorableRunState =>
  typeof value === "string" &&
  RUN_STATE_VALUES.some((candidate): boolean => candidate === value)

/**
 * The sentinel `games.state` carries when the manifest's `state` is not one of
 * the seven.
 *
 * `games.state` is `NOT NULL` over a seven-value enum and a manifest may carry
 * any string, a number, or nothing at all. A domain query must read
 * `state = 'invalid' AND extras->'manifest' ? 'state'` as *unrecognized*, not
 * as "the run was invalid".
 */
export const UNRECOGNIZED_STATE = "invalid" as const

/** int32, the width of every `integer` column in the schema. */
const INT32_MIN = -2147483648
const INT32_MAX = 2147483647

// ---------------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------------

/**
 * The board phase's configuration. `Option.none()` on {@link IngestOptions}
 * means `--skip-boards`: the fast, parity-shaped sweep.
 */
export interface BoardOptions {
  /**
   * `--cache-root`, passed to the bridge verbatim. Stable across sweeps on
   * purpose: `save_replay._load_cache` validates each entry against the source
   * save's `{device, inode, size, mtime_ns, ctime_ns}`, so a second sweep over
   * the same disk re-uses every parse, while a per-sweep `mkdtemp` throws the
   * whole cache away.
   */
  readonly cacheRoot: string
  /** The checkout holding `agent_eval/` — the bridge's working directory. */
  readonly repoRoot: string
  /** At most this many turns per game per sweep; `0` is "no limit". */
  readonly turnLimit: number
  /** How many *games* derive concurrently. Within a game it is always serial. */
  readonly concurrency: number
  /**
   * The derivation backend. `Option.none()` builds the python bridge from
   * `cacheRoot`/`repoRoot`; a test injects its own so the phase is exercisable
   * without a python checkout.
   */
  readonly runner: Option.Option<DerivationRunner>
}

/** Everything a sweep needs. Build one with {@link ingestOptions}. */
export interface IngestOptions {
  /** The `runs_root` to walk. Resolved with {@link resolveRunsRoot} on entry. */
  readonly runsRoot: string
  /** Restrict the sweep to these ids. Empty means "every run under the root". */
  readonly gameIds: ReadonlySet<string>
  /** Read and compare, write nothing. */
  readonly dryRun: boolean
  /**
   * Delete stored games that are not on the disk. Opt-in because the schema has
   * no `runs_root` column: an unscoped prune against a database that also holds
   * another archive deletes rows whose files are alive.
   */
  readonly prune: boolean
  /** How much `replay.jsonl` is decoded. See {@link DEFAULT_MAX_REPLAY_BYTES}. */
  readonly maxReplayBytes: number
  /** The board phase, or `Option.none()` for `--skip-boards`. */
  readonly boards: Option.Option<BoardOptions>
}

/** The checkout this package lives in — the bridge's default `repoRoot`. */
export const PACKAGE_REPO_ROOT: string = fileURLToPath(new URL("../../..", import.meta.url))

/** Default board options for a cache root: every turn, four games at a time. */
export const boardOptions = (cacheRoot: string): BoardOptions => ({
  cacheRoot,
  repoRoot: PACKAGE_REPO_ROOT,
  turnLimit: 0,
  concurrency: 4,
  runner: Option.none()
})

/**
 * Defaults for a root: whole sweep, real writes, no pruning, **no boards**.
 *
 * Boards are off by default because they spawn processes: a caller that wants
 * them says so, and the parity rig — which must not couple the oracle to
 * `board_state` at all — gets the cheap sweep for free.
 */
export const ingestOptions = (runsRoot: string): IngestOptions => ({
  runsRoot,
  gameIds: new Set<string>(),
  dryRun: false,
  prune: false,
  maxReplayBytes: DEFAULT_MAX_REPLAY_BYTES,
  boards: Option.none()
})

/**
 * The `runs_root` could not be listed.
 *
 * The sweep aborts and nothing is deleted. This is the one place ingest is
 * *less* tolerant than the gateway, on purpose: the gateway answers an
 * unreadable root with an empty index for one request, while an ingester that
 * reported success on a root it never read would licence a deletion pass over
 * an archive it cannot see.
 */
export class RunsRootUnreadable extends Data.TaggedError("RunsRootUnreadable")<{
  readonly runsRoot: string
}> {}

/**
 * The board cache root is unusable.
 *
 * `save_replay` raises `SaveReplayError` when the cache root nests with a run's
 * `saves/` directory, and a cache root inside `runs_root` would also make the
 * sweep's own output look like archive content on the next walk. Both are
 * command-line mistakes, so they fail the sweep before it writes anything.
 */
export class BoardCacheRootInvalid extends Data.TaggedError("BoardCacheRootInvalid")<{
  readonly cacheRoot: string
  readonly runsRoot: string
}> {}

/** Everything {@link ingest} can fail with. */
export type IngestError = SqlError | RunsRootUnreadable | BoardCacheRootInvalid

// ---------------------------------------------------------------------------
// Report
// ---------------------------------------------------------------------------

/** Why a directory entry, a document, a row or a board produced nothing. */
export type SkipReason =
  /** The name fails `GAME_ID_RE`. */
  | "notAGameId"
  /** The entry is a symlink — a run may not point out of `runs_root`. */
  | "symlink"
  /** The entry is not a directory. */
  | "notADirectory"
  /** It resolves outside `runs_root`. */
  | "escapesRunsRoot"
  /** `--game` named an id that is not a run under the root. */
  | "requestedButAbsent"
  /**
   * A document parsed but carries U+0000 or a lone surrogate, which no column
   * in this schema can hold. Stored `unusable`; a declared divergence.
   */
  | "documentUnstorable"
  /** `replay.jsonl` is larger than `maxReplayBytes`; later turns are missing. */
  | "replayTruncated"
  /** A replay line, seat or player row could not be represented and was dropped. */
  | "rowUnstorable"
  /** The bridge had no artifacts for a turn, or refused it. Retried next sweep. */
  | "boardUnavailable"
  /** An autosave exists for a turn `replay.jsonl` never recorded (FK forbids it). */
  | "boardTurnNotRecorded"
  /**
   * The database refused this run's rows. Its transaction rolled back, the run
   * kept whatever it had before, and **the sweep carried on** — the fs backend
   * answers per run, so one hostile run may not keep the rest of an archive out
   * of the database.
   */
  | "databaseRefused"

/** One entry, document, row or board that did not make it in. */
export interface IngestSkip {
  /** The directory entry, which is a game id only when `reason` says so. */
  readonly entry: string
  readonly reason: SkipReason
  /** What exactly, for the reasons that scope to something inside a run. */
  readonly detail: Option.Option<string>
}

const skip = (
  entry: string,
  reason: SkipReason,
  detail?: string
): IngestSkip => ({
  entry,
  reason,
  detail: detail === undefined ? Option.none() : Option.some(detail)
})

/**
 * Rows actually written, by table.
 *
 * "Actually" is exact: every statement returns the rows it changed, and the
 * `IS DISTINCT FROM` guards mean an unchanged row inside a changed run does not
 * count.
 */
export interface WriteCounters {
  readonly games: number
  readonly seats: number
  readonly turns: number
  readonly playerTurns: number
  readonly agentStats: number
  readonly boards: number
  /** Rows removed: stale children of a changed run, plus pruned games. */
  readonly deletes: number
}

/** No rows written. */
export const NO_WRITES: WriteCounters = {
  games: 0,
  seats: 0,
  turns: 0,
  playerTurns: 0,
  agentStats: 0,
  boards: 0,
  deletes: 0
}

/** Add two counter sets. */
export const addWrites = (left: WriteCounters, right: WriteCounters): WriteCounters => ({
  games: left.games + right.games,
  seats: left.seats + right.seats,
  turns: left.turns + right.turns,
  playerTurns: left.playerTurns + right.playerTurns,
  agentStats: left.agentStats + right.agentStats,
  boards: left.boards + right.boards,
  deletes: left.deletes + right.deletes
})

const sumWrites = (parts: ReadonlyArray<WriteCounters>): WriteCounters =>
  parts.reduce(addWrites, NO_WRITES)

/** Every row written — the number that must be `0` when nothing changed. */
export const dataWrites = (counters: WriteCounters): number =>
  counters.games +
  counters.seats +
  counters.turns +
  counters.playerTurns +
  counters.agentStats +
  counters.boards +
  counters.deletes

/** What one run's document transaction did. */
export type RunOutcome = "inserted" | "updated" | "unchanged"

/** One run, as the sweep saw it. */
export interface RunResult {
  readonly gameId: string
  readonly outcome: RunOutcome
  /** `manifest_status`, as stored. */
  readonly manifestStatus: DocumentStatus
  /** `report_status`, as stored. */
  readonly reportStatus: DocumentStatus
  /** Turns whose board was derived and stored by this sweep. */
  readonly boardsWritten: number
  readonly writes: WriteCounters
}

/** The outcome of one sweep. */
export interface IngestReport {
  /** The resolved root the sweep walked. */
  readonly runsRoot: string
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

const skipLine = (entry: IngestSkip): string =>
  `    ${entry.entry}: ${entry.reason}${
    Option.match(entry.detail, { onNone: () => "", onSome: (detail) => ` (${detail})` })
  }`

/** The CLI's summary. Contains no connection string, by construction. */
export const describeReport = (report: IngestReport): string => {
  const writes = report.writes
  const lines: ReadonlyArray<string> = [
    `runs_root      ${report.runsRoot}${report.dryRun ? " (dry run)" : ""} — ${report.status}`,
    `entries seen   ${report.seen}`,
    `games          ${count(report.runs, "inserted")} inserted, ${
      count(report.runs, "updated")
    } updated, ${count(report.runs, "unchanged")} unchanged, ${report.deleted.length} deleted`,
    `rows written   ${dataWrites(writes)} (games ${writes.games}, seats ${writes.seats}, ` +
    `turns ${writes.turns}, player_turns ${writes.playerTurns}, ` +
    `agent_stats ${writes.agentStats}, boards ${writes.boards}, deletes ${writes.deletes})`,
    `writing txns   ${report.transactionsWithWrites}`,
    // Called out on its own line rather than left to be counted out of the skip
    // list: a run the database refused, and a document this schema cannot hold,
    // are the two skips an operator has to act on.
    `refused runs   ${
      report.skipped.filter((entry) => entry.reason === "databaseRefused").length
    }`,
    `unstorable     ${
      report.skipped.filter((entry) => entry.reason === "documentUnstorable").length
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
 * fstat size is stored separately from the digest of what was read.
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
 * `flags` is the caller's, because the asymmetry matters: `manifest.json` and
 * `report.json` are opened with `O_NOFOLLOW`, while `replay.jsonl` is not —
 * `makeLastReplayTurn` follows a symlinked replay, and hardening it here would
 * change an answer.
 */
const readHead = (
  path: string,
  flags: number,
  cap: number
): Either.Either<ReadFile, FsFailure> =>
  Either.flatMap(attempt("open", path, () => openSync(path, flags)), (fd) => {
    const result = Either.flatMap(statFd(fd), (stat) =>
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
 * `_safe_archive_directory` (`replay_gateway.py:871`) — `saves/` must exist,
 * not be a symlink, be a directory, and resolve to a direct child of the run
 * root. The board phase needs it; nothing is stored about it.
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

// ---------------------------------------------------------------------------
// Hashing, decoding, and what Postgres can actually hold
// ---------------------------------------------------------------------------

const sha256 = (bytes: Uint8Array): Uint8Array =>
  Uint8Array.from(createHash("sha256").update(bytes).digest())

const hex = (bytes: Uint8Array): string =>
  Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("")

const UTF8 = new TextDecoder("utf-8", { fatal: true })

/** `bytes.decode("utf-8")` — strict, so a `UnicodeError` stays a failure. */
const decodeUtf8 = (bytes: Uint8Array): Option.Option<string> =>
  Option.getRight(Either.try(() => UTF8.decode(bytes)))



/**
 * Can Postgres hold this string faithfully?
 *
 * Measured on PGlite 0.5.x: a `text` column refuses U+0000 outright
 * (`invalid byte sequence for encoding "UTF8": 0x00`) and silently rewrites a
 * lone surrogate to U+FFFD; `jsonb` refuses both. There is no repair that is
 * not a lie, so a string containing either is *unstorable* and the caller
 * decides what that costs.
 */
export const isStorableText = (text: string): boolean =>
  !text.includes("\u0000") && Option.isNone(findLoneSurrogate(text))

const firstSome = (found: ReadonlyArray<Option.Option<string>>): Option.Option<string> =>
  found.find(Option.isSome) ?? Option.none()

/** The first key or string in `value` Postgres cannot hold, as a JSON pointer. */
const unstorableAt = (value: JsonValue, path: string): Option.Option<string> => {
  if (typeof value === "string") {
    return isStorableText(value) ? Option.none() : Option.some(path === "" ? "/" : path)
  }
  if (Array.isArray(value)) {
    return firstSome(value.map((entry, index) => unstorableAt(entry, `${path}/${String(index)}`)))
  }
  if (isJsonObject(value)) {
    return firstSome(
      Object.entries(value).map(([key, entry]) =>
        isStorableText(key)
          ? unstorableAt(entry, `${path}/${pointerToken(key)}`)
          : Option.some(`${path}/<key>`)
      )
    )
  }
  return Option.none()
}

/** RFC 6901 token escaping, so a pointer stays readable and unambiguous. */
const pointerToken = (key: string): string => key.replaceAll("~", "~0").replaceAll("/", "~1")

/** Every JSON pointer at which the source document spelled a Python integer. */
const integerPointers = (value: CanonValue, path: string): ReadonlyArray<string> =>
  typeof value === "bigint"
    ? [path === "" ? "/" : path]
    : Array.isArray(value)
    ? value.flatMap((entry, index) => integerPointers(entry, `${path}/${String(index)}`))
    : typeof value === "object" && value !== null
    ? Object.entries(value).flatMap(([key, entry]) =>
      integerPointers(entry, `${path}/${pointerToken(key)}`)
    )
    : []

/** A canonical document encoded for jsonb, with integer paths carried separately. */
const storedJson = (value: CanonValue): JsonValue => {
  if (typeof value === "bigint") {
    return Number.isSafeInteger(Number(value)) ? Number(value) : String(value)
  }
  if (Array.isArray(value)) {
    return value.map(storedJson)
  }
  if (typeof value === "object" && value !== null) {
    return Object.fromEntries(Object.entries(value).map(([key, entry]) => [key, storedJson(entry)]))
  }
  return value
}

interface StoredDocumentObject {
  readonly value: JsonObject
  readonly integerPointers: ReadonlyArray<string>
}

const storedDocumentObject = (bytes: Uint8Array): Option.Option<StoredDocumentObject> =>
  Option.flatMap(
    Option.flatMap(decodeUtf8(bytes), (text) => Option.getRight(parsePythonJsonObject(text))),
    (record: CanonRecord) => {
      const value = storedJson(record)
      return isJsonObject(value)
        ? Option.some({ value, integerPointers: integerPointers(record, "") })
        : Option.none()
    }
  )

/** Every JSON pointer at which `value` carries a negative zero. */
const negativeZeroPointers = (value: JsonValue, path: string): ReadonlyArray<string> =>
  typeof value === "number"
    ? Object.is(value, -0) ? [path === "" ? "/" : path] : []
    : Array.isArray(value)
    ? value.flatMap((entry, index) => negativeZeroPointers(entry, `${path}/${String(index)}`))
    : isJsonObject(value)
    ? Object.entries(value).flatMap(([key, entry]) =>
      negativeZeroPointers(entry, `${path}/${pointerToken(key)}`)
    )
    : []

/**
 * A `CanonValue` as plain JSON, refusing to launder.
 *
 * `canonToJson` (the gateway's, for `resolved_places`) maps every `bigint`
 * through `Number`, which is right for places — they are small and the loaders
 * want plain JSON — and wrong for a column: `9007199254740993` would arrive as
 * `…992` with nothing said. Here an unsafe integer makes the whole value
 * unstorable, and the caller stores `NULL` instead of a quiet lie.
 */
const exactJson = (value: CanonValue): Option.Option<JsonValue> => {
  if (typeof value === "bigint") {
    return Number.isSafeInteger(Number(value))
      ? Option.some(Number(value))
      : Option.none()
  }
  if (typeof value === "string") {
    return isStorableText(value) ? Option.some(value) : Option.none()
  }
  if (Array.isArray(value)) {
    return Option.map(
      Option.all(value.map(exactJson)),
      (parts): JsonValue => parts
    )
  }
  if (typeof value === "object" && value !== null) {
    return Option.map(
      Option.all(
        Object.entries(value).map(([key, entry]) =>
          isStorableText(key)
            ? Option.map(exactJson(entry), (part) => [key, part] as const)
            : Option.none<readonly [string, JsonValue]>()
        )
      ),
      (pairs): JsonValue => Object.fromEntries(pairs)
    )
  }
  return typeof value === "number" || typeof value === "boolean" || value === null
    ? Option.some(value)
    : Option.none()
}

// ---------------------------------------------------------------------------
// Column guards
//
// Every one of these is a *demotion* decision, and every one of them exists
// because the driver does something silent when it is skipped:
// `bigint({mode:"number"})` launders 9007199254740993 to …992 with no error,
// and `integer` rounds 5.5 to 6.
// ---------------------------------------------------------------------------

/** A JSON number that a `double precision` column holds exactly. */
const float8Of = (value: JsonValue | undefined): Option.Option<number> =>
  typeof value === "number" && !Object.is(value, -0) ? Option.some(value) : Option.none()

/** A JSON number that an `integer` column holds exactly. */
const int32Of = (value: JsonValue | undefined): Option.Option<number> =>
  typeof value === "number" &&
    Number.isInteger(value) &&
    !Object.is(value, -0) &&
    value >= INT32_MIN &&
    value <= INT32_MAX
    ? Option.some(value)
    : Option.none()

/** A JSON number that a `bigint({mode:"number"})` column holds exactly. */
const safeIntOf = (value: JsonValue | undefined): Option.Option<number> =>
  typeof value === "number" && Number.isSafeInteger(value) && !Object.is(value, -0)
    ? Option.some(value)
    : Option.none()

const booleanOf = (value: JsonValue | undefined): Option.Option<boolean> =>
  typeof value === "boolean" ? Option.some(value) : Option.none()

/** A string a `text` column holds faithfully. */
const textOf = (value: JsonValue | undefined): Option.Option<string> =>
  typeof value === "string" && isStorableText(value) ? Option.some(value) : Option.none()

/** A `jsonb` value: anything JSON, as long as every string in it can be stored. */
const jsonbOf = (value: JsonValue | undefined): Option.Option<JsonValue> =>
  value === undefined || Option.isSome(unstorableAt(value, ""))
    ? Option.none()
    : Option.some(value)

// ---------------------------------------------------------------------------
// Canonical (`parsePythonJson`) accessors, for `replay.jsonl`
//
// The replay tail is read the way the fs backend reads it — a Python `int` is a
// `bigint`, a Python `float` is a `number` — because `_last_replay_turn`'s
// `isinstance(turn, int)` is what makes `{"turn": 2.0}` answer "no turn". The
// same reader is used for the whole file so that `9007199254740993` in a row is
// *seen* as itself rather than as the double it would have collapsed to.
// ---------------------------------------------------------------------------

const canonField = (value: CanonValue, key: string): CanonValue | undefined =>
  typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as { readonly [k: string]: CanonValue })[key]
    : undefined

/** An `integer` column from a canonical value: a Python `int` or an integral float. */
const canonInt32 = (value: CanonValue | undefined): Option.Option<number> =>
  typeof value === "bigint"
    ? value >= BigInt(INT32_MIN) && value <= BigInt(INT32_MAX)
      ? Option.some(Number(value))
      : Option.none()
    : int32Of(typeof value === "number" ? value : undefined)

const canonSafeInt = (value: CanonValue | undefined): Option.Option<number> =>
  typeof value === "bigint"
    ? Number.isSafeInteger(Number(value)) && BigInt(Number(value)) === value
      ? Option.some(Number(value))
      : Option.none()
    : safeIntOf(typeof value === "number" ? value : undefined)

const canonFloat = (value: CanonValue | undefined): Option.Option<number> =>
  typeof value === "bigint"
    ? Number.isSafeInteger(Number(value)) ? Option.some(Number(value)) : Option.none()
    : float8Of(typeof value === "number" ? value : undefined)

const canonText = (value: CanonValue | undefined): Option.Option<string> =>
  typeof value === "string" && isStorableText(value) ? Option.some(value) : Option.none()

const canonBoolean = (value: CanonValue | undefined): Option.Option<boolean> =>
  typeof value === "boolean" ? Option.some(value) : Option.none()

const canonJsonb = (value: CanonValue | undefined): Option.Option<JsonValue> =>
  value === undefined ? Option.none() : exactJson(value)

// ---------------------------------------------------------------------------
// The three documents of one run
// ---------------------------------------------------------------------------

/** `document_status`, as the frozen enum spells it. */
export type DocumentStatus = "ok" | "unusable" | "absent"

/** One archive document, as ingest judged it. */
export interface ReadDocument {
  readonly file: string
  readonly status: DocumentStatus
  /** The **fstat** size that the gate saw. `0` when the file is absent. */
  readonly byteSize: number
  /** sha256 over the bytes read. Empty when the file is absent. */
  readonly digest: Uint8Array
  /** The parsed object, present exactly when `status === 'ok'`. */
  readonly value: Option.Option<JsonObject>
  /** JSON pointers whose source tokens were Python integers rather than floats. */
  readonly integerPointers: ReadonlyArray<string>
  /** Where the document held something Postgres cannot store, if it did. */
  readonly unstorable: Option.Option<string>
}

const ABSENT_DIGEST = new Uint8Array(0)

/**
 * `manifest.json` / `report.json`, per `_read_archive_json`
 * (`replay_gateway.py:573`) plus this schema's storability policy.
 *
 * `open` failing is `absent` — the fs backend's 404. A file that opens and
 * fails a gate is `unusable` — the fs backend's 503 — and its `byte_size` is
 * the fstat size *that failed the gate*, not the number of bytes read (a 3 GiB
 * document is `unusable` at 3 GiB, having been read only to the 8 MiB cap).
 */
const readDocument = (runRoot: string, file: string): ReadDocument => {
  const read = Option.getRight(
    readHead(join(runRoot, file), O_RDONLY | O_NOFOLLOW | O_CLOEXEC, MAX_DOCUMENT_BYTES)
  )
  return Option.match(read, {
    onNone: (): ReadDocument => ({
      file,
      status: "absent",
      byteSize: 0,
      digest: ABSENT_DIGEST,
      value: Option.none(),
      integerPointers: [],
      unstorable: Option.none()
    }),
    onSome: (found) => {
      const parsed = found.regular && found.size > 0 && found.size <= MAX_DOCUMENT_BYTES
        ? storedDocumentObject(found.bytes)
        : Option.none<StoredDocumentObject>()
      const unstorable = Option.flatMap(parsed, ({ value }) => unstorableAt(value, ""))
      return {
        file,
        status: Option.isNone(parsed) || Option.isSome(unstorable) ? "unusable" : "ok",
        byteSize: found.size,
        digest: sha256(found.bytes),
        value: Option.isSome(unstorable) ? Option.none() : Option.map(parsed, ({ value }) => value),
        integerPointers: Option.isSome(unstorable)
          ? []
          : Option.match(parsed, {
            onNone: (): ReadonlyArray<string> => [],
            onSome: ({ integerPointers: pointers }) => pointers
          }),
        unstorable
      }
    }
  })
}

// ---------------------------------------------------------------------------
// `replay.jsonl` — the tail answer, and the rows
// ---------------------------------------------------------------------------

/**
 * `bytes.splitlines()` — `\n`, `\r` and `\r\n`, and nothing else.
 *
 * Latin-1 is a lossless byte↔code-unit mapping, so splitting there and mapping
 * back gives exactly the byte ranges Python's `splitlines` produces, without a
 * UTF-8 decode that a torn multi-byte sequence at a window boundary would fail.
 */
/**
 * `bytes.decode("latin-1")`, spelled the way `services/runs.ts` spells it.
 *
 * Not `new TextDecoder("latin1")`: Bun's `TextDecoder` types admit only
 * `utf-8` / `utf-16` / `windows-1252`, and `windows-1252` is *not* latin-1 —
 * it maps 0x80–0x9F to typographic characters instead of to the control range,
 * which would move a byte's code unit and break the round trip
 * {@link latin1Bytes} depends on. The chunking is only so that a 256 MiB replay
 * does not spread a quarter of a billion arguments into one call.
 */
const LATIN1_CHUNK = 8192

const decodeLatin1 = (bytes: Uint8Array): string =>
  Array.from(
    { length: Math.ceil(bytes.length / LATIN1_CHUNK) },
    (_, index) =>
      String.fromCharCode(
        ...bytes.subarray(index * LATIN1_CHUNK, (index + 1) * LATIN1_CHUNK)
      )
  ).join("")

const splitLines = (bytes: Uint8Array): ReadonlyArray<string> =>
  decodeLatin1(bytes).split(/\r\n|\n|\r/)

/** `bytes.strip()` — ASCII whitespace only, which is what `if not raw.strip()` tests. */
const BLANK_LINE_RE = /^[ \t\v\f]*$/

const latin1Bytes = (line: string): Uint8Array =>
  Uint8Array.from(line, (character) => character.charCodeAt(0))

const parseLine = (line: string): Option.Option<CanonValue> =>
  Option.flatMap(
    decodeUtf8(latin1Bytes(line)),
    (text) => Option.getRight(parsePythonJson(text))
  )

/**
 * `_last_replay_turn` (`replay_gateway.py:1178`), computed at ingest.
 *
 * Scans the 64 KiB tail backwards, skipping blank lines and lines that do not
 * parse (a torn final write), and **stops at the first line that does**: a
 * parseable row without a positive integer `turn` answers "no turn", it does
 * not keep looking. That single `return None` is what hides a lobby husk from
 * the index instead of resurrecting an older turn — and it is why this column
 * is not `max(turns.turn)`.
 */
const lastReplayTurnOf = (bytes: Uint8Array, size: number): Option.Option<bigint> => {
  if (size <= 0) {
    return Option.none()
  }
  const parsed = splitLines(bytes)
    .filter((line) => !BLANK_LINE_RE.test(line))
    .toReversed()
    .map(parseLine)
    .find(Option.isSome)
  if (parsed === undefined) {
    return Option.none()
  }
  const turn = canonField(parsed.value, "turn")
  return typeof turn === "bigint" && turn > 0n ? Option.some(turn) : Option.none()
}

/** One `turns` row. */
export interface TurnRow {
  readonly turn: number
  readonly year: number | null
}

/** One `player_turns` row. */
export interface PlayerTurnRow {
  readonly turn: number
  readonly seatId: string
  readonly playerId: number | null
  readonly playerName: string | null
  readonly nation: string | null
  readonly government: string | null
  readonly alive: boolean | null
  readonly score: number | null
  readonly cities: number | null
  readonly citizens: number | null
  readonly population: number | null
  readonly units: number | null
  readonly gold: number | null
  readonly culture: number | null
  readonly futureTechs: number | null
  readonly knownTechIds: JsonValue | null
  readonly research: JsonValue | null
}

/** Everything `replay.jsonl` contributed. */
interface ReplayData {
  /** Whether the file exists at all. */
  readonly present: boolean
  /** The whole file's size — `<= 0` is what makes the tail answer "no turn". */
  readonly byteSize: number
  /** sha256 over the bytes decoded into rows (the whole file, up to the cap). */
  readonly digest: Uint8Array
  readonly lastReplayTurn: Option.Option<bigint>
  readonly turns: readonly TurnRow[]
  readonly playerTurns: readonly PlayerTurnRow[]
  readonly skips: readonly IngestSkip[]
}

const NO_REPLAY: ReplayData = {
  present: false,
  byteSize: 0,
  digest: ABSENT_DIGEST,
  lastReplayTurn: Option.none(),
  turns: [],
  playerTurns: [],
  skips: []
}

/** One player entry of one replay line, as a row — or a reason it is not one. */
const playerRow = (
  turn: number,
  entry: CanonValue
): Either.Either<PlayerTurnRow, string> => {
  const seatId = canonText(canonField(entry, "seat_id"))
  if (Option.isNone(seatId)) {
    return Either.left(`turn ${String(turn)}: a player row has no storable seat_id`)
  }
  return Either.right({
    turn,
    seatId: seatId.value,
    playerId: Option.getOrNull(canonInt32(canonField(entry, "player_id"))),
    playerName: Option.getOrNull(canonText(canonField(entry, "player_name"))),
    nation: Option.getOrNull(canonText(canonField(entry, "nation"))),
    government: Option.getOrNull(canonText(canonField(entry, "government"))),
    alive: Option.getOrNull(canonBoolean(canonField(entry, "alive"))),
    score: Option.getOrNull(canonFloat(canonField(entry, "score"))),
    cities: Option.getOrNull(canonInt32(canonField(entry, "cities"))),
    citizens: Option.getOrNull(canonInt32(canonField(entry, "citizens"))),
    population: Option.getOrNull(canonSafeInt(canonField(entry, "population"))),
    units: Option.getOrNull(canonInt32(canonField(entry, "units"))),
    gold: Option.getOrNull(canonFloat(canonField(entry, "gold"))),
    culture: Option.getOrNull(canonFloat(canonField(entry, "culture"))),
    futureTechs: Option.getOrNull(canonInt32(canonField(entry, "future_techs"))),
    knownTechIds: Option.getOrNull(canonJsonb(canonField(entry, "known_tech_ids"))),
    research: Option.getOrNull(canonJsonb(canonField(entry, "research")))
  })
}

/** The rows one parsed replay line contributes. */
interface LineRows {
  readonly turn: Option.Option<TurnRow>
  readonly players: readonly PlayerTurnRow[]
  readonly skips: readonly string[]
}

const NO_LINE_ROWS: LineRows = { turn: Option.none(), players: [], skips: [] }

/**
 * One `replay.jsonl` line as rows.
 *
 * `turn` must be a Python `int` inside int32 — the same int-ness the tail
 * reader demands, so a line the gateway would refuse to count is a line this
 * refuses to store, and an `integer` column never sees a value it would round.
 */
const lineRows = (value: CanonValue): LineRows => {
  const turn = canonField(value, "turn")
  if (typeof turn !== "bigint") {
    return NO_LINE_ROWS
  }
  const stored = canonInt32(turn)
  if (Option.isNone(stored)) {
    return { ...NO_LINE_ROWS, skips: [`turn ${String(turn)} does not fit an integer column`] }
  }
  const players = canonField(value, "players")
  const rows = (Array.isArray(players) ? players : []).map((entry) =>
    playerRow(stored.value, entry)
  )
  return {
    turn: Option.some({
      turn: stored.value,
      year: Option.getOrNull(canonInt32(canonField(value, "year")))
    }),
    players: Arr.getRights(rows),
    skips: Arr.getLefts(rows)
  }
}

/** Last write wins, keyed by `key`, preserving first-seen order. */
const dedupe = <A>(rows: readonly A[], key: (row: A) => string): readonly A[] => {
  const index = new Map<string, A>()
  rows.forEach((row) => index.set(key(row), row))
  return [...index.values()]
}

/**
 * `replay.jsonl`, read twice on purpose.
 *
 * The tail window is read positionally, exactly as `_last_replay_turn` reads
 * it, so `games.last_replay_turn` is the fs backend's answer rather than a
 * re-derivation of it. The whole file (up to `maxReplayBytes`) is then read for
 * the domain rows; if the cap cut it short the final line is dropped as
 * possibly torn and the run carries a `replayTruncated` skip.
 */
const readReplay = (
  runRoot: string,
  gameId: string,
  maxReplayBytes: number
): ReplayData => {
  const tail = Option.getRight(readTail(join(runRoot, REPLAY_JSONL_FILE)))
  if (Option.isNone(tail)) {
    return NO_REPLAY
  }
  const whole = Option.getRight(
    readHead(join(runRoot, REPLAY_JSONL_FILE), O_RDONLY | O_CLOEXEC, maxReplayBytes)
  )
  const bytes = Option.match(whole, {
    onNone: () => new Uint8Array(0),
    onSome: (read) => read.bytes
  })
  const truncated = tail.value.size > maxReplayBytes
  const lines = splitLines(bytes).filter((line) => !BLANK_LINE_RE.test(line))
  const decoded = (truncated ? lines.slice(0, -1) : lines)
    .map(parseLine)
    .flatMap(Option.toArray)
    .map(lineRows)
  return {
    present: true,
    byteSize: tail.value.size,
    digest: sha256(bytes),
    lastReplayTurn: lastReplayTurnOf(tail.value.bytes, tail.value.size),
    turns: dedupe(decoded.flatMap((row) => Option.toArray(row.turn)), (row) => String(row.turn)),
    playerTurns: dedupe(
      decoded.flatMap((row) => row.players),
      (row) => `${String(row.turn)}${row.seatId}`
    ),
    skips: [
      ...(truncated
        ? [skip(gameId, "replayTruncated", `${String(tail.value.size)} bytes`)]
        : []),
      ...decoded.flatMap((row) => row.skips).map((detail) => skip(gameId, "rowUnstorable", detail))
    ]
  }
}

// ---------------------------------------------------------------------------
// `manifest.json` → the `games` row
// ---------------------------------------------------------------------------

/**
 * `games.extras` — our own envelope, with exactly three reserved keys.
 *
 * `manifest` and `report` keep their JSON shape. Python integers are stored as
 * safe JSON numbers (or decimal strings when unsafe), while `derived` records
 * their JSON pointers so reconstruction can restore bigint/float spelling.
 */
export interface ExtrasEnvelope {
  /** Demoted and un-columned manifest keys, verbatim. Present iff `manifest_status = 'ok'`. */
  readonly manifest?: JsonObject
  /** The whole report document, verbatim. Present iff `report_status = 'ok'`. */
  readonly report?: JsonObject
  /** Facts ingest computed. Diagnostic, plus the one overflow carrier. */
  readonly derived?: JsonObject
}

/** The `games` columns lifted out of one manifest, plus its extras. */
export interface GameFields {
  readonly state: StorableRunState
  readonly schemaVersion: number | null
  readonly createdAt: number | null
  readonly startedAt: number | null
  readonly finishedAt: number | null
  readonly currentTurn: number | null
  readonly benchmarkValid: boolean | null
  readonly name: string | null
  readonly ruleset: string | null
  readonly mode: string | null
  readonly timingMode: string | null
  readonly maxTurns: number | null
  readonly objective: string | null
  readonly config: JsonValue | null
  readonly extrasManifest: JsonObject
}

/**
 * The manifest keys that have a typed column, and the guard each one takes.
 *
 * `game_id` is deliberately **not** here: it is never reconstructed from the
 * primary key. `games.game_id` is the *directory name* while the manifest's
 * `game_id` is a *claim*, and `game_parity_wrong_id_06` exists precisely
 * because they can differ — the fs backend answers 404 by re-running that
 * cross-check, and the pg backend can only do the same if the claim survives.
 * So it is stored unconditionally in `extras.manifest`.
 */
type ColumnGuard = (value: JsonValue | undefined) => Option.Option<JsonValue>

const COLUMN_GUARDS: ReadonlyArray<readonly [string, ColumnGuard]> = [
  ["schema_version", int32Of],
  ["created_at", float8Of],
  ["started_at", float8Of],
  ["finished_at", float8Of],
  ["current_turn", int32Of],
  ["benchmark_valid", booleanOf]
]

/** The keys of `manifest` that their column can hold, and so are not demoted. */
const columnizedKeys = (manifest: JsonObject): ReadonlyArray<string> =>
  COLUMN_GUARDS
    .filter(([key, guard]) => key in manifest && Option.isSome(guard(manifest[key])))
    .map(([key]) => key)

const columnValue = <A>(
  manifest: JsonObject,
  key: string,
  guard: (value: JsonValue | undefined) => Option.Option<A>
): A | null => (key in manifest ? Option.getOrNull(guard(manifest[key])) : null)

/**
 * `manifest.json` decoded into columns and `extras.manifest`.
 *
 * Every key of the document ends up in exactly one of the two, which is what
 * makes reconstruction a total function rather than a best effort.
 */
export const gameFields = (manifest: JsonObject): GameFields => {
  const rawState = manifest["state"]
  const stateColumn = isStorableRunState(rawState)
  const config = manifest["config"]
  // `config: null` is a **demotion**, not a column: `jsonb` holds a JSON `null`
  // perfectly well, but the column cannot then be told apart from an absent
  // key, and reconstruction would drop a key the document had. Every other
  // guard refuses `null` for the same reason; this one has to say so, because
  // `jsonbOf(null)` is legitimately `Some(null)`.
  const configColumn = "config" in manifest && config !== null
    ? jsonbOf(config)
    : Option.none<JsonValue>()
  const lifted = isJsonObject(config) ? config : {}
  const columnKeys = new Set<string>([
    // `'invalid'` is both an enum spelling and the column's unrecognized
    // sentinel; a manifest that spells it literally must ALSO keep the key in
    // extras, or reconstruction cannot tell it from an absent key.
    ...(stateColumn && rawState !== UNRECOGNIZED_STATE ? ["state"] : []),
    ...(Option.isSome(configColumn) ? ["config"] : []),
    ...columnizedKeys(manifest)
  ])
  return {
    state: isStorableRunState(rawState) ? rawState : UNRECOGNIZED_STATE,
    schemaVersion: columnValue(manifest, "schema_version", int32Of),
    createdAt: columnValue(manifest, "created_at", float8Of),
    startedAt: columnValue(manifest, "started_at", float8Of),
    finishedAt: columnValue(manifest, "finished_at", float8Of),
    currentTurn: columnValue(manifest, "current_turn", int32Of),
    benchmarkValid: columnValue(manifest, "benchmark_valid", booleanOf),
    // The lifted config columns are **write-only query projections**. Nothing
    // reconstructs from them and no response body may read one: the fs backend
    // applies `publicText(config.mode, 'unknown', 32)` and these have not been
    // through `publicText`.
    name: Option.getOrNull(textOf(lifted["name"])),
    ruleset: Option.getOrNull(textOf(lifted["ruleset"])),
    mode: Option.getOrNull(textOf(lifted["mode"])),
    timingMode: Option.getOrNull(textOf(lifted["timing_mode"])),
    maxTurns: Option.getOrNull(
      Option.orElse(int32Of(lifted["max_turns"]), () => int32Of(lifted["turns"]))
    ),
    objective: Option.getOrNull(textOf(lifted["objective"])),
    config: Option.getOrNull(configColumn),
    extrasManifest: Object.fromEntries(
      Object.entries(manifest).filter(([key]) => !columnKeys.has(key))
    )
  }
}

// ---------------------------------------------------------------------------
// `config.seats[]` + `manifest.resolved_places[]` → the `seats` rows
// ---------------------------------------------------------------------------

/** One `seats` row. */
export interface SeatRow {
  readonly seatIndex: number
  readonly seatId: string | null
  readonly kind: "agent" | "cpu" | null
  readonly label: string | null
  readonly fingerprint: string | null
  readonly metadata: JsonValue | null
}

/** The seat's identity, as a stable digest across games. */
const SEAT_IDENTITY_FIELDS = ["id", "type", "model", "instructions", "base_url", "options"] as const

/**
 * sha256 over the seat's identity config — "the same agent setup" across games.
 *
 * Canonical text, not `JSON.stringify`: `options` is an object whose key order
 * is the writer's, and a fingerprint that moved with key order would join
 * nothing. A seat the canonicaliser refuses (a lone surrogate, 100 levels of
 * nesting) simply has no fingerprint; it is a join key, not a response field.
 */
const seatFingerprint = (seat: JsonObject): Option.Option<string> =>
  Option.map(
    Option.all(
      SEAT_IDENTITY_FIELDS.map((field) =>
        Option.getRight(canonicalText((seat[field] ?? null) as CanonValue, CANON_ASCII))
      )
    ),
    (parts) => hex(sha256(new TextEncoder().encode(encodeFields(parts))))
  )

/**
 * `seat_kind` — `cpu` iff the seat is configured `native` **or** the resolved
 * place with the same `seat_id` is controlled by `native_classic_ai`.
 *
 * The reverse mapping is deliberately not here (see `seats.ts` if it is ever
 * wanted): it would invent `publicPlaces`' defaults from an enum that has
 * already thrown away which of label/type/model were recorded, and a place row
 * whose `controller_label` was absent publishes *no key* while the reverse map
 * would publish one.
 */
const seatKindOf = (seat: JsonObject, places: ReadonlyArray<JsonValue>): "agent" | "cpu" => {
  if (seat["type"] === "native") {
    return "cpu"
  }
  const seatId = seat["id"]
  const place = places.find((entry) =>
    isJsonObject(entry) && typeof seatId === "string" && entry["seat_id"] === seatId
  )
  return isJsonObject(place) && place["controller"] === Gateway.NATIVE_CONTROLLER ? "cpu" : "agent"
}

/**
 * The configured seat list.
 *
 * `label` is `controller_label` when it is a non-blank string and `NULL`
 * otherwise — **not** `NATIVE_CONTROLLER_LABEL`. `publicPlaces` applies
 * `orDefault(rawLabel, 'Freeciv Classic AI')` at serve time, and baking that
 * into storage would apply it twice.
 */
export const seatRows = (manifest: JsonObject): readonly SeatRow[] => {
  const config = manifest["config"]
  const configured = isJsonObject(config) ? config["seats"] : undefined
  const places = manifest["resolved_places"]
  const placeRows = Array.isArray(places) ? places : []
  return (Array.isArray(configured) ? configured : []).map((seat, seatIndex): SeatRow =>
    isJsonObject(seat)
      ? {
        seatIndex,
        seatId: Option.getOrNull(textOf(seat["id"])),
        kind: seatKindOf(seat, placeRows),
        label: Option.getOrNull(
          Option.filter(textOf(seat["controller_label"]), (label) => label.trim() !== "")
        ),
        fingerprint: Option.getOrNull(seatFingerprint(seat)),
        metadata: Option.getOrNull(jsonbOf(seat["controller_metadata"]))
      }
      : {
        seatIndex,
        seatId: null,
        kind: null,
        label: null,
        fingerprint: null,
        metadata: null
      }
  )
}

// ---------------------------------------------------------------------------
// `report.json` → `agent_stats` (a projection, never a source of truth)
// ---------------------------------------------------------------------------

/** One `agent_stats` row. */
export interface AgentStatRow {
  readonly seatId: string
  readonly controllerFingerprint: string | null
  readonly turns: number | null
  readonly decisions: number | null
  readonly fallbacks: number | null
  readonly inputTokens: number | null
  readonly outputTokens: number | null
  readonly meanLatencyMs: number | null
}

/**
 * `report.seat_stats` as rows.
 *
 * This is a **queryable projection**, exactly the relationship `games.mode` has
 * to `config`: the whole report document lives in `extras.report` and
 * reconstruction reads only that. The alternative — partition `seat_stats` out
 * and rebuild it — would have to reproduce `mean_latency_ms`'s int-vs-float
 * spelling (`scoring.py` writes the integer `0` when `decisions === 0`, a float
 * otherwise) and its `latency_ms` accumulator sibling, which exists exactly
 * then and has no column. That value is unrecoverable from these rows.
 */
export const agentStatRows = (report: JsonObject): readonly AgentStatRow[] => {
  const stats = report["seat_stats"]
  return isJsonObject(stats)
    ? Object.entries(stats).flatMap(([seatId, value]): readonly AgentStatRow[] =>
      isStorableText(seatId) && isJsonObject(value)
        ? [{
          seatId,
          controllerFingerprint: Option.getOrNull(textOf(value["controller_fingerprint"])),
          turns: Option.getOrNull(int32Of(value["turns"])),
          decisions: Option.getOrNull(int32Of(value["decisions"])),
          fallbacks: Option.getOrNull(int32Of(value["fallbacks"])),
          inputTokens: Option.getOrNull(safeIntOf(value["input_tokens"])),
          outputTokens: Option.getOrNull(safeIntOf(value["output_tokens"])),
          meanLatencyMs: Option.getOrNull(float8Of(value["mean_latency_ms"]))
        }]
        : []
    )
    : []
}

// ---------------------------------------------------------------------------
// `content_hash`
// ---------------------------------------------------------------------------

/**
 * Fields as one unambiguous string: `<length>:<field>` per field, concatenated.
 *
 * The length is in the same units the field is measured in (UTF-16 code units),
 * so the encoding is injective by construction — a reader takes the prefix,
 * consumes exactly that many units, and starts again. Nothing a field contains
 * can imitate a separator, because there is none.
 *
 * This is what makes the content hash an idempotency key rather than a
 * suggestion. With a separator-joined encoding, a crafted status or filename
 * reproduces a whole record, and since an equal hash means "commit nothing", a
 * forged listing re-ingests as *unchanged* over a real one.
 */
export const encodeFields = (fields: ReadonlyArray<string>): string =>
  fields.map((field) => `${String(field.length)}:${field}`).join("")

/**
 * The listing this version of ingest hashes. Bumped when the decoded surface
 * changes, so an upgrade re-reads every run instead of trusting a hash that
 * was taken over different questions.
 */
const HASH_HEADER = "arena-ingest v3\n"

interface HashEntry {
  readonly category: string
  readonly name: string
  readonly byteSize: number
  readonly status: string
  readonly digest: Uint8Array
}

const contentHash = (entries: readonly HashEntry[]): Uint8Array => {
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
  return sha256(new TextEncoder().encode(HASH_HEADER + lines.join("")))
}

// ---------------------------------------------------------------------------
// One run, read
// ---------------------------------------------------------------------------

/** Everything one run directory contributes to the database. */
export interface RunData {
  readonly gameId: string
  readonly manifest: ReadDocument
  readonly report: ReadDocument
  readonly replay: ReplayData
  readonly fields: Option.Option<GameFields>
  readonly seats: readonly SeatRow[]
  readonly agentStats: readonly AgentStatRow[]
  readonly extras: ExtrasEnvelope
  /** `games.last_replay_turn`, `NULL` when the answer does not fit `integer`. */
  readonly lastReplayTurn: number | null
  readonly contentHash: Uint8Array
  readonly skips: readonly IngestSkip[]
}

/**
 * `extras.derived` — the two facts that are not document keys.
 *
 * `last_replay_turn` is a decimal **string**, present only when the tail's
 * answer does not fit `integer`: the turn is an unbounded Python `int` read
 * through `parsePythonJson`, and `lastReplayTurn` must answer the same `bigint`
 * the fs backend answers.
 *
 * `manifest_integer_pointers` / `report_integer_pointers` preserve CPython's
 * integer-vs-float distinction through jsonb. `manifest_negative_zero` /
 * `report_negative_zero` record pointers at which a `-0` was flattened;
 * Postgres cannot carry that sign, so the loss remains explicit.
 */
const derivedFacts = (
  manifest: ReadDocument,
  report: ReadDocument,
  lastReplayTurn: Option.Option<bigint>,
  storedLastReplayTurn: number | null
): JsonObject => {
  const manifestZeros = Option.match(manifest.value, {
    onNone: (): ReadonlyArray<string> => [],
    onSome: (value) => negativeZeroPointers(value, "")
  })
  const reportZeros = Option.match(report.value, {
    onNone: (): ReadonlyArray<string> => [],
    onSome: (value) => negativeZeroPointers(value, "")
  })
  return {
    ...(storedLastReplayTurn === null && Option.isSome(lastReplayTurn)
      ? { last_replay_turn: String(lastReplayTurn.value) }
      : {}),
    ...(manifest.integerPointers.length > 0
      ? { manifest_integer_pointers: [...manifest.integerPointers] }
      : {}),
    ...(report.integerPointers.length > 0
      ? { report_integer_pointers: [...report.integerPointers] }
      : {}),
    ...(manifestZeros.length > 0 ? { manifest_negative_zero: [...manifestZeros] } : {}),
    ...(reportZeros.length > 0 ? { report_negative_zero: [...reportZeros] } : {})
  }
}

const documentSkips = (gameId: string, document: ReadDocument): readonly IngestSkip[] =>
  Option.match(document.unstorable, {
    onNone: (): readonly IngestSkip[] => [],
    onSome: (pointer) => [
      skip(
        gameId,
        "documentUnstorable",
        `${document.file} holds U+0000 or a lone surrogate at ${pointer}`
      )
    ]
  })

/**
 * Read one run directory. Never fails: an unreadable document is a status, not
 * an exception, which is what the fs backend's 404/503 for the same file
 * already looks like from a client's side.
 */
export const collectRun = (
  runsRoot: string,
  gameId: string,
  options: IngestOptions
): RunData => {
  const runRoot = resolveStrictly(runsRoot, gameId)
  const manifest = readDocument(runRoot, MANIFEST_FILE)
  const report = readDocument(runRoot, REPORT_FILE)
  const replay = readReplay(runRoot, gameId, options.maxReplayBytes)

  const fields = Option.map(manifest.value, gameFields)
  const storedLastReplayTurn = Option.getOrNull(
    Option.filterMap(replay.lastReplayTurn, (turn) =>
      turn >= BigInt(INT32_MIN) && turn <= BigInt(INT32_MAX)
        ? Option.some(Number(turn))
        : Option.none())
  )
  const derived = derivedFacts(manifest, report, replay.lastReplayTurn, storedLastReplayTurn)

  return {
    gameId,
    manifest,
    report,
    replay,
    fields,
    seats: Option.match(manifest.value, {
      onNone: (): readonly SeatRow[] => [],
      onSome: seatRows
    }),
    agentStats: Option.match(report.value, {
      onNone: (): readonly AgentStatRow[] => [],
      onSome: agentStatRows
    }),
    extras: {
      ...Option.match(fields, {
        onNone: () => ({}),
        onSome: (value) => ({ manifest: value.extrasManifest })
      }),
      ...Option.match(report.value, {
        onNone: () => ({}),
        onSome: (value) => ({ report: value })
      }),
      ...(Object.keys(derived).length > 0 ? { derived } : {})
    },
    lastReplayTurn: storedLastReplayTurn,
    contentHash: contentHash([
      ...(manifest.status === "absent"
        ? []
        : [{
          category: "document",
          name: MANIFEST_FILE,
          byteSize: manifest.byteSize,
          status: manifest.status,
          digest: manifest.digest
        }]),
      ...(report.status === "absent"
        ? []
        : [{
          category: "document",
          name: REPORT_FILE,
          byteSize: report.byteSize,
          status: report.status,
          digest: report.digest
        }]),
      // The whole file, not v1's tail window: v2 decodes every line into
      // `turns`/`player_turns`, so a change in the middle of a long replay is a
      // change to the stored rows and must move the hash.
      ...(replay.present
        ? [{
          category: "replay",
          name: REPLAY_JSONL_FILE,
          byteSize: replay.byteSize,
          status: "ok",
          digest: replay.digest
        }]
        : [])
    ]),
    skips: [
      ...documentSkips(gameId, manifest),
      ...documentSkips(gameId, report),
      ...replay.skips
    ]
  }
}

/**
 * The `runs_root` as the repository resolves it (`replay_gateway.py:196`,
 * `Path(...).expanduser().resolve()`). The gateway resolves it identically and
 * both backends answer `runsRoot` with the same string.
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
 * Shared with {@link isRunOnDisk} deliberately: the prune phase re-asks this
 * exact question at the end of a sweep, and a second copy of the rule that
 * drifted would decide to delete a run the walk would have kept.
 */
const classifyEntry = (
  runsRoot: string,
  entry: string
): Either.Either<string, IngestSkip> => {
  if (!isGameId(entry)) {
    return Either.left(skip(entry, "notAGameId"))
  }
  if (isSymlink(join(runsRoot, entry))) {
    return Either.left(skip(entry, "symlink"))
  }
  const runRoot = resolveStrictly(runsRoot, entry)
  if (dirname(runRoot) !== runsRoot) {
    return Either.left(skip(entry, "escapesRunsRoot"))
  }
  return isDirectory(runRoot)
    ? Either.right(entry)
    : Either.left(skip(entry, "notADirectory"))
}

/**
 * Is `gameId` a run under `runsRoot` **now**?
 *
 * Asked inside the prune transaction, about ids the walk did not see. A run
 * that is on the disk at that moment was ingested by somebody else while this
 * sweep was reading, and deleting it — which is what a seen-set captured at
 * readdir time does — silently destroys another process's work.
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
      const absent = Array.from(filter)
        .filter((id) => !considered.includes(id))
        .map((entry): IngestSkip => skip(entry, "requestedButAbsent"))
      return Either.right({
        gameIds: Arr.getRights(classified),
        skipped: [...Arr.getLefts(classified), ...absent]
      })
    }
  })

// ---------------------------------------------------------------------------
// Writing
// ---------------------------------------------------------------------------

type Database = PgDrizzle.PgDrizzle["Type"]

const rowsWritten = (rows: ReadonlyArray<unknown>): number => rows.length

/**
 * The parameter budget for one statement — the **smaller** of the two ceilings
 * this code runs against, both measured rather than assumed.
 *
 * - **Live Postgres**: the extended protocol's `Bind` counts parameters in an
 *   unsigned int16 (65535) and refuses the statement past it.
 * - **PGlite 0.5.x**: the ceiling is a *signed* int16 (32767) and crossing it is
 *   **silent** — no error, no rows, and the transaction's earlier statements
 *   lost with it.
 *
 * The lower bound is the one to honour: it is the only value at which the
 * hermetic tests predict the live server.
 */
export const MAX_BIND_PARAMETERS = 32767

/** Room for the parameters a statement spends outside its `VALUES` list. */
const RESERVED_BIND_PARAMETERS = 64

/**
 * `rows` split so no chunk can exceed {@link MAX_BIND_PARAMETERS}.
 *
 * `Arr.chunksOf` on an empty array is `[]`, not `[[]]`, so a caller with
 * nothing to write issues no statement — which is what the idempotence contract
 * needs, since an empty `INSERT` is still a statement.
 */
export const chunkRows = <A>(
  rows: ReadonlyArray<A>,
  columnsPerRow: number
): ReadonlyArray<ReadonlyArray<A>> =>
  Arr.chunksOf(
    rows,
    Math.max(1, Math.floor((MAX_BIND_PARAMETERS - RESERVED_BIND_PARAMETERS) / columnsPerRow))
  )

const chunkedRowsWritten = (chunks: ReadonlyArray<ReadonlyArray<unknown>>): number =>
  chunks.reduce((total, rows) => total + rows.length, 0)

/**
 * Columns per row, **counted off the table** rather than written down.
 *
 * A hand-maintained count is one edit away from being wrong, and being wrong is
 * silent: `player_turns` was budgeted at 17 columns when the frozen schema
 * declares 18, so a chunk of 1 923 rows spent 34 614 parameters against a
 * ceiling of 32 767 — measured, PGlite accepted the statement, wrote **zero**
 * rows, returned no error, and took the rest of that run's transaction with it.
 * A game of ~1 000 turns and two seats is enough to reach it. The count now
 * comes from the same declaration the `INSERT` is built from, so the two cannot
 * disagree.
 */
export const columnCount = (table: Table): number => Object.keys(getTableColumns(table)).length

/**
 * `<column> <> all($1::<type>[])` — a whole delete predicate as **one**
 * parameter.
 *
 * `notInArray(column, values)` renders `x not in ($1, $2, …)`, which spends a
 * parameter per surviving value and so has the same int16 ceiling the inserts
 * do, one long game away. The explicit cast is required: without it the planner
 * sees an untyped parameter on the right of `ALL` and refuses. An empty array
 * makes the predicate true for every row, which is the wanted answer — nothing
 * is expected, so everything stored is stale.
 */
const notAmongInts = (column: PgColumn, values: ReadonlyArray<number>) =>
  sql`${column} <> all(${sql.param([...values])}::int[])`

const notAmongTexts = (column: PgColumn, values: ReadonlyArray<string>) =>
  sql`${column} <> all(${sql.param([...values])}::text[])`

/**
 * The `games` row.
 *
 * Reached only when the content hash already differs, so the `IS DISTINCT FROM`
 * guard is belt and braces — but it also keeps `ingested_at` still for a run
 * whose content did not move, which is what the column means.
 */
const upsertGame = (
  db: Database,
  data: RunData
): Effect.Effect<number, SqlError> => {
  const fields = Option.getOrNull(data.fields)
  const extras = Object.keys(data.extras).length > 0 ? data.extras : null
  return Effect.map(
    db
      .insert(games)
      .values({
        gameId: data.gameId,
        state: fields?.state ?? UNRECOGNIZED_STATE,
        schemaVersion: fields?.schemaVersion ?? null,
        createdAt: fields?.createdAt ?? null,
        startedAt: fields?.startedAt ?? null,
        finishedAt: fields?.finishedAt ?? null,
        currentTurn: fields?.currentTurn ?? null,
        lastReplayTurn: data.lastReplayTurn,
        benchmarkValid: fields?.benchmarkValid ?? null,
        name: fields?.name ?? null,
        ruleset: fields?.ruleset ?? null,
        mode: fields?.mode ?? null,
        timingMode: fields?.timingMode ?? null,
        maxTurns: fields?.maxTurns ?? null,
        objective: fields?.objective ?? null,
        config: fields?.config ?? null,
        manifestStatus: data.manifest.status,
        manifestByteSize: Option.getOrNull(safeIntOf(data.manifest.byteSize)),
        reportStatus: data.report.status,
        reportByteSize: Option.getOrNull(safeIntOf(data.report.byteSize)),
        contentHash: data.contentHash,
        extras
      })
      .onConflictDoUpdate({
        target: games.gameId,
        set: {
          state: sql`excluded.state`,
          schemaVersion: sql`excluded.schema_version`,
          createdAt: sql`excluded.created_at`,
          startedAt: sql`excluded.started_at`,
          finishedAt: sql`excluded.finished_at`,
          currentTurn: sql`excluded.current_turn`,
          lastReplayTurn: sql`excluded.last_replay_turn`,
          benchmarkValid: sql`excluded.benchmark_valid`,
          name: sql`excluded.name`,
          ruleset: sql`excluded.ruleset`,
          mode: sql`excluded.mode`,
          timingMode: sql`excluded.timing_mode`,
          maxTurns: sql`excluded.max_turns`,
          objective: sql`excluded.objective`,
          config: sql`excluded.config`,
          manifestStatus: sql`excluded.manifest_status`,
          manifestByteSize: sql`excluded.manifest_byte_size`,
          reportStatus: sql`excluded.report_status`,
          reportByteSize: sql`excluded.report_byte_size`,
          contentHash: sql`excluded.content_hash`,
          ingestedAt: sql`now()`,
          extras: sql`excluded.extras`
        },
        setWhere: sql`${games.contentHash} is distinct from excluded.content_hash`
      })
      .returning({ gameId: games.gameId }),
    rowsWritten
  )
}

const writeSeats = (
  db: Database,
  data: RunData
): Effect.Effect<WriteCounters, SqlError> =>
  Effect.gen(function*() {
    const removed = yield* db
      .delete(seats)
      .where(
        and(
          eq(seats.gameId, data.gameId),
          notAmongInts(seats.seatIndex, data.seats.map((row) => row.seatIndex))
        )
      )
      .returning({ seatIndex: seats.seatIndex })
    const written = yield* Effect.forEach(
      chunkRows(data.seats, columnCount(seats)),
      (chunk) =>
        db
          .insert(seats)
          .values(chunk.map((row) => ({ gameId: data.gameId, ...row })))
          .onConflictDoUpdate({
            target: [seats.gameId, seats.seatIndex],
            set: {
              seatId: sql`excluded.seat_id`,
              kind: sql`excluded.kind`,
              label: sql`excluded.label`,
              fingerprint: sql`excluded.fingerprint`,
              metadata: sql`excluded.metadata`
            },
            setWhere: sql`${seats.seatId} is distinct from excluded.seat_id
              or ${seats.kind} is distinct from excluded.kind
              or ${seats.label} is distinct from excluded.label
              or ${seats.fingerprint} is distinct from excluded.fingerprint
              or ${seats.metadata} is distinct from excluded.metadata`
          })
          .returning({ seatIndex: seats.seatIndex }),
      { concurrency: 1 }
    )
    return { ...NO_WRITES, seats: chunkedRowsWritten(written), deletes: rowsWritten(removed) }
  })

/**
 * `turns`, then `player_turns`.
 *
 * The delete comes first and is a *set difference*, not a truncate: a run whose
 * manifest changed keeps every turn row it already had, which is what keeps
 * `board_state` — whose composite FK cascades from `turns` — alive across an
 * unrelated edit.
 */
const writeTurns = (
  db: Database,
  data: RunData
): Effect.Effect<WriteCounters, SqlError> =>
  Effect.gen(function*() {
    const removed = yield* db
      .delete(turns)
      .where(
        and(
          eq(turns.gameId, data.gameId),
          notAmongInts(turns.turn, data.replay.turns.map((row) => row.turn))
        )
      )
      .returning({ turn: turns.turn })
    const written = yield* Effect.forEach(
      chunkRows(data.replay.turns, columnCount(turns)),
      (chunk) =>
        db
          .insert(turns)
          .values(chunk.map((row) => ({ gameId: data.gameId, turn: row.turn, year: row.year })))
          .onConflictDoUpdate({
            target: [turns.gameId, turns.turn],
            set: { year: sql`excluded.year` },
            setWhere: sql`${turns.year} is distinct from excluded.year`
          })
          .returning({ turn: turns.turn }),
      { concurrency: 1 }
    )
    return { ...NO_WRITES, turns: chunkedRowsWritten(written), deletes: rowsWritten(removed) }
  })

/**
 * The stale-row predicate for a two-column key, as **two** parameters.
 *
 * `unnest($1::int[], $2::text[])` walks the two arrays in step, so the whole
 * surviving key set costs two binds however many turns a game has — which
 * matters at 5 000 turns × 8 seats, where a pair-per-parameter predicate would
 * cross the int16 ceiling that PGlite crosses *silently*.
 */
const playerTurnsNotAmong = (rows: readonly PlayerTurnRow[]) =>
  sql`not exists (
    select 1 from unnest(
      ${sql.param(rows.map((row) => row.turn))}::int[],
      ${sql.param(rows.map((row) => row.seatId))}::text[]
    ) as expected(turn, seat_id)
    where expected.turn = ${playerTurns.turn} and expected.seat_id = ${playerTurns.seatId}
  )`

const writePlayerTurns = (
  db: Database,
  data: RunData
): Effect.Effect<WriteCounters, SqlError> =>
  Effect.gen(function*() {
    const removed = yield* db
      .delete(playerTurns)
      .where(
        and(eq(playerTurns.gameId, data.gameId), playerTurnsNotAmong(data.replay.playerTurns))
      )
      .returning({ turn: playerTurns.turn })
    const written = yield* Effect.forEach(
      chunkRows(data.replay.playerTurns, columnCount(playerTurns)),
      (chunk) =>
        db
          .insert(playerTurns)
          .values(chunk.map((row) => ({ gameId: data.gameId, ...row })))
          .onConflictDoUpdate({
            target: [playerTurns.gameId, playerTurns.turn, playerTurns.seatId],
            set: {
              playerId: sql`excluded.player_id`,
              playerName: sql`excluded.player_name`,
              nation: sql`excluded.nation`,
              government: sql`excluded.government`,
              alive: sql`excluded.alive`,
              score: sql`excluded.score`,
              cities: sql`excluded.cities`,
              citizens: sql`excluded.citizens`,
              population: sql`excluded.population`,
              units: sql`excluded.units`,
              gold: sql`excluded.gold`,
              culture: sql`excluded.culture`,
              futureTechs: sql`excluded.future_techs`,
              knownTechIds: sql`excluded.known_tech_ids`,
              research: sql`excluded.research`
            },
            setWhere: sql`${playerTurns.playerId} is distinct from excluded.player_id
              or ${playerTurns.playerName} is distinct from excluded.player_name
              or ${playerTurns.nation} is distinct from excluded.nation
              or ${playerTurns.government} is distinct from excluded.government
              or ${playerTurns.alive} is distinct from excluded.alive
              or ${playerTurns.score} is distinct from excluded.score
              or ${playerTurns.cities} is distinct from excluded.cities
              or ${playerTurns.citizens} is distinct from excluded.citizens
              or ${playerTurns.population} is distinct from excluded.population
              or ${playerTurns.units} is distinct from excluded.units
              or ${playerTurns.gold} is distinct from excluded.gold
              or ${playerTurns.culture} is distinct from excluded.culture
              or ${playerTurns.futureTechs} is distinct from excluded.future_techs
              or ${playerTurns.knownTechIds} is distinct from excluded.known_tech_ids
              or ${playerTurns.research} is distinct from excluded.research`
          })
          .returning({ turn: playerTurns.turn }),
      { concurrency: 1 }
    )
    return {
      ...NO_WRITES,
      playerTurns: chunkedRowsWritten(written),
      deletes: rowsWritten(removed)
    }
  })

const writeAgentStats = (
  db: Database,
  data: RunData
): Effect.Effect<WriteCounters, SqlError> =>
  Effect.gen(function*() {
    const removed = yield* db
      .delete(agentStats)
      .where(
        and(
          eq(agentStats.gameId, data.gameId),
          notAmongTexts(agentStats.seatId, data.agentStats.map((row) => row.seatId))
        )
      )
      .returning({ seatId: agentStats.seatId })
    const written = yield* Effect.forEach(
      chunkRows(data.agentStats, columnCount(agentStats)),
      (chunk) =>
        db
          .insert(agentStats)
          .values(chunk.map((row) => ({ gameId: data.gameId, ...row })))
          .onConflictDoUpdate({
            target: [agentStats.gameId, agentStats.seatId],
            set: {
              controllerFingerprint: sql`excluded.controller_fingerprint`,
              turns: sql`excluded.turns`,
              decisions: sql`excluded.decisions`,
              fallbacks: sql`excluded.fallbacks`,
              inputTokens: sql`excluded.input_tokens`,
              outputTokens: sql`excluded.output_tokens`,
              meanLatencyMs: sql`excluded.mean_latency_ms`
            },
            setWhere:
              sql`${agentStats.controllerFingerprint} is distinct from excluded.controller_fingerprint
              or ${agentStats.turns} is distinct from excluded.turns
              or ${agentStats.decisions} is distinct from excluded.decisions
              or ${agentStats.fallbacks} is distinct from excluded.fallbacks
              or ${agentStats.inputTokens} is distinct from excluded.input_tokens
              or ${agentStats.outputTokens} is distinct from excluded.output_tokens
              or ${agentStats.meanLatencyMs} is distinct from excluded.mean_latency_ms`
          })
          .returning({ seatId: agentStats.seatId }),
      { concurrency: 1 }
    )
    return { ...NO_WRITES, agentStats: chunkedRowsWritten(written), deletes: rowsWritten(removed) }
  })

/** The stored hash for a game, if it has ever been ingested. */
const storedHash = (
  db: Database,
  gameId: string
): Effect.Effect<Option.Option<Uint8Array>, SqlError> =>
  Effect.map(
    db.select({ contentHash: games.contentHash }).from(games).where(eq(games.gameId, gameId)),
    (rows) => Option.map(Option.fromNullable(rows[0]), (row) => row.contentHash)
  )

const sameBytes = (left: Uint8Array, right: Uint8Array): boolean =>
  left.length === right.length && left.every((byte, index) => byte === right[index])

/**
 * `pg_current_xact_id_if_assigned()` — `NULL` until the transaction writes.
 *
 * Reading it is free: the `_if_assigned` variant never allocates an id. This is
 * the hermetic proof behind {@link IngestReport.transactionsWithWrites}.
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

const runResult = (
  data: RunData,
  outcome: RunOutcome,
  writes: WriteCounters
): RunResult => ({
  gameId: data.gameId,
  outcome,
  manifestStatus: data.manifest.status,
  reportStatus: data.report.status,
  boardsWritten: 0,
  writes
})

/**
 * One run's documents, in one transaction.
 *
 * The hash comparison is a plain `SELECT`. A `FOR UPDATE` is deliberately
 * absent: a row lock is recorded in the tuple's `xmax`, so it assigns a
 * transaction id and would destroy the zero-write proof — while buying nothing,
 * because the upsert that follows is idempotent by content and atomic under
 * `ON CONFLICT`.
 */
const ingestRun = (
  db: Database,
  client: SqlClient.SqlClient,
  data: RunData
): Effect.Effect<RunWrite, SqlError> =>
  client.withTransaction(
    Effect.gen(function*() {
      const existing = yield* storedHash(db, data.gameId)
      const unchanged = Option.match(existing, {
        onNone: () => false,
        onSome: (hash) => sameBytes(hash, data.contentHash)
      })
      if (unchanged) {
        return {
          result: runResult(data, "unchanged", NO_WRITES),
          wroteInTransaction: yield* transactionWrote(client)
        }
      }
      const gameWrites = yield* upsertGame(db, data)
      const children = yield* Effect.all(
        [
          writeSeats(db, data),
          writeTurns(db, data),
          writePlayerTurns(db, data),
          writeAgentStats(db, data)
        ],
        { concurrency: 1 }
      )
      return {
        result: runResult(
          data,
          Option.isNone(existing) ? "inserted" : "updated",
          addWrites({ ...NO_WRITES, games: gameWrites }, sumWrites(children))
        ),
        wroteInTransaction: yield* transactionWrote(client)
      }
    })
  )

// ---------------------------------------------------------------------------
// The board phase — domain data, explicitly off the parity path
//
// `/v1/games/{id}/board.json` derives from the savegames on **every** request,
// in **both** backends, through the same python bridge. `board_state` exists so
// the database is self-sufficient for scrubbing, frame regeneration and replay
// later; it is written here and read by nothing the 3-way oracle looks at. A
// parity test that reads `board_state` is a defect.
// ---------------------------------------------------------------------------

/**
 * A path with every existing component resolved, even when the leaf does not
 * exist yet.
 *
 * `resolve` alone is not enough for {@link nestsWith}: a `--cache-root` under a
 * symlinked ancestor (`/var/folders/…` → `/private/var/folders/…` on darwin, the
 * shape every `mkdtemp` produces) is lexically unrelated to the realpath'd
 * `runs_root` and would slip past the check into the archive it was meant to be
 * kept out of. The cache root is *created* by the bridge, so its leaf is
 * routinely absent — hence the walk up to the deepest ancestor that resolves.
 */
const resolveExisting = (path: string): string => {
  const absolute = resolvePath(path)
  return Either.match(attempt("realpath", absolute, () => realpathSync(absolute)), {
    onRight: (resolved) => resolved,
    onLeft: () => {
      const parent = dirname(absolute)
      return parent === absolute ? absolute : join(resolveExisting(parent), basename(absolute))
    }
  })
}

/** Does one path nest with the other, in either direction? */
const nestsWith = (left: string, right: string): boolean => {
  const a = resolveExisting(left)
  const b = resolveExisting(right)
  return a === b || a.startsWith(`${b}/`) || b.startsWith(`${a}/`)
}

/**
 * The cache root must live outside `runs_root`.
 *
 * `save_replay` raises `SaveReplayError` when its cache root nests with a run's
 * `saves/` directory, and a cache root *inside* the archive would also make the
 * sweep's own output look like archive content on the next walk.
 */
export const boardCacheRootValid = (runsRoot: string, cacheRoot: string): boolean =>
  !nestsWith(runsRoot, cacheRoot)

/** `turn-N-(auto|final).sav*` in `<run>/saves`, as turns that fit `integer`. */
const savedTurns = (runsRoot: string, gameId: string): readonly number[] =>
  Option.match(
    safeArchiveDirectory(resolveStrictly(runsRoot, gameId), Gateway.ARCHIVE_SAVES_DIRECTORY),
    {
      onNone: (): readonly number[] => [],
      onSome: (directory) =>
        archiveRegularFiles(directory, SAVE_NAME_RE).flatMap((name): readonly number[] => {
          const digits = SAVE_NAME_RE.exec(name)?.[1]
          if (digits === undefined) {
            return []
          }
          // `BigInt` is total on a `\d+` capture and exact where `Number` is
          // not: `turn-9007199254740993-auto.sav` must not become …992.
          const exact = BigInt(digits)
          return exact >= BigInt(INT32_MIN) && exact <= BigInt(INT32_MAX) ? [Number(exact)] : []
        })
    }
  )

const recordedTurns = (
  db: Database,
  gameId: string
): Effect.Effect<ReadonlySet<number>, SqlError> =>
  Effect.map(
    db.select({ turn: turns.turn }).from(turns).where(eq(turns.gameId, gameId)),
    (rows) => new Set(rows.map((row) => row.turn))
  )

const storedBoardTurns = (
  db: Database,
  gameId: string
): Effect.Effect<ReadonlySet<number>, SqlError> =>
  Effect.map(
    db.select({ turn: boardState.turn }).from(boardState).where(eq(boardState.gameId, gameId)),
    (rows) => new Set(rows.map((row) => row.turn))
  )

/** One derived board, ready to store. */
interface BoardRow {
  readonly turn: number
  readonly board: JsonValue
}

/**
 * The turns this sweep will attempt, and the ones it recorded as skips.
 *
 * ```
 * candidates = { N : turn-N-(auto|final).sav* exists }        (SAVE_NAME_RE)
 *            ∩ { N : a `turns` row exists }                   (replay.jsonl had it)
 *            \ { N : a `board_state` row exists }             (resumability)
 * ```
 *
 * An autosave with no `turns` row cannot be stored — the composite FK forbids
 * it — so it is a recorded skip rather than an error, and a turn with a replay
 * line but no autosave is never attempted at all.
 */
const boardCandidates = (
  gameId: string,
  saved: readonly number[],
  recorded: ReadonlySet<number>,
  stored: ReadonlySet<number>,
  limit: number
): { readonly turns: readonly number[]; readonly skips: readonly IngestSkip[] } => {
  const missingTurnRow = saved.filter((turn) => !recorded.has(turn))
  const attempted = saved
    .filter((turn) => recorded.has(turn) && !stored.has(turn))
    .toSorted((left, right) => left - right)
  return {
    turns: limit > 0 ? attempted.slice(0, limit) : attempted,
    skips: missingTurnRow.map((turn) =>
      skip(gameId, "boardTurnNotRecorded", `turn ${String(turn)}`)
    )
  }
}

const writeBoards = (
  db: Database,
  client: SqlClient.SqlClient,
  gameId: string,
  rows: readonly BoardRow[]
): Effect.Effect<{ readonly written: number; readonly wrote: boolean }, SqlError> =>
  rows.length === 0
    ? Effect.succeed({ written: 0, wrote: false })
    : client.withTransaction(
      Effect.gen(function*() {
        // Chunked at 200 rather than at the bind ceiling: a board is a large
        // jsonb document, and a failed statement should cost a little work
        // rather than a whole game's derivation.
        const written = yield* Effect.forEach(
          Arr.chunksOf(rows, Math.min(200, Math.floor(MAX_BIND_PARAMETERS / columnCount(boardState)))),
          (chunk) =>
            db
              .insert(boardState)
              .values(chunk.map((row) => ({ gameId, turn: row.turn, board: row.board })))
              .onConflictDoNothing({ target: [boardState.gameId, boardState.turn] })
              .returning({ turn: boardState.turn }),
          { concurrency: 1 }
        )
        return { written: chunkedRowsWritten(written), wrote: yield* transactionWrote(client) }
      })
    )

/**
 * Derive one turn's board through the bridge.
 *
 * The stored value is plain JSON — `board_state.board` is `jsonb`, which has no
 * `bigint`, and boards are domain data. {@link exactJson} refuses to launder an
 * integer past 2^53 rather than storing a quiet lie; nothing on a savegame's
 * board can reach that, and if one ever does the turn is skipped and said so.
 */
const deriveBoard = (
  runner: DerivationRunner,
  gameId: string,
  places: ReadonlyArray<JsonObject>,
  turn: number
): Effect.Effect<Either.Either<BoardRow, IngestSkip>> =>
  Effect.map(
    Effect.either(runner({ operation: "board", gameId, places, turn: BigInt(turn) })),
    Either.match({
      onLeft: (error: DerivationError): Either.Either<BoardRow, IngestSkip> =>
        Either.left(
          skip(gameId, "boardUnavailable", `turn ${String(turn)}: ${error._tag}`)
        ),
      onRight: (record) =>
        Option.match(exactJson(record), {
          onNone: (): Either.Either<BoardRow, IngestSkip> =>
            Either.left(
              skip(gameId, "boardUnavailable", `turn ${String(turn)}: board is not storable JSON`)
            ),
          onSome: (board) => Either.right({ turn, board })
        })
    })
  )

/** What one game's board phase did. */
interface BoardPass {
  readonly written: number
  readonly wroteInTransaction: boolean
  readonly skips: readonly IngestSkip[]
}

const NO_BOARDS: BoardPass = { written: 0, wroteInTransaction: false, skips: [] }

/**
 * One game's boards: discover, resume, derive serially, commit separately.
 *
 * Serial *within* a game because the python loaders share
 * `<cache_root>/<game_id>` and are not concurrency-safe — the same reason
 * `replay_lock` exists in the gateway. The commit is its own transaction,
 * after the run's document transaction: a board failure must never roll back a
 * run's `games`/`turns`/`player_turns` rows.
 */
const ingestBoards = (
  db: Database,
  client: SqlClient.SqlClient,
  boards: BoardOptions,
  runner: DerivationRunner,
  runsRoot: string,
  data: RunData
): Effect.Effect<BoardPass, SqlError> =>
  Effect.gen(function*() {
    const saved = savedTurns(runsRoot, data.gameId)
    if (saved.length === 0) {
      return NO_BOARDS
    }
    const candidates = boardCandidates(
      data.gameId,
      saved,
      yield* recordedTurns(db, data.gameId),
      yield* storedBoardTurns(db, data.gameId),
      boards.turnLimit
    )
    if (candidates.turns.length === 0) {
      return { ...NO_BOARDS, skips: candidates.skips }
    }
    const places = Option.match(data.manifest.value, {
      onNone: (): ReadonlyArray<JsonObject> => [],
      onSome: derivationPlaces
    })
    const derived = yield* Effect.forEach(
      candidates.turns,
      (turn) => deriveBoard(runner, data.gameId, places, turn),
      { concurrency: 1 }
    )
    const committed = yield* writeBoards(db, client, data.gameId, Arr.getRights(derived))
    return {
      written: committed.written,
      wroteInTransaction: committed.wrote,
      skips: [...candidates.skips, ...Arr.getLefts(derived)]
    }
  })

// ---------------------------------------------------------------------------
// Sweeps
// ---------------------------------------------------------------------------

/** A record `object` — the narrowing {@link describeSqlProblem} needs. */
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
 * How far down `cause` to look: `pg` puts the server's fields on the
 * `DatabaseError` that *is* `SqlError.cause`, while PGlite wraps that error once
 * more. Four links reaches both and cannot loop on a cyclic `cause`.
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

/** What may be said about a `SqlError` in a report line. Never its `cause`. */
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

/** One run's pass through the sweep: what it wrote, and what it could not. */
interface RunPass {
  /** `Option.none()` exactly when the database refused the run. */
  readonly write: Option.Option<RunWrite>
  readonly skips: readonly IngestSkip[]
  readonly data: RunData
}

/**
 * Read one run and write it, as one step of the sweep.
 *
 * Two properties, both structural:
 *
 * 1. **Bounded residency.** `collectRun` runs *inside* `Effect.suspend`, so the
 *    run's bytes are read when this step runs and are unreachable the moment it
 *    finishes. Building every run's data up front makes peak memory linear in
 *    the *archive*; collect, write, drop bounds it by the largest single run.
 * 2. **Isolation.** A `SqlError` from one run's transaction becomes that run's
 *    reported skip and nothing more. The fs backend answers per run; so does
 *    this.
 */
const ingestOne = (
  db: Database,
  client: SqlClient.SqlClient,
  runsRoot: string,
  gameId: string,
  options: IngestOptions
): Effect.Effect<RunPass> =>
  Effect.suspend(() => {
    const data = collectRun(runsRoot, gameId, options)
    return Effect.map(
      Effect.either(ingestRun(db, client, data)),
      Either.match({
        onLeft: (error): RunPass => ({
          write: Option.none(),
          skips: [
            ...data.skips,
            skip(gameId, "databaseRefused", describeSqlProblem(error))
          ],
          data
        }),
        onRight: (write): RunPass => ({ write: Option.some(write), skips: data.skips, data })
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
    const data = collectRun(runsRoot, gameId, options)
    return Effect.map(storedHash(db, gameId), (existing) => ({
      result: runResult(
        data,
        Option.match(existing, {
          onNone: (): RunOutcome => "inserted",
          onSome: (hash) => (sameBytes(hash, data.contentHash) ? "unchanged" : "updated")
        }),
        NO_WRITES
      ),
      skips: data.skips
    }))
  })

/**
 * The stored games that the sweep did not see **and that are not on the disk
 * now**.
 *
 * The third clause is the one that matters. The walk happens at the start of a
 * sweep and this at the end; a captured seen-set says nothing about the window
 * between. Re-asking the filesystem costs one `lstat` per *candidate* — a set
 * that is empty on every healthy sweep — and it is the same evidence, from the
 * same source, that put the row there.
 */
const staleGames = (
  db: Database,
  runsRoot: string,
  seen: readonly string[],
  filter: ReadonlySet<string>
): Effect.Effect<readonly string[], SqlError> =>
  Effect.map(
    db.select({ gameId: games.gameId }).from(games),
    (rows) =>
      rows
        .map((row) => row.gameId)
        .filter((gameId) =>
          !seen.includes(gameId) &&
          (filter.size === 0 || filter.has(gameId)) &&
          !isRunOnDisk(runsRoot, gameId)
        )
  )

const deleteGames = (
  db: Database,
  gameIds: readonly string[]
): Effect.Effect<number, SqlError> =>
  gameIds.length === 0
    ? Effect.succeed(0)
    : Effect.map(
      db
        .delete(games)
        // `inArray`, emphatically not `notInArray(…, [])`: drizzle renders an
        // empty `notInArray` as the literal `true`, which would delete every
        // game the moment one went stale.
        .where(inArray(games.gameId, [...gameIds]))
        .returning({ gameId: games.gameId }),
      rowsWritten
    )

/**
 * Walk a `runs_root` and reconcile it into the database.
 *
 * The whole pipeline: classify every directory entry, read and write each run's
 * documents in its own transaction, derive the boards each run is missing (in
 * their own transactions, after the documents), prune when asked. A dry run
 * does all of the reading and none of the writing, and reports exactly what
 * would have changed.
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

    const boards = Option.filter(options.boards, () => !options.dryRun)
    yield* Effect.forEach(
      Option.toArray(boards),
      (configured) =>
        boardCacheRootValid(runsRoot, configured.cacheRoot)
          ? Effect.void
          : Effect.fail(
            new BoardCacheRootInvalid({ cacheRoot: configured.cacheRoot, runsRoot })
          ),
      { discard: true }
    )

    const walked = yield* walk(runsRoot, options.gameIds)
    const seen = walked.gameIds.length + walked.skipped.length

    if (options.dryRun) {
      const inspected = yield* Effect.forEach(
        walked.gameIds,
        (gameId) => inspectOne(db, runsRoot, gameId, options),
        { concurrency: 1 }
      )
      const stale = options.prune
        ? yield* staleGames(db, runsRoot, walked.gameIds, options.gameIds)
        : []
      return {
        runsRoot,
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

    const passes = yield* Effect.forEach(
      walked.gameIds,
      (gameId) => ingestOne(db, client, runsRoot, gameId, options),
      { concurrency: 1 }
    )
    const written = passes.flatMap((pass) => Option.toArray(pass.write))

    // The board phase runs after every document transaction has committed, so a
    // board derived here is derived against rows that already exist. Across
    // games it is concurrent; within one it is serial.
    const boarded = yield* Option.match(boards, {
      onNone: () => Effect.succeed<ReadonlyArray<readonly [string, BoardPass]>>([]),
      onSome: (configured) => {
        const runner = Option.getOrElse(
          configured.runner,
          () =>
            pythonDerivationRunner({
              repoRoot: configured.repoRoot,
              runsRoot,
              cacheRoot: configured.cacheRoot
            })
        )
        return Effect.forEach(
          passes.filter((pass) => Option.isSome(pass.write)),
          (pass) =>
            Effect.map(
              Effect.either(ingestBoards(db, client, configured, runner, runsRoot, pass.data)),
              (outcome) =>
                [
                  pass.data.gameId,
                  Either.getOrElse(outcome, (error): BoardPass => ({
                    ...NO_BOARDS,
                    skips: [skip(pass.data.gameId, "databaseRefused", describeSqlProblem(error))]
                  }))
                ] as const
            ),
          { concurrency: Math.max(1, configured.concurrency) }
        )
      }
    })
    const boardsByGame = new Map(boarded)

    const stale = options.prune
      // The `SELECT` that finds the stale ids, the filesystem recheck that
      // confirms them and the `DELETE` that removes them are one transaction, so
      // no row can be inserted between the decision and the deletion.
      ? yield* client.withTransaction(
        Effect.flatMap(
          staleGames(db, runsRoot, walked.gameIds, options.gameIds),
          (ids) => Effect.map(deleteGames(db, ids), (deleted) => ({ ids, deleted }))
        )
      )
      : { ids: [] as readonly string[], deleted: 0 }

    const boardWrites = [...boardsByGame.values()].reduce(
      (total, pass) => total + pass.written,
      0
    )
    return {
      runsRoot,
      dryRun: false,
      status: "complete" as const,
      seen,
      runs: written.map((entry) => ({
        ...entry.result,
        boardsWritten: boardsByGame.get(entry.result.gameId)?.written ?? 0
      })),
      deleted: stale.ids,
      skipped: [
        ...walked.skipped,
        ...passes.flatMap((pass) => pass.skips),
        ...[...boardsByGame.values()].flatMap((pass) => pass.skips)
      ],
      writes: addWrites(
        sumWrites(written.map((entry) => entry.result.writes)),
        { ...NO_WRITES, boards: boardWrites, deletes: stale.deleted }
      ),
      transactionsWithWrites: written.filter((entry) => entry.wroteInTransaction).length +
        [...boardsByGame.values()].filter((pass) => pass.wroteInTransaction).length
    }
  })
