/**
 * Putting a run's binary artifacts back on a filesystem, from `bytea`.
 *
 * Two consumers need real files and neither can be taught a database:
 *
 * 1. **The python bridge.** `python3 -m agent_eval.replay_derive_cli` reads
 *    `<runs_root>/<game_id>/saves/turn-N-(auto|final).sav[.gz…]` and
 *    `…/saves/turn-N-*.map.ppm` with `lstat` + `S_ISREG`, and it is a
 *    subprocess. `ReplayDerivationPython` therefore keeps working unchanged in
 *    pg mode: it is pointed at a *materialize root* instead of the real
 *    `runs_root`, and {@link RunArchiveMaterializer.ensureRunArchive} makes that
 *    root look like the archive.
 * 2. **`http/routes/archive.ts`.** Its `frameSources` lists
 *    `<run_root>/watch_frames` and `<run_root>/saves` directly off
 *    `TerminalArchive.runRoot`, and `TerminalArchive.runRoot` is documented in
 *    `services/runs.ts` as "the resolved run directory, for the binary routes
 *    and nothing else". Three call sites reach around the repository that way —
 *    the two listings and the PPM open — and none of them has been refactored
 *    to take a `BinaryArtifact` yet. Until they are, a pg-backed
 *    `TerminalArchive.runRoot` has to be a directory that really holds those
 *    files, and this module is what makes one.
 *
 * ## Why the paths are *stable* and the writes are conditional
 *
 * `save_replay._load_cache` validates every cache entry against the source
 * save's `{device, inode, size, mtime_ns, ctime_ns}`. A fresh `mkdtemp` per
 * request — or an unconditional rewrite of an unchanged file — changes the inode
 * and the ctime and so **permanently disables the python-side cache**, turning a
 * warm replay page into a full re-parse of every autosave (measured: up to 1506
 * saves in one run). So:
 *
 * - the layout is `<materializeRoot>/<gameId>/{saves,watch_frames}/…`, a
 *   deterministic path that survives the process;
 * - a file whose size already matches what we would write is **left alone** — no
 *   open, no `utimes`, no `chmod`;
 * - a file that must change is written to `<name>.tmp-<n>` and `rename(2)`d into
 *   place, so it gets a new inode exactly once per content change, which is
 *   correct because the content did change;
 * - `derivation_workdirs.saves_hash` short-circuits the whole reconciliation:
 *   when the expected set hashes the same as last time and the directory still
 *   holds exactly those names, nothing is stat'ed at all.
 *
 * A PPM is materialized from `head_bytes`, which is deliberately shorter than
 * the original (schema.ts): no reader ever looks past a PPM header, and
 * `is_whole` records that the shortness was on purpose.
 *
 * ## What this module may not do
 *
 * It never writes into `runs_root` and never writes into `cache_root`
 * (`save_replay._cache_directory` raises `SaveReplayError` when the cache root
 * nests with a saves directory — see {@link nestsWith}). The materialize root
 * must be a *sibling* of the cache root, which is what
 * `<cacheRoot>-saves` gives you.
 *
 * ## Why reconciliation is serialized per game, and why the lock lives *here*
 *
 * Reconciliation is idempotent but it is **not concurrent**: it lists the
 * directory, writes what is missing through a temporary name and a `rename(2)`,
 * and then deletes every entry no row names. Two reconciliations of the same
 * game running together therefore race — one deletes the other's `.tmp-…` file,
 * one lists a tree the other is half-way through rewriting — and the loser fails
 * with {@link MaterializeFailed}, which the gateway turns into a **503 `game
 * manifest is unavailable`** (`runs-repository-pg.ts`'s `terminalArchive`).
 *
 * Measured, before the lock: a 24-way burst against one cold game answered
 * `200` **2** times and `503` **22** times in round 0, then `200` 24/24 in every
 * round after — the tree is complete once anybody finishes building it, so the
 * defect is invisible to anything but a cold burst, which is exactly the shape
 * the parity rig hit and exactly why a single warm-up request "fixed" it.
 *
 * {@link makeMaterializer} owns one {@link Effect.Semaphore} **per game id** and
 * holds it across the whole of {@link RunArchiveMaterializer.ensureRunArchive},
 * so the second caller for a game *waits* instead of failing while callers for
 * different games stay parallel. Once the tree matches the rows the guarded
 * section is one `SELECT` and one `readdir`, so the steady-state cost is a
 * lock acquisition. `Effect.cachedFunction` keeps the semaphores: one per
 * distinct id, created on demand, owned by the factory's closure — never a
 * module-level `Map`. It never evicts, which is the right trade here: the key
 * space is the ids under one `runs_root`, a semaphore is two words, and an
 * eviction policy would be a second thing that can be wrong.
 *
 * **One materializer per gateway, therefore one lock table per gateway.** The
 * lock is only a lock if every caller shares it, and there are two: the
 * repository's `terminalArchive` and the derivation bridge's runner
 * (`derivation-cache-pg.ts`). Both take it from the {@link Materializer} tag,
 * which {@link layer} provides *once* — the same layer memoization that gives
 * both of them one connection pool. Constructing a second materializer beside
 * the first would silently reopen the race, so neither consumer may call
 * {@link makeMaterializer} itself.
 *
 * ### The lock order, and why it cannot cycle
 *
 * There are exactly two locks on this path, and one of them nests inside the
 * other in exactly one direction:
 *
 * 1. `replay_lock` — the process-wide `Effect.Semaphore(1)` that
 *    `makeReplayDerivation` wraps every derivation in (`derivation.ts`);
 * 2. this module's per-game semaphore.
 *
 * A derivation takes (1) and then (2) for its own game. An archive read takes
 * (2) alone. Nothing acquires (1) while holding (2), and nothing holds two
 * per-game semaphores at once — the guarded section calls only the database and
 * the filesystem, never another `ensureRunArchive`. The order `replay_lock <
 * game(id)` is therefore total over every acquisition in the process, so no
 * cycle exists and no wait can deadlock. It also means the derivation bridge and
 * a concurrent archive read of the same game are serialized against each other,
 * which is the race that survived the gateway-side lock this replaced.
 *
 * ### What the lock does *not* cover
 *
 * `http/routes/archive.ts` lists `<run_root>/watch_frames` and
 * `<run_root>/saves` itself, with `readdirSync`, *after* the repository handed
 * it a `TerminalArchive` — the third of the reach-arounds this module's header
 * describes. That listing is outside every lock here, and it was outside the
 * gateway-side one too, so nothing about it changed. It is also nearly harmless:
 * the tree is complete when the archive is handed over, only a re-ingest can
 * make it stale, a reader never sees a partial file (temporaries are written
 * under `<name>.tmp-<digest>`, which matches neither `ARCHIVE_PNG_RE` nor
 * `ARCHIVE_PPM_RE`), and a file deleted between the listing and the open is a
 * race the filesystem gateway runs against a live `runs_root` too. It closes for
 * good when that route takes a `BinaryArtifact` instead of a path, at which
 * point no caller outside this module touches the directory at all.
 */

import type { FileSystem } from "@effect/platform/FileSystem"
import { FileSystem as FileSystemTag } from "@effect/platform/FileSystem"
import type { Path } from "@effect/platform/Path"
import { Path as PathTag } from "@effect/platform/Path"
import * as PgDrizzle from "@effect/sql-drizzle/Pg"
import type { SqlError } from "@effect/sql/SqlError"
import { and, eq, sql } from "drizzle-orm"
import * as Context from "effect/Context"
import * as Data from "effect/Data"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import { createHash } from "node:crypto"

import { derivationWorkdirs, runFrames, runSaves, runVideos } from "./schema.ts"

/** The drizzle handle, as `@effect/sql-drizzle/Pg` publishes it. */
export type Database = PgDrizzle.PgDrizzle["Type"]

/** Whatever `@effect/platform` failed with. Named so the signatures below read. */
type PlatformFailure = Effect.Effect.Error<ReturnType<FileSystem["writeFile"]>>

/** `watch_frames`, relative to a run root. Spelled by `@arena/wire`'s `Gateway`. */
const FRAMES_DIRECTORY = "watch_frames"

/** `saves`, relative to a run root. */
const SAVES_DIRECTORY = "saves"

/** `game.mp4`, relative to a run root. */
const VIDEO_FILE = "game.mp4"

/**
 * What was written into a run directory, and what it hashed to.
 *
 * Size alone cannot decide freshness: a re-ingested frame can change content
 * without changing length, and a materializer that skipped it would serve stale
 * bytes forever. The sidecar records the *digest* of every file this module put
 * on disk, so a file is left alone only when its recorded digest is the expected
 * one and its size still matches. It lives inside the run directory so it moves,
 * and is thrown away, with the tree it describes.
 */
const SIDECAR_FILE = ".arena-materialized.json"

/**
 * `JSON.parse`, as a value.
 *
 * Deliberately `Schema.parseJson()` and not a bare `JSON.parse`: the sidecar is
 * a file on a disk this module does not own, a truncated one is *expected*
 * (a crash mid-write leaves one), and a `SyntaxError` thrown inside the
 * `Effect.map` below would become a **defect**, which neither the
 * `Effect.orElseSucceed` around it nor {@link MaterializeFailed}'s
 * `Effect.mapError` can recover — both work on the error channel only. A
 * corrupt sidecar has to mean "rewrite everything", and that answer is only
 * reachable if the parse failure is a *value*.
 */
const parseSidecarJson = Schema.decodeUnknownOption(Schema.parseJson())

/** A JSON object — the only shape the sidecar is ever written in. */
const isDigestRecord = (value: unknown): value is Record<string, unknown> =>
  value !== null && typeof value === "object" && !Array.isArray(value)

/**
 * Materialization failed, and the caller has to decide what that means.
 *
 * The two call sites classify it differently and both are right: the derivation
 * bridge turns it into a 503 (the loaders could not be given their inputs), and
 * a binary route turns it into a 503 for the same reason. Nothing turns it into
 * a 404 — the rows exist, it is the disk that would not cooperate.
 */
export class MaterializeFailed extends Data.TaggedError("MaterializeFailed")<{
  readonly gameId: string
  /** Diagnostic only. Never put this in a response body. */
  readonly detail: string
}> {}

/** One file the archive should contain, identified without reading its bytes. */
interface ExpectedFile {
  /** Relative to the run directory, e.g. `saves/turn-0001-auto.sav.gz`. */
  readonly relativePath: string
  /** The number of bytes that will be written — a PPM head, not the original size. */
  readonly length: number
  /** The stored digest, used as the identity of the content. */
  readonly digest: string
}

const hex = (bytes: Uint8Array): string =>
  Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("")

/**
 * Fields as one unambiguous string: `<length>:<field>` per field, concatenated.
 *
 * The length is in the same units the field is measured in (UTF-16 code units),
 * so the encoding is injective by construction: a reader takes the prefix,
 * consumes exactly that many units, and starts again. No field content — a save
 * name, above all — can imitate a separator, because there is none.
 */
const encodeFields = (fields: ReadonlyArray<string>): string =>
  fields.map((field) => `${String(field.length)}:${field}`).join("")

/**
 * The identity of an expected *set* of files.
 *
 * Order-independent by construction (the listing is sorted before it is
 * hashed), so two ingests that produced the same archive in different physical
 * orders short-circuit against each other rather than rewriting the tree.
 */
const setDigest = (files: ReadonlyArray<ExpectedFile>): Uint8Array => {
  const listing = files
    // Length-prefixed, not space-joined: `relativePath` carries a save name
    // straight off the disk, and `saves/` is listed with a match-everything
    // pattern, so a name may contain any separator this encoding could pick.
    // `<n>:<field>` is injective for the same reason netstrings are — the
    // prefix is read first and says exactly how far the field runs — so no
    // filename can make two different sets hash the same and short-circuit a
    // reconciliation that was needed.
    .map((file) => encodeFields([file.relativePath, String(file.length), file.digest]))
    .toSorted()
    .join("\n")
  return new Uint8Array(createHash("sha256").update(listing, "utf8").digest())
}

/** Every file a run's archive should have on disk, cheapest query first: no bytes. */
const expectedFiles = (
  db: Database,
  runsRoot: string,
  gameId: string
): Effect.Effect<ReadonlyArray<ExpectedFile>, SqlError> =>
  Effect.gen(function*() {
    const key = (table: typeof runSaves | typeof runFrames | typeof runVideos) =>
      and(eq(table.runsRoot, runsRoot), eq(table.gameId, gameId))

    // `octet_length`, deliberately *not* `byte_size`. `byte_size` is the fstat
    // size of the original file: for a PPM it is far larger than the stored head
    // (schema.ts), and even for a whole file a short `readFully` can leave the
    // stored bytes below it. What lands on disk is exactly what is stored.
    const saves = yield* db
      .select({
        name: runSaves.name,
        length: sql<number>`octet_length(${runSaves.headBytes})`,
        sha256: runSaves.sha256
      })
      .from(runSaves)
      .where(key(runSaves))
    const frames = yield* db
      .select({
        name: runFrames.name,
        length: sql<number>`octet_length(${runFrames.bytes})`,
        sha256: runFrames.sha256
      })
      .from(runFrames)
      .where(key(runFrames))
    const videos = yield* db
      .select({
        length: sql<number>`octet_length(${runVideos.bytes})`,
        sha256: runVideos.sha256
      })
      .from(runVideos)
      .where(key(runVideos))

    return [
      ...saves.map((row) => ({
        relativePath: `${SAVES_DIRECTORY}/${row.name}`,
        length: row.length,
        digest: hex(row.sha256)
      })),
      ...frames.map((row) => ({
        relativePath: `${FRAMES_DIRECTORY}/${row.name}`,
        length: row.length,
        digest: hex(row.sha256)
      })),
      ...videos.map((row) => ({
        relativePath: VIDEO_FILE,
        length: row.length,
        digest: hex(row.sha256)
      }))
    ]
  })

/** The bytes of the files named by `relativePaths`, fetched only once they are needed. */
const fetchBytes = (
  db: Database,
  runsRoot: string,
  gameId: string,
  wanted: ReadonlySet<string>
): Effect.Effect<ReadonlyMap<string, Uint8Array>, SqlError> =>
  Effect.gen(function*() {
    const key = (table: typeof runSaves | typeof runFrames | typeof runVideos) =>
      and(eq(table.runsRoot, runsRoot), eq(table.gameId, gameId))

    const saves = wanted.size === 0 ? [] : yield* db
      .select({ name: runSaves.name, bytes: runSaves.headBytes })
      .from(runSaves)
      .where(key(runSaves))
    const frames = wanted.size === 0 ? [] : yield* db
      .select({ name: runFrames.name, bytes: runFrames.bytes })
      .from(runFrames)
      .where(key(runFrames))
    const videos = wanted.size === 0 ? [] : yield* db
      .select({ bytes: runVideos.bytes })
      .from(runVideos)
      .where(key(runVideos))

    return new Map(
      [
        ...saves.map((row) => [`${SAVES_DIRECTORY}/${row.name}`, row.bytes] as const),
        ...frames.map((row) => [`${FRAMES_DIRECTORY}/${row.name}`, row.bytes] as const),
        ...videos.map((row) => [VIDEO_FILE, row.bytes] as const)
      ].filter(([relativePath]) => wanted.has(relativePath))
    )
  })

/** Everything the materializer needs that is not a database. */
export interface MaterializeOptions {
  /** The logical scope key every row is stamped with. */
  readonly runsRoot: string
  /**
   * Where run directories are built. Must not nest with `--cache-root`; the
   * default the gateway should pass is `<cacheRoot>-saves`.
   */
  readonly materializeRoot: string
}

/** Materializing one run's archive, idempotently. */
export interface RunArchiveMaterializer {
  /**
   * The roots this materializer was built over.
   *
   * Published so its consumers do not have to be told them a second time: the
   * repository scopes its rows by `runsRoot` and the derivation bridge is
   * pointed at `materializeRoot`, and a caller that carried its own copy could
   * be pointed at a directory nobody holds the lock for.
   */
  readonly options: MaterializeOptions
  /** `<materializeRoot>/<gameId>` — deterministic, and valid before it exists. */
  readonly runRoot: (gameId: string) => string
  /** `<runRoot>/watch_frames/<name>`, where a materialized PNG lands. */
  readonly framePath: (gameId: string, name: string) => string
  /** `<runRoot>/game.mp4`, where a materialized video lands. */
  readonly videoPath: (gameId: string) => string
  /**
   * Reconcile `<materializeRoot>/<gameId>` against the rows, and answer with its
   * path. A run whose archive is already on disk costs one `SELECT` and one
   * `readdir`.
   */
  readonly ensureRunArchive: (gameId: string) => Effect.Effect<string, MaterializeFailed>
}

/**
 * `save_replay._cache_directory`'s refusal, as a predicate.
 *
 * It raises `SaveReplayError("Replay cache must be separate from game saves.")`
 * when the resolved cache root equals, contains, or is contained by the saves
 * directory — so a materialize root that nests with the cache root produces a
 * 503 on the first replay request instead of a startup error. Exported so the
 * gateway's config can reject the pair at construction, on the one exit-2 site.
 */
export const nestsWith = (left: string, right: string): boolean => {
  const a = left.replace(/\/+$/, "")
  const b = right.replace(/\/+$/, "")
  return a === b || a.startsWith(`${b}/`) || b.startsWith(`${a}/`)
}

const failed = (gameId: string, detail: string) => (cause: unknown): MaterializeFailed =>
  new MaterializeFailed({ gameId, detail: `${detail}: ${String(cause)}` })

/**
 * Build a materializer over an open database and an open filesystem.
 *
 * `fs` and `path` are taken as *values*, not as context requirements, so every
 * method comes out with `R = never` and the result can satisfy a service
 * interface whose methods promise no environment.
 *
 * It is an `Effect` because the result *owns* the per-game locks described in
 * this module's docstring, and a semaphore is an effect to create. Call it once
 * per gateway — {@link layer} is how — and share the result: two materializers
 * over one materialize root are two lock tables, which is no lock at all.
 */
export const makeMaterializer = (
  db: Database,
  fs: FileSystem,
  path: Path,
  options: MaterializeOptions
): Effect.Effect<RunArchiveMaterializer> => {
  const runRoot = (gameId: string): string => path.join(options.materializeRoot, gameId)

  const listing = (directory: string): Effect.Effect<ReadonlyArray<string>> =>
    Effect.orElseSucceed(
      Effect.map(
        fs.readDirectory(directory, { recursive: true }),
        (entries) => entries.map((entry) => entry.split(path.sep).join("/"))
      ),
      (): ReadonlyArray<string> => []
    )

  /**
   * Entries the tree may hold that no row names: the directories the expected
   * files live in, and the sidecar itself.
   */
  const isStructural = (entry: string, files: ReadonlyArray<ExpectedFile>): boolean =>
    entry === SIDECAR_FILE ||
    files.some((file) => file.relativePath.startsWith(`${entry}/`))

  /** `true` when every expected name is present and nothing else is. */
  const directoryMatches = (
    directory: string,
    files: ReadonlyArray<ExpectedFile>
  ): Effect.Effect<boolean> =>
    Effect.map(listing(directory), (entries) => {
      const present = new Set(entries)
      const expected = new Set(files.map((file) => file.relativePath))
      // `readDirectory(recursive)` lists the directories too, so a present entry
      // that is not expected is only an extra when it is not structural.
      const extras = entries.filter(
        (entry) => !expected.has(entry) && !isStructural(entry, files)
      )
      return extras.length === 0 && files.every((file) => present.has(file.relativePath))
    })

  /** The digest this module last wrote for each name, or an empty map. */
  const readSidecar = (
    directory: string
  ): Effect.Effect<ReadonlyMap<string, string>> =>
    Effect.orElseSucceed(
      Effect.map(
        fs.readFileString(path.join(directory, SIDECAR_FILE)),
        (raw): ReadonlyMap<string, string> =>
          Option.match(Option.filter(parseSidecarJson(raw), isDigestRecord), {
            onNone: (): ReadonlyMap<string, string> => new Map(),
            onSome: (parsed) =>
              new Map(
                Object.entries(parsed).flatMap(([name, digest]) =>
                  typeof digest === "string" ? [[name, digest] as const] : []
                )
              )
          })
      ),
      // Missing, unreadable or corrupt all mean the same thing: nothing on disk
      // can be trusted to be current, so every file is rewritten. `corrupt`
      // includes "not JSON at all", which is why the parse above is a schema
      // decode and not a `JSON.parse` that would throw past this handler.
      (): ReadonlyMap<string, string> => new Map()
    )

  const writeSidecar = (
    directory: string,
    files: ReadonlyArray<ExpectedFile>
  ): Effect.Effect<void, PlatformFailure> =>
    fs.writeFileString(
      path.join(directory, SIDECAR_FILE),
      `${JSON.stringify(
        Object.fromEntries(
          files
            .map((file) => [file.relativePath, file.digest] as const)
            // By *name*, and by name alone. The elements are `[name, digest]`
            // pairs, and a bare `toSorted()` compares `String(pair)` — i.e.
            // `"name,digest"` — so `a/b` and `a/b-2` would be ordered by their
            // digests whenever one name is a prefix of the other. The explicit
            // comparator is the same code-unit order `sort` uses on strings,
            // applied to the field that is supposed to decide it.
            .toSorted(([left], [right]) => (left < right ? -1 : left > right ? 1 : 0))
        ),
        null,
        2
      )}\n`
    )

  /** The recorded set digest for this run, or none. */
  const recordedDigest = (
    gameId: string
  ): Effect.Effect<Option.Option<string>, SqlError> =>
    Effect.map(
      db
        .select({ savesHash: derivationWorkdirs.savesHash, path: derivationWorkdirs.path })
        .from(derivationWorkdirs)
        .where(
          and(
            eq(derivationWorkdirs.runsRoot, options.runsRoot),
            eq(derivationWorkdirs.gameId, gameId)
          )
        ),
      (rows) =>
        Option.map(
          Option.fromNullable(rows.find((row) => row.path === runRoot(gameId))),
          (row) => hex(row.savesHash)
        )
    )

  /** Whether the file at `target` already holds `length` bytes. */
  const alreadySized = (target: string, length: number): Effect.Effect<boolean> =>
    Effect.map(
      Effect.either(fs.stat(target)),
      (info) =>
        info._tag === "Right" && info.right.type === "File" &&
        info.right.size === BigInt(length)
    )

  /**
   * Write one file through a temporary name and `rename(2)`.
   *
   * The temporary lives in the destination directory so the rename is within one
   * filesystem and therefore atomic, and it is named from the digest so two
   * concurrent materializations of the same content cannot collide on a
   * half-written file.
   */
  const writeAtomically = (
    target: string,
    bytes: Uint8Array,
    digest: string
  ): Effect.Effect<void, PlatformFailure> => {
    const temporary = `${target}.tmp-${digest.slice(0, 12)}`
    return Effect.flatMap(
      fs.makeDirectory(path.dirname(target), { recursive: true }),
      () =>
        Effect.flatMap(
          fs.writeFile(temporary, bytes),
          () => fs.rename(temporary, target)
        )
    )
  }

  const reconcile = (
    gameId: string,
    directory: string,
    files: ReadonlyArray<ExpectedFile>
  ): Effect.Effect<void, SqlError | PlatformFailure> =>
    Effect.gen(function*() {
      const sidecar = yield* readSidecar(directory)
      // Fresh means *both*: the recorded digest is the one we want, and the file
      // is still the length that digest was taken over. Size alone would miss a
      // same-length content change; the digest alone would miss a truncation.
      const stale = yield* Effect.filter(files, (file) =>
        Effect.map(
          alreadySized(path.join(directory, file.relativePath), file.length),
          (sized) => !(sized && sidecar.get(file.relativePath) === file.digest)
        ))
      const bytes = yield* fetchBytes(
        db,
        options.runsRoot,
        gameId,
        new Set(stale.map((file) => file.relativePath))
      )
      yield* Effect.forEach(
        stale,
        (file) =>
          Option.match(Option.fromNullable(bytes.get(file.relativePath)), {
            // A row that vanished between the two queries is a concurrent
            // re-ingest, not an error: the next request materializes it.
            onNone: () => Effect.void,
            onSome: (payload) =>
              writeAtomically(path.join(directory, file.relativePath), payload, file.digest)
          }),
        { discard: true }
      )

      // Deletion detection: anything in the tree that no row names is removed,
      // so a re-ingest that dropped a save does not leave the bridge reading a
      // file the archive no longer has.
      const expected = new Set(files.map((file) => file.relativePath))
      const entries = yield* listing(directory)
      yield* Effect.forEach(
        entries.filter((entry) => !expected.has(entry) && !isStructural(entry, files)),
        (entry) => Effect.ignore(fs.remove(path.join(directory, entry), { recursive: true })),
        { discard: true }
      )

      // Last, so a crash mid-reconcile leaves a sidecar that under-claims rather
      // than one that promises files it never wrote.
      yield* writeSidecar(directory, files)
    })

  const reconcileRunArchive = (
    gameId: string
  ): Effect.Effect<string, MaterializeFailed> => {
    const directory = runRoot(gameId)
    return Effect.gen(function*() {
      const files = yield* expectedFiles(db, options.runsRoot, gameId)
      const digest = setDigest(files)
      const recorded = yield* recordedDigest(gameId)

      // The recorded digest is the short circuit the design asks for; the
      // listing check on top of it is one `readdir` that catches a tree someone
      // deleted out from under the row, which a bare digest match would not.
      const stamped = Option.match(recorded, {
        onNone: () => false,
        onSome: (value) => value === hex(digest)
      })
      if (stamped && (yield* directoryMatches(directory, files))) {
        return directory
      }

      yield* fs.makeDirectory(directory, { recursive: true })
      yield* reconcile(gameId, directory, files)
      yield* db
        .insert(derivationWorkdirs)
        .values({
          runsRoot: options.runsRoot,
          gameId,
          path: directory,
          savesHash: digest
        })
        .onConflictDoUpdate({
          target: [derivationWorkdirs.runsRoot, derivationWorkdirs.gameId],
          // `now()` server-side rather than a client `Date`: the column is
          // bookkeeping, and the database's clock is the one an operator reads.
          set: { path: directory, savesHash: digest, materializedAt: sql`now()` }
        })
      return directory
    }).pipe(Effect.mapError(failed(gameId, "could not materialize the run archive")))
  }

  // One semaphore per game id, created on demand and kept for the life of the
  // materializer. `Effect.cachedFunction` is what keeps them: the lookup is
  // itself an effect, so two fibers asking for the same id at the same instant
  // get the same semaphore rather than one each.
  return Effect.map(
    Effect.cachedFunction((_gameId: string) => Effect.makeSemaphore(1)),
    (lockFor): RunArchiveMaterializer => ({
      options,
      runRoot,
      framePath: (gameId, name) => path.join(runRoot(gameId), FRAMES_DIRECTORY, name),
      videoPath: (gameId) => path.join(runRoot(gameId), VIDEO_FILE),
      // The *whole* reconciliation is inside the permit, the short circuit
      // included: the fast path reads the directory the slow path is rewriting,
      // so a check that ran beside a reconcile could answer from a tree that is
      // momentarily missing a file it is about to have.
      ensureRunArchive: (gameId) =>
        Effect.flatMap(lockFor(gameId), (lock) =>
          lock.withPermits(1)(reconcileRunArchive(gameId)))
    })
  )
}

/**
 * The one materializer a gateway has.
 *
 * A tag rather than a constructor call at each use site, because sharing is the
 * whole point: the per-game lock only serializes callers that hold the *same*
 * materializer, and this package has two callers — `runs-repository-pg.ts` and
 * `derivation-cache-pg.ts`. Taking it from the context makes forgetting to
 * share it a type error instead of a race.
 */
export class Materializer extends Context.Tag("@arena/db/Materializer")<
  Materializer,
  RunArchiveMaterializer
>() {}

/**
 * Provide the materializer for one `(runs_root, materialize_root)` pair.
 *
 * Provide this **once** per gateway and let both consumers take it from there:
 * `Layer.provide` memoizes by layer reference, so a single value handed to a
 * `Layer.merge` of the repository and the derivation bridge builds one
 * materializer and therefore one lock table — the same way both of them already
 * share one connection pool.
 */
export const layer = (
  options: MaterializeOptions
): Layer.Layer<Materializer, never, PgDrizzle.PgDrizzle | FileSystem | Path> =>
  Layer.effect(
    Materializer,
    Effect.gen(function*() {
      const db = yield* PgDrizzle.PgDrizzle
      const fs = yield* FileSystemTag
      const path = yield* PathTag
      return yield* makeMaterializer(db, fs, path, options)
    })
  )
