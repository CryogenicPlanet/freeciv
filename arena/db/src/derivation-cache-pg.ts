/**
 * The derivation cache, mirrored into Postgres.
 *
 * `<cache_root>/<game_id>/…` is where `save_replay` and `game_events` memoize
 * their parses, and the gateway itself never reads or writes inside it — it
 * creates the root at startup and passes it to a **subprocess**, which cannot be
 * taught a database. So the on-disk cache stays the working copy and
 * `derivation_cache` becomes the durable one, with two best-effort steps around
 * each derivation, both inside the `replay_lock` semaphore:
 *
 * 1. **hydrate** — write any stored entry the working directory is missing, so a
 *    fresh container or a wiped `--cache-root` starts warm;
 * 2. **capture** — read the working directory back and upsert the entries whose
 *    bytes changed.
 *
 * ## Why both steps may fail silently
 *
 * This is a *cache*, and both python loaders re-validate every entry they read:
 * `save_replay._load_cache` checks the entry against the source save's
 * `{device, inode, size, mtime_ns, ctime_ns}` and returns `None` on any
 * mismatch, and `game_events` re-checks `_places_digest`. A stale row therefore
 * costs a re-parse and can never produce a wrong answer, which is exactly what
 * makes swallowing a failure here correct rather than lazy. A hydrate or capture
 * that throws is logged and the derivation proceeds; it is never a request
 * failure.
 *
 * ## `cache_key` is the resolved `--cache-root`
 *
 * Not a hash of it, and not `runs_root`: the on-disk cache is namespaced by
 * cache root and by nothing else, so the pg namespace and the fs namespace are
 * the same namespace under two spellings. That is what keeps the parity rig's
 * rule — the two gateways get *different* `--cache-root`s, so neither can answer
 * from the other's cache — holding without a change on this side.
 *
 * ## Entry names
 *
 * Exactly the direct children of `<cache_root>/<game_id>`, which the loaders
 * spell `turn-{turn:010d}-{sha256(source_name)[:12]}.json`
 * (`save_replay._cache_path`) and `events.json`
 * (`game_events._events_cache_path`). Subdirectories are not mirrored: the
 * loaders never make one, and a mirror that invented a layout would be a second
 * place the cache discipline lives.
 */

import type { FileSystem } from "@effect/platform/FileSystem"
import { FileSystem as FileSystemTag } from "@effect/platform/FileSystem"
import type { Path } from "@effect/platform/Path"
import { Path as PathTag } from "@effect/platform/Path"
import * as PgDrizzle from "@effect/sql-drizzle/Pg"
import type { SqlError } from "@effect/sql/SqlError"
import { and, eq, sql } from "drizzle-orm"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"

import {
  type DerivationRunner,
  derivationCacheDirectory,
  DerivationUnavailable,
  layerFromRunner,
  type PythonDerivationOptions,
  pythonDerivationRunner,
  ReplayDerivation
} from "../../harness/src/gateway/services/derivation.ts"

import { type Database, Materializer } from "./materialize.ts"
import { derivationCache } from "./schema.ts"

/** How to point the mirror at one cache namespace. */
export interface DerivationCacheOptions {
  /** The resolved `--cache-root`, verbatim: this *is* the namespace key. */
  readonly cacheRoot: string
}

/** The durable half of `<cache_root>/<game_id>`. */
export interface DerivationCacheMirror {
  /** The `cache_key` every row of this mirror carries. */
  readonly cacheKey: string
  /** Write stored entries the working directory does not already have. */
  readonly hydrate: (gameId: string) => Effect.Effect<ReadonlyArray<string>>
  /** Store working-directory entries whose bytes are new or changed. */
  readonly capture: (gameId: string) => Effect.Effect<ReadonlyArray<string>>
}

const sameBytes = (left: Uint8Array, right: Uint8Array): boolean =>
  left.length === right.length && left.every((byte, index) => byte === right[index])

/** One cache document, on disk or in the database. */
interface CacheEntry {
  readonly entryName: string
  readonly bytes: Uint8Array
}

/** Whatever `@effect/platform` failed with. Named so the signatures below read. */
type PlatformFailure = Effect.Effect.Error<ReturnType<FileSystem["writeFile"]>>

/**
 * Log and continue: a cache failure is a performance loss, never an answer.
 *
 * Both loaders re-validate every entry they read — `save_replay._load_cache`
 * against the source save's stat signature, `game_events` against
 * `_places_digest` — so a mirror that quietly did nothing is indistinguishable
 * from a cold cache, which is a legal state.
 */
const bestEffort = (
  step: string,
  gameId: string,
  effect: Effect.Effect<ReadonlyArray<string>, SqlError | PlatformFailure>
): Effect.Effect<ReadonlyArray<string>> =>
  Effect.catchAll(effect, (cause) =>
    Effect.as(
      Effect.logWarning(`derivation cache ${step} skipped`).pipe(
        Effect.annotateLogs({ gameId, cause: String(cause) })
      ),
      []
    ))

/**
 * Build a mirror over an open database and an open filesystem.
 *
 * As in `./materialize.ts`, `fs` and `path` are values rather than context
 * requirements, so every method comes out with `R = never`.
 */
export const makeDerivationCacheMirror = (
  db: Database,
  fs: FileSystem,
  path: Path,
  options: DerivationCacheOptions
): DerivationCacheMirror => {
  // `derivationCacheDirectory` is imported, not transcribed: it is the layout
  // `save_replay._cache_directory` computes, and a second spelling of it here
  // would be a second place to get the trailing-slash handling wrong.
  const directoryOf = (gameId: string): string =>
    derivationCacheDirectory(options.cacheRoot, gameId)

  const cacheKey = options.cacheRoot

  const stored = (
    gameId: string
  ): Effect.Effect<ReadonlyArray<CacheEntry>, SqlError> =>
    db
      .select({ entryName: derivationCache.entryName, bytes: derivationCache.bytes })
      .from(derivationCache)
      .where(and(eq(derivationCache.cacheKey, cacheKey), eq(derivationCache.gameId, gameId)))

  /**
   * The direct children of the working directory that are regular files.
   *
   * An entry that cannot be stat'ed or read contributes nothing rather than
   * failing the step: the loaders would have re-derived it anyway, and a cache
   * directory being written by the very subprocess that just ran is a place
   * where a vanished file is normal.
   */
  const onDisk = (
    directory: string
  ): Effect.Effect<ReadonlyArray<CacheEntry>, PlatformFailure> =>
    Effect.flatMap(fs.readDirectory(directory), (entries) =>
      Effect.map(
        Effect.forEach(entries.toSorted(), (entryName) => {
          const target = path.join(directory, entryName)
          return Effect.orElseSucceed(
            Effect.flatMap(fs.stat(target), (info) =>
              info.type === "File"
                ? Effect.map(fs.readFile(target), (bytes): ReadonlyArray<CacheEntry> => [
                  { entryName, bytes }
                ])
                : Effect.succeed<ReadonlyArray<CacheEntry>>([])),
            (): ReadonlyArray<CacheEntry> => []
          )
        }),
        (found) => found.flat()
      ))

  const hydrate = (gameId: string): Effect.Effect<ReadonlyArray<string>> => {
    const directory = directoryOf(gameId)
    return bestEffort(
      "hydrate",
      gameId,
      Effect.gen(function*() {
        const rows = yield* stored(gameId)
        if (rows.length === 0) {
          return []
        }
        yield* fs.makeDirectory(directory, { recursive: true })
        const present = yield* Effect.orElseSucceed(
          onDisk(directory),
          (): ReadonlyArray<CacheEntry> => []
        )
        const byName = new Map(present.map((entry) => [entry.entryName, entry.bytes]))
        const missing = rows.filter((row) =>
          Option.match(Option.fromNullable(byName.get(row.entryName)), {
            onNone: () => true,
            onSome: (bytes) => !sameBytes(bytes, row.bytes)
          })
        )
        // Written through a temporary and renamed, for the same reason
        // `./materialize.ts` does it: the loaders stat what they read, and a
        // half-written cache entry must never be visible under its final name.
        yield* Effect.forEach(missing, (row) => {
          const target = path.join(directory, row.entryName)
          const temporary = `${target}.tmp-hydrate`
          return Effect.flatMap(fs.writeFile(temporary, row.bytes), () =>
            fs.rename(temporary, target))
        }, { discard: true })
        return missing.map((row) => row.entryName)
      })
    )
  }

  const capture = (gameId: string): Effect.Effect<ReadonlyArray<string>> =>
    bestEffort(
      "capture",
      gameId,
      Effect.gen(function*() {
        const present = yield* Effect.orElseSucceed(
          onDisk(directoryOf(gameId)),
          (): ReadonlyArray<CacheEntry> => []
        )
        if (present.length === 0) {
          return []
        }
        const rows = yield* stored(gameId)
        const byName = new Map(rows.map((row) => [row.entryName, row.bytes]))
        const changed = present.filter((entry) =>
          Option.match(Option.fromNullable(byName.get(entry.entryName)), {
            onNone: () => true,
            onSome: (bytes) => !sameBytes(bytes, entry.bytes)
          })
        )
        yield* Effect.forEach(changed, (entry) =>
          db
            .insert(derivationCache)
            .values({
              cacheKey,
              gameId,
              entryName: entry.entryName,
              bytes: entry.bytes,
              byteSize: entry.bytes.length
            })
            .onConflictDoUpdate({
              target: [
                derivationCache.cacheKey,
                derivationCache.gameId,
                derivationCache.entryName
              ],
              // `now()` server-side rather than a client `Date`: the column is
              // bookkeeping, and the database's clock is the one an operator reads.
              set: {
                bytes: entry.bytes,
                byteSize: entry.bytes.length,
                writtenAt: sql`now()`
              }
            }), { discard: true })
        return changed.map((entry) => entry.entryName)
      })
    )

  return { cacheKey, hydrate, capture }
}

/**
 * Everything `ReplayDerivationPg` needs beyond a database and a materializer.
 *
 * Neither root the archive lives under appears here. `runs_root` is the
 * materializer's scope key and `materializeRoot` is the directory it builds, and
 * the bridge is pointed at whatever the *shared* materializer holds the lock
 * for — a copy passed in beside it could name a different directory, which is
 * the one way this wiring can be wrong. `cacheRoot` stays, because it is this
 * module's own namespace and must remain a *sibling* of the materialize root:
 * `save_replay._cache_directory` raises `SaveReplayError("Replay cache must be
 * separate from game saves.")` for a cache root that nests with a saves
 * directory, and the gateway's default is `<cacheRoot>-saves`.
 */
export type ReplayDerivationPgOptions =
  & Omit<PythonDerivationOptions, "runsRoot">
  & DerivationCacheOptions

/**
 * `ReplayDerivationPython`, over a materialized archive and a mirrored cache.
 *
 * The runner is the *unchanged* `pythonDerivationRunner` — same argv, same
 * stdin protocol, same `parsePythonJsonObject` on the way back, which is what
 * keeps `replay.json`'s integers spelled as integers — pointed at
 * `materializeRoot` instead of at `runs_root`. Everything pg-specific happens
 * around it:
 *
 * ```
 * ensureRunArchive(gameId)  →  hydrate(gameId)  →  bridge  →  capture(gameId)
 * ```
 *
 * All four run **inside** the single `Effect.makeSemaphore(1)` that
 * `makeReplayDerivation` creates, because `layerFromRunner` wraps the whole
 * runner: the on-disk cache is not concurrency-safe, and now neither is the
 * working directory. Materialization is the only step whose failure is fatal to
 * the request — without saves the loaders have nothing to read — and it becomes
 * the 503 a `SaveReplayError` would have become.
 *
 * The materializer comes from the {@link Materializer} tag rather than from a
 * constructor call here, and that is load-bearing rather than tidy: it carries
 * the per-game lock, and the caller it has to be shared with is the archive read
 * on the other side of the gateway. A derivation of game `g` and an archive read
 * of game `g` reconcile the *same* directory, so before the tag they raced
 * through two different mutexes. `replay_lock` is still taken first and the
 * per-game semaphore second — an acquisition order `materialize.ts`'s docstring
 * shows is acyclic.
 */
export const ReplayDerivationPg = (
  options: ReplayDerivationPgOptions
): Layer.Layer<
  ReplayDerivation,
  never,
  PgDrizzle.PgDrizzle | Materializer | FileSystem | Path
> =>
  Layer.unwrapEffect(
    Effect.gen(function*() {
      const db = yield* PgDrizzle.PgDrizzle
      const fs = yield* FileSystemTag
      const path = yield* PathTag
      const materializer = yield* Materializer
      const mirror = makeDerivationCacheMirror(db, fs, path, options)
      const bridge = pythonDerivationRunner({
        repoRoot: options.repoRoot,
        // The bridge's `runs_root` *is* the materialize root: the loaders read
        // the directory this materializer builds, not the real archive.
        runsRoot: materializer.options.materializeRoot,
        cacheRoot: options.cacheRoot,
        ...(options.python === undefined ? {} : { python: options.python }),
        ...(options.timeout === undefined ? {} : { timeout: options.timeout }),
        ...(options.maxOutputBytes === undefined
          ? {}
          : { maxOutputBytes: options.maxOutputBytes })
      })
      const runner: DerivationRunner = (request) =>
        Effect.mapError(materializer.ensureRunArchive(request.gameId), (cause) =>
          new DerivationUnavailable({
            operation: request.operation,
            gameId: request.gameId,
            detail: cause.detail
          })).pipe(
            Effect.flatMap(() => mirror.hydrate(request.gameId)),
            Effect.flatMap(() => bridge(request)),
            Effect.tap(() => mirror.capture(request.gameId))
          )
      return layerFromRunner(runner)
    })
  )
