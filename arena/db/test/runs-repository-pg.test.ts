/**
 * The pg `RunsRepository`, differentially against the filesystem one.
 *
 * **The filesystem repository is the oracle.** Every assertion below runs
 * `makeRunsRepository(runs_root)` and `makeRunsRepositoryPg(db, …)` over the
 * *same* eight parity fixtures and compares the two answers. Nothing here
 * asserts that a `SELECT` returned the right rows — that is not the bar. The bar
 * is that a caller cannot tell the two repositories apart, because the gateway
 * above them is byte-compared against CPython and any difference here becomes a
 * difference there.
 *
 * The corpus is `arena/harness/test/parity/fixtures/runs`, copied into a scratch
 * directory with `cp -R` so the symlinked run (`game_parity_symlink_07 →
 * game_parity_terminal_valid_01`) stays a symlink — it is the fixture that
 * proves the pg backend reproduces a containment refusal it cannot perform,
 * because the ingester never wrote a row for it. A `game.mp4` is added to the
 * one terminal fixture, because the corpus ships none and `videoFile`'s success
 * path would otherwise be untested on both sides.
 *
 * Four comparisons need a word:
 *
 * - **Payloads are compared as canonical text.** `canonicalText(value, CANON_UTF8)`
 *   is the writer the gateway serves with, so comparing its output compares the
 *   response body rather than a structural equality that might tolerate a `1`
 *   where CPython wrote `1.0`.
 * - **Failures are compared as `tag:problem`.** Both repositories fail with the
 *   same `../errors.ts` classes, and that pair decides a status and a message —
 *   the whole observable content of a failure.
 * - **`TerminalArchive.runRoot` is excluded, and only it.** The pg archive points
 *   at the materialized directory (see `src/runs-repository-pg.ts`'s docstring);
 *   every other field, the `manifest` and `report` documents included, is equal.
 * - **Binaries are compared as bytes**, never as paths: the fs backend answers
 *   with a path into `runs_root` and the pg backend with bytes out of `bytea`.
 *
 * Nothing here touches a live Postgres. PGlite runs the same committed
 * migrations and the same drizzle statements, so what is asserted here is
 * asserted about production.
 */

import { BunContext } from "@effect/platform-bun"
import { FileSystem } from "@effect/platform/FileSystem"
import { Path } from "@effect/platform/Path"
import * as PgDrizzle from "@effect/sql-drizzle/Pg"
import { and, eq } from "drizzle-orm"
import * as Effect from "effect/Effect"
import * as Either from "effect/Either"
import * as Layer from "effect/Layer"
import * as ManagedRuntime from "effect/ManagedRuntime"
import * as Option from "effect/Option"
import * as Ref from "effect/Ref"
import { afterAll, describe, expect, it } from "bun:test"
import { readFileSync, rmSync, statSync } from "node:fs"
import { fileURLToPath } from "node:url"

import { CANON_UTF8, type CanonValue, canonicalText, type FrameIndex, Gateway } from "@arena/wire"

import { interruptedCandidates } from "../../harness/src/gateway/archive.ts"
import {
  type BinaryArtifact,
  isArchiveBytes,
  makeRunsRepository,
  RunsRepository,
  type RunsError,
  type RunsRepositoryApi,
  type TerminalArchive
} from "../../harness/src/gateway/services/runs.ts"

import {
  type DerivationRequest,
  pythonDerivationRunner,
  ReplayDerivation
} from "../../harness/src/gateway/services/derivation.ts"

import { makeDerivationCacheMirror, ReplayDerivationPg } from "../src/derivation-cache-pg.ts"
import * as Ingest from "../src/ingest.ts"
import {
  makeMaterializer,
  Materializer,
  nestsWith,
  type RunArchiveMaterializer
} from "../src/materialize.ts"
import * as Migrate from "../src/migrate.ts"
import * as Pglite from "../src/pglite.ts"
import {
  layer as runsRepositoryPgLayer,
  makeRunsRepositoryPg,
  type RunsRepositoryPgApi
} from "../src/runs-repository-pg.ts"
import { derivationCache, derivationWorkdirs, runFrames, runs } from "../src/schema.ts"

// ------------------------------------------------------------------ scratch --

const fixturesRoot = fileURLToPath(
  new URL("../../harness/test/parity/fixtures/runs", import.meta.url)
)

/** The checkout the python bridge is spawned in — it needs `agent_eval/`. */
const repoRoot = fileURLToPath(new URL("../../../", import.meta.url))

/** Every id the corpus contains, including the one that is a symlink. */
const FIXTURE_IDS = [
  "game_parity_terminal_valid_01",
  "game_parity_terminal_nowin_02",
  "game_parity_interrupted_03",
  "game_parity_lobby_husk_04",
  "game_parity_malformed_05",
  "game_parity_wrong_id_06",
  "game_parity_symlink_07",
  "game_parity_torn_tail_08"
]

/** Ids no fixture claims: well-formed absentees and three `GAME_ID_RE` refuses. */
const ABSENT_IDS = [
  "game_parity_absent_99",
  "../escape",
  "game_x",
  "GAME_PARITY_TERMINAL_VALID_01"
]

const EVERY_ID = [...FIXTURE_IDS, ...ABSENT_IDS]

const TERMINAL_ID = "game_parity_terminal_valid_01"
const NOWIN_ID = "game_parity_terminal_nowin_02"

/** A short but structurally real MP4 head, so `videoFile`'s success path exists. */
const VIDEO_BYTES = new Uint8Array([
  0x00, 0x00, 0x00, 0x18, 0x66, 0x74, 0x79, 0x70, 0x69, 0x73, 0x6f, 0x6d, 0x00, 0x00, 0x02, 0x00,
  0x69, 0x73, 0x6f, 0x6d, 0x69, 0x73, 0x6f, 0x32, 0xff, 0xfe, 0x00, 0x5c
])

interface Scratch {
  /** The `mktemp -d` directory everything below lives in, removed in `afterAll`. */
  readonly base: string
  readonly runsRoot: string
  readonly materializeRoot: string
  readonly cacheRoot: string
}

/**
 * A private copy of the corpus, under the process's own temporary directory.
 *
 * `cp -R` rather than a recursive walk: it preserves the symlinked run, and a
 * hand-rolled copier that quietly dereferenced it would delete the one fixture
 * whose entire purpose is to be a symlink. Nothing outside this directory is
 * written, and `runs_root` itself is never modified — the corpus is copied, not
 * used in place.
 */
const makeScratch = async (): Promise<Scratch> => {
  const base = Bun.spawnSync(["mktemp", "-d", "/tmp/arena-pg-repo-XXXXXX"]).stdout.toString().trim()
  const runsRoot = `${base}/runs`
  const copy = Bun.spawnSync(["cp", "-R", fixturesRoot, runsRoot])
  // The one throw in this file, and deliberate: a corpus that did not copy makes
  // every assertion below meaningless, and no value a fixture builder returns
  // would be read by a later expectation as "the environment broke". The house
  // rule's carve-out for a programmer/environment error, not a failure mode of
  // the code under test.
  if (copy.exitCode !== 0) {
    throw new Error(`cp -R failed: ${copy.stderr.toString()}`)
  }
  await Bun.write(`${runsRoot}/${TERMINAL_ID}/game.mp4`, VIDEO_BYTES)
  return { base, runsRoot, materializeRoot: `${base}/cache-saves`, cacheRoot: `${base}/cache` }
}

// -------------------------------------------------------------------- layers --

const testLayer = Layer.mergeAll(
  Layer.provideMerge(PgDrizzle.layer, Pglite.layer(Pglite.memory)),
  BunContext.layer
)

type TestContext = Layer.Layer.Success<typeof testLayer>

/**
 * One PGlite instance for the whole file.
 *
 * A `ManagedRuntime` rather than a per-test `Effect.provide`: the layer is
 * memoized, so every test sees the *same* database, and `dispose` in `afterAll`
 * is what closes it. Building the layer per test would give each one a fresh,
 * empty PGlite — which is right for `schema.test.ts`, whose subject is the
 * migrations, and wrong here, whose subject is a corpus.
 */
const runtime = ManagedRuntime.make(testLayer)

const run = <A, E>(effect: Effect.Effect<A, E, TestContext>): Promise<A> =>
  runtime.runPromise(effect)

afterAll(async () => {
  await runtime.dispose()
  // The corpus copy is ~13 MB and the staged archive is another few; a suite
  // that leaves one of each per run fills a developer's /tmp quietly.
  //
  // `Option` rather than a swallowed `catch`: a fixture that never built has
  // nothing to remove, and teardown is not where that failure is reported —
  // every test that needed it has already failed.
  const built = await fixture.then(Option.some, () => Option.none<Fixture>())
  if (Option.isSome(built) && built.value.scratch.base.startsWith("/tmp/arena-pg-repo-")) {
    rmSync(built.value.scratch.base, { recursive: true, force: true })
  }
})

/** Both repositories over one scratch corpus, plus the handles the tests poke. */
interface Fixture {
  readonly fs: RunsRepositoryApi
  readonly pg: RunsRepositoryPgApi
  readonly scratch: Scratch
  readonly db: PgDrizzle.PgDrizzle["Type"]
  readonly report: Ingest.IngestReport
  /**
   * The *one* materializer the pg repository was built over.
   *
   * Kept on the fixture rather than rebuilt per test because it owns the
   * per-game lock: a second materializer over the same root would be a second
   * lock table, and every test below that says two callers are serialized would
   * be asserting nothing.
   */
  readonly materializer: RunArchiveMaterializer
}

/**
 * `Effect.orDie`: a fixture that cannot be built is a defect, not a failure the
 * suite should classify. It fails every test with the real cause, which is what
 * a broken migration or an unreadable corpus should do.
 */
const buildFixture = (scratch: Scratch): Effect.Effect<Fixture, never, TestContext> =>
  Effect.orDie(Effect.gen(function*() {
    yield* Migrate.run
    const report = yield* Ingest.ingest(Ingest.ingestOptions(scratch.runsRoot))
    const db = yield* PgDrizzle.PgDrizzle
    const fileSystem = yield* FileSystem
    const path = yield* Path
    const fs = makeRunsRepository(scratch.runsRoot)
    const materializer = yield* makeMaterializer(db, fileSystem, path, {
      runsRoot: fs.runsRoot,
      materializeRoot: scratch.materializeRoot
    })
    const pg = makeRunsRepositoryPg(db, materializer)
    return { fs, pg, scratch, db, report, materializer }
  }))

/**
 * The corpus, ingested once.
 *
 * A module-level promise rather than a `beforeAll` writing into a mutable
 * binding: it starts at import, every test awaits the same value, and there is
 * no moment at which the fixture is half-built.
 */
const fixture: Promise<Fixture> = makeScratch().then((scratch) => run(buildFixture(scratch)))

const withFixture = <A, E>(
  body: (fixture: Fixture) => Effect.Effect<A, E, TestContext>
): Promise<A> => fixture.then((value) => run(body(value)))

// --------------------------------------------------------------- comparators --

/** A failure reduced to what a client can see: the status and the message. */
const describeFailure = (error: RunsError): string => `${error._tag}:${error.problem}`

const settle = <A>(
  effect: Effect.Effect<A, RunsError>
): Effect.Effect<Either.Either<A, string>> =>
  Effect.map(Effect.either(effect), Either.mapLeft(describeFailure))

/** The canonical text of a payload — the bytes the gateway would have served. */
const canonical = (value: CanonValue): string =>
  Either.getOrElse(canonicalText(value, CANON_UTF8), (error) => `!canon:${error._tag}`)

/**
 * A payload as the canonical writer sees it.
 *
 * `Canonical<A>` intersects every field with `CanonValue` but the *aliases* the
 * repositories return (`Gateway.Manifest`, an array of rows) are not themselves
 * declared as `CanonValue`, so the writer's parameter needs the assertion. It is
 * checked at run time by `canonicalText`, which refuses anything it cannot spell
 * — an `undefined`, a function, a `NaN` — with an error this file renders into
 * the comparison rather than swallowing.
 */
const asCanon = (value: unknown): CanonValue => value as CanonValue

const bytesOf = (value: Uint8Array): ReadonlyArray<number> => Array.from(value)

const readBytes = (artifact: BinaryArtifact): ReadonlyArray<number> =>
  isArchiveBytes(artifact) ? bytesOf(artifact.bytes) : bytesOf(readFileSync(artifact))

/** `TerminalArchive` minus the one field the two backends spell differently. */
const withoutRunRoot = (archive: TerminalArchive): Omit<TerminalArchive, "runRoot"> => {
  const { runRoot: _runRoot, ...rest } = archive
  return rest
}

const frameIndex = (name: string): Option.Option<FrameIndex> =>
  Option.getRight(Gateway.decodeArchivePngName(name))

// -------------------------------------------------------------------- ingest --

describe("ingest of the parity corpus", () => {
  it("stores every run the fs backend can see, and skips the symlinked one", () =>
    withFixture((f) =>
      Effect.sync(() => {
        expect(f.report.runs.map((result) => result.gameId).toSorted()).toEqual([
          "game_parity_interrupted_03",
          "game_parity_lobby_husk_04",
          "game_parity_malformed_05",
          "game_parity_terminal_nowin_02",
          "game_parity_terminal_valid_01",
          "game_parity_torn_tail_08",
          "game_parity_wrong_id_06"
        ])
        expect(f.report.skipped.map((skip) => skip.entry)).toEqual(["game_parity_symlink_07"])
        // The logical scope key must be the *same string* the fs repository
        // resolved, or every `WHERE runs_root = $1` silently matches nothing.
        expect(f.report.runsRoot).toBe(f.fs.runsRoot)
      })
    ))

  it("is idempotent: a second sweep reports no change", () =>
    withFixture((f) =>
      Effect.map(
        Ingest.ingest(Ingest.ingestOptions(f.scratch.runsRoot)),
        (again) => {
          expect(again.runs.map((result) => result.outcome)).toEqual(
            again.runs.map(() => "unchanged")
          )
        }
      )
    ))
})

// -------------------------------------------------------------- readManifest --

describe("readManifest", () => {
  it.each(EVERY_ID)("agrees with the fs backend for %s", (gameId) =>
    withFixture((f) =>
      Effect.gen(function*() {
        const expected = yield* settle(f.fs.readManifest(gameId))
        const actual = yield* settle(f.pg.readManifest(gameId))
        expect(Either.map(actual, (value) => JSON.stringify(value))).toEqual(
          Either.map(expected, (value) => JSON.stringify(value))
        )
      })
    ))

  it("keeps the three answers distinct: 404 game, 503 unusable, 404 wrong id", () =>
    withFixture((f) =>
      Effect.gen(function*() {
        // The symlinked run has no row at all, so it is `gameNotFound` — the
        // same answer the fs backend reaches through `Path.is_symlink`.
        expect(yield* settle(f.pg.readManifest("game_parity_symlink_07"))).toEqual(
          Either.left("NotFound:gameNotFound")
        )
        // A manifest that is not JSON is stored with `status = 'unusable'` and
        // its bytes intact, so the 503 is reproduced rather than asserted.
        expect(yield* settle(f.pg.readManifest("game_parity_malformed_05"))).toEqual(
          Either.left("ArchiveUnavailable:manifestUnavailable")
        )
        // A manifest whose `game_id` disagrees with its directory is a 404 on
        // the *game*, decided on the read side over the stored bytes.
        expect(yield* settle(f.pg.readManifest("game_parity_wrong_id_06"))).toEqual(
          Either.left("NotFound:gameNotFound")
        )
      })
    ))
})

// ------------------------------------------------------------ decodeManifest --

describe("decodeManifest", () => {
  it.each(FIXTURE_IDS)("agrees with the fs backend for %s", (gameId) =>
    withFixture((f) =>
      Effect.gen(function*() {
        const expected = yield* Effect.either(f.fs.decodeManifest(gameId))
        const actual = yield* Effect.either(f.pg.decodeManifest(gameId))
        expect(Either.map(actual, (value) => canonical(asCanon(value)))).toEqual(
          Either.map(expected, (value) => canonical(asCanon(value)))
        )
        expect(Either.mapLeft(actual, (error) => error._tag)).toEqual(
          Either.mapLeft(expected, (error) => error._tag)
        )
      })
    ))
})

// ----------------------------------------------------------- terminalArchive --

describe("terminalArchive", () => {
  it.each(EVERY_ID)("agrees with the fs backend for %s", (gameId) =>
    withFixture((f) =>
      Effect.gen(function*() {
        const expected = yield* settle(f.fs.terminalArchive(gameId))
        const actual = yield* settle(f.pg.terminalArchive(gameId))
        expect(Either.map(actual, withoutRunRoot)).toEqual(Either.map(expected, withoutRunRoot))
      })
    ))

  it("serves victory.json from the stored bytes, spelling included", () =>
    withFixture((f) =>
      Effect.map(f.pg.terminalArchive(TERMINAL_ID), (archive) => {
        const outcome = canonical(asCanon(archive.outcome))
        // `turn` and `year` are relayed with no validation at all, and the only
        // reason they come out as Python ints is that the *bytes* were re-parsed
        // here rather than a decoded record being stored.
        expect(outcome).toContain(`"turn":753`)
        expect(outcome).toContain(`"year":1995`)
      })
    ))

  it("answers terminalArchiveNotFound for a live run, before it looks for a report", () =>
    withFixture((f) =>
      Effect.map(settle(f.pg.terminalArchive("game_parity_interrupted_03")), (outcome) => {
        expect(outcome).toEqual(Either.left("NotFound:terminalArchiveNotFound"))
      })
    ))
})

// ------------------------------------------------------------ lastReplayTurn --

/**
 * The ids `lastReplayTurn` can be *reached* with.
 *
 * `_last_replay_turn` is the one read in the fs backend that validates nothing:
 * no `GAME_ID_RE`, no symlink refusal, no containment check — it opens
 * `join(runs_root, game_id, "replay.jsonl")` and follows whatever is there. Two
 * ids in this corpus therefore answer out of a run the archive does not contain,
 * and the pg backend cannot follow them because no row exists: the symlinked run
 * (`game_parity_symlink_07 → game_parity_terminal_valid_01`) and, on a
 * case-insensitive filesystem, a case variant of a real id. Both divergences are
 * unreachable through the service, which is asserted below rather than argued.
 */
const UNCONTAINED_IDS = ["game_parity_symlink_07", "GAME_PARITY_TERMINAL_VALID_01"]

const REPLAY_TURN_IDS = EVERY_ID.filter((gameId) => !UNCONTAINED_IDS.includes(gameId))

describe("lastReplayTurn", () => {
  it.each(REPLAY_TURN_IDS)("agrees with the fs backend for %s", (gameId) =>
    withFixture((f) =>
      Effect.gen(function*() {
        const expected = yield* f.fs.lastReplayTurn(gameId)
        const actual = yield* f.pg.lastReplayTurn(gameId)
        expect(Option.getOrNull(actual)).toEqual(Option.getOrNull(expected))
      })
    ))

  it("reads a turn out of a window whose first line is torn", () =>
    withFixture((f) =>
      Effect.gen(function*() {
        const expected = yield* f.fs.lastReplayTurn("game_parity_torn_tail_08")
        const actual = yield* f.pg.lastReplayTurn("game_parity_torn_tail_08")
        expect(Option.isSome(expected)).toBe(true)
        expect(Option.getOrNull(actual)).toEqual(Option.getOrNull(expected))
      })
    ))

  it("diverges only where the fs reader leaves the archive, and only unreachably", () =>
    withFixture((f) =>
      Effect.gen(function*() {
        // The fs reader follows the symlink and the case-insensitive name into a
        // run the archive does contain, and answers from *its* replay.jsonl.
        // The pg reader has no row to follow, so it answers `none`.
        yield* Effect.forEach(UNCONTAINED_IDS, (gameId) =>
          Effect.gen(function*() {
            expect(Option.isSome(yield* f.fs.lastReplayTurn(gameId))).toBe(true)
            expect(Option.isNone(yield* f.pg.lastReplayTurn(gameId))).toBe(true)
          }), { discard: true })

        // ...and neither id can ever be asked about, because the only caller is
        // `diskRowsWithInterrupted`, whose candidates come from the index, whose
        // ids are the run directories the walk accepted.
        const rows = yield* f.pg.diskGamesIndex()
        const candidates = interruptedCandidates(rows.games, new Set<string>())
        expect(candidates.some((gameId) => UNCONTAINED_IDS.includes(gameId))).toBe(false)
        const fsRows = yield* f.fs.diskGamesIndex()
        expect(
          interruptedCandidates(fsRows.games, new Set<string>()).some((gameId) =>
            UNCONTAINED_IDS.includes(gameId)
          )
        ).toBe(false)
      })
    ))

  it("hides a lobby husk exactly as the fs backend does", () =>
    withFixture((f) =>
      Effect.gen(function*() {
        const expected = yield* f.fs.lastReplayTurn("game_parity_lobby_husk_04")
        const actual = yield* f.pg.lastReplayTurn("game_parity_lobby_husk_04")
        expect(Option.isNone(expected)).toBe(true)
        expect(Option.isNone(actual)).toBe(true)
      })
    ))
})

// ------------------------------------------------------------ diskGamesIndex --

describe("diskGamesIndex", () => {
  it("is byte-identical to the fs index", () =>
    withFixture((f) =>
      Effect.gen(function*() {
        const expected = yield* f.fs.diskGamesIndex()
        const actual = yield* f.pg.diskGamesIndex()
        expect(canonical(actual)).toBe(canonical(expected))
        // Not vacuous: the corpus really does produce rows.
        expect(actual.games.length).toBeGreaterThan(0)
      })
    ))

  it("is byte-identical with terminalOnly", () =>
    withFixture((f) =>
      Effect.gen(function*() {
        const expected = yield* f.fs.diskGamesIndex({ terminalOnly: true })
        const actual = yield* f.pg.diskGamesIndex({ terminalOnly: true })
        expect(canonical(actual)).toBe(canonical(expected))
        expect(actual.games.length).toBeLessThan(
          (yield* f.pg.diskGamesIndex()).games.length
        )
      })
    ))
})

// -------------------------------------------------- diskRowsWithInterrupted --

const LIVE_SETS: Array<[string, Array<string>]> = [
  ["(none)", []],
  ["interrupted", ["game_parity_interrupted_03"]],
  ["terminal", [TERMINAL_ID]],
  ["two running", ["game_parity_interrupted_03", "game_parity_torn_tail_08"]],
  ["husk and absentee", ["game_parity_lobby_husk_04", "game_parity_absent_99"]]
]

describe("diskRowsWithInterrupted", () => {
  it.each(LIVE_SETS)("agrees with the fs backend when live = %s", (_label, ids) =>
    withFixture((f) =>
      Effect.gen(function*() {
        const live = new Set<string>(ids)
        const expected = yield* f.fs.diskRowsWithInterrupted(live)
        const actual = yield* f.pg.diskRowsWithInterrupted(live)
        expect(canonical(asCanon(actual))).toBe(canonical(asCanon(expected)))
      })
    ))

  it("relabels an orphaned run through archive.ts, not through SQL", () =>
    withFixture((f) =>
      Effect.map(f.pg.diskRowsWithInterrupted(new Set<string>()), (rows) => {
        const interrupted = rows.filter((row) => row.state === Gateway.INTERRUPTED_STATUS)
        expect(interrupted.length).toBeGreaterThan(0)
        // The summary quotes the *post-`max`* turn, which is what `asInterrupted`
        // computes and what no `SELECT` could have produced.
        expect(
          interrupted.map((row) => row.outcome.summary)
        ).toEqual(
          interrupted.map((row) =>
            Gateway.interruptedSummary(
              typeof row.current_turn === "bigint" ? row.current_turn : 0n
            )
          )
        )
      })
    ))
})

// ----------------------------------------------------------------- binaries --

describe("frameFile and videoFile", () => {
  const archives = (f: Fixture) =>
    Effect.all({ fs: f.fs.terminalArchive(TERMINAL_ID), pg: f.pg.terminalArchive(TERMINAL_ID) })

  it("serves the same PNG bytes for latest and for every stored index", () =>
    withFixture((f) =>
      Effect.gen(function*() {
        const both = yield* archives(f)
        const wanted: ReadonlyArray<Option.Option<FrameIndex>> = [
          Option.none(),
          frameIndex("000000.png"),
          frameIndex("000752.png")
        ]
        yield* Effect.forEach(wanted, (index) =>
          Effect.gen(function*() {
            const expected = yield* settle(f.fs.frameFile(both.fs, index))
            const actual = yield* settle(f.pg.frameBytes(both.pg, index))
            expect(Either.isRight(expected)).toBe(true)
            expect(Either.map(actual, bytesOf)).toEqual(Either.map(expected, readBytes))
          }), { discard: true })
      })
    ))

  it("refuses an index that is not there, and a run whose frames directory failed", () =>
    withFixture((f) =>
      Effect.gen(function*() {
        const both = yield* archives(f)
        const missing = frameIndex("000123.png")
        expect(Option.isSome(missing)).toBe(true)
        expect(yield* settle(f.pg.frameBytes(both.pg, missing))).toEqual(
          Either.left("NotFound:mapFrameDoesNotExist")
        )
        expect(
          Either.getLeft(yield* settle(f.fs.frameFile(both.fs, missing)))
        ).toEqual(Either.getLeft(yield* settle(f.pg.frameBytes(both.pg, missing))))

        // `game_parity_terminal_nowin_02` has frames but no `saves/`; its PNGs
        // are still listable, so the two backends must agree on that too.
        const nowin = yield* Effect.all({
          fs: f.fs.terminalArchive(NOWIN_ID),
          pg: f.pg.terminalArchive(NOWIN_ID)
        })
        const expected = yield* settle(f.fs.frameFile(nowin.fs, Option.none()))
        const actual = yield* settle(f.pg.frameBytes(nowin.pg, Option.none()))
        expect(Either.map(actual, bytesOf)).toEqual(Either.map(expected, readBytes))
      })
    ))

  it("serves the same video bytes, and the same 404 when there is none", () =>
    withFixture((f) =>
      Effect.gen(function*() {
        const both = yield* archives(f)
        const expected = yield* settle(f.fs.videoFile(both.fs))
        const actual = yield* settle(f.pg.videoBytes(both.pg))
        expect(Either.map(actual, bytesOf)).toEqual(Either.map(expected, readBytes))
        expect(Either.map(actual, bytesOf)).toEqual(Either.right(bytesOf(VIDEO_BYTES)))

        const nowin = yield* Effect.all({
          fs: f.fs.terminalArchive(NOWIN_ID),
          pg: f.pg.terminalArchive(NOWIN_ID)
        })
        expect(yield* settle(f.pg.videoBytes(nowin.pg))).toEqual(
          Either.left("NotFound:replayVideoNotFound")
        )
        expect(yield* settle(f.fs.videoFile(nowin.fs))).toEqual(
          Either.left("NotFound:replayVideoNotFound")
        )
      })
    ))

  it("answers bytes artifacts straight from bytea, no staged file", () =>
    withFixture((f) =>
      Effect.gen(function*() {
        const archive = yield* f.pg.terminalArchive(TERMINAL_ID)
        const frameArtifact = yield* f.pg.frameFile(archive, Option.none())
        const videoArtifact = yield* f.pg.videoFile(archive)
        const frameBytes = yield* f.pg.frameBytes(archive, Option.none())
        const videoBytes = yield* f.pg.videoBytes(archive)

        // `http/routes/archive.ts#sendArtifact` streams a bytes artifact with
        // contentLength = bytes.length, so identity with the stored bytes is
        // the whole contract; no file is staged for frames or video.
        expect(isArchiveBytes(frameArtifact)).toBe(true)
        expect(isArchiveBytes(videoArtifact)).toBe(true)
        expect(readBytes(frameArtifact)).toEqual(bytesOf(frameBytes))
        expect(readBytes(videoArtifact)).toEqual(bytesOf(videoBytes))
      })
    ))
})

// -------------------------------------------------------------- materialized --

describe("materialization", () => {
  it("reproduces the run's saves and frames byte for byte", () =>
    withFixture((f) =>
      Effect.map(f.pg.terminalArchive(TERMINAL_ID), (archive) => {
        const source = `${f.fs.runsRoot}/${TERMINAL_ID}`
        // An autosave is stored whole, so it must match the original exactly —
        // this is the file the python bridge actually parses.
        const save = "saves/turn-0001-auto.sav.gz"
        expect(readBytes(`${archive.runRoot}/${save}`)).toEqual(readBytes(`${source}/${save}`))
        const frame = "watch_frames/000000.png"
        expect(readBytes(`${archive.runRoot}/${frame}`)).toEqual(readBytes(`${source}/${frame}`))
        // A PPM is deliberately a *head*: no reader looks past the header, and
        // storing 2.4 MB of pixels per frame is what the head exists to avoid.
        const ppm = "saves/turn-0001-M-bc--tuZ1Pall.map.ppm"
        const staged = readFileSync(`${archive.runRoot}/${ppm}`)
        const original = readFileSync(`${source}/${ppm}`)
        expect(staged.length).toBeLessThanOrEqual(original.length)
        expect(bytesOf(staged)).toEqual(bytesOf(original.subarray(0, staged.length)))
      })
    ))

  it("changes no inode and no ctime when nothing changed", () =>
    withFixture((f) =>
      Effect.gen(function*() {
        const first = yield* f.pg.terminalArchive(TERMINAL_ID)
        const target = `${first.runRoot}/saves/turn-0001-auto.sav.gz`
        const before = statSync(target)
        yield* f.pg.terminalArchive(TERMINAL_ID)
        const after = statSync(target)
        // `save_replay._load_cache` validates its entries against exactly these
        // fields; an unconditional rewrite would silently and permanently
        // disable the python-side cache.
        expect(after.ino).toBe(before.ino)
        expect(after.ctimeMs).toBe(before.ctimeMs)
        expect(after.mtimeMs).toBe(before.mtimeMs)
      })
    ))

  it("rewrites a file whose content changed but whose length did not", () =>
    withFixture((f) =>
      Effect.gen(function*() {
        const before = yield* f.pg.terminalArchive(TERMINAL_ID)
        const staged = `${before.runRoot}/watch_frames/000000.png`
        const original = readFileSync(staged)
        const frameKey = and(
          eq(runFrames.runsRoot, f.fs.runsRoot),
          eq(runFrames.gameId, TERMINAL_ID),
          eq(runFrames.name, "000000.png")
        )
        const stored = yield* f.db
          .select({ bytes: runFrames.bytes, sha256: runFrames.sha256 })
          .from(runFrames)
          .where(frameKey)
        const restore = stored[0]
        expect(restore).toBeDefined()

        // A same-length re-ingest is the case a size-only freshness check gets
        // wrong: nothing about the file's `stat` changes, and serving the old
        // bytes forever would be silent.
        const replacement = new Uint8Array(original.length).fill(0x5a)
        yield* f.db
          .update(runFrames)
          .set({ bytes: replacement, sha256: new Uint8Array(32).fill(0x11) })
          .where(frameKey)

        const after = yield* f.pg.terminalArchive(TERMINAL_ID)
        expect(readBytes(`${after.runRoot}/watch_frames/000000.png`)).toEqual(
          bytesOf(replacement)
        )

        // Put the row back and materialize again, so the tests after this one
        // see the corpus. A re-ingest would *not* do it: `runs.content_hash`
        // still matches the untouched disk, which is exactly the zero-write
        // property the ingester is built for.
        yield* f.db
          .update(runFrames)
          .set({
            bytes: restore?.bytes ?? new Uint8Array(),
            sha256: restore?.sha256 ?? new Uint8Array()
          })
          .where(frameKey)
        yield* f.pg.terminalArchive(TERMINAL_ID)
        expect(readBytes(staged)).toEqual(bytesOf(original))
      })
    ))

  it("removes a file no row names", () =>
    withFixture((f) =>
      Effect.gen(function*() {
        const archive = yield* f.pg.terminalArchive(TERMINAL_ID)
        const fileSystem = yield* FileSystem
        const intruder = `${archive.runRoot}/saves/intruder.sav.gz`
        yield* fileSystem.writeFileString(intruder, "not from the archive")
        expect(yield* fileSystem.exists(intruder)).toBe(true)

        // Drop the recorded set digest so the next call reconciles instead of
        // short-circuiting — which is exactly what a re-ingest would do.
        yield* f.db
          .delete(derivationWorkdirs)
          .where(
            and(
              eq(derivationWorkdirs.runsRoot, f.fs.runsRoot),
              eq(derivationWorkdirs.gameId, TERMINAL_ID)
            )
          )

        yield* f.pg.terminalArchive(TERMINAL_ID)
        expect(yield* fileSystem.exists(intruder)).toBe(false)
        // And the real files are still there, untouched.
        expect(yield* fileSystem.exists(`${archive.runRoot}/saves/turn-0001-auto.sav.gz`)).toBe(
          true
        )
      })
    ))
})

// -------------------------------------------------------------- cache mirror --

describe("the derivation cache mirror", () => {
  it("captures what the bridge wrote and hydrates it back, byte for byte", () =>
    withFixture((f) =>
      Effect.gen(function*() {
        const fileSystem = yield* FileSystem
        const path = yield* Path
        const mirror = makeDerivationCacheMirror(f.db, fileSystem, path, {
          cacheRoot: f.scratch.cacheRoot
        })
        const directory = `${f.scratch.cacheRoot}/${TERMINAL_ID}`
        // `2.0` must not become `2`, and the escaped NUL must survive: neither
        // would through a `json` or `jsonb` column.
        const document = new TextEncoder().encode(`{"turns":[1,2.0],"nul":"a\\u0000b"}`)

        yield* fileSystem.makeDirectory(directory, { recursive: true })
        yield* fileSystem.writeFile(`${directory}/events.json`, document)

        expect(yield* mirror.capture(TERMINAL_ID)).toEqual(["events.json"])
        // A second capture writes nothing: the bytes are unchanged.
        expect(yield* mirror.capture(TERMINAL_ID)).toEqual([])

        const stored = yield* f.db
          .select()
          .from(derivationCache)
          .where(eq(derivationCache.gameId, TERMINAL_ID))
        expect(stored.length).toBe(1)
        // The namespace key is the resolved `--cache-root`, exactly as the
        // on-disk cache is namespaced.
        expect(stored[0]?.cacheKey).toBe(f.scratch.cacheRoot)
        expect(bytesOf(stored[0]?.bytes ?? new Uint8Array())).toEqual(bytesOf(document))

        yield* fileSystem.remove(directory, { recursive: true })
        expect(yield* mirror.hydrate(TERMINAL_ID)).toEqual(["events.json"])
        expect(bytesOf(yield* fileSystem.readFile(`${directory}/events.json`))).toEqual(
          bytesOf(document)
        )
        // Hydrating again is a no-op.
        expect(yield* mirror.hydrate(TERMINAL_ID)).toEqual([])
      })
    ))
})

// ------------------------------------------------- physical-order independence --

/**
 * The index cannot depend on the heap order of the rows.
 *
 * This is the test that would fail the first time someone "optimizes"
 * `diskGamesIndex` with an `ORDER BY`: Postgres orders `text` by collation and
 * `sortDiskGameRows` orders by code point, and the two disagree above U+FFFF —
 * but only for some inputs, which is why the property is asserted over several
 * physical orders rather than argued about.
 *
 * It runs last because it rebuilds the corpus in the database; the final sweep
 * restores it.
 */
describe("physical order independence", () => {
  it("answers the same index whatever order the rows were written in", () =>
    withFixture((f) =>
      Effect.gen(function*() {
        const baseline = canonical(yield* f.pg.diskGamesIndex())
        const ids = f.report.runs.map((result) => result.gameId).toSorted()

        yield* Effect.forEach([0, 2, 3, 5], (offset) =>
          Effect.gen(function*() {
            // Cascades to every child table, so the whole corpus goes.
            yield* f.db.delete(runs).where(eq(runs.runsRoot, f.fs.runsRoot))
            const rotated = [...ids.slice(offset % ids.length), ...ids.slice(0, offset % ids.length)]
            yield* Effect.forEach(rotated, (gameId) =>
              Ingest.ingest({
                ...Ingest.ingestOptions(f.scratch.runsRoot),
                gameIds: new Set([gameId])
              }), { discard: true, concurrency: 1 })
            expect(canonical(yield* f.pg.diskGamesIndex())).toBe(baseline)
          }), { discard: true, concurrency: 1 })

        yield* Ingest.ingest(Ingest.ingestOptions(f.scratch.runsRoot))
        expect(canonical(yield* f.pg.diskGamesIndex())).toBe(baseline)
      })
    ))
})

// ------------------------------------------------------- the python bridge --

/**
 * The claim `./materialize.ts` exists to make, tested end to end.
 *
 * `python3 -m agent_eval.replay_derive_cli` is run **twice** with the same
 * request: once against the real `runs_root`, and once through
 * {@link ReplayDerivationPg} against the materialized one. The two documents must
 * be byte-identical, which is what "`ReplayDerivationPython` works unchanged in
 * pg mode" means — and it is also the differential that licenses storing a PPM
 * as a *head*, since `board.json` is built from the PPM header the loader reads.
 *
 * The two runs get **different** cache roots, for the same reason the parity rig
 * gives its two gateways different ones: sharing would let the first run answer
 * for the second, and the comparison would then be of one document with itself.
 */
describe("the python derivation bridge over a materialized archive", () => {
  const REQUEST: DerivationRequest = {
    operation: "board",
    gameId: TERMINAL_ID,
    places: [],
    turn: 1n
  }

  it("produces a byte-identical board.json from the materialized saves", () =>
    withFixture((f) =>
      Effect.gen(function*() {
        // The oracle: the bridge against the archive as it really is on disk.
        const expected = yield* pythonDerivationRunner({
          repoRoot,
          runsRoot: f.fs.runsRoot,
          cacheRoot: `${f.scratch.cacheRoot}-oracle`
        })(REQUEST)

        // The layer under test *is* the entry point here: this test exists to
        // build it and run one derivation through it. It is given the fixture's
        // materializer — the same instance the repository holds — because that
        // sharing is what the bridge's roots and its per-game lock both come
        // from.
        // oxlint-disable-next-line effecttsgo/strict-effect-provide
        const actual = yield* Effect.provide(
          Effect.flatMap(ReplayDerivation, (derivation) =>
            derivation.board({ gameId: TERMINAL_ID, turn: 1n })),
          ReplayDerivationPg({
            repoRoot,
            cacheRoot: `${f.scratch.cacheRoot}-bridge`
          }).pipe(Layer.provide(Layer.succeed(Materializer, f.materializer)))
        )

        expect(canonical(asCanon(actual))).toBe(canonical(asCanon(expected)))
        // Not vacuous: a board really was derived.
        expect(Object.keys(actual).length).toBeGreaterThan(0)
      })
    ), 120_000)

  it("captures the bridge's cache entries into the mirror", () =>
    withFixture((f) =>
      Effect.gen(function*() {
        const rows = yield* f.db
          .select({ cacheKey: derivationCache.cacheKey, entryName: derivationCache.entryName })
          .from(derivationCache)
          .where(eq(derivationCache.cacheKey, `${f.scratch.cacheRoot}-bridge`))
        // The bridge wrote `turn-…json` under its own cache root, and `capture`
        // carried it into the durable copy under exactly that namespace key.
        expect(rows.length).toBeGreaterThan(0)
        expect(rows.every((row) => row.entryName.endsWith(".json"))).toBe(true)
      })
    ))

  it("refuses a materialize root that nests with the cache root", () => {
    // `save_replay._cache_directory` raises `SaveReplayError` for these, and it
    // has to be caught at construction rather than as a 503 on first replay.
    expect(nestsWith("/srv/cache", "/srv/cache")).toBe(true)
    expect(nestsWith("/srv/cache/saves", "/srv/cache")).toBe(true)
    expect(nestsWith("/srv/cache", "/srv/cache/saves")).toBe(true)
    expect(nestsWith("/srv/cache/", "/srv/cache")).toBe(true)
    // The default the gateway should pass: a sibling, not a relative.
    expect(nestsWith("/srv/cache-saves", "/srv/cache")).toBe(false)
  })
})

// ------------------------------------------------ one lock, every caller --

/**
 * A `FileSystem` that records how many fibers are inside one game's directory.
 *
 * The materializer's reconciliation is idempotent but not concurrent — it
 * lists the directory, writes through `<name>.tmp-<digest>` and `rename(2)`,
 * then deletes every entry no row names — so two fibers inside
 * `<materialize-root>/<game-id>` at once is precisely the defect: one deletes
 * the other's temporary file, one lists a tree the other is half-way through
 * rewriting, and the loser fails with `MaterializeFailed`, which the gateway
 * serves as a 503. Measured before the lock, on a 24-way burst against one cold
 * game: `200` twice and `503` twenty-two times in round 0, then `200` 24/24 in
 * every round after — a defect only a cold burst can see.
 *
 * Every call is attributed to the game whose directory it touches and delayed a
 * millisecond, which widens the window enough that an unlocked materializer
 * fails these tests every time rather than occasionally.
 */
interface DirectoryProbe {
  /** The instrumented filesystem, to build a materializer over. */
  readonly fs: FileSystem
  /** The most fibers ever inside one game's directory at once. */
  readonly peak: (gameId: string) => number
  /** The most fibers ever inside the materialize root at once, any game. */
  readonly peakOverall: () => number
}

const directoryProbe = (fileSystem: FileSystem, materializeRoot: string): DirectoryProbe => {
  const inside = new Map<string, number>()
  const peaks = new Map<string, number>()
  const overall = { inside: 0, peak: 0 }

  /** The game a path belongs to: the first segment under the materialize root. */
  const gameOf = (target: string): Option.Option<string> => {
    const [first] = target.startsWith(`${materializeRoot}/`)
      ? target.slice(materializeRoot.length + 1).split("/")
      : []
    return first === undefined || first === "" ? Option.none() : Option.some(first)
  }

  const enter = (gameId: string): void => {
    const depth = (inside.get(gameId) ?? 0) + 1
    inside.set(gameId, depth)
    peaks.set(gameId, Math.max(peaks.get(gameId) ?? 0, depth))
    overall.inside += 1
    overall.peak = Math.max(overall.peak, overall.inside)
  }

  const leave = (gameId: string): void => {
    inside.set(gameId, (inside.get(gameId) ?? 1) - 1)
    overall.inside -= 1
  }

  const guard = <A, E>(target: string, effect: Effect.Effect<A, E>): Effect.Effect<A, E> =>
    Option.match(gameOf(target), {
      onNone: () => effect,
      onSome: (gameId) =>
        Effect.acquireUseRelease(
          Effect.sync(() => enter(gameId)),
          () => Effect.flatMap(Effect.sleep("1 millis"), () => effect),
          () => Effect.sync(() => leave(gameId))
        )
    })

  return {
    // Every call the materializer makes, so an overlap cannot hide in the one
    // method the probe forgot.
    fs: {
      ...fileSystem,
      makeDirectory: (target, options) => guard(target, fileSystem.makeDirectory(target, options)),
      readDirectory: (target, options) => guard(target, fileSystem.readDirectory(target, options)),
      readFileString: (target, encoding) =>
        guard(target, fileSystem.readFileString(target, encoding)),
      remove: (target, options) => guard(target, fileSystem.remove(target, options)),
      // By destination: a rename into the tree is a write to the tree.
      rename: (from, to) => guard(to, fileSystem.rename(from, to)),
      stat: (target) => guard(target, fileSystem.stat(target)),
      writeFile: (target, data, options) =>
        guard(target, fileSystem.writeFile(target, data, options)),
      writeFileString: (target, data, options) =>
        guard(target, fileSystem.writeFileString(target, data, options))
    },
    peak: (gameId) => peaks.get(gameId) ?? 0,
    peakOverall: () => overall.peak
  }
}

/** A materializer over a private, cold materialize root and a probe. */
const probedMaterializer = (
  f: Fixture,
  name: string
): Effect.Effect<
  { readonly materializer: RunArchiveMaterializer; readonly probe: DirectoryProbe },
  never,
  TestContext
> =>
  Effect.gen(function*() {
    const fileSystem = yield* FileSystem
    const path = yield* Path
    // A root of its own per test, so every one of them starts *cold*: a warm
    // tree short-circuits on the recorded digest and reconciles nothing, which
    // would make these tests pass against a materializer with no lock at all.
    const materializeRoot = `${f.scratch.base}/${name}`
    const probe = directoryProbe(fileSystem, materializeRoot)
    const materializer = yield* makeMaterializer(f.db, probe.fs, path, {
      runsRoot: f.fs.runsRoot,
      materializeRoot
    })
    return { materializer, probe }
  })

/**
 * The lock the cold-burst 503s needed, where every caller can reach it.
 *
 * It used to be a per-game semaphore in the gateway's repository decorator
 * (`main.ts`'s `withFailureLog`), which covered the archive reads and nothing
 * else — the derivation bridge materializes the same directory through
 * `ReplayDerivationPg`, holding only `replay_lock`, so a replay and an archive
 * read of one game still raced. The lock is `makeMaterializer`'s now, and the
 * materializer is a service both of them take from the same tag.
 *
 * These tests share the fixture's database and corpus but never its materialize
 * root: each builds its own, so `derivation_workdirs.path` is re-stamped as they
 * run. That is a legal state — the row records where the archive was last built
 * — and the reads above re-materialize under their own root if they follow.
 */
describe("materialization is single-flight per game", () => {
  it("a 24-way burst on one cold game never has two fibers in its directory", () =>
    withFixture((f) =>
      Effect.gen(function*() {
        const { materializer, probe } = yield* probedMaterializer(f, "lock-burst")
        const roots = yield* Effect.all(
          Array.from({ length: 24 }, () => materializer.ensureRunArchive(TERMINAL_ID)),
          { concurrency: "unbounded" }
        )

        expect(probe.peak(TERMINAL_ID)).toBe(1)
        // Not vacuous: the archive really was built, and every caller was told
        // the same directory rather than 22 of them getting a 503.
        expect(new Set(roots).size).toBe(1)
        const fileSystem = yield* FileSystem
        expect(yield* fileSystem.exists(`${roots[0] ?? ""}/saves/turn-0001-auto.sav.gz`)).toBe(true)
      })
    ), 60_000)

  it("different games are still materialized in parallel", () =>
    withFixture((f) =>
      Effect.gen(function*() {
        const games = [TERMINAL_ID, NOWIN_ID, "game_parity_interrupted_03", "game_parity_torn_tail_08"]
        const { materializer, probe } = yield* probedMaterializer(f, "lock-parallel")
        yield* Effect.all(
          games.flatMap((gameId) =>
            Array.from({ length: 4 }, () => materializer.ensureRunArchive(gameId))
          ),
          { concurrency: "unbounded" }
        )

        // Serialized within a game…
        expect(games.map((gameId) => probe.peak(gameId))).toEqual(games.map(() => 1))
        // …and not across games: the lock is per id, not one global mutex.
        expect(probe.peakOverall()).toBeGreaterThan(1)
      })
    ), 60_000)

  it("a derivation and an archive read of one game take the same lock", () =>
    withFixture((f) =>
      Effect.gen(function*() {
        const { materializer, probe } = yield* probedMaterializer(f, "lock-bridge")
        const repository = makeRunsRepositoryPg(f.db, materializer)

        // The bridge is pointed at a python that does not exist, so it fails
        // the instant materialization hands off to it: what is under test is
        // the reconciliation *before* the spawn, and a real derivation here
        // would spend a minute proving nothing extra.
        const bridge = ReplayDerivationPg({
          repoRoot,
          cacheRoot: `${f.scratch.cacheRoot}-lock`,
          python: "/nonexistent/arena-lock-probe-python3"
        }).pipe(Layer.provide(Layer.succeed(Materializer, materializer)))

        const [archive, derived] = yield* Effect.all(
          [
            Effect.either(repository.terminalArchive(TERMINAL_ID)),
            // oxlint-disable-next-line effecttsgo/strict-effect-provide
            Effect.provide(
              Effect.either(
                Effect.flatMap(ReplayDerivation, (derivation) =>
                  derivation.board({ gameId: TERMINAL_ID, turn: 1n }))
              ),
              bridge
            )
          ],
          { concurrency: "unbounded" }
        )

        // The claim: two callers that hold *different* mutexes reconciled the
        // same directory one at a time, because the mutex they share is the
        // materializer's.
        expect(probe.peak(TERMINAL_ID)).toBe(1)
        // The archive read answered rather than 503-ing on a half-built tree…
        expect(Either.isRight(archive)).toBe(true)
        // …and the derivation got all the way past materialization to the
        // spawn, which is the only reason it failed.
        expect(
          Either.match(derived, {
            onLeft: (error) => `${error._tag}:${error.detail.slice(0, 12)}`,
            onRight: () => "derived"
          })
        ).toBe("DerivationUnavailable:spawn failed")
      })
    ), 60_000)

  it("the repository and the bridge are built from one materializer", () =>
    withFixture((f) =>
      Effect.gen(function*() {
        // The wiring `main.ts` relies on, as an assertion: one materializer
        // layer handed to a `Layer.merge` of both consumers is *built once*,
        // because `Layer.provide` memoizes by layer reference. If that ever
        // stopped holding, both halves would still typecheck and each would
        // quietly get a lock table of its own — the exact defect the tag exists
        // to make impossible.
        const fileSystem = yield* FileSystem
        const path = yield* Path
        const built = yield* Ref.make(0)
        const materializerLayer = Layer.effect(
          Materializer,
          Effect.flatMap(Ref.update(built, (count) => count + 1), () =>
            makeMaterializer(f.db, fileSystem, path, {
              runsRoot: f.fs.runsRoot,
              materializeRoot: `${f.scratch.base}/lock-shared`
            }))
        )

        // oxlint-disable-next-line effecttsgo/strict-effect-provide
        yield* Effect.provide(
          Effect.all([RunsRepository, ReplayDerivation], { concurrency: 1 }),
          Layer.merge(
            runsRepositoryPgLayer,
            ReplayDerivationPg({ repoRoot, cacheRoot: `${f.scratch.cacheRoot}-shared` })
          ).pipe(Layer.provide(materializerLayer))
        )

        expect(yield* Ref.get(built)).toBe(1)
      })
    ))

  it("two materializers over one root are two lock tables — the shape this removed", () =>
    withFixture((f) =>
      Effect.gen(function*() {
        // The falsifier for the three tests above: the probe reports an overlap
        // the moment the callers stop sharing a materializer, which is exactly
        // what the gateway had when the archive read locked in `main.ts` and the
        // derivation bridge built a materializer of its own. Nothing asserts
        // success here — a losing fiber failing with `MaterializeFailed` *is*
        // the defect, and it is why the lock has one home.
        const fileSystem = yield* FileSystem
        const path = yield* Path
        const materializeRoot = `${f.scratch.base}/lock-unshared`
        const probe = directoryProbe(fileSystem, materializeRoot)
        const options = { runsRoot: f.fs.runsRoot, materializeRoot }
        const first = yield* makeMaterializer(f.db, probe.fs, path, options)
        const second = yield* makeMaterializer(f.db, probe.fs, path, options)

        yield* Effect.all(
          [first, second].flatMap((materializer) =>
            Array.from({ length: 6 }, () =>
              Effect.either(materializer.ensureRunArchive(TERMINAL_ID)))
          ),
          { concurrency: "unbounded" }
        )

        expect(probe.peak(TERMINAL_ID)).toBeGreaterThan(1)
      })
    ), 60_000)
})
