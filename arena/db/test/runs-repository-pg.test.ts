/**
 * The pg `RunsRepository`, differentially against the filesystem one.
 *
 * **The filesystem repository is the oracle.** Every assertion below runs
 * `makeRunsRepository(runs_root)` and `makeRunsRepositoryPg(db, runs_root)` over
 * the *same* eight parity fixtures and compares the two answers, method for
 * method. Nothing here asserts that a `SELECT` returned the right rows — that is
 * not the bar. The bar is that a caller cannot tell the two repositories apart,
 * because the gateway above them is byte-compared against CPython and any
 * difference here becomes a difference there.
 *
 * Two things changed from the v1 version of this file, and both make the
 * comparison *stronger*:
 *
 * - **`TerminalArchive.runRoot` is no longer excluded.** v1 pointed a pg archive
 *   at a materialized mirror, so one field had to be waived. v2 resolves
 *   `<runs-root>/<game_id>` in both backends, so the archives are compared
 *   whole.
 * - **The lifted config columns are poisoned on purpose.** `name`, `ruleset`,
 *   `mode`, `timing_mode`, `max_turns` and `objective` are written with values
 *   that are *not* what `config` says (see {@link POISON}). They are write-only
 *   query projections that have never been through `publicText`, and a pg-side
 *   answer built from one would be silently, differently truncated. If any of
 *   them ever reaches a response body, every payload comparison below fails
 *   loudly instead of by two characters.
 *
 * ## The ingest stand-in, and why it is here
 *
 * `src/ingest.ts`'s v2 rewrite is a separate work item; this file needs rows, so
 * {@link storeRun} is a **minimal, deliberately dumb** implementation of the
 * plan's §1.2 storage rules: the two document gates, the demotion table, the
 * `extras` envelope and `_last_replay_turn`'s window. It writes no `seats`, no
 * `turns`, no `player_turns` and no `board_state` — the repository reads none of
 * them. When the real ingest lands, this function is deleted and the fixture
 * calls it instead; what must **not** happen is that the real ingest quietly
 * disagrees with it, so `describe("the storage rules the repository depends on")`
 * pins the three rules a rewrite could get wrong and still look green.
 *
 * Nothing here touches a live Postgres. PGlite runs the same committed
 * migrations and the same drizzle statements, so what is asserted here is
 * asserted about production.
 */

import { BunContext } from "@effect/platform-bun"
import * as PgDrizzle from "@effect/sql-drizzle/Pg"
import { eq } from "drizzle-orm"
import * as Effect from "effect/Effect"
import * as Either from "effect/Either"
import * as Layer from "effect/Layer"
import * as ManagedRuntime from "effect/ManagedRuntime"
import * as Option from "effect/Option"
import { afterAll, describe, expect, it } from "bun:test"
import { createHash } from "node:crypto"
import {
  closeSync,
  fstatSync,
  lstatSync,
  openSync,
  readdirSync,
  readFileSync,
  readSync,
  rmSync,
  statSync
} from "node:fs"
import { join } from "node:path"
import { fileURLToPath } from "node:url"

import {
  CANON_UTF8,
  type CanonValue,
  canonicalText,
  decodeJsonValueFromString,
  type FrameIndex,
  Gateway,
  isGameId,
  type JsonArray,
  type JsonObject,
  type JsonValue
} from "@arena/wire"

import { MAX_PROXY_JSON_BYTES } from "../../harness/src/gateway/constants.ts"
import { untrustedField } from "../../harness/src/gateway/public.ts"
import { parsePythonJson } from "../../harness/src/gateway/python-json.ts"
import {
  type BinaryArtifact,
  isArchiveBytes,
  makeRunsRepository,
  MANIFEST_FILE,
  O_CLOEXEC,
  O_NOFOLLOW,
  O_RDONLY,
  REPLAY_JSONL_FILE,
  REPLAY_TAIL_BYTES,
  REPORT_FILE,
  type RunsError,
  type RunsRepositoryApi,
  type TerminalArchive
} from "../../harness/src/gateway/services/runs.ts"

import * as Migrate from "../src/migrate.ts"
import * as Pglite from "../src/pglite.ts"
import {
  type Database,
  type GameDocumentRow,
  makeRunsRepositoryPg,
  reconstructManifest,
  reconstructReport
} from "../src/runs-repository-pg.ts"
import { games, runState } from "../src/schema.ts"

// ------------------------------------------------------------------ scratch --

const fixturesRoot = fileURLToPath(
  new URL("../../harness/test/parity/fixtures/runs", import.meta.url)
)

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
const INTERRUPTED_ID = "game_parity_interrupted_03"
const HUSK_ID = "game_parity_lobby_husk_04"
const WRONG_ID = "game_parity_wrong_id_06"

/** A short but structurally real MP4 head, so `videoFile`'s success path exists. */
const VIDEO_BYTES = new Uint8Array([
  0x00, 0x00, 0x00, 0x18, 0x66, 0x74, 0x79, 0x70, 0x69, 0x73, 0x6f, 0x6d, 0x00, 0x00, 0x02, 0x00,
  0x69, 0x73, 0x6f, 0x6d, 0x69, 0x73, 0x6f, 0x32, 0xff, 0xfe, 0x00, 0x5c
])

interface Scratch {
  /** The `mktemp -d` directory everything below lives in, removed in `afterAll`. */
  readonly base: string
  readonly runsRoot: string
}

/**
 * A private copy of the corpus, under the process's own temporary directory.
 *
 * `cp -R` rather than a recursive walk: it preserves the symlinked run, and a
 * hand-rolled copier that quietly dereferenced it would delete the one fixture
 * whose entire purpose is to be a symlink.
 */
const makeScratch = async (): Promise<Scratch> => {
  const base = Bun.spawnSync(["mktemp", "-d", "/tmp/arena-pg-repo-XXXXXX"]).stdout.toString().trim()
  const runsRoot = `${base}/runs`
  const copy = Bun.spawnSync(["cp", "-R", fixturesRoot, runsRoot])
  // The one throw in this file, and deliberate: a corpus that did not copy makes
  // every assertion below meaningless, and no value a fixture builder returns
  // would be read by a later expectation as "the environment broke".
  if (copy.exitCode !== 0) {
    throw new Error(`cp -R failed: ${copy.stderr.toString()}`)
  }
  await Bun.write(`${runsRoot}/${TERMINAL_ID}/game.mp4`, VIDEO_BYTES)
  return { base, runsRoot }
}

// ------------------------------------------------------- the ingest stand-in --

const attempt = <A>(thunk: () => A): Option.Option<A> => Option.getRight(Either.try(thunk))

const UTF8 = new TextDecoder("utf-8", { fatal: true })

const decodeUtf8 = (bytes: Uint8Array): Option.Option<string> =>
  Option.getRight(Either.try(() => UTF8.decode(bytes)))

const parseJson = (text: string): Option.Option<JsonValue> =>
  Option.getRight(decodeJsonValueFromString(text))

// `Array.isArray` does not narrow a `ReadonlyArray` member out of a union in its
// false branch; a declared guard does.
const isJsonArray = (value: JsonValue): value is JsonArray => Array.isArray(value)

const asJsonObject = (value: JsonValue): Option.Option<JsonObject> =>
  value !== null && typeof value === "object" && !isJsonArray(value)
    ? Option.some(value)
    : Option.none()

/** `read(2)` from `position` until `length` bytes are in hand or the file ends. */
const readAt = (fd: number, position: number, length: number): Uint8Array => {
  const buffer = new Uint8Array(length)
  const step = (offset: number): number => {
    if (offset >= length) {
      return offset
    }
    const read = readSync(fd, buffer, offset, length - offset, position + offset)
    return read <= 0 ? offset : step(offset + read)
  }
  return buffer.subarray(0, step(0))
}

/** `_read_archive_json`'s verdict (`:573`), plus the document when there is one. */
interface StoredDocument {
  readonly status: "ok" | "unusable" | "absent"
  /** The **fstat** size that the gate saw; `0` when the open failed. */
  readonly byteSize: number
  readonly document: Option.Option<JsonObject>
}

const ABSENT: StoredDocument = { status: "absent", byteSize: 0, document: Option.none() }

/**
 * The two document gates, exactly as `services/runs.ts#readArchiveJson` applies
 * them: `O_NOFOLLOW` (a symlinked `manifest.json` is *absent*, not followed),
 * then regular / non-empty / at most 8 MiB / strict UTF-8 / a JSON **object**.
 *
 * A document Postgres cannot hold faithfully — a lone surrogate or a `U+0000`
 * anywhere in it — is `unusable` by policy (see {@link storable}), which is a
 * *declared* divergence from the fs backend and is pinned in
 * `hunt-pg-divergence.test.ts`, not hidden here.
 */
const readDocument = (path: string): StoredDocument =>
  Option.match(attempt(() => openSync(path, O_RDONLY | O_NOFOLLOW | O_CLOEXEC)), {
    onNone: () => ABSENT,
    onSome: (fd) => {
      const info = attempt(() => fstatSync(fd))
      const document = Option.flatMap(info, (stat) =>
        !stat.isFile() || stat.size <= 0 || stat.size > MAX_PROXY_JSON_BYTES
          ? Option.none<JsonObject>()
          : Option.flatMap(
            Option.flatMap(attempt(() => readAt(fd, 0, stat.size)), decodeUtf8),
            (text) =>
              Option.filter(Option.flatMap(parseJson(text), asJsonObject), (value) =>
                storable(value))
          ))
      Option.getOrElse(attempt(() => closeSync(fd)), () => undefined)
      return {
        status: Option.isSome(document) ? "ok" : "unusable",
        byteSize: Option.match(info, { onNone: () => 0, onSome: (stat) => stat.size }),
        document
      }
    }
  })

/** A lone surrogate: half a pair, which is valid JSON text and invalid jsonb. */
const LONE_SURROGATE_RE = /[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/

const storableText = (value: string): boolean =>
  !value.includes("\u0000") && !LONE_SURROGATE_RE.test(value)

/**
 * Whether Postgres can hold this document faithfully (measured, §7 of the plan).
 *
 * `jsonb` **errors** on a `U+0000` and on a lone surrogate, in a key or in a
 * value, and `text` errors on the NUL byte. There is no bytes-shaped carrier in
 * the frozen schema, and stripping the offending characters would silently
 * change `resolved_places` and the derivation places digest — so the honest
 * answer is to refuse the document at ingest and answer 503.
 */
const storable = (value: JsonValue): boolean => {
  if (typeof value === "string") {
    return storableText(value)
  }
  if (Array.isArray(value)) {
    return value.every(storable)
  }
  if (value !== null && typeof value === "object") {
    return Object.entries(value).every(([key, member]) => storableText(key) && storable(member))
  }
  return true
}

const INT32_MIN = -2147483648
const INT32_MAX = 2147483647

const int32Column = (value: unknown): number | null =>
  typeof value === "number" && Number.isInteger(value) && value >= INT32_MIN && value <= INT32_MAX
    ? value
    : null

/** `float8` holds every double except `-0`, which the parameter binding flattens. */
const floatColumn = (value: unknown): number | null =>
  typeof value === "number" && !Object.is(value, -0) ? value : null

const boolColumn = (value: unknown): boolean | null =>
  typeof value === "boolean" ? value : null

/** The seven spellings `games.state` accepts, `'invalid'` among them. */
type RunStateValue = (typeof runState.enumValues)[number]

const RUN_STATES: ReadonlySet<string> = new Set<string>(runState.enumValues)

const isRunState = (value: string): value is RunStateValue => RUN_STATES.has(value)

/** Every manifest key with a typed column; everything else is `extras.manifest`. */
const COLUMN_KEYS: ReadonlySet<string> = new Set([
  "state",
  "schema_version",
  "created_at",
  "started_at",
  "finished_at",
  "current_turn",
  "benchmark_valid",
  "config"
])

/**
 * What the lifted config columns are written with.
 *
 * Not the config's values: they are a *query* projection that has never been
 * through `publicText`, and the only way to prove no response body reads one is
 * to make reading one visible. A pg answer that ever contains this string is a
 * defect the diff spells out.
 */
const POISON = "!! lifted column, never served !!"

/** The `games` row one run becomes, minus the bookkeeping columns. */
interface StoredGame {
  readonly state: RunStateValue
  readonly schemaVersion: number | null
  readonly createdAt: number | null
  readonly startedAt: number | null
  readonly finishedAt: number | null
  readonly currentTurn: number | null
  readonly benchmarkValid: boolean | null
  readonly config: JsonValue | null
  readonly extrasManifest: JsonObject
}

/**
 * §1.2's partition: every key exactly once, in its column or in
 * `extras.manifest`, and a key whose column cannot hold it losslessly is demoted
 * whole and verbatim.
 *
 * `state` is the one key stored twice, and deliberately: the column is `NOT
 * NULL` over an enum with an `'invalid'` sentinel, so it cannot distinguish *no
 * `state` key* from a manifest that literally said `"invalid"` — and
 * `manifestState`'s `untrustedFieldOr(manifest, 'state', 'status')` reads those
 * two differently. The verbatim copy is what
 * {@link reconstructManifest} prefers; the column stays for domain queries.
 */
const partitionManifest = (document: JsonObject): StoredGame => {
  const has = (key: string): boolean => Object.hasOwn(document, key)
  // A decoded JSON object holds no `undefined`, so the index signature's
  // `undefined` is exactly "the key is absent" — and `has` is what distinguishes
  // it from a present `null` everywhere it matters.
  const at = (key: string): JsonValue => document[key] ?? null
  const rawState = at("state")
  const columns: ReadonlyArray<readonly [string, JsonValue | null]> = [
    ["schema_version", int32Column(at("schema_version"))],
    ["created_at", floatColumn(at("created_at"))],
    ["started_at", floatColumn(at("started_at"))],
    ["finished_at", floatColumn(at("finished_at"))],
    ["current_turn", int32Column(at("current_turn"))],
    ["benchmark_valid", boolColumn(at("benchmark_valid"))],
    // jsonb holds any JSON value — object, array, string, number, bool — so the
    // only demotion left for `config` is an explicit `null`, which a column
    // cannot tell from an absent key. (A document jsonb could not hold at all
    // never reaches here: it was `unusable`.)
    ["config", has("config") && at("config") !== null ? at("config") : null]
  ]
  const column = (key: string): JsonValue | null =>
    columns.find(([name]) => name === key)?.[1] ?? null
  const numberColumn = (key: string): number | null => {
    const value = column(key)
    return typeof value === "number" ? value : null
  }
  return {
    state: typeof rawState === "string" && isRunState(rawState) ? rawState : "invalid",
    schemaVersion: numberColumn("schema_version"),
    createdAt: numberColumn("created_at"),
    startedAt: numberColumn("started_at"),
    finishedAt: numberColumn("finished_at"),
    currentTurn: numberColumn("current_turn"),
    benchmarkValid: column("benchmark_valid") === null
      ? null
      : column("benchmark_valid") === true,
    config: column("config"),
    extrasManifest: Object.fromEntries<JsonValue>([
      ...Object.entries(document).filter(([key]) => !COLUMN_KEYS.has(key)),
      ...(has("state") ? [["state", rawState] as const] : []),
      ...columns
        .filter(([key, value]) => has(key) && value === null)
        .map(([key]) => [key, at(key)] as const)
    ])
  }
}

/** `bytes.splitlines()` — `\n`, `\r` and `\r\n`, over the lossless Latin-1 map. */
const splitLines = (bytes: Uint8Array): ReadonlyArray<string> =>
  Array.from(bytes, (byte) => String.fromCharCode(byte)).join("").split(/\r\n|\n|\r/)

const BLANK_LINE_RE = /^[ \t\v\f]*$/

const latin1Bytes = (line: string): Uint8Array =>
  Uint8Array.from(line, (character) => character.charCodeAt(0))

/**
 * `_last_replay_turn` (`:1178`) — the 64 KiB tail, backwards, stopping at the
 * **first line that parses**. `parsePythonJson`, so `{"turn": 2.0}` is refused
 * for being a float exactly as CPython's `isinstance(turn, int)` refuses it.
 *
 * Transcribed rather than delegated to the fs repository on purpose: reading the
 * column's value *through the oracle* would make `lastReplayTurn`'s differential
 * a tautology.
 */
const tailTurn = (path: string): Option.Option<bigint> =>
  Option.match(attempt(() => openSync(path, O_RDONLY | O_CLOEXEC)), {
    onNone: () => Option.none<bigint>(),
    onSome: (fd) => {
      const size = Option.match(attempt(() => fstatSync(fd).size), {
        onNone: () => 0,
        onSome: (value) => value
      })
      const start = Math.max(0, size - REPLAY_TAIL_BYTES)
      const tail = size <= 0
        ? new Uint8Array(0)
        : Option.getOrElse(attempt(() => readAt(fd, start, size - start)), () => new Uint8Array(0))
      Option.getOrElse(attempt(() => closeSync(fd)), () => undefined)
      const parsed = splitLines(tail)
        .filter((line) => !BLANK_LINE_RE.test(line))
        .toReversed()
        .map((line) =>
          Option.flatMap(
            decodeUtf8(latin1Bytes(line)),
            (text) => Option.getRight(parsePythonJson(text))
          )
        )
        .find(Option.isSome)
      if (parsed === undefined) {
        return Option.none<bigint>()
      }
      const turn = untrustedField(parsed.value, "turn")
      return typeof turn === "bigint" && turn > 0n ? Option.some(turn) : Option.none<bigint>()
    }
  })

/** `Path.is_symlink()` / `Path.is_dir()` — `False` on any `OSError`. */
const isSymlink = (path: string): boolean =>
  Option.match(attempt(() => lstatSync(path)), {
    onNone: () => false,
    onSome: (info) => info.isSymbolicLink()
  })

const isDirectory = (path: string): boolean =>
  Option.match(attempt(() => statSync(path)), {
    onNone: () => false,
    onSome: (info) => info.isDirectory()
  })

/**
 * The run directories ingest writes a row for.
 *
 * `GAME_ID_RE`, not a symlink, and a directory — the same three refusals
 * `_read_manifest` performs per request. A run that fails one gets **no row**,
 * which is how the pg backend answers the 404 the fs backend answers by
 * re-running the check. `game_parity_symlink_07` is the fixture that proves it.
 */
const runDirectories = (runsRoot: string): ReadonlyArray<string> =>
  Option.getOrElse(attempt(() => readdirSync(runsRoot)), (): ReadonlyArray<string> => [])
    .filter((name) =>
      isGameId(name) && !isSymlink(join(runsRoot, name)) && isDirectory(join(runsRoot, name))
    )

/**
 * One run, stored the way §1.2/§1.5 say to store it.
 *
 * `content_hash` is bookkeeping the repository never reads, so it is the id's
 * digest and nothing more; the real ingest's length-prefixed listing belongs to
 * the sweep's idempotence proof, not to this file's subject.
 */
const storeRun = (
  db: Database,
  runsRoot: string,
  gameId: string
): Effect.Effect<unknown, never> => {
  const runRoot = join(runsRoot, gameId)
  const manifest = readDocument(join(runRoot, MANIFEST_FILE))
  const report = readDocument(join(runRoot, REPORT_FILE))
  const turn = tailTurn(join(runRoot, REPLAY_JSONL_FILE))
  const stored = Option.map(manifest.document, partitionManifest)
  const fits = Option.exists(turn, (value) =>
    value >= BigInt(INT32_MIN) && value <= BigInt(INT32_MAX))
  const derived: JsonObject = Option.match(turn, {
    onNone: (): JsonObject => ({}),
    onSome: (value): JsonObject => fits ? {} : { last_replay_turn: value.toString() }
  })
  return Effect.orDie(db.insert(games).values({
    gameId,
    state: Option.match(stored, { onNone: () => "invalid" as const, onSome: (row) => row.state }),
    schemaVersion: Option.getOrNull(Option.map(stored, (row) => row.schemaVersion)),
    createdAt: Option.getOrNull(Option.map(stored, (row) => row.createdAt)),
    startedAt: Option.getOrNull(Option.map(stored, (row) => row.startedAt)),
    finishedAt: Option.getOrNull(Option.map(stored, (row) => row.finishedAt)),
    currentTurn: Option.getOrNull(Option.map(stored, (row) => row.currentTurn)),
    lastReplayTurn: fits ? Number(Option.getOrElse(turn, () => 0n)) : null,
    benchmarkValid: Option.getOrNull(Option.map(stored, (row) => row.benchmarkValid)),
    // The six write-only projections, deliberately wrong. See POISON.
    name: POISON,
    ruleset: POISON,
    mode: POISON,
    timingMode: POISON,
    maxTurns: 424242,
    objective: POISON,
    config: Option.getOrNull(Option.map(stored, (row) => row.config)),
    manifestStatus: manifest.status,
    manifestByteSize: manifest.byteSize,
    reportStatus: report.status,
    reportByteSize: report.byteSize,
    contentHash: new Uint8Array(createHash("sha256").update(gameId).digest()),
    extras: {
      ...Option.match(stored, {
        onNone: (): JsonObject => ({}),
        onSome: (row): JsonObject => ({ manifest: row.extrasManifest })
      }),
      ...Option.match(report.document, {
        onNone: (): JsonObject => ({}),
        onSome: (document): JsonObject => ({ report: document })
      }),
      derived
    }
  }))
}

const ingestCorpus = (db: Database, runsRoot: string): Effect.Effect<number, never> =>
  Effect.map(
    Effect.forEach(runDirectories(runsRoot), (gameId) => storeRun(db, runsRoot, gameId)),
    (rows) => rows.length
  )

// -------------------------------------------------------------------- layers --

const testLayer = Layer.mergeAll(
  Layer.provideMerge(PgDrizzle.layer, Pglite.layer(Pglite.memory)),
  // `Migrate.run` reads `drizzle/` through `@effect/platform`'s `FileSystem`.
  BunContext.layer
)

type TestContext = Layer.Layer.Success<typeof testLayer>

/**
 * One PGlite instance for the whole file.
 *
 * A `ManagedRuntime` rather than a per-test `Effect.provide`: the layer is
 * memoized, so every test sees the *same* database and the corpus is ingested
 * once.
 */
const runtime = ManagedRuntime.make(testLayer)

const run = <A, E>(effect: Effect.Effect<A, E, TestContext>): Promise<A> =>
  runtime.runPromise(effect)

/** Both repositories over one scratch corpus. */
interface Fixture {
  readonly fs: RunsRepositoryApi
  readonly pg: RunsRepositoryApi
  readonly scratch: Scratch
  readonly db: Database
  readonly stored: number
}

const buildFixture = (scratch: Scratch): Effect.Effect<Fixture, never, TestContext> =>
  Effect.orDie(Effect.gen(function*() {
    yield* Migrate.run
    const db = yield* PgDrizzle.PgDrizzle
    const stored = yield* ingestCorpus(db, scratch.runsRoot)
    return {
      fs: makeRunsRepository(scratch.runsRoot),
      pg: makeRunsRepositoryPg(db, scratch.runsRoot),
      scratch,
      db,
      stored
    }
  }))

/**
 * The corpus, ingested once, and then **held still**.
 *
 * §4.4: the manifest comes from the database while the binaries and
 * `victory.json` come from disk, so a run whose directory changes after ingest
 * can legitimately answer `200 /status` and `404 /frames`. Every test below
 * reads; none writes into `runs_root` after this point.
 */
const fixture: Promise<Fixture> = makeScratch().then((scratch) => run(buildFixture(scratch)))

const withFixture = <A, E>(
  body: (fixture: Fixture) => Effect.Effect<A, E, TestContext>
): Promise<A> => fixture.then((value) => run(body(value)))

afterAll(async () => {
  await runtime.dispose()
  const built = await fixture.then(Option.some, () => Option.none<Fixture>())
  if (Option.isSome(built) && built.value.scratch.base.startsWith("/tmp/arena-pg-repo-")) {
    rmSync(built.value.scratch.base, { recursive: true, force: true })
  }
})

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
 * repositories return are not themselves declared as `CanonValue`, so the
 * writer's parameter needs the assertion. It is checked at run time by
 * `canonicalText`, which refuses anything it cannot spell — an `undefined`, a
 * function, a `NaN` — with an error this file renders into the comparison rather
 * than swallowing.
 */
const asCanon = (value: unknown): CanonValue => value as CanonValue

const bytesOf = (value: Uint8Array): ReadonlyArray<number> => Array.from(value)

const readBytes = (artifact: BinaryArtifact): ReadonlyArray<number> =>
  isArchiveBytes(artifact) ? bytesOf(artifact.bytes) : bytesOf(readFileSync(artifact))

const frameIndex = (name: string): Option.Option<FrameIndex> =>
  Option.getRight(Gateway.decodeArchivePngName(name))

/** A `bigint` anywhere in a reconstructed document is R4, the int/float trap. */
const hasBigInt = (value: unknown): boolean =>
  typeof value === "bigint" ||
  (Array.isArray(value)
    ? value.some(hasBigInt)
    : typeof value === "object" && value !== null
    ? Object.values(value).some(hasBigInt)
    : false)

/**
 * The document on disk, as the *filesystem backend* sees it — the §0 oracle's
 * right side.
 *
 * `decodeJsonValueFromString` rather than a bare `JSON.parse`: it is literally
 * what `readManifest` runs (`Schema.JsonNumber`, so every number is a JS
 * `number`), and a reconstruction that matched `JSON.parse` but not the reader
 * would be matching the wrong oracle.
 */
const documentOnDisk = (runsRoot: string, gameId: string, file: string): JsonObject | null =>
  Option.getOrNull(
    Option.flatMap(parseJson(readFileSync(join(runsRoot, gameId, file), "utf8")), asJsonObject)
  )

/**
 * One stored row, as {@link GameDocumentRow}.
 *
 * `Effect.orDie` on the absence: a test that names a run the corpus does not
 * hold is a defect in the test, not a failure of the code under test.
 */
const rowOf = (db: Database, gameId: string): Effect.Effect<GameDocumentRow, never> =>
  Effect.orDie(Effect.flatMap(
    db.select().from(games).where(eq(games.gameId, gameId)),
    (rows) =>
      Option.match(Option.fromNullable(rows[0]), {
        onNone: () => Effect.die(`no games row for ${gameId}`),
        onSome: (row): Effect.Effect<GameDocumentRow> => Effect.succeed(row)
      })
  ))

// ------------------------------------------------------------------- ingest --

describe("the corpus, as rows", () => {
  it("stores every run the fs backend can see, and skips the symlinked one", () =>
    withFixture((f) =>
      Effect.map(f.db.select({ gameId: games.gameId }).from(games), (rows) => {
        expect(rows.map((row) => row.gameId).toSorted()).toEqual([
          "game_parity_interrupted_03",
          "game_parity_lobby_husk_04",
          "game_parity_malformed_05",
          "game_parity_terminal_nowin_02",
          "game_parity_terminal_valid_01",
          "game_parity_torn_tail_08",
          "game_parity_wrong_id_06"
        ])
        expect(f.stored).toBe(7)
      })
    ))

  it("records the malformed manifest as unusable, with the fstat size that failed", () =>
    withFixture((f) =>
      Effect.map(
        f.db.select().from(games),
        (rows) => {
          const malformed = rows.find((row) => row.gameId === "game_parity_malformed_05")
          expect(malformed?.manifestStatus).toBe("unusable")
          expect(malformed?.manifestByteSize).toBe(
            statSync(join(f.scratch.runsRoot, "game_parity_malformed_05", MANIFEST_FILE)).size
          )
          // `unusable` carries no reconstructable document at all.
          expect(malformed?.extras).toEqual({ derived: {} })
        }
      )
    ))
})

// ------------------------------------------------- the §0 reconstruction oracle --

describe("reconstruction is deep-equal to the document on disk", () => {
  it.each(["game_parity_terminal_valid_01", "game_parity_terminal_nowin_02", INTERRUPTED_ID, HUSK_ID, WRONG_ID, "game_parity_torn_tail_08"])(
    "%s: manifest.json survives the round trip",
    (gameId) =>
      withFixture((f) =>
        Effect.map(rowOf(f.db, gameId), (row) => {
          const rebuilt = reconstructManifest(row)
          expect(Option.isSome(rebuilt)).toBe(true)
          expect(Option.getOrNull(rebuilt)).toEqual(
            documentOnDisk(f.scratch.runsRoot, gameId, MANIFEST_FILE)
          )
          expect(hasBigInt(Option.getOrNull(rebuilt))).toBe(false)
        })
      )
  )

  it.each([TERMINAL_ID, NOWIN_ID, WRONG_ID])(
    "%s: report.json survives the round trip",
    (gameId) =>
      withFixture((f) =>
        Effect.map(rowOf(f.db, gameId), (row) => {
          expect(Option.getOrNull(reconstructReport(row))).toEqual(
            documentOnDisk(f.scratch.runsRoot, gameId, REPORT_FILE)
          )
        })
      )
  )
})

// ------------------------------------------------------------- readManifest --

describe("readManifest", () => {
  it.each(EVERY_ID)("answers identically for %s", (gameId) =>
    withFixture((f) =>
      Effect.gen(function*() {
        const fs = yield* settle(f.fs.readManifest(gameId))
        const pg = yield* settle(f.pg.readManifest(gameId))
        expect(pg).toEqual(fs)
      })
    ))

  it("keeps the three answers distinct: 404 game, 503 unusable, 404 wrong id", () =>
    withFixture((f) =>
      Effect.gen(function*() {
        expect(yield* settle(f.pg.readManifest("game_parity_symlink_07"))).toEqual(
          Either.left("NotFound:gameNotFound")
        )
        expect(yield* settle(f.pg.readManifest("game_parity_malformed_05"))).toEqual(
          Either.left("ArchiveUnavailable:manifestUnavailable")
        )
        expect(yield* settle(f.pg.readManifest(WRONG_ID))).toEqual(
          Either.left("NotFound:gameNotFound")
        )
      })
    ))

  it("never publishes a lifted column", () =>
    withFixture((f) =>
      Effect.map(f.pg.readManifest(TERMINAL_ID), (manifest) => {
        expect(canonical(asCanon(manifest))).not.toContain(POISON)
      })
    ))
})

describe("decodeManifest", () => {
  it.each(EVERY_ID)("answers identically for %s", (gameId) =>
    withFixture((f) =>
      Effect.gen(function*() {
        const fs = yield* Effect.either(f.fs.decodeManifest(gameId))
        const pg = yield* Effect.either(f.pg.decodeManifest(gameId))
        expect(Either.map(pg, (value) => canonical(asCanon(value)))).toEqual(
          Either.map(fs, (value) => canonical(asCanon(value)))
        )
        expect(Either.mapLeft(pg, (error) => error._tag)).toEqual(
          Either.mapLeft(fs, (error) => error._tag)
        )
      })
    ))
})

// ----------------------------------------------------------- terminalArchive --

describe("terminalArchive", () => {
  it.each(EVERY_ID)("answers identically for %s, runRoot included", (gameId) =>
    withFixture((f) =>
      Effect.gen(function*() {
        const fs = yield* settle(f.fs.terminalArchive(gameId))
        const pg = yield* settle(f.pg.terminalArchive(gameId))
        expect(pg).toEqual(fs)
        expect(Either.map(pg, (archive: TerminalArchive) => canonical(asCanon(archive.outcome))))
          .toEqual(
            Either.map(fs, (archive: TerminalArchive) => canonical(asCanon(archive.outcome)))
          )
      })
    ))

  it("reads victory.json from the run directory, spelling included", () =>
    withFixture((f) =>
      Effect.gen(function*() {
        const archive = yield* f.pg.terminalArchive(TERMINAL_ID)
        // The *resolved* root — `/tmp` is a symlink to `/private/tmp` on darwin,
        // and both backends resolve it once at construction.
        expect(archive.runRoot).toBe(join(f.fs.runsRoot, TERMINAL_ID))
        expect(canonical(asCanon(archive.outcome.victory))).toBe(
          canonical(asCanon((yield* f.fs.terminalArchive(TERMINAL_ID)).outcome.victory))
        )
        // The nowin fixture has no victory.json at all.
        const nowin = yield* f.pg.terminalArchive(NOWIN_ID)
        expect(nowin.outcome.victory).toBeNull()
      })
    ))

  it("answers terminalArchiveNotFound for a live run, before it looks for a report", () =>
    withFixture((f) =>
      Effect.gen(function*() {
        expect(yield* settle(f.pg.terminalArchive(INTERRUPTED_ID))).toEqual(
          Either.left("NotFound:terminalArchiveNotFound")
        )
        expect(yield* settle(f.fs.terminalArchive(INTERRUPTED_ID))).toEqual(
          Either.left("NotFound:terminalArchiveNotFound")
        )
      })
    ))
})

// ------------------------------------------------------------ lastReplayTurn --

describe("lastReplayTurn", () => {
  /** Every id but the two the fs reader answers from outside the archive. */
  const UNGUARDED = new Set(["game_parity_symlink_07", "GAME_PARITY_TERMINAL_VALID_01"])
  const TAIL_IDS = EVERY_ID.filter((gameId) => !UNGUARDED.has(gameId))

  it.each(TAIL_IDS)("answers identically for %s", (gameId) =>
    withFixture((f) =>
      Effect.gen(function*() {
        const fs = yield* f.fs.lastReplayTurn(gameId)
        const pg = yield* f.pg.lastReplayTurn(gameId)
        expect(Option.getOrNull(pg)).toEqual(Option.getOrNull(fs))
      })
    ))

  /**
   * The two divergences, and both are the fs reader's own inconsistency.
   *
   * `_last_replay_turn` opens `<runs_root>/<id>/replay.jsonl` with **no**
   * `O_NOFOLLOW`, no `GAME_ID_RE` check and no run-directory check
   * (`services/runs.ts` says so in as many words). So it reads *through* the
   * symlinked run the rest of the gateway refuses, and — on a case-insensitive
   * filesystem — through an id no `GAME_ID_RE` accepts. The pg backend has no
   * row for either and answers nothing.
   *
   * Both are unreachable: the only caller is `diskRowsWithInterrupted`, whose
   * candidates come from `interruptedCandidates(rows, …)` — rows of the index,
   * which contains neither id in either backend. Pinned rather than waived, so
   * that a future caller of `lastReplayTurn` has to come past this comment.
   */
  it.each([...UNGUARDED])(
    "diverges for %s only where the fs reader leaves the archive, and unreachably",
    (gameId) =>
      withFixture((f) =>
        Effect.gen(function*() {
          expect(Option.getOrNull(yield* f.fs.lastReplayTurn(gameId))).toBe(3n)
          expect(Option.isNone(yield* f.pg.lastReplayTurn(gameId))).toBe(true)
          // …and it cannot reach a payload: the id is in neither index.
          const pgRows = yield* f.pg.diskRowsWithInterrupted(new Set())
          const fsRows = yield* f.fs.diskRowsWithInterrupted(new Set())
          expect(pgRows.map((row) => row.game_id)).not.toContain(gameId)
          expect(fsRows.map((row) => row.game_id)).not.toContain(gameId)
        })
      )
  )

  it("reads the column, not the file: the tail window was read once, at ingest", () =>
    withFixture((f) =>
      Effect.map(f.pg.lastReplayTurn(INTERRUPTED_ID), (turn) => {
        // §4.4's skew window is the design, and this is the half of it the
        // database owns: the answer is a column, not a 64 KiB read.
        expect(Option.getOrNull(turn)).toBe(4n)
      })
    ))

  it("hides a lobby husk exactly as the fs backend does", () =>
    withFixture((f) =>
      Effect.gen(function*() {
        expect(Option.isNone(yield* f.pg.lastReplayTurn(HUSK_ID))).toBe(true)
        expect(Option.isNone(yield* f.fs.lastReplayTurn(HUSK_ID))).toBe(true)
      })
    ))
})

// ------------------------------------------------------------ diskGamesIndex --

describe("diskGamesIndex", () => {
  it("is byte-identical to the fs index", () =>
    withFixture((f) =>
      Effect.gen(function*() {
        const fs = yield* f.fs.diskGamesIndex()
        const pg = yield* f.pg.diskGamesIndex()
        expect(canonical(asCanon(pg))).toBe(canonical(asCanon(fs)))
        expect(canonical(asCanon(pg))).not.toContain(POISON)
      })
    ))

  it("is byte-identical with terminalOnly", () =>
    withFixture((f) =>
      Effect.gen(function*() {
        const fs = yield* f.fs.diskGamesIndex({ terminalOnly: true })
        const pg = yield* f.pg.diskGamesIndex({ terminalOnly: true })
        expect(canonical(asCanon(pg))).toBe(canonical(asCanon(fs)))
      })
    ))
})

describe("diskRowsWithInterrupted", () => {
  const LIVE_SETS: ReadonlyArray<ReadonlyArray<string>> = [
    [],
    [INTERRUPTED_ID],
    [TERMINAL_ID, HUSK_ID],
    [...FIXTURE_IDS]
  ]

  it.each(LIVE_SETS.map((ids) => [ids.join(",") || "(none)", ids] as const))(
    "relabels identically with live ids %s",
    (_label, ids) =>
      withFixture((f) =>
        Effect.gen(function*() {
          const live = new Set(ids)
          const fs = yield* f.fs.diskRowsWithInterrupted(live)
          const pg = yield* f.pg.diskRowsWithInterrupted(live)
          expect(canonical(asCanon(pg))).toBe(canonical(asCanon(fs)))
        })
      )
  )
})

// -------------------------------------------------------- frames and video --

describe("frameFile and videoFile", () => {
  const FRAME_NAMES = ["000000.png", "000752.png"]

  it.each(FRAME_NAMES)("answers the same artifact for %s", (name) =>
    withFixture((f) =>
      Effect.gen(function*() {
        const fsArchive = yield* f.fs.terminalArchive(TERMINAL_ID)
        const pgArchive = yield* f.pg.terminalArchive(TERMINAL_ID)
        const fsFrame = yield* settle(f.fs.frameFile(fsArchive, frameIndex(name)))
        const pgFrame = yield* settle(f.pg.frameFile(pgArchive, frameIndex(name)))
        expect(pgFrame).toEqual(fsFrame)
        expect(Either.map(pgFrame, readBytes)).toEqual(Either.map(fsFrame, readBytes))
      })
    ))

  it("answers the same latest frame, and the same 404 for one that is not there", () =>
    withFixture((f) =>
      Effect.gen(function*() {
        const fsArchive = yield* f.fs.terminalArchive(TERMINAL_ID)
        const pgArchive = yield* f.pg.terminalArchive(TERMINAL_ID)
        expect(yield* settle(f.pg.frameFile(pgArchive, Option.none()))).toEqual(
          yield* settle(f.fs.frameFile(fsArchive, Option.none()))
        )
        expect(yield* settle(f.pg.frameFile(pgArchive, frameIndex("000999.png")))).toEqual(
          yield* settle(f.fs.frameFile(fsArchive, frameIndex("000999.png")))
        )
      })
    ))

  it("answers the same video, and the same 404 when there is none", () =>
    withFixture((f) =>
      Effect.gen(function*() {
        const fsArchive = yield* f.fs.terminalArchive(TERMINAL_ID)
        const pgArchive = yield* f.pg.terminalArchive(TERMINAL_ID)
        const fsVideo = yield* settle(f.fs.videoFile(fsArchive))
        const pgVideo = yield* settle(f.pg.videoFile(pgArchive))
        expect(pgVideo).toEqual(fsVideo)
        expect(Either.map(pgVideo, readBytes)).toEqual(Either.right(bytesOf(VIDEO_BYTES)))

        const fsNowin = yield* f.fs.terminalArchive(NOWIN_ID)
        const pgNowin = yield* f.pg.terminalArchive(NOWIN_ID)
        expect(yield* settle(f.pg.videoFile(pgNowin))).toEqual(
          yield* settle(f.fs.videoFile(fsNowin))
        )
      })
    ))
})

describe("runsRoot", () => {
  it("is the same resolved string in both backends", () =>
    withFixture((f) => Effect.sync(() => expect(f.pg.runsRoot).toBe(f.fs.runsRoot))))
})

// ------------------------------------- the storage rules the repository needs --

describe("the storage rules the repository depends on", () => {
  it("stores an explicit null in extras, never as a NULL column", () =>
    withFixture((f) =>
      Effect.map(rowOf(f.db, HUSK_ID), (row) => {
        // `current_turn: null` in the fixture: a NULL column *and* a demoted key,
        // which is what makes absent-vs-present-null decidable (R5).
        expect(row.currentTurn).toBeNull()
        const extras = row.extras
        expect(extras).toBeTruthy()
        const demoted = Option.getOrNull(reconstructManifest(row))
        expect(demoted !== null && Object.hasOwn(demoted, "current_turn")).toBe(true)
        expect(demoted?.["current_turn"]).toBeNull()
      })
    ))

  it("keeps the manifest's game_id claim, and never reads the primary key", () =>
    withFixture((f) =>
      Effect.map(rowOf(f.db, WRONG_ID), (row) => {
        expect(Option.getOrNull(reconstructManifest(row))?.["game_id"]).toBe(
          "game_parity_wrong_id_06_other"
        )
      })
    ))

  it("records state verbatim as well as in the column", () =>
    withFixture((f) =>
      Effect.map(rowOf(f.db, TERMINAL_ID), (row) => {
        expect(row.state).toBe("completed")
        expect(Option.getOrNull(reconstructManifest(row))?.["state"]).toBe("completed")
      })
    ))
})
