/**
 * `RunsRepository` — every read the replay gateway performs against `runs_root`.
 *
 * This is the *I/O half* of the disk view.  The shaping half is
 * `../archive.ts` and `../public.ts`, which are deliberately free of the
 * filesystem: they take a manifest, a report and a victory record and return
 * payloads.  This module is what opens the files, refuses the symlinks,
 * enumerates the frames, reads the tail of `replay.jsonl`, and swallows
 * exactly the failures Python swallows.  Bare `:NNN` citations are
 * `agent_eval/replay_gateway.py`.
 *
 * ```
 * _read_manifest              :557   readManifest        (+ ../archive.ts)
 * _read_archive_json          :573   readArchiveJson
 * _archive_victory            :681   readVictoryRecord   (+ archiveVictory)
 * _terminal_archive           :773   terminalArchive     (+ terminalArchiveView)
 * _safe_archive_directory     :871   safeArchiveDirectory
 * _archive_regular_files      :885   archiveRegularFiles
 * _archive_frame_path        :1007   frameFile           (+ selectFramePng)
 * _archive_video_path        :1022   videoFile
 * _last_replay_turn          :1178   lastReplayTurn
 * _disk_games_index          :1242   diskGamesIndex      (+ diskGameRow)
 * _disk_rows_with_interrupted:1582   diskRowsWithInterrupted
 * ```
 *
 * ## Three rules this module exists to keep
 *
 * **1. `diskGamesIndex` cannot fail.**  Python wraps every per-run read in
 * `except GatewayProblem: continue` (`:1258`), so a half-written manifest, a
 * symlinked run directory, or a `report.json` that is a directory removes one
 * row and nothing else.  Its error channel here is `never` — the only way to
 * make that structural rather than remembered.
 *
 * **2. Everything else fails as a `../errors.ts` value.**  Nothing here builds
 * a response: a route `catchTag`s `NotFound` / `ArchiveUnavailable` and
 * `../http/respond.ts` renders them, once.
 *
 * **3. Untrusted JSON stays untrusted.**  `readManifest` hands back the
 * document, not a decoded `Gateway.Manifest`, because the projections read a
 * *dict* through `publicText` / `publicInt` / `publicNumber` and coerce
 * whatever they find.  Decoding with the strict schema first would drop rows
 * Python serves — a manifest missing `commands_file` still produces an index
 * row.  {@link RunsRepositoryApi.decodeManifest} is there for callers that do
 * want the schema.
 *
 * ## Two deliberate divergences, both unreachable from a real run
 *
 * - `_last_replay_turn` accepts a *Python* `int`, so `{"turn": 5.0}` is
 *   rejected (a float) while `{"turn": true}` is accepted (`bool` subclasses
 *   `int`, and `True > 0`).  The float half is reproduced exactly — the tail is
 *   read with `../python-json.ts`, which keeps `5` and `5.0` apart — and the
 *   bool half is **not**: Python would then publish `"current_turn": true`,
 *   and `Gateway.GameRow` (whose schema this port may not edit) types that
 *   field as an integer, so `true` is refused here and the row is dropped.
 *   `save_replay` writes turns with `int`, so neither spelling occurs.
 * - `json.load` accepts `NaN` / `Infinity` literals and `JSON.parse` does not.
 *   For `manifest.json` and `report.json` the outcomes agree anyway — Python
 *   parses such a document and then fails the `isinstance(value, dict)` check
 *   with the same 503 this port raises on the parse.
 *
 * @module
 */
import { Array as Arr, Context, Data, Effect, Either, Layer, Option, type Scope } from 'effect';
import {
  closeSync,
  fstatSync,
  lstatSync,
  openSync,
  readdirSync,
  readSync,
  realpathSync,
  statSync,
} from 'node:fs';
import { dirname, join, resolve as resolvePath } from 'node:path';
import {
  compareCodePoints,
  decodeJsonValueFromString,
  Gateway,
  isGameId,
  isTerminalRunState,
  type FrameIndex,
  type JsonArray,
  type JsonObject,
  type JsonValue,
  type WireDecodeError,
} from '@arena/wire';
import {
  type ArchivePng,
  type ArchiveView,
  archiveVictory,
  diskGameRow,
  diskRowsWithInterrupted as relabelDiskRows,
  gamesIndex,
  interruptedCandidates,
  manifestState,
  selectFramePng,
  terminalArchiveView,
} from '../archive.ts';
import { MAX_PROXY_JSON_BYTES } from '../constants.ts';
import {
  ArchiveUnavailable,
  type ArchiveUnavailableProblem,
  NOT_FOUND_PROBLEMS,
  NotFound,
  type NotFoundProblem,
} from '../errors.ts';
import { type Canonical, type Untrusted, untrustedField } from '../public.ts';
import { parsePythonJson } from '../python-json.ts';

/** What a read of `runs_root` can fail with: a 404 or a 503, never anything else. */
export type RunsError = ArchiveUnavailable | NotFound;

const NOT_FOUND_PROBLEM_SET: ReadonlySet<string> = new Set<string>(NOT_FOUND_PROBLEMS);

const isNotFoundProblem = (problem: string): problem is NotFoundProblem =>
  NOT_FOUND_PROBLEM_SET.has(problem);

/**
 * A problem name from `../archive.ts` as the 404 it is.
 *
 * `terminalArchiveView` and `selectFramePng` answer with a
 * `Gateway.GatewayProblemName`, and every name either of them can return is a
 * {@link NotFoundProblem}.  The fallback is unreachable and still the right
 * answer if it ever is not: `notFound` is the same 404.
 */
const asNotFound = (problem: Gateway.GatewayProblemName): NotFound =>
  new NotFound({ problem: isNotFoundProblem(problem) ? problem : 'notFound' });

// ---------------------------------------------------------------------------
// darwin open(2) flags — spike law
// ---------------------------------------------------------------------------

/** `O_RDONLY`. */
export const O_RDONLY = 0x0000_0000;

/**
 * darwin `O_NOFOLLOW`.
 *
 * Spelled out for the same reason as {@link O_CLOEXEC}: a constant that reads
 * back `undefined` contributes `0` to a flag word instead of failing, and the
 * symlink refusal (`test_replay_gateway.py:1309` — a `watch_frames/000000.png`
 * replaced by a link to `auth.json`) would silently stop happening.  Linux
 * spells this `0o400000`; this port targets darwin.
 */
export const O_NOFOLLOW = 0x0000_0100;

/** darwin `O_CLOEXEC` — absent from Bun's `node:fs` constants (spike law). */
export const O_CLOEXEC = 0x0100_0000;

/** The tail of `replay.jsonl` `_last_replay_turn` reads (`:1189`). */
export const REPLAY_TAIL_BYTES = 65536;

/** `manifest.json`, relative to a run root. */
export const MANIFEST_FILE = 'manifest.json';

/** `report.json`, relative to a run root. */
export const REPORT_FILE = 'report.json';

/** `replay.jsonl`, relative to a run root. */
export const REPLAY_JSONL_FILE = 'replay.jsonl';

/** `victory.json`, relative to a run root (`:691`). */
export const VICTORY_FILE = 'victory.json';

/** The two `_read_archive_json` labels, and the pair of failures each names. */
const ARCHIVE_JSON_PROBLEMS: {
  readonly [L in Gateway.ArchiveJsonLabel]: {
    readonly missing: NotFoundProblem;
    readonly unusable: ArchiveUnavailableProblem;
  };
} = {
  'game manifest': { missing: 'manifestNotFound', unusable: 'manifestUnavailable' },
  'game report': { missing: 'reportNotFound', unusable: 'reportUnavailable' },
};

// ---------------------------------------------------------------------------
// Filesystem interop — the only place this module reaches for `node:fs`
//
// Not `@effect/platform`'s `FileSystem`: this module needs `open(2)` with a
// raw flag word (`O_NOFOLLOW`, `O_CLOEXEC`), `fstat` on the descriptor it
// already holds, and positional reads for the 64 KiB tail.  `FileSystem`
// exposes none of the three, and re-opening a path to stat it is the race the
// symlink checks exist to close.  Everything below is confined to this
// section, and every call is an errors-are-values `Either`.
// ---------------------------------------------------------------------------

/** A `node:fs` call that threw.  Never escapes the module. */
class FsFailure extends Data.TaggedError('FsFailure')<{
  readonly operation: string;
  readonly path: string;
  readonly cause: unknown;
}> {}

const attempt = <A>(
  operation: string,
  path: string,
  thunk: () => A,
): Either.Either<A, FsFailure> =>
  Either.try({
    try: thunk,
    catch: (cause) => new FsFailure({ operation, path, cause }),
  });

/** `Path.is_symlink()` — `False` on any `OSError`, exactly like Python's. */
const isSymlink = (path: string): boolean =>
  Either.match(attempt('lstat', path, () => lstatSync(path)), {
    onLeft: () => false,
    onRight: (info) => info.isSymbolicLink(),
  });

/** `Path.is_dir()` — follows symlinks, `False` on any `OSError`. */
const isDirectory = (path: string): boolean =>
  Either.match(attempt('stat', path, () => statSync(path)), {
    onLeft: () => false,
    onRight: (info) => info.isDirectory(),
  });

/**
 * `Path.resolve()` with `strict=False`: resolve as far as the filesystem
 * allows, then keep the remaining components.  A run directory that does not
 * exist still resolves — which is what makes `run_root.parent != runs_root`
 * (`:563`) a *symlink* test rather than an existence test.
 */
const resolveStrictly = (root: string, name: string): string =>
  Either.getOrElse(
    attempt('realpath', join(root, name), () => realpathSync(join(root, name))),
    () => join(root, name),
  );

/** An open descriptor, released when the surrounding scope closes. */
const openFd = (
  path: string,
  flags: number,
): Effect.Effect<number, FsFailure, Scope.Scope> =>
  Effect.acquireRelease(
    Effect.suspend(() => attempt('open', path, () => openSync(path, flags))),
    (fd) => Effect.ignore(Effect.try(() => closeSync(fd))),
  );

interface FileInfo {
  readonly size: number;
  readonly regular: boolean;
}

const statFd = (fd: number): Either.Either<FileInfo, FsFailure> =>
  Either.map(
    attempt('fstat', `fd:${String(fd)}`, () => fstatSync(fd)),
    (info) => ({ size: info.size, regular: info.isFile() }),
  );

/**
 * `read(2)` until `length` bytes are in hand or the file ends — a single
 * `readSync` may come up short, and Python's `stream.read()` does not.
 */
const readFully = (
  fd: number,
  position: number,
  length: number,
): Either.Either<Uint8Array, FsFailure> =>
  attempt('read', `fd:${String(fd)}`, () => {
    const buffer = new Uint8Array(length);
    const step = (offset: number): number => {
      if (offset >= length) {
        return offset;
      }
      const read = readSync(fd, buffer, offset, length - offset, position + offset);
      return read <= 0 ? offset : step(offset + read);
    };
    return buffer.subarray(0, step(0));
  });

const UTF8 = new TextDecoder('utf-8', { fatal: true });

/** `bytes.decode("utf-8")` — strict, so a `UnicodeError` stays a failure. */
const decodeUtf8 = (bytes: Uint8Array): Option.Option<string> =>
  Option.getRight(Either.try(() => UTF8.decode(bytes)));

/**
 * Latin-1: the lossless byte↔code-unit mapping.  Written out rather than taken
 * from a `TextDecoder`, whose Bun typings admit only `utf-8`, `windows-1252`
 * and `utf-16` — and `windows-1252` is *not* the same mapping (`0x80`-`0x9f`
 * move).
 */
const decodeLatin1 = (bytes: Uint8Array): string =>
  Array.from(bytes, (byte) => String.fromCharCode(byte)).join('');

/** `json.loads`, narrowed to what `JSON.parse` accepts; see the module doc. */
const parseJson = (text: string): Option.Option<JsonValue> =>
  Option.getRight(decodeJsonValueFromString(text));

// `Array.isArray` does not narrow a `ReadonlyArray` member out of a union in
// its false branch; a declared guard does.
const isJsonArray = (value: JsonValue): value is JsonArray => Array.isArray(value);

const asJsonObject = (value: JsonValue): Option.Option<JsonObject> =>
  value !== null && typeof value === 'object' && !isJsonArray(value)
    ? Option.some(value)
    : Option.none();

/** `Path.read_text` — follows symlinks, no size ceiling, closes either way. */
const readWholeFile = (path: string): Either.Either<Uint8Array, FsFailure> =>
  Either.flatMap(attempt('open', path, () => openSync(path, O_RDONLY | O_CLOEXEC)), (fd) => {
    const bytes = Either.flatMap(statFd(fd), (info) => readFully(fd, 0, info.size));
    Either.getOrElse(attempt('close', path, () => closeSync(fd)), () => undefined);
    return bytes;
  });

// ---------------------------------------------------------------------------
// The service surface
// ---------------------------------------------------------------------------

/**
 * `TerminalArchive` (`:88`) — `../archive.ts`'s payload view plus the one
 * field it deliberately drops: the directory the files came from.
 */
export interface TerminalArchive extends ArchiveView {
  /** The resolved run directory, for the binary routes and nothing else. */
  readonly runRoot: string;
}

/** Options for {@link RunsRepositoryApi.diskGamesIndex}. */
export interface DiskGamesIndexOptions {
  /** `terminal_only=True` (`:1243`) — drop rows that never reached a terminal state. */
  readonly terminalOnly?: boolean;
}

/** Every read the gateway makes against `runs_root`. */
export interface RunsRepositoryApi {
  /** The resolved `runs_root`, as `gateway_config` computes it (`:196`). */
  readonly runsRoot: string;

  /** `_read_manifest` (`:557`) — the raw `manifest.json`, id-checked. */
  readonly readManifest: (gameId: string) => Effect.Effect<JsonObject, RunsError>;

  /** {@link readManifest} through wire's strict schema, for callers that want it. */
  readonly decodeManifest: (
    gameId: string,
  ) => Effect.Effect<Gateway.Manifest, RunsError | WireDecodeError>;

  /** `_terminal_archive` (`:773`) — 404 `terminal archive not found` when not terminal. */
  readonly terminalArchive: (gameId: string) => Effect.Effect<TerminalArchive, RunsError>;

  /** `_last_replay_turn` (`:1178`) — the 64 KiB tail of `replay.jsonl`. */
  readonly lastReplayTurn: (gameId: string) => Effect.Effect<Option.Option<bigint>>;

  /** `_disk_games_index` (`:1242`).  Cannot fail; malformed runs are dropped. */
  readonly diskGamesIndex: (
    options?: DiskGamesIndexOptions,
  ) => Effect.Effect<Canonical<Gateway.GamesIndexResponse>>;

  /** `_disk_rows_with_interrupted` (`:1582`).  Cannot fail. */
  readonly diskRowsWithInterrupted: (
    liveIds: ReadonlySet<string>,
  ) => Effect.Effect<readonly Canonical<Gateway.GameRow>[]>;

  /** `_archive_frame_path` (`:1007`); {@link Option.none} is `latest.png`. */
  readonly frameFile: (
    archive: TerminalArchive,
    index: Option.Option<FrameIndex>,
  ) => Effect.Effect<string, RunsError>;

  /** `_archive_video_path` (`:1022`). */
  readonly videoFile: (archive: TerminalArchive) => Effect.Effect<string, RunsError>;
}

/** The `runs_root` reader, as a service. */
export class RunsRepository extends Context.Tag('@arena/harness/gateway/RunsRepository')<
  RunsRepository,
  RunsRepositoryApi
>() {}

// ---------------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------------

/**
 * `_read_archive_json` (`:573`).
 *
 * Two failures with two different statuses, and the split is load-bearing: the
 * `open` failing is a **404** (`{label} not found`), while a file that opens
 * but is not a non-empty regular file of at most 8 MiB, or does not parse as a
 * JSON *object*, is a **503** (`{label} is unavailable`).
 */
const readArchiveJson = (
  path: string,
  label: Gateway.ArchiveJsonLabel,
): Effect.Effect<JsonObject, RunsError> =>
  Effect.scoped(
    Effect.flatMap(
      Effect.mapError(
        openFd(path, O_RDONLY | O_NOFOLLOW | O_CLOEXEC),
        () => new NotFound({ problem: ARCHIVE_JSON_PROBLEMS[label].missing }),
      ),
      (fd) =>
        Either.fromOption(
          Option.flatMap(Option.getRight(statFd(fd)), (info) =>
            !info.regular || info.size <= 0 || info.size > MAX_PROXY_JSON_BYTES
              ? Option.none()
              : Option.flatMap(
                  Option.flatMap(Option.getRight(readFully(fd, 0, info.size)), decodeUtf8),
                  (text) => Option.flatMap(parseJson(text), asJsonObject),
                ),
          ),
          () => new ArchiveUnavailable({ problem: ARCHIVE_JSON_PROBLEMS[label].unusable }),
        ),
    ),
  );

/**
 * `_read_manifest` (`:557`).
 *
 * The four 404s in order: a game id that fails `GAME_ID_RE`; a run directory
 * that is a *symlink* (a run may not point out of `runs_root`); a resolved
 * path whose parent is not `runs_root`, or that is not a directory; and a
 * manifest whose `game_id` disagrees with the path it was found at.
 */
const makeReadManifest =
  (runsRoot: string) =>
  (gameId: string): Effect.Effect<JsonObject, RunsError> =>
    Effect.suspend(() => {
      const gone: Either.Either<JsonObject, RunsError> = Either.left(
        new NotFound({ problem: 'gameNotFound' }),
      );
      if (!isGameId(gameId) || isSymlink(join(runsRoot, gameId))) {
        return gone;
      }
      const runRoot = resolveStrictly(runsRoot, gameId);
      if (dirname(runRoot) !== runsRoot || !isDirectory(runRoot)) {
        return gone;
      }
      return Effect.flatMap(
        readArchiveJson(join(runRoot, MANIFEST_FILE), 'game manifest'),
        (value) => (untrustedField(value, 'game_id') === gameId ? Effect.succeed(value) : gone),
      );
    });

/**
 * `_archive_victory`'s file read (`:684`).
 *
 * `Path.read_text` — **no** `O_NOFOLLOW`, no size ceiling, and every failure
 * (missing, unreadable, unparseable) collapses to "no record" without a trace.
 *
 * `turn` and `year` are passed through *unvalidated* (`:705`), and
 * `record.get(...)` puts the key there as `None` when the file omits it.  Both
 * facts belong to `../archive.ts#archiveVictory`, which relays the two fields
 * through the same integral-number rule it applies to the rest of the record —
 * recursively, which is what a record whose `turn` is an object needs.  This
 * module used to re-apply that rule on top, non-recursively, through an
 * `as unknown as JsonValue` double cast; the override agreed with
 * `archiveVictory` on a scalar, disagreed on a container, and lied to the
 * compiler either way.  Deleted: there is one relayer, and it is next door.
 */
const readVictoryRecord = (
  runRoot: string,
): Option.Option<Canonical<Gateway.MatchVictory>> =>
  archiveVictory(
    Option.flatMap(
      Option.flatMap(Option.getRight(readWholeFile(join(runRoot, VICTORY_FILE))), decodeUtf8),
      parseJson,
    ),
  );

/**
 * `_terminal_archive` (`:773`).
 *
 * Note the order: the manifest is read *and* the state checked before
 * `report.json` is opened, so a live run costs one file read and answers
 * `terminal archive not found` — which is also the answer for a terminal run
 * whose report names a different game.  `victory.json` is read last and only
 * for a run that has an archive at all.
 */
const makeTerminalArchive =
  (runsRoot: string, readManifest: RunsRepositoryApi['readManifest']) =>
  (gameId: string): Effect.Effect<TerminalArchive, RunsError> =>
    Effect.flatMap(readManifest(gameId), (manifest) => {
      if (!isGameId(gameId)) {
        // Unreachable: `readManifest` has already validated the id.
        return Either.left(new NotFound({ problem: 'gameNotFound' }));
      }
      const runRoot = resolveStrictly(runsRoot, gameId);
      return Effect.flatMap(
        // `_terminal_archive` checks the state before it opens `report.json`,
        // and `terminalArchiveView` needs both — so the state gate runs here.
        isTerminalManifest(manifest)
          ? readArchiveJson(join(runRoot, REPORT_FILE), 'game report')
          : Either.left(new NotFound({ problem: 'terminalArchiveNotFound' })),
        (report) =>
          Either.mapBoth(
            terminalArchiveView(gameId, manifest, report, readVictoryRecord(runRoot)),
            {
              onLeft: asNotFound,
              onRight: (view): TerminalArchive => ({ ...view, runRoot }),
            },
          ),
      );
    });

/**
 * The `state in TERMINAL_STATES` gate of `_terminal_archive` (`:778`).
 *
 * `terminalArchiveView` applies it too, but *after* it has been handed a
 * report — and Python checks it *before* opening one.  The difference is
 * visible: a live run with no `report.json` answers `terminal archive not
 * found`, not `game report not found`.
 */
const isTerminalManifest = (manifest: Untrusted): boolean =>
  isTerminalRunState(manifestState(manifest));

/**
 * `bytes.splitlines()` — `\n`, `\r` and `\r\n`, and nothing else.
 *
 * Latin-1 is a lossless byte↔code-unit mapping, so splitting there and mapping
 * back gives exactly the byte ranges Python's `splitlines` produces, without a
 * UTF-8 decode that a torn multi-byte sequence at the 64 KiB boundary would
 * fail.  Python drops the empty final element after a trailing newline; this
 * keeps it, and the blank-line filter removes it either way.
 */
const splitLines = (bytes: Uint8Array): ReadonlyArray<string> =>
  decodeLatin1(bytes).split(/\r\n|\n|\r/);

/** `bytes.strip()` — ASCII whitespace only, which is what `if not raw.strip()` tests. */
const BLANK_LINE_RE = /^[ \t\v\f]*$/;

const latin1Bytes = (line: string): Uint8Array =>
  Uint8Array.from(line, (character) => character.charCodeAt(0));

/**
 * `_last_replay_turn` (`:1178`).
 *
 * Scans the tail backwards, skipping blank lines and lines that do not parse
 * (a torn final write), and **stops at the first line that does**: a parseable
 * row without a positive integer `turn` answers "no turn", it does not keep
 * looking.  That single `return None` inside the loop (`:1207`) is what hides
 * a lobby husk from the index instead of resurrecting an older turn.
 *
 * `path.open("rb")` carries no `O_NOFOLLOW`, so a symlinked `replay.jsonl` is
 * followed here — deliberately unlike every other read in this module.
 */
const makeLastReplayTurn =
  (runsRoot: string) =>
  (gameId: string): Effect.Effect<Option.Option<bigint>> =>
    Effect.scoped(
      Effect.orElseSucceed(
        Effect.flatMap(
          openFd(join(runsRoot, gameId, REPLAY_JSONL_FILE), O_RDONLY | O_CLOEXEC),
          (fd) =>
            Effect.map(statFd(fd), (info) => {
              if (info.size <= 0) {
                return Option.none<bigint>();
              }
              const start = Math.max(0, info.size - REPLAY_TAIL_BYTES);
              const tail = Option.getOrElse(
                Option.getRight(readFully(fd, start, info.size - start)),
                () => new Uint8Array(0),
              );
              const parsed = splitLines(tail)
                .filter((line) => !BLANK_LINE_RE.test(line))
                .toReversed()
                .map((line) =>
                  Option.flatMap(decodeUtf8(latin1Bytes(line)), (text) =>
                    Option.getRight(parsePythonJson(text)),
                  ),
                )
                .find(Option.isSome);
              if (parsed === undefined) {
                return Option.none<bigint>();
              }
              // `isinstance(turn, int) and turn > 0` (`:1206`).  The reader
              // keeps CPython's int/float distinction, so a `bigint` is a
              // Python `int` and a tail row of `{"turn": 2.0}` is refused here
              // exactly as it is there — which drops the run from the index
              // rather than publishing a row Python hides.
              const turn = untrustedField(parsed.value, 'turn');
              return typeof turn === 'bigint' && turn > 0n
                ? Option.some(turn)
                : Option.none<bigint>();
            }),
        ),
        () => Option.none<bigint>(),
      ),
    );

/**
 * `_disk_games_index` (`:1242`).
 *
 * Every per-run failure is swallowed, including the ones raised *inside*
 * `_terminal_archive` — a terminal run whose `report.json` is missing or
 * corrupt loses its row entirely rather than appearing without a leaderboard.
 *
 * The rows come back sorted (`sortDiskGameRows`, `:1274`) and *not* wrapped:
 * `_disk_rows_with_interrupted` consumes the list, `/v1/games` consumes the
 * `{schema_version, games}` envelope, and only the second one is a payload.
 */
const makeDiskGameRows =
  (
    runsRoot: string,
    readManifest: RunsRepositoryApi['readManifest'],
    terminalArchive: RunsRepositoryApi['terminalArchive'],
  ) =>
  (
    options?: DiskGamesIndexOptions,
  ): Effect.Effect<readonly Canonical<Gateway.GameRow>[]> =>
    Effect.map(
      Effect.forEach(
        Either.getOrElse(
          attempt('readdir', runsRoot, () => readdirSync(runsRoot)),
          (): ReadonlyArray<string> => [],
        ).filter(
          (name) =>
            isGameId(name) &&
            !isSymlink(join(runsRoot, name)) &&
            isDirectory(join(runsRoot, name)),
        ),
        (name): Effect.Effect<Option.Option<Canonical<Gateway.GameRow>>> =>
          Effect.flatMap(Effect.either(readManifest(name)), (manifest) =>
            Either.match(manifest, {
              onLeft: () => Effect.succeed(Option.none<Canonical<Gateway.GameRow>>()),
              onRight: (value) =>
                Option.match(diskGameRow(value), {
                  onNone: () => Effect.succeed(Option.none<Canonical<Gateway.GameRow>>()),
                  onSome: (row) =>
                    isTerminalRow(row)
                      ? Effect.map(Effect.either(terminalArchive(name)), (archive) =>
                          Either.match(archive, {
                            onLeft: () => Option.none<Canonical<Gateway.GameRow>>(),
                            onRight: (found) =>
                              Option.some(
                                // `Object.assign`: see `asInterrupted` in
                                // `../archive.ts` — a spread would drop the
                                // `CanonRecord` half of `Canonical`.
                                Object.assign({}, row, {
                                  leaderboard: found.leaderboard,
                                  outcome: found.outcome,
                                }),
                              ),
                          }),
                        )
                      : Effect.succeed(
                          options?.terminalOnly === true
                            ? Option.none<Canonical<Gateway.GameRow>>()
                            : Option.some(row),
                        ),
                }),
            }),
          ),
      ),
      (rows) => sortDiskRows(Arr.getSomes(rows)),
    );

/**
 * `_disk_rows_with_interrupted` (`:1582`) — `../archive.ts`'s relabelling with
 * the one input only this layer can supply: `_last_replay_turn` per candidate.
 *
 * `interruptedCandidates` names exactly the rows that need a tail read, so a
 * terminal or live run never touches the disk a second time.
 */
const makeDiskRowsWithInterrupted =
  (
    diskGameRows: (
      options?: DiskGamesIndexOptions,
    ) => Effect.Effect<readonly Canonical<Gateway.GameRow>[]>,
    lastReplayTurn: RunsRepositoryApi['lastReplayTurn'],
  ) =>
  (
    liveIds: ReadonlySet<string>,
  ): Effect.Effect<readonly Canonical<Gateway.GameRow>[]> =>
    Effect.flatMap(diskGameRows(), (rows) =>
      Effect.map(
        Effect.forEach(interruptedCandidates(rows, liveIds), (gameId) =>
          Effect.map(lastReplayTurn(gameId), (turn) => [gameId, turn] as const),
        ),
        (turns) =>
          relabelDiskRows(
            rows,
            liveIds,
            new Map(
              turns.flatMap(([gameId, turn]) =>
                Option.match(turn, {
                  onNone: (): ReadonlyArray<readonly [string, bigint]> => [],
                  onSome: (value) => [[gameId, value] as const],
                }),
              ),
            ),
          ),
      ),
    );

/**
 * `created_at` as the float Python sorts on.
 *
 * `Canonical<GameRow>` intersects every field with `CanonValue`, which widens
 * this one to `number | bigint | null` in the type system even though
 * `_public_number` (`:348`) can only ever produce a float.  Nothing but a
 * `number` is reachable; anything else sorts as `0.0`, which is what a `null`
 * from an upstream row would have done anyway.
 */
const createdAt = (row: Canonical<Gateway.GameRow>): number =>
  typeof row.created_at === 'number' ? row.created_at : 0;

/**
 * `_disk_games_index`'s ordering (`:1274`): `(created_at, game_id)`
 * **descending on both**.
 *
 * `../archive.ts` exports this as `sortDiskGameRows`, and it is inlined here
 * because that signature's `A extends { created_at: number | null }` constraint
 * rejects a `Canonical<GameRow>` for the reason {@link createdAt} describes:
 * the intersection widens the field to `number | bigint | null`.  Widening the
 * constraint there deletes this function; until then the two must agree, and
 * the differential against CPython is what proves they do.
 */
const sortDiskRows = (
  rows: readonly Canonical<Gateway.GameRow>[],
): readonly Canonical<Gateway.GameRow>[] =>
  rows.toSorted((left, right) =>
    createdAt(left) === createdAt(right)
      ? compareCodePoints(right.game_id, left.game_id)
      : createdAt(right) - createdAt(left),
  );

/** A row `_disk_games_index` will try to enrich from a terminal archive (`:1265`). */
const isTerminalRow = (row: Canonical<Gateway.GameRow>): boolean =>
  isTerminalRunState(row.state);

/**
 * `_safe_archive_directory` (`:871`) — the containment check for
 * `watch_frames/` and `saves/`: it must exist, must not be a symlink, must be
 * a directory, and must resolve to a direct child of the run root.
 *
 * Exported because `../http/routes/archive.ts` needs the *listings* themselves
 * (`_archive_frames` enumerates both directories) and used to carry its own
 * transcription of this rule.  Two copies of a symlink-containment check is one
 * copy too many: correct one and the other keeps serving the old answer on a
 * different route family.
 */
export const safeArchiveDirectory = (
  root: string,
  name: string,
): Either.Either<string, RunsError> => {
  const missing: Either.Either<string, RunsError> = Either.left(
    new NotFound({ problem: 'archiveDataNotFound' }),
  );
  const info = attempt('lstat', join(root, name), () => lstatSync(join(root, name)));
  if (Either.isLeft(info) || info.right.isSymbolicLink() || !info.right.isDirectory()) {
    return missing;
  }
  const resolved = resolveStrictly(root, name);
  return dirname(resolved) === root ? Either.right(resolved) : missing;
};

/**
 * `_archive_regular_files` (`:885`) — names matching `pattern`, each of which
 * must be a non-empty *regular* file that is not a symlink.  An unreadable
 * directory is an empty listing, not a failure.
 *
 * Exported for the same reason as {@link safeArchiveDirectory}.
 */
export const archiveRegularFiles = (
  directory: string,
  pattern: RegExp,
): ReadonlyArray<string> =>
  Either.getOrElse(
    attempt('readdir', directory, () => readdirSync(directory)),
    (): ReadonlyArray<string> => [],
  ).filter((name) => {
    if (!pattern.test(name)) {
      return false;
    }
    const info = attempt('lstat', join(directory, name), () => lstatSync(join(directory, name)));
    return (
      Either.isRight(info) &&
      info.right.isFile() &&
      !info.right.isSymbolicLink() &&
      info.right.size > 0
    );
  });

/** The PNGs `_archive_frame_path` may choose between, as `../archive.ts` names them. */
const listFramePngs = (runRoot: string): Either.Either<readonly ArchivePng[], RunsError> =>
  Either.map(
    safeArchiveDirectory(runRoot, Gateway.ARCHIVE_FRAMES_DIRECTORY),
    (directory) =>
      archiveRegularFiles(directory, Gateway.ARCHIVE_PNG_RE).flatMap((name) =>
        Either.match(Gateway.decodeArchivePngName(name), {
          onLeft: (): ReadonlyArray<ArchivePng> => [],
          onRight: (index) => [{ index, name }],
        }),
      ),
  );

/** `_archive_frame_path` (`:1007`) — the listing, then `../archive.ts`'s choice. */
const frameFilePath = (
  archive: TerminalArchive,
  index: Option.Option<FrameIndex>,
): Either.Either<string, RunsError> =>
  Either.flatMap(listFramePngs(archive.runRoot), (pngs) =>
    Either.mapBoth(selectFramePng(pngs, index), {
      onLeft: asNotFound,
      onRight: (png) =>
        join(archive.runRoot, Gateway.ARCHIVE_FRAMES_DIRECTORY, png.name),
    }),
  );

/** `_archive_video_path` (`:1022`) — `game.mp4`: non-symlink, regular, non-empty. */
const videoFilePath = (archive: TerminalArchive): Either.Either<string, RunsError> => {
  const path = join(archive.runRoot, Gateway.ARCHIVE_VIDEO_FILE);
  const missing: Either.Either<string, RunsError> = Either.left(
    new NotFound({ problem: 'replayVideoNotFound' }),
  );
  const info = attempt('lstat', path, () => lstatSync(path));
  if (Either.isLeft(info)) {
    return missing;
  }
  return info.right.isSymbolicLink() || !info.right.isFile() || info.right.size <= 0
    ? missing
    : Either.right(path);
};

/**
 * Build the repository over one `runs_root`.
 *
 * The root is resolved once, at construction, the way `gateway_config` does
 * (`:196`, `Path(...).expanduser().resolve()`): every containment check
 * compares against it, so a root itself reached through a symlink has to be
 * normalized before, not during.
 */
export const makeRunsRepository = (runsRoot: string): RunsRepositoryApi => {
  const root = Either.getOrElse(
    attempt('realpath', runsRoot, () => realpathSync(resolvePath(runsRoot))),
    () => resolvePath(runsRoot),
  );
  const readManifest = makeReadManifest(root);
  const terminalArchive = makeTerminalArchive(root, readManifest);
  const lastReplayTurn = makeLastReplayTurn(root);
  const diskGameRows = makeDiskGameRows(root, readManifest, terminalArchive);
  return {
    runsRoot: root,
    readManifest,
    decodeManifest: (gameId) =>
      Effect.flatMap(readManifest(gameId), (manifest) => Gateway.decodeManifest(manifest)),
    terminalArchive,
    lastReplayTurn,
    diskGamesIndex: (options) => Effect.map(diskGameRows(options), gamesIndex),
    diskRowsWithInterrupted: makeDiskRowsWithInterrupted(diskGameRows, lastReplayTurn),
    frameFile: (archive, index) => Effect.suspend(() => frameFilePath(archive, index)),
    videoFile: (archive) => Effect.suspend(() => videoFilePath(archive)),
  };
};

/** The service, over one `runs_root`. */
export const layer = (runsRoot: string): Layer.Layer<RunsRepository> =>
  Layer.sync(RunsRepository, () => makeRunsRepository(runsRoot));
