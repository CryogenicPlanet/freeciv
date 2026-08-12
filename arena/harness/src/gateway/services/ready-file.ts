/**
 * The gateway ready file — one publisher, one record, cleaned up by whoever
 * wrote it.
 *
 * Port of the ready-file half of `run_replay_gateway`
 * (`agent_eval/replay_gateway.py:2114-2171`), which is three cooperating
 * pieces the Python spells out in order:
 *
 * ```python
 * ready_lock = _acquire_ready_lock(ready_path)   # :2153  flock(LOCK_EX|LOCK_NB) on <ready>.lock
 * _write_private_json(ready_path, ready)         # :2154  atomic, 0600, indent=2 sort_keys=True
 * on_ready(ready)                                # :2156  main prints _canonical(...) to stdout
 * ...
 * finally:
 *     _remove_owned_ready_file(ready_path, identity, pid)   # :2166  guarded unlink
 *     _release_ready_lock(ready_lock)                       # :2169  AFTER the unlink
 * ```
 *
 * Here that is one scoped resource: {@link ReadyFileService.publish} acquires
 * the lock and the record, and hands the release back to the caller's
 * `Scope`.  Nesting the two `acquireRelease`s in the Python's acquisition
 * order gives the Python's *release* order for free — unlink first, unlock
 * second — which is the ordering that makes the guard sound: a replacement
 * publisher can only take the lock after the incumbent has already decided
 * whether to delete the record, so it can never lose a newer record to an
 * older process's cleanup.
 *
 * ## The three byte formats
 *
 * The *same* object is emitted in two different spellings, and this module
 * owns both:
 *
 * | Sink | Spelling | Source |
 * |---|---|---|
 * | `<ready-file>` | `json.dump(indent=2, sort_keys=True)` + `"\n"`, `ensure_ascii=True` | `_write_private_json`, `:221-240` |
 * | stdout | `_canonical` — compact, sorted, `ensure_ascii=False` | `main`, `:2201-2203` |
 *
 * The file is **not** canonical JSON.  Emitting canonical bytes into the file
 * would still parse, and would still pass every Python test, and would still
 * be wrong for anything that diffs or digests the file.  {@link readyFileText}
 * is the pretty writer; the canonical one already lives in `@arena/wire` and
 * is imported, not rewritten.
 *
 * ## What the port adds
 *
 * `flock(2)` is not in Bun's `node:fs`, so it comes through `bun:ffi` against
 * `libSystem.B.dylib` with `__error()` for `errno` — the arrangement spike S3
 * proved interoperates with `fcntl.flock` in both directions
 * (`test/spikes/s3-flock.ts`).  The lock descriptor additionally carries
 * `O_CLOEXEC`, which the Python does not set: a `flock` lives on the open file
 * description, so an inherited descriptor in a spawned freeciv process would
 * keep the lock alive past this process's death.  `O_CLOEXEC` is spelled out
 * as a literal because `node:fs`'s `constants.O_CLOEXEC` reads back
 * `undefined` under Bun on darwin, and `undefined` in a `|` chain contributes
 * a silent `0`.
 *
 * @module
 */

import { FFIType, dlopen, read } from 'bun:ffi';
import {
  chmodSync,
  closeSync,
  constants,
  fsyncSync,
  mkdirSync,
  openSync,
  readFileSync,
  realpathSync,
  renameSync,
  unlinkSync,
  writeSync,
} from 'node:fs';
import { homedir } from 'node:os';
import { basename, dirname, join, resolve, sep } from 'node:path';
import { Context, Data, Effect, Either, Layer, Option, type Scope } from 'effect';
import {
  CANON_ASCII,
  CANON_UTF8,
  type CanonError,
  type CanonRecord,
  type CanonValue,
  canonicalText,
  compareCodePoints,
} from '@arena/wire';

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/** `0o600` — the mode both the record and its lock are created and forced to. */
export const READY_FILE_MODE = 0o600;

/** `path.with_name(path.name + ".lock")` (`:245`). */
export const READY_LOCK_SUFFIX = '.lock';

/** darwin `sys/file.h`; identical values to Linux. */
export const LOCK_EX = 2;
export const LOCK_NB = 4;
export const LOCK_UN = 8;

/** darwin `EAGAIN === EWOULDBLOCK === 35` — what Python raises as `BlockingIOError`. */
export const EWOULDBLOCK = 35;

/**
 * darwin `O_CLOEXEC`.  Written out because `node:fs`'s `constants` does not
 * carry it under Bun: it reads back `undefined`, which would contribute `0` to
 * the flag word instead of failing.
 */
export const O_CLOEXEC = 0x0100_0000;

/** Reported when libc will not say where `errno` lives. */
const UNKNOWN_ERRNO = -1;

/** `json.dump(..., indent=2)`. */
const INDENT_UNIT = '  ';

/** `secrets.token_hex(6)` — 6 random bytes, 12 lowercase hex characters. */
const TEMP_TOKEN_BYTES = 6;

const ENCODER = new TextEncoder();

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/** `bun:ffi` could not bind `flock`, so no lock can be taken at all. */
export class ReadyLockUnavailable extends Data.TaggedError('ReadyLockUnavailable')<{
  readonly reason: string;
  readonly cause: unknown;
}> {}

/**
 * Another publisher already holds `<ready>.lock`.
 *
 * The Python surfaces this as `BlockingIOError` out of `run_replay_gateway`,
 * which `main` reports as `error: …` on stderr and exit 2 (`:2205-2207`).
 * Here it is a value, so the caller decides.
 */
export class ReadyFileLocked extends Data.TaggedError('ReadyFileLocked')<{
  readonly lockPath: string;
  readonly errno: number;
}> {}

/** A filesystem call the publish path cannot proceed without failed. */
export class ReadyFileIoError extends Data.TaggedError('ReadyFileIoError')<{
  readonly operation: string;
  readonly path: string;
  readonly cause: unknown;
}> {}

/** The payload has no CPython JSON spelling (`NaN`, a lone surrogate, …). */
export class ReadyFileEncodeError extends Data.TaggedError('ReadyFileEncodeError')<{
  readonly cause: CanonError;
}> {}

/** Every way publishing a ready record can fail. */
export type ReadyFileError =
  | ReadyLockUnavailable
  | ReadyFileLocked
  | ReadyFileIoError
  | ReadyFileEncodeError;

// ---------------------------------------------------------------------------
// The two byte formats
// ---------------------------------------------------------------------------

/**
 * `Array.isArray` refuses to narrow the negative branch of a `ReadonlyArray`
 * union, so the object case gets an explicit guard (same shape as
 * `@arena/wire`'s writer).
 */
const isRecord = (value: readonly CanonValue[] | CanonRecord): value is CanonRecord =>
  !Array.isArray(value);

const indentAt = (depth: number): string => INDENT_UNIT.repeat(depth);

/**
 * `json.dump(value, indent=2, sort_keys=True)` — CPython's pretty writer.
 *
 * Leaves delegate to `@arena/wire`'s canonical writer in `ensure_ascii=True`
 * mode: a scalar's canonical spelling and its pretty spelling are the same
 * string, so the float `repr`, the escaping table and the `bigint`/`number`
 * split are shared rather than re-derived.  Containers are the only thing this
 * function actually adds, and CPython special-cases the empty ones to `{}` and
 * `[]` with no newline inside.
 *
 * Depth is not guarded here because {@link readyLineText} runs the whole
 * document through the canonical writer first, and that writer refuses
 * anything nested deeper than `CANON_MAX_DEPTH`.
 */
const prettyText = (
  value: CanonValue,
  depth: number,
): Either.Either<string, CanonError> => {
  if (value === null || typeof value !== 'object') return canonicalText(value, CANON_ASCII);
  const inner = indentAt(depth + 1);
  const outer = indentAt(depth);
  if (isRecord(value)) {
    const entries = Object.entries(value)
      .toSorted((left, right) => compareCodePoints(left[0], right[0]))
      .map(([key, item]) =>
        Either.zipWith(
          canonicalText(key, CANON_ASCII),
          prettyText(item, depth + 1),
          (encodedKey, encodedValue) => `${inner}${encodedKey}: ${encodedValue}`,
        ),
      );
    return Either.map(Either.all(entries), (parts) =>
      parts.length === 0 ? '{}' : `{\n${parts.join(',\n')}\n${outer}}`,
    );
  }
  const items = Array.from(value, (item) =>
    Either.map(prettyText(item, depth + 1), (encoded) => `${inner}${encoded}`),
  );
  return Either.map(Either.all(items), (parts) =>
    parts.length === 0 ? '[]' : `[\n${parts.join(',\n')}\n${outer}]`,
  );
};

/**
 * The exact text `_write_private_json` puts in the file (`:232-233`):
 * `json.dump(value, indent=2, sort_keys=True)` followed by `stream.write("\n")`.
 * `ensure_ascii` is CPython's default `True`, so the result is pure ASCII and
 * a non-ASCII path in the payload arrives as `\uXXXX` escapes — where the
 * stdout line, written with `ensure_ascii=False`, carries the same character
 * literally.
 */
export const readyFileText = (
  payload: CanonRecord,
): Either.Either<string, CanonError> =>
  Either.map(prettyText(payload, 0), (text) => `${text}\n`);

/**
 * The stdout handshake line: `_canonical(value).decode("utf-8")` (`:2201`),
 * i.e. compact, key-sorted, `ensure_ascii=False`.  `print` adds the newline;
 * this is the line without it.
 */
export const readyLineText = (
  payload: CanonRecord,
): Either.Either<string, CanonError> => canonicalText(payload, CANON_UTF8);

const encodeOrFail = (
  encoded: Either.Either<string, CanonError>,
): Effect.Effect<string, ReadyFileEncodeError> =>
  Either.match(encoded, {
    onLeft: (cause) => Effect.fail(new ReadyFileEncodeError({ cause })),
    onRight: (text) => Effect.succeed(text),
  });

// ---------------------------------------------------------------------------
// Paths
// ---------------------------------------------------------------------------

/** `Path.expanduser()`, for the only form the launcher ever passes: a bare `~`. */
const expandUser = (path: string): string =>
  path === '~'
    ? homedir()
    : path.startsWith(`~${sep}`)
      ? join(homedir(), path.slice(2))
      : path;

/**
 * `Path.resolve()` with `strict=False`: make it absolute, then replace the
 * longest existing prefix with its real path and re-attach the rest.  On
 * darwin that is the difference between `/tmp/x` and `/private/tmp/x`, which
 * matters because the resolved string is what the pid guard and the caller
 * both report.
 */
const resolveExisting = (candidate: string): Effect.Effect<string> =>
  Effect.orElse(
    Effect.try(() => realpathSync(candidate)),
    () => {
      const parent = dirname(candidate);
      return parent === candidate
        ? Effect.succeed(candidate)
        : Effect.map(resolveExisting(parent), (real) => join(real, basename(candidate)));
    },
  );

/** `Path(ready_file).expanduser().resolve()` (`:2147`). */
export const resolveReadyPath = (path: string): Effect.Effect<string> =>
  resolveExisting(resolve(expandUser(path)));

const makeParent = (path: string): Effect.Effect<void, ReadyFileIoError> =>
  Effect.asVoid(
    Effect.try({
      try: () => mkdirSync(dirname(path), { recursive: true }),
      catch: (cause) => new ReadyFileIoError({ operation: 'mkdir', path: dirname(path), cause }),
    }),
  );

// ---------------------------------------------------------------------------
// libc
// ---------------------------------------------------------------------------

interface Libc {
  readonly flock: (fd: number, operation: number) => number;
  readonly currentErrno: () => number;
}

/** What `flock(2)` returned, and — when it failed — the `errno` behind it. */
interface FlockOutcome {
  readonly returnCode: number;
  readonly errno: number;
}

const loadLibc: Effect.Effect<Libc, ReadyLockUnavailable> = Effect.try({
  try: (): Libc => {
    const library = dlopen('libSystem.B.dylib', {
      flock: { args: [FFIType.i32, FFIType.i32], returns: FFIType.i32 },
      // `errno` is a macro over a per-thread location; libc exposes it as `__error()`.
      __error: { args: [], returns: FFIType.ptr },
    });
    return {
      flock: (fd, operation) => library.symbols.flock(fd, operation),
      currentErrno: () => {
        const location = library.symbols.__error();
        return location === null ? UNKNOWN_ERRNO : read.i32(location, 0);
      },
    };
  },
  catch: (cause) =>
    new ReadyLockUnavailable({ reason: 'dlopen(libSystem.B.dylib) failed', cause }),
});

/**
 * The binding, as a value a `Layer` can hold.
 *
 * This used to be `Effect.runSync(Effect.either(loadLibc))` at module scope —
 * the only `Effect.run*` anywhere under `src/gateway/**`, executing a
 * `dlopen('libSystem.B.dylib')` at *import* time: before argv was parsed,
 * before a single `Layer` was built, outside any `Scope`, with the handle never
 * released, and paid for even by {@link ReadyFileDisabled}, the layer whose
 * stated purpose is to work on a platform this binding does not cover.
 * Deferring the *error* was the point of that line and it did that; it did not
 * defer the *work*.
 *
 * Now the handle is opened by {@link layer}, when the layer is built, from a
 * process that has already parsed its argv — the discipline `./upstream.ts`
 * states for its own client ("a `Context.Tag` rather than a module-level
 * singleton").  An `Either` rather than a failure channel so that a gateway on
 * a platform without `flock` still *builds* and reports the problem from the
 * `publish` that needed it.
 *
 * It is deliberately **not** wrapped in an `acquireRelease` that `dlclose`s:
 * `libSystem.B.dylib` is the process's own C library, and handing it to
 * `dlclose` at scope exit is a bus error on darwin — measured, not assumed.
 * The handle is a reference to something already resident; what this change
 * removes is the *import-time work*, not a leak.
 */
export const acquireLibc: Effect.Effect<Either.Either<Libc, ReadyLockUnavailable>> =
  Effect.either(loadLibc);

/** True when `flock` is bound; the lock path is unavailable otherwise. */
export const lockSupported: Effect.Effect<boolean> = Effect.map(
  Effect.either(loadLibc),
  Either.isRight,
);

const flockCall = (
  libc: Either.Either<Libc, ReadyLockUnavailable>,
  fd: number,
  operation: number,
): Effect.Effect<FlockOutcome, ReadyLockUnavailable> =>
  Effect.map(libc, (lib) => {
    const returnCode = lib.flock(fd, operation);
    return { returnCode, errno: returnCode === 0 ? 0 : lib.currentErrno() };
  });

// ---------------------------------------------------------------------------
// The lock
// ---------------------------------------------------------------------------

const closeQuietly = (fd: number): Effect.Effect<void> =>
  Effect.ignore(Effect.try(() => closeSync(fd)));

/**
 * `os.open(lock_path, O_RDWR | O_CREAT, 0o600)` (`:247`) plus the explicit
 * `chmod` on `:249` — the mode argument only applies when the file is created,
 * and the lock file outlives individual runs, so the mode is forced every time.
 */
const openLockFd = (lockPath: string): Effect.Effect<number, ReadyFileIoError> =>
  Effect.tap(
    Effect.try({
      try: () =>
        openSync(
          lockPath,
          constants.O_RDWR | constants.O_CREAT | O_CLOEXEC,
          READY_FILE_MODE,
        ),
      catch: (cause) => new ReadyFileIoError({ operation: 'open', path: lockPath, cause }),
    }),
    () =>
      Effect.try({
        try: () => chmodSync(lockPath, READY_FILE_MODE),
        catch: (cause) => new ReadyFileIoError({ operation: 'chmod', path: lockPath, cause }),
      }),
  );

/**
 * `EWOULDBLOCK` is the contended case and nothing else is: `EBADF` or `EINVAL`
 * would mean this module opened the descriptor wrong, which is an I/O fault to
 * report, not a peer to wait for.
 */
const lockOutcome = (
  outcome: FlockOutcome,
  lockPath: string,
): Effect.Effect<void, ReadyFileLocked | ReadyFileIoError> =>
  outcome.returnCode === 0
    ? Effect.void
    : outcome.errno === EWOULDBLOCK
      ? Effect.fail(new ReadyFileLocked({ lockPath, errno: outcome.errno }))
      : Effect.fail(
          new ReadyFileIoError({
            operation: 'flock',
            path: lockPath,
            cause: `flock failed with errno ${outcome.errno}`,
          }),
        );

const takeLock = (
  libc: Either.Either<Libc, ReadyLockUnavailable>,
  fd: number,
  lockPath: string,
): Effect.Effect<void, ReadyLockUnavailable | ReadyFileLocked | ReadyFileIoError> =>
  Effect.flatMap(flockCall(libc, fd, LOCK_EX | LOCK_NB), (outcome) =>
    lockOutcome(outcome, lockPath),
  );

/**
 * The companion lock, held for as long as the scope lives.
 *
 * `_acquire_ready_lock` closes the descriptor on *any* acquisition failure
 * (`except BaseException: os.close(descriptor); raise`, `:250-252`); the
 * `Effect.onError` reproduces that for interruption as well as failure, so a
 * publisher that loses the race leaks nothing.  Release is `LOCK_UN` then
 * `close`, both ignored — a finalizer that fails would mask the reason the
 * scope is closing.
 */
const acquireReadyLock = (
  libc: Either.Either<Libc, ReadyLockUnavailable>,
  lockPath: string,
): Effect.Effect<
  number,
  ReadyLockUnavailable | ReadyFileLocked | ReadyFileIoError,
  Scope.Scope
> =>
  Effect.acquireRelease(
    Effect.flatMap(
      Effect.zipRight(makeParent(lockPath), openLockFd(lockPath)),
      (fd) =>
        Effect.as(Effect.onError(takeLock(libc, fd, lockPath), () => closeQuietly(fd)), fd),
    ),
    (fd) => Effect.zipRight(Effect.ignore(flockCall(libc, fd, LOCK_UN)), closeQuietly(fd)),
  );

// ---------------------------------------------------------------------------
// The atomic 0600 write
// ---------------------------------------------------------------------------

const tokenHex = (bytes: number): string =>
  Array.from(crypto.getRandomValues(new Uint8Array(bytes)), (byte) =>
    byte.toString(16).padStart(2, '0'),
  ).join('');

const writeAll = (
  fd: number,
  bytes: Uint8Array,
  path: string,
): Effect.Effect<void, ReadyFileIoError> =>
  bytes.length === 0
    ? Effect.void
    : Effect.flatMap(
        Effect.try({
          try: () => writeSync(fd, bytes),
          catch: (cause) => new ReadyFileIoError({ operation: 'write', path, cause }),
        }),
        (written) =>
          written > 0
            ? writeAll(fd, bytes.subarray(written), path)
            : Effect.fail(
                new ReadyFileIoError({
                  operation: 'write',
                  path,
                  cause: 'write(2) made no progress',
                }),
              ),
      );

/**
 * `_write_private_json` (`:221-240`), step for step: create
 * `.{name}.{token_hex(6)}.tmp` with `O_EXCL` at mode 0600 so a hostile
 * pre-existing temp cannot be written through, write, `flush`, `fsync`,
 * `os.replace` onto the destination, `chmod` the destination back to 0600
 * (`os.replace` keeps the temp's mode, but an operator-hostile umask is not
 * the only way a mode drifts), and unlink the temp in a `finally` whether or
 * not any of that worked.
 */
const writePrivateJson = (
  destination: string,
  text: string,
): Effect.Effect<Uint8Array, ReadyFileIoError> =>
  Effect.gen(function* () {
    const bytes = ENCODER.encode(text);
    yield* makeParent(destination);
    const temporary = yield* Effect.sync(() =>
      join(
        dirname(destination),
        `.${basename(destination)}.${tokenHex(TEMP_TOKEN_BYTES)}.tmp`,
      ),
    );
    yield* Effect.acquireUseRelease(
      Effect.try({
        try: () =>
          openSync(
            temporary,
            constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL,
            READY_FILE_MODE,
          ),
        catch: (cause) => new ReadyFileIoError({ operation: 'open', path: temporary, cause }),
      }),
      (fd) =>
        Effect.zipRight(
          writeAll(fd, bytes, temporary),
          Effect.try({
            try: () => fsyncSync(fd),
            catch: (cause) => new ReadyFileIoError({ operation: 'fsync', path: temporary, cause }),
          }),
        ),
      (fd) => closeQuietly(fd),
    ).pipe(
      Effect.zipRight(
        Effect.try({
          try: () => renameSync(temporary, destination),
          catch: (cause) =>
            new ReadyFileIoError({ operation: 'replace', path: destination, cause }),
        }),
      ),
      Effect.zipRight(
        Effect.try({
          try: () => chmodSync(destination, READY_FILE_MODE),
          catch: (cause) =>
            new ReadyFileIoError({ operation: 'chmod', path: destination, cause }),
        }),
      ),
      Effect.ensuring(Effect.ignore(Effect.try(() => unlinkSync(temporary)))),
    );
    return bytes;
  });

// ---------------------------------------------------------------------------
// The guarded unlink
// ---------------------------------------------------------------------------

/** `isinstance(value, dict)` over a `JSON.parse` result — arrays are not dicts. */
const isJsonObject = (value: unknown): value is object =>
  typeof value === 'object' && value !== null && !Array.isArray(value);

/**
 * `dict.get(key)`.  `Reflect.get` rather than an index signature assertion:
 * the value came off the disk, and claiming a shape for it is exactly the kind
 * of unchecked narrowing that would make the pid guard pass on a record it
 * never read.
 */
const propertyOf = (value: object, key: string): unknown => Reflect.get(value, key);

/**
 * Does the value read back out of the file mean the same thing as the value we
 * wrote?  `JSON.parse` hands back a `number` where the payload carries a
 * `bigint`, so identity on its own would never match on `pid` — and a guard
 * that never matches turns "clean up after yourself" into "never clean up".
 */
const sameJsonValue = (expected: CanonValue | undefined, actual: unknown): boolean => {
  if (expected === undefined) return actual === undefined;
  if (typeof expected === 'bigint') {
    return typeof actual === 'number' && Number.isInteger(actual) && BigInt(actual) === expected;
  }
  return expected === actual;
};

const decodeUtf8Strict = (bytes: Uint8Array): string =>
  new TextDecoder('utf-8', { fatal: true }).decode(bytes);

/**
 * `_remove_owned_ready_file` (`:2101-2111`): re-read, and unlink **only** if
 * the record is still an object whose `identity` and `pid` are ours.  Every
 * read or parse failure means "not provably mine", which is a `return`, not an
 * error — the file belongs to whoever wrote it.
 *
 * The `.lock` companion is never unlinked, by the Python or by this: the lock
 * is what serializes publishers, and deleting it would hand two of them
 * different inodes to lock.
 */
const removeOwnedReadyFile = (
  path: string,
  identity: CanonValue | undefined,
  pid: CanonValue | undefined,
): Effect.Effect<void> =>
  Effect.ignore(
    Effect.flatMap(
      // A schema would only re-describe `unknown` here: the guard's whole
      // contract is CPython's "any decode failure means not mine", which is
      // the `Effect.try` boundary, not the shape of what came back.
      Effect.try(() => JSON.parse(decodeUtf8Strict(readFileSync(path))) as unknown),
      (record) =>
        isJsonObject(record) &&
        sameJsonValue(identity, propertyOf(record, 'identity')) &&
        sameJsonValue(pid, propertyOf(record, 'pid'))
          ? Effect.ignore(Effect.try(() => unlinkSync(path)))
          : Effect.void,
    ),
  );

// ---------------------------------------------------------------------------
// The service
// ---------------------------------------------------------------------------

/**
 * Where the stdout handshake line goes.
 *
 * A parameter rather than a hard-wired `console.log` so a test can capture the
 * exact line without racing the runner's own stdout — and so the one place
 * that writes to stdout in the whole gateway stays visible in a type
 * (`log_message` is a no-op at `:1332`; nothing else prints).
 */
export type ReadyLineSink = (line: string) => Effect.Effect<void, ReadyFileIoError>;

/** `print(line, flush=True)` on the real process. */
export const stdoutSink: ReadyLineSink = (line) =>
  Effect.asVoid(
    Effect.tryPromise({
      try: () => Bun.write(Bun.stdout, `${line}\n`),
      catch: (cause) =>
        new ReadyFileIoError({ operation: 'write', path: '<stdout>', cause }),
    }),
  );

/** What a publisher got: `Option.none()` everywhere the record was suppressed. */
export interface ReadyPublication {
  /** The resolved ready-file path, absent when publishing is disabled. */
  readonly path: Option.Option<string>;
  /** `<path>.lock`, absent when publishing is disabled. */
  readonly lockPath: Option.Option<string>;
  /** The canonical one-line spelling that went to the sink. */
  readonly line: string;
  /** The exact bytes written to the file. */
  readonly bytes: Option.Option<Uint8Array>;
}

/** Publishing the ready record, as a scoped resource. */
export interface ReadyFileService {
  /**
   * Take the lock, write the record, emit the stdout line — and register the
   * guarded unlink plus the lock release on the caller's `Scope`.
   *
   * `payload` must already carry the *bound* port: `identity_payload` reads
   * `server.server_address` after the socket is listening (`:1302`), so a
   * `--port 0` gateway publishes the port the kernel handed out and never `0`.
   * Per spike law the caller must also have finished `serve()` before calling
   * this — a Bun socket 404s from library code until it returns, so a record
   * published earlier would advertise a URL that is not yet answering.
   */
  readonly publish: (
    payload: CanonRecord,
  ) => Effect.Effect<ReadyPublication, ReadyFileError, Scope.Scope>;
}

/** The ready-file publisher. */
export class ReadyFile extends Context.Tag('@arena/harness/gateway/ReadyFile')<
  ReadyFile,
  ReadyFileService
>() {}

/** How a live publisher is configured. */
export interface ReadyFileOptions {
  /** `--ready-file`; resolved with `expanduser()` + `resolve()` before use. */
  readonly path: string;
  /** Where the canonical handshake line goes; {@link stdoutSink} in production. */
  readonly sink: ReadyLineSink;
}

const makeReadyFile = (
  options: ReadyFileOptions,
  libc: Either.Either<Libc, ReadyLockUnavailable>,
): ReadyFileService => ({
  publish: (payload) =>
    Effect.gen(function* () {
      // Both spellings are computed before anything is created: an
      // unencodable payload then fails with nothing on disk and no lock held,
      // where the Python would have written the file and blown up at `print`.
      const line = yield* encodeOrFail(readyLineText(payload));
      const text = yield* encodeOrFail(readyFileText(payload));
      const destination = yield* resolveReadyPath(options.path);
      const lockPath = `${destination}${READY_LOCK_SUFFIX}`;

      yield* acquireReadyLock(libc, lockPath);
      const bytes = yield* Effect.acquireRelease(
        writePrivateJson(destination, text),
        () => removeOwnedReadyFile(destination, payload['identity'], payload['pid']),
      );
      yield* options.sink(line);

      return {
        path: Option.some(destination),
        lockPath: Option.some(lockPath),
        line,
        bytes: Option.some(bytes),
      };
    }),
});

/**
 * A publisher that owns `options.path` and its `.lock` companion.  The
 * counterpart for tests that want a gateway without a filesystem record is
 * {@link ReadyFileDisabled}.
 *
 * `Layer.scoped`, because the `libSystem` handle is a resource: it is opened
 * when this layer is built and closed when it is released, so importing this
 * module costs nothing and a gateway that never publishes never `dlopen`s.
 */
export const layer = (options: ReadyFileOptions): Layer.Layer<ReadyFile> =>
  Layer.effect(ReadyFile, Effect.map(acquireLibc, (libc) => makeReadyFile(options, libc)));

const disabled: ReadyFileService = {
  publish: (payload) =>
    Effect.map(encodeOrFail(readyLineText(payload)), (line) => ({
      path: Option.none(),
      lockPath: Option.none(),
      line,
      bytes: Option.none(),
    })),
};

/**
 * The `ready_file=None` arm of `run_replay_gateway` (`:2151`), minus the
 * stdout print: a gateway under test publishes nothing, locks nothing, and
 * leaves no finalizer — but still proves its payload has a canonical
 * spelling, which is the one failure mode a test would otherwise only meet in
 * production.
 */
export const ReadyFileDisabled: Layer.Layer<ReadyFile> = Layer.succeed(ReadyFile, disabled);
