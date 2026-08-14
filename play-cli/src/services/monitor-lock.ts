/**
 * The persistent monitor's singleton lock, and the holder record inside it.
 *
 * Ports `_monitor_lock_path` (client.py:592-594), `_monitor_lock` (596-662),
 * `_read_monitor_holder` (665-673) and `_monitor_holder` (676-711).
 *
 * The kernel-backed `flock(2)` provides idempotency and crash recovery: a
 * second monitor cannot acquire the lock, and process death closes the scoped
 * descriptor automatically. The lock file doubles as the holder record,
 * written only after acquisition, so `--status` and `--stop` need no PID file.
 * A bounded `O_EXCL` sentinel remains the fallback where libc cannot bind.
 */
import type { FileSystem } from '@effect/platform';
import type * as Scope from 'effect/Scope';
import { dlopen, FFIType } from 'bun:ffi';
import { Effect, Either, Schema } from 'effect';
import { type PlayerError, playerError, attemptOr } from 'src/errors';
import { JsonObjectSchema, type JsonObject } from 'src/schema/primitives';
import { pyJsonDumps } from 'src/services/json-output';
import { monitorLockPath } from 'src/services/locks';
import { PrivateFs, type PrivateFsApi } from 'src/services/private-fs';
import { fileSystem, path } from 'src/services/platform';

export { monitorLockPath };

const LOCK_EX = 2;
const LOCK_SH = 1;
const LOCK_NB = 4;
const LOCK_UN = 8;
const HOLDER_BYTES = 4096;

type FlockFn = (descriptor: number, operation: number) => number;
const LIBC_CANDIDATES = ['libc.dylib', 'libc.so.6', 'libc.so', 'libSystem.dylib'] as const;

interface FlockCache {
  resolved: boolean;
  value: FlockFn | null;
}

const flockBinding = ((): (() => FlockFn | null) => {
  const cache: FlockCache = { resolved: false, value: null };
  return () => {
    if (cache.resolved) return cache.value;
    cache.resolved = true;
    const bound = LIBC_CANDIDATES.flatMap((candidate) =>
      attemptOr(
        (): ReadonlyArray<FlockFn> => {
          const library = dlopen(candidate, {
            flock: { args: [FFIType.i32, FFIType.i32], returns: FFIType.i32 },
          });
          return [library.symbols.flock];
        },
        (): ReadonlyArray<FlockFn> => []
      )
    );
    cache.value = bound[0] ?? null;
    return cache.value;
  };
})();

/** Read a monitor holder record from an already-open lock file. */
export const readMonitorHolder = (
  opened: FileSystem.File
): Effect.Effect<JsonObject> =>
  Effect.orElseSucceed(
    Effect.gen(function* () {
      yield* opened.seek(0, 'start');
      const content = yield* opened.readAlloc(HOLDER_BYTES);
      if (content._tag === 'None') return {};
      const value = Schema.decodeUnknownEither(Schema.parseJson(JsonObjectSchema))(
        new TextDecoder().decode(content.value)
      );
      return Either.isRight(value) ? value.right : {};
    }),
    (): JsonObject => ({})
  );

const lockFile = (
  files: PrivateFsApi,
  sessionPath: string,
  create: boolean
): Effect.Effect<string, PlayerError> =>
  Effect.gen(function* () {
    const { relative } = yield* files.resolve(monitorLockPath(sessionPath));
    const parent = yield* files.openDirectory(relative.slice(0, -1), { create });
    const leaf = relative.at(-1);
    return leaf === undefined
      ? yield* playerError('the monitor lock path is invalid')
      : path.join(parent, leaf);
  });

const sentinelOf = (file: string): string => `${file}.held`;
const ErrnoSchema = Schema.Struct({ code: Schema.String });

const isLive = (pid: number): boolean =>
  attemptOr(
    () => {
      process.kill(pid, 0);
      return true;
    },
    (cause) => Schema.is(ErrnoSchema)(cause) && cause.code === 'EPERM'
  );

const refuseSymbolicLink = (target: string): Effect.Effect<void, PlayerError> =>
  Effect.flatMap(Effect.either(fileSystem.readLink(target)), (link) =>
    Either.isRight(link)
      ? Effect.fail(playerError('the monitor lock must be a mode-0600 file'))
      : Effect.void
  );

const openMonitorFile = (
  file: string,
  create: boolean
): Effect.Effect<FileSystem.File, PlayerError, Scope.Scope> =>
  Effect.gen(function* () {
    yield* refuseSymbolicLink(file);
    if (create) {
      const existing = yield* Effect.either(fileSystem.stat(file));
      if (Either.isLeft(existing)) {
        yield* Effect.ignore(
          fileSystem.writeFile(file, new Uint8Array(), { flag: 'wx', mode: 0o600 })
        );
      }
      yield* refuseSymbolicLink(file);
    }
    const opened = yield* Effect.mapError(
      fileSystem.open(file, { flag: create ? 'r+' : 'r' }),
      () => playerError('cannot safely lock the monitor')
    );
    const info = yield* Effect.mapError(
      opened.stat,
      () => playerError('cannot safely lock the monitor')
    );
    if (info.type !== 'File' || (info.mode & 0o777) !== 0o600) {
      return yield* playerError('the monitor lock must be a mode-0600 file');
    }
    return opened;
  });

const writeHolder = (
  opened: FileSystem.File,
  holder: JsonObject
): Effect.Effect<void, PlayerError> =>
  Effect.gen(function* () {
    const record = new TextEncoder().encode(pyJsonDumps(holder, { sortKeys: true }));
    yield* opened.truncate(0);
    yield* opened.seek(0, 'start');
    yield* opened.writeAll(record);
    yield* opened.sync;
  }).pipe(
    Effect.mapError(() => playerError('cannot safely lock the monitor'))
  );

const clearHolder = (opened: FileSystem.File): Effect.Effect<void> =>
  Effect.ignore(opened.truncate(0));

const runNative = <A, E, R>(
  flock: FlockFn,
  file: string,
  holder: JsonObject,
  body: (running: JsonObject | null) => Effect.Effect<A, E, R>
): Effect.Effect<A, E | PlayerError, R | Scope.Scope> =>
  Effect.gen(function* () {
    const opened = yield* openMonitorFile(file, true);
    const descriptor = Number(opened.fd);
    if (flock(descriptor, LOCK_EX | LOCK_NB) !== 0) {
      return yield* body(yield* readMonitorHolder(opened));
    }
    yield* Effect.onError(writeHolder(opened, holder), () =>
      Effect.sync(() => {
        attemptOr(() => flock(descriptor, LOCK_UN), () => 0);
      })
    );
    return yield* Effect.ensuring(
      body(null),
      Effect.zipRight(
        clearHolder(opened),
        Effect.sync(() => {
          attemptOr(() => flock(descriptor, LOCK_UN), () => 0);
        })
      )
    );
  });

const sentinelPid = (sentinel: string): Effect.Effect<number | null> =>
  Effect.map(
    Effect.either(fileSystem.readFileString(sentinel)),
    (content) => {
      if (Either.isLeft(content)) return null;
      const pid = Number.parseInt(content.right.trim(), 10);
      return Number.isInteger(pid) ? pid : null;
    }
  );

const runSentinel = <A, E, R>(
  file: string,
  holder: JsonObject,
  body: (running: JsonObject | null) => Effect.Effect<A, E, R>
): Effect.Effect<A, E | PlayerError, R | Scope.Scope> =>
  Effect.gen(function* () {
    const sentinel = sentinelOf(file);
    const opened = yield* openMonitorFile(file, true);
    const claim = () =>
      Effect.orElseSucceed(
        Effect.gen(function* () {
          const guard = yield* fileSystem.open(sentinel, { flag: 'wx', mode: 0o600 });
          return yield* Effect.onError(
            Effect.as(guard.writeAll(new TextEncoder().encode(`${process.pid}\n`)), true),
            () => Effect.ignore(fileSystem.remove(sentinel, { force: true }))
          );
        }),
        () => false
      );
    const acquired = yield* Effect.flatMap(claim(), (claimed) =>
      claimed
        ? Effect.succeed(true)
        : Effect.flatMap(sentinelPid(sentinel), (pid) =>
            pid !== null && !isLive(pid)
              ? Effect.zipRight(Effect.ignore(fileSystem.remove(sentinel)), claim())
              : Effect.succeed(false)
          )
    );
    if (!acquired) return yield* body(yield* readMonitorHolder(opened));
    return yield* Effect.ensuring(
      Effect.zipRight(writeHolder(opened, holder), body(null)),
      Effect.zipRight(
        clearHolder(opened),
        Effect.ignore(fileSystem.remove(sentinel, { force: true }))
      )
    );
  });

/** Hold the monitor singleton lock, or report the recorded current holder. */
export const withMonitorLock = <A, E, R>(
  sessionPath: string,
  holder: JsonObject,
  body: (running: JsonObject | null) => Effect.Effect<A, E, R>
): Effect.Effect<A, E | PlayerError, R | PrivateFs> =>
  Effect.gen(function* () {
    const files = yield* PrivateFs;
    const file = yield* lockFile(files, sessionPath, true);
    const flock = flockBinding();
    return yield* Effect.scoped(
      flock === null
        ? runSentinel(file, holder, body)
        : runNative(flock, file, holder, body)
    );
  });

/** Report the running monitor, or `null` when the lock is free. */
export const monitorHolder = (
  sessionPath: string
): Effect.Effect<JsonObject | null, never, PrivateFs> =>
  Effect.gen(function* () {
    const files = yield* PrivateFs;
    const resolved = yield* Effect.either(lockFile(files, sessionPath, false));
    if (Either.isLeft(resolved)) return null;
    return yield* Effect.orElseSucceed(
      Effect.scoped(
        Effect.gen(function* () {
          const opened = yield* openMonitorFile(resolved.right, false);
          const flock = flockBinding();
          if (flock === null) {
            const pid = yield* sentinelPid(sentinelOf(resolved.right));
            return pid === null || !isLive(pid) ? null : yield* readMonitorHolder(opened);
          }
          const descriptor = Number(opened.fd);
          if (flock(descriptor, LOCK_SH | LOCK_NB) !== 0) {
            return yield* readMonitorHolder(opened);
          }
          flock(descriptor, LOCK_UN);
          return null;
        })
      ),
      () => null
    );
  });
