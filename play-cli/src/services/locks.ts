/**
 * Advisory locks.
 *
 * Ports `_private_advisory_lock` (client.py:531-595), `_v2_state_lock_path` /
 * `_v2_request_lock_path` (580-589) and `_v2_state_lock` / `_v2_request_lock`
 * (714-726).  The monitor's singleton lock is U17's; it is built on
 * {@link withAdvisoryLock}'s primitives but has its own holder-record contract.
 *
 * Two kernel properties are the whole point and are why this is `flock(2)` and
 * not a PID file:
 *
 * - idempotency by construction — a second holder simply cannot acquire;
 * - crash recovery by construction — the kernel releases on process death, so a
 *   `kill -9` leaves nothing stale behind.
 *
 * Neither Node nor Bun exposes `flock`, so it is bound through `bun:ffi`
 * against libc.  If that binding cannot be made (an unexpected platform), the
 * fallback below keeps mutual exclusion via an `O_EXCL` sentinel and pays for
 * it with a liveness probe — the exact bookkeeping the Python was avoiding, and
 * a divergence recorded in NOTES.md §4.
 */
import type * as Scope from 'effect/Scope';
import { fileSystem, path } from 'src/services/platform';
import { dlopen, FFIType } from 'bun:ffi';
import { Clock, Effect, Either } from 'effect';
import { LockTimeoutError, type PlayerError, playerError, attemptOr } from 'src/errors';
import { V2_REQUEST_LOCK_TIMEOUT_S, V2_STATE_LOCK_TIMEOUT_S } from 'src/constants';
import { PrivateFs, type PrivateFsApi } from 'src/services/private-fs';

export const LOCK_EX = 2;
export const LOCK_UN = 8;
export const LOCK_NB = 4;

export const LOCK_BUSY_MESSAGE =
  'another player command is updating this session; retry once after it finishes';

type FlockFn = (descriptor: number, operation: number) => number;

const LIBC_CANDIDATES = ['libc.dylib', 'libc.so.6', 'libc.so', 'libSystem.dylib'] as const;

interface FlockCache {
  resolved: boolean;
  value: FlockFn | null;
}

/** Bind `flock(2)` once per process; `null` means "no binding on this platform". */
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
        (): ReadonlyArray<FlockFn> => [] /* this libc spelling is absent here */
      )
    );
    cache.value = bound[0] ?? null;
    if (cache.value !== null) return cache.value;
    return cache.value;
  };
})();

/** Report whether the real `flock(2)` is available. Tests assert this is true. */
export const hasNativeFlock = (): boolean => flockBinding() !== null;

const POLL_MILLIS = 50;

const refuseSymbolicLink = (target: string): Effect.Effect<void, PlayerError> =>
  Effect.flatMap(Effect.either(fileSystem.readLink(target)), (link) =>
    Either.isRight(link)
      ? Effect.fail(playerError('private state lock must be a mode-0600 file'))
      : Effect.void
  );

const runNative = <A, E, R>(
  flock: FlockFn,
  file: string,
  timeoutS: number,
  body: Effect.Effect<A, E, R>
): Effect.Effect<A, E | PlayerError | LockTimeoutError, R | Scope.Scope> =>
  Effect.gen(function* () {
    yield* refuseSymbolicLink(file);
    const existing = yield* Effect.either(fileSystem.stat(file));
    if (Either.isLeft(existing)) {
      yield* Effect.ignore(
        fileSystem.writeFile(file, new Uint8Array(), { flag: 'wx', mode: 0o600 })
      );
    }
    yield* refuseSymbolicLink(file);
    const opened = yield* Effect.mapError(
      fileSystem.open(file, { flag: 'r+' }),
      () => playerError('cannot safely lock private player state')
    );
    const info = yield* Effect.mapError(
      opened.stat,
      () => playerError('cannot safely lock private player state')
    );
    if (info.type !== 'File' || (info.mode & 0o777) !== 0o600) {
      return yield* playerError('private state lock must be a mode-0600 file');
    }
    const descriptor = Number(opened.fd);
    const deadline = (yield* Clock.currentTimeMillis) + timeoutS * 1000;
    const poll = (): Effect.Effect<void, LockTimeoutError> =>
      Effect.gen(function* () {
        if (flock(descriptor, LOCK_EX | LOCK_NB) === 0) return undefined;
        if ((yield* Clock.currentTimeMillis) >= deadline) {
          return yield* new LockTimeoutError({ message: LOCK_BUSY_MESSAGE, path: file });
        }
        yield* Effect.sleep(POLL_MILLIS);
        yield* poll();
        return undefined;
      });
    yield* poll();
    return yield* Effect.ensuring(
      body,
      Effect.sync(() => {
        attemptOr(
          () => flock(descriptor, LOCK_UN),
          () => 0 /* the scoped file close releases the kernel lock too */
        );
      })
    );
  });

const runSentinel = <A, E, R>(
  file: string,
  timeoutS: number,
  body: Effect.Effect<A, E, R>
): Effect.Effect<A, E | LockTimeoutError, R | Scope.Scope> =>
  Effect.gen(function* () {
    const sentinel = `${file}.held`;
    const deadline = (yield* Clock.currentTimeMillis) + timeoutS * 1000;
    const claim = () =>
      Effect.orElseSucceed(
        Effect.gen(function* () {
          const opened = yield* fileSystem.open(sentinel, { flag: 'wx', mode: 0o600 });
          return yield* Effect.onError(
            Effect.as(opened.writeAll(new TextEncoder().encode(`${process.pid}\n`)), opened),
            () => Effect.ignore(fileSystem.remove(sentinel, { force: true }))
          );
        }),
        () => null
      );
    const reapedStale = (): Effect.Effect<boolean> =>
      Effect.gen(function* () {
        const content = yield* Effect.either(fileSystem.readFileString(sentinel));
        if (Either.isLeft(content)) return false;
        const holder = Number.parseInt(content.right.trim(), 10);
        if (!Number.isInteger(holder)) return false;
        const alive = attemptOr(
          () => {
            process.kill(holder, 0);
            return true;
          },
          () => false
        );
        if (alive) return false;
        return Either.isRight(yield* Effect.either(fileSystem.remove(sentinel)));
      });
    const poll = (): Effect.Effect<void, LockTimeoutError, Scope.Scope> =>
      Effect.gen(function* () {
        const held = yield* claim();
        if (held !== null) return undefined;
        if (yield* reapedStale()) {
          yield* poll();
          return undefined;
        }
        if ((yield* Clock.currentTimeMillis) >= deadline) {
          return yield* new LockTimeoutError({ message: LOCK_BUSY_MESSAGE, path: file });
        }
        yield* Effect.sleep(POLL_MILLIS);
        yield* poll();
        return undefined;
      });
    yield* poll();
    return yield* Effect.ensuring(
      body,
      Effect.ignore(fileSystem.remove(sentinel, { force: true }))
    );
  });

/**
 * Hold `file` exclusively for the duration of `body`.
 *
 * The lock file is created inside the private-state sandbox (mode 0600, no
 * symlinked component), and is released whatever `body` does — including
 * interruption, which is what makes `monitor` and `do` safe to Ctrl-C.
 */
export const withAdvisoryLock = <A, E, R>(
  target: string,
  timeoutS: number,
  body: Effect.Effect<A, E, R>
): Effect.Effect<A, E | PlayerError | LockTimeoutError, R | PrivateFs> =>
  Effect.gen(function* () {
    const files = yield* PrivateFs;
    const { relative } = yield* files.resolve(target);
    const parent = yield* files.openDirectory(relative.slice(0, -1), { create: true });
    const leaf = relative.at(-1);
    if (leaf === undefined) return yield* playerError('private state lock path is invalid');
    const file = path.join(parent, leaf);
    const flock = flockBinding();
    return yield* Effect.scoped(
      flock === null
        ? runSentinel(file, timeoutS, body)
        : runNative(flock, file, timeoutS, body)
    );
  });

// ---------------------------------------------------------------------------
// The two named session locks
// ---------------------------------------------------------------------------

/** `pathlib.Path.with_suffix`: replace the final extension, or append one. */
export const withSuffix = (target: string, suffix: string): string => {
  const parsed = path.parse(target);
  return path.join(parsed.dir, parsed.name + suffix);
};

/** `_v2_state_path` — the `.v2-state` sibling of a session file. */
export const v2StatePath = (sessionPath: string): string => withSuffix(sessionPath, '.v2-state');

export const v2StateLockPath = (sessionPath: string): string => `${v2StatePath(sessionPath)}.lock`;

export const v2RequestLockPath = (sessionPath: string): string =>
  withSuffix(sessionPath, '.v2-request.lock');

export const monitorLockPath = (sessionPath: string): string =>
  withSuffix(sessionPath, '.monitor.lock');

export const withV2StateLock = <A, E, R>(
  sessionPath: string,
  body: Effect.Effect<A, E, R>
): Effect.Effect<A, E | PlayerError | LockTimeoutError, R | PrivateFs> =>
  withAdvisoryLock(v2StateLockPath(sessionPath), V2_STATE_LOCK_TIMEOUT_S, body);

export const withV2RequestLock = <A, E, R>(
  sessionPath: string,
  body: Effect.Effect<A, E, R>
): Effect.Effect<A, E | PlayerError | LockTimeoutError, R | PrivateFs> =>
  withAdvisoryLock(v2RequestLockPath(sessionPath), V2_REQUEST_LOCK_TIMEOUT_S, body);

/** Escape hatch for U17, whose lock file doubles as the holder record. */
export const lockFilePath = (
  files: PrivateFsApi,
  target: string
): Effect.Effect<string, PlayerError> =>
  Effect.gen(function* () {
    const { relative } = yield* files.resolve(target);
    const parent = yield* files.openDirectory(relative.slice(0, -1), { create: true });
    const leaf = relative.at(-1);
    if (leaf === undefined) return yield* playerError('private state lock path is invalid');
    return path.join(parent, leaf);
  });
