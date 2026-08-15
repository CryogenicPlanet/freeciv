/**
 * Advisory locks.
 *
 * The mechanism matters as much as the behaviour: `flock(2)` gives idempotency
 * and crash recovery for free, and the first test fails loudly if the binding
 * silently degraded to the sentinel fallback on this platform.
 */
import { afterEach, describe, expect, test } from 'bun:test';
import { Effect, Either, Layer } from 'effect';
import { PrivateFs } from 'src/services/private-fs';
import {
  hasNativeFlock,
  monitorLockPath,
  v2RequestLockPath,
  v2StateLockPath,
  v2StatePath,
  withAdvisoryLock,
  withSuffix,
} from 'src/services/locks';
import { scratchWorkspace, type Scratch } from 'test/_fixtures';
import { effectTest, provideTestLayer } from 'test/_effect-test';
import { path, withTestFileSystem } from 'test/_test-platform';

const scratches: Scratch[] = [];

const fresh = (): Effect.Effect<Scratch> =>
  Effect.tap(scratchWorkspace(), (scratch) =>
    Effect.sync(() => {
      scratches.push(scratch);
    })
  );

afterEach(() =>
  Effect.runPromise(
    Effect.forEach(scratches.splice(0), (scratch) => scratch.cleanup, { discard: true })
  )
);

describe('path derivation', () => {
  test('withSuffix mirrors pathlib.Path.with_suffix', () => {
    expect(withSuffix('/a/b/seat.json', '.v2-state')).toBe('/a/b/seat.v2-state');
    expect(withSuffix('/a/b/seat', '.v2-state')).toBe('/a/b/seat.v2-state');
    expect(withSuffix('/a.b/seat.json', '.lock')).toBe('/a.b/seat.lock');
  });

  test('the four session-derived paths are siblings of the session file', () => {
    expect(v2StatePath('/w/.sessions/g/seat.json')).toBe('/w/.sessions/g/seat.v2-state');
    expect(v2StateLockPath('/w/.sessions/g/seat.json')).toBe('/w/.sessions/g/seat.v2-state.lock');
    expect(v2RequestLockPath('/w/.sessions/g/seat.json')).toBe(
      '/w/.sessions/g/seat.v2-request.lock'
    );
    expect(monitorLockPath('/w/.sessions/g/seat.json')).toBe('/w/.sessions/g/seat.monitor.lock');
  });
});

describe('holding', () => {
  test('flock(2) is bound, so the lock is kernel-backed rather than a PID file', () => {
    expect(hasNativeFlock()).toBe(true);
  });

  effectTest('the body runs and the lock file is left mode 0600', () =>
    withTestFileSystem((files) =>
      Effect.gen(function* () {
        const scratch = yield* fresh();
        const target = path.join(scratch.workspace.stateRoot, 'seat.v2-state.lock');
        const result = yield* provideTestLayer(
          withAdvisoryLock(target, 1, Effect.succeed('done')),
          Layer.succeed(PrivateFs, scratch.files)
        );
        expect(result).toBe('done');
        expect((yield* files.stat(target)).mode & 0o777).toBe(0o600);
      }).pipe(Effect.orDie)
    )
  );

  effectTest('the lock is released even when the body fails', () =>
    Effect.gen(function* () {
      const scratch = yield* fresh();
      const target = path.join(scratch.workspace.stateRoot, 'seat.v2-state.lock');
      const provided = Layer.succeed(PrivateFs, scratch.files);
      const first = yield* Effect.either(
        provideTestLayer(withAdvisoryLock(target, 1, Effect.fail('boom')), provided)
      );
      expect(Either.isLeft(first)).toBe(true);
      // If the release had leaked, this second acquisition would time out.
      const second = yield* provideTestLayer(
        withAdvisoryLock(target, 1, Effect.succeed(2)),
        provided
      );
      expect(second).toBe(2);
    }).pipe(Effect.orDie)
  );

  effectTest('a lock outside PLAY_STATE_DIR is refused before any file is opened', () =>
    Effect.gen(function* () {
      const scratch = yield* fresh();
      const either = yield* Effect.either(
        provideTestLayer(
          withAdvisoryLock('/etc/play.lock', 1, Effect.succeed(0)),
          Layer.succeed(PrivateFs, scratch.files)
        )
      );
    expect(Either.isLeft(either)).toBe(true);
    if (Either.isLeft(either)) {
      expect(either.left).toMatchObject({
        message: 'private state files must stay inside PLAY_STATE_DIR',
      });
    }
    })
  );
});
