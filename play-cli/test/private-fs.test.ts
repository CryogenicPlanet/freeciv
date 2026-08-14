/**
 * The private-state sandbox.
 *
 * These are the port's security tests: containment, symlink refusal, mode 0600
 * and atomic replacement.  `test_client.py`'s `AtomicityTests` and `LeakTests`
 * are the Python originals — a partial file must never be observable and a
 * world-readable one must never be accepted.
 */
import { afterEach, describe, expect } from 'bun:test';
import { Effect, Either } from 'effect';
import { workspacePaths } from 'src/services/private-fs';
import { scratchWorkspace, type Scratch } from 'test/_fixtures';
import { effectTest } from 'test/_effect-test';
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

const run = <A, E>(effect: Effect.Effect<A, E>): Effect.Effect<Either.Either<A, E>> =>
  Effect.either(effect);

const message = <A>(either: Either.Either<A, { readonly message: string }>): string => {
  expect(Either.isLeft(either)).toBe(true);
  return Either.isLeft(either) ? either.left.message : '';
};

describe('containment', () => {
  effectTest('a path outside the state root is refused', () =>
    Effect.gen(function* () {
      const { files } = yield* fresh();
      expect(message(yield* run(files.resolve('/etc/passwd')))).toBe(
        'private state files must stay inside PLAY_STATE_DIR'
      );
    })
  );

  effectTest('a traversal out of the state root is refused after normalization', () =>
    Effect.gen(function* () {
      const { files, workspace } = yield* fresh();
      const target = path.join(workspace.stateRoot, '..', '..', 'escaped.json');
      expect(message(yield* run(files.resolve(target)))).toBe(
        'private state files must stay inside PLAY_STATE_DIR'
      );
    })
  );

  effectTest('the state root itself is not a writable state file', () =>
    Effect.gen(function* () {
      const { files, workspace } = yield* fresh();
      expect(message(yield* run(files.resolve(workspace.stateRoot)))).toBe(
        'private state path is invalid'
      );
    })
  );

  effectTest('PLAY_STATE_DIR outside the workspace is refused at construction', () =>
    Effect.gen(function* () {
      const { workspace } = yield* fresh();
      const either = yield* run(
        workspacePaths({ PLAY_ROOT: workspace.root, PLAY_STATE_DIR: '/tmp' }, workspace.root)
      );
      expect(message(either)).toBe('PLAY_STATE_DIR must stay inside the player workspace');
    })
  );
});

describe('writes', () => {
  effectTest('a written file is mode 0600 and leaves no temp file behind', () =>
    withTestFileSystem((platformFiles) =>
      Effect.gen(function* () {
        const { files, workspace } = yield* fresh();
        const target = path.join(workspace.stateRoot, 'game_abc', 'seat.json');
        yield* files.writeJson(target, { a: 1 });
        expect((yield* platformFiles.stat(target)).mode & 0o777).toBe(0o600);
        expect(yield* platformFiles.readDirectory(path.dirname(target))).toEqual(['seat.json']);
      }).pipe(Effect.orDie)
    )
  );

  effectTest('writeJson round-trips through loadObject', () =>
    Effect.gen(function* () {
      const { files, workspace } = yield* fresh();
      const target = path.join(workspace.stateRoot, 'seat.json');
      yield* files.writeJson(target, { b: 2, a: [1] });
      expect(yield* run(files.loadObject(target, 'session'))).toEqual(
        Either.right({ b: 2, a: [1] })
      );
    }).pipe(Effect.orDie)
  );

  effectTest('writeJson emits the indent=2 sorted shape with a trailing newline', () =>
    withTestFileSystem((platformFiles) =>
      Effect.gen(function* () {
        const { files, workspace } = yield* fresh();
        const target = path.join(workspace.stateRoot, 'seat.json');
        yield* files.writeJson(target, { b: 2, a: 1 });
        expect(yield* platformFiles.readFileString(target)).toBe('{\n  "a": 1,\n  "b": 2\n}\n');
      }).pipe(Effect.orDie)
    )
  );

  effectTest('a replaced file is never observed half-written', () =>
    withTestFileSystem((platformFiles) =>
      Effect.gen(function* () {
        const { files, workspace } = yield* fresh();
        const target = path.join(workspace.stateRoot, 'seat.json');
        yield* files.writeText(target, 'first');
        const before = yield* platformFiles.readFileString(target);
        yield* files.writeText(target, 'second-and-longer');
        expect(before).toBe('first');
        expect(yield* platformFiles.readFileString(target)).toBe('second-and-longer');
      }).pipe(Effect.orDie)
    )
  );

  effectTest('appendText adds without rewriting, as a log must', () =>
    withTestFileSystem((platformFiles) =>
      Effect.gen(function* () {
        const { files, workspace } = yield* fresh();
        const target = path.join(workspace.stateRoot, 'receipts.log');
        yield* files.appendText(target, 'one\n');
        yield* files.appendText(target, 'two\n');
        expect(yield* platformFiles.readFileString(target)).toBe('one\ntwo\n');
        expect((yield* platformFiles.stat(target)).mode & 0o777).toBe(0o600);
      }).pipe(Effect.orDie)
    )
  );
});

describe('reads', () => {
  effectTest('a world-readable state file is refused', () =>
    withTestFileSystem((platformFiles) =>
      Effect.gen(function* () {
        const { files, workspace } = yield* fresh();
        const target = path.join(workspace.stateRoot, 'seat.json');
        yield* files.writeText(target, '{}');
        yield* platformFiles.chmod(target, 0o644);
        expect(message(yield* Effect.either(files.readText(target, 'session')))).toBe(
          'private session must be a mode-0600 file'
        );
      }).pipe(Effect.orDie)
    )
  );

  effectTest('invalid JSON names the label, not the parser', () =>
    Effect.gen(function* () {
      const { files, workspace } = yield* fresh();
      const target = path.join(workspace.stateRoot, 'seat.json');
      yield* files.writeText(target, 'not json');
      expect(message(yield* run(files.loadObject(target, 'session')))).toBe(
        'cannot read session: invalid JSON'
      );
    }).pipe(Effect.orDie)
  );

  effectTest('a JSON array is not a state object', () =>
    Effect.gen(function* () {
      const { files, workspace } = yield* fresh();
      const target = path.join(workspace.stateRoot, 'seat.json');
      yield* files.writeText(target, '[1]');
      expect(message(yield* run(files.loadObject(target, 'session')))).toBe(
        'session must contain a JSON object'
      );
    }).pipe(Effect.orDie)
  );
});

describe('symlinks', () => {
  effectTest('a symlinked directory component is refused', () =>
    withTestFileSystem((platformFiles) =>
      Effect.gen(function* () {
        const { files, workspace } = yield* fresh();
        const outside = yield* platformFiles.makeTempDirectory({
          directory: workspace.root,
          prefix: 'outside-',
        });
        yield* platformFiles.symlink(outside, path.join(workspace.stateRoot, 'game_link'));
        const target = path.join(workspace.stateRoot, 'game_link', 'seat.json');
        expect(message(yield* Effect.either(files.writeText(target, '{}')))).toBe(
          'private state directories must be real directories inside PLAY_STATE_DIR'
        );
      }).pipe(Effect.orDie)
    )
  );

  effectTest('an existing directory is walked without complaint', () =>
    withTestFileSystem((platformFiles) =>
      Effect.gen(function* () {
        const { files, workspace } = yield* fresh();
        yield* platformFiles.makeDirectory(path.join(workspace.stateRoot, 'game_abc'), {
          mode: 0o700,
        });
        const target = path.join(workspace.stateRoot, 'game_abc', 'seat.json');
        yield* files.writeText(target, '{}');
      }).pipe(Effect.orDie)
    )
  );

  effectTest('reading a directory that does not exist says so', () =>
    Effect.gen(function* () {
      const { files, workspace } = yield* fresh();
      const target = path.join(workspace.stateRoot, 'missing', 'seat.json');
      expect(message(yield* run(files.readText(target, 'session')))).toBe(
        'private state directory does not exist'
      );
    })
  );
});
