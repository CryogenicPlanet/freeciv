/**
 * The private-state sandbox.
 *
 * These are the port's security tests: containment, symlink refusal, mode 0600
 * and atomic replacement.  `test_client.py`'s `AtomicityTests` and `LeakTests`
 * are the Python originals — a partial file must never be observable and a
 * world-readable one must never be accepted.
 */
import { afterEach, describe, expect, test } from 'bun:test';
import { Effect, Either } from 'effect';
import { workspacePaths } from 'src/services/private-fs';
import { scratchWorkspace, type Scratch } from 'test/_fixtures';
import { effectTest } from 'test/_effect-test';
import { path, withTestFileSystem } from 'test/_test-platform';

const scratches: Scratch[] = [];

const fresh = (): Scratch => {
  const scratch = scratchWorkspace();
  scratches.push(scratch);
  return scratch;
};

afterEach(() => Promise.all(scratches.splice(0).map((scratch) => scratch.cleanup())));

const run = <A, E>(effect: Effect.Effect<A, E>): Either.Either<A, E> =>
  Effect.runSync(Effect.either(effect));

const message = <A>(either: Either.Either<A, { readonly message: string }>): string => {
  expect(Either.isLeft(either)).toBe(true);
  return Either.isLeft(either) ? either.left.message : '';
};

describe('containment', () => {
  test('a path outside the state root is refused', () => {
    const { files } = fresh();
    expect(message(run(files.resolve('/etc/passwd')))).toBe(
      'private state files must stay inside PLAY_STATE_DIR'
    );
  });

  test('a traversal out of the state root is refused after normalization', () => {
    const { files, workspace } = fresh();
    const target = path.join(workspace.stateRoot, '..', '..', 'escaped.json');
    expect(message(run(files.resolve(target)))).toBe(
      'private state files must stay inside PLAY_STATE_DIR'
    );
  });

  test('the state root itself is not a writable state file', () => {
    const { files, workspace } = fresh();
    expect(message(run(files.resolve(workspace.stateRoot)))).toBe('private state path is invalid');
  });

  test('PLAY_STATE_DIR outside the workspace is refused at construction', () => {
    const { workspace } = fresh();
    const either = run(
      workspacePaths({ PLAY_ROOT: workspace.root, PLAY_STATE_DIR: '/tmp' }, workspace.root)
    );
    expect(message(either)).toBe('PLAY_STATE_DIR must stay inside the player workspace');
  });
});

describe('writes', () => {
  effectTest('a written file is mode 0600 and leaves no temp file behind', () =>
    withTestFileSystem((platformFiles) =>
      Effect.gen(function* () {
        const { files, workspace } = fresh();
        const target = path.join(workspace.stateRoot, 'game_abc', 'seat.json');
        yield* files.writeJson(target, { a: 1 });
        expect((yield* platformFiles.stat(target)).mode & 0o777).toBe(0o600);
        expect(yield* platformFiles.readDirectory(path.dirname(target))).toEqual(['seat.json']);
      }).pipe(Effect.orDie)
    )
  );

  test('writeJson round-trips through loadObject', () => {
    const { files, workspace } = fresh();
    const target = path.join(workspace.stateRoot, 'seat.json');
    Effect.runSync(files.writeJson(target, { b: 2, a: [1] }));
    expect(run(files.loadObject(target, 'session'))).toEqual(Either.right({ b: 2, a: [1] }));
  });

  effectTest('writeJson emits the indent=2 sorted shape with a trailing newline', () =>
    withTestFileSystem((platformFiles) =>
      Effect.gen(function* () {
        const { files, workspace } = fresh();
        const target = path.join(workspace.stateRoot, 'seat.json');
        yield* files.writeJson(target, { b: 2, a: 1 });
        expect(yield* platformFiles.readFileString(target)).toBe('{\n  "a": 1,\n  "b": 2\n}\n');
      }).pipe(Effect.orDie)
    )
  );

  effectTest('a replaced file is never observed half-written', () =>
    withTestFileSystem((platformFiles) =>
      Effect.gen(function* () {
        const { files, workspace } = fresh();
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
        const { files, workspace } = fresh();
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
        const { files, workspace } = fresh();
        const target = path.join(workspace.stateRoot, 'seat.json');
        yield* files.writeText(target, '{}');
        yield* platformFiles.chmod(target, 0o644);
        expect(message(yield* Effect.either(files.readText(target, 'session')))).toBe(
          'private session must be a mode-0600 file'
        );
      }).pipe(Effect.orDie)
    )
  );

  test('invalid JSON names the label, not the parser', () => {
    const { files, workspace } = fresh();
    const target = path.join(workspace.stateRoot, 'seat.json');
    Effect.runSync(files.writeText(target, 'not json'));
    expect(message(run(files.loadObject(target, 'session')))).toBe(
      'cannot read session: invalid JSON'
    );
  });

  test('a JSON array is not a state object', () => {
    const { files, workspace } = fresh();
    const target = path.join(workspace.stateRoot, 'seat.json');
    Effect.runSync(files.writeText(target, '[1]'));
    expect(message(run(files.loadObject(target, 'session')))).toBe(
      'session must contain a JSON object'
    );
  });
});

describe('symlinks', () => {
  effectTest('a symlinked directory component is refused', () =>
    withTestFileSystem((platformFiles) =>
      Effect.gen(function* () {
        const { files, workspace } = fresh();
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
        const { files, workspace } = fresh();
        yield* platformFiles.makeDirectory(path.join(workspace.stateRoot, 'game_abc'), {
          mode: 0o700,
        });
        const target = path.join(workspace.stateRoot, 'game_abc', 'seat.json');
        yield* files.writeText(target, '{}');
      }).pipe(Effect.orDie)
    )
  );

  test('reading a directory that does not exist says so', () => {
    const { files, workspace } = fresh();
    const target = path.join(workspace.stateRoot, 'missing', 'seat.json');
    expect(message(run(files.readText(target, 'session')))).toBe(
      'private state directory does not exist'
    );
  });
});
