/** Ready-file ownership, permissions, locking, atomicity, and cleanup coverage. */
import { describe, expect, test } from 'bun:test';
import { Data, Effect, Either, Exit, Fiber, Option, Ref, type Scope } from 'effect';
import {
  existsSync,
  mkdtempSync,
  readFileSync,
  readdirSync,
  rmSync,
  statSync,
  writeFileSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { CANON_UTF8, type CanonRecord, canonicalText } from '@arena/wire';
import {
  EWOULDBLOCK,
  O_CLOEXEC,
  READY_FILE_MODE,
  READY_LOCK_SUFFIX,
  ReadyFile,
  ReadyFileDisabled,
  ReadyFileEncodeError,
  ReadyFileLocked,
  type ReadyLineSink,
  type ReadyPublication,
  layer,
  lockSupported,
  openLockFd,
  readyFileText,
  readyLineText,
  resolveReadyPath,
} from 'src/gateway/services/ready-file';

// ---------------------------------------------------------------------------
// Rig
// ---------------------------------------------------------------------------

class RigError extends Data.TaggedError('RigError')<{
  readonly reason: string;
  readonly detail: string;
}> {}

const run = <A, E>(effect: Effect.Effect<A, E, Scope.Scope>): Promise<A> =>
  Effect.runPromise(Effect.scoped(effect));

/** The checkout, so `python3 -c "import agent_eval…"` resolves. */
const REPO_ROOT = join(import.meta.dir, '..', '..', '..', '..');

/** A private directory that is deleted with the scope that made it. */
const scratchDirectory: Effect.Effect<string, RigError, Scope.Scope> = Effect.acquireRelease(
  Effect.try({
    try: () => mkdtempSync(join(tmpdir(), 'ready-file-')),
    catch: (cause) => new RigError({ reason: 'mkdtemp failed', detail: String(cause) }),
  }),
  (directory) =>
    Effect.ignore(Effect.try(() => rmSync(directory, { recursive: true, force: true }))),
);

/** Poll rather than sleep-and-hope: the file, not a duration, is the signal. */
const waitUntil = (
  probe: Effect.Effect<boolean>,
  what: string,
  remaining = 400,
): Effect.Effect<void, RigError> =>
  Effect.flatMap(probe, (ready) =>
    ready
      ? Effect.void
      : remaining <= 0
        ? Effect.fail(new RigError({ reason: 'timed out waiting', detail: what }))
        : Effect.zipRight(Effect.sleep('5 millis'), waitUntil(probe, what, remaining - 1)),
  );

/** A sink that keeps every line, so the stdout handshake is assertable. */
interface CapturedSink {
  readonly sink: ReadyLineSink;
  readonly lines: Effect.Effect<readonly string[]>;
}

const capturedSink: Effect.Effect<CapturedSink> = Effect.map(
  Ref.make<readonly string[]>([]),
  (recorded) => ({
    sink: (line) => Ref.update(recorded, (lines) => [...lines, line]),
    lines: Ref.get(recorded),
  }),
);

/** The two fields the cleanup guard compares (`_remove_owned_ready_file`, `:2101-2111`). */
const OUR_IDENTITY = 'a1b2c3d4e5f60718293a';
const OUR_PID = BigInt(process.pid);

/** `identity_payload()` (`:1301-1325`) with a port the kernel already handed out. */
const identityPayload = (overrides: CanonRecord = {}): CanonRecord => ({
  schema_version: 1n,
  ok: true,
  kind: 'freeciv-replay-gateway',
  protocol_version: 1n,
  identity: OUR_IDENTITY,
  pid: OUR_PID,
  host: '127.0.0.1',
  port: 51234n,
  url: 'http://127.0.0.1:51234',
  repo_root: '/checkout/freeciv',
  upstream_service_url: 'http://127.0.0.1:8080',
  runs_root: '/checkout/freeciv/runs',
  cache_root: '/checkout/.cache',
  ...overrides,
});

const readyFileService = (
  path: string,
  sink: ReadyLineSink,
): Effect.Effect<typeof ReadyFile.Service> =>
  Effect.provide(ReadyFile, layer({ path, sink }));

const publishInScope = (
  path: string,
  payload: CanonRecord,
  sink: ReadyLineSink,
): Effect.Effect<ReadyPublication, never, Scope.Scope> =>
  Effect.flatMap(readyFileService(path, sink), (service) =>
    Effect.orDie(service.publish(payload)),
  );

const modeOf = (path: string): number => statSync(path).mode & 0o777;

const textOf = (path: string): string => readFileSync(path, 'utf8');

const fileText = (payload: CanonRecord): string => Either.getOrThrow(readyFileText(payload));

const lineText = (payload: CanonRecord): string => Either.getOrThrow(readyLineText(payload));

// ---------------------------------------------------------------------------
// The python3 oracle
// ---------------------------------------------------------------------------

const PAYLOAD_VARIABLE = 'READY_PAYLOAD_JSON';
const TARGET_VARIABLE = 'READY_TARGET_PATH';
const LOCK_VARIABLE = 'READY_LOCK_PATH';

const childEnvironment = (
  variables: Readonly<Record<string, string>>,
): NodeJS.ProcessEnv => ({ ...process.env, ...variables });

const pythonRun = (
  source: string,
  variables: Readonly<Record<string, string>>,
): Effect.Effect<string, RigError> =>
  Effect.flatMap(
    Effect.sync(() =>
      Bun.spawnSync(['python3', '-c', source], {
        cwd: REPO_ROOT,
        stdout: 'pipe',
        stderr: 'pipe',
        env: childEnvironment(variables),
      }),
    ),
    (result) =>
      result.exitCode === 0
        ? Effect.succeed(result.stdout.toString().trim())
        : Effect.fail(
            new RigError({
              reason: 'python3 exited non-zero',
              detail: result.stderr.toString().trim(),
            }),
          ),
  );

/**
 * The oracle is the shipping code, not a re-implementation: `_canonical` and
 * `_write_private_json` are imported straight out of
 * `agent_eval.replay_gateway` and handed the same payload this test just
 * encoded. `json.loads` preserves the int/float split (`1` -> `int`, `1.0` ->
 * `float`), which is exactly the distinction the canonical text carries, so
 * the round trip through the environment does not launder the one thing being
 * measured.
 */
const PYTHON_WRITER = `
import json, os
from agent_eval.replay_gateway import _canonical, _write_private_json

value = json.loads(os.environ["${PAYLOAD_VARIABLE}"])
_write_private_json(os.environ["${TARGET_VARIABLE}"], value)
print(_canonical(value).decode("utf-8"))
`;

interface PythonWriterResult {
  readonly fileText: string;
  readonly line: string;
  readonly mode: number;
}

/** Run the Python writer on `payload` and report what it produced. */
const pythonWrite = (
  payload: CanonRecord,
  target: string,
): Effect.Effect<PythonWriterResult, RigError> =>
  Effect.flatMap(
    Either.match(canonicalText(payload, CANON_UTF8), {
      onLeft: (cause) =>
        Effect.fail(new RigError({ reason: 'payload is not canonical', detail: cause.message })),
      onRight: (text) => Effect.succeed(text),
    }),
    (encoded) =>
      Effect.flatMap(
        pythonRun(PYTHON_WRITER, {
          [PAYLOAD_VARIABLE]: encoded,
          [TARGET_VARIABLE]: target,
        }),
        (line) =>
          Effect.try({
            try: (): PythonWriterResult => ({
              fileText: textOf(target),
              line,
              mode: modeOf(target),
            }),
            catch: (cause) =>
              new RigError({
                reason: 'reading the python record failed',
                detail: String(cause),
              }),
          }),
      ),
  );

/** `fcntl.flock(LOCK_EX | LOCK_NB)`, one shot, reported either way. */
const PYTHON_ATTEMPT = `
import fcntl, os
fd = os.open(os.environ["${LOCK_VARIABLE}"], os.O_RDWR | os.O_CREAT, 0o600)
try:
    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
except BlockingIOError as exc:
    print("BLOCKED", exc.errno)
else:
    print("ACQUIRED")
    fcntl.flock(fd, fcntl.LOCK_UN)
`;

const pythonAttempt = (lockPath: string): Effect.Effect<string, RigError> =>
  pythonRun(PYTHON_ATTEMPT, { [LOCK_VARIABLE]: lockPath });

/**
 * `_acquire_ready_lock` in a live process: it takes the lock on `<ready>.lock`,
 * says so, and holds it until its stdin is nudged. Killed with the scope
 * either way, so a failed assertion cannot wedge a later test.
 */
const PYTHON_HOLDER = `
import os, sys
from pathlib import Path
from agent_eval.replay_gateway import _acquire_ready_lock, _release_ready_lock

fd = _acquire_ready_lock(Path(os.environ["${TARGET_VARIABLE}"]))
print("HELD", flush=True)
sys.stdin.readline()
_release_ready_lock(fd)
print("RELEASED", flush=True)
sys.stdin.readline()
`;

/** Just the part of a stream reader this file needs — Bun's and the DOM's differ. */
interface ChunkReader {
  readonly read: () => Promise<{
    readonly done?: boolean;
    readonly value?: Uint8Array | undefined;
  }>;
}

const readLine = (
  reader: ChunkReader,
  buffer: Ref.Ref<string>,
  decoder: TextDecoder,
): Effect.Effect<string, RigError> =>
  Effect.flatMap(Ref.get(buffer), (pending) => {
    const newline = pending.indexOf('\n');
    return newline >= 0
      ? Effect.as(Ref.set(buffer, pending.slice(newline + 1)), pending.slice(0, newline).trim())
      : Effect.flatMap(
          Effect.tryPromise({
            try: () => reader.read(),
            catch: (cause) =>
              new RigError({ reason: 'reading holder stdout failed', detail: String(cause) }),
          }),
          (chunk) =>
            chunk.done === true || chunk.value === undefined
              ? Effect.fail(new RigError({ reason: 'holder stdout closed early', detail: pending }))
              : Effect.flatMap(Ref.set(buffer, pending + decoder.decode(chunk.value)), () =>
                  readLine(reader, buffer, decoder),
                ),
        );
  });

interface PythonHolder {
  /** Unlock in Python and wait for it to confirm; the descriptor stays open. */
  readonly release: Effect.Effect<void, RigError>;
}

const pythonHolder = (readyPath: string): Effect.Effect<PythonHolder, RigError, Scope.Scope> =>
  Effect.flatMap(
    Effect.acquireRelease(
      Effect.sync(() =>
        Bun.spawn(['python3', '-u', '-c', PYTHON_HOLDER], {
          cwd: REPO_ROOT,
          stdin: 'pipe',
          stdout: 'pipe',
          stderr: 'pipe',
          env: childEnvironment({ [TARGET_VARIABLE]: readyPath }),
        }),
      ),
      (child) => Effect.ignore(Effect.sync(() => child.kill())),
    ),
    (child) =>
      Effect.flatMap(Ref.make(''), (buffer) => {
        const reader = child.stdout.getReader();
        const decoder = new TextDecoder();
        const awaitWord = (word: string): Effect.Effect<void, RigError> =>
          Effect.flatMap(
            Effect.timeoutFail(readLine(reader, buffer, decoder), {
              duration: '15 seconds',
              onTimeout: () =>
                new RigError({ reason: `timed out waiting for ${word}`, detail: readyPath }),
            }),
            (line) =>
              line === word
                ? Effect.void
                : Effect.fail(new RigError({ reason: `expected ${word}`, detail: line })),
          );
        // `write`/`flush` are sync-or-async depending on the sink; resolving
        // whatever they return keeps this one shape either way.
        const write = (
          operation: () => number | Promise<number>,
        ): Effect.Effect<number, RigError> =>
          Effect.tryPromise({
            try: () => Promise.resolve(operation()),
            catch: (cause) =>
              new RigError({ reason: 'writing to the holder failed', detail: String(cause) }),
          });
        const nudge = Effect.zipRight(
          write(() => child.stdin.write('go\n')),
          write(() => child.stdin.flush()),
        );
        return Effect.as(awaitWord('HELD'), {
          release: Effect.flatMap(nudge, () => awaitWord('RELEASED')),
        });
      }),
  );

// ---------------------------------------------------------------------------

describe('ready file: the bytes', () => {
  test('the file is pretty-sorted with a trailing newline, the line is canonical', () =>
    run(
      Effect.gen(function* () {
        const directory = yield* scratchDirectory;
        const path = join(directory, 'gateway.json');
        const captured = yield* capturedSink;
        const payload = identityPayload();

        const published = yield* publishInScope(path, payload, captured.sink);

        expect(Option.getOrThrow(published.path)).toBe(yield* resolveReadyPath(path));
        expect(textOf(path)).toBe(fileText(payload));
        expect(textOf(path).startsWith('{\n  "cache_root": "/checkout/.cache",\n')).toBe(true);
        expect(textOf(path).endsWith('\n}\n')).toBe(true);
        expect(published.line).toBe(lineText(payload));
        expect(yield* captured.lines).toEqual([published.line]);
      }),
    ));

  test('the two spellings differ exactly where CPython says they do', () => {
    // `ensure_ascii` is True in the file and False on stdout, and only the
    // file is indented.  A port that used one writer for both would pass
    // every Python assertion and still be wrong.
    const payload = identityPayload({ runs_root: '/runs/césar', nested: { a: 1n } });

    expect(fileText(payload)).toContain('"/runs/c\\u00e9sar"');
    expect(lineText(payload)).toContain('"/runs/césar"');
    expect(fileText(payload)).toContain('"nested": {\n    "a": 1\n  }');
    expect(lineText(payload)).toContain('"nested":{"a":1}');
  });

  test("empty containers, floats and astral keys follow CPython's pretty writer", () => {
    const payload: CanonRecord = {
      empty_object: {},
      empty_array: [],
      whole_float: 1.0,
      integer: 1n,
      array: [1n, { deep: [true, null] }],
      '\u{1d11e}': 'astral key sorts by code point',
      z: 'last',
    };

    expect(fileText(payload)).toContain('"empty_object": {}');
    expect(fileText(payload)).toContain('"empty_array": []');
    // `bigint` is a Python int, `number` is a Python float: 1 vs 1.0.
    expect(fileText(payload)).toContain('"integer": 1,');
    expect(fileText(payload)).toContain('"whole_float": 1.0');
    expect(fileText(payload)).toContain(
      '"array": [\n    1,\n    {\n      "deep": [\n        true,\n        null\n      ]\n    }\n  ]',
    );
    // sort_keys orders by code point, so the astral key follows "z".
    expect(fileText(payload).indexOf('"z"')).toBeLessThan(
      fileText(payload).indexOf('\\ud834\\udd1e'),
    );
  });

  test('a payload with no CPython spelling fails before anything is created', () =>
    run(
      Effect.gen(function* () {
        const directory = yield* scratchDirectory;
        const path = join(directory, 'gateway.json');
        const captured = yield* capturedSink;
        const service = yield* readyFileService(path, captured.sink);

        const failure = yield* Effect.flip(service.publish({ port: Number.NaN }));

        expect(failure).toBeInstanceOf(ReadyFileEncodeError);
        expect(existsSync(path)).toBe(false);
        expect(existsSync(`${path}${READY_LOCK_SUFFIX}`)).toBe(false);
        expect(yield* captured.lines).toEqual([]);
      }),
    ));
});

describe('ready file: differential against agent_eval.replay_gateway', () => {
  const cases: ReadonlyArray<readonly [string, CanonRecord]> = [
    ['the identity payload', identityPayload()],
    ['with viewer_public_url', identityPayload({ viewer_public_url: 'https://freeciv.localhost' })],
    [
      'non-ASCII, floats, nesting, empties and control characters',
      identityPayload({
        runs_root: '/runs/césar/\u{1d11e}',
        timing: { action_timeout_s: 7.5, mode: 'custom' },
        empty_object: {},
        empty_array: [],
        reasons: ['a', 'b'],
        // DEL is the interesting one: `ensure_ascii=True` escapes it in the
        // file (CPython's fast path stops at `~`) and leaves it literal on
        // stdout, so it separates the two writers all by itself.
        escapes: 'quote " backslash \\ tab \t newline \n del \u007f',
        big: 9007199254740993n,
      }),
    ],
    ['a bare record', { identity: 'x', pid: 1n }],
  ];

  cases.forEach(([name, payload]) => {
    test(`${name}: file bytes and stdout line match`, () =>
      run(
        Effect.gen(function* () {
          const directory = yield* scratchDirectory;
          const captured = yield* capturedSink;
          const oursPath = join(directory, 'ours.json');
          const theirsPath = join(directory, 'theirs.json');

          const published = yield* publishInScope(oursPath, payload, captured.sink);
          const theirs = yield* pythonWrite(payload, theirsPath);

          expect(textOf(oursPath)).toBe(theirs.fileText);
          expect(published.line).toBe(theirs.line);
          expect(modeOf(oursPath)).toBe(theirs.mode);
          expect(theirs.mode).toBe(READY_FILE_MODE);
        }),
      ));
  });

  test('the record and its lock are 0600, the parents are made, no temp survives', () =>
    run(
      Effect.gen(function* () {
        const directory = yield* scratchDirectory;
        const path = join(directory, 'nested', 'deeper', 'gateway.json');
        const captured = yield* capturedSink;

        const published = yield* publishInScope(path, identityPayload(), captured.sink);
        const resolved = Option.getOrThrow(published.path);

        expect(modeOf(resolved)).toBe(READY_FILE_MODE);
        expect(modeOf(Option.getOrThrow(published.lockPath))).toBe(READY_FILE_MODE);
        // `.{name}.{token}.tmp` is unlinked in a `finally`, so nothing else is here.
        expect(readdirSync(dirname(resolved)).toSorted()).toEqual([
          'gateway.json',
          'gateway.json.lock',
        ]);
      }),
    ));
});

describe('ready file: the lock, both directions', () => {
  test('bun:ffi binds the host libc with platform errno and CLOEXEC constants', async () => {
    // An `Effect`, not a module constant: `dlopen` must not run at import time.
    expect(await Effect.runPromise(lockSupported)).toBe(true);
    expect(EWOULDBLOCK).toBe(process.platform === 'linux' ? 11 : 35);
    expect(O_CLOEXEC).toBe(process.platform === 'linux' ? 0x0008_0000 : 0x0100_0000);
  });

  test('a chmod failure closes the just-opened lock descriptor', async () => {
    const directory = mkdtempSync(join(tmpdir(), 'ready-file-fd-'));
    const path = join(directory, 'gateway.json.lock');
    const descriptors = (): number => readdirSync('/dev/fd').length;
    const before = descriptors();
    for (let attempt = 0; attempt < 128; attempt += 1) {
      const failure = await Effect.runPromise(
        Effect.flip(
          openLockFd(path, () => {
            throw new Error('injected chmod failure');
          }),
        ),
      );
      expect(failure).toMatchObject({ _tag: 'ReadyFileIoError', operation: 'chmod', path });
    }
    expect(descriptors()).toBeLessThanOrEqual(before + 1);
    rmSync(directory, { recursive: true, force: true });
  });

  test('a python3 holder blocks the publisher, and the incumbent record survives', () =>
    run(
      Effect.gen(function* () {
        const directory = yield* scratchDirectory;
        const path = join(directory, 'gateway.json');
        const captured = yield* capturedSink;
        const resolved = yield* resolveReadyPath(path);

        // An incumbent publisher: its record is already on disk and its
        // process holds <ready>.lock.
        const incumbent = '{\n  "identity": "incumbent",\n  "pid": 1\n}\n';
        yield* Effect.sync(() => writeFileSync(path, incumbent, { mode: READY_FILE_MODE }));
        const holder = yield* pythonHolder(path);

        const service = yield* readyFileService(path, captured.sink);
        const failure = yield* Effect.flip(service.publish(identityPayload()));

        expect(failure).toBeInstanceOf(ReadyFileLocked);
        expect(failure).toMatchObject({
          _tag: 'ReadyFileLocked',
          errno: EWOULDBLOCK,
          lockPath: `${resolved}${READY_LOCK_SUFFIX}`,
        });
        // R4: the loser must not disturb the record it could not replace.
        expect(textOf(path)).toBe(incumbent);
        expect(yield* captured.lines).toEqual([]);

        // And once Python lets go, the same publisher succeeds.
        yield* holder.release;
        const published = yield* Effect.orDie(service.publish(identityPayload()));
        expect(Option.isSome(published.bytes)).toBe(true);
        expect(textOf(path)).toBe(fileText(identityPayload()));
      }),
    ));

  test(`while held, Python reports BlockingIOError(errno=${String(EWOULDBLOCK)})`, () =>
    run(
      Effect.gen(function* () {
        const directory = yield* scratchDirectory;
        const path = join(directory, 'gateway.json');
        const captured = yield* capturedSink;
        const resolved = yield* resolveReadyPath(path);
        const lockPath = `${resolved}${READY_LOCK_SUFFIX}`;

        const fiber = yield* Effect.fork(
          Effect.scoped(
            Effect.zipRight(publishInScope(path, identityPayload(), captured.sink), Effect.never),
          ),
        );
        yield* waitUntil(Effect.sync(() => existsSync(resolved)), 'the ready record');

        expect(yield* pythonAttempt(lockPath)).toBe(`BLOCKED ${EWOULDBLOCK}`);

        yield* Fiber.interrupt(fiber);

        expect(yield* pythonAttempt(lockPath)).toBe('ACQUIRED');
      }),
    ));
});

describe('ready file: the guarded unlink', () => {
  test('a clean release removes our own record and never the lock', () =>
    run(
      Effect.gen(function* () {
        const directory = yield* scratchDirectory;
        const path = join(directory, 'gateway.json');
        const captured = yield* capturedSink;

        const resolved = yield* Effect.scoped(
          Effect.map(publishInScope(path, identityPayload(), captured.sink), (published) =>
            Option.getOrThrow(published.path),
          ),
        );

        expect(existsSync(resolved)).toBe(false);
        // The `.lock` companion is deliberately left behind (`:2166` unlinks
        // the record only), so the next publisher locks the same inode.
        expect(existsSync(`${resolved}${READY_LOCK_SUFFIX}`)).toBe(true);
      }),
    ));

  test('a foreign record survives the release; our own does not', () =>
    run(
      Effect.gen(function* () {
        const directory = yield* scratchDirectory;
        const captured = yield* capturedSink;

        /** Publish, then overwrite the record the way a replacement would. */
        const publishThenReplace = (
          name: string,
          replacement: string,
        ): Effect.Effect<string, never, Scope.Scope> =>
          Effect.flatMap(
            Effect.sync(() => join(directory, name)),
            (path) =>
              Effect.as(
                Effect.scoped(
                  Effect.zipRight(
                    publishInScope(path, identityPayload(), captured.sink),
                    Effect.sync(() => writeFileSync(path, replacement)),
                  ),
                ),
                path,
              ),
          );

        const foreignPid = yield* publishThenReplace(
          'a.json',
          fileText({ identity: OUR_IDENTITY, pid: OUR_PID + 1n }),
        );
        const foreignIdentity = yield* publishThenReplace(
          'b.json',
          fileText({ identity: 'ffffffffffffffffffff', pid: OUR_PID }),
        );
        const noGuardFields = yield* publishThenReplace('c.json', fileText({ other: true }));
        const unparseable = yield* publishThenReplace('d.json', '{not json');
        const stillOurs = yield* publishThenReplace(
          'e.json',
          fileText({ identity: OUR_IDENTITY, pid: OUR_PID, extra: 'rewritten' }),
        );

        const hugePid = 9_007_199_254_740_993n;
        const hugeOwned = yield* Effect.scoped(
          Effect.as(
            publishInScope(
              join(directory, 'huge-owned.json'),
              identityPayload({ pid: hugePid }),
              captured.sink,
            ),
            join(directory, 'huge-owned.json'),
          ),
        );
        const hugeForeign = yield* Effect.scoped(
          Effect.as(
            Effect.zipRight(
              publishInScope(
                join(directory, 'huge-foreign.json'),
                identityPayload({ pid: hugePid }),
                captured.sink,
              ),
              Effect.sync(() =>
                writeFileSync(
                  join(directory, 'huge-foreign.json'),
                  fileText({ identity: OUR_IDENTITY, pid: hugePid + 1n }),
                ),
              ),
            ),
            join(directory, 'huge-foreign.json'),
          ),
        );

        expect(existsSync(foreignPid)).toBe(true);
        expect(existsSync(foreignIdentity)).toBe(true);
        expect(existsSync(noGuardFields)).toBe(true);
        expect(textOf(unparseable)).toBe('{not json');
        // Both guard fields still match, including beyond Number.MAX_SAFE_INTEGER.
        expect(existsSync(stillOurs)).toBe(false);
        expect(existsSync(hugeOwned)).toBe(false);
        expect(existsSync(hugeForeign)).toBe(true);
      }),
    ));
});

describe('ready file: the finalizer', () => {
  test('an interrupted fiber still unlinks and unlocks', () =>
    run(
      Effect.gen(function* () {
        const directory = yield* scratchDirectory;
        const path = join(directory, 'gateway.json');
        const captured = yield* capturedSink;
        const resolved = yield* resolveReadyPath(path);

        const fiber = yield* Effect.fork(
          Effect.scoped(
            Effect.zipRight(publishInScope(path, identityPayload(), captured.sink), Effect.never),
          ),
        );
        yield* waitUntil(Effect.sync(() => existsSync(resolved)), 'the ready record');
        expect(existsSync(resolved)).toBe(true);

        const exit = yield* Fiber.interrupt(fiber);

        expect(Exit.isInterrupted(exit)).toBe(true);
        expect(existsSync(resolved)).toBe(false);
        // The lock really was released, not merely dropped on the floor.
        expect(yield* pythonAttempt(`${resolved}${READY_LOCK_SUFFIX}`)).toBe('ACQUIRED');
      }),
    ));
});

describe('ready file: disabled', () => {
  test('publishes nothing, locks nothing, and still yields the canonical line', () =>
    run(
      Effect.gen(function* () {
        const directory = yield* scratchDirectory;
        const payload = identityPayload();
        const service = yield* Effect.provide(ReadyFile, ReadyFileDisabled);

        const published = yield* service.publish(payload);

        expect(Option.isNone(published.path)).toBe(true);
        expect(Option.isNone(published.lockPath)).toBe(true);
        expect(Option.isNone(published.bytes)).toBe(true);
        expect(published.line).toBe(lineText(payload));
        expect(readdirSync(directory)).toEqual([]);
      }),
    ));
});
