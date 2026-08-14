/**
 * The justfile cutover: command mentions follow PLAY_PROG.
 *
 * Parity mode (`PLAY_PROG=just`, the test-runner default from _preload.ts) is
 * what every golden test and the diff oracle pin; these tests cover the other
 * spelling — the one provisioned workspaces actually see.
 */
import { afterEach, describe, expect, test } from 'bun:test';
import { Effect } from 'effect';
import { PROG_ENV, resolveProg, rewriteProgMentions } from 'src/services/prog-prefix';
import { writeMirror } from 'src/services/mirror/store';
import { scratchWorkspace, type Scratch } from 'test/_fixtures';
import { effectTest, provideTestLayer } from 'test/_effect-test';
import { path } from 'test/_test-platform';

const scratches: Scratch[] = [];

afterEach(() =>
  Effect.runPromise(
    Effect.asVoid(Effect.all(scratches.splice(0).map((scratch) => scratch.cleanup)))
  )
);

const setProg = (value: string | undefined): Effect.Effect<void> =>
  Effect.sync(() => {
    if (value === undefined) {
      delete Bun.env[PROG_ENV];
    } else {
      Bun.env[PROG_ENV] = value;
    }
  });

const withProg = <A, E, R>(
  value: string | undefined,
  body: Effect.Effect<A, E, R>
): Effect.Effect<A, E, R> =>
  Effect.acquireUseRelease(
    Effect.flatMap(Effect.sync(() => Bun.env[PROG_ENV]), (previous) =>
      Effect.as(setProg(value), previous)
    ),
    () => body,
    setProg
  );

describe('resolveProg', () => {
  effectTest('defaults to ./play when unset', () =>
    withProg(undefined, Effect.sync(() => expect(resolveProg()).toBe('./play')))
  );
  effectTest('defaults to ./play when blank', () =>
    withProg('  ', Effect.sync(() => expect(resolveProg()).toBe('./play')))
  );
  effectTest('honours an explicit spelling', () =>
    withProg('just', Effect.sync(() => expect(resolveProg()).toBe('just')))
  );
});

describe('rewriteProgMentions', () => {
  test('parity mode is the identity', () => {
    const line = 'next: just wait — or add --await --brief';
    expect(rewriteProgMentions(line, 'just')).toBe(line);
  });

  test('rewrites every registered verb', () => {
    expect(rewriteProgMentions('run just wait then just turn --end', './play')).toBe(
      'run ./play wait then ./play turn --end'
    );
    expect(rewriteProgMentions('just legal --actor_id u3 --all', './play')).toBe(
      './play legal --actor_id u3 --all'
    );
    expect(
      rewriteProgMentions('just do "u1 found_city London; u2 route 32,73" --end', './play')
    ).toBe('./play do "u1 found_city London; u2 route 32,73" --end');
  });

  test('multiple mentions in one text all move', () => {
    const briefing = [
      'ERRORS carry their own remedy.',
      'just start                                get into the game',
      'just turn                                 one briefing, one revision',
      'A failed wait command is a harness error; run just health first.',
    ].join('\n');
    const rewritten = rewriteProgMentions(briefing, './play');
    expect(rewritten).not.toContain('just start');
    expect(rewritten).toContain('./play start');
    expect(rewritten).toContain('./play turn');
    expect(rewritten).toContain('./play health');
  });

  test('prose, justfile, and unknown verbs survive', () => {
    const prose = 'it is just one call; adjust waiting; the justfile is gone; just because';
    expect(rewriteProgMentions(prose, './play')).toBe(prose);
    // `--list` is not a registered verb: the mention dies with the justfile.
    expect(rewriteProgMentions('just --list', './play')).toBe('just --list');
  });

  effectTest('reads PLAY_PROG per call', () =>
    Effect.gen(function* () {
      yield* withProg(
        './play',
        Effect.sync(() =>
          expect(rewriteProgMentions('next: just wait')).toBe('next: ./play wait')
        )
      );
      yield* withProg(
        'just',
        Effect.sync(() =>
          expect(rewriteProgMentions('next: just wait')).toBe('next: just wait')
        )
      );
    })
  );
});

describe('writeMirror', () => {
  const mirrorThrough = (prog: string): Effect.Effect<string> =>
    Effect.gen(function* () {
      const scratch = yield* scratchWorkspace();
      scratches.push(scratch);
      return yield* withProg(
        prog,
        Effect.gen(function* () {
          yield* provideTestLayer(
            writeMirror(scratch.workspace.stateRoot, ['header.txt'], 'NOT YOUR TURN — next: just wait'),
            scratch.layer
          );
          return yield* scratch.files.readText(
            path.join(scratch.workspace.stateRoot, 'header.txt'),
            'mirror'
          );
        }).pipe(Effect.orDie)
      );
    });

  effectTest('spells guidance per PLAY_PROG', () =>
    Effect.gen(function* () {
      expect(yield* mirrorThrough('./play')).toBe('NOT YOUR TURN — next: ./play wait\n');
      expect(yield* mirrorThrough('just')).toBe('NOT YOUR TURN — next: just wait\n');
    })
  );
});

const spawnHelp = (env: Readonly<Record<string, string | undefined>>): string => {
  const overrides = Object.fromEntries(
    Object.entries(env).filter((entry): entry is [string, string] => entry[1] !== undefined)
  );
  const inherited = Object.fromEntries(
    Object.entries(Bun.env).filter(
      (entry): entry is [string, string] =>
        entry[1] !== undefined && !(Object.hasOwn(env, entry[0]) && env[entry[0]] === undefined)
    )
  );
  const result = Bun.spawnSync({
    cmd: ['bun', 'run', path.join(import.meta.dir, '..', 'src', 'bin.ts'), 'help'],
    env: { ...inherited, ...overrides },
    stdout: 'pipe',
    stderr: 'pipe',
  });
  return new TextDecoder().decode(result.stdout);
};

describe('the spawned CLI', () => {
  test('defaults to ./play spelling', () => {
    const out = spawnHelp({ PLAY_PROG: undefined });
    expect(out).toContain('./play join');
    expect(out).not.toMatch(/\bjust (join|turn|wait|do)\b/);
  }, 30_000);

  test('PLAY_PROG=just restores parity', () => {
    const out = spawnHelp({ PLAY_PROG: 'just' });
    expect(out).toContain('just join');
  }, 30_000);
});
