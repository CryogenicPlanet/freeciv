/**
 * `help` and `rules` — the two doc surfaces the justfile carried.
 *
 * The recipes were `@cat docs/play.md` and `@cat docs/gameplay.md`
 * (justfile:396-403), so the contract is not "prints something like the card",
 * it is "prints the file".  Two things therefore get asserted here:
 *
 * 1. the embedded constants are byte-identical to the archived
 *    `play/docs/*.md`, so an edit there fails this suite instead of drifting;
 * 2. running the command emits those bytes and exactly those bytes, trailing
 *    newline included — `cat` adds nothing and neither may this.
 */
import { describe, expect, test } from 'bun:test';
import { Command } from '@effect/cli';
import { BunContext } from '@effect/platform-bun';
import { Effect } from 'effect';
import { helpCommand } from 'src/commands/help.cmd';
import { rulesCommand } from 'src/commands/rules.cmd';
import { GAMEPLAY_RULES } from 'src/docs/gameplay-rules';
import { PLAY_CARD } from 'src/docs/play-card';
import { effectTest, provideTestLayer } from 'test/_effect-test';
import { path, withTestFileSystem } from 'test/_test-platform';

/** The archived Python tree this package supersedes. */
const docPath = (name: string): string =>
  path.resolve(import.meta.dir, '..', '..', 'archive', 'play', 'docs', name);

/**
 * Run one subcommand and collect stdout as raw bytes.
 *
 * `Console.log` appends exactly one newline to its joined arguments, so
 * re-adding it here is what makes the comparison a byte comparison rather than
 * a line comparison — the trailing newline is the part `cat` would emit and a
 * line-wise assertion would never notice missing.
 */
const runForBytes = <Name extends string, A>(
  command: Command.Command<Name, never, never, A>,
  argv: ReadonlyArray<string>
): Effect.Effect<string> => {
  const root = Command.make('play', {}, () => Effect.void).pipe(
    Command.withSubcommands([command])
  );
  let out = '';
  const original = console.log;
  console.log = (...parts: ReadonlyArray<unknown>) => {
    out += `${parts.join(' ')}\n`;
  };
  return provideTestLayer(
    Command.run(root, { name: 'play', version: '0.1.0' })(['bun', 'play', ...argv]),
    BunContext.layer
  ).pipe(
    Effect.orDie,
    Effect.ensuring(Effect.sync(() => {
      console.log = original;
    })),
    Effect.map(() => out)
  );
};

describe('the embedded documents', () => {
  effectTest('the play card is play/docs/play.md, byte for byte', () =>
    withTestFileSystem((files) =>
      Effect.gen(function* () {
        expect(`${PLAY_CARD}\n`).toBe(yield* files.readFileString(docPath('play.md')));
      }).pipe(Effect.orDie)
    )
  );

  effectTest('the rules are play/docs/gameplay.md, byte for byte', () =>
    withTestFileSystem((files) =>
      Effect.gen(function* () {
        expect(`${GAMEPLAY_RULES}\n`).toBe(yield* files.readFileString(docPath('gameplay.md')));
      }).pipe(Effect.orDie)
    )
  );

  test('the constants carry no trailing newline of their own', () => {
    // The printer supplies it. Carrying it in the constant *and* printing it
    // would put a blank line at the end of every `just help`.
    expect(PLAY_CARD.endsWith('\n')).toBe(false);
    expect(GAMEPLAY_RULES.endsWith('\n')).toBe(false);
  });

  test('the em dash survives the copy, so the bytes are UTF-8 not ASCII', () => {
    expect(PLAY_CARD).toContain('a whole turn in **one call**');
    expect(PLAY_CARD).toContain('—');
  });
});

describe('play help', () => {
  effectTest('prints docs/play.md and nothing else', () =>
    withTestFileSystem((files) =>
      Effect.gen(function* () {
        expect(yield* runForBytes(helpCommand, ['help'])).toBe(
          yield* files.readFileString(docPath('play.md'))
        );
      }).pipe(Effect.orDie)
    )
  );

  effectTest('does not print the harness-author reference the recipe withheld', () =>
    Effect.gen(function* () {
      // justfile:396-397 — every doc character an agent reads is part of its
      // per-turn budget, so `commands.md` stays out of the agent-facing card.
      const printed = yield* runForBytes(helpCommand, ['help']);
      expect(printed).not.toContain('# Commands');
      expect(printed.length).toBeLessThan(6000);
    })
  );
});

describe('play rules', () => {
  effectTest('prints docs/gameplay.md and nothing else', () =>
    withTestFileSystem((files) =>
      Effect.gen(function* () {
        expect(yield* runForBytes(rulesCommand, ['rules'])).toBe(
          yield* files.readFileString(docPath('gameplay.md'))
        );
      }).pipe(Effect.orDie)
    )
  );

  effectTest('the two surfaces are different documents', () =>
    Effect.gen(function* () {
      const card = yield* runForBytes(helpCommand, ['help']);
      const rules = yield* runForBytes(rulesCommand, ['rules']);
      expect(card).not.toBe(rules);
    })
  );
});
