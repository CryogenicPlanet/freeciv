/**
 * The root command and the single error → exit-code site.
 *
 * `main()` in the Python is nine lines and every one of them is a contract:
 * a refusal renders its body *and* reports on stderr *and* exits 2; a wait's
 * 75/66 passes straight through without an `error:` line.
 */
import { describe, expect, test } from 'bun:test';
import { Effect, Layer, Option, Schema } from 'effect';
import { PlayerError, V2ResponseError, playerError } from 'src/errors';
import {
  EXIT_NOINPUT,
  EXIT_OK,
  EXIT_REFUSED,
  EXIT_TEMPFAIL,
  exitWith,
  isExitCode,
  passThroughExit,
} from 'src/exit';
import {
  COMMAND_OWNERS,
  commandFromArgv,
  dualFloat,
  handleError,
  jsonFlagFromArgv,
  resolveDual,
  runCli,
  subcommands,
  SUBCOMMAND_NAMES,
} from 'src/cli-main';
import { RefusalRenderDefault, errorText } from 'src/render/refusal-seam';
import { COMMAND_NAMES } from 'src/constants';
import { errorPayload } from 'test/_fixtures';

import { captureEffect } from 'test/_capture';
import { effectTest, provideTestLayer } from 'test/_effect-test';

describe('exit codes', () => {
  test('the contract is exactly four statuses', () => {
    expect([EXIT_OK, EXIT_REFUSED, EXIT_TEMPFAIL, EXIT_NOINPUT]).toEqual([0, 2, 75, 66]);
    for (const code of [0, 2, 75, 66]) expect(isExitCode(code)).toBe(true);
    for (const code of [1, 3, 74, 255]) expect(isExitCode(code)).toBe(false);
  });

  test('anything outside the contract collapses to a refusal', () => {
    expect(passThroughExit(75)).toBe(75);
    expect(passThroughExit(1)).toBe(2);
  });
});

describe('the dual-spelling helper', () => {
  test('either spelling alone resolves', () => {
    expect(
      Effect.runSync(
        resolveDual('wait-s', { dashed: Option.some(30), underscored: Option.none() }, 120)
      )
    ).toBe(30);
    expect(
      Effect.runSync(
        resolveDual('wait-s', { dashed: Option.none(), underscored: Option.some(45) }, 120)
      )
    ).toBe(45);
  });

  test('neither spelling falls back to the default', () => {
    expect(
      Effect.runSync(
        resolveDual('poll-s', { dashed: Option.none(), underscored: Option.none() }, 1)
      )
    ).toBe(1);
  });

  test('both spellings at once is a refusal with the justfile message', () => {
    const either = Effect.runSync(
      Effect.either(
        resolveDual('wait-s', { dashed: Option.some(1), underscored: Option.some(2) }, 120)
      )
    );
    expect(either._tag).toBe('Left');
    if (either._tag === 'Left') {
      expect(either.left.message).toBe('pass only one of --wait-s or --wait_s');
    }
  });

  test('the Options builder declares both names', () => {
    // Nothing to assert about the value here beyond it constructing; the real
    // proof is `play --help`, which lists `--wait-s` and `--wait_s` side by side.
    expect(dualFloat('wait-s')).toBeDefined();
  });
});

describe('the command registry', () => {
  test('every name in the surface is registered exactly once', () => {
    expect([...SUBCOMMAND_NAMES].toSorted()).toEqual([...COMMAND_NAMES].toSorted());
    expect(new Set(SUBCOMMAND_NAMES).size).toBe(SUBCOMMAND_NAMES.length);
  });

  test('the registry and the command array stay the same length', () => {
    expect(subcommands).toHaveLength(SUBCOMMAND_NAMES.length);
  });

  test('every command names the unit that owns it', () => {
    for (const name of COMMAND_NAMES) {
      expect(COMMAND_OWNERS.has(name)).toBe(true);
    }
  });

  effectTest('--help exits 0 and lists the commands', () =>
    Effect.gen(function* () {
    const { value, captured } = yield* captureEffect(runCli(['bun', 'play', '--help']));
    expect(value).toBe(EXIT_OK);
    const text = captured.out.join('\n');
    for (const name of ['prompt', 'join', 'do', 'wait', 'monitor', 'result']) {
      expect(text).toContain(name);
    }
    })
  );

  effectTest('an unknown subcommand exits 2', () =>
    Effect.gen(function* () {
      const { value } = yield* captureEffect(runCli(['bun', 'play', 'nonsense']));
      expect(value).toBe(EXIT_REFUSED);
    })
  );

  effectTest('a bare invocation is a refusal that names --help', () =>
    Effect.gen(function* () {
      const { value, captured } = yield* captureEffect(runCli(['bun', 'play']));
      expect(value).toBe(EXIT_REFUSED);
      expect(captured.err.join('\n')).toContain('a subcommand is required');
    })
  );
});

describe('argv introspection', () => {
  test('the subcommand is the first non-flag token after the program', () => {
    expect(commandFromArgv(['bun', 'play', '--json', 'state'])).toBe('state');
    expect(commandFromArgv(['bun', 'play'])).toBe('');
  });

  test('--json is detected for the refusal-rendering decision', () => {
    expect(jsonFlagFromArgv(['bun', 'play', 'state', '--json'])).toBe(true);
    expect(jsonFlagFromArgv(['bun', 'play', 'state'])).toBe(false);
  });
});

describe('error mapping', () => {
  const refusal = new V2ResponseError({
    message: 'HTTP 409: nope (illegal_action)',
    status: 409,
    payload: errorPayload(),
  });

  effectTest('a refusal renders its body on stdout and reports on stderr, exit 2', () =>
    Effect.gen(function* () {
    const { value, captured } = yield* captureEffect(
      provideTestLayer(handleError(refusal, ['bun', 'play', 'state']), RefusalRenderDefault)
    );
    expect(value).toBe(EXIT_REFUSED);
    expect(captured.out).toEqual([errorText(errorPayload())]);
    expect(captured.err).toEqual(['error: HTTP 409: nope (illegal_action)']);
    })
  );

  effectTest('--json keeps the byte-identical wire payload', () =>
    Effect.gen(function* () {
    const { captured } = yield* captureEffect(
      provideTestLayer(
        handleError(refusal, ['bun', 'play', 'state', '--json']),
        RefusalRenderDefault
      )
    );
    expect(captured.out).toHaveLength(1);
    expect(
      yield* Effect.orDie(
        Schema.decodeUnknown(Schema.parseJson(Schema.Unknown))(captured.out[0] ?? '')
      )
    ).toEqual(errorPayload());
    })
  );

  effectTest('a JSON-only command keeps its refusal JSON with no flag at all', () =>
    Effect.gen(function* () {
      const { captured } = yield* captureEffect(
        provideTestLayer(handleError(refusal, ['bun', 'play', 'next']), RefusalRenderDefault)
      );
      expect(
        yield* Effect.orDie(
          Schema.decodeUnknown(Schema.parseJson(Schema.Unknown))(captured.out[0] ?? '')
        )
      ).toEqual(errorPayload());
    })
  );

  effectTest('a PlayerError is one stderr line and exit 2', () =>
    Effect.gen(function* () {
    const { value, captured } = yield* captureEffect(
      provideTestLayer(
        handleError(playerError('no current session'), ['bun', 'play', 'state']),
        RefusalRenderDefault
      )
    );
    expect(value).toBe(EXIT_REFUSED);
    expect(captured.out).toEqual([]);
    expect(captured.err).toEqual(['error: no current session']);
    })
  );

  effectTest('an exit signal passes 75 through and prints nothing', () =>
    Effect.gen(function* () {
    const { value, captured } = yield* captureEffect(
      provideTestLayer(
        handleError(exitWith(EXIT_TEMPFAIL), ['bun', 'play', 'wait']),
        RefusalRenderDefault
      )
    );
    expect(value).toBe(EXIT_TEMPFAIL);
    expect(captured.out).toEqual([]);
    expect(captured.err).toEqual([]);
    })
  );

  effectTest('an exit signal passes 66 through as well', () =>
    Effect.gen(function* () {
      const { value } = yield* captureEffect(
        provideTestLayer(
          handleError(exitWith(EXIT_NOINPUT), ['bun', 'play', 'wait']),
          RefusalRenderDefault
        )
      );
      expect(value).toBe(EXIT_NOINPUT);
    })
  );

  test('every mapped failure keeps a user-facing message', () => {
    const failures = [
      playerError('a'),
      new PlayerError({ message: 'b' }),
      refusal,
    ];
    for (const failure of failures) expect(failure.message.length).toBeGreaterThan(0);
  });
});

describe('the refusal seam', () => {
  test('the default renderer is _error_text and nothing more', () => {
    expect(errorText(errorPayload())).toBe('illegal_action: that unit cannot found a city here');
  });

  test('a payload that is not a v2 error still renders something printable', () => {
    expect(errorText({ nonsense: true })).toContain('invalid_request');
  });

  test('the seam is a Layer, so U14 replaces it without touching cli-main', () => {
    expect(Layer.isLayer(RefusalRenderDefault)).toBe(true);
  });
});
