/**
 * `use` — one workspace, one seat.
 *
 * Ports `test_use_binds_by_path_or_game_id_and_fails_closed_when_ambiguous`,
 * `test_use_on_an_unbound_workspace_names_join`, the `use` row of
 * `test_a_preconfigured_workspace_answers_every_v2_command_with_join`, and the
 * shim's `not bound to a seat` assertion from
 * `test_the_play_shim_is_the_same_cli_as_client_py`.
 *
 * `use` is the only command that prints a session path, and it prints it in the
 * exact workspace-relative form it accepts back — every refusal below is
 * checked for the command that repairs it, because that string is the agent's
 * whole recovery surface.
 */
import { afterEach, describe, expect } from 'bun:test';
import { Command, ValidationError } from '@effect/cli';
import { BunContext } from '@effect/platform-bun';
import { Effect, Either, Layer, Option, Schema } from 'effect';
import type { MappedError } from 'src/cli-main';
import { FULL_CONTROL_V2 } from 'src/constants';
import {
  commandUse,
  resolveUseTarget,
  useCommand,
  workspaceRelative,
} from 'src/commands/use.cmd';
import type { PlayerError } from 'src/errors';
import type { JsonObject } from 'src/schema/primitives';
import {
  type PrivateFs,
  type Workspace,
  type PrivateFsApi,
  type WorkspacePaths,
} from 'src/services/private-fs';
import {
  SessionStore,
  emptyV2ClientState,
  sessionStoreFor,
  type SessionStoreApi,
} from 'src/services/session-store';
import { scratchWorkspace, type Scratch } from 'test/_fixtures';
import { captureEffect } from 'test/_capture';
import { awaitTest, effectTest, provideTestLayer } from 'test/_effect-test';
import { path, withTestFileSystem } from 'test/_test-platform';

const FIRST = 'game_Hsit9YEuBjKdJPPouFoGVYlk';
const SECOND = 'game_9SecondBoundGame00000000';
const MISSING = 'game_NeverJoinedByThisSeat00';

const scratches: Scratch[] = [];

afterEach(() =>
  Effect.runPromise(
    Effect.asVoid(Effect.all(scratches.splice(0).map((scratch) => scratch.cleanup)))
  )
);

const schema = {
  empty: emptyV2ClientState,
  validate: () => Effect.void,
  cursorExpired: (): boolean => false,
};

interface Bench {
  readonly workspace: WorkspacePaths;
  readonly files: PrivateFsApi;
  readonly store: SessionStoreApi;
  readonly layer: Layer.Layer<Workspace | PrivateFs | SessionStore>;
  /** Write one mode-0600 session file and return its absolute path. */
  readonly seat: (
    gameId: string,
    name: string,
    overrides?: JsonObject
  ) => Effect.Effect<string>;
}

const bench = (): Effect.Effect<Bench> =>
  Effect.gen(function* () {
    const scratch = yield* scratchWorkspace();
    scratches.push(scratch);
    const { workspace, files } = scratch;
    const store = sessionStoreFor(workspace, files, schema, {});
    return {
      workspace,
      files,
      store,
      layer: Layer.merge(scratch.layer, Layer.succeed(SessionStore, store)),
      seat: (gameId, name, overrides = {}) => {
        const target = path.join(workspace.stateRoot, gameId, `${name}.json`);
        return Effect.as(
          files.writeJson(target, {
            schema_version: 1,
            control_protocol: FULL_CONTROL_V2,
            game_id: gameId,
            agent_id: 'agent-v2',
            agent_token: 'agent-v2-secret',
            service_url: 'http://127.0.0.1:8765',
            controller_label: name,
            ...overrides,
          }),
          target
        ).pipe(Effect.orDie);
      },
    };
  });

interface Captured {
  readonly out: string;
  readonly result: Either.Either<void, PlayerError>;
}

const useJsonPayload = Schema.Struct({
  game_id: Schema.String,
  session_file: Schema.String,
  bound_at: Schema.String,
  rebound_from: Schema.NullOr(Schema.String),
});

const decodeUseJson = Schema.decodeUnknownSync(Schema.parseJson(useJsonPayload));

const gameIdJson = Schema.decodeUnknownSync(
  Schema.parseJson(Schema.Struct({ game_id: Schema.String }))
);

const captureUse = (
  fixture: Bench,
  target = '',
  options: { readonly json?: boolean; readonly env?: Record<string, string | undefined> } = {}
): Effect.Effect<Captured> =>
  captureEffect(
    Effect.either(
      provideTestLayer(
        commandUse({ target, json: options.json ?? false }, options.env ?? {}),
        fixture.layer
      )
    )
  ).pipe(
    Effect.map(({ value, captured }) => ({
      out: captured.out.join('\n'),
      result: value,
    }))
  );

const failure = (either: Either.Either<unknown, PlayerError>): string => {
  expect(Either.isLeft(either)).toBe(true);
  if (Either.isLeft(either)) return either.left.message;
  return '';
};

const ok = <A, E>(either: Either.Either<A, E>): void => {
  if (Either.isLeft(either)) {
    throw new Error(`expected success, got ${JSON.stringify(either.left)}`);
  }
};

const boundGame = (fixture: Bench): Effect.Effect<string> =>
  Effect.map(fixture.store.readSeatBinding(), (binding) => {
    if (Option.isNone(binding)) throw new Error('expected bound workspace');
    return binding.value.gameId;
  }).pipe(Effect.orDie);

const resolve = (fixture: Bench, value: string) =>
  Effect.either(resolveUseTarget(fixture.workspace, fixture.files, fixture.store, value));

const cliFailureText = (error: MappedError | ValidationError.ValidationError): string => {
  if (ValidationError.isValidationError(error)) return error._tag;
  if (error._tag === 'ExitCodeSignal') return String(error.code);
  return error.message;
};

const cli = (
  fixture: Bench,
  argv: ReadonlyArray<string>
): Effect.Effect<{ readonly failure: string | null; readonly out: string }> => {
  const root = Command.make('play', {}, () => Effect.void).pipe(
    Command.withSubcommands([useCommand])
  );
  return captureEffect(
    Effect.either(
      provideTestLayer(
        Command.run(root, { name: 'play', version: '0.1.0' })(['bun', 'play', ...argv]),
        Layer.merge(fixture.layer, BunContext.layer)
      )
    )
  ).pipe(
    Effect.map(({ value, captured }) => ({
      failure: value._tag === 'Left' ? cliFailureText(value.left) : null,
      out: captured.out.join('\n'),
    }))
  );
};

// ---------------------------------------------------------------------------

describe('_workspace_relative', () => {
  effectTest('a state path is spelled the way `use` accepts it back', () => Effect.gen(function* () {
    const fixture = yield* bench();
    const seat = yield* fixture.seat(FIRST, 'codex-first-model');
    expect(workspaceRelative(fixture.workspace, seat)).toBe(
      path.join('.sessions', FIRST, 'codex-first-model.json')
    );
  }));

  effectTest('a path outside the workspace is left absolute rather than mangled', () => Effect.gen(function* () {
    const fixture = yield* bench();
    expect(workspaceRelative(fixture.workspace, '/elsewhere/session.json')).toBe(
      '/elsewhere/session.json'
    );
  }));
});

describe('_resolve_use_target', () => {
  effectTest('a game ID with exactly one seat resolves to that seat', () => Effect.gen(function* () {
    const fixture = yield* bench();
    const seat = yield* fixture.seat(FIRST, 'codex-first-model');
    expect(yield* resolve(fixture, FIRST)).toEqual(Either.right(seat));
  }));

  effectTest('a game this workspace never joined names the join command', () => Effect.gen(function* () {
    expect(failure(yield* resolve(yield* bench(), MISSING))).toBe(
      `this workspace holds no joined seat for ${MISSING}. Join one with ` +
        `\`just join --game_id ${MISSING} --name HARNESS-MODEL\`.`
    );
  }));

  effectTest('two seats in one game fail closed, listing both commands', () => Effect.gen(function* () {
    const fixture = yield* bench();
    const first = yield* fixture.seat(FIRST, 'codex-first-model');
    const second = yield* fixture.seat(FIRST, 'codex-sibling-model');
    const message = failure(yield* resolve(fixture, FIRST));
    expect(message).toContain(`${FIRST} has 2 joined seats in this workspace;`);
    expect(message).toContain('name the one you are playing:');
    expect(message).toContain(`\`just use ${workspaceRelative(fixture.workspace, first)}\``);
    expect(message).toContain(`\`just use ${workspaceRelative(fixture.workspace, second)}\``);
  }));

  effectTest('a workspace-relative path resolves without being a game ID', () => Effect.gen(function* () {
    const fixture = yield* bench();
    const seat = yield* fixture.seat(FIRST, 'codex-first-model');
    expect(yield* resolve(fixture, path.join('.sessions', FIRST, 'codex-first-model.json'))).toEqual(
      Either.right(seat)
    );
  }));

  effectTest('a path outside PLAY_STATE_DIR is refused', () => Effect.gen(function* () {
    const fixture = yield* bench();
    expect(failure(yield* resolve(fixture, '/etc/passwd'))).toBe(
      'private state files must stay inside PLAY_STATE_DIR'
    );
  }));
});

describe('bare `use`', () => {
  /**
   * `(getattr(args, "target", "") or "").strip()` uses CPython's whitespace
   * class, which is not `String.prototype.trim`'s: the four ASCII separators
   * and NEL are blank to Python and not to JavaScript, and `\uFEFF` is the
   * reverse.  Getting it wrong decides whether `use` reports the bound seat or
   * goes looking for a session file named after a control character.
   */
  for (const [character, label] of [
    ['\u001c', 'FS'],
    ['\u001f', 'US'],
    ['\u0085', 'NEL'],
  ] as const) {
    awaitTest(`a target of a lone ${label} is blank, so bare \`use\` answers`, function* (wait) {
      const message = failure((yield* wait(captureUse(yield* bench(), character))).result);
      expect(message).toContain('this workspace is not bound to a seat.');
    });
  }

  awaitTest('a target of a lone ZWNBSP is NOT blank, so it is resolved as a path', function* (wait) {
    const message = failure((yield* wait(captureUse(yield* bench(), '\uFEFF'))).result);
    expect(message).not.toContain('this workspace is not bound to a seat.');
  });

  awaitTest('an unbound workspace names the join command', function* (wait) {
    const { result } = yield* wait(captureUse(yield* bench()));
    const message = failure(result);
    expect(message).toBe(
      'this workspace is not bound to a seat. Join one with ' +
        '`just join --game_id GAME_ID --name HARNESS-MODEL`, or bind ' +
        'a seat you already joined with `just use GAME_ID`.'
    );
    expect(message).toContain('not bound to a seat');
    expect(message).toContain('just join --game_id GAME_ID');
  });

  awaitTest(
    'a pre-configured workspace names bare `just join`, never the generic form',
    function* (wait) {
      const fixture = yield* bench();
      yield* wait(
        withTestFileSystem((files) =>
          files
            .writeFileString(
              path.join(fixture.workspace.root, '.playconfig.json'),
              JSON.stringify({
                schema_version: 1,
                game_id: FIRST,
                name: 'codex-test-model',
                place: null,
              })
            )
            .pipe(Effect.orDie)
        )
      );
      const message = failure((yield* wait(captureUse(fixture))).result);
      expect(message).toBe(
        'run `just join` first — this workspace is ' +
          `pre-configured for ${FIRST}, and joining binds the ` +
          'seat this command reports'
      );
      expect(message).toContain('`just join`');
      expect(message).toContain(FIRST);
      expect(message).not.toContain('--game_id');
      expect(message).not.toContain('multiple private sessions');
    }
  );

  awaitTest('a bound workspace reports the seat, never a token', function* (wait) {
    const fixture = yield* bench();
    const seat = yield* fixture.seat(FIRST, 'codex-first-model');
    ok((yield* wait(captureUse(fixture, seat))).result);
    const report = yield* wait(captureUse(fixture));
    ok(report.result);
    expect(report.out).toContain(`playing ${FIRST} | seat `);
    expect(report.out).toContain(workspaceRelative(fixture.workspace, seat));
    expect(report.out).toContain(
      'commands need no --session; `just use GAME_ID` rebinds this workspace'
    );
    expect(report.out).not.toContain('agent-v2-secret');
  });

  awaitTest('--json reports the binding as a payload with no rebind', function* (wait) {
    const fixture = yield* bench();
    const seat = yield* fixture.seat(FIRST, 'codex-first-model');
    ok((yield* wait(captureUse(fixture, seat))).result);
    const report = yield* wait(captureUse(fixture, '', { json: true }));
    ok(report.result);
    const payload = decodeUseJson(report.out);
    expect(payload.game_id).toBe(FIRST);
    expect(payload.session_file).toBe(seat);
    expect(payload.rebound_from).toBeNull();
    expect(payload.bound_at).toMatch(/^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ$/);
    expect(report.out).not.toContain('agent-v2-secret');
  });
});

describe('`use TARGET`', () => {
  awaitTest(
    'it binds by exact path, then by game ID, saying what it left each time',
    function* (wait) {
      const fixture = yield* bench();
      const first = yield* fixture.seat(FIRST, 'codex-first-model');
      yield* fixture.seat(SECOND, 'codex-second-model');

      const initial = yield* wait(captureUse(fixture, SECOND));
      ok(initial.result);
      expect(initial.out).toBe(
        `this workspace is now playing ${SECOND} — commands need no --session`
      );

      const byPath = yield* wait(captureUse(fixture, workspaceRelative(fixture.workspace, first)));
      ok(byPath.result);
      expect(byPath.out).toContain(
        `this workspace is now playing ${FIRST}, rebound from ${SECOND}`
      );
      expect(yield* boundGame(fixture)).toBe(FIRST);

      const byGame = yield* wait(captureUse(fixture, SECOND));
      ok(byGame.result);
      expect(byGame.out).toContain(
        `this workspace is now playing ${SECOND}, rebound from ${FIRST}`
      );
      expect(yield* boundGame(fixture)).toBe(SECOND);
    }
  );

  awaitTest('rebinding to another seat in the same game says exactly that', function* (wait) {
    const fixture = yield* bench();
    const first = yield* fixture.seat(FIRST, 'codex-first-model');
    const sibling = yield* fixture.seat(FIRST, 'codex-sibling-model');
    ok((yield* wait(captureUse(fixture, workspaceRelative(fixture.workspace, first)))).result);
    const rebound = yield* wait(captureUse(fixture, workspaceRelative(fixture.workspace, sibling)));
    ok(rebound.result);
    expect(rebound.out).toBe(
      `this workspace is now playing ${FIRST}, rebound to another seat in ` +
        'the same game — commands need no --session'
    );
  });

  awaitTest('re-binding the seat already bound reports no rebind at all', function* (wait) {
    const fixture = yield* bench();
    const seat = yield* fixture.seat(FIRST, 'codex-first-model');
    ok((yield* wait(captureUse(fixture, seat))).result);
    const again = yield* wait(captureUse(fixture, seat));
    ok(again.result);
    expect(again.out).toBe(
      `this workspace is now playing ${FIRST} — commands need no --session`
    );
  });

  awaitTest('an ambiguous game leaves the previous binding untouched', function* (wait) {
    const fixture = yield* bench();
    yield* fixture.seat(FIRST, 'codex-first-model');
    yield* fixture.seat(FIRST, 'codex-sibling-model');
    yield* fixture.seat(SECOND, 'codex-second-model');
    ok((yield* wait(captureUse(fixture, SECOND))).result);
    expect(failure((yield* wait(captureUse(fixture, FIRST))).result)).toContain(
      'name the one you are playing'
    );
    expect(yield* boundGame(fixture)).toBe(SECOND);
  });

  awaitTest('a file that is not a session this workspace joined is refused by name', function* (wait) {
    const fixture = yield* bench();
    const stray = path.join(fixture.workspace.stateRoot, FIRST, 'not-a-session.json');
    yield* fixture.files.writeJson(stray, { hello: 'world' });
    const target = workspaceRelative(fixture.workspace, stray);
    expect(failure((yield* wait(captureUse(fixture, target))).result)).toBe(
      `${target} is not a session this workspace joined. Bind a seat ` +
        'by its game with `just use GAME_ID`.'
    );
  });

  awaitTest('a session without a bearer token is refused too', function* (wait) {
    const fixture = yield* bench();
    const seat = yield* fixture.seat(FIRST, 'codex-first-model', { agent_token: null });
    expect(failure((yield* wait(captureUse(fixture, seat))).result)).toContain(
      'is not a session this workspace joined'
    );
  });

  awaitTest('--json reports the game it rebound from', function* (wait) {
    const fixture = yield* bench();
    yield* fixture.seat(FIRST, 'codex-first-model');
    const second = yield* fixture.seat(SECOND, 'codex-second-model');
    ok((yield* wait(captureUse(fixture, FIRST))).result);
    const rebound = yield* wait(captureUse(fixture, second, { json: true }));
    ok(rebound.result);
    const payload = decodeUseJson(rebound.out);
    expect(payload.game_id).toBe(SECOND);
    expect(payload.session_file).toBe(second);
    expect(payload.rebound_from).toBe(FIRST);
  });

  awaitTest('binding also repoints the current-session pointer', function* (wait) {
    const fixture = yield* bench();
    const seat = yield* fixture.seat(FIRST, 'codex-first-model');
    ok((yield* wait(captureUse(fixture, seat))).result);
    yield* wait(
      withTestFileSystem((files) =>
        Effect.gen(function* () {
          const current = yield* files.readFileString(
            path.join(fixture.workspace.stateRoot, 'current')
          );
          expect(current).toBe(`${path.join(FIRST, 'codex-first-model.json')}\n`);
        }).pipe(Effect.orDie)
      )
    );
    expect(yield* Effect.either(fixture.store.sessionPath(''))).toEqual(
      Either.right(seat)
    );
  });

  awaitTest('a whitespace-only target is bare `use`, not a path', function* (wait) {
    expect(failure((yield* wait(captureUse(yield* bench(), '   '))).result)).toContain(
      'not bound to a seat'
    );
  });
});

describe('the CLI surface', () => {
  awaitTest('the positional target is optional — bare `play use` reports the seat', function* (wait) {
    const fixture = yield* bench();
    const seat = yield* fixture.seat(FIRST, 'codex-first-model');
    expect((yield* wait(cli(fixture, ['use', seat]))).failure).toBeNull();
    const report = yield* wait(cli(fixture, ['use']));
    expect(report.failure).toBeNull();
    expect(report.out).toContain(`playing ${FIRST} | seat`);
  });

  awaitTest('`play use GAME_ID --json` takes the flag after the positional', function* (wait) {
    const fixture = yield* bench();
    yield* fixture.seat(FIRST, 'codex-first-model');
    const run = yield* wait(cli(fixture, ['use', FIRST, '--json']));
    expect(run.failure).toBeNull();
    expect(gameIdJson(run.out).game_id).toBe(FIRST);
  });
});

describe('PLAY_JSON', () => {
  awaitTest('it turns the report into a payload without a flag', function* (wait) {
    const fixture = yield* bench();
    const seat = yield* fixture.seat(FIRST, 'codex-first-model');
    ok((yield* wait(captureUse(fixture, seat))).result);
    const report = yield* wait(captureUse(fixture, '', { env: { PLAY_JSON: 'yes' } }));
    ok(report.result);
    expect(gameIdJson(report.out).game_id).toBe(FIRST);
  });
});
