/**
 * `join` — the card, the conduct block, and the three writes.
 *
 * Ports `test_v2_health_and_join_render_compact_cards` (the card half),
 * `test_v2_join_card_and_state_header_carry_the_same_contract`,
 * `test_join_identity_defaults_come_from_playconfig`,
 * `test_join_reports_and_saves_exact_timing_contract`,
 * `test_join_rejects_controller_label_different_from_requested_name`,
 * `test_full_control_join_advertises_capability_and_never_prints_v1_loop`,
 * `test_full_control_join_rejects_unplayable_terminal_or_error_result`,
 * `test_join_binds_this_workspace_and_a_second_join_rebinds_it` and
 * `test_stale_invite_rejection_names_owner_recovery_command`.
 *
 * The invariant behind most of them: **nothing is written until the whole join
 * result has been proved**, and the bearer token never reaches stdout, stderr
 * or the seat-binding file.
 */
import { afterEach, describe, expect, test } from 'bun:test';
import { Command, HelpDoc, ValidationError } from '@effect/cli';
import { BunContext } from '@effect/platform-bun';
import { Effect, Either, Layer, Option, Schema } from 'effect';
import type { MappedError } from 'src/cli-main';
import { FULL_CONTROL_V2, SEAT_BINDING_NAME } from 'src/constants';
import { compactJson } from 'src/services/json-output';
import {
  V2_PROTOCOL_CARD,
  deadlineText,
  joinGuidance,
  renderJoin,
  seatBindingLine,
  timingModeText,
} from 'src/render/join';
import {
  applyPlayDefaults,
  commandJoin,
  joinCommand,
  type JoinArgs,
} from 'src/commands/join.cmd';
import { type Http, httpLayer } from 'src/services/http';
import {
  type PrivateFs,
  type PrivateFsApi,
  type Workspace,
  type WorkspacePaths,
} from 'src/services/private-fs';
import {
  SessionStore,
  emptyV2ClientState,
  sessionKey,
  sessionStoreFor,
} from 'src/services/session-store';
import type { JsonObject } from 'src/schema/primitives';
import type { SeatBinding } from 'src/services/session-store';
import {
  recordingFetch,
  scratchWorkspace,
  type FakeRoute,
  type RecordedRequest,
  type Scratch,
} from 'test/_fixtures';
import { captureEffect } from 'test/_capture';
import { awaitTest, effectTest, provideTestLayer } from 'test/_effect-test';
import { leftValue, observedFirst, parseFixtureObject } from 'test/_expect';
import { fileSystem, path } from 'test/_test-platform';

type FetchArguments = Parameters<typeof fetch>;

const completeFetch = (
  handler: (...args: FetchArguments) => ReturnType<typeof fetch>
): typeof fetch => Object.assign(handler, { preconnect: fetch.preconnect });

const GAME_ID = 'game_Hsit9YEuBjKdJPPouFoGVYlk';
const SECOND_ID = 'game_9SecondBoundGame00000000';
const CONTROLLER = 'codex-bind-model';
const BASE = 'http://127.0.0.1:8765';
const TOKEN = 'agent-v2-secret';

const scratches: Scratch[] = [];

afterEach(() =>
  Effect.runPromise(
    Effect.forEach(scratches.splice(0), (scratch) => scratch.cleanup, { discard: true })
  )
);

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

const schema = {
  empty: emptyV2ClientState,
  validate: () => Effect.void,
  cursorExpired: (): boolean => false,
};

interface Bench {
  readonly scratch: Scratch;
  readonly root: string;
  readonly workspace: WorkspacePaths;
  readonly files: PrivateFsApi;
  readonly layer: (
    fetchImpl: typeof fetch
  ) => Layer.Layer<Workspace | PrivateFs | SessionStore | Http>;
}

const bench = (): Effect.Effect<Bench> =>
  Effect.map(scratchWorkspace(), (scratch) => {
    scratches.push(scratch);
    const { workspace, files } = scratch;
    const store = sessionStoreFor(workspace, files, schema, {});
    return {
      scratch,
      root: workspace.root,
      workspace,
      files,
      layer: (fetchImpl) =>
        Layer.mergeAll(scratch.layer, Layer.succeed(SessionStore, store), httpLayer(fetchImpl)),
    };
  });

const stageInvite = (fixture: Bench, gameId: string): Effect.Effect<void> =>
  Effect.gen(function* () {
    const invites = path.join(fixture.workspace.root, '.invites');
    yield* fileSystem.makeDirectory(invites, { mode: 0o700, recursive: true });
    const target = path.join(invites, `${gameId}.json`);
    yield* fileSystem.writeFileString(target, inviteJson(gameId));
    yield* fileSystem.chmod(target, 0o600);
  }).pipe(Effect.orDie);

interface Captured {
  readonly out: string;
  readonly err: string;
  readonly requests: ReadonlyArray<RecordedRequest>;
}

const inviteJson = (gameId: string): string =>
  compactJson({
    schema_version: 1,
    game_id: gameId,
    service_url: BASE,
    join_token: 'join-secret',
  });

const v2Result = (
  gameId: string,
  controller: string,
  overrides: JsonObject = {}
): JsonObject => {
  const prefix = `${BASE}/v2/games/${gameId}/me`;
  return {
    game_id: gameId,
    agent_id: 'agent-v2',
    agent_token: TOKEN,
    place: 1,
    seat_id: 'place-1',
    player_name: 'AgentPlace1',
    controller_label: controller,
    controller_metadata: {},
    controller_fingerprint: 'f'.repeat(64),
    control_protocol: FULL_CONTROL_V2,
    supported_control_protocols: [FULL_CONTROL_V2],
    objective: 'Win by the configured evaluation objective.',
    max_turns: 321,
    turns_remaining: null,
    v2_transport_available: true,
    health_url: `${prefix}/health`,
    state_url: `${prefix}/state`,
    legal_actions_url: `${prefix}/legal-actions`,
    batches_url: `${prefix}/batches`,
    receipts_url: `${prefix}/receipts/{batch_id}`,
    wait_url: `${prefix}/wait`,
    openapi_url: `${BASE}/v2/openapi.json`,
    state: 'running',
    timing_mode: 'default',
    action_timeout_s: 600,
    ...overrides,
  };
};

const args = (overrides: Partial<JoinArgs> = {}): JoinArgs => ({
  gameId: GAME_ID,
  name: CONTROLLER,
  place: '',
  invite: '',
  joinToken: '',
  json: false,
  ...overrides,
});

const v2Plan = (overrides: JsonObject = {}) => ({
  status: { body: { control_protocol: FULL_CONTROL_V2 } },
  join: { body: v2Result(GAME_ID, CONTROLLER, overrides) },
});

/** Run one join with the supervisor answering `health`, `status` and `join`. */
const join = (
  fixture: Bench,
  plan: {
    readonly status?: FakeRoute;
    readonly join: FakeRoute;
  },
  overrides: Partial<JoinArgs> = {}
): Effect.Effect<{
  readonly result: Either.Either<void, { readonly message: string }>;
  readonly captured: Captured;
}> => {
  const recorder = recordingFetch(
    new Map<string, FakeRoute>([
      ['/join', plan.join],
      ['/status', plan.status ?? { body: {} }],
      ['/health', { body: {} }],
    ])
  );
  return captureEffect(
    Effect.either(
      provideTestLayer(commandJoin(args(overrides), {}), fixture.layer(recorder.fetch))
    )
  ).pipe(
    Effect.map(({ value, captured }) => ({
      result: value,
      captured: {
        out: captured.out.join('\n'),
        err: captured.err.join('\n'),
        requests: recorder.requests,
      },
    }))
  );
};

const failure = <A>(either: Either.Either<A, { readonly message: string }>): string => {
  expect(Either.isLeft(either)).toBe(true);
  return leftValue(either).message;
};

const ok = <A, E>(either: Either.Either<A, E>): void => {
  if (Either.isLeft(either)) {
    throw new Error(`expected success, got ${JSON.stringify(either.left)}`);
  }
};

const stateRootEntries = (fixture: Bench): Effect.Effect<ReadonlyArray<string>> =>
  fileSystem.readDirectory(fixture.workspace.stateRoot).pipe(Effect.orDie);

const fileMode = (target: string): Effect.Effect<number> =>
  Effect.map(fileSystem.stat(target).pipe(Effect.orDie), (stat) => stat.mode & 0o777);

const sessionFilesOf = (
  fixture: Bench,
  gameId: string
): Effect.Effect<ReadonlyArray<string>> =>
  Effect.gen(function* () {
    const directory = path.join(fixture.workspace.stateRoot, gameId);
    const exists = yield* fileSystem.exists(directory);
    if (!exists) return [];
    const names = yield* fileSystem.readDirectory(directory);
    return names
      .filter((name) => name.endsWith('.json'))
      .map((name) => path.join(directory, name));
  }).pipe(Effect.orDie);

const parseJsonText = (text: string): JsonObject => parseFixtureObject(text);

const readJsonAt = (target: string): Effect.Effect<JsonObject> =>
  Effect.map(fileSystem.readFileString(target).pipe(Effect.orDie), parseJsonText);

const protocolLeadLine = (): string => observedFirst(V2_PROTOCOL_CARD);

const binding = (gameId: string): Option.Option<SeatBinding> =>
  Option.some({ gameId, session: '/x', relative: 'x', boundAt: '' });

const playConfigIdentity = { gameId: '', name: '', place: '' };

const playConfigText = (value: JsonObject | string): string => {
  const asString = Schema.decodeUnknownEither(Schema.String)(value);
  return Either.isRight(asString) ? asString.right : compactJson(value);
};

const writePlayConfig = (fixture: Bench, value: JsonObject | string): Effect.Effect<void> =>
  fileSystem
    .writeFileString(path.join(fixture.root, '.playconfig.json'), playConfigText(value))
    .pipe(Effect.orDie, Effect.asVoid);

const writeInvalidUtf8PlayConfig = (fixture: Bench): Effect.Effect<void> =>
  fileSystem
    .writeFile(path.join(fixture.root, '.playconfig.json'), invalidUtf8PlayConfig())
    .pipe(Effect.orDie, Effect.asVoid);

const runPlayDefaults = (fixture: Bench, given = playConfigIdentity) =>
  Effect.either(applyPlayDefaults(fixture.workspace, given));

const stagedJoin = (
  body: JsonObject,
  status: FakeRoute = { body: {} }
): Effect.Effect<{ readonly fixture: Bench; readonly message: string }> =>
  Effect.gen(function* () {
    const fixture = yield* bench();
    yield* stageInvite(fixture, GAME_ID);
    const { result } = yield* join(fixture, { status, join: { body } });
    return { fixture, message: failure(result) };
  });

const cliFailureText = (error: MappedError | ValidationError.ValidationError): string => {
  if (ValidationError.isValidationError(error)) return HelpDoc.toAnsiText(error.error);
  if (error._tag === 'ExitCodeSignal') return String(error.code);
  return error.message;
};

const joinCli = (
  fixture: Bench,
  argv: ReadonlyArray<string>,
  plan: FakeRoute = { body: v2Result(GAME_ID, CONTROLLER) }
): Effect.Effect<{ readonly failure: string | null; readonly out: string }> => {
  const recorder = recordingFetch(
    new Map<string, FakeRoute>([
      ['/join', plan],
      ['/status', { body: { control_protocol: FULL_CONTROL_V2 } }],
      ['/health', { body: {} }],
    ])
  );
  const root = Command.make('play', {}, () => Effect.void).pipe(
    Command.withSubcommands([joinCommand])
  );
  return captureEffect(
    Effect.either(
      provideTestLayer(
        Command.run(root, { name: 'play', version: '0.1.0' })(['bun', 'play', ...argv]),
        Layer.merge(fixture.layer(recorder.fetch), BunContext.layer)
      )
    )
  ).pipe(
    Effect.map(({ value, captured }) => ({
      failure: value._tag === 'Left' ? cliFailureText(value.left) : null,
      out: captured.out.join('\n'),
    }))
  );
};

const unplayableJoinCases: ReadonlyArray<readonly [string, JsonObject, string]> = [
  [
    'transport',
    { v2_transport_available: false, state: 'starting' },
    'the full-control-v2 transport did not become playable; stop and tell the game owner',
  ],
  [
    'terminal',
    { v2_transport_available: true, state: 'failed' },
    'the full-control-v2 transport did not become playable; stop and tell the game owner',
  ],
  [
    'error',
    { v2_transport_available: true, state: 'running', error: 'sidecar startup failed' },
    'the full-control-v2 transport did not become playable; stop and tell the game owner',
  ],
  [
    'availability',
    { v2_transport_available: 'yes' },
    'the v2 join result omitted transport availability',
  ],
  [
    'protocol',
    { supported_control_protocols: [] },
    'the v2 join result omitted the negotiated protocol',
  ],
];

const invalidUtf8PlayConfig = (): Uint8Array => {
  const prefix = new TextEncoder().encode(
    `{"schema_version":1,"game_id":"${GAME_ID}","name":"co`
  );
  const suffix = new TextEncoder().encode('dex","place":null}');
  const bytes = new Uint8Array(prefix.length + 1 + suffix.length);
  bytes.set(prefix, 0);
  bytes[prefix.length] = 0xff;
  bytes.set(suffix, prefix.length + 1);
  return bytes;
};

// ---------------------------------------------------------------------------
// The protocol card
// ---------------------------------------------------------------------------

describe('the one protocol card', () => {
  test('it opens on the alias dialect and the wire contract', () => {
    expect(observedFirst(V2_PROTOCOL_CARD).startsWith('ALIASES')).toBe(true);
    expect(V2_PROTOCOL_CARD[0]).toContain('dies with its revision');
    expect(V2_PROTOCOL_CARD[0]).toContain("the wire carries the server's opaque ID");
    expect(V2_PROTOCOL_CARD[1]?.startsWith('ERRORS carry their own remedy')).toBe(true);
  });

  test('it never leaks a state token', () => {
    expect(V2_PROTOCOL_CARD.join('\n')).not.toContain('state_token');
  });

  test('it is exactly the twenty lines join and state/header.txt share', () => {
    expect(V2_PROTOCOL_CARD).toHaveLength(20);
  });
});

// ---------------------------------------------------------------------------
// _render_join
// ---------------------------------------------------------------------------

const card = (overrides: JsonObject = {}, result: JsonObject = { state: 'running' }) =>
  renderJoin(
    {
      game_id: GAME_ID,
      controller_label: 'codex-test-model',
      place: 1,
      player_name: 'AgentPlace1',
      control_protocol: FULL_CONTROL_V2,
      timing_mode: 'default',
      action_timeout_s: 180,
      ...overrides,
    },
    result
  );

describe('the join card', () => {
  test('the head line names the seat, the protocol, the state and the timing', () => {
    const lines = card({
      objective: 'Maximize final score',
      max_turns: 5000,
      turns_remaining: null,
    });
    expect(lines[0]).toBe(
      `joined ${GAME_ID} as codex-test-model | seat 1 AgentPlace1 | ` +
        'proto full-control-v2 | state running | timing default 180s per turn'
    );
  });

  test('the second line is the binding, and no session path is ever printed', () => {
    const lines = card();
    expect(lines[1]).toBe(
      `this workspace is now playing ${GAME_ID} — commands need no --session`
    );
    expect(lines.join('\n')).not.toContain('codex-test.json');
    expect(lines.join('\n')).not.toContain('.sessions');
  });

  test('the evaluation frame prints only when the seat carries one', () => {
    expect(
      card({ objective: 'Maximize final score', max_turns: 5000, turns_remaining: null })[2]
    ).toBe('objective Maximize final score | max_turns 5000 | turns_remaining -');
    expect(card()[2]?.startsWith('objective ')).toBe(false);
  });

  test('a timeout-free seat says so instead of printing a null', () => {
    expect(card({ timing_mode: 'infinite', action_timeout_s: null })[0]).toContain(
      'timing infinite no deadline'
    );
  });

  test('a full-control-v2 seat gets the whole card and the header pointer', () => {
    const lines = card();
    for (const line of V2_PROTOCOL_CARD) expect(lines).toContain(line);
    expect(lines.some((line) => line.includes('state/header.txt'))).toBe(true);
    expect(lines.some((line) => line.includes('just turn'))).toBe(true);
    expect(lines.some((line) => line.includes('--json'))).toBe(true);
    expect(lines.join('\n')).not.toContain('agent_token');
  });

  test('a strategic-v1 seat never sees the v2 card', () => {
    const lines = card({ control_protocol: 'strategic-v1' }, { state: 'lobby' });
    expect(lines).not.toContain(protocolLeadLine());
    expect(lines).toContain('PROTOCOL strategic-v1 — poll a turn, submit one action.');
    expect(lines).toContain('  just next --after_turn LAST_TURN');
    expect(lines).toContain('  just act --turn TURN --observation_id ID --action JSON');
  });

  test('a rebind says which game it left, once', () => {
    expect(card({}, { state: 'running' })[1]).not.toContain('rebound');
    expect(
      renderJoin(
        { game_id: GAME_ID, controller_label: 'x', control_protocol: 'strategic-v1' },
        {},
        binding(SECOND_ID)
      )[1]
    ).toBe(
      `this workspace is now playing ${GAME_ID}, rebound from ${SECOND_ID} — ` +
        'commands need no --session'
    );
  });
});

describe('_seat_binding_line', () => {
  test('a fresh binding, a cross-game rebind and a same-game rebind read differently', () => {
    expect(seatBindingLine(GAME_ID)).toBe(
      `this workspace is now playing ${GAME_ID} — commands need no --session`
    );
    expect(seatBindingLine(GAME_ID, binding(SECOND_ID))).toContain(
      `rebound from ${SECOND_ID}`
    );
    expect(seatBindingLine(GAME_ID, binding(GAME_ID))).toContain(
      'rebound to another seat in the same game'
    );
  });
});

describe('the deadline sentence', () => {
  test('None, a number and a drifted value each have their own words', () => {
    expect(deadlineText(null)).toBe('no agent deadline');
    expect(deadlineText(600)).toBe('600 seconds per agent turn');
    expect(deadlineText(1.5)).toBe('1.5 seconds per agent turn');
    expect(deadlineText('soon')).toBe('deadline unavailable');
  });
});

// ---------------------------------------------------------------------------
// .playconfig.json
// ---------------------------------------------------------------------------

describe('_apply_play_defaults', () => {
  effectTest('a pre-configured workspace fills game, name and place', () =>
    Effect.gen(function* () {
      const fixture = yield* bench();
      yield* writePlayConfig(fixture, {
        schema_version: 1,
        game_id: GAME_ID,
        name: CONTROLLER,
        place: 2,
      });
      expect(yield* runPlayDefaults(fixture)).toEqual(
        Either.right({ gameId: GAME_ID, name: CONTROLLER, place: '2' })
      );
    })
  );

  effectTest('explicit arguments always win', () =>
    Effect.gen(function* () {
      const fixture = yield* bench();
      yield* writePlayConfig(fixture, {
        schema_version: 1,
        game_id: GAME_ID,
        name: CONTROLLER,
        place: 2,
      });
      expect(
        yield* runPlayDefaults(fixture, {
          gameId: SECOND_ID,
          name: 'pi-gpt-5.5',
          place: '',
        })
      ).toEqual(Either.right({ gameId: SECOND_ID, name: 'pi-gpt-5.5', place: '2' }));
    })
  );

  effectTest('a missing config leaves every argument untouched', () =>
    Effect.gen(function* () {
      const fixture = yield* bench();
      expect(yield* runPlayDefaults(fixture)).toEqual(Either.right(playConfigIdentity));
    })
  );

  effectTest('a malformed config fails closed rather than guessing', () =>
    Effect.gen(function* () {
      const fixture = yield* bench();
      yield* writePlayConfig(fixture, {
        schema_version: 1,
        game_id: 'nope',
        name: 'x',
        place: null,
      });
      expect(failure(yield* runPlayDefaults(fixture))).toContain('.playconfig.json');
    })
  );

  effectTest('unparseable JSON names the file too', () =>
    Effect.gen(function* () {
      const fixture = yield* bench();
      yield* writePlayConfig(fixture, '{');
      expect(failure(yield* runPlayDefaults(fixture))).toContain('invalid .playconfig.json:');
    })
  );

  effectTest('inherited config fields are rejected', () =>
    Effect.gen(function* () {
      const fixture = yield* bench();
      const inherited = Object.create({
        schema_version: 1,
        game_id: GAME_ID,
        name: CONTROLLER,
      });
      Object.defineProperty(inherited, 'place', { value: null, enumerable: true });
      yield* writePlayConfig(fixture, inherited);
      expect(failure(yield* runPlayDefaults(fixture))).toContain('.playconfig.json');
    })
  );

  effectTest('an own __proto__ payload cannot provide config fields', () =>
    Effect.gen(function* () {
      const fixture = yield* bench();
      yield* writePlayConfig(
        fixture,
        `{"__proto__":{"schema_version":1,"game_id":"${GAME_ID}","name":"${CONTROLLER}"},"place":null}`
      );
      expect(failure(yield* runPlayDefaults(fixture))).toContain('.playconfig.json');
    })
  );

  effectTest('a name that is only CPython whitespace is a malformed config', () =>
    Effect.gen(function* () {
      // `not raw["name"].strip()`.  `.trim()` calls `"\x1f"` non-blank and would
      // accept it as a controller name.
      for (const name of ['', '', '', ' \t\n']) {
        const fixture = yield* bench();
        yield* writePlayConfig(fixture, {
          schema_version: 1,
          game_id: GAME_ID,
          name,
          place: null,
        });
        expect(failure(yield* runPlayDefaults(fixture))).toContain('.playconfig.json');
      }
    })
  );

  effectTest('an argument that is only CPython whitespace is still omitted', () =>
    Effect.gen(function* () {
      const fixture = yield* bench();
      yield* writePlayConfig(fixture, {
        schema_version: 1,
        game_id: GAME_ID,
        name: CONTROLLER,
        place: 2,
      });
      expect(
        yield* runPlayDefaults(fixture, { gameId: '', name: '', place: '' })
      ).toEqual(Either.right({ gameId: GAME_ID, name: CONTROLLER, place: '2' }));
    })
  );

  effectTest('invalid UTF-8 is invalid .playconfig.json, not U+FFFD', () =>
    Effect.gen(function* () {
      // `read_text(encoding="utf-8")` raises `UnicodeDecodeError`, which
      // `except ValueError` turns into the refusal.  Node's `'utf8'` reader
      // would substitute and accept a config CPython refuses.
      const fixture = yield* bench();
      yield* writeInvalidUtf8PlayConfig(fixture);
      expect(failure(yield* runPlayDefaults(fixture))).toContain('invalid .playconfig.json:');
    })
  );

  effectTest('place 0 and a boolean place are both refused', () =>
    Effect.gen(function* () {
      for (const place of [0, true, 'two']) {
        const fixture = yield* bench();
        yield* writePlayConfig(fixture, {
          schema_version: 1,
          game_id: GAME_ID,
          name: CONTROLLER,
          place,
        });
        expect(failure(yield* runPlayDefaults(fixture))).toContain('.playconfig.json');
      }
    })
  );
});

// ---------------------------------------------------------------------------
// command_join, end to end
// ---------------------------------------------------------------------------

describe('a strategic-v1 join', () => {
  awaitTest('it reports and saves the exact timing contract', function* (wait) {
    const fixture = yield* bench();
    yield* stageInvite(fixture, GAME_ID);
    const joined: JsonObject = {
      schema_version: 1,
      game_id: GAME_ID,
      agent_id: 'agent-test',
      agent_token: 'agent-private-secret',
      place: 1,
      seat_id: 'place-1',
      player_name: 'AgentPlace1',
      controller_label: CONTROLLER,
      controller_metadata: {},
      controller_fingerprint: 'f'.repeat(64),
      timing_mode: 'infinite',
      action_timeout_s: null,
    };
    const { result, captured } = yield* wait(join(fixture, { join: { body: joined } }));
    ok(result);
    expect(captured.out).not.toContain('agent-private-secret');
    expect(captured.err).not.toContain('agent-private-secret');
    expect(captured.err).toContain('Joined in infinite timing mode: no agent deadline');
    expect(captured.err).toContain('choose its action directly');

    const files = yield* sessionFilesOf(fixture, GAME_ID);
    expect(files).toHaveLength(1);
    const session = yield* readJsonAt(observedFirst(files));
    expect(session['timing_mode']).toBe('infinite');
    expect(session['action_timeout_s']).toBeNull();
    expect(session['control_protocol']).toBe('strategic-v1');
    expect(session['supported_control_protocols']).toEqual([]);

    const post = captured.requests.find((request) => request.method === 'POST');
    expect(post?.body).not.toContain('supported_control_protocols');
  });

  awaitTest('the session file is named for the controller and written 0600', function* (wait) {
    const fixture = yield* bench();
    yield* stageInvite(fixture, GAME_ID);
    const { result } = yield* wait(
      join(fixture, {
        join: {
          body: {
            game_id: GAME_ID,
            agent_id: 'agent-test',
            agent_token: 'agent-private-secret',
            controller_label: CONTROLLER,
          },
        },
      })
    );
    ok(result);
    const expected = path.join(
      fixture.workspace.stateRoot,
      GAME_ID,
      `${sessionKey(CONTROLLER)}.json`
    );
    expect(yield* fileMode(expected)).toBe(0o600);
  });
});

describe('a join the supervisor answered wrongly', () => {
  awaitTest('a different controller label is refused, and nothing is written', function* (wait) {
    const { fixture, message } = yield* wait(
      stagedJoin({
        game_id: GAME_ID,
        agent_id: 'agent-test',
        agent_token: 'agent-private-secret',
        controller_label: 'claude-returned-model',
      })
    );
    expect(message).toBe(
      'the join response controller label does not match the requested ' +
        'harness-model identity'
    );
    expect(yield* stateRootEntries(fixture)).toEqual([]);
  });

  awaitTest('an incomplete response is refused before anything is written', function* (wait) {
    const { fixture, message } = yield* wait(stagedJoin({ game_id: GAME_ID }));
    expect(message).toBe('the supervisor returned an incomplete join response');
    expect(yield* stateRootEntries(fixture)).toEqual([]);
  });

  awaitTest('a response for another game is refused', function* (wait) {
    const { message } = yield* wait(
      stagedJoin({
        game_id: SECOND_ID,
        agent_id: 'a',
        agent_token: 'b',
        controller_label: CONTROLLER,
      })
    );
    expect(message).toBe('the join response belongs to a different game');
  });

  awaitTest('a protocol switch between preflight and join is refused', function* (wait) {
    const { message } = yield* wait(
      stagedJoin(
        {
          game_id: GAME_ID,
          agent_id: 'a',
          agent_token: 'b',
          controller_label: CONTROLLER,
          control_protocol: 'strategic-v1',
        },
        { body: { control_protocol: FULL_CONTROL_V2 } }
      )
    );
    expect(message).toBe('the join result changed the preflight control protocol');
  });

  awaitTest('an unsupported preflight protocol names the value it saw', function* (wait) {
    const fixture = yield* bench();
    yield* stageInvite(fixture, GAME_ID);
    const { result } = yield* wait(
      join(fixture, {
        status: { body: { control_protocol: 'full-control-v3' } },
        join: { body: {} },
      })
    );
    expect(failure(result)).toBe(
      "game requires unsupported control protocol 'full-control-v3'"
    );
  });
});

describe('a full-control-v2 join', () => {
  awaitTest('a second join re-binds the held seat instead of claiming again', function* (wait) {
    const fixture = yield* bench();
    yield* stageInvite(fixture, GAME_ID);
    const first = yield* wait(join(fixture, v2Plan()));
    ok(first.result);
    expect(first.captured.requests.filter((r) => r.method === 'POST')).toHaveLength(1);

    const second = yield* wait(join(fixture, v2Plan()));
    ok(second.result);
    expect(second.captured.requests).toHaveLength(0);
    expect(second.captured.out).toContain(`already joined ${GAME_ID} as ${CONTROLLER}`);
    expect(second.captured.out).toContain('seat 1 AgentPlace1');
    expect(second.captured.err).toContain('joining again would claim a second seat');
    expect(second.captured.err).toContain(`delete .sessions/${GAME_ID}/`);
    expect(yield* sessionFilesOf(fixture, GAME_ID)).toHaveLength(1);
  });

  awaitTest('a held-but-corrupt session refuses rather than silently re-claiming', function* (wait) {
    const fixture = yield* bench();
    yield* stageInvite(fixture, GAME_ID);
    const first = yield* wait(join(fixture, v2Plan()));
    ok(first.result);
    const file = observedFirst(yield* sessionFilesOf(fixture, GAME_ID));
    yield* fileSystem.writeFileString(file, 'not json').pipe(Effect.orDie);
    const second = yield* wait(join(fixture, v2Plan()));
    expect(Either.isLeft(second.result)).toBe(true);
    const message = leftValue(second.result).message;
    expect(message).toContain(`already holds a session for ${GAME_ID}`);
    expect(message).toContain(`delete .sessions/${GAME_ID}/`);
    expect(second.captured.requests).toHaveLength(0);
  });

  awaitTest('it advertises the capability and never prints the v1 loop', function* (wait) {
    const fixture = yield* bench();
    yield* stageInvite(fixture, GAME_ID);
    const { result, captured } = yield* wait(join(fixture, v2Plan()));
    ok(result);

    const post = captured.requests.find((request) => request.method === 'POST');
    expect(parseJsonText(post?.body ?? '{}')['supported_control_protocols']).toEqual([
      FULL_CONTROL_V2,
    ]);
    expect(captured.err).toContain('Do not use strategic');
    expect(captured.err).not.toContain('just next --session');
    expect(captured.err).toContain('LOBBY FIRST');
    expect(captured.err).toContain('pregame_nations');
    expect(captured.err).toContain('pregame_styles');
    expect(captured.err).toContain('pregame.configure');
    expect(captured.err).toContain('pregame.set_ready');
    expect(captured.err).toContain('Objective: Win by');
    expect(captured.err).toContain('321 maximum');
    expect(captured.err).toContain('remaining turns unavailable until native play starts.');
    expect(captured.err).not.toContain(TOKEN);

    const saved = yield* readJsonAt(observedFirst(yield* sessionFilesOf(fixture, GAME_ID)));
    expect(saved['control_protocol']).toBe(FULL_CONTROL_V2);
    expect(saved['objective']).toBe('Win by the configured evaluation objective.');
    expect(saved['max_turns']).toBe(321);
    expect(saved['turns_remaining']).toBeNull();
  });

  awaitTest('the conduct block leads with the capitalized binding sentence', function* (wait) {
    const fixture = yield* bench();
    yield* stageInvite(fixture, GAME_ID);
    const { captured } = yield* wait(join(fixture, v2Plan()));
    expect(captured.err).toContain(
      `This workspace is now playing ${GAME_ID} — commands need no --session.`
    );
    expect(captured.err).toContain('Timing mode: default; 600 seconds per agent turn.');
  });

  for (const [name, override, expected] of unplayableJoinCases) {
    awaitTest(`an unplayable result (${name}) is refused and writes nothing`, function* (wait) {
      const fixture = yield* bench();
      yield* stageInvite(fixture, GAME_ID);
      const { result } = yield* wait(join(fixture, v2Plan(override)));
      expect(failure(result)).toBe(expected);
      expect(yield* stateRootEntries(fixture)).toEqual([]);
    });
  }

  awaitTest('a cross-origin endpoint is refused by name', function* (wait) {
    const fixture = yield* bench();
    yield* stageInvite(fixture, GAME_ID);
    const { result } = yield* wait(
      join(fixture, v2Plan({ state_url: 'http://evil.test/v2/state' }))
    );
    expect(failure(result)).toBe('the v2 join result has an invalid same-origin state_url');
  });

  awaitTest('a malformed evaluation frame is refused before the transport checks', function* (wait) {
    const fixture = yield* bench();
    yield* stageInvite(fixture, GAME_ID);
    const { result } = yield* wait(join(fixture, v2Plan({ max_turns: 0 })));
    expect(failure(result)).toBe('invalid v2 join result: evaluation context is malformed');
  });
});

describe('the workspace binding', () => {
  awaitTest('a join binds this workspace and a second join rebinds it', function* (wait) {
    const fixture = yield* bench();
    yield* stageInvite(fixture, GAME_ID);
    yield* stageInvite(fixture, SECOND_ID);

    const first = yield* wait(
      join(fixture, {
        status: { body: { control_protocol: FULL_CONTROL_V2 } },
        join: { body: v2Result(GAME_ID, CONTROLLER) },
      })
    );
    ok(first.result);
    expect(first.captured.out).toContain(
      `this workspace is now playing ${GAME_ID} — commands need no --session`
    );
    expect(first.captured.out).not.toContain('.sessions');
    expect(first.captured.out).not.toContain(fixture.root);
    expect(first.captured.err).not.toContain('Session file:');

    const bindingPath = path.join(fixture.workspace.stateRoot, SEAT_BINDING_NAME);
    expect(yield* fileMode(bindingPath)).toBe(0o600);
    const text = yield* fileSystem.readFileString(bindingPath).pipe(Effect.orDie);
    expect(text).not.toContain(TOKEN);
    const saved = parseJsonText(text);
    const sessionName = path.basename(observedFirst(yield* sessionFilesOf(fixture, GAME_ID)));
    expect(saved['game_id']).toBe(GAME_ID);
    expect(saved['session']).toBe(`${GAME_ID}/${sessionName}`);
    expect(saved['bound_at']).toMatch(/^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ$/);

    const second = yield* wait(
      join(
        fixture,
        {
          status: { body: { control_protocol: FULL_CONTROL_V2 } },
          join: { body: v2Result(SECOND_ID, CONTROLLER) },
        },
        { gameId: SECOND_ID }
      )
    );
    ok(second.result);
    expect(second.captured.out).toContain(
      `this workspace is now playing ${SECOND_ID}, rebound from ${GAME_ID} — ` +
        'commands need no --session'
    );
  });
});

describe('a stale invitation', () => {
  awaitTest('a 401 from the join POST names the owner recovery command', function* (wait) {
    const fixture = yield* bench();
    yield* stageInvite(fixture, GAME_ID);
    const { result } = yield* wait(
      join(fixture, {
        join: { status: 401, body: { error: { code: 'unauthorized', message: 'unauthorized' } } },
      })
    );
    const message = failure(result);
    expect(message).toContain('HTTP 401: unauthorized');
    expect(message).toContain(`just invite ${GAME_ID}`);
    expect(message).toContain('The game invitation may be stale.');
  });

  awaitTest('a 500 is passed through without the stale-invite advice', function* (wait) {
    const fixture = yield* bench();
    yield* stageInvite(fixture, GAME_ID);
    const { result } = yield* wait(
      join(fixture, {
        join: { status: 500, body: { error: { code: 'internal_error', message: 'boom' } } },
      })
    );
    expect(failure(result)).toBe('HTTP 500: boom');
  });

  awaitTest('an unreachable supervisor tells the user to stop', function* (wait) {
    const fixture = yield* bench();
    yield* stageInvite(fixture, GAME_ID);
    const failing = completeFetch(() => Promise.reject(new Error('connection refused')));
    const captured = yield* wait(
      Effect.either(provideTestLayer(commandJoin(args(), {}), fixture.layer(failing)))
    );
    expect(failure(captured)).toContain(
      'The assigned game cannot be joined. Stop and tell the user.'
    );
  });
});

describe('--json', () => {
  awaitTest('it prints the raw payload, minus the bearer, plus where it was saved', function* (wait) {
    const fixture = yield* bench();
    yield* stageInvite(fixture, GAME_ID);
    const { result, captured } = yield* wait(
      join(
        fixture,
        {
          status: { body: { control_protocol: FULL_CONTROL_V2 } },
          join: { body: v2Result(GAME_ID, CONTROLLER) },
        },
        { json: true }
      )
    );
    ok(result);
    const payload = parseJsonText(captured.out);
    expect(payload['agent_token']).toBeUndefined();
    expect(payload['session_saved']).toBe(true);
    expect(payload['session_file']).toBe(
      path.join(fixture.workspace.stateRoot, GAME_ID, `${sessionKey(CONTROLLER)}.json`)
    );
    expect(payload['game_id']).toBe(GAME_ID);
    expect(captured.out).not.toContain('ALIASES');
    expect(captured.err).toContain('Joined a full-control-v2 session.');
  });

  awaitTest('PLAY_JSON=1 is exactly --json', function* (wait) {
    const fixture = yield* bench();
    yield* stageInvite(fixture, GAME_ID);
    const recorder = recordingFetch(
      new Map<string, FakeRoute>([
        ['/join', { body: v2Result(GAME_ID, CONTROLLER) }],
        ['/status', { body: { control_protocol: FULL_CONTROL_V2 } }],
        ['/health', { body: {} }],
      ])
    );
    const { value, captured } = yield* wait(
      captureEffect(
        Effect.either(
          provideTestLayer(
            commandJoin(args(), { PLAY_JSON: '1' }),
            fixture.layer(recorder.fetch)
          )
        )
      )
    );
    ok(value);
    const out = captured.out.join('\n');
    expect(out).not.toContain('ALIASES');
    expect(parseJsonText(out)['session_saved']).toBe(true);
  });
});

describe('the flag surface', () => {
  awaitTest('the underscored spellings the protocol card prints are accepted', function* (wait) {
    const fixture = yield* bench();
    yield* stageInvite(fixture, GAME_ID);
    const run = yield* wait(joinCli(fixture, ['join', '--game_id', GAME_ID, '--name', CONTROLLER]));
    expect(run.failure).toBeNull();
    expect(run.out).toContain(`this workspace is now playing ${GAME_ID}`);
  });

  awaitTest('both spellings at once is a refusal, not a silent pick', function* (wait) {
    const fixture = yield* bench();
    yield* stageInvite(fixture, GAME_ID);
    const run = yield* wait(
      joinCli(fixture, [
        'join',
        '--game_id',
        GAME_ID,
        '--game-id',
        GAME_ID,
        '--name',
        CONTROLLER,
      ])
    );
    expect(run.failure).toBe('pass only one of --game-id or --game_id');
  });

  awaitTest('a pre-configured workspace joins with no arguments at all', function* (wait) {
    const fixture = yield* bench();
    yield* stageInvite(fixture, GAME_ID);
    yield* writePlayConfig(fixture, {
      schema_version: 1,
      game_id: GAME_ID,
      name: CONTROLLER,
      place: null,
    });
    const run = yield* wait(joinCli(fixture, ['join']));
    expect(run.failure).toBeNull();
    expect(run.out).toContain(`this workspace is now playing ${GAME_ID}`);
  });

  awaitTest('a bare workspace with no arguments refuses on the game ID', function* (wait) {
    const fixture = yield* bench();
    const run = yield* wait(joinCli(fixture, ['join']));
    expect(run.failure).toBe('a valid assigned game ID is required');
  });
});

describe('the conduct block', () => {
  test('a strategic-v1 seat is told to read the gameplay doc, not the v2 card', () => {
    const guidance = joinGuidance(
      { control_protocol: 'strategic-v1' },
      { timing_mode: 'blitz', action_timeout_s: 60 },
      seatBindingLine(GAME_ID)
    );
    expect(guidance).toContain('Joined in blitz timing mode: 60 seconds per agent turn.');
    expect(guidance).toContain('You—the assigned harness/model—must inspect each observation');
    expect(guidance).toContain('Read docs/gameplay.md');
    expect(guidance).toContain(`This workspace is now playing ${GAME_ID}`);
    expect(guidance).not.toContain('LOBBY FIRST');
  });

  test('a seat with a turn budget prints the remaining turns when it has them', () => {
    const guidance = joinGuidance(
      {
        control_protocol: FULL_CONTROL_V2,
        objective: 'Win',
        max_turns: 321,
        turns_remaining: 300,
      },
      { timing_mode: 'default', action_timeout_s: null },
      seatBindingLine(GAME_ID)
    );
    expect(guidance).toContain('Turn budget: 321 maximum; 300 remaining.');
    expect(guidance).toContain('Timing mode: default; no agent deadline.');
    expect(guidance).toContain('An ambiguous receipt is terminal and must never be replayed');
  });

  test('a missing timing mode reads "unknown" rather than "None"', () => {
    expect(
      joinGuidance({ control_protocol: 'strategic-v1' }, {}, seatBindingLine(GAME_ID))
    ).toContain('Joined in unknown timing mode: no agent deadline.');
  });

  /**
   * `str(result.get("timing_mode") or "unknown")` (client.py:6350).
   *
   * `timing_mode` is a v1 join field with no schema in
   * `full-control-v2.openapi.json`, so CPython's `or` is the drift absorber and
   * this table is what it absorbs.  Every expectation was taken from
   * `python3 -c "str(v or 'unknown')"`, not from the port.
   */
  describe('the timing-mode drift table', () => {
    const rows: ReadonlyArray<readonly [string, JsonObject, string]> = [
      ['a missing key', {}, 'unknown'],
      ['null', { timing_mode: null }, 'unknown'],
      ['the empty string', { timing_mode: '' }, 'unknown'],
      ['zero', { timing_mode: 0 }, 'unknown'],
      ['false', { timing_mode: false }, 'unknown'],
      ['an empty list', { timing_mode: [] }, 'unknown'],
      ['an empty object', { timing_mode: {} }, 'unknown'],
      ['a name', { timing_mode: 'blitz' }, 'blitz'],
      ['true', { timing_mode: true }, 'True'],
      ['a number', { timing_mode: 5 }, '5'],
      ['a fraction', { timing_mode: 2.5 }, '2.5'],
      ['a list', { timing_mode: ['a', 1] }, "['a', 1]"],
      ['an object', { timing_mode: { a: 1, b: null } }, "{'a': 1, 'b': None}"],
    ];

    for (const [label, result, expected] of rows) {
      test(`${label} prints ${expected}`, () => {
        expect(timingModeText(result)).toBe(expected);
      });

      test(`${label} reaches both conduct blocks as ${expected}`, () => {
        expect(
          joinGuidance({ control_protocol: 'strategic-v1' }, result, seatBindingLine(GAME_ID))
        ).toContain(`Joined in ${expected} timing mode: no agent deadline.`);
        expect(
          joinGuidance({ control_protocol: FULL_CONTROL_V2 }, result, seatBindingLine(GAME_ID))
        ).toContain(`Timing mode: ${expected}; no agent deadline.`);
      });
    }
  });
});
