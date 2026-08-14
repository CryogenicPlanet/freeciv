/**
 * `play start` end to end.
 *
 * Ports `test_v2_start_resolves_names_then_configures_then_readies`
 * (test_client.py:6158), `…with_no_arguments_resolves_every_choice_itself`
 * (8643), `…flags_each_override_exactly_what_they_name` (8692),
 * `…falls_back_to_the_lobby_leader_when_the_label_is_unusable` (8725),
 * `…picks_the_same_sex_twice_when_the_lobby_names_none` (8775) and
 * `…fails_closed_when_the_lobby_offers_no_nation` (8816).  The seventh,
 * `…sanitizes_a_label_and_picks_a_sex_deterministically` (8762), is a pure
 * assertion over `_sanitized_leader` and lives in `test/pregame.test.ts`.
 *
 * The cross-unit seams (U11's enumeration, U13's persist/submit, U14's
 * disposition render) are supplied here as test hooks that talk to the same
 * fake supervisor, so the request *order* — the property the Python test
 * actually guards, because the re-enumeration between configure and set_ready
 * is mandatory — is asserted for real rather than stubbed away.
 */
import { afterEach, describe, expect } from 'bun:test';
import { Effect, Either, Layer, Schema } from 'effect';
import { FULL_CONTROL_V2 } from 'src/constants';
import { playerError, type PlayError } from 'src/errors';
import type { ExitCodeSignal } from 'src/exit';
import { liveStartHooks, runStart, type StartHooksFor, type StartOptions } from 'src/commands/start.cmd';
import { isJsonObject, type JsonObject, type JsonValue } from 'src/schema/primitives';
import { httpFor } from 'src/services/http';
import { V2_PROTOCOL_CARD } from 'src/render/join';
import { scalar } from 'src/render/primitives';
import { NOT_READIED_LINE } from 'src/render/pregame';
import { DEFAULT_COMMAND_CARD, mirrorDir } from 'src/services/mirror';
import { orderReceiptOk, type PregameAction, type PregameItem, type StartHooks } from 'src/services/pregame';
import { PrivateFs } from 'src/services/private-fs';
import { batchDisposition } from 'src/services/batch';
import {
  SessionStore,
  credentialsOf,
  sessionStoreFor,
  type Session,
} from 'src/services/session-store';
import { V2Client, v2ClientFor, type V2ClientApi, type V2Credentials } from 'src/services/v2-client';
import { v2StateSchema } from 'src/services/aliases';
import { FIXTURE_AGENT_ID, FIXTURE_GAME_ID, scratchWorkspace, sessionFile, type Scratch } from 'test/_fixtures';
import { captureEffect } from 'test/_capture';
import { awaitTest, provideTestLayer } from 'test/_effect-test';
import { path, withTestFileSystem } from 'test/_test-platform';

const scratches: Scratch[] = [];

afterEach(() =>
  Promise.all(scratches.splice(0).map((scratch) => scratch.cleanup()))
);

// ---------------------------------------------------------------------------
// Wire payloads (the Python test class's helpers, ported)
// ---------------------------------------------------------------------------

const CONTROLLER = 'codex-test-model';
const NATION_ENGLISH = `nation_${'a'.repeat(32)}`;
const NATION_ZULU = `nation_${'c'.repeat(32)}`;
const STYLE_EUROPEAN = `style_${'b'.repeat(32)}`;
const PLAYER = `player_${'f'.repeat(32)}`;

const revision = (number: number): JsonObject => ({
  turn: 0,
  revision: number,
  state_token: `state_${String(number).padStart(32, '0')}`,
});

const LOBBY = revision(4);
const CONFIGURED = revision(5);
const READIED = revision(6);

const envelope = (body: JsonObject): JsonObject => ({
  schema_version: 2,
  control_protocol: FULL_CONTROL_V2,
  game_id: FIXTURE_GAME_ID,
  agent_id: FIXTURE_AGENT_ID,
  ...body,
});

const sectionPage = (
  section: string,
  stateRevision: JsonObject,
  items: ReadonlyArray<JsonValue>
): JsonObject =>
  envelope({
    state_revision: stateRevision,
    page: { section, items: [...items], total_items: items.length, next_cursor: null },
  });

const legalPage = (stateRevision: JsonObject, items: ReadonlyArray<JsonValue>): JsonObject =>
  envelope({
    state_revision: stateRevision,
    page: {
      section: 'legal_actions',
      items: [...items],
      total_items: items.length,
      next_cursor: null,
    },
  });

const pregameAction = (
  stateRevision: JsonObject,
  actionId: string,
  kind: string,
  operation: string,
  label: string,
  schema: JsonObject,
  target: JsonObject
): JsonObject => ({
  action_id: actionId,
  kind,
  label,
  subject: {
    operation,
    actor: { id: PLAYER, type: 'player', name: 'AgentPlace1' },
    target,
    variant: null,
    consuming: false,
    legality: 'legal',
    probability: { kind: 'exact', minimum_percent: 100, maximum_percent: 100 },
  },
  arguments_schema: schema,
  state_revision: stateRevision,
});

const CONFIGURE = pregameAction(
  LOBBY,
  `action_${'1'.repeat(26)}`,
  'pregame.configure',
  'configure',
  'Choose nation, leader, sex, and style',
  {
    type: 'object',
    properties: {
      nation_id: { type: 'string' },
      leader_name: { type: 'string' },
      is_male: { type: 'boolean' },
      style_id: { type: 'string' },
    },
    required: ['nation_id', 'leader_name', 'is_male', 'style_id'],
  },
  { type: 'pregame_configuration' }
);

const SET_READY = pregameAction(
  CONFIGURED,
  `action_${'2'.repeat(26)}`,
  'pregame.set_ready',
  'set_ready',
  'Mark ready',
  {
    type: 'object',
    properties: { ready: { type: 'boolean', enum: [true] } },
    required: ['ready'],
  },
  { type: 'pregame_readiness', desired_ready: true }
);

const lobbyHealth = (gameState = 'lobby', controller = CONTROLLER): JsonObject => ({
  schema_version: 2,
  control_protocol: FULL_CONTROL_V2,
  game_id: FIXTURE_GAME_ID,
  agent: { agent_id: FIXTURE_AGENT_ID, controller_label: controller },
  game_state: gameState,
  seat: { place: 1, seat_id: 'seat_one', player_name: 'Alice' },
  sidecar: { state: 'ready', generation: 1 },
  observation_available: false,
  legal_actions_available: false,
  phase: null,
  last_phase_end: null,
});

const OFFERED: ReadonlyArray<JsonObject> = [
  { id: NATION_ENGLISH, name: 'English', default_style_id: STYLE_EUROPEAN },
  { id: NATION_ZULU, name: 'Zulu', default_style_id: STYLE_EUROPEAN },
];

// ---------------------------------------------------------------------------
// The fake supervisor
// ---------------------------------------------------------------------------

type FetchArguments = Parameters<typeof fetch>;

const completeFetch = (
  handler: (...args: FetchArguments) => ReturnType<typeof fetch>
): typeof fetch => Object.assign(handler, { preconnect: fetch.preconnect });

const urlOf = (input: Parameters<typeof fetch>[0]): string =>
  input instanceof Request ? input.url : new URL(input).href;

const initBodyText = (init: RequestInit | undefined): Promise<string> => {
  if (init?.body === undefined || init?.body === null) return Promise.resolve('{}');
  return new Response(init.body).text();
};

const json = (body: JsonValue, status = 200): Response =>
  new Response(JSON.stringify(body), {
    status,
    headers: { 'content-type': 'application/json' },
  });

interface ResponderOptions {
  readonly nations?: ReadonlyArray<JsonObject>;
  readonly leader?: string;
  readonly sex?: string;
  readonly gameState?: string;
  readonly controller?: string;
  /** Refuse the first N configure batches in the catalog-freshness class. */
  readonly staleConfigures?: number;
}

interface Recorded {
  readonly steps: ReadonlyArray<string>;
  readonly bodies: ReadonlyArray<JsonObject>;
  readonly fetch: typeof fetch;
}

/** `start_responder` (test_client.py:8534), as a `fetch`. */
const startResponder = (options: ResponderOptions = {}): Recorded => {
  const steps: string[] = [];
  const bodies: JsonObject[] = [];
  const nations = options.nations ?? OFFERED;
  const catalogs = [legalPage(LOBBY, [CONFIGURE]), legalPage(CONFIGURED, [SET_READY])];
  const handler = (input: Parameters<typeof fetch>[0], init?: RequestInit): ReturnType<typeof fetch> =>
    Effect.runPromise(
      Effect.gen(function* () {
        const url = urlOf(input);
        if (url.includes('/health')) {
          steps.push('health');
          return json(lobbyHealth(options.gameState ?? 'lobby', options.controller ?? CONTROLLER));
        }
        if (url.includes('section=pregame_nations')) {
          steps.push('nations');
          return json(sectionPage('pregame_nations', LOBBY, nations));
        }
        if (url.includes('section=pregame_styles')) {
          steps.push('styles');
          return json(
            sectionPage('pregame_styles', LOBBY, [{ id: STYLE_EUROPEAN, name: 'European' }])
          );
        }
        if (url.includes('section=overview')) {
          steps.push('overview');
          return json(
            sectionPage('overview', LOBBY, [
              {
                client_state: 'preparing',
                turn: 0,
                phase: null,
                player: {
                  id: PLAYER,
                  leader_name: options.leader ?? 'Boudica',
                  nation: null,
                  sex: options.sex ?? 'female',
                  style: null,
                  ready: false,
                },
              },
            ])
          );
        }
        if (url.includes('legal-actions')) {
          steps.push('legal');
          const seen = steps.filter((step) => step === 'legal').length;
          return json(catalogs[Math.min(seen, catalogs.length) - 1] ?? null);
        }
        steps.push('batch');
        const bodyText = yield* Effect.promise(() => initBodyText(init));
        const raw = yield* Schema.decodeUnknown(Schema.parseJson(Schema.Unknown))(bodyText);
        if (!isJsonObject(raw)) {
          return yield* Effect.die(new Error('the batch body was not an object'));
        }
        bodies.push(raw);
        const staleBudget = options.staleConfigures ?? 0;
        const batchesSeen = steps.filter((step) => step === 'batch').length;
        if (batchesSeen <= staleBudget) {
          return json({
            schema_version: 2,
            control_protocol: FULL_CONTROL_V2,
            game_id: FIXTURE_GAME_ID,
            agent_id: FIXTURE_AGENT_ID,
            batch_id: scalar(raw['batch_id'] ?? null),
            receipt_state: 'rejected',
            idempotent: false,
            state_revision: LOBBY,
            error: {
              schema_version: 2,
              control_protocol: FULL_CONTROL_V2,
              error: {
                code: 'illegal_action',
                message:
                  'The style_id field is not one of the IDs the pregame_styles ' +
                  'section advertises at this revision; only that field is wrong.',
                retryable: false,
                details: { rejection_reason: 'pregame_style_unknown' },
              },
              state_revision: LOBBY,
            },
            observation: null,
          });
        }
        const sent = raw['state_revision'];
        const sameRevision = isJsonObject(sent) && sent['revision'] === LOBBY['revision'];
        return json({
          schema_version: 2,
          control_protocol: FULL_CONTROL_V2,
          game_id: FIXTURE_GAME_ID,
          agent_id: FIXTURE_AGENT_ID,
          batch_id: scalar(raw['batch_id'] ?? null),
          receipt_state: 'applied',
          idempotent: true,
          state_revision: sameRevision ? CONFIGURED : READIED,
          error: null,
          observation: null,
        });
      })
    );
  return { steps, bodies, fetch: completeFetch(handler) };
};

// ---------------------------------------------------------------------------
// The cross-unit seams, as test hooks
// ---------------------------------------------------------------------------

const objectsAt = (page: JsonObject): ReadonlyArray<JsonObject> => {
  const body = page['page'];
  const items = isJsonObject(body) ? body['items'] : undefined;
  return Array.isArray(items) ? items.filter(isJsonObject) : [];
};

const asAction = (descriptor: JsonObject): PregameAction => ({
  action_id: scalar(descriptor['action_id'] ?? null),
  kind: scalar(descriptor['kind'] ?? null),
  argument_schema: (descriptor['arguments_schema'] ?? null),
});

/**
 * `_drain_legal_unlocked` + `_resolve_kind_action` + `_persist_batch_for_action`
 * + `_submit_persisted_batch` + `_render_disposition`, reduced to exactly the
 * behaviour `start` depends on: enumerate once per cache miss, persist the
 * descriptor's own revision, POST it, and name the receipt.
 */
const hooksFor = (
  client: V2ClientApi,
  session: Session,
  choose: (items: ReadonlyArray<PregameItem>) => PregameItem
): StartHooks => {
  const credentials: V2Credentials = credentialsOf(session);
  let cached: ReadonlyArray<JsonObject> = [];
  let issued = 0;
  const persisted = new Map<string, JsonObject>();
  const drain = Effect.gen(function* () {
    const page = yield* client.get(credentials, '/legal-actions');
    cached = objectsAt(page);
  });
  const cachedOf = (kind: string): JsonObject | null => {
    const matches = cached.filter((descriptor) => descriptor['kind'] === kind);
    return matches.length === 1 ? (matches[0] ?? null) : null;
  };
  return {
    mirrorPage: () => Effect.void,
    choose,
    receiptOk: orderReceiptOk,
    drainLegal: () => drain,
    resolveKindAction: (kind, remedy) =>
      Effect.gen(function* () {
        if (cachedOf(kind) === null) yield* drain;
        const found = cachedOf(kind);
        if (found === null) {
          return yield* playerError(
            `no ${kind} action is enumerable for this seat right now; ${remedy}`
          );
        }
        return asAction(found);
      }),
    persistBatchForAction: (actionId, argumentValues) =>
      Effect.sync(() => {
        issued += 1;
        const batchId = `batch_${String(issued).padStart(24, '0')}`;
        const descriptor = cached.find((entry) => entry['action_id'] === actionId);
        persisted.set(batchId, {
          schema_version: 2,
          control_protocol: FULL_CONTROL_V2,
          game_id: credentials.gameId,
          agent_id: FIXTURE_AGENT_ID,
          batch_id: batchId,
          state_revision: (descriptor?.['state_revision'] ?? null),
          commands: [{ action_id: actionId, arguments: argumentValues }],
        });
        return batchId;
      }),
    submitPersistedBatch: (batchId) =>
      Effect.gen(function* () {
        const body = persisted.get(batchId);
        if (body === undefined) {
          return yield* Effect.die(new Error(`no persisted batch ${batchId}`));
        }
        const wire = yield* client.post(credentials, '/batches', body);
        return {
          disposition: yield* batchDisposition(session, batchId, 'receipt_terminal', {
            receipt: wire,
          }),
          warning: null,
          exitCode: 0,
        };
      }),
    renderDisposition: (disposition, intent) =>
      Effect.succeed([
        `${intent} → ${disposition.receipt?.receipt_state ?? 'unknown'} rev5/t0`,
      ]),
  };
};

// ---------------------------------------------------------------------------
// The fixture
// ---------------------------------------------------------------------------

interface Fixture {
  readonly layer: Layer.Layer<SessionStore | V2Client | PrivateFs>;
  readonly sessionPath: string;
  readonly hooks: StartHooksFor;
}

const fixture = (
  recorded: Recorded,
  options: {
    readonly controller?: string;
    readonly choose?: (items: ReadonlyArray<PregameItem>) => PregameItem;
  } = {}
): Fixture => {
  const scratch = scratchWorkspace();
  scratches.push(scratch);
  const sessionPath = path.join(scratch.workspace.stateRoot, FIXTURE_GAME_ID, 'codex-test.json');
  Effect.runSync(
    scratch.files.writeJson(
      sessionPath,
      sessionFile({ controller_label: options.controller ?? CONTROLLER })
    )
  );
  const store = sessionStoreFor(scratch.workspace, scratch.files, v2StateSchema, {});
  const client = v2ClientFor(httpFor(recorded.fetch), () => Effect.void);
  const choose =
    options.choose ??
    ((items: ReadonlyArray<PregameItem>): PregameItem => {
      const first = items[0];
      if (first === undefined) throw new Error('the draw must not happen');
      return first;
    });
  return {
    sessionPath,
    layer: Layer.mergeAll(
      Layer.succeed(SessionStore, store),
      Layer.succeed(V2Client, client),
      Layer.succeed(PrivateFs, scratch.files)
    ),
    hooks: (_sessionPath, session) => Effect.succeed(hooksFor(client, session, choose)),
  };
};

const startOptions = (sessionPath: string, overrides: Partial<StartOptions> = {}): StartOptions => ({
  session: sessionPath,
  nation: '',
  leader: '',
  style: '',
  male: false,
  female: false,
  json: false,
  ...overrides,
});

const run = (
  fix: Fixture,
  overrides: Partial<StartOptions> = {}
): Effect.Effect<ReadonlyArray<string>, PlayError | ExitCodeSignal> =>
  captureEffect(
    provideTestLayer(runStart(startOptions(fix.sessionPath, overrides), fix.hooks), fix.layer)
  ).pipe(Effect.map(({ captured }) => captured.out));

/** Rebuild a fixture's hooks with one or two seams replaced. */
const withHooks = (
  fix: Fixture,
  over: (hooks: StartHooks, session: Session) => StartHooks
): Fixture => ({
  ...fix,
  hooks: (sessionPath, session) =>
    Effect.map(fix.hooks(sessionPath, session), (hooks) => over(hooks, session)),
});

interface Captured {
  readonly out: ReadonlyArray<string>;
  readonly err: ReadonlyArray<string>;
  readonly outcome: Either.Either<void, PlayError | ExitCodeSignal>;
}

/** Both streams and the outcome, for the paths that use all three. */
const runRaw = (
  fix: Fixture,
  overrides: Partial<StartOptions> = {}
): Effect.Effect<Captured> =>
  captureEffect(
    Effect.either(
      provideTestLayer(runStart(startOptions(fix.sessionPath, overrides), fix.hooks), fix.layer)
    )
  ).pipe(
    Effect.map(({ value: outcome, captured }) => ({
      out: captured.out,
      err: captured.err,
      outcome,
    }))
  );

const refuse = (fix: Fixture, overrides: Partial<StartOptions> = {}): Effect.Effect<string> =>
  Effect.gen(function* () {
    const captured = yield* runRaw(fix, overrides);
    if (Either.isRight(captured.outcome)) {
      return yield* Effect.die(new Error('expected a refusal'));
    }
    const failure = captured.outcome.left;
    if (failure._tag !== 'PlayerError') {
      return yield* Effect.die(new Error(`expected PlayerError, got ${failure._tag}`));
    }
    return failure.message;
  });

const argumentsOf = (body: JsonObject): JsonValue => {
  const commands = body['commands'];
  const first = Array.isArray(commands) ? commands[0] : undefined;
  return isJsonObject(first) ? ((first['arguments'] ?? null)) : null;
};

const commandOf = (body: JsonObject): JsonValue => {
  const commands = body['commands'];
  const first = Array.isArray(commands) ? commands[0] : undefined;
  return isJsonObject(first) ? first : null;
};

const startJsonSchema = Schema.parseJson(
  Schema.Struct({
    schema_version: Schema.Literal(1),
    command: Schema.Literal('start'),
    nation: Schema.String,
    leader: Schema.String,
    is_male: Schema.Boolean,
    style_id: Schema.String,
    dispositions: Schema.Array(Schema.Unknown),
  })
);

// ---------------------------------------------------------------------------
// The tests
// ---------------------------------------------------------------------------

describe('play start', () => {
  awaitTest('names resolve, then configure, then RE-ENUMERATE, then ready', function* () {
    const recorded = startResponder();
    const fix = fixture(recorded);
    const lines = yield* run(fix, { nation: 'eNgLiSh', leader: 'Ada', female: true });

    // Lobby check, catalog, enumerate, configure, re-enumerate, ready: the
    // refresh between the two steps is mandatory.
    expect(recorded.steps).toEqual([
      'health',
      'nations',
      'styles',
      'legal',
      'batch',
      'legal',
      'batch',
    ]);
    expect(commandOf(recorded.bodies[0] ?? {})).toEqual({
      action_id: scalar(CONFIGURE['action_id'] ?? null),
      arguments: {
        nation_id: NATION_ENGLISH,
        leader_name: 'Ada',
        is_male: false,
        style_id: STYLE_EUROPEAN,
      },
    });
    expect(commandOf(recorded.bodies[1] ?? {})).toEqual({
      action_id: scalar(SET_READY['action_id'] ?? null),
      arguments: { ready: true },
    });
    expect(recorded.bodies[1]?.['state_revision']).toEqual(CONFIGURED);
    expect(lines).toHaveLength(3);
    expect(lines[0]).toBe('starting as English — Ada (female), style European');
    expect(lines[1]?.startsWith('configure English Ada female →')).toBe(true);
    expect(lines[2]?.startsWith('set ready → applied')).toBe(true);
  });

  awaitTest('a catalog-freshness refusal re-reads and retries, bounded', function* () {
    // game_Dn9l…: the lobby revision advances in the background, so a
    // configure whose sections were read a revision ago refuses as
    // pregame_style_unknown.  One fresh re-read wins; unrelated refusals
    // must not retry.
    const recorded = startResponder({ staleConfigures: 1 });
    const fix = fixture(recorded);
    const lines = yield* run(fix, { nation: 'English', leader: 'Ada', female: true });

    // Two configure submissions, with a fresh nations+styles read between.
    expect(recorded.steps).toEqual([
      'health',
      'nations',
      'styles',
      'legal',
      'batch',
      'nations',
      'styles',
      'batch',
      'legal',
      'batch',
    ]);
    expect(lines.some((line) => line.includes('re-reading and retrying (attempt 2 of 3)'))).toBe(
      true
    );
    expect(lines.some((line) => line.startsWith('set ready → applied'))).toBe(true);
  });

  awaitTest('three stale configures exhaust the retry budget and stop', function* () {
    const recorded = startResponder({ staleConfigures: 99 });
    const fix = fixture(recorded);
    const lines = yield* run(fix, { nation: 'English', leader: 'Ada', female: true });
    expect(recorded.steps.filter((step) => step === 'batch')).toHaveLength(3);
    expect(lines.some((line) => line.includes('attempt 3 of 3'))).toBe(true);
    // The seat was never readied; the final line is the not-readied refusal.
    expect(lines.some((line) => line.startsWith('set ready'))).toBe(false);
  });

  awaitTest('a nation that is not on the catalog is refused by name', function* () {
    const fix = fixture(startResponder());
    const message = yield* refuse(fix, { nation: 'Atlantean', leader: 'Ada', male: true });
    expect(message).toContain("no nation named 'Atlantean'");
    expect(message).toContain('English');
  });

  awaitTest('an opaque id is not a name: the catalog is matched on the display name only', function* () {
    // CPython's `_pregame_choice` compares `item["name"]` and nothing else, so
    // `--nation nation_aaa…` is an unknown *name*.  See NOTES.md §U18.
    const fix = fixture(startResponder());
    const message = yield* refuse(fix, { nation: NATION_ENGLISH, leader: 'Ada', male: true });
    expect(message).toBe(
      `no nation named '${NATION_ENGLISH}' is offered; try one of: English Zulu`
    );
  });

  awaitTest('sex is optional but exclusive, and the refusal precedes the first request', function* () {
    const recorded = startResponder();
    const fix = fixture(recorded);
    const message = yield* refuse(fix, {
      nation: 'English',
      leader: 'Ada',
      male: true,
      female: true,
    });
    expect(message).toBe('just start takes at most one of --male or --female');
    expect(recorded.steps).toEqual([]);
  });

  awaitTest('with no arguments it resolves every choice itself', function* () {
    const recorded = startResponder();
    const drawn: ReadonlyArray<string>[] = [];
    const fix = fixture(recorded, {
      choose: (items) => {
        drawn.push(items.map((entry) => entry.name));
        const last = items[items.length - 1];
        if (last === undefined) throw new Error('the catalog was empty');
        return last;
      },
    });
    const lines = yield* run(fix);

    // The nation is drawn from what the lobby actually offers, sorted, so
    // seeding the RNG reproduces the pick.
    expect(drawn[0]).toEqual(['English', 'Zulu']);
    expect(recorded.steps).toEqual([
      'health',
      'nations',
      'styles',
      'overview',
      'legal',
      'batch',
      'legal',
      'batch',
    ]);
    expect(argumentsOf(recorded.bodies[0] ?? {})).toEqual({
      nation_id: NATION_ZULU,
      // The controller label, reduced to what Freeciv accepts.
      leader_name: 'codex-test-model',
      // The seat's own lobby default, not a client invention.
      is_male: false,
      style_id: STYLE_EUROPEAN,
    });
    expect(lines[0]).toBe(
      'starting as Zulu — codex-test-model (female), style European'
    );
  });

  awaitTest('each flag overrides exactly what it names and nothing else', function* () {
    const recorded = startResponder();
    const fix = fixture(recorded, {
      choose: () => {
        throw new Error('a named nation must never draw');
      },
    });
    yield* run(fix, { nation: 'english', male: true });
    // A named nation never draws, and a named sex never reads the lobby
    // overview: only what is missing is fetched.
    expect(recorded.steps).not.toContain('overview');
    expect(argumentsOf(recorded.bodies[0] ?? {})).toEqual({
      nation_id: NATION_ENGLISH,
      leader_name: 'codex-test-model',
      is_male: true,
      style_id: STYLE_EUROPEAN,
    });
  });

  awaitTest('a named style is resolved against its own catalog and printed by name', function* () {
    const recorded = startResponder();
    const fix = fixture(recorded);
    const lines = yield* run(fix, {
      nation: 'English',
      leader: 'Ada',
      style: 'european',
      male: true,
    });
    expect(recorded.steps.slice(0, 3)).toEqual(['health', 'nations', 'styles']);
    expect(lines[0]).toBe('starting as English — Ada (male), style European');
    expect(argumentsOf(recorded.bodies[0] ?? {})).toEqual({
      nation_id: NATION_ENGLISH,
      leader_name: 'Ada',
      is_male: true,
      style_id: STYLE_EUROPEAN,
    });
  });

  awaitTest('an unknown style is refused with its own catalog quoted back', function* () {
    const fix = fixture(startResponder());
    const message = yield* refuse(fix, { nation: 'English', style: 'Martian', male: true });
    expect(message).toBe("no style named 'Martian' is offered; try one of: European");
  });

  awaitTest('an unusable controller label falls back to the leader the lobby holds', function* () {
    const recorded = startResponder({ leader: 'Boudica', sex: 'male', controller: '***' });
    const fix = fixture(recorded, { controller: '***' });
    yield* run(fix);
    expect(argumentsOf(recorded.bodies[0] ?? {})).toMatchObject({
      leader_name: 'Boudica',
      is_male: true,
    });
  });

  awaitTest('the same zero-argument command picks the same sex twice', function* () {
    const chosen: unknown[] = [];
    for (const _run of [0, 1]) {
      // The lobby volunteers no usable sex, so the fallback is a pure function
      // of the resolved leader name.
      const recorded = startResponder({ sex: 'unspecified' });
      const fix = fixture(recorded);
      yield* run(fix);
      const values = argumentsOf(recorded.bodies[0] ?? {});
      chosen.push(isJsonObject(values) ? values['is_male'] : null);
    }
    expect(chosen[0]).toBe(chosen[1]);
    const digest = new Bun.CryptoHasher('sha256').update('codex-test-model').digest();
    expect(chosen[0]).toBe((digest[0] ?? 0) % 2 !== 0);
  });

  awaitTest('a lobby that offers no nation fails closed before any batch', function* () {
    const recorded = startResponder({ nations: [] });
    const fix = fixture(recorded);
    const message = yield* refuse(fix);
    expect(message).toContain('just state --section pregame_nations');
    expect(recorded.steps).not.toContain('batch');
  });

  awaitTest('a game that has already left the lobby is told so, and told what to run', function* () {
    const recorded = startResponder({ gameState: 'running' });
    const fix = fixture(recorded);
    const message = yield* refuse(fix, { nation: 'English', leader: 'Ada', male: true });
    expect(message).toBe(
      'just start configures a lobby seat; this game is running -- run `just turn`'
    );
    expect(recorded.steps).toEqual(['health']);
  });

  awaitTest('--leader is bounded by the byte budget, not the character count', function* () {
    const recorded = startResponder();
    const fix = fixture(recorded);
    const message = yield* refuse(fix, { leader: 'é'.repeat(24) });
    expect(message).toBe('--leader must be at most 47 UTF-8 bytes');
    expect(recorded.steps).toEqual([]);
  });

  awaitTest('--json prints the composite payload and no prose', function* () {
    const recorded = startResponder();
    const fix = fixture(recorded);
    const out = yield* run(fix, { nation: 'English', leader: 'Ada', female: true, json: true });
    expect(out).toHaveLength(1);
    const payload = Schema.decodeUnknownSync(startJsonSchema)(out[0] ?? '');
    expect(payload).toMatchObject({
      schema_version: 1,
      command: 'start',
      nation: 'English',
      leader: 'Ada',
      is_male: false,
      style_id: STYLE_EUROPEAN,
    });
    expect(payload.dispositions.length).toBeGreaterThan(0);
  });

  /**
   * The regression guard for PORT_MAP's U18 addendum: `startCommand` is
   * `startCommandWith(liveStartHooks)`, so a `liveStartHooks` that still
   * refuses at a seam is a `play start` that cannot claim a seat at all — the
   * one thing the command exists to do.  This runs the *shipped* bundle
   * against the same fake supervisor the hand-built hooks use, so every seam
   * it names is exercised for real: enumerate, persist, POST, RE-ENUMERATE,
   * enumerate, persist, POST.
   */
  awaitTest('the shipped hooks configure and ready the seat, re-enumerating between', function* () {
    const recorded = startResponder();
    const fix = fixture(recorded);
    const captured = yield* captureEffect(
      Effect.either(
        provideTestLayer(
          runStart(
            startOptions(fix.sessionPath, { nation: 'English', leader: 'Ada', male: true }),
            liveStartHooks
          ),
          fix.layer
        )
      )
    );
    const outcome = captured.value;
    const out = captured.captured.out;
    expect(Either.isRight(outcome)).toBe(true);
    expect(recorded.steps).toEqual([
      'health',
      'nations',
      'styles',
      'legal',
      'batch',
      'legal',
      'batch',
    ]);
    expect(commandOf(recorded.bodies[0] ?? {})).toEqual({
      action_id: scalar(CONFIGURE['action_id'] ?? null),
      arguments: {
        nation_id: NATION_ENGLISH,
        leader_name: 'Ada',
        is_male: true,
        style_id: STYLE_EUROPEAN,
      },
    });
    expect(commandOf(recorded.bodies[1] ?? {})).toEqual({
      action_id: scalar(SET_READY['action_id'] ?? null),
      arguments: { ready: true },
    });
    // U14's real `_render_disposition`, not the fixture's one-line stand-in.
    expect(out[0]).toBe('starting as English — Ada (male), style European');
    expect(out[1]?.startsWith('configure English Ada male → applied')).toBe(true);
    expect(out[2]?.startsWith('set ready → applied')).toBe(true);
  });

  awaitTest('each enumeration carries the remedy that unblocks exactly its own failure', function* () {
    const recorded = startResponder();
    const remedies: string[] = [];
    const fix = withHooks(fixture(recorded), (hooks) => ({
      ...hooks,
      resolveKindAction: (kind, remedy) => {
        remedies.push(remedy);
        return hooks.resolveKindAction(kind, remedy);
      },
    }));
    yield* run(fix, { nation: 'English', leader: 'Ada', male: true });
    expect(remedies).toEqual([
      'this seat may already be ready -- run `just legal --kind pregame.set_ready ' +
        '--all` and withdraw readiness before configuring again',
      'run `just legal --kind pregame.set_ready --all` once the lobby offers ' +
        'readiness, then `just batch` its action_id',
    ]);
  });

  awaitTest('an unproven configure is never readied, and its status leaves quietly', function* () {
    const recorded = startResponder();
    const fix = withHooks(fixture(recorded), (hooks, session) => ({
      ...hooks,
      submitPersistedBatch: (batchId) =>
        Effect.map(batchDisposition(session, batchId, 'receipt_first'), (disposition) => ({
          disposition,
          warning: `transport outcome is unknown for batch ${batchId}.`,
          exitCode: 2,
        })),
    }));
    const captured = yield* runRaw(fix, { nation: 'English', leader: 'Ada', male: true });

    // The seat was configured-or-not; readying on top of that would be a
    // second unproven mutation.
    expect(captured.out.at(-1)).toBe(NOT_READIED_LINE);
    expect(recorded.steps.filter((step) => step === 'legal')).toHaveLength(1);
    // The warning is stderr's, never stdout's.
    expect(captured.err[0]).toContain('transport outcome is unknown');
    // And the status carries no second `error: …` sentence with it.
    expect(Either.isLeft(captured.outcome)).toBe(true);
    if (Either.isLeft(captured.outcome)) {
      expect(captured.outcome.left._tag).toBe('ExitCodeSignal');
    }
  });

  /**
   * `_mirror_health` (client.py:3062-3072) passes `commands=V2_PROTOCOL_CARD`
   * unconditionally, so the `state/header.txt` that `command_start`'s health
   * probe writes ends in the whole card — not `_DEFAULT_COMMAND_CARD`.  `just
   * show` and `just show header` print that file verbatim (client.py:11170
   * maps the name to it), which puts it on the offline byte-diff oracle's
   * read-only path (PLAN §The oracle item 2).  Forwarding no options silently
   * downgrades the header to five lines and costs the agent the
   * ALIASES/ERRORS/ONE CALL PER TURN/MULTIPLAYER/WHICH BINDING block.  The
   * `wait` half of this assertion is `test/pvp-wait-interop.test.ts`; the
   * Python half is `test_v2_join_card_and_state_header_carry_the_same_contract`
   * (tests/test_client.py:7194-7254).
   */
  awaitTest("start's health probe writes the full protocol card into state/header.txt", function* () {
    const fix = fixture(startResponder());
    yield* run(fix, { nation: 'English', leader: 'Ada', male: true });

    const dir = Effect.runSync(mirrorDir(fix.sessionPath));
    yield* withTestFileSystem((files) =>
      Effect.gen(function* () {
        const header = yield* files.readFileString(path.join(dir, 'state', 'header.txt'));
        for (const line of V2_PROTOCOL_CARD) expect(header).toContain(line);
        for (const line of DEFAULT_COMMAND_CARD) expect(header).not.toContain(line);
      }).pipe(Effect.orDie)
    );
  });
});
