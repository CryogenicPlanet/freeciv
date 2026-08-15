/**
 * `batch`: dispositions, the JSON escape hatch, and the persist-then-send order.
 *
 * Ports `test_v2_batch_persists_before_send_and_retry_is_receipt_first`
 * (test_client.py:3701), `test_v2_batch_prints_each_closed_disposition_exactly_once`
 * (4304), `test_v2_batch_invalid_or_unproved_response_is_receipt_first` (4382)
 * and the `batch` case of
 * `test_v2_json_escape_hatch_covers_turn_batch_retry_and_wait` (1040).
 *
 * The safety property under test is one sentence: **no path out of `batch` ever
 * permits a blind resend.**  Every case below asserts the *disposition* the
 * command reached, because that value is what `retry` (U14) and `do` (U16) read
 * to decide whether another request is allowed at all.
 */
import { afterEach, describe, expect, test } from 'bun:test';
import { Effect, Either, Layer, Ref, Runtime, Schema } from 'effect';
import type { PlayError } from 'src/errors';
import type { ExitCodeSignal } from 'src/exit';
import { FULL_CONTROL_V2 } from 'src/constants';
import { runBatch, liveBatchHooks } from 'src/commands/batch.cmd';
import { decodeLegalPage } from 'src/schema/page';
import { decodeReceipt } from 'src/schema/receipt';
import type { Disposition } from 'src/schema/batch';
import type { JsonObject, JsonValue } from 'src/schema/primitives';
import {
  batchDisposition,
  batchErrorDisposition,
  batchIntent,
  pageLimit,
  parseJsonObject,
  submitBatch,
} from 'src/services/batch';
import { persistBatchForAction } from 'src/services/batch-persist';
import { canonicalText, type PyObject } from 'src/services/canonical-body';
import { renderDisposition } from 'src/render/receipt';
import { rememberPage, rememberReceipt, v2StateSchema } from 'src/services/aliases';
import { httpFor, type JsonResponse } from 'src/services/http';
import { compactJson } from 'src/services/json-output';
import { HEADER_FILE, mirrorDir, writeMirror } from 'src/services/mirror';
import { PrivateFs } from 'src/services/private-fs';
import {
  SessionStore,
  sessionStoreFor,
  type Session,
  type SessionStoreApi,
  type V2ClientState,
} from 'src/services/session-store';
import { V2Client, v2ClientFor } from 'src/services/v2-client';
import {
  FIXTURE_AGENT_ID,
  FIXTURE_GAME_ID,
  scratchWorkspace,
  sessionFile,
  type Scratch,
} from 'test/_fixtures';
import { captureEffect } from 'test/_capture';
import { awaitTest, provideTestLayer } from 'test/_effect-test';
import { fixtureObject, fixtureString, parseFixtureObject } from 'test/_expect';
import { fileSystem, path } from 'test/_test-platform';

// ---------------------------------------------------------------------------
// Fixture
// ---------------------------------------------------------------------------

const scratches: Scratch[] = [];
afterEach(() =>
  Effect.runPromise(
    Effect.forEach(scratches.splice(0), (scratch) => scratch.cleanup, { discard: true })
  )
);

interface TestRevision {
  readonly turn: number;
  readonly revision: number;
  readonly state_token: string;
  readonly [key: string]: JsonValue;
}

const revision = (number = 7, turn = 3): TestRevision => ({
  turn,
  revision: number,
  state_token: `state_${String(number).padStart(32, '0')}`,
});

const UNIT_ONE = `unit_${'a'.repeat(32)}`;
const ACTION_ONE = `action_${'1'.repeat(26)}`;

const descriptor = (stateRevision: TestRevision, actionId = ACTION_ONE): JsonObject => ({
  action_id: actionId,
  kind: 'unit.found_city',
  label: 'Found city',
  subject: {
    operation: 'found_city',
    actor: { id: UNIT_ONE, type: 'unit', name: 'Settlers' },
  },
  arguments_schema: { type: 'object' },
  state_revision: stateRevision,
});

const legalPage = (
  stateRevision: TestRevision,
  items: ReadonlyArray<JsonValue>
): JsonObject => ({
  schema_version: 2,
  control_protocol: FULL_CONTROL_V2,
  game_id: FIXTURE_GAME_ID,
  agent_id: FIXTURE_AGENT_ID,
  state_revision: stateRevision,
  page: {
    section: 'legal_actions',
    items,
    total_items: items.length,
    next_cursor: null,
    cursor_expires_at: null,
  },
});

const receiptBody = (
  batchId: string,
  state = 'applied',
  stateRevision: TestRevision = revision(8),
  overrides: JsonObject = {}
): JsonObject => ({
  schema_version: 2,
  control_protocol: FULL_CONTROL_V2,
  game_id: FIXTURE_GAME_ID,
  agent_id: FIXTURE_AGENT_ID,
  batch_id: batchId,
  receipt_state: state,
  idempotent: false,
  state_revision: stateRevision,
  error: null,
  observation: null,
  ...overrides,
});

const errorBody = (
  code: string,
  details: JsonObject,
  retryable = false,
  stateRevision: JsonValue = null
): JsonObject => ({
  schema_version: 2,
  control_protocol: FULL_CONTROL_V2,
  error: {
    code,
    message: `the supervisor refused with ${code}`,
    retryable,
    details,
  },
  state_revision: stateRevision,
});

/** A `fetch` the test drives directly, so a POST body can be inspected. */
interface ResponderAnswer {
  readonly status: number;
  readonly body: JsonValue;
}

type Responder = (
  url: string,
  body: string | null
) => ResponderAnswer | Effect.Effect<ResponderAnswer, PlayError>;

type FetchInput = Parameters<typeof fetch>[0];
type FetchArguments = Parameters<typeof fetch>;
type BatchFailure = PlayError | ExitCodeSignal;
type BatchServices = SessionStore | PrivateFs | V2Client;

const completeFetch = (
  handler: (...args: FetchArguments) => ReturnType<typeof fetch>
): typeof fetch => Object.assign(handler, { preconnect: fetch.preconnect });

const urlOf = (input: FetchInput): string =>
  input instanceof Request ? input.url : new URL(input).href;

const requestBodyText = (init: RequestInit | undefined): Effect.Effect<string | null> => {
  const body = init?.body;
  return body === undefined || body === null
    ? Effect.succeed(null)
    : Effect.promise(() => new Response(body).text());
};

const postBatchId = (body: string | null): string =>
  Schema.decodeUnknownSync(Schema.parseJson(Schema.Struct({ batch_id: Schema.String })))(
    body ?? '{}'
  ).batch_id;

const batchKeysOnDisk = (f: Fixture): Effect.Effect<ReadonlyArray<string>, PlayError> =>
  Effect.map(f.store.readState(f.sessionPath, f.session), (state) => Object.keys(state.batches));

const parseArguments = (text: string): Either.Either<PyObject, { readonly message: string }> =>
  Effect.runSync(Effect.either(parseJsonObject(text, '--arguments')));

const canonicalArguments = (text: string): string =>
  Effect.runSync(canonicalText(ok(parseArguments(text))));

const deepJson = (depth: number): string => `${'{"a":'.repeat(depth)}1${'}'.repeat(depth)}`;

const stateWith = (batches: JsonObject, actions: JsonObject = {}): V2ClientState => ({
  schema_version: 5,
  game_id: FIXTURE_GAME_ID,
  agent_id: FIXTURE_AGENT_ID,
  last_revision: null,
  actions,
  pending_catalogs: {},
  batches,
  receipts: {},
  action_aliases: { state_revision: null, by_alias: {} },
  entity_aliases: {},
  tile_aliases: {},
  drained_actors: [],
});

const commandBody = (command: JsonValue): string =>
  JSON.stringify({ batch_id: 'batch_x', commands: [command] });

const jsonResponse = (status: number, body: JsonObject): JsonResponse => ({
  status,
  value: body,
  headers: {},
});

const recoveryContract = (batchId: string, safeNext: string): JsonObject => ({
  batch_id: batchId,
  acceptance: 'not_accepted',
  safe_next: safeNext,
});

type DispositionPayloadInput = {
  readonly receipt?: JsonObject;
  readonly error?: JsonObject;
};

const dispositionPayload = (
  receipt: JsonObject | null,
  err: JsonObject | null
): DispositionPayloadInput => {
  if (receipt !== null && err !== null) return { receipt, error: err };
  if (receipt !== null) return { receipt };
  if (err !== null) return { error: err };
  return {};
};

interface Fixture {
  readonly store: SessionStoreApi;
  readonly sessionPath: string;
  readonly session: Session;
  readonly statePath: string;
  readonly posts: ReadonlyArray<string>;
  readonly layer: Layer.Layer<BatchServices>;
  readonly run: <A, E>(
    effect: Effect.Effect<A, E, BatchServices>
  ) => Effect.Effect<Either.Either<A, E>>;
}

const fixture = (
  responder: Responder = () => ({ status: 200, body: {} })
): Effect.Effect<Fixture, PlayError> =>
  Effect.gen(function* () {
    const scratch = yield* scratchWorkspace();
    scratches.push(scratch);
    const store = sessionStoreFor(scratch.workspace, scratch.files, v2StateSchema, {});
    const sessionPath = path.join(scratch.workspace.stateRoot, FIXTURE_GAME_ID, 'seat.json');
    yield* scratch.files.writeJson(sessionPath, sessionFile());
    const loaded = yield* store.resolveV2(sessionPath);
    const posts: string[] = [];
    const runPromise = Runtime.runPromise(yield* Effect.runtime());
    const fakeFetch = completeFetch((input, init) =>
      runPromise(
        Effect.gen(function* () {
          const body = yield* requestBodyText(init);
          if (body !== null) posts.push(body);
          const response = responder(urlOf(input), body);
          const answer = yield* Effect.isEffect(response) ? response : Effect.succeed(response);
          return new Response(compactJson(answer.body), {
            status: answer.status,
            headers: { 'content-type': 'application/json' },
          });
        })
      )
    );
    const client = v2ClientFor(httpFor(fakeFetch), () => Effect.void);
    const layer = Layer.mergeAll(
      Layer.succeed(SessionStore, store),
      Layer.succeed(PrivateFs, scratch.files),
      Layer.succeed(V2Client, client)
    );
    return {
      store,
      sessionPath,
      session: loaded.session,
      statePath: store.statePath(sessionPath),
      posts,
      layer,
      run: (effect) => Effect.either(provideTestLayer(effect, layer)),
    };
  });

const ok = <A, E>(either: Either.Either<A, E>): A => {
  if (Either.isLeft(either)) {
    throw new Error(`expected success, got ${JSON.stringify(either.left)}`);
  }
  return either.right;
};

const failure = <A>(either: Either.Either<A, { readonly message: string }>): string => {
  expect(Either.isLeft(either)).toBe(true);
  return Either.isLeft(either) ? either.left.message : '';
};

const seed = (fx: Fixture, stateRevision: TestRevision): Effect.Effect<void> =>
  Effect.map(
    fx.run(
      Effect.flatMap(
        Effect.mapError(
          decodeLegalPage(legalPage(stateRevision, [descriptor(stateRevision)]), fx.session),
          (error) => ({ message: error.message })
        ),
        (decoded) => rememberPage(fx.sessionPath, fx.session, { legal: true, page: decoded })
      )
    ),
    (result) => {
      ok(result);
    }
  );

const stale = (fx: Fixture): Effect.Effect<void> =>
  Effect.gen(function* () {
    yield* seed(fx, revision(7));
    const result = yield* fx.run(
      Effect.flatMap(
        Effect.mapError(
          decodeReceipt(receiptBody(`batch_${'Z'.repeat(24)}`, 'applied'), fx.session, {
            batchId: `batch_${'Z'.repeat(24)}`,
          }),
          (error) => ({ message: error.message })
        ),
        (receipt) => rememberReceipt(fx.sessionPath, fx.session, receipt)
      )
    );
    ok(result);
  });

interface Captured {
  readonly out: ReadonlyArray<string>;
  readonly err: ReadonlyArray<string>;
  readonly outcome: Either.Either<void, BatchFailure>;
}

const capture = (
  effect: Effect.Effect<void, BatchFailure, BatchServices>,
  fx: Fixture
): Effect.Effect<Captured> =>
  Effect.map(
    captureEffect(Effect.either(provideTestLayer(effect, fx.layer))),
    ({ value, captured }) => ({
      out: captured.out,
      err: captured.err,
      outcome: value,
    })
  );

const exitCodeOf = (outcome: Either.Either<void, BatchFailure>): number => {
  if (Either.isRight(outcome)) return 0;
  return outcome.left._tag === 'ExitCodeSignal' ? outcome.left.code : -1;
};

const refusalMessage = (outcome: Either.Either<void, BatchFailure>): string => {
  if (Either.isRight(outcome)) return '';
  return outcome.left._tag === 'ExitCodeSignal' ? '' : outcome.left.message;
};

const args = (overrides: Partial<Parameters<typeof runBatch>[0]> = {}) => ({
  session: '',
  actionId: ACTION_ONE,
  arguments: '{}',
  noRefresh: false,
  json: true,
  ...overrides,
});

const pinned = (token: string) => ({ ...liveBatchHooks, token: () => token });

// ---------------------------------------------------------------------------
// _limit and _parse_json_object
// ---------------------------------------------------------------------------

describe('_limit', () => {
  test('only the canonical spellings of 1..16 are a page size', () => {
    expect(Effect.runSync(pageLimit(null))).toBe(16);
    expect(Effect.runSync(pageLimit('1'))).toBe(1);
    expect(Effect.runSync(pageLimit('16'))).toBe(16);
    for (const bad of ['0', '17', '08', '+8', ' 8', '8.0', '']) {
      expect(failure(Effect.runSync(Effect.either(pageLimit(bad))))).toBe(
        'limit must be a canonical integer from 1 through 16'
      );
    }
  });
});

describe('--arguments', () => {

  test('a strict JSON object is copied through', () => {
    expect(canonicalArguments('{"name":"London","size":3}')).toBe('{"name":"London","size":3}');
    expect(canonicalArguments('{}')).toBe('{}');
    expect(canonicalArguments('{"a":[1,{"b":null},true,"x"]}')).toBe('{"a":[1,{"b":null},true,"x"]}');
  });

  test('a number keeps the Python type its literal named', () => {
    // The one that reaches the wire: `--arguments` is agent input, and
    // CPython persists these four spellings exactly as written.
    expect(canonicalArguments('{"tax":40.0}')).toBe('{"tax":40.0}');
    expect(canonicalArguments('{"a":1e16}')).toBe('{"a":1e+16}');
    expect(canonicalArguments('{"a":0.00001}')).toBe('{"a":1e-05}');
    expect(canonicalArguments('{"a":10000000000000000001}')).toBe('{"a":10000000000000000001}');
  });

  test('an unrepresentable number is refused by _json_value, not silently sent', () => {
    expect(failure(parseArguments('{"a":1e400}'))).toBe('invalid --arguments: number is not finite');
  });

  test("_json_value's limits are the ones CPython closes", () => {
    expect(ok(parseArguments(deepJson(12)))).toBeDefined();
    expect(failure(parseArguments(deepJson(13)))).toBe('invalid --arguments: JSON is nested too deeply');
    expect(failure(parseArguments(`{"${'k'.repeat(129)}":1}`))).toBe('invalid --arguments: invalid object');
    expect(failure(parseArguments('{"":1}'))).toBe('invalid --arguments: invalid object');
    // A pathological document costs a refusal, not a stack: the parser stops
    // descending long before the runtime would, and says the same sentence
    // `_json_value` says for anything past 12.
    expect(failure(parseArguments(deepJson(200_000)))).toBe(
      'invalid --arguments: JSON is nested too deeply'
    );
  });

  test('a duplicate key is a refusal, never a silent last-one-wins', () => {
    expect(failure(parseArguments('{"name":"A","name":"B"}'))).toBe(
      '--arguments must not contain duplicate keys'
    );
    // Nested, and escaped into the same key: both are the same key.
    expect(failure(parseArguments('{"a":{"k":1,"k":2}}'))).toBe(
      '--arguments must not contain duplicate keys'
    );
    expect(failure(parseArguments('{"n\\u0061me":1,"name":2}'))).toBe(
      '--arguments must not contain duplicate keys'
    );
  });

  test('a repeated key inside two sibling objects is not a duplicate', () => {
    expect(canonicalArguments('{"a":{"k":1},"b":{"k":2}}')).toBe('{"a":{"k":1},"b":{"k":2}}');
    expect(canonicalArguments('{"a":[{"k":1},{"k":2}]}')).toBe('{"a":[{"k":1},{"k":2}]}');
    // A `:` inside a string value must not be read as a key separator.
    expect(canonicalArguments('{"a":"k\\":1,\\"k","b":2}')).toBe('{"a":"k\\":1,\\"k","b":2}');
    // A literal `__proto__` argument name is a key like any other.
    expect(canonicalArguments('{"__proto__":{"x":1}}')).toBe('{"__proto__":{"x":1}}');
  });

  test('a non-object and a non-JSON value are each named for what they are', () => {
    expect(failure(parseArguments('[1,2]'))).toBe('--arguments must be a JSON object');
    expect(failure(parseArguments('"text"'))).toBe('--arguments must be a JSON object');
    expect(failure(parseArguments('null'))).toBe('--arguments must be a JSON object');
    expect(failure(parseArguments('1'))).toBe('--arguments must be a JSON object');
  });

  /**
   * The most reachable refusal in the unit: a typo in `--arguments` is the whole
   * failure mode, and the sentence has to be CPython's, position and all.
   *
   * Every expectation below was produced by running the CPython original —
   *
   *     json.loads(TEXT, object_pairs_hook=pairs, parse_constant=…)
   *
   * — under the same `python3` the byte-diff oracle runs `play/client.py` with.
   * The messages and offsets are the **C** `_json` accelerator's, which is what
   * `import json` uses; see NOTES.md §17.1 for the version this pins to.
   */
  test('a syntax error is CPython\u2019s sentence, with CPython\u2019s position', () => {
    const cases: ReadonlyArray<readonly [string, string]> = [
      ['{"a":}', 'Expecting value: line 1 column 6 (char 5)'],
      ['{"a":01}', "Expecting ',' delimiter: line 1 column 7 (char 6)"],
      ['{"a":1}x', 'Extra data: line 1 column 8 (char 7)'],
      ['{name:1}', 'Expecting property name enclosed in double quotes: line 1 column 2 (char 1)'],
      ['{', 'Expecting property name enclosed in double quotes: line 1 column 2 (char 1)'],
      ['{"a"}', "Expecting ':' delimiter: line 1 column 5 (char 4)"],
      ['{"a":1,}', 'Illegal trailing comma before end of object: line 1 column 7 (char 6)'],
      ['{"a":[1,2,]}', 'Illegal trailing comma before end of array: line 1 column 10 (char 9)'],
      ['{"a":"x', 'Unterminated string starting at: line 1 column 6 (char 5)'],
      ['{"a":"\\q"}', 'Invalid \\escape: line 1 column 7 (char 6)'],
      ['{"a":"\\uZZZZ"}', 'Invalid \\uXXXX escape: line 1 column 8 (char 7)'],
      ['{"a":"a\tb"}', 'Invalid control character at: line 1 column 8 (char 7)'],
      ['', 'Expecting value: line 1 column 1 (char 0)'],
      ['   ', 'Expecting value: line 1 column 4 (char 3)'],
      ['{"a":1}\n\n x', 'Extra data: line 3 column 2 (char 10)'],
      // The `parse_constant` hook's own `ValueError`, which carries no position.
      ['{"a":NaN}', 'non-finite number NaN'],
      ['{"a":Infinity}', 'non-finite number Infinity'],
      ['{"a":-Infinity}', 'non-finite number -Infinity'],
      ['{"a":[NaN]}', 'non-finite number NaN'],
    ];
    for (const [text, detail] of cases) {
      expect(failure(parseArguments(text))).toBe(`--arguments must be valid strict JSON: ${detail}`);
    }
  });

  test('a position is a code point offset, as CPython\u2019s is', () => {
    // An astral character is one character to Python and two UTF-16 units to
    // JavaScript; reading the second as a position would name the wrong column.
    expect(failure(parseArguments('{"a":"\u{1F600}"x}'))).toBe(
      "--arguments must be valid strict JSON: Expecting ',' delimiter: line 1 column 9 (char 8)"
    );
  });

  test('a duplicate key beats a later syntax error, and a trailing comma beats it', () => {
    // `object_pairs_hook` fires when the object *closes*, so which refusal wins
    // is decided by which one the scanner reaches first.
    expect(failure(parseArguments('{"a":1,"a":2}x'))).toBe('--arguments must not contain duplicate keys');
    expect(failure(parseArguments('{"a":1,"a":2,}'))).toBe(
      '--arguments must be valid strict JSON: Illegal trailing comma before end of ' +
        'object: line 1 column 13 (char 12)'
    );
    expect(failure(parseArguments('{"a":1,"a":2'))).toBe(
      "--arguments must be valid strict JSON: Expecting ',' delimiter: line 1 " +
        'column 13 (char 12)'
    );
  });

  test('a lone surrogate parses, and is refused only where CPython refuses it', () => {
    // `"\ud800"` *is* strict JSON, so the parser must accept it; the refusal
    // belongs to `_canonical_body`'s `.encode("utf-8")` (see
    // test/batch-persist.test.ts), not here.
    const parsed = ok(parseArguments('{"a":"\\ud800"}'));
    expect(parsed['a']).toBe('\ud800');
    expect(ok(parseArguments('{"a":"\\ud83d\\ude00"}'))['a']).toBe('\u{1F600}');
    // A lead with no trail keeps its lone surrogate, and a reversed pair is two.
    expect(ok(parseArguments('{"a":"\\ud800x"}'))['a']).toBe('\ud800x');
    expect(ok(parseArguments('{"a":"\\udc00\\ud800"}'))['a']).toBe('\udc00\ud800');
  });
});

// ---------------------------------------------------------------------------
// _batch_intent
// ---------------------------------------------------------------------------

describe('batchIntent', () => {

  test('a cached descriptor names the batch by kind and label', () => {
    const state = stateWith(
      { batch_x: commandBody({ action_id: ACTION_ONE, arguments: {} }) },
      { [ACTION_ONE]: { kind: 'unit.found_city', label: 'Found city' } }
    );
    expect(batchIntent(state, 'batch_x')).toBe('unit.found_city Found city');
  });

  test('arguments are appended in the order the persisted body carries them', () => {
    const state = stateWith(
      {
        batch_x: commandBody({
          action_id: ACTION_ONE,
          arguments: { name: 'München', size: 3, ready: true },
        }),
      },
      { [ACTION_ONE]: { kind: 'unit.found_city', label: 'Found city' } }
    );
    expect(batchIntent(state, 'batch_x')).toBe(
      'unit.found_city Found city {name=München,size=3,ready=yes}'
    );
  });

  test('_scalar prints a float through %g and an int as itself', () => {
    // The persisted text is the only record of which one the agent typed, so
    // `1234567.0` and `1234567` are two different intent lines in CPython.
    const intent = (encoded: string): string =>
      batchIntent(
        stateWith(
          { batch_x: encoded },
          { [ACTION_ONE]: { kind: 'unit.found_city', label: 'Found city' } }
        ),
        'batch_x'
      );
    const persisted = (argumentsJson: string): string =>
      `{"batch_id":"batch_x","commands":[{"action_id":"${ACTION_ONE}","arguments":${argumentsJson}}]}`;
    expect(intent(persisted('{"tax":1234567.0}'))).toBe(
      'unit.found_city Found city {tax=1.23457e+06}'
    );
    expect(intent(persisted('{"tax":1234567}'))).toBe(
      'unit.found_city Found city {tax=1234567}'
    );
    expect(intent(persisted('{"tax":0.5,"seed":10000000000000000001}'))).toBe(
      'unit.found_city Found city {tax=0.5,seed=10000000000000000001}'
    );
    expect(intent(persisted('{"where":[1,2.0],"who":null}'))).toBe(
      'unit.found_city Found city {where=[1,2.0],who=-}'
    );
  });

  test('an expired descriptor degrades to the action ID, never to a request', () => {
    const state = stateWith({ batch_x: commandBody({ action_id: ACTION_ONE, arguments: {} }) });
    expect(batchIntent(state, 'batch_x')).toBe(ACTION_ONE);
  });

  test('anything unreadable is the bare word batch', () => {
    expect(batchIntent(stateWith({}), 'batch_x')).toBe('batch');
    expect(batchIntent(stateWith({ batch_x: 'not json' }), 'batch_x')).toBe('batch');
    expect(batchIntent(stateWith({ batch_x: '{"commands":[]}' }), 'batch_x')).toBe('batch');
    expect(
      batchIntent(stateWith({ batch_x: '{"commands":[{"action_id":7}]}' }), 'batch_x')
    ).toBe('batch');
  });
});

// ---------------------------------------------------------------------------
// _batch_disposition
// ---------------------------------------------------------------------------

describe('batchDisposition', () => {
  const fx = (): Effect.Effect<Fixture, PlayError> => fixture();

  awaitTest('a disposition outside the closed set is refused', function* (wait) {
    const f = yield* fx();
    const outcome = yield* wait(f.run(batchDisposition(f.session, 'batch_x', 'looks_fine')));
    expect(failure(outcome)).toBe('invalid batch disposition');
  });

  awaitTest('every disposition whose payload would lie about the outcome is refused', function* (wait) {
    const f = yield* fx();
    const applied = receiptBody('batch_x', 'applied');
    const accepted = receiptBody('batch_x', 'accepted');
    const error = errorBody('conflict', {});
    const cases: ReadonlyArray<readonly [string, JsonObject | null, JsonObject | null]> = [
      // receipt_terminal without a terminal receipt
      ['receipt_terminal', accepted, null],
      ['receipt_terminal', null, null],
      // receipt_poll with anything but an accepted receipt
      ['receipt_poll', applied, null],
      ['receipt_poll', null, null],
      // the two "not accepted" dispositions need an error and no receipt
      ['retry_exact', null, null],
      ['refresh', null, null],
      // receipt_first is the "outcome unknown" disposition; a receipt contradicts it
      ['receipt_first', applied, null],
    ];
    for (const [disposition, receipt, err] of cases) {
      const outcome = yield* wait(f.run(
        batchDisposition(f.session, 'batch_x', disposition, dispositionPayload(receipt, err))
      ));
      expect(failure(outcome)).toBe('invalid batch disposition payload');
    }
    // And the shapes that do agree are accepted.
    expect(
      ok(
        yield* wait(f.run(
          batchDisposition(f.session, 'batch_x', 'receipt_terminal', { receipt: applied })
        ))
      ).disposition
    ).toBe('receipt_terminal');
    expect(
      ok(
        yield* wait(f.run(
          batchDisposition(f.session, 'batch_x', 'receipt_poll', { receipt: accepted })
        ))
      ).disposition
    ).toBe('receipt_poll');
    expect(
      ok(yield* wait(f.run(batchDisposition(f.session, 'batch_x', 'refresh', { error }))))
        .disposition
    ).toBe('refresh');
    expect(
      ok(yield* wait(f.run(batchDisposition(f.session, 'batch_x', 'receipt_first')))).disposition
    ).toBe('receipt_first');
  });
});

// ---------------------------------------------------------------------------
// _batch_error_disposition
// ---------------------------------------------------------------------------

describe('batchErrorDisposition', () => {

  awaitTest('a refusal that never claimed the batch was unaccepted proves nothing', function* (wait) {
    const f = yield* fixture();
    for (const details of [
      {},
      { batch_id: 'batch_other', acceptance: 'not_accepted', safe_next: 'refresh' },
      { batch_id: 'batch_x', acceptance: 'accepted', safe_next: 'refresh' },
      { batch_id: 'batch_x', acceptance: 'not_accepted', safe_next: 'receipt_poll' },
    ]) {
      const outcome = yield* wait(f.run(
        batchErrorDisposition(
          jsonResponse(409, errorBody('stale_revision', details)),
          f.session,
          'batch_x'
        )
      ));
      expect(failure(outcome)).toBe('batch error omitted its safe recovery contract');
    }
  });

  awaitTest('a contract that contradicts its own error code is refused', function* (wait) {
    const f = yield* fixture();
    const outcome = yield* wait(f.run(
      batchErrorDisposition(
        jsonResponse(503, errorBody('sidecar_unavailable', recoveryContract('batch_x', 'retry_exact'), false)),
        f.session,
        'batch_x'
      )
    ));
    expect(failure(outcome)).toBe('batch error recovery contract contradicts its code');
  });

  awaitTest('the code decides the safe next step, and the server has to agree with it', function* (wait) {
    const f = yield* fixture();
    const cases: ReadonlyArray<readonly [number, string, boolean, Disposition]> = [
      [409, 'conflict', false, 'receipt_first'],
      [500, 'internal_error', false, 'receipt_first'],
      [409, 'action_outcome_ambiguous', false, 'receipt_first'],
      [429, 'rate_limited', true, 'retry_exact'],
      [503, 'sidecar_unavailable', true, 'retry_exact'],
      [503, 'sidecar_unavailable', false, 'refresh'],
      [409, 'stale_revision', false, 'refresh'],
      [422, 'illegal_action', false, 'refresh'],
      // Retryable, but not at the status its retry contract requires.
      [500, 'rate_limited', true, 'refresh'],
    ];
    for (const [status, code, retryable, expected] of cases) {
      const built = ok(
        yield* wait(f.run(
          batchErrorDisposition(
            jsonResponse(status, errorBody(code, recoveryContract('batch_x', expected), retryable)),
            f.session,
            'batch_x'
          )
        ))
      );
      expect(built.disposition).toBe(expected);
      expect(built.receipt).toBeNull();
      expect(built.error?.error.code).toBe(code);
    }
  });
});

// ---------------------------------------------------------------------------
// _submit_persisted_batch
// ---------------------------------------------------------------------------

describe('submitBatch', () => {
  awaitTest('a batch that was never persisted is refused before any request', function* (wait) {
    const f = yield* fixture();
    const outcome = yield* wait(f.run(submitBatch(f.sessionPath, f.session, 'batch_missing')));
    expect(failure(outcome)).toBe("no persisted command batch 'batch_missing'");
    expect(f.posts).toHaveLength(0);
  });

  awaitTest('the exact persisted bytes are what goes on the wire', function* (wait) {
    const f = yield* fixture((_url, body) => ({
      status: 200,
      body: receiptBody(
        postBatchId(body),
        'applied'
      ),
    }));
    yield* wait(seed(f, revision(7)));
    const batchId = ok(
      yield* wait(f.run(
        persistBatchForAction(f.sessionPath, f.session, ACTION_ONE, { city: 'München' }, {
          token: () => 'A'.repeat(24),
        })
      ))
    );
    const stored = fixtureString(ok(yield* wait(f.run(f.store.readState(f.sessionPath, f.session)))).batches[batchId]);
    const submitted = ok(yield* wait(f.run(submitBatch(f.sessionPath, f.session, batchId))));
    expect(f.posts).toEqual([stored]);
    expect(submitted.disposition.disposition).toBe('receipt_terminal');
    expect(submitted.exitCode).toBe(0);
    expect(submitted.warning).toBeNull();
  });

  awaitTest('an unreadable success response is receipt-first, never a retry', function* (wait) {
    const f = yield* fixture(() => ({ status: 200, body: { fine: true } }));
    yield* wait(seed(f, revision(7)));
    const batchId = ok(
      yield* wait(f.run(
        persistBatchForAction(f.sessionPath, f.session, ACTION_ONE, {}, {
          token: () => 'A'.repeat(24),
        })
      ))
    );
    const submitted = ok(yield* wait(f.run(submitBatch(f.sessionPath, f.session, batchId))));
    expect(submitted.disposition.disposition).toBe('receipt_first');
    expect(submitted.warning).toBe(
      'the server returned an invalid success response; resolve the persisted batch ' +
        'by receipt before any retry'
    );
    expect(submitted.exitCode).toBe(2);
  });

  awaitTest('a refusal without its recovery contract is receipt-first too', function* (wait) {
    const f = yield* fixture(() => ({ status: 503, body: errorBody('sidecar_unavailable', {}, true) }));
    yield* wait(seed(f, revision(7)));
    const batchId = ok(
      yield* wait(f.run(
        persistBatchForAction(f.sessionPath, f.session, ACTION_ONE, {}, {
          token: () => 'Z'.repeat(24),
        })
      ))
    );
    const submitted = ok(yield* wait(f.run(submitBatch(f.sessionPath, f.session, batchId))));
    expect(submitted.disposition.disposition).toBe('receipt_first');
    expect(submitted.warning).toBe(
      'the server response did not prove that this persisted batch was unaccepted; ' +
        'resolve its receipt first'
    );
  });

  awaitTest('an accepted receipt is pollable, not terminal', function* (wait) {
    const f = yield* fixture((_url, body) => ({
      status: 202,
      body: receiptBody(
        postBatchId(body),
        'accepted'
      ),
    }));
    yield* wait(seed(f, revision(7)));
    const batchId = ok(
      yield* wait(f.run(
        persistBatchForAction(f.sessionPath, f.session, ACTION_ONE, {}, {
          token: () => 'A'.repeat(24),
        })
      ))
    );
    const submitted = ok(yield* wait(f.run(submitBatch(f.sessionPath, f.session, batchId))));
    expect(submitted.disposition.disposition).toBe('receipt_poll');
    // The receipt is cached, so `retry` can read it without a request.
    const cached = ok(yield* wait(f.run(f.store.readState(f.sessionPath, f.session))));
    expect(fixtureString(fixtureObject(cached.receipts[batchId])['receipt_state'])).toBe('accepted');
  });
});

// ---------------------------------------------------------------------------
// command_batch
// ---------------------------------------------------------------------------

describe('play batch', () => {
  awaitTest('the batch is on disk before the request, so a dead transport is recoverable', function* (wait) {
    const dying = yield* fixture((): never => {
      throw new Error('connection reset');
    });
    yield* wait(seed(dying, revision(7)));
    const captured = yield* wait(capture(
      runBatch(args({ arguments: '{"city":"München"}' }), pinned('A'.repeat(24))),
      dying
    ));
    expect(exitCodeOf(captured.outcome)).toBe(2);
    const printed = parseFixtureObject(captured.out[0] ?? '');
    expect(printed.batch_id).toBe(`batch_${'A'.repeat(24)}`);
    expect(printed.disposition).toBe('receipt_first');
    expect(captured.err[0]).toContain('transport outcome is unknown for batch');
    // The record survived the failure: `retry` has something to resolve.
    const persisted = yield* batchKeysOnDisk(dying);
    expect(persisted).toEqual([`batch_${'A'.repeat(24)}`]);
  });

  awaitTest('the state file already holds the batch when the request is made', function* (wait) {
    const onDisk = yield* Ref.make<ReadonlyArray<string>>([]);
    const observed = yield* Ref.make<Fixture | null>(null);
    const f = yield* fixture(() =>
      Effect.flatMap(Ref.get(observed), (current) =>
        current === null
          ? Effect.die(new Error('fixture was not ready'))
          : Effect.flatMap(batchKeysOnDisk(current), (keys) =>
              Effect.zipRight(
                Ref.set(onDisk, keys),
                Effect.die(new Error('connection reset'))
              )
            )
      )
    );
    yield* Ref.set(observed, f);
    yield* wait(seed(f, revision(7)));
    yield* wait(capture(runBatch(args(), pinned('B'.repeat(24))), f));
    expect(yield* Ref.get(onDisk)).toEqual([`batch_${'B'.repeat(24)}`]);
  });

  awaitTest('each closed disposition is reached exactly once, and printed as one object', function* (wait) {
    const cases: ReadonlyArray<
      readonly [string, number, string | null, string | null, Disposition, number]
    > = [
      ['poll', 202, 'accepted', null, 'receipt_poll', 0],
      ['terminal', 200, 'applied', null, 'receipt_terminal', 0],
      ['rate', 429, null, 'rate_limited', 'retry_exact', 2],
      ['busy', 503, null, 'sidecar_unavailable', 'retry_exact', 2],
      ['stopped', 503, null, 'sidecar_unavailable', 'refresh', 2],
      ['stale', 409, null, 'stale_revision', 'refresh', 2],
      ['argument', 422, null, 'illegal_action', 'refresh', 2],
      ['conflict', 409, null, 'conflict', 'receipt_first', 2],
    ];
    for (const [label, status, receiptState, code, expected, expectedExit] of cases) {
      const token = label.padEnd(24, 'x');
      const batchId = `batch_${token}`;
      let responseBody: JsonObject;
      if (receiptState !== null) {
        responseBody = receiptBody(batchId, receiptState);
      } else {
        if (code === null) throw new Error(`missing error code for ${label}`);
        responseBody = errorBody(
          code,
          {
            batch_id: batchId,
            acceptance: 'not_accepted',
            safe_next: expected,
          },
          code === 'rate_limited' || label === 'busy'
        );
      }
      const f = yield* fixture(() => ({ status, body: responseBody }));
      yield* wait(seed(f, revision(7)));
      const captured = yield* wait(capture(runBatch(args(), pinned(token)), f));
      expect(captured.out).toHaveLength(1);
      const printed = parseFixtureObject(captured.out[0] ?? '');
      expect([label, printed.batch_id, printed.disposition]).toEqual([
        label,
        batchId,
        expected,
      ]);
      expect(exitCodeOf(captured.outcome)).toBe(expectedExit);
    }
  });

  awaitTest('--json prints exactly one canonical object, and the text form is a projection', function* (wait) {
    const token = 'J'.repeat(24);
    const batchId = `batch_${token}`;
    const build = (): Effect.Effect<Fixture, PlayError> =>
      fixture(() => ({ status: 200, body: receiptBody(batchId, 'applied') }));

    const jsonRun = yield* build();
    yield* wait(seed(jsonRun, revision(7)));
    const raw = yield* wait(capture(
      runBatch(args({ arguments: '{"ready":true}' }), pinned(token)),
      jsonRun
    ));
    expect(raw.out).toHaveLength(1);
    // Exactly one canonical object: no pretty-printing, no second line, nothing
    // a machine consumer must strip.
    const parsed = parseFixtureObject(raw.out[0] ?? '');
    expect(raw.out[0]).toBe(compactJson(parsed));

    const textRun = yield* build();
    yield* wait(seed(textRun, revision(7)));
    const text = yield* wait(capture(
      runBatch(args({ arguments: '{"ready":true}', json: false }), pinned(token)),
      textRun
    ));
    expect(text.out[0]).not.toStartWith('{');
    // `_batch_command` (client.py:8595-8600) closes an OK receipt with
    // `_next_focus_line(path, state, {actor})`, and this mirror holds no unit
    // or city table, so the focus degrades to the "nothing left" sentence.
    expect(text.out).toEqual([
      `unit.found_city Found city {ready=yes} → applied rev8/t3  ${batchId}`,
      'next: no actors need orders — just turn --end --await --brief',
    ]);
  });

  /**
   * `_submit_persisted_batch` (client.py:8528) calls `_mirror_receipt` next to
   * its `_remember_receipt`, so every send that produces a receipt appends an
   * `applied batch … at rev N` entry — and, when the tables are behind, the
   * "state files still show rev N" lag sentence — to `state/delta.md`.  That
   * file is what `just show delta` prints back verbatim, so a send that skips
   * the projection prints strictly fewer lines than CPython on every later
   * `show`.
   */
  awaitTest('the receipt is projected into state/delta.md, as _submit_persisted_batch does', function* (wait) {
    const token = 'P'.repeat(24);
    const batchId = `batch_${token}`;
    const f = yield* fixture(() => ({ status: 200, body: receiptBody(batchId, 'applied') }));
    yield* wait(seed(f, revision(7)));
    const captured = yield* wait(capture(runBatch(args(), pinned(token)), f));
    expect(exitCodeOf(captured.outcome)).toBe(0);
    const delta = path.join(yield* mirrorDir(f.sessionPath), 'state', 'delta.md');
    expect(yield* fileSystem.exists(delta)).toBe(true);
    expect(yield* fileSystem.readFileString(delta)).toContain(
      `applied batch ${batchId.slice(0, 16)} at rev 8`
    );
  });

  /**
   * `_resolve_alias_arguments(..., ("action_id",))` (client.py:8557) re-binds a
   * stale `aN` by draining its actor's catalog, which is the hot path after any
   * revision bump — an agent that read a catalog, ended a phase and then sent
   * the `a3` it was told about must not be refused for it.  With the drain
   * unwired every invocation behaved as though `--no-refresh` had been passed.
   */
  awaitTest('a stale action alias re-enumerates and re-binds, and --no-refresh does not', function* (wait) {
    const token = 'R'.repeat(24);
    const batchId = `batch_${token}`;
    const rebound = `action_${'2'.repeat(26)}`;
    const build = () =>
      Effect.gen(function* () {
        const urls: string[] = [];
        const fx = yield* fixture((url) => {
          urls.push(url);
          return url.includes('legal-actions')
            ? {
                status: 200,
                body: legalPage(revision(8), [descriptor(revision(8), rebound)]),
              }
            : { status: 200, body: receiptBody(batchId, 'applied', revision(8)) };
        });
        return { fx, urls };
      });

    // The alias table is pinned at rev7 while a receipt has moved the seat to
    // rev8: `a1` still names an action, but not one this revision knows.
    const refreshing = yield* build();
    yield* wait(stale(refreshing.fx));
    const captured = yield* wait(capture(
      runBatch(args({ actionId: 'a1', json: false }), pinned(token)),
      refreshing.fx
    ));
    expect(exitCodeOf(captured.outcome)).toBe(0);
    expect(refreshing.urls.some((url) => url.includes('legal-actions'))).toBe(true);
    expect(captured.out[0]).toBe('a1 rebound at rev8');
    expect(refreshing.fx.posts[0]).toContain(rebound);

    const refusing = yield* build();
    yield* wait(stale(refusing.fx));
    const refused = yield* wait(capture(
      runBatch(args({ actionId: 'a1', noRefresh: true, json: false }), pinned(token)),
      refusing.fx
    ));
    expect(exitCodeOf(refused.outcome)).toBe(-1);
    expect(refusing.urls.some((url) => url.includes('legal-actions'))).toBe(false);
    expect(refusing.fx.posts).toHaveLength(0);
  });

  /**
   * `_batch_command` (client.py:8586-8604) answers a refused batch with up to
   * `V2_REFUSAL_LEGAL_ROWS` of what the actor can actually do, and an accepted
   * one with `_next_focus_line`.  Both come from `_order_actor` reading the
   * descriptor out of `.v2-state` *before* the send, because applying the
   * action wipes it.
   */
  awaitTest('a refused batch prints what its actor can still do', function* (wait) {
    const token = 'S'.repeat(24);
    const batchId = `batch_${token}`;
    const f = yield* fixture((url) =>
      url.includes('legal-actions')
        ? { status: 200, body: legalPage(revision(7), [descriptor(revision(7))]) }
        : {
            status: 200,
            body: receiptBody(batchId, 'rejected', revision(7), {
              error: errorBody('invalid_request', {}, false, revision(7)),
            }),
          }
    );
    yield* wait(seed(f, revision(7)));
    const captured = yield* wait(capture(runBatch(args({ json: false }), pinned(token)), f));
    expect(exitCodeOf(captured.outcome)).toBe(0);
    expect(captured.out.some((line) => line.startsWith('u1 can (rev7/t3):'))).toBe(true);
    expect(captured.out.some((line) => line.includes('unit.found_city'))).toBe(true);
  });

  awaitTest('an ambiguous receipt says so on stderr, because it must never be replayed', function* (wait) {
    const token = 'M'.repeat(24);
    const batchId = `batch_${token}`;
    const ambiguousRevision = revision(8);
    const f = yield* fixture(() => ({
      status: 200,
      body: receiptBody(batchId, 'ambiguous', ambiguousRevision, {
        error: errorBody(
          'action_outcome_ambiguous',
          {},
          false,
          ambiguousRevision
        ),
      }),
    }));
    yield* wait(seed(f, revision(7)));
    const captured = yield* wait(capture(runBatch(args(), pinned(token)), f));
    expect(exitCodeOf(captured.outcome)).toBe(0);
    expect(captured.err).toContain('Ambiguous is terminal; never replay this batch.');
  });

  awaitTest('an unknown action is refused before anything is sent', function* (wait) {
    const f = yield* fixture(() => ({ status: 200, body: {} }));
    yield* wait(seed(f, revision(7)));
    const captured = yield* wait(capture(
      runBatch(args({ actionId: `action_${'9'.repeat(26)}` }), pinned('N'.repeat(24))),
      f
    ));
    expect(Either.isLeft(captured.outcome)).toBe(true);
    expect(f.posts).toHaveLength(0);
  });

  awaitTest('a stale action alias keeps its plain refusal when no drain is available', function* (wait) {
    const f = yield* fixture(() => ({ status: 200, body: {} }));
    yield* wait(seed(f, revision(7)));
    // A newer state page retires the catalog, so `a1` names an expired handle.
    const state = ok(yield* wait(f.run(f.store.readState(f.sessionPath, f.session))));
    ok(
      yield* wait(f.run(
        f.store.writeState(f.sessionPath, {
          ...state,
          last_revision: { turn: 3, revision: 9, state_token: revision(9).state_token },
          actions: {},
        })
      ))
    );
    const captured = yield* wait(capture(
      runBatch(args({ actionId: 'a1', noRefresh: true }), pinned('P'.repeat(24))),
      f
    ));
    expect(Either.isLeft(captured.outcome)).toBe(true);
    expect(f.posts).toHaveLength(0);
  });
});

// ---------------------------------------------------------------------------
// _phase_aware_refusal (client.py:10802-10817)
// ---------------------------------------------------------------------------

/**
 * The anti-loop line, on every refusal CPython raises as a `PlayerError`.
 *
 * `_opaque` and `_private_advisory_lock` both raise `PlayerError` in the
 * Python; the port gave them `DriftError` and `LockTimeoutError`, so matching
 * on the tag alone would have dropped the note from exactly the refusals an
 * agent hits after its phase ended — and then the cache-miss remedy "succeeds"
 * without fixing anything and the agent loops.
 */
describe('a refusal after the phase ended', () => {
  const STALLED = 'your phase is not active (state ending) — just wait';

  const stall = (f: Fixture): Effect.Effect<void> =>
    Effect.gen(function* () {
      const dir = ok(yield* f.run(mirrorDir(f.sessionPath)));
      ok(
        yield* f.run(
          writeMirror(dir, HEADER_FILE, 'phase     ending · turn 3 phase 0 · active yes\n')
        )
      );
    });

  const refusal = (
    f: Fixture,
    batchArgs: Parameters<typeof runBatch>[0]
  ): Effect.Effect<string> =>
    Effect.gen(function* () {
      const captured = yield* capture(runBatch(batchArgs, pinned('Q'.repeat(24))), f);
      expect(Either.isLeft(captured.outcome)).toBe(true);
      return refusalMessage(captured.outcome);
    });

  awaitTest('leads an invalid action ID, which is a DriftError here and a PlayerError there', function* (wait) {
    const f = yield* fixture();
    yield* wait(seed(f, revision(7)));
    yield* wait(stall(f));
    expect(yield* wait(refusal(f, args({ actionId: '' })))).toBe(`${STALLED}\ninvalid action ID`);
    expect(yield* wait(refusal(f, args({ actionId: 'not an id' })))).toBe(
      `${STALLED}\ninvalid action ID`
    );
    expect(f.posts).toHaveLength(0);
  });

  awaitTest('leads a plain PlayerError too, and says the phase fact exactly once', function* (wait) {
    const f = yield* fixture();
    yield* wait(seed(f, revision(7)));
    yield* wait(stall(f));
    expect(yield* wait(refusal(f, args({ actionId: `action_${'9'.repeat(26)}` })))).toBe(
      `${STALLED}\nunknown or expired action ID; run the matching \`just legal\` query`
    );
    // The whole sentence, through the command: CPython's message reaches
    // stderr unchanged behind the phase fact.
    expect(yield* wait(refusal(f, args({ arguments: '{"a":' })))).toBe(
      `${STALLED}\n--arguments must be valid strict JSON: Expecting value: line 1 ` +
        'column 6 (char 5)'
    );
    expect(f.posts).toHaveLength(0);
  });

  /**
   * The whole fail-open in one command: `"\ud800"` parses, so only
   * `_canonical_body`'s `.encode("utf-8")` stands between it and the wire.
   */
  awaitTest('a lone --arguments surrogate never reaches .v2-state or the wire', function* (wait) {
    const f = yield* fixture();
    yield* wait(seed(f, revision(7)));
    expect(yield* wait(refusal(f, args({ arguments: '{"name":"\\ud800"}' })))).toContain(
      "command batch is not canonical JSON: 'utf-8' codec can't encode character " +
        "'\\ud800' in position "
    );
    expect(f.posts).toHaveLength(0);
  });

  awaitTest('says nothing extra while the mirror does not claim the phase is dead', function* (wait) {
    const f = yield* fixture();
    yield* wait(seed(f, revision(7)));
    expect(yield* wait(refusal(f, args({ actionId: '' })))).toBe('invalid action ID');
  });

  awaitTest('never touches the quiet exit-2 signal a reported disposition carries', function* (wait) {
    const token = 'R'.repeat(24);
    const batchId = `batch_${token}`;
    const f = yield* fixture(() => ({
      status: 409,
      body: errorBody('conflict', {
        batch_id: batchId,
        acceptance: 'not_accepted',
        safe_next: 'receipt_first',
      }),
    }));
    yield* wait(seed(f, revision(7)));
    yield* wait(stall(f));
    const captured = yield* wait(capture(runBatch(args(), pinned(token)), f));
    // The disposition is *reported*: stdout carries it, the exit code is 2, and
    // no `error:` line is invented — least of all a phase note.
    expect(exitCodeOf(captured.outcome)).toBe(2);
    expect(captured.out).toHaveLength(1);
    expect(captured.err.some((line) => line.includes('your phase is not active'))).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// The disposition line, as `batch` prints it
//
// The renderer is U14's (`src/render/receipt.ts`); these cases assert the three
// shapes `batch` can reach through it, because the disposition is what decides
// whether another request is allowed at all.
// ---------------------------------------------------------------------------

describe('the disposition line', () => {
  awaitTest('a receipt-carrying disposition renders the receipt', function* (wait) {
    const f = yield* fixture();
    const built = ok(
      yield* wait(f.run(
        batchDisposition(f.session, 'batch_x', 'receipt_poll', {
          receipt: receiptBody('batch_x', 'accepted', revision(8), { idempotent: true }),
        })
      ))
    );
    expect(renderDisposition(built, 'unit.found_city Found city')).toEqual([
      'unit.found_city Found city → accepted idempotent (not final; resolve with ' +
        'just receipt) rev8/t3  batch_x',
    ]);
  });

  awaitTest('a not-accepted disposition names the error and the safe next step', function* (wait) {
    const f = yield* fixture();
    const built = ok(
      yield* wait(f.run(
        batchDisposition(f.session, 'batch_x', 'refresh', {
          error: errorBody('stale_revision', {}),
        })
      ))
    );
    expect(renderDisposition(built, 'a1')).toEqual([
      'a1 → not accepted: stale_revision: the supervisor refused with stale_revision ' +
        'next=refresh  batch_x',
    ]);
  });

  awaitTest('an unknown outcome says so rather than implying a retry is safe', function* (wait) {
    const f = yield* fixture();
    const built = ok(yield* wait(f.run(batchDisposition(f.session, 'batch_x', 'receipt_first'))));
    expect(renderDisposition(built, 'a1')).toEqual([
      'a1 → outcome unknown next=receipt_first  batch_x',
    ]);
  });
});
