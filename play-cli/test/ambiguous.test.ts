/**
 * Ambiguity is terminal. This is the suite where a bug loses a game.
 *
 * Ports `test_v2_retry_accepted_then_missing_is_ambiguous_without_resend`
 * (test_client.py:3784) and adds the adversarial cases the Python does not
 * have: an `ambiguous` receipt met by `retry`, a process killed between the
 * persist and the POST, and a receipt that flips `accepted → ambiguous`
 * between two polls.
 *
 * The invariant under test, stated once:
 *
 *   A batch this client has ever seen `accepted` — or whose receipt says
 *   `applied`, `rejected` or `ambiguous` — must never go back on the wire.
 *   The only licence to submit is "the server has no record of this batch AND
 *   this client has never seen a receipt for it".
 *
 * Every test below is a way of trying to break that and failing.
 */
import { afterEach, describe, expect, test } from 'bun:test';
import { Effect, Either, Layer, Schema } from 'effect';
import { V2_RECEIPT_STATES, V2_TERMINAL_RECEIPTS } from 'src/constants';
import type { PlayError } from 'src/errors';
import { runReceipt } from 'src/commands/receipt.cmd';
import { retryLocked, runRetry } from 'src/commands/retry.cmd';
import {
  isPollableReceipt,
  isTerminalReceipt,
  mayResubmitCachedReceipt,
  orderReceiptOk,
} from 'src/services/disposition';
import { decodeBatchDisposition, type BatchDisposition } from 'src/schema/batch';
import { decodeReceipt, type ReceiptState } from 'src/schema/receipt';
import { isJsonObject, type JsonObject } from 'src/schema/primitives';
import { FULL_CONTROL_V2 } from 'src/constants';
import { rememberReceipt } from 'src/services/aliases';
import { PrivateFs } from 'src/services/private-fs';
import {
  ACCEPTED_RECEIPT_VANISHED,
  type SubmitOutcome,
  AMBIGUOUS_IS_TERMINAL,
  AMBIGUOUS_REPLAY_UNSAFE,
  missingAcceptedReceipt,
} from 'src/services/receipts';
import { SessionStore } from 'src/services/session-store';
import {
  FIXTURE_AGENT_ID,
  FIXTURE_GAME_ID,
  identity,
  type FakeRoute,
} from 'test/_fixtures';
import {
  BATCH_ID,
  DESCRIPTOR,
  batchBody,
  buildFixture,
  capture,
  cleanupScratches,
  errorEnvelope,
  messageOf,
  receiptWire,
  recordingHooks,
  testClock,
  type Fixture,
} from 'test/receipt-harness';
import { awaitTest, provideTestLayer } from 'test/_effect-test';
import { fixtureString } from 'test/_expect';

afterEach(() => Effect.runPromise(cleanupScratches()));

const RECEIPT_ROUTE = '/receipts/';
const ok = (body: JsonObject): FakeRoute => ({ status: 200, body });
const missing = (): FakeRoute => ({ status: 404, body: errorEnvelope('invalid_request', null) });

const receiptStateJson = Schema.decodeUnknownSync(
  Schema.parseJson(Schema.Struct({ receipt_state: Schema.String }))
);

const buildDisposition = (kind: string, state: ReceiptState | null): BatchDisposition =>
  Effect.runSync(
    decodeBatchDisposition(
      {
        schema_version: 2,
        control_protocol: FULL_CONTROL_V2,
        game_id: FIXTURE_GAME_ID,
        agent_id: FIXTURE_AGENT_ID,
        batch_id: BATCH_ID,
        disposition: kind,
        receipt: state === null ? null : receiptWire(BATCH_ID, state),
        error: null,
      },
      identity()
    )
  );

/** Everything `batch` writes before it sends: the body, and nothing else. */
const halfWritten = (fixture: Fixture): Effect.Effect<void, PlayError> =>
  fixture.seed({
    actions: { action_opaque: DESCRIPTOR },
    batches: { [BATCH_ID]: batchBody(BATCH_ID) },
  });

// ---------------------------------------------------------------------------
// The truth table
// ---------------------------------------------------------------------------

describe('the four receipt states', () => {
  test('exactly three are terminal and exactly one is pollable', () => {
    const states: ReadonlyArray<ReceiptState> = [
      'accepted',
      'applied',
      'rejected',
      'ambiguous',
    ];
    // The vocabulary itself has not drifted.
    expect([...V2_RECEIPT_STATES].toSorted()).toEqual([...states].toSorted());
    expect(states.filter(isTerminalReceipt).toSorted()).toEqual([
      'ambiguous',
      'applied',
      'rejected',
    ]);
    expect(states.filter(isPollableReceipt)).toEqual(['accepted']);
    expect([...V2_TERMINAL_RECEIPTS].toSorted()).toEqual(['ambiguous', 'applied', 'rejected']);
    // And nothing in the cache is ever re-sendable, terminal or not.
    expect(states.map(mayResubmitCachedReceipt)).toEqual([false, false, false, false]);
  });

  test('orderReceiptOk: four states × both receipt-bearing dispositions', () => {
    // `receipt_poll` is only ever built for `accepted`, and `receipt_terminal`
    // only for the three terminal states — that is the server contract. The
    // table covers every legal (disposition, state) pair.
    expect(orderReceiptOk(buildDisposition('receipt_poll', 'accepted'))).toBe(true);
    expect(orderReceiptOk(buildDisposition('receipt_terminal', 'applied'))).toBe(true);
    expect(orderReceiptOk(buildDisposition('receipt_terminal', 'rejected'))).toBe(false);
    expect(orderReceiptOk(buildDisposition('receipt_terminal', 'ambiguous'))).toBe(false);

    // The receipt-less dispositions are never "ok", whatever they advise.
    for (const kind of ['receipt_first', 'retry_exact', 'refresh'] as const) {
      const value =
        kind === 'receipt_first'
          ? buildDisposition(kind, null)
          : Effect.runSync(
              decodeBatchDisposition(
                {
                  schema_version: 2,
                  control_protocol: FULL_CONTROL_V2,
                  game_id: FIXTURE_GAME_ID,
                  agent_id: FIXTURE_AGENT_ID,
                  batch_id: BATCH_ID,
                  disposition: kind,
                  receipt: null,
                  error: errorEnvelope('illegal_action', null),
                },
                identity()
              )
            );
      expect(orderReceiptOk(value)).toBe(false);
    }
  });

  test('an ambiguous outcome can never read as "the order went through"', () => {
    // Stated as its own test because it is the sentence a `do` summary is
    // built from: 1/1 applied vs 0/1. Getting this wrong replays a battle.
    const ambiguous = Effect.runSync(
      decodeBatchDisposition(
        {
          schema_version: 2,
          control_protocol: FULL_CONTROL_V2,
          game_id: FIXTURE_GAME_ID,
          agent_id: FIXTURE_AGENT_ID,
          batch_id: BATCH_ID,
          disposition: 'receipt_terminal',
          receipt: receiptWire(BATCH_ID, 'ambiguous'),
          error: null,
        },
        identity()
      )
    );
    expect(orderReceiptOk(ambiguous)).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// The manufactured terminal receipt
// ---------------------------------------------------------------------------

describe('missingAcceptedReceipt', () => {
  awaitTest('it is ambiguous, non-retryable, and carries the accepted revision', function* () {
    const accepted = Effect.runSync(
      decodeReceipt(receiptWire(BATCH_ID, 'accepted', { idempotent: true }), identity(), {
        batchId: BATCH_ID,
      })
    );
    const fixture = yield* buildFixture(new Map<string, FakeRoute>());
    const terminal = Effect.runSync(
      missingAcceptedReceipt(fixture.session, accepted, BATCH_ID)
    );
    expect(terminal.receipt_state).toBe('ambiguous');
    expect(terminal.batch_id).toBe(BATCH_ID);
    expect(terminal.idempotent).toBe(true);
    expect(terminal.state_revision).toEqual(accepted.state_revision);
    expect(terminal.error?.error.code).toBe('action_outcome_ambiguous');
    expect(terminal.error?.error.retryable).toBe(false);
    expect(terminal.error?.error.message).toBe(AMBIGUOUS_REPLAY_UNSAFE);
    expect(terminal.observation).toBeNull();
  });

  test('it goes through the real validator, so it cannot be retryable', () => {
    // The validator refuses an ambiguous receipt whose error is retryable; the
    // manufactured one is built to satisfy it rather than to bypass it.
    const retryableAmbiguous = receiptWire(BATCH_ID, 'ambiguous', {
      error: errorEnvelope('action_outcome_ambiguous', undefined, true),
    });
    const outcome = Effect.runSync(
      Effect.either(decodeReceipt(retryableAmbiguous, identity(), { batchId: BATCH_ID }))
    );
    expect(Either.isLeft(outcome)).toBe(true);
    if (Either.isLeft(outcome)) expect(messageOf(outcome.left)).toContain('ambiguous receipt');
  });
});

// ---------------------------------------------------------------------------
// retry meets an ambiguous receipt
// ---------------------------------------------------------------------------

describe('retry refuses to resubmit an ambiguous batch', () => {
  awaitTest('a cached ambiguous receipt: no GET, no POST, and the warning on stderr', (wait) => Effect.gen(function* () {
    const fixture = yield* buildFixture(new Map<string, FakeRoute>());
    yield* fixture.seed({
      actions: { action_opaque: DESCRIPTOR },
      batches: { [BATCH_ID]: batchBody(BATCH_ID) },
      receipts: { [BATCH_ID]: receiptWire(BATCH_ID, 'ambiguous') },
    });
    const hooks = recordingHooks();
    const { out, err, result } = yield* wait(capture(
      runRetry({ session: fixture.sessionPath, batchId: BATCH_ID, json: false }, hooks.make),
      fixture
    ));
    expect(Either.isRight(result)).toBe(true);
    expect(fixture.requests).toEqual([]);
    expect(hooks.submits).toEqual([]);
    expect(out).toEqual([
      `phase.end End phase {ready=yes} → ambiguous action_outcome_ambiguous: ` +
        `validated test error rev8/t3  ${BATCH_ID}`,
    ]);
    expect(err).toEqual([AMBIGUOUS_IS_TERMINAL]);
  }));

  awaitTest('the warning is on stderr even under --json, and stdout stays parseable', (wait) => Effect.gen(function* () {
    const fixture = yield* buildFixture(new Map<string, FakeRoute>());
    yield* fixture.seed({
      batches: { [BATCH_ID]: batchBody(BATCH_ID) },
      receipts: { [BATCH_ID]: receiptWire(BATCH_ID, 'ambiguous') },
    });
    const { out, err } = yield* wait(capture(
      runRetry({ session: fixture.sessionPath, batchId: BATCH_ID, json: true }),
      fixture
    ));
    expect(out).toHaveLength(1);
    expect(receiptStateJson(out[0] ?? '').receipt_state).toBe('ambiguous');
    expect(err).toEqual([AMBIGUOUS_IS_TERMINAL]);
  }));

  awaitTest('a server-sent ambiguous receipt stops the command dead', (wait) => Effect.gen(function* () {
    const fixture = yield* buildFixture(new Map([[RECEIPT_ROUTE, ok(receiptWire(BATCH_ID, 'ambiguous'))]]));
    yield* fixture.seed({ batches: { [BATCH_ID]: batchBody(BATCH_ID) } });
    const hooks = recordingHooks();
    const { err, result } = yield* wait(capture(
      runRetry({ session: fixture.sessionPath, batchId: BATCH_ID, json: false }, hooks.make),
      fixture
    ));
    expect(Either.isRight(result)).toBe(true);
    expect(fixture.requests).toHaveLength(1);
    expect(hooks.submits).toEqual([]);
    expect(err).toEqual([AMBIGUOUS_IS_TERMINAL]);
  }));

  awaitTest('and a second retry after that answers from the cache, silently offline', (wait) => Effect.gen(function* () {
    const fixture = yield* buildFixture(new Map([[RECEIPT_ROUTE, ok(receiptWire(BATCH_ID, 'ambiguous'))]]));
    yield* fixture.seed({ batches: { [BATCH_ID]: batchBody(BATCH_ID) } });
    yield* wait(capture(
      runRetry({ session: fixture.sessionPath, batchId: BATCH_ID, json: false }),
      fixture
    ));
    const after = fixture.requests.length;
    const { err, result } = yield* wait(capture(
      runRetry({ session: fixture.sessionPath, batchId: BATCH_ID, json: false }),
      fixture
    ));
    expect(Either.isRight(result)).toBe(true);
    expect(fixture.requests).toHaveLength(after);
    expect(err).toEqual([AMBIGUOUS_IS_TERMINAL]);
  }));

  awaitTest('`receipt` warns too, and still exits successfully', (wait) => Effect.gen(function* () {
    const fixture = yield* buildFixture(new Map([[RECEIPT_ROUTE, ok(receiptWire(BATCH_ID, 'ambiguous'))]]));
    yield* fixture.seed({ batches: { [BATCH_ID]: batchBody(BATCH_ID) } });
    const { out, err, result } = yield* wait(capture(
      runReceipt({ session: fixture.sessionPath, batchId: BATCH_ID, json: false }),
      fixture
    ));
    expect(Either.isRight(result)).toBe(true);
    expect(out[0]).toContain('→ ambiguous action_outcome_ambiguous');
    expect(err).toEqual([AMBIGUOUS_IS_TERMINAL]);
  }));
});

// ---------------------------------------------------------------------------
// accepted → vanished (the ported CPython case)
// ---------------------------------------------------------------------------

describe('an accepted receipt that disappears', () => {
  awaitTest('it becomes ambiguous without a resend, and is persisted that way', (wait) => Effect.gen(function* () {
    const fixture = yield* buildFixture(new Map([[RECEIPT_ROUTE, missing()]]));
    yield* fixture.seed({
      batches: { [BATCH_ID]: batchBody(BATCH_ID) },
      receipts: { [BATCH_ID]: receiptWire(BATCH_ID, 'accepted') },
    });
    const hooks = recordingHooks();
    const { out, err, result } = yield* wait(capture(
      runRetry({ session: fixture.sessionPath, batchId: BATCH_ID, json: true }, hooks.make),
      fixture
    ));
    expect(Either.isRight(result)).toBe(true);
    expect(hooks.submits).toEqual([]);
    expect(fixture.requests.map((request) => request.method)).toEqual(['GET']);
    expect(receiptStateJson(out[0] ?? '').receipt_state).toBe('ambiguous');
    expect(err).toEqual([ACCEPTED_RECEIPT_VANISHED]);
    // The ambiguity is now the persisted truth, so no later command can undo it.
    const persisted = (yield* fixture.readState()).receipts[BATCH_ID];
    expect(isJsonObject(persisted) ? persisted['receipt_state'] : null).toBe('ambiguous');
  }));

  awaitTest('an accepted receipt seen only during this poll counts just as much', (wait) => Effect.gen(function* () {
    // Nothing was cached: the FIRST poll returns `accepted`, the second 404s.
    // CPython terminalizes on the strength of that in-flight observation, and
    // so does this. A client that only trusted the cache would re-send here.
    const fixture = yield* buildFixture([ok(receiptWire(BATCH_ID, 'accepted')), missing()]);
    yield* fixture.seed({ batches: { [BATCH_ID]: batchBody(BATCH_ID) } });
    const hooks = recordingHooks();
    const { err, result } = yield* wait(capture(
      retryLocked(
        fixture.sessionPath,
        fixture.session,
        { session: fixture.sessionPath, batchId: BATCH_ID, json: false },
        hooks.make,
        testClock()
      ),
      fixture
    ));
    expect(Either.isRight(result)).toBe(true);
    expect(hooks.submits).toEqual([]);
    expect(err).toEqual([ACCEPTED_RECEIPT_VANISHED]);
    expect(hooks.remembered.map((receipt) => receipt.receipt_state)).toEqual([
      'accepted',
      'ambiguous',
    ]);
  }));
});

// ---------------------------------------------------------------------------
// SIGKILL between the persist and the POST
// ---------------------------------------------------------------------------

describe('killed between the persist and the POST', () => {
  awaitTest('retry re-sends exactly the persisted bytes, exactly once', (wait) => Effect.gen(function* () {
    const fixture = yield* buildFixture(new Map([[RECEIPT_ROUTE, missing()]]));
    yield* halfWritten(fixture);
    const posted: string[] = [];
    const provided = Layer.merge(
      Layer.succeed(SessionStore, fixture.store),
      Layer.succeed(PrivateFs, fixture.scratch.files)
    );
    const hooks = recordingHooks({
      submitPersistedBatch: (batchId: string) =>
        Effect.gen(function* () {
          const state = yield* fixture.store.readState(fixture.sessionPath, fixture.session);
          const encoded = state.batches[batchId];
          posted.push(fixtureString(encoded));
          const receipt = yield* decodeReceipt(receiptWire(batchId), fixture.session, {
            batchId,
          });
          yield* provideTestLayer(
            rememberReceipt(fixture.sessionPath, fixture.session, receipt),
            provided
          );
          const disposition = yield* decodeBatchDisposition(
            {
              schema_version: 2,
              control_protocol: FULL_CONTROL_V2,
              game_id: FIXTURE_GAME_ID,
              agent_id: FIXTURE_AGENT_ID,
              batch_id: batchId,
              disposition: 'receipt_terminal',
              receipt: receiptWire(batchId),
              error: null,
            },
            fixture.session
          );
          const submission: SubmitOutcome = { disposition, warning: null, exitCode: 0 };
          return submission;
        }).pipe(Effect.orDie),
    });
    const { out, result } = yield* wait(capture(
      runRetry({ session: fixture.sessionPath, batchId: BATCH_ID, json: false }, hooks.make),
      fixture
    ));
    expect(Either.isRight(result)).toBe(true);
    // Byte-identical to what the killed process wrote — a re-derived body would
    // be a different idempotency key and could apply the order twice.
    expect(posted).toEqual([batchBody(BATCH_ID)]);
    expect(out[0]).toContain('phase.end End phase {ready=yes} → applied');

    // Run it again: the receipt is terminal now, so the send does not repeat.
    const again = recordingHooks();
    yield* wait(capture(
      runRetry({ session: fixture.sessionPath, batchId: BATCH_ID, json: false }, again.make),
      fixture
    ));
    expect(again.submits).toEqual([]);
  }));

  awaitTest('a kill AFTER the POST but before the receipt read never re-sends', (wait) => Effect.gen(function* () {
    // The server has the batch and answers with its receipt; nothing about the
    // local state says the POST happened, and it still must not go again.
    const fixture = yield* buildFixture(new Map([[RECEIPT_ROUTE, ok(receiptWire(BATCH_ID, 'applied'))]]));
    yield* halfWritten(fixture);
    const hooks = recordingHooks();
    const { out, result } = yield* wait(capture(
      runRetry({ session: fixture.sessionPath, batchId: BATCH_ID, json: false }, hooks.make),
      fixture
    ));
    expect(Either.isRight(result)).toBe(true);
    expect(hooks.submits).toEqual([]);
    expect(out[0]).toContain('→ applied');
  }));

  awaitTest('a kill leaving an accepted receipt behind never re-sends either', (wait) => Effect.gen(function* () {
    const fixture = yield* buildFixture(new Map([[RECEIPT_ROUTE, missing()]]));
    yield* fixture.seed({
      batches: { [BATCH_ID]: batchBody(BATCH_ID) },
      receipts: { [BATCH_ID]: receiptWire(BATCH_ID, 'accepted') },
    });
    const hooks = recordingHooks();
    yield* wait(capture(
      runRetry({ session: fixture.sessionPath, batchId: BATCH_ID, json: false }, hooks.make),
      fixture
    ));
    expect(hooks.submits).toEqual([]);
  }));
});

// ---------------------------------------------------------------------------
// accepted → ambiguous between two polls
// ---------------------------------------------------------------------------

describe('a receipt that flips accepted → ambiguous between polls', () => {
  awaitTest('the poll stops at the flip, warns, and never sends', (wait) => Effect.gen(function* () {
    const fixture = yield* buildFixture([
      ok(receiptWire(BATCH_ID, 'accepted')),
      ok(receiptWire(BATCH_ID, 'accepted')),
      ok(receiptWire(BATCH_ID, 'ambiguous')),
      // A fourth response would mean the loop kept going past a terminal
      // state; it is deliberately an `applied` so the test fails loudly.
      ok(receiptWire(BATCH_ID, 'applied')),
    ]);
    yield* fixture.seed({ batches: { [BATCH_ID]: batchBody(BATCH_ID) } });
    const hooks = recordingHooks();
    const clock = testClock();
    const { out, err, result } = yield* wait(capture(
      retryLocked(
        fixture.sessionPath,
        fixture.session,
        { session: fixture.sessionPath, batchId: BATCH_ID, json: false },
        hooks.make,
        clock
      ),
      fixture
    ));
    expect(Either.isRight(result)).toBe(true);
    expect(fixture.requests).toHaveLength(3);
    expect(hooks.submits).toEqual([]);
    expect(out).toHaveLength(1);
    expect(out[0]).toContain('→ ambiguous action_outcome_ambiguous');
    expect(out[0]).not.toContain('applied');
    expect(err).toEqual([AMBIGUOUS_IS_TERMINAL]);
    expect(hooks.remembered.map((receipt) => receipt.receipt_state)).toEqual([
      'accepted',
      'accepted',
      'ambiguous',
    ]);
  }));

  awaitTest('the flip is durable: the state layer refuses to walk it back', (wait) => Effect.gen(function* () {
    // Even if a later response claimed `applied` for the same batch, U03's
    // receipt ledger refuses the transition. Terminal means terminal on disk,
    // not just in this process.
    const fixture = yield* buildFixture(new Map<string, FakeRoute>());
    yield* fixture.seed({
      batches: { [BATCH_ID]: batchBody(BATCH_ID) },
      receipts: { [BATCH_ID]: receiptWire(BATCH_ID, 'ambiguous') },
    });
    const applied = (yield* wait(decodeReceipt(receiptWire(BATCH_ID, 'applied'), fixture.session, { batchId: BATCH_ID })));
    const outcome = yield* wait(Effect.either(
        provideTestLayer(
          rememberReceipt(fixture.sessionPath, fixture.session, applied),
          Layer.merge(
            Layer.succeed(SessionStore, fixture.store),
            Layer.succeed(PrivateFs, fixture.scratch.files)
          )
        )
      ));
    expect(Either.isLeft(outcome)).toBe(true);
    if (Either.isLeft(outcome)) {
      expect(messageOf(outcome.left)).toContain('regressed or changed terminal state');
    }
  }));
});
