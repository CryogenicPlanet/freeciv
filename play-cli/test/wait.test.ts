/**
 * The wait engine.
 *
 * Ports the `test_v2_wait_*` cases from `play/tests/test_client.py` — the
 * request shape (`/me/wait?…`, never `/state?…`), the three refusals, the
 * legacy fallback for supervisors predating the private route, and the
 * deadline-bounded `--for-turn` loop with its per-tick marker write and
 * transcript line.
 *
 * Wall-clock time is the thing under test in the `--for-turn` cases — the whole
 * point of a deadline-bounded wait is how long it blocks for — so the tests own
 * the clock rather than sleeping through it, exactly as the Python's `clocked`
 * context manager does.
 */
import { afterEach, describe, expect, test } from 'bun:test';
import { Effect, Either, Layer } from 'effect';
import { decodeHealth, type HealthEnvelope } from 'src/schema/health';
import type { PageEnvelope } from 'src/schema/page';
import type { JsonObject, JsonValue } from 'src/schema/primitives';
import { decodeWait, type WaitEnvelope } from 'src/schema/wait';
import { renderHealth } from 'src/render/health';
import { awaitLine, renderWait, waitingTickLine } from 'src/render/wait';
import { httpFor } from 'src/services/http';
import {
  SessionStore,
  emptyV2ClientState,
  sessionStoreFor,
  type Session,
  type SessionStoreApi,
} from 'src/services/session-store';
import { V2Client, v2ClientFor } from 'src/services/v2-client';
import {
  V2_FOR_TURN_GRACE_S,
  V2_WAIT_S_MAX,
  V2_WAIT_TICK_S,
  holderRemainingS,
  legacyWaitValue,
  localWaitResponse,
  seatRebound,
  waitArgs,
  waitCommandValue,
  waitCtx,
  waitUntilTurn,
  waitValue,
  type HolderSeatFn,
  type WaitClock,
  type WaitCtx,
  type WaitHooks,
} from 'src/services/wait';
import {
  FIXTURE_AGENT_ID,
  FIXTURE_GAME_ID,
  healthPayload,
  jsonResponse,
  pagePayload,
  scratchWorkspace,
  sessionFile,
  waitPayload,
  type Scratch,
} from 'test/_fixtures';
import { awaitTest, provideTestLayer } from 'test/_effect-test';
import { leftValue, rightValue } from 'test/_expect';
import { fileSystem, path } from 'test/_test-platform';

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

type FetchInput = Parameters<typeof fetch>[0];
type FetchArguments = Parameters<typeof fetch>;

const urlOf = (input: FetchInput): string =>
  input instanceof Request ? input.url : new URL(input).href;

const completeFetch = (
  handler: (...args: FetchArguments) => ReturnType<typeof fetch>
): typeof fetch => Object.assign(handler, { preconnect: fetch.preconnect });

const scratches: Scratch[] = [];

afterEach(() =>
  Promise.all(scratches.splice(0).map((scratch) => scratch.cleanup()))
);

/** The core placeholder for the U03 seam; alias behaviour is U03's to prove. */
const stateSchema = {
  empty: emptyV2ClientState,
  validate: () => Effect.void,
  cursorExpired: (): boolean => false,
};

interface Bench {
  readonly scratch: Scratch;
  readonly sessionPath: string;
  readonly session: Session;
  readonly store: SessionStoreApi;
}

const bench = (): Bench => {
  const scratch = scratchWorkspace();
  scratches.push(scratch);
  const sessionPath = path.join(scratch.workspace.stateRoot, 'session-codex-gpt-5.6-sol.json');
  Effect.runSync(scratch.files.writeJson(sessionPath, sessionFile()));
  const store = sessionStoreFor(scratch.workspace, scratch.files, stateSchema, {});
  const loaded = Effect.runSync(store.resolveV2(sessionPath));
  return { scratch, sessionPath, session: loaded.session, store };
};

/**
 * `_holder_seat` (client.py:5350-5367).
 *
 * U06 owns the real one; the engine takes it as a hook so wave 1 can be tested
 * without it, and this restatement is the hook these tests inject.
 */
const holderSeat: HolderSeatFn = (phase) => {
  if (phase === null ||  phase.active) return null;
  const waitingOn = phase.waiting_on;
  if (waitingOn === undefined || waitingOn === null) return null;
  const others = waitingOn.seats.filter((row) => row.is_self === false);
  return others.length === 1 ? (others[0] ?? null) : null;
};

interface Recorded {
  readonly pages: PageEnvelope[];
  readonly mirroredPages: PageEnvelope[];
  readonly mirroredHealth: HealthEnvelope[];
  readonly echoed: HealthEnvelope[];
  readonly order: string[];
}

interface Kit {
  readonly hooks: WaitHooks;
  readonly log: Recorded;
  readonly echo: (health: HealthEnvelope) => Effect.Effect<void>;
}

const recorder = (): Kit => {
  const log: Recorded = {
    pages: [],
    mirroredPages: [],
    mirroredHealth: [],
    echoed: [],
    order: [],
  };
  return {
    log,
    echo: (health) =>
      Effect.sync(() => {
        log.echoed.push(health);
        log.order.push('echo');
      }),
    hooks: {
      rememberPage: (page) =>
        Effect.sync(() => {
          log.pages.push(page);
          log.order.push('remember');
        }),
      mirrorPage: (page) =>
        Effect.sync(() => {
          log.mirroredPages.push(page);
          log.order.push('mirror-page');
        }),
      mirrorHealth: (health) =>
        Effect.sync(() => {
          log.mirroredHealth.push(health);
          log.order.push('mirror-health');
        }),
      holderSeat,
    },
  };
};

const layers = (
  fetchImpl: typeof fetch,
  store: SessionStoreApi
): Layer.Layer<V2Client | SessionStore> =>
  Layer.mergeAll(
    Layer.succeed(V2Client, v2ClientFor(httpFor(fetchImpl), () => Effect.void)),
    Layer.succeed(SessionStore, store)
  );

const run = <A, E>(
  effect: Effect.Effect<A, E, V2Client | SessionStore>,
  fetchImpl: typeof fetch,
  store: SessionStoreApi
): Promise<Either.Either<A, E>> =>
  Effect.runPromise(Effect.either(provideTestLayer(effect, layers(fetchImpl, store))));

const right = <A, E>(either: Either.Either<A, E>): A => rightValue(either);

const message = <A>(either: Either.Either<A, { readonly message: string }>): string =>
  leftValue(either).message;

interface Reply {
  readonly status?: number;
  readonly body: JsonValue;
}

interface ScriptedServer {
  readonly fetch: typeof fetch;
  readonly urls: ReadonlyArray<string>;
}

/** A fetch that records every URL and answers from a per-URL script. */
const scripted = (answer: (url: string) => Reply): ScriptedServer => {
  const urls: string[] = [];
  return {
    fetch: completeFetch((input) => {
      const url = urlOf(input);
      const reply = answer(url);
      urls.push(url);
      return Promise.resolve(jsonResponse(reply.body, reply.status ?? 200));
    }),
    urls,
  };
};

// ---------------------------------------------------------------------------
// The PvP payloads: one seat holding the phase, one waiting on it
// ---------------------------------------------------------------------------

const OPPONENT: JsonObject = {
  place: 2,
  seat_id: 'place-2',
  player_name: 'AgentPlace2',
  controller_label: 'pi-gpt-5.6-sol',
  standing: 'active',
  is_self: false,
};

interface PvpHealthFixture {
  readonly mine: boolean;
  readonly remainingS?: number | null;
  readonly elapsedS?: number | null;
  readonly gameState?: string;
  readonly selfHeld?: boolean;
}

const pvpHealth = (fixture: PvpHealthFixture): JsonObject => {
  const elapsedS = fixture.elapsedS === undefined ? 13 : fixture.elapsedS;
  const remainingS = fixture.remainingS === undefined ? 587 : fixture.remainingS;
  const waitingOn: JsonObject = {
    kind: 'other_seat',
    summary: 'Seat 2 AgentPlace2 (pi-gpt-5.6-sol) holds turn 3 phase 1 and has not ended it.',
    waiting_s: elapsedS,
    seats: fixture.selfHeld === true ? [{ ...OPPONENT, is_self: true }] : [OPPONENT],
  };
  const timing: JsonObject = {
    mode: 'default',
    timeout_s: 600,
    deadline_started_at: 1000,
    deadline_at: 1600,
    elapsed_s: elapsedS,
    remaining_s: remainingS,
  };
  const phase: JsonObject = fixture.mine
    ? { state: 'awaiting_agent', turn: 3, phase: 1, active: true, timing }
    : {
        state: 'awaiting_agent',
        turn: 3,
        phase: 1,
        active: false,
        timing,
        waiting_on: waitingOn,
      };
  return healthPayload({
    game_state: fixture.gameState ?? 'running',
    phase,
  });
};

const pvpWake = (reason: string, fixture: PvpHealthFixture): JsonObject =>
  waitPayload({ wake_reason: reason, health: pvpHealth(fixture) });

// ---------------------------------------------------------------------------
// _wait_args
// ---------------------------------------------------------------------------

describe('waitArgs', () => {
  test('a missing knob takes the default, a given one is untouched', () => {
    expect(waitArgs({})).toEqual({ waitS: 120, pollS: 1, until: 'phase' });
    expect(waitArgs({ waitS: 30, pollS: 0.5, until: 'revision' })).toEqual({
      waitS: 30,
      pollS: 0.5,
      until: 'revision',
    });
  });

  test('--wait-s 0 is a legal non-blocking poll, not a missing value', () => {
    expect(waitArgs({ waitS: 0 }).waitS).toBe(0);
    expect(waitArgs({ pollS: 0 }).pollS).toBe(0);
  });

  test('the override replaces wait-s for one internal poll and nothing else', () => {
    expect(waitArgs({ waitS: 120, pollS: 2, until: 'revision' }, 15)).toEqual({
      waitS: 15,
      pollS: 2,
      until: 'revision',
    });
  });
});

// ---------------------------------------------------------------------------
// _wait_value
// ---------------------------------------------------------------------------

describe('waitValue', () => {
  awaitTest('uses the server actionable phase without ever fetching state', function* (wait) {
    const seat = bench();
    const server = scripted(() => ({ body: waitPayload() }));
    const ctx = waitCtx({ sessionPath: seat.sessionPath, session: seat.session, hooks: recorder().hooks });
    const wake = right(yield* wait(run(waitValue(ctx, waitArgs({})), server.fetch, seat.store)));

    expect(wake.wake_reason).toBe('phase_active');
    expect(server.urls).toHaveLength(1);
    expect(server.urls[0]).toContain('/me/wait?');
    expect(server.urls[0]).not.toContain('/state?');
    expect(server.urls[0]).toContain('wait_s=120');
    expect(server.urls[0]).toContain('until=phase');
    expect(server.urls[0]).not.toContain('after_state_token');
  });

  awaitTest('wait_s reaches the wire in CPython %g', function* (wait) {
    const seat = bench();
    const ctx = waitCtx({ sessionPath: seat.sessionPath, session: seat.session, hooks: recorder().hooks });
    for (const [given, encoded] of [
      [615, 'wait_s=615'],
      [0, 'wait_s=0'],
      [0.5, 'wait_s=0.5'],
      [1.5, 'wait_s=1.5'],
    ] as const) {
      const server = scripted(() => ({ body: waitPayload() }));
      right(yield* wait(run(waitValue(ctx, waitArgs({ waitS: given })), server.fetch, seat.store)));
      expect(server.urls[0]).toContain(encoded);
    }
  });

  awaitTest('the ceiling covers a whole opponent phase and refuses one second more', function* (wait) {
    expect(V2_WAIT_S_MAX).toBe(615);
    const seat = bench();
    const server = scripted(() => ({ body: waitPayload() }));
    const ctx = waitCtx({ sessionPath: seat.sessionPath, session: seat.session, hooks: recorder().hooks });
    expect(message(yield* wait(run(waitValue(ctx, waitArgs({ waitS: 616 })), server.fetch, seat.store)))).toBe(
      'wait-s must be in [0, 615]'
    );
    expect(message(yield* wait(run(waitValue(ctx, waitArgs({ waitS: -1 })), server.fetch, seat.store)))).toBe(
      'wait-s must be in [0, 615]'
    );
    expect(server.urls).toHaveLength(0);
  });

  awaitTest('poll-s is bounded too', function* (wait) {
    const seat = bench();
    const server = scripted(() => ({ body: waitPayload() }));
    const ctx = waitCtx({ sessionPath: seat.sessionPath, session: seat.session, hooks: recorder().hooks });
    expect(message(yield* wait(run(waitValue(ctx, waitArgs({ pollS: 0.04 })), server.fetch, seat.store)))).toBe(
      'poll-s must be in [0.05, 30]'
    );
    expect(message(yield* wait(run(waitValue(ctx, waitArgs({ pollS: 31 })), server.fetch, seat.store)))).toBe(
      'poll-s must be in [0.05, 30]'
    );
  });

  awaitTest('--until takes exactly two values', function* (wait) {
    const seat = bench();
    const server = scripted(() => ({ body: waitPayload() }));
    const ctx = waitCtx({ sessionPath: seat.sessionPath, session: seat.session, hooks: recorder().hooks });
    expect(
      message(yield* wait(run(waitValue(ctx, waitArgs({ until: 'forever' })), server.fetch, seat.store)))
    ).toBe('wait --until must be phase or revision');
  });

  awaitTest('--until revision without a validated state page is refused', function* (wait) {
    const seat = bench();
    const server = scripted(() => ({ body: waitPayload() }));
    const ctx = waitCtx({ sessionPath: seat.sessionPath, session: seat.session, hooks: recorder().hooks });
    expect(
      message(yield* wait(run(waitValue(ctx, waitArgs({ until: 'revision' })), server.fetch, seat.store)))
    ).toBe('wait --until revision requires a previously validated state page');
  });

  awaitTest('a stateless wait is refused outside phase mode', function* (wait) {
    const seat = bench();
    const server = scripted(() => ({ body: waitPayload() }));
    const ctx = waitCtx({ sessionPath: seat.sessionPath, session: seat.session, hooks: recorder().hooks });
    expect(
      message(
        yield* wait(run(
          waitValue(ctx, waitArgs({ until: 'revision' }), { stateless: true }),
          server.fetch,
          seat.store
        ))
      )
    ).toBe('a stateless wait is phase-mode only');
  });

  awaitTest('a stateless wait never reads .v2-state', function* (wait) {
    const seat = bench();
    const server = scripted(() => ({ body: waitPayload() }));
    const ctx = waitCtx({ sessionPath: seat.sessionPath, session: seat.session, hooks: recorder().hooks });
    right(yield* wait(run(waitValue(ctx, waitArgs({}), { stateless: true }), server.fetch, seat.store)));
    expect(yield* wait(fileSystem.exists(seat.store.statePath(seat.sessionPath)))).toBe(false);
    expect(server.urls[0]).toContain('until=phase');
    expect(server.urls[0]).not.toContain('after_state_token');
  });

  awaitTest('--until revision sends the cached baseline as after_state_token', function* (wait) {
    const seat = bench();
    Effect.runSync(
      seat.store.writeState(seat.sessionPath, {
        ...emptyV2ClientState(seat.session),
        last_revision: { turn: 5, revision: 12, state_token: 'token_5_12' },
      })
    );
    const server = scripted(() => ({
      body: waitPayload({
        wake_reason: 'revision_changed',
        state_revision: { turn: 5, revision: 13, state_token: 'token_5_13' },
      }),
    }));
    const ctx = waitCtx({ sessionPath: seat.sessionPath, session: seat.session, hooks: recorder().hooks });
    const wake = right(
      yield* wait(run(waitValue(ctx, waitArgs({ until: 'revision' })), server.fetch, seat.store))
    );
    expect(wake.wake_reason).toBe('revision_changed');
    expect(server.urls[0]).toContain('until=revision');
    expect(server.urls[0]).toContain('after_state_token=token_5_12');
  });

  awaitTest('a structured non-2xx refusal is raised, never downgraded to polling', function* (wait) {
    const seat = bench();
    const server = scripted(() => ({
      status: 404,
      body: {
        schema_version: 2,
        control_protocol: 'full-control-v2',
        error: { code: 'not_found', message: 'no such seat', retryable: false, details: {} },
        state_revision: null,
      },
    }));
    const ctx = waitCtx({ sessionPath: seat.sessionPath, session: seat.session, hooks: recorder().hooks });
    const either = yield* wait(run(waitValue(ctx, waitArgs({})), server.fetch, seat.store));
    expect(Either.isLeft(either)).toBe(true);
    // One request: the legacy fallback must not have started polling /health.
    expect(server.urls).toHaveLength(1);
  });
});

// ---------------------------------------------------------------------------
// _legacy_wait_value
// ---------------------------------------------------------------------------

describe('the legacy fallback', () => {
  const missingRoute: Reply = { status: 404, body: { error: 'Not Found' } };

  awaitTest('a bare-string 404 downgrades to polling /health', function* (wait) {
    const seat = bench();
    const server = scripted((url) =>
      url.includes('/me/wait?') ? missingRoute : { body: healthPayload() }
    );
    const ctx = waitCtx({ sessionPath: seat.sessionPath, session: seat.session, hooks: recorder().hooks });
    const wake = right(yield* wait(run(waitValue(ctx, waitArgs({})), server.fetch, seat.store)));
    expect(wake.wake_reason).toBe('phase_active');
    expect(server.urls[1]).toContain('/me/health');
    expect(server.urls.filter((url) => url.includes('/state?'))).toHaveLength(0);
  });

  awaitTest('a terminal game answers game_terminal without waiting out the clock', function* (wait) {
    const seat = bench();
    const server = scripted((url) =>
      url.includes('/me/wait?')
        ? missingRoute
        : { body: healthPayload({ game_state: 'completed', phase: null }) }
    );
    const ctx = waitCtx({ sessionPath: seat.sessionPath, session: seat.session, hooks: recorder().hooks });
    const wake = right(yield* wait(run(waitValue(ctx, waitArgs({})), server.fetch, seat.store)));
    expect(wake.wake_reason).toBe('game_terminal');
    expect(wake.state_revision).toBeNull();
  });

  awaitTest('an inactive phase polls until the budget runs out, then times out', function* (wait) {
    const seat = bench();
    const server = scripted((url) =>
      url.includes('/me/wait?') ? missingRoute : { body: pvpHealth({ mine: false }) }
    );
    const now = { seconds: 0 };
    const clock: WaitClock = {
      monotonic: () => Effect.sync(() => now.seconds),
      sleep: (seconds) =>
        Effect.sync(() => {
          now.seconds += seconds;
        }),
    };
    const ctx = waitCtx({
      sessionPath: seat.sessionPath,
      session: seat.session,
      hooks: recorder().hooks,
      clock,
    });
    const wake = right(
      yield* wait(run(waitValue(ctx, waitArgs({ waitS: 3, pollS: 1 })), server.fetch, seat.store))
    );
    expect(wake.wake_reason).toBe('timeout');
    // One `/wait` 404, then a `/health` poll at t=0, 1, 2 and 3.
    expect(server.urls.filter((url) => url.includes('/me/health'))).toHaveLength(4);
    expect(now.seconds).toBe(3);
  });

  awaitTest('revision mode fetches the overview page, remembers it and mirrors it', function* (wait) {
    const seat = bench();
    const kit = recorder();
    const bumped = pagePayload([{ id: 'unit_0', name: 'Warriors' }], {
      state_revision: { turn: 5, revision: 13, state_token: 'token_5_13' },
    });
    const server = scripted((url) => {
      if (url.includes('/me/health')) return { body: pvpHealth({ mine: false }) };
      return { body: bumped };
    });
    const ctx = waitCtx({ sessionPath: seat.sessionPath, session: seat.session, hooks: kit.hooks });
    const wake = right(
      yield* wait(run(
        legacyWaitValue(ctx, waitArgs({ waitS: 5 }), {
          until: 'revision',
          baseline: { turn: 5, revision: 12, state_token: 'token_5_12' },
        }),
        server.fetch,
        seat.store
      ))
    );
    expect(wake.wake_reason).toBe('revision_changed');
    expect(wake.state_revision?.state_token).toBe('token_5_13');
    expect(kit.log.pages).toHaveLength(1);
    expect(kit.log.mirroredPages).toHaveLength(1);
    // Remembered before mirrored, exactly as the Python orders them.
    expect(kit.log.order).toEqual(['remember', 'mirror-page']);
    expect(server.urls.some((url) => url.includes('section=overview&limit=16'))).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// _local_wait_response
// ---------------------------------------------------------------------------

describe('localWaitResponse', () => {
  test('carries this seat identity and nothing the server did not say', () => {
    const seat = bench();
    const health = Effect.runSync(decodeHealth(healthPayload(), seat.session));
    expect(localWaitResponse(seat.session, 'timeout', health, null)).toEqual({
      schema_version: 2,
      control_protocol: 'full-control-v2',
      game_id: FIXTURE_GAME_ID,
      agent_id: FIXTURE_AGENT_ID,
      wake_reason: 'timeout',
      health,
      state_revision: null,
    });
  });
});

// ---------------------------------------------------------------------------
// _holder_remaining_s
// ---------------------------------------------------------------------------

describe('holderRemainingS', () => {
  const decodedHealth = (fixture: PvpHealthFixture): HealthEnvelope =>
    Effect.runSync(decodeHealth(pvpHealth(fixture), bench().session));

  test('names the deadline when exactly one other seat holds the phase', () => {
    expect(holderRemainingS(decodedHealth({ mine: false, remainingS: 40 }), holderSeat)).toBe(40);
  });

  test('an active phase has no holder to name', () => {
    expect(holderRemainingS(decodedHealth({ mine: true }), holderSeat)).toBeNull();
  });

  test('a seat waiting on itself is not a holder', () => {
    expect(
      holderRemainingS(decodedHealth({ mine: false, selfHeld: true }), holderSeat)
    ).toBeNull();
  });

  test('a spent deadline is zero and an absent one is unknown', () => {
    // A negative `remaining_s` never survives `_validate_health`, so the
    // `max(0, …)` floor is defensive; zero is the value a pinned holder sends.
    expect(holderRemainingS(decodedHealth({ mine: false, remainingS: 0 }), holderSeat)).toBe(0);
    expect(
      holderRemainingS(decodedHealth({ mine: false, remainingS: null }), holderSeat)
    ).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// _wait_until_turn, on a clock the test owns
// ---------------------------------------------------------------------------

interface Clocked {
  readonly fetch: typeof fetch;
  readonly clock: WaitClock;
  readonly blocked: number[];
  readonly now: { seconds: number };
}

/**
 * A fake `/wait` on a clock that only advances by what it blocked for.
 *
 * The Python's `PvPWaitInteropTests.clocked`, restated: the responder reads
 * `wait_s` out of the query, advances the clock by exactly that, and answers
 * from the caller's script at the new time.
 */
const clocked = (script: (elapsed: number) => JsonValue): Clocked => {
  const now = { seconds: 0 };
  const blocked: number[] = [];
  return {
    fetch: completeFetch((input) => {
      const url = new URL(urlOf(input));
      const waited = Number(url.searchParams.get('wait_s') ?? '0');
      blocked.push(waited);
      now.seconds += waited;
      return Promise.resolve(jsonResponse(script(now.seconds)));
    }),
    blocked,
    now,
    clock: {
      monotonic: () => Effect.sync(() => now.seconds),
      sleep: (seconds) =>
        Effect.sync(() => {
          now.seconds += seconds;
        }),
    },
  };
};

const clockedCtx = (kit: Kit, fake: Clocked, seat: Bench): WaitCtx =>
  waitCtx({
    sessionPath: seat.sessionPath,
    session: seat.session,
    hooks: kit.hooks,
    clock: fake.clock,
  });

describe('waitUntilTurn', () => {
  awaitTest('an interactive await keeps even the FIRST poll short', function* (wait) {
    // The 49-second silence: with echo defined, the first poll used to run
    // for the whole waitS budget, so no tick line ever printed before the
    // wake.  Interactive composites now poll at V2_WAIT_TICK_S from the
    // first request.
    const seat = bench();
    const kit = recorder();
    const fake = clocked((elapsed) =>
      pvpWake(elapsed >= 45 ? 'phase_active' : 'timeout', {
        mine: elapsed >= 45,
        remainingS: Math.max(0, 300 - elapsed),
        elapsedS: Math.min(600, 300 + elapsed),
      })
    );
    right(
      yield* wait(run(
        waitUntilTurn(clockedCtx(kit, fake, seat), waitArgs({ waitS: 120 }), {
          forTurn: false,
          echo: kit.echo,
        }),
        fake.fetch,
        seat.store
      ))
    );
    expect(fake.blocked[0]).toBeLessThanOrEqual(V2_WAIT_TICK_S);
    expect(kit.log.echoed.length).toBeGreaterThan(0);
  });

  awaitTest('a silent await still spends its budget on one long first poll', function* (wait) {
    const seat = bench();
    const kit = recorder();
    const fake = clocked(() => pvpWake('phase_active', { mine: true }));
    right(
      yield* wait(run(
        waitUntilTurn(clockedCtx(kit, fake, seat), waitArgs({ waitS: 120 }), {
          forTurn: false,
        }),
        fake.fetch,
        seat.store
      ))
    );
    expect(fake.blocked[0]).toBe(120);
  });

  awaitTest('is bounded by the holder deadline plus exactly one grace window', function* (wait) {
    const seat = bench();
    const kit = recorder();
    const fake = clocked((elapsed) =>
      pvpWake('timeout', {
        mine: false,
        remainingS: Math.max(0, 40 - elapsed),
        elapsedS: Math.min(600, 560 + elapsed),
      })
    );
    const wake = right(
      yield* wait(run(
        waitUntilTurn(clockedCtx(kit, fake, seat), waitArgs({}), {
          forTurn: true,
          echo: kit.echo,
        }),
        fake.fetch,
        seat.store
      ))
    );

    expect(wake.wake_reason).toBe('timeout');
    // Short internal polls, never one 120 s block.
    expect(fake.blocked.every((item) => item <= V2_WAIT_TICK_S)).toBe(true);
    // It waited out the 40 s deadline plus one 15 s grace, and stopped rather
    // than rolling the grace forward against a holder pinned at zero.
    expect(fake.now.seconds).toBeGreaterThanOrEqual(40);
    expect(fake.now.seconds).toBeLessThanOrEqual(40 + V2_FOR_TURN_GRACE_S);
    expect(kit.log.echoed).toHaveLength(fake.blocked.length - 1);
  });

  awaitTest('the marker is refreshed on every tick, before the transcript line', function* (wait) {
    const seat = bench();
    const kit = recorder();
    const fake = clocked((elapsed) =>
      pvpWake(elapsed >= 45 ? 'phase_active' : 'timeout', {
        mine: elapsed >= 45,
        remainingS: Math.max(0, 300 - elapsed),
        elapsedS: Math.min(600, 300 + elapsed),
      })
    );
    right(
      yield* wait(run(
        waitUntilTurn(clockedCtx(kit, fake, seat), waitArgs({}), {
          forTurn: true,
          echo: kit.echo,
        }),
        fake.fetch,
        seat.store
      ))
    );
    // One marker write per poll; one transcript line per poll but the last.
    expect(kit.log.mirroredHealth).toHaveLength(fake.blocked.length);
    expect(kit.log.echoed).toHaveLength(fake.blocked.length - 1);
    // Ordering: the watcher's file is fresh before the transcript says so.
    expect(kit.log.order).toEqual([
      'mirror-health',
      'echo',
      'mirror-health',
      'echo',
      'mirror-health',
    ]);
    // The marker sees a live deadline, not one frozen for the whole block.
    expect(kit.log.mirroredHealth.map((item) => item.phase?.timing.remaining_s)).toEqual([
      285, 270, 255,
    ]);
  });

  awaitTest('returns the moment the phase is genuinely ours', function* (wait) {
    const seat = bench();
    const kit = recorder();
    const fake = clocked((elapsed) =>
      pvpWake(elapsed >= 30 ? 'phase_active' : 'timeout', {
        mine: elapsed >= 30,
        remainingS: Math.max(0, 300 - elapsed),
      })
    );
    const wake = right(
      yield* wait(run(
        waitUntilTurn(clockedCtx(kit, fake, seat), waitArgs({}), { forTurn: true }),
        fake.fetch,
        seat.store
      ))
    );
    expect(wake.wake_reason).toBe('phase_active');
    expect(fake.now.seconds).toBe(30);
  });

  awaitTest('a cap is a hard ceiling over the holder deadline', function* (wait) {
    const seat = bench();
    const kit = recorder();
    const fake = clocked((elapsed) =>
      pvpWake('timeout', { mine: false, remainingS: Math.max(0, 600 - elapsed) })
    );
    const wake = right(
      yield* wait(run(
        waitUntilTurn(clockedCtx(kit, fake, seat), waitArgs({}), { forTurn: true, capS: 45 }),
        fake.fetch,
        seat.store
      ))
    );
    expect(wake.wake_reason).toBe('timeout');
    expect(fake.now.seconds).toBe(45);
  });

  awaitTest('without --for-turn the first poll is the caller own --wait-s', function* (wait) {
    const seat = bench();
    const kit = recorder();
    // A holder whose deadline has already run out: the loop continues only
    // while the server names a seat that still has time left.
    const fake = clocked((elapsed) =>
      pvpWake('timeout', { mine: false, remainingS: Math.max(0, 20 - elapsed) })
    );
    right(
      yield* wait(run(
        waitUntilTurn(clockedCtx(kit, fake, seat), waitArgs({ waitS: 30 }), { forTurn: false }),
        fake.fetch,
        seat.store
      ))
    );
    expect(fake.blocked[0]).toBe(30);
    expect(fake.blocked).toHaveLength(1);
  });

  awaitTest('without --for-turn and no holder named, one poll and out', function* (wait) {
    const seat = bench();
    const kit = recorder();
    // No `waiting_on`: the server named nobody, so a single-seat game behaves
    // exactly as it always did.
    const fake = clocked(() => waitPayload({ wake_reason: 'timeout', health: healthPayload() }));
    const wake = right(
      yield* wait(run(
        waitUntilTurn(clockedCtx(kit, fake, seat), waitArgs({}), { forTurn: false }),
        fake.fetch,
        seat.store
      ))
    );
    expect(wake.wake_reason).toBe('timeout');
    expect(fake.blocked).toHaveLength(1);
  });

  awaitTest('the mirror override replaces the default marker write', function* (wait) {
    const seat = bench();
    const kit = recorder();
    const own: HealthEnvelope[] = [];
    const fake = clocked(() => pvpWake('phase_active', { mine: true }));
    right(
      yield* wait(run(
        waitUntilTurn(clockedCtx(kit, fake, seat), waitArgs({}), {
          forTurn: true,
          mirror: (health) =>
            Effect.sync(() => {
              own.push(health);
            }),
        }),
        fake.fetch,
        seat.store
      ))
    );
    expect(own).toHaveLength(1);
    expect(kit.log.mirroredHealth).toHaveLength(0);
  });
});

// ---------------------------------------------------------------------------
// _wait_command_value
// ---------------------------------------------------------------------------

describe('waitCommandValue', () => {
  awaitTest('--max without --for-turn is refused rather than ignored', function* (wait) {
    const seat = bench();
    const kit = recorder();
    const server = scripted(() => ({ body: waitPayload() }));
    const ctx = waitCtx({ sessionPath: seat.sessionPath, session: seat.session, hooks: kit.hooks });
    expect(
      message(yield* wait(run(waitCommandValue(ctx, waitArgs({}), { maxS: 30 }), server.fetch, seat.store)))
    ).toBe('wait --max bounds --for-turn; pass both or neither');
    expect(server.urls).toHaveLength(0);
  });

  awaitTest('--max is bounded by the same ceiling the wait is', function* (wait) {
    const seat = bench();
    const kit = recorder();
    const server = scripted(() => ({ body: waitPayload() }));
    const ctx = waitCtx({ sessionPath: seat.sessionPath, session: seat.session, hooks: kit.hooks });
    expect(
      message(
        yield* wait(run(
          waitCommandValue(ctx, waitArgs({}), { forTurn: true, maxS: 616 }),
          server.fetch,
          seat.store
        ))
      )
    ).toBe('max must be in [0, 615]');
  });

  awaitTest('a plain wait makes exactly one request and never loops', function* (wait) {
    const seat = bench();
    const kit = recorder();
    const server = scripted(() => ({ body: pvpWake('timeout', { mine: false }) }));
    const ctx = waitCtx({ sessionPath: seat.sessionPath, session: seat.session, hooks: kit.hooks });
    right(yield* wait(run(waitCommandValue(ctx, waitArgs({})), server.fetch, seat.store)));
    expect(server.urls).toHaveLength(1);
    expect(server.urls[0]).toContain('wait_s=120');
    // No `--for-turn`, so nothing mirrors and nothing ticks.
    expect(kit.log.mirroredHealth).toHaveLength(0);
    expect(kit.log.echoed).toHaveLength(0);
  });
});

// ---------------------------------------------------------------------------
// _seat_rebound
// ---------------------------------------------------------------------------

const reboundOf = (seat: Bench): Promise<boolean> =>
  Effect.runPromise(provideTestLayer(seatRebound(seat.sessionPath), Layer.succeed(SessionStore, seat.store)));

describe('seatRebound', () => {
  awaitTest('is false when no current session can be resolved at all', function* (wait) {
    expect(yield* wait(reboundOf(bench()))).toBe(false);
  });

  awaitTest('is false while the workspace still points at this seat', function* (wait) {
    const seat = bench();
    Effect.runSync(seat.store.setCurrentSession(seat.sessionPath));
    expect(yield* wait(reboundOf(seat))).toBe(false);
  });

  awaitTest('is true once `use` has pointed the workspace at another seat', function* (wait) {
    const seat = bench();
    const other = path.join(seat.scratch.workspace.stateRoot, 'session-other-harness.json');
    Effect.runSync(
      seat.scratch.files.writeJson(other, sessionFile({ controller_label: 'other-harness' }))
    );
    Effect.runSync(seat.store.setCurrentSession(other));
    expect(yield* wait(reboundOf(seat))).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// The renderers
// ---------------------------------------------------------------------------

const decodedWake = (payload: JsonObject, session: Session): WaitEnvelope =>
  Effect.runSync(decodeWait(payload, session, { until: 'phase', afterStateToken: null }));

describe('the wait renderers', () => {
  test('a timeout names the holder instead of calling itself a wake', () => {
    const seat = bench();
    const wake = decodedWake(pvpWake('timeout', { mine: false }), seat.session);
    const line = awaitLine(wake);
    expect(line).toContain('still seat 2 AgentPlace2 (pi-gpt-5.6-sol)');
    expect(line).toContain('held 13s');
    expect(line).toContain('9m47s left');
    // The old tail pointed at a command that can only be refused.
    expect(line).not.toContain('next: just turn');
    expect(line).toContain('just wait --for-turn');
    expect(line).toContain('[exit 75]');
  });

  test('an actionable wake keeps the plain briefing tail', () => {
    const seat = bench();
    const wake = decodedWake(pvpWake('phase_active', { mine: true }), seat.session);
    const line = awaitLine(wake);
    expect(line.startsWith('T3 | ')).toBe(true);
    expect(line).toContain('next: just turn');
    expect(line).not.toContain('woke phase_active');
  });

  test('a non-phase_active wake on our own phase says which wake it was', () => {
    const seat = bench();
    const wake = decodedWake(pvpWake('boundary_recovered', { mine: true }), seat.session);
    expect(awaitLine(wake)).toContain('woke boundary_recovered');
  });

  test('an empty follow drops the tail entirely, on both branches', () => {
    const seat = bench();
    const mine = decodedWake(pvpWake('phase_active', { mine: true }), seat.session);
    const theirs = decodedWake(pvpWake('timeout', { mine: false }), seat.session);
    expect(awaitLine(mine, '')).not.toContain('next:');
    expect(awaitLine(theirs, '')).not.toContain('next:');
  });

  test('the tick line names the seat, how long it has held and what is left', () => {
    const seat = bench();
    const wake = decodedWake(pvpWake('timeout', { mine: false }), seat.session);
    const line = waitingTickLine(wake.health);
    expect(line.startsWith('… waiting on ')).toBe(true);
    expect(line).toContain('seat 2 AgentPlace2 (pi-gpt-5.6-sol)');
    expect(line).toContain('held 13s');
    expect(line).toContain('9m47s left');
  });

  test('with nobody named, the tick line says only that the phase is shut', () => {
    const seat = bench();
    const wake = decodedWake(pvpWake('timeout', { mine: true }), seat.session);
    expect(waitingTickLine(wake.health)).toBe("… waiting for this seat's phase to open");
  });

  test('renderWait is the await header over the health block', () => {
    const seat = bench();
    const wake = decodedWake(pvpWake('phase_active', { mine: true }), seat.session);
    expect(renderWait(wake)).toEqual([
      awaitLine(wake),
      ...renderHealth(wake.health),
    ]);
  });
});
