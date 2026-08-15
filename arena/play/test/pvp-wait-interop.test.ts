/**
 * The multiplayer wait surface, end to end.
 *
 * Ports `PvPWaitInteropTests` from `play/tests/test_client.py:10959-11407`,
 * minus the cases that belong to other units (the phase-marker file is U04's,
 * the health one-liners and `prior_end` are U06's).
 *
 * Every case reproduces something a live two-agent match actually did: a wake
 * reason the client could not parse, an exit status that said "success" for
 * "still not your turn", a briefing printed for a phase the caller did not
 * hold, and a marker file frozen for the whole of somebody else's ten minutes.
 */
import { Command } from '@effect/cli';
import { BunContext } from '@effect/platform-bun';
import { afterEach, describe, expect } from 'bun:test';
import { Effect, Either, Layer, Schema } from 'effect';
import { V2_SATISFIED_WAKE_REASONS, V2_WAKE_REASONS } from 'src/constants';
import { V2_WAIT_EXIT_ACTIVE, V2_WAIT_EXIT_RETRY, V2_WAIT_EXIT_TERMINAL } from 'src/exit';
import { decodeHealth, type HealthEnvelope } from 'src/schema/health';
import type { JsonObject, JsonValue } from 'src/schema/primitives';
import { decodeWait, type WaitEnvelope } from 'src/schema/wait';
import { liveWaitHooks, waitCommandWith, type WaitHooksFor } from 'src/commands/wait.cmd';
import { V2_PROTOCOL_CARD } from 'src/render/join';
import { renderWait } from 'src/render/wait';
import { httpFor } from 'src/services/http';
import { DEFAULT_COMMAND_CARD, mirrorDir } from 'src/services/mirror';
import { type PrivateFsApi } from 'src/services/private-fs';
import { pyJsonDumps } from 'src/services/json-output';
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
  waitArgs,
  waitCommandValue,
  waitCtx,
  waitExitCode,
  type HolderSeatFn,
  type WaitClock,
  type WaitHooks,
} from 'src/services/wait';
import { captureEffect } from 'test/_capture';
import { awaitTest, provideTestLayer } from 'test/_effect-test';
import { fixtureBoolean, fixtureNumber, fixtureObject, leftValue, rightValue } from 'test/_expect';
import {
  healthPayload,
  jsonResponse,
  pagePayload,
  scratchWorkspace,
  sessionFile,
  waitPayload,
  type Scratch,
} from 'test/_fixtures';
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

interface AnsweringServer {
  readonly fetch: typeof fetch;
  readonly urls: ReadonlyArray<string>;
}

interface LegacyRevisionServer {
  readonly fetch: typeof fetch;
  readonly urls: ReadonlyArray<string>;
}

const OpenApiWakeReasons = Schema.Struct({
  components: Schema.Struct({
    schemas: Schema.Struct({
      WaitEnvelope: Schema.Struct({
        properties: Schema.Struct({
          wake_reason: Schema.Struct({
            enum: Schema.Array(Schema.String),
          }),
        }),
      }),
    }),
  }),
});

const OPENAPI_PATH = path.resolve(
  import.meta.dir,
  '..',
  '..',
  'archive',
  'play',
  'docs',
  'full-control-v2.openapi.json'
);

const scratches: Scratch[] = [];

afterEach(() =>
  Effect.runPromise(
    Effect.asVoid(Effect.all(scratches.splice(0).map((scratch) => scratch.cleanup)))
  )
);

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
  readonly files: PrivateFsApi;
}

const bench = (): Effect.Effect<Bench> =>
  Effect.gen(function* () {
    const scratch = yield* scratchWorkspace();
    scratches.push(scratch);
    const sessionPath = path.join(
      scratch.workspace.stateRoot,
      'session-codex-gpt-5.6-sol.json'
    );
    yield* scratch.files.writeJson(sessionPath, sessionFile());
    const store = sessionStoreFor(scratch.workspace, scratch.files, stateSchema, {});
    const loaded = yield* store.resolveV2(sessionPath);
    return { scratch, sessionPath, session: loaded.session, store, files: scratch.files };
  }).pipe(Effect.orDie);

/** `_holder_seat`; U06 owns the real one, the engine takes it as a hook. */
const holderSeat: HolderSeatFn = (phase) => {
  if (phase === null || phase.active) return null;
  const waitingOn = phase.waiting_on;
  if (waitingOn === undefined || waitingOn === null) return null;
  const others = waitingOn.seats.filter((row) => row.is_self === false);
  return others.length === 1 ? (others[0] ?? null) : null;
};

interface Kit {
  readonly hooks: WaitHooks;
  readonly mirrored: HealthEnvelope[];
  readonly ticks: HealthEnvelope[];
  readonly echo: (health: HealthEnvelope) => Effect.Effect<void>;
}

const recorder = (): Kit => {
  const mirrored: HealthEnvelope[] = [];
  const ticks: HealthEnvelope[] = [];
  return {
    mirrored,
    ticks,
    echo: (health) =>
      Effect.sync(() => {
        ticks.push(health);
      }),
    hooks: {
      rememberPage: () => Effect.void,
      mirrorPage: () => Effect.void,
      mirrorHealth: (health) =>
        Effect.sync(() => {
          mirrored.push(health);
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
): Effect.Effect<Either.Either<A, E>> =>
  Effect.either(provideTestLayer(effect, layers(fetchImpl, store)));

const right = <A, E>(either: Either.Either<A, E>): A => rightValue(either);

const message = <A>(either: Either.Either<A, { readonly message: string }>): string =>
  leftValue(either).message;

const answering = (body: JsonValue): AnsweringServer => {
  const urls: string[] = [];
  return {
    urls,
    fetch: completeFetch((input) => {
      urls.push(urlOf(input));
      return Promise.resolve(jsonResponse(body));
    }),
  };
};

// ---------------------------------------------------------------------------
// The PvP payloads
// ---------------------------------------------------------------------------

const OPPONENT: JsonObject = {
  place: 2,
  seat_id: 'place-2',
  player_name: 'AgentPlace2',
  controller_label: 'pi-gpt-5.6-sol',
  standing: 'active',
  is_self: false,
};

interface PvpWaitFixture {
  readonly mine: boolean;
  readonly remainingS?: number;
  readonly elapsedS?: number;
  readonly gameState?: string;
}

const pvpPhaseBody = (
  fixture: PvpWaitFixture,
  elapsedS: number,
  remainingS: number
): JsonObject => {
  const timing: JsonObject = {
    mode: 'default',
    timeout_s: 600,
    deadline_started_at: 1000,
    deadline_at: 1600,
    elapsed_s: elapsedS,
    remaining_s: remainingS,
  };
  if (fixture.mine) {
    return {
      state: 'awaiting_agent',
      turn: 3,
      phase: 1,
      active: true,
      timing,
    };
  }
  return {
    state: 'awaiting_agent',
    turn: 3,
    phase: 1,
    active: false,
    timing,
    waiting_on: {
      kind: 'other_seat',
      summary:
        'Seat 2 AgentPlace2 (pi-gpt-5.6-sol) holds turn 3 phase 1 and has not ended it.',
      waiting_s: elapsedS,
      seats: [OPPONENT],
    },
  };
};

const pvpHealth = (fixture: PvpWaitFixture): JsonObject => {
  const elapsedS = fixture.elapsedS ?? 13;
  const remainingS = fixture.remainingS ?? 587;
  const terminal = fixture.gameState !== undefined && fixture.gameState !== 'running';
  if (terminal) {
    return healthPayload({
      game_state: fixture.gameState ?? 'completed',
      phase: null,
      observation_available: false,
      legal_actions_available: false,
    });
  }
  return healthPayload({
    game_state: 'running',
    phase: pvpPhaseBody(fixture, elapsedS, remainingS),
  });
};

const pvpWake = (reason: string, fixture: PvpWaitFixture): JsonObject =>
  waitPayload({ wake_reason: reason, health: pvpHealth(fixture) });

const decodedWake = (payload: JsonObject, session: Session): Effect.Effect<WaitEnvelope> =>
  decodeWait(payload, session, { until: 'phase', afterStateToken: null }).pipe(Effect.orDie);

const loadOpenApiWakeReasons = (): Effect.Effect<ReadonlyArray<string>> =>
  Effect.gen(function* () {
    const contractText = yield* fileSystem.readFileString(OPENAPI_PATH);
    const parsed = yield* Schema.decodeUnknown(Schema.parseJson(Schema.Unknown))(contractText);
    const contract = yield* Schema.decodeUnknown(OpenApiWakeReasons)(parsed);
    return contract.components.schemas.WaitEnvelope.properties.wake_reason.enum;
  }).pipe(Effect.orDie);

// ---------------------------------------------------------------------------
// P0a: the wake reason the server could always send
// ---------------------------------------------------------------------------

describe('boundary_recovered', () => {
  awaitTest('is a wake reason the client accepts, and a satisfied one', function* () {
    // It arrives when this seat's native boundary was republished under a wait
    // — and on the `--end --await` path it surfaced as `await failed:` *after*
    // the phase end had applied, which is the one moment a client must not be
    // telling the agent it does not understand the server.
    expect(V2_WAKE_REASONS.has('boundary_recovered')).toBe(true);
    const seat = yield* bench();
    const wake = yield* decodedWake(pvpWake('boundary_recovered', { mine: true }), seat.session);
    expect(wake.wake_reason).toBe('boundary_recovered');
    expect(waitExitCode(wake)).toBe(V2_WAIT_EXIT_ACTIVE);
    expect(V2_SATISFIED_WAKE_REASONS.has('boundary_recovered')).toBe(true);
  });

  awaitTest('the served OpenAPI lists every wake reason the client takes', function* () {
    const enumerated = yield* loadOpenApiWakeReasons();
    expect([...enumerated].toSorted((a, b) => a.localeCompare(b))).toEqual(
      [...V2_WAKE_REASONS].toSorted((a, b) => a.localeCompare(b))
    );
  });
});

// ---------------------------------------------------------------------------
// P1: the exit status carries the wake reason
// ---------------------------------------------------------------------------

describe('the exit status', () => {
  const CASES = [
    ['phase_active', { mine: true }, V2_WAIT_EXIT_ACTIVE],
    ['boundary_recovered', { mine: true }, V2_WAIT_EXIT_ACTIVE],
    ['timeout', { mine: false }, V2_WAIT_EXIT_RETRY],
    ['game_terminal', { mine: false, gameState: 'completed' }, V2_WAIT_EXIT_TERMINAL],
  ] as const;

  for (const [reason, fixture, code] of CASES) {
    awaitTest(`a real ${reason} wake exits ${code}`, function* (wait) {
      const seat = yield* bench();
      const kit = recorder();
      const server = answering(pvpWake(reason, fixture));
      const ctx = waitCtx({
        sessionPath: seat.sessionPath,
        session: seat.session,
        hooks: kit.hooks,
      });
      const wake = right(yield* wait(run(waitCommandValue(ctx, waitArgs({})), server.fetch, seat.store)));
      expect(waitExitCode(wake)).toBe(code);
    });
  }

  awaitTest('a lobby timeout is EX_TEMPFAIL, not success, and calls no state route', function* (wait) {
    const seat = yield* bench();
    const kit = recorder();
    const server = answering(
      waitPayload({
        wake_reason: 'timeout',
        health: healthPayload({
          game_state: 'lobby',
          phase: {
            state: 'awaiting_agent',
            turn: 0,
            phase: 0,
            active: false,
            timing: {
              mode: 'default',
              timeout_s: null,
              deadline_started_at: null,
              deadline_at: null,
              elapsed_s: null,
              remaining_s: null,
            },
          },
        }),
      })
    );
    const ctx = waitCtx({ sessionPath: seat.sessionPath, session: seat.session, hooks: kit.hooks });
    const wake = right(
      yield* wait(run(waitCommandValue(ctx, waitArgs({ waitS: 0 })), server.fetch, seat.store))
    );
    // A timeout means "still not yours, call me again", which is EX_TEMPFAIL
    // and not success — and the lobby never costs a `/state` round trip.
    expect(server.urls).toHaveLength(1);
    expect(server.urls[0]).toContain('/me/wait?');
    expect(server.urls.some((url) => url.includes('/state'))).toBe(false);
    expect(waitExitCode(wake)).toBe(V2_WAIT_EXIT_RETRY);
  });

  awaitTest('a terminal game stops the loop with EX_NOINPUT', function* (wait) {
    const seat = yield* bench();
    const kit = recorder();
    const server = answering(pvpWake('game_terminal', { mine: false, gameState: 'completed' }));
    const ctx = waitCtx({ sessionPath: seat.sessionPath, session: seat.session, hooks: kit.hooks });
    const wake = right(yield* wait(run(waitCommandValue(ctx, waitArgs({})), server.fetch, seat.store)));
    expect(waitExitCode(wake)).toBe(V2_WAIT_EXIT_TERMINAL);
  });

  awaitTest('the JSON payload is unchanged by the exit status', function* (wait) {
    const seat = yield* bench();
    const kit = recorder();
    const payload = pvpWake('timeout', { mine: false });
    const server = answering(payload);
    const ctx = waitCtx({ sessionPath: seat.sessionPath, session: seat.session, hooks: kit.hooks });
    const wake = right(yield* wait(run(waitCommandValue(ctx, waitArgs({})), server.fetch, seat.store)));
    expect(waitExitCode(wake)).toBe(V2_WAIT_EXIT_RETRY);
    // What `--json` prints is the *validated* envelope, and it must round-trip
    // to the wire payload byte for byte.
    expect(JSON.parse(pyJsonDumps(wake, { indent: 2, sortKeys: true }))).toEqual(payload);
  });
});

// ---------------------------------------------------------------------------
// P2: bounds and --for-turn
// ---------------------------------------------------------------------------

interface Clocked {
  readonly fetch: typeof fetch;
  readonly clock: WaitClock;
  readonly blocked: number[];
  readonly now: { seconds: number };
}

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

describe('the wait ceiling', () => {
  awaitTest('covers a whole opponent phase', function* (wait) {
    expect(V2_WAIT_S_MAX).toBe(615);
    const seat = yield* bench();
    const kit = recorder();
    const server = answering(pvpWake('phase_active', { mine: true }));
    const ctx = waitCtx({ sessionPath: seat.sessionPath, session: seat.session, hooks: kit.hooks });
    right(yield* wait(run(waitCommandValue(ctx, waitArgs({ waitS: 615 })), server.fetch, seat.store)));
    expect(server.urls[0]).toContain('wait_s=615');
    expect(
      message(yield* wait(run(waitCommandValue(ctx, waitArgs({ waitS: 616 })), server.fetch, seat.store)))
    ).toContain('[0, 615]');
  });
});

describe('--for-turn', () => {
  awaitTest('is bounded by the holder remaining deadline', function* (wait) {
    const seat = yield* bench();
    const kit = recorder();
    const fake = clocked((elapsed) =>
      pvpWake('timeout', {
        mine: false,
        remainingS: Math.max(0, 40 - elapsed),
        elapsedS: Math.min(600, 560 + elapsed),
      })
    );
    const ctx = waitCtx({
      sessionPath: seat.sessionPath,
      session: seat.session,
      hooks: kit.hooks,
      clock: fake.clock,
    });
    const wake = right(
      yield* wait(
        run(
          waitCommandValue(ctx, waitArgs({}), { forTurn: true, echo: kit.echo }),
          fake.fetch,
          seat.store
        )
      )
    );
    expect(waitExitCode(wake)).toBe(V2_WAIT_EXIT_RETRY);
    // Short internal polls, never one 120 s block.
    expect(fake.blocked.every((item) => item <= V2_WAIT_TICK_S)).toBe(true);
    expect(fake.now.seconds).toBeGreaterThanOrEqual(40);
    expect(fake.now.seconds).toBeLessThanOrEqual(40 + V2_FOR_TURN_GRACE_S);
    // Every tick said what it was waiting on.
    expect(kit.ticks).toHaveLength(fake.blocked.length - 1);
    expect(kit.ticks[0]?.phase?.waiting_on?.seats[0]?.player_name).toBe('AgentPlace2');
  });

  awaitTest('returns the moment the phase is ours', function* (wait) {
    const seat = yield* bench();
    const kit = recorder();
    const fake = clocked((elapsed) =>
      pvpWake(elapsed >= 30 ? 'phase_active' : 'timeout', {
        mine: elapsed >= 30,
        remainingS: Math.max(0, 300 - elapsed),
      })
    );
    const ctx = waitCtx({
      sessionPath: seat.sessionPath,
      session: seat.session,
      hooks: kit.hooks,
      clock: fake.clock,
    });
    const wake = right(
      yield* wait(run(waitCommandValue(ctx, waitArgs({}), { forTurn: true }), fake.fetch, seat.store))
    );
    expect(waitExitCode(wake)).toBe(V2_WAIT_EXIT_ACTIVE);
    expect(fake.now.seconds).toBe(30);
  });

  awaitTest('--max is a hard ceiling over the holder deadline', function* (wait) {
    const seat = yield* bench();
    const kit = recorder();
    const fake = clocked((elapsed) =>
      pvpWake('timeout', { mine: false, remainingS: Math.max(0, 600 - elapsed) })
    );
    const ctx = waitCtx({
      sessionPath: seat.sessionPath,
      session: seat.session,
      hooks: kit.hooks,
      clock: fake.clock,
    });
    const wake = right(
      yield* wait(
        run(
          waitCommandValue(ctx, waitArgs({}), { forTurn: true, maxS: 45 }),
          fake.fetch,
          seat.store
        )
      )
    );
    expect(waitExitCode(wake)).toBe(V2_WAIT_EXIT_RETRY);
    expect(fake.now.seconds).toBe(45);
  });

  awaitTest('--max without --for-turn is refused rather than ignored', function* (wait) {
    const seat = yield* bench();
    const kit = recorder();
    const server = answering(pvpWake('phase_active', { mine: true }));
    const ctx = waitCtx({ sessionPath: seat.sessionPath, session: seat.session, hooks: kit.hooks });
    expect(
      message(
        yield* wait(run(waitCommandValue(ctx, waitArgs({}), { maxS: 30 }), server.fetch, seat.store))
      )
    ).toContain('--for-turn');
  });

  awaitTest('a plain wait still makes exactly one request', function* (wait) {
    const seat = yield* bench();
    const kit = recorder();
    const server = answering(pvpWake('timeout', { mine: false }));
    const ctx = waitCtx({ sessionPath: seat.sessionPath, session: seat.session, hooks: kit.hooks });
    right(yield* wait(run(waitCommandValue(ctx, waitArgs({})), server.fetch, seat.store)));
    expect(server.urls).toHaveLength(1);
    expect(server.urls[0]).toContain('wait_s=120');
  });
});

// ---------------------------------------------------------------------------
// P3: the marker file, refreshed on every tick
// ---------------------------------------------------------------------------

describe('the phase marker', () => {
  awaitTest('is written on every tick of a wait, not once at the end', function* (wait) {
    const seat = yield* bench();
    const kit = recorder();
    const fake = clocked((elapsed) =>
      pvpWake(elapsed >= 45 ? 'phase_active' : 'timeout', {
        mine: elapsed >= 45,
        remainingS: Math.max(0, 300 - elapsed),
        elapsedS: Math.min(600, 300 + elapsed),
      })
    );
    const ctx = waitCtx({
      sessionPath: seat.sessionPath,
      session: seat.session,
      hooks: kit.hooks,
      clock: fake.clock,
    });
    right(
      yield* wait(
        run(
          waitCommandValue(ctx, waitArgs({}), { forTurn: true, echo: kit.echo }),
          fake.fetch,
          seat.store
        )
      )
    );
    // One write per request: refreshed between every pair of polls.
    expect(kit.mirrored).toHaveLength(fake.blocked.length);
    expect(kit.mirrored.map((item) => item.phase?.timing.remaining_s)).toEqual([285, 270, 255]);
    // The last write is the wake itself, and it is this seat's own phase.
    expect(kit.mirrored.at(-1)?.phase?.active).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// P4a: the strings
// ---------------------------------------------------------------------------

describe('the rendered wake', () => {
  awaitTest('a timeout wake names the holder instead of calling it a wake', function* () {
    const seat = yield* bench();
    const wake = yield* decodedWake(pvpWake('timeout', { mine: false }), seat.session);
    const lines = renderWait(wake);
    expect(lines[0]).toContain('still seat 2 AgentPlace2 (pi-gpt-5.6-sol)');
    expect(lines[0]).toContain('held 13s');
    expect(lines[0]).toContain('9m47s left');
    // The old tail pointed at a command that can only be refused.
    expect(lines[0]).not.toContain('next: just turn');
    expect(lines[0]).toContain('just wait --for-turn');
    expect(lines[0]).toContain(`[exit ${V2_WAIT_EXIT_RETRY}]`);
    expect(lines[1]).toContain('NOT YOUR TURN · seat 2 AgentPlace2');
  });

  awaitTest('text is the default and nothing raw leaks into it', function* () {
    const seat = yield* bench();
    const wake = yield* decodedWake(pvpWake('phase_active', { mine: true }), seat.session);
    const text = renderWait(wake).join('\n');
    expect(text.startsWith('{')).toBe(false);
    expect(text).toContain('YOUR TURN · t3/p1');
    expect(text).toContain('next: just turn');
    expect(renderWait(wake)[1]?.startsWith('health running')).toBe(true);
    expect(text).not.toContain('deadline_started_at');
  });
});

// ---------------------------------------------------------------------------
// `play wait`, end to end
// ---------------------------------------------------------------------------

/**
 * Run the real command over a fake supervisor and report status + stdout.
 *
 * `makeHooks` is the seam the command takes: the recorder for cases that only
 * care about the wire and the exit status, and `liveWaitHooks` for the cases
 * that must see what the command actually writes into the mirror.
 */
const runCommandWith = (
  seat: Bench,
  makeHooks: WaitHooksFor,
  fetchImpl: typeof fetch,
  flags: ReadonlyArray<string>
): Effect.Effect<{ readonly code: number; readonly out: ReadonlyArray<string> }> => {
  const command = waitCommandWith(makeHooks);
  return captureEffect(
    Effect.either(
      provideTestLayer(
        Command.run(command, { name: 'play', version: '0.1.0' })([
          'bun',
          'play',
          '--session',
          seat.sessionPath,
          ...flags,
        ]),
        Layer.mergeAll(layers(fetchImpl, seat.store), BunContext.layer, seat.scratch.layer)
      )
    )
  ).pipe(
    Effect.map(({ value, captured }) => {
      const out = captured.out;
      if (Either.isRight(value)) return { code: 0, out };
      const failure = value.left;
      return {
        code: failure._tag === 'ExitCodeSignal' ? failure.code : 2,
        out,
      };
    })
  );
};

/** Blocked once, then ours: exactly one internal tick, then the wake. */
const oneTickThenOurs = (): typeof fetch => {
  const calls = { count: 0 };
  return completeFetch(() => {
    calls.count += 1;
    return Promise.resolve(
      calls.count === 1
        ? jsonResponse(
            pvpWake('timeout', { mine: false, remainingS: 30 })
          )
        : jsonResponse(pvpWake('phase_active', { mine: true }))
    );
  });
};

/** The recorder seam: the wire and the exit status, no filesystem writes. */
const runWaitCommand = (
  seat: Bench,
  kit: Kit,
  fetchImpl: typeof fetch,
  flags: ReadonlyArray<string>
): Effect.Effect<{ readonly code: number; readonly out: ReadonlyArray<string> }> =>
  runCommandWith(seat, () => Effect.succeed(kit.hooks), fetchImpl, flags);

describe('play wait', () => {
  const CASES = [
    ['phase_active', { mine: true }, V2_WAIT_EXIT_ACTIVE],
    ['boundary_recovered', { mine: true }, V2_WAIT_EXIT_ACTIVE],
    ['timeout', { mine: false }, V2_WAIT_EXIT_RETRY],
    ['game_terminal', { mine: false, gameState: 'completed' }, V2_WAIT_EXIT_TERMINAL],
  ] as const;

  for (const [reason, fixture, code] of CASES) {
    awaitTest(`exits ${reason} → ${code} on a real wake`, function* (wait) {
      const seat = yield* bench();
      const kit = recorder();
      const result = yield* wait(runWaitCommand(seat, kit, answering(pvpWake(reason, fixture)).fetch, []));
      expect(result.code).toBe(code);
    });
  }

  awaitTest('prints compact text and keeps JSON behind the flag', function* (wait) {
    const seat = yield* bench();
    const payload = pvpWake('phase_active', { mine: true });

    const text = yield* wait(runWaitCommand(seat, recorder(), answering(payload).fetch, []));
    expect(text.code).toBe(V2_WAIT_EXIT_ACTIVE);
    expect(text.out[0]?.startsWith('{')).toBe(false);
    expect(text.out[0]).toContain('YOUR TURN · t3/p1');
    expect(text.out[0]).toContain('next: just turn');
    expect(text.out[1]?.startsWith('health running')).toBe(true);
    expect(text.out.join('\n')).not.toContain('deadline_started_at');

    const json = yield* wait(runWaitCommand(seat, recorder(), answering(payload).fetch, ['--json']));
    expect(json.code).toBe(V2_WAIT_EXIT_ACTIVE);
    expect(JSON.parse(json.out.join('\n'))).toEqual(payload);
  });

  awaitTest('the JSON payload is unchanged by the exit status', function* (wait) {
    const seat = yield* bench();
    const payload = pvpWake('timeout', { mine: false });
    const json = yield* wait(runWaitCommand(seat, recorder(), answering(payload).fetch, ['--json']));
    expect(json.code).toBe(V2_WAIT_EXIT_RETRY);
    expect(JSON.parse(json.out.join('\n'))).toEqual(payload);
  });

  awaitTest('a --for-turn tick is prose on stdout, and the wake follows it', function* (wait) {
    const seat = yield* bench();
    const result = yield* wait(runWaitCommand(seat, recorder(), oneTickThenOurs(), ['--for-turn']));
    expect(result.code).toBe(V2_WAIT_EXIT_ACTIVE);
    expect(result.out[0]).toContain('… waiting on seat 2 AgentPlace2 (pi-gpt-5.6-sol)');
    expect(result.out[1]).toContain('YOUR TURN · t3/p1');
  });

  awaitTest('--json prints one object and no tick prose', function* (wait) {
    const seat = yield* bench();
    const result = yield* wait(runWaitCommand(seat, recorder(), oneTickThenOurs(), ['--json', '--for-turn']));
    expect(result.code).toBe(V2_WAIT_EXIT_ACTIVE);
    expect(result.out.join('\n')).not.toContain('… waiting');
    expect(JSON.parse(result.out.join('\n')).wake_reason).toBe('phase_active');
  });

  awaitTest('both spellings of --wait-s are accepted, and never together', function* (wait) {
    const seat = yield* bench();
    const payload = pvpWake('phase_active', { mine: true });
    for (const spelling of ['--wait-s', '--wait_s']) {
      const server = answering(payload);
      const result = yield* wait(runWaitCommand(seat, recorder(), server.fetch, [spelling, '30']));
      expect(result.code).toBe(V2_WAIT_EXIT_ACTIVE);
      expect(server.urls[0]).toContain('wait_s=30');
    }
    const both = yield* wait(runWaitCommand(seat, recorder(), answering(payload).fetch, [
      '--wait-s',
      '30',
      '--wait_s',
      '30',
    ]));
    expect(both.code).toBe(2);
  });

  awaitTest('--max without --for-turn refuses instead of ignoring the flag', function* (wait) {
    const seat = yield* bench();
    const server = answering(pvpWake('phase_active', { mine: true }));
    const result = yield* wait(runWaitCommand(seat, recorder(), server.fetch, ['--max', '30']));
    expect(result.code).toBe(2);
    expect(server.urls).toHaveLength(0);
  });
});

// ---------------------------------------------------------------------------
// The live hook record
// ---------------------------------------------------------------------------

const headerText = (seat: Bench): Effect.Effect<string> =>
  Effect.gen(function* () {
    const dir = yield* mirrorDir(seat.sessionPath);
    return yield* fileSystem.readFileString(path.join(dir, 'state', 'header.txt'));
  }).pipe(Effect.orDie);

const OVERVIEW_PAGE: JsonObject = pagePayload([], {
  state_revision: { turn: 5, revision: 13, state_token: 'token_5_13' },
  page: {
    section: 'overview',
    items: [
      {
        client_state: 'running',
        turn: 5,
        map: { width: 64, height: 48 },
        player: {
          government: 'Despotism',
          economy: { gold: 50, tax: 40, luxury: 0, science: 60 },
        },
        counts: { cities: 1, units: 2, known_tiles: 40, legal_actions: 9, chat: 4 },
      },
    ],
    total_items: 1,
    next_cursor: null,
    cursor_expires_at: null,
  },
});

/**
 * `_legacy_wait_value` (client.py:9976-9977) runs `_remember_page` *and*
 * `_mirror_page(path, cached, overview, "wait")` on the overview page it
 * polled.  The wake is identical either way, which is exactly why an inert
 * `mirrorPage` was invisible: the divergence only shows up in the *next*
 * command, which reads a `state/*.tsv` still stamped at the old revision.
 *
 * The route this reaches is the pre-private-`/wait` supervisor's bare
 * `{"error": "..."}` 404 — the shape `isMissingRouteRefusal` detects.
 */
const legacyRevisionServer = (overview: JsonObject): LegacyRevisionServer => {
  const urls: string[] = [];
  return {
    urls,
    fetch: completeFetch((input) => {
      const url = urlOf(input);
      urls.push(url);
      if (url.includes('/me/wait?')) return Promise.resolve(jsonResponse({ error: 'Not Found' }, 404));
      if (url.includes('/me/health')) return Promise.resolve(jsonResponse(pvpHealth({ mine: false })));
      return Promise.resolve(jsonResponse(overview));
    }),
  };
};

/**
 * Everything above drives the command through the recorder seam, which proves
 * the engine and the exit contract but never touches `liveWaitHooks` — the
 * record the shipped binary actually runs.  These cases run the real hooks
 * against a real scratch mirror.
 *
 * The one that matters is `mirrorHealth`.  `_mirror_health`
 * (client.py:3068-3072) passes `commands=V2_PROTOCOL_CARD` unconditionally, and
 * `_wait_until_turn` calls it on every tick — so a `--for-turn` wait rewrites
 * `state/header.txt`, and `just show header` (client.py:11170 maps it to that
 * file) must still print the whole card afterwards.  Forwarding no options
 * silently downgrades the header to the 5-line `_DEFAULT_COMMAND_CARD`, which
 * costs the agent the ALIASES/ERRORS/ONE CALL PER TURN/MULTIPLAYER/WHICH
 * BINDING block it is told to read.  This is the TS half of
 * `test_v2_join_card_and_state_header_carry_the_same_contract`
 * (tests/test_client.py:7194-7254).
 */
describe('liveWaitHooks', () => {
  awaitTest('mirrorHealth writes the full protocol card into state/header.txt', function* () {
    const seat = yield* bench();
    const health = yield* decodeHealth(pvpHealth({ mine: true }), seat.session);
    const hooks = yield* provideTestLayer(
      liveWaitHooks(seat.sessionPath, seat.session),
      Layer.merge(seat.scratch.layer, Layer.succeed(SessionStore, seat.store))
    );
    yield* hooks.mirrorHealth(health, 'wait');

    const header = yield* headerText(seat);
    for (const line of V2_PROTOCOL_CARD) expect(header).toContain(line);
    // The default card is what a dropped option falls back to; none of its
    // lines belong in a header written by the client.
    for (const line of DEFAULT_COMMAND_CARD) expect(header).not.toContain(line);
    // And the secrets stay out, exactly as the Python asserts.
    expect(header).not.toContain(seat.session.agentToken);
    expect(header).not.toContain('state_token');
  });

  awaitTest('a --for-turn wait leaves a header a later `show header` can still read', function* (wait) {
    const seat = yield* bench();
    const result = yield* wait(runCommandWith(seat, liveWaitHooks, oneTickThenOurs(), ['--for-turn']));
    expect(result.code).toBe(V2_WAIT_EXIT_ACTIVE);
    // Two ticks wrote the header; the last write must still carry the card.
    const header = yield* headerText(seat);
    for (const line of V2_PROTOCOL_CARD) expect(header).toContain(line);
    expect(header).toContain('ONE CALL PER TURN');
    expect(header).toContain('MULTIPLAYER');
    expect(header).toContain('WHICH BINDING');
  });

  awaitTest('the marker file is refreshed on the way through, not only at the wake', function* (wait) {
    const seat = yield* bench();
    const result = yield* wait(runCommandWith(seat, liveWaitHooks, oneTickThenOurs(), ['--for-turn']));
    expect(result.code).toBe(V2_WAIT_EXIT_ACTIVE);
    const dir = yield* mirrorDir(seat.sessionPath);
    const markerText = yield* fileSystem.readFileString(path.join(dir, 'state', 'phase.json'));
    const marker = fixtureObject(JSON.parse(markerText));
    expect(fixtureNumber(marker['turn'])).toBe(3);
    expect(fixtureBoolean(marker['active'])).toBe(true);
  });

  awaitTest('the legacy --until revision fallback projects the page it woke on', function* (wait) {
    const seat = yield* bench();
    yield* seat.store.writeState(seat.sessionPath, {
      ...emptyV2ClientState(seat.session),
      last_revision: { turn: 5, revision: 12, state_token: 'token_5_12' },
    });
    const server = legacyRevisionServer(OVERVIEW_PAGE);
    const result = yield* wait(runCommandWith(seat, liveWaitHooks, server.fetch, ['--until', 'revision']));

    // `revision_changed` is a satisfied wake, so the status is 0 either way —
    // the mirror is the only place the missing projection was ever visible.
    expect(result.code).toBe(V2_WAIT_EXIT_ACTIVE);
    expect(server.urls.some((url) => url.includes('section=overview&limit=16'))).toBe(true);

    const dir = yield* mirrorDir(seat.sessionPath);
    const overview = yield* fileSystem.readFileString(path.join(dir, 'state', 'overview.tsv'));
    // Stamped at the revision the wake carried, not the baseline it started at.
    expect(overview.split('\n')[0]).toBe('# rev 13 turn 5');
    expect(overview).toContain('64x48');
    expect(overview).toContain('tax40 lux0 sci60');
    expect(overview).toContain('count_chat');
    // `state/delta.md` moves with it, exactly as `update_from_page` writes it.
    expect(yield* fileSystem.exists(path.join(dir, 'state', 'delta.md'))).toBe(true);
  });

  awaitTest('the projection uses the aliases this seat just learned from the page', function* (wait) {
    const seat = yield* bench();
    yield* seat.store.writeState(seat.sessionPath, {
      ...emptyV2ClientState(seat.session),
      last_revision: { turn: 5, revision: 12, state_token: 'token_5_12' },
    });
    const result = yield* wait(
      runCommandWith(
        seat,
        liveWaitHooks,
        legacyRevisionServer(OVERVIEW_PAGE).fetch,
        ['--until', 'revision']
      )
    );
    expect(result.code).toBe(V2_WAIT_EXIT_ACTIVE);
    // `_remember_page` runs before `_mirror_page` and CPython passes
    // `_alias_map(cached)` — the state the ingestion just folded the page into.
    // Reading it back must therefore see the wake's own revision.
    const state = yield* seat.store.readState(seat.sessionPath, seat.session);
    expect(state.last_revision?.state_token).toBe('token_5_13');
  });
});
