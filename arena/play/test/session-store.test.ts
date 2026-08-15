/**
 * Sessions, the seat binding and the `.v2-state` cache.
 *
 * "One workspace plays one seat" is the whole design, so the resolution order
 * (explicit → `PLAY_SESSION` → binding → sole session → pointer) and the refusal
 * to guess between two unbound seats are the tests that matter.
 */
import { afterEach, describe, expect, test } from 'bun:test';
import { Clock, Effect, Either, Option, Schema } from 'effect';
import {
  controllerName,
  emptyV2ClientState,
  gameId,
  sessionKey,
  sessionStoreFor,
  type SessionStoreApi,
} from 'src/services/session-store';
import type { JsonValue } from 'src/schema/primitives';
import { FIXTURE_GAME_ID, scratchWorkspace, sessionFile, type Scratch } from 'test/_fixtures';
import { effectTest } from 'test/_effect-test';
import { path, withTestFileSystem } from 'test/_test-platform';

const bindingSchema = Schema.parseJson(
  Schema.Struct({
    schema_version: Schema.Literal(1),
    game_id: Schema.String,
    session: Schema.String,
  })
);
const configSchema = Schema.parseJson(Schema.Struct({ game_id: Schema.String }));

const scratches: Scratch[] = [];

afterEach(() =>
  Effect.runPromise(
    Effect.asVoid(Effect.all(scratches.splice(0).map((scratch) => scratch.cleanup)))
  )
);

/**
 * The core placeholder for the U03 seam.  When U03 lands its real validators
 * these tests should be re-pointed at that layer, and the alias-table proofs
 * belong there rather than here.
 */
const schema = {
  empty: emptyV2ClientState,
  validate: () => Effect.void,
  cursorExpired: (expiresAt: string | null): boolean =>
    expiresAt === null
      ? false
      : Date.parse(expiresAt) <= Effect.runSync(Clock.currentTimeMillis),
};

interface Fixture {
  readonly scratch: Scratch;
  readonly store: SessionStoreApi;
  readonly write: (relative: string, value: JsonValue) => Effect.Effect<string>;
}

const fresh = (
  environment: Record<string, string | undefined> = {}
): Effect.Effect<Fixture> =>
  Effect.gen(function* () {
    const scratch = yield* scratchWorkspace();
    scratches.push(scratch);
    const store = sessionStoreFor(scratch.workspace, scratch.files, schema, environment);
    return {
      scratch,
      store,
      write: (relative, value) => {
        const target = path.join(scratch.workspace.stateRoot, relative);
        return Effect.as(scratch.files.writeJson(target, value), target).pipe(Effect.orDie);
      },
    };
  });

const run = <A, Err>(effect: Effect.Effect<A, Err>): Either.Either<A, Err> =>
  Effect.runSync(Effect.either(effect));

const message = <A>(either: Either.Either<A, { readonly message: string }>): string => {
  expect(Either.isLeft(either)).toBe(true);
  return Either.isLeft(either) ? either.left.message : '';
};

const right = <A, Err>(either: Either.Either<A, Err>): A => {
  expect(Either.isRight(either)).toBe(true);
  if (Either.isLeft(either)) throw new Error('expected success');
  return either.right;
};

describe('name validation', () => {
  test('a game ID must carry the assigned shape', () => {
    expect(right(run(gameId(FIXTURE_GAME_ID)))).toBe(FIXTURE_GAME_ID);
    expect(message(run(gameId('game_short')))).toBe('a valid assigned game ID is required');
  });

  test('a controller must be a truthful, non-generic harness-model label', () => {
    expect(right(run(controllerName('codex-gpt-5.6-sol')))).toBe('codex-gpt-5.6-sol');
    for (const bad of ['agent', 'harness-model', 'nodash', '-leading', 'trailing-']) {
      expect(message(run(controllerName(bad)))).toContain('truthful non-generic');
    }
  });

  test('the session key is a stable slug plus a digest of the exact label', () => {
    const key = sessionKey('codex-gpt-5.6-sol');
    expect(key).toMatch(/^codex-gpt-5-6-sol-[0-9a-f]{12}$/);
    expect(sessionKey('codex-gpt-5.6-sol')).toBe(key);
    expect(sessionKey('codex-gpt-5.6-SOL')).not.toBe(key);
  });
});

describe('session resolution', () => {
  effectTest('a sole private session needs no --session', () =>
    Effect.gen(function* () {
      const fixture = yield* fresh();
      const target = yield* fixture.write(`${FIXTURE_GAME_ID}/seat.json`, sessionFile());
      expect(yield* fixture.store.sessionPath('')).toBe(target);
    }).pipe(Effect.orDie)
  );

  effectTest('two unbound sessions are refused rather than guessed between', () =>
    Effect.gen(function* () {
      const fixture = yield* fresh();
      yield* fixture.write(`${FIXTURE_GAME_ID}/one.json`, sessionFile());
      yield* fixture.write(`${FIXTURE_GAME_ID}/two.json`, sessionFile());
      expect(message(yield* Effect.either(fixture.store.sessionPath('')))).toContain(
        'multiple private sessions exist'
      );
    })
  );

  effectTest('a bound seat wins over the count', () =>
    Effect.gen(function* () {
      const fixture = yield* fresh();
      yield* fixture.write(`${FIXTURE_GAME_ID}/one.json`, sessionFile());
      const two = yield* fixture.write(`${FIXTURE_GAME_ID}/two.json`, sessionFile());
      yield* fixture.store.bindWorkspaceSeat(two, FIXTURE_GAME_ID);
      expect(yield* fixture.store.sessionPath('')).toBe(two);
    }).pipe(Effect.orDie)
  );

  effectTest('a binding whose seat file is gone is stale, not authoritative', () =>
    withTestFileSystem((files) =>
      Effect.gen(function* () {
        const fixture = yield* fresh();
        const one = yield* fixture.write(`${FIXTURE_GAME_ID}/one.json`, sessionFile());
        const two = yield* fixture.write(`${FIXTURE_GAME_ID}/two.json`, sessionFile());
        yield* fixture.store.bindWorkspaceSeat(two, FIXTURE_GAME_ID);
        yield* files.remove(two);
        expect(yield* fixture.store.sessionPath('')).toBe(one);
      }).pipe(Effect.orDie)
    )
  );

  effectTest('PLAY_SESSION is honoured when no --session is given', () =>
    Effect.gen(function* () {
      // A relative PLAY_SESSION is workspace-relative, not state-relative — the
      // Python joins it onto `ROOT`, so it has to name `.sessions/` itself.
      const fixture = yield* fresh({ PLAY_SESSION: `.sessions/${FIXTURE_GAME_ID}/one.json` });
      const one = yield* fixture.write(`${FIXTURE_GAME_ID}/one.json`, sessionFile());
      yield* fixture.write(`${FIXTURE_GAME_ID}/two.json`, sessionFile());
      expect(yield* fixture.store.sessionPath('')).toBe(one);
    }).pipe(Effect.orDie)
  );

  effectTest('a PLAY_SESSION outside PLAY_STATE_DIR is refused, not followed', () =>
    Effect.gen(function* () {
      const fixture = yield* fresh({ PLAY_SESSION: `${FIXTURE_GAME_ID}/one.json` });
      yield* fixture.write(`${FIXTURE_GAME_ID}/one.json`, sessionFile());
      expect(message(yield* Effect.either(fixture.store.sessionPath('')))).toBe(
        'private state files must stay inside PLAY_STATE_DIR'
      );
    })
  );

  effectTest('with nothing at all, the remedy names `just join`', () =>
    Effect.gen(function* () {
      const fixture = yield* fresh();
      expect(message(yield* Effect.either(fixture.store.sessionPath('')))).toBe(
        'no current session; run `just join --game_id ... --name ...` first'
      );
    })
  );

  effectTest('a pre-configured workspace gets the argument-free remedy instead', () =>
    withTestFileSystem((files) =>
      Effect.gen(function* () {
        const fixture = yield* fresh();
        const config = yield* Schema.encode(configSchema)({ game_id: FIXTURE_GAME_ID });
        yield* files.writeFileString(
          path.join(fixture.scratch.workspace.root, '.playconfig.json'),
          config
        );
        expect(message(yield* Effect.either(fixture.store.sessionPath('')))).toBe(
          'run `just join` first — this workspace is pre-configured for ' +
            `${FIXTURE_GAME_ID}, and every other command needs the seat it creates`
        );
      }).pipe(Effect.orDie)
    )
  );
});

describe('seat binding', () => {
  effectTest('binding writes a pointer, never a credential', () =>
    Effect.gen(function* () {
      const fixture = yield* fresh();
      const target = yield* fixture.write(`${FIXTURE_GAME_ID}/seat.json`, sessionFile());
      yield* fixture.store.bindWorkspaceSeat(target, FIXTURE_GAME_ID);
      const raw = yield* fixture.scratch.files.readText(
        fixture.store.seatBindingPath,
        'seat binding'
      );
      expect(raw).not.toContain('secret-token');
      expect(yield* Schema.decode(bindingSchema)(raw)).toEqual({
        schema_version: 1,
        game_id: FIXTURE_GAME_ID,
        session: path.join(FIXTURE_GAME_ID, 'seat.json'),
      });
    }).pipe(Effect.orDie)
  );

  effectTest('re-binding the same seat reports no replacement', () =>
    Effect.gen(function* () {
      const fixture = yield* fresh();
      const target = yield* fixture.write(`${FIXTURE_GAME_ID}/seat.json`, sessionFile());
      yield* fixture.store.bindWorkspaceSeat(target, FIXTURE_GAME_ID);
      const replaced = yield* fixture.store.bindWorkspaceSeat(target, FIXTURE_GAME_ID);
      expect(Option.isNone(replaced)).toBe(true);
    }).pipe(Effect.orDie)
  );

  effectTest('re-binding a different seat reports the one it replaced', () =>
    Effect.gen(function* () {
      const fixture = yield* fresh();
      const one = yield* fixture.write(`${FIXTURE_GAME_ID}/one.json`, sessionFile());
      const two = yield* fixture.write(`${FIXTURE_GAME_ID}/two.json`, sessionFile());
      yield* fixture.store.bindWorkspaceSeat(one, FIXTURE_GAME_ID);
      const replaced = yield* fixture.store.bindWorkspaceSeat(two, FIXTURE_GAME_ID);
      expect(Option.isSome(replaced)).toBe(true);
      if (Option.isSome(replaced)) {
        expect(replaced.value.relative).toBe(path.join(FIXTURE_GAME_ID, 'one.json'));
      }
    }).pipe(Effect.orDie)
  );

  effectTest('an unreadable binding names the repair, not the parser', () =>
    Effect.gen(function* () {
      const fixture = yield* fresh();
      yield* fixture.scratch.files.writeJson(fixture.store.seatBindingPath, {
        game_id: 'nope',
      });
      expect(message(yield* Effect.either(fixture.store.readSeatBinding()))).toContain(
        'just use GAME_ID'
      );
    })
  );
});

describe('v2 sessions', () => {
  effectTest('a strategic-v1 session is refused by a v2 command', () =>
    Effect.gen(function* () {
      const fixture = yield* fresh();
      yield* fixture.write(
        `${FIXTURE_GAME_ID}/seat.json`,
        sessionFile({ control_protocol: 'strategic-v1' })
      );
      expect(message(yield* Effect.either(fixture.store.resolveV2('')))).toBe(
        'this command is full-control-v2 only'
      );
    })
  );

  effectTest('a v2 session normalizes its own service URL on every load', () =>
    Effect.gen(function* () {
      const fixture = yield* fresh();
      yield* fixture.write(
        `${FIXTURE_GAME_ID}/seat.json`,
        sessionFile({ service_url: 'HTTP://127.0.0.1:8765/' })
      );
      const loaded = yield* fixture.store.resolveV2('');
      expect(loaded.session.serviceUrl).toBe('http://127.0.0.1:8765');
    }).pipe(Effect.orDie)
  );

  effectTest('a session smuggling credentials into the URL is refused', () =>
    Effect.gen(function* () {
      const fixture = yield* fresh();
      yield* fixture.write(
        `${FIXTURE_GAME_ID}/seat.json`,
        sessionFile({ service_url: 'http://user:pass@127.0.0.1:8765' })
      );
      expect(message(yield* Effect.either(fixture.store.resolveV2('')))).toContain(
        'without credentials'
      );
    })
  );

  effectTest('a missing controller label is an incomplete v2 session', () =>
    Effect.gen(function* () {
      const fixture = yield* fresh();
      yield* fixture.write(
        `${FIXTURE_GAME_ID}/seat.json`,
        sessionFile({ controller_label: '' })
      );
      expect(message(yield* Effect.either(fixture.store.resolveV2('')))).toBe(
        'the private full-control-v2 session is incomplete'
      );
    })
  );
});

describe('.v2-state', () => {
  effectTest('a missing cache reads as the empty schema-5 shape', () =>
    Effect.gen(function* () {
      const fixture = yield* fresh();
      const target = yield* fixture.write(`${FIXTURE_GAME_ID}/seat.json`, sessionFile());
      const loaded = yield* fixture.store.resolveV2('');
      const state = yield* fixture.store.readState(target, loaded.session);
      expect(state.schema_version).toBe(5);
      expect(state.drained_actors).toEqual([]);
      expect(state.action_aliases).toEqual({ state_revision: null, by_alias: {} });
    }).pipe(Effect.orDie)
  );

  effectTest('a written cache round-trips', () =>
    Effect.gen(function* () {
      const fixture = yield* fresh();
      const target = yield* fixture.write(`${FIXTURE_GAME_ID}/seat.json`, sessionFile());
      const loaded = yield* fixture.store.resolveV2('');
      const next = {
        ...emptyV2ClientState(loaded.session),
        batches: { batch_one: '{"a":1}' },
      };
      yield* fixture.store.writeState(target, next);
      const read = yield* fixture.store.readState(target, loaded.session);
      expect(read.batches).toEqual({ batch_one: '{"a":1}' });
    }).pipe(Effect.orDie)
  );

  effectTest('a schema-1 cache is migrated and every executable action is dropped', () =>
    Effect.gen(function* () {
      const fixture = yield* fresh();
      const target = yield* fixture.write(`${FIXTURE_GAME_ID}/seat.json`, sessionFile());
      const loaded = yield* fixture.store.resolveV2('');
      yield* fixture.scratch.files.writeJson(fixture.store.statePath(target), {
        schema_version: 1,
        game_id: FIXTURE_GAME_ID,
        agent_id: loaded.session.agentId,
        last_revision: null,
        actions: { action_old: { anything: true } },
        batches: { batch_one: '{"a":1}' },
        receipts: { batch_one: { kept: true } },
      });
      const state = yield* fixture.store.readState(target, loaded.session);
      expect(state.schema_version).toBe(5);
      expect(state.actions).toEqual({});
      expect(state.batches).toEqual({ batch_one: '{"a":1}' });
      expect(state.receipts).toEqual({ batch_one: { kept: true } });
    }).pipe(Effect.orDie)
  );

  effectTest('a persisted batch body must be the exact bytes, not a re-parsed object', () =>
    Effect.gen(function* () {
      const fixture = yield* fresh();
      const target = yield* fixture.write(`${FIXTURE_GAME_ID}/seat.json`, sessionFile());
      const loaded = yield* fixture.store.resolveV2('');
      yield* fixture.scratch.files.writeJson(fixture.store.statePath(target), {
        ...emptyV2ClientState(loaded.session),
        batches: { batch_one: { a: 1 } },
      });
      const either = yield* Effect.either(fixture.store.readState(target, loaded.session));
      expect(message(either)).toContain('is invalid');
    }).pipe(Effect.orDie)
  );

  effectTest('a cache belonging to another agent is refused', () =>
    Effect.gen(function* () {
      const fixture = yield* fresh();
      const target = yield* fixture.write(`${FIXTURE_GAME_ID}/seat.json`, sessionFile());
      const loaded = yield* fixture.store.resolveV2('');
      yield* fixture.scratch.files.writeJson(fixture.store.statePath(target), {
        ...emptyV2ClientState(loaded.session),
        agent_id: 'agent_someoneelse',
      });
      const either = yield* Effect.either(fixture.store.readState(target, loaded.session));
      expect(message(either)).toContain('is invalid');
    }).pipe(Effect.orDie)
  );
});
