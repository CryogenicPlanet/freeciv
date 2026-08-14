/**
 * The mirror's two safety properties, ported first because everything else in
 * U04 depends on them: `AtomicityTests` and `LeakTests` from
 * `play/tests/test_state_mirror.py:1074-1173`.
 *
 * A crash mid-write must leave the prior file byte-identical and no temp file
 * behind, and nothing private — a bearer token, an invite, a `state_token` —
 * may ever reach a projection.
 */
import { afterEach, describe, expect } from 'bun:test';
import { Effect, Either } from 'effect';
import type { PrivateFs } from 'src/services/private-fs';
import {
  DELTA_FILE,
  HEADER_FILE,
  PHASE_FILE,
  mirrorDir,
  tableText,
  updateFromHealth,
  writeMirror,
} from 'src/services/mirror';
import { scratchWorkspace, type Scratch } from 'test/_fixtures';
import { effectTest, provideTestLayer } from 'test/_effect-test';
import { path, withTestFileSystem } from 'test/_test-platform';

const GAME_ID = 'game_12345678901234567890';
const SECRET = 'v2-agent-secret-bearer-token';

const scratches: Scratch[] = [];

afterEach(() =>
  Effect.runPromise(
    Effect.asVoid(Effect.all(scratches.splice(0).map((scratch) => scratch.cleanup)))
  )
);

interface Mirror {
  readonly dir: string;
  readonly run: <A, E>(
    effect: Effect.Effect<A, E, PrivateFs>
  ) => Effect.Effect<Either.Either<A, E>>;
}

const freshMirror = (): Effect.Effect<Mirror> =>
  Effect.gen(function* () {
    const scratch = yield* scratchWorkspace();
    scratches.push(scratch);
    const sessionPath = path.join(scratch.workspace.stateRoot, GAME_ID, 'codex-test.json');
    const dir = yield* mirrorDir(sessionPath);
    return {
      dir,
      run: <A, E>(
        effect: Effect.Effect<A, E, PrivateFs>
      ): Effect.Effect<Either.Either<A, E>> =>
        Effect.either(provideTestLayer(effect, scratch.layer)),
    };
  }).pipe(Effect.orDie);

const REVISION = { turn: 3, revision: 9 } as const;

const unitsTable = (moves: string): string =>
  tableText(REVISION, ['units 1/1 complete'], ['alias', 'unit', 'moves'], [
    ['u1', 'Settlers', moves],
  ]);

describe('atomicity', () => {
  effectTest('a failed write leaves the previous file and no partial', () =>
    withTestFileSystem((files) =>
      Effect.gen(function* () {
        const { dir, run } = yield* freshMirror();
        const target = path.join(dir, 'state', 'units.tsv');
        expect(
          Either.isRight(
            yield* run(writeMirror(dir, ['state', 'units.tsv'], unitsTable('3/3')))
          )
        ).toBe(true);
        const before = yield* files.readFileString(target);
        yield* files.chmod(path.dirname(target), 0o500);
        const failed = yield* run(writeMirror(dir, ['state', 'units.tsv'], unitsTable('1/3')));
        yield* files.chmod(path.dirname(target), 0o700);
        expect(Either.isLeft(failed)).toBe(true);
        expect(yield* files.readFileString(target)).toBe(before);
        expect(
          (yield* files.readDirectory(path.dirname(target))).filter((name) => name.startsWith('.'))
        ).toEqual([]);
      }).pipe(Effect.orDie)
    )
  );

  effectTest('a failed first write creates no file at all', () =>
    withTestFileSystem((files) =>
      Effect.gen(function* () {
        const { dir, run } = yield* freshMirror();
        const state = path.join(dir, 'state');
        yield* files.makeDirectory(state, { recursive: true, mode: 0o700 });
        yield* files.chmod(state, 0o500);
        const failed = yield* run(writeMirror(dir, ['state', 'cities.tsv'], unitsTable('3/3')));
        yield* files.chmod(state, 0o700);
        expect(Either.isLeft(failed)).toBe(true);
        expect(yield* files.exists(path.join(state, 'cities.tsv'))).toBe(false);
        expect(yield* files.readDirectory(state)).toEqual([]);
      }).pipe(Effect.orDie)
    )
  );

  effectTest('the failure is a state-mirror PlayerError, never a thrown exception', () =>
    withTestFileSystem((files) =>
      Effect.gen(function* () {
        const { dir, run } = yield* freshMirror();
        const state = path.join(dir, 'state');
        yield* files.makeDirectory(state, { recursive: true, mode: 0o700 });
        yield* files.chmod(state, 0o500);
        const failed = yield* run(writeMirror(dir, ['state', 'units.tsv'], unitsTable('3/3')));
        yield* files.chmod(state, 0o700);
        expect(Either.isLeft(failed) ? failed.left._tag : '').toBe('PlayerError');
      }).pipe(Effect.orDie)
    )
  );

  effectTest('a rename that fails over an occupied name leaks no temp file', () =>
    withTestFileSystem((files) =>
      Effect.gen(function* () {
        const { dir, run } = yield* freshMirror();
        const occupied = path.join(dir, 'state', 'units.tsv');
        yield* files.makeDirectory(occupied, { recursive: true });
        yield* files.writeFileString(path.join(occupied, 'occupied'), 'x');
        const failed = yield* run(writeMirror(dir, ['state', 'units.tsv'], unitsTable('3/3')));
        expect(Either.isLeft(failed)).toBe(true);
        expect(
          (yield* files.readDirectory(path.dirname(occupied))).filter((name) => name.startsWith('.'))
        ).toEqual([]);
      }).pipe(Effect.orDie)
    )
  );
});

const HEALTH = {
  schema_version: 2,
  control_protocol: 'full-control-v2',
  game_id: GAME_ID,
  agent: { agent_id: 'agent_test-controller', controller_label: 'codex-test-model' },
  game_state: 'running',
  seat: { place: 1, seat_id: 'place-1', player_name: 'AgentPlace1' },
  sidecar: { state: 'running', generation: 1, server_connected: true },
  observation_available: true,
  legal_actions_available: true,
  phase: {
    state: 'awaiting_agent',
    turn: 3,
    phase: 0,
    active: true,
    timing: {
      mode: 'default',
      timeout_s: 180,
      deadline_started_at: 10.0,
      deadline_at: 190.0,
      elapsed_s: 139.0,
      remaining_s: 41.0,
    },
  },
  last_phase_end: null,
  objective: 'Maximize final Freeciv civilization score.',
  max_turns: 5000,
  turns_remaining: 4997,
} as const;

const STATE_TOKEN = `state_token_${'5'.repeat(20)}`;

describe('leaks', () => {
  effectTest('no token or private state reaches the mirror', () =>
    withTestFileSystem((files) =>
      Effect.gen(function* () {
        const { dir, run } = yield* freshMirror();
        const poisoned = {
          ...HEALTH,
          agent: { ...HEALTH.agent, agent_token: SECRET },
          sidecar: { ...HEALTH.sidecar, error_code: SECRET },
          authorization: `Bearer ${SECRET}`,
          invite: `invite_${'7'.repeat(32)}`,
        };
        const written = yield* run(
          updateFromHealth(dir, 'turn', poisoned, {
            revision: { turn: 3, revision: 9, state_token: STATE_TOKEN },
          })
        );
        expect(Either.isRight(written)).toBe(true);
        expect(Either.isRight(written) ? written.right.length : 0).toBe(2);
        for (const relative of [HEADER_FILE, PHASE_FILE]) {
          const text = yield* files.readFileString(path.join(dir, ...relative));
          expect(text).not.toContain(SECRET);
          expect(text).not.toContain('Bearer');
          expect(text).not.toContain('invite_');
          expect(text).not.toContain(STATE_TOKEN);
          expect(text).not.toContain('state_token');
        }
      }).pipe(Effect.orDie)
    )
  );

  effectTest('mirror files are private to the seat', () =>
    withTestFileSystem((files) =>
      Effect.gen(function* () {
        const { dir, run } = yield* freshMirror();
        expect(
          Either.isRight(
            yield* run(writeMirror(dir, ['state', 'units.tsv'], unitsTable('3/3')))
          )
        ).toBe(true);
        expect(
          Either.isRight(yield* run(updateFromHealth(dir, 'turn', HEALTH, { revision: REVISION })))
        ).toBe(true);
        expect(Either.isRight(yield* run(writeMirror(dir, DELTA_FILE, '# rev 9 turn 3\n')))).toBe(
          true
        );
        for (const relative of [['state', 'units.tsv'], HEADER_FILE, PHASE_FILE, DELTA_FILE]) {
          const mode = (yield* files.stat(path.join(dir, ...relative))).mode & 0o777;
          expect([relative.join('/'), mode.toString(8)]).toEqual([relative.join('/'), '600']);
        }
      }).pipe(Effect.orDie)
    )
  );

  effectTest('a hostile value can never forge a header line', () =>
    withTestFileSystem((files) =>
      Effect.gen(function* () {
        const { dir, run } = yield* freshMirror();
        const hostile = { ...HEALTH, game_state: 'run\nning\t# rev 999 turn 999' };
        expect(
          Either.isRight(
            yield* run(updateFromHealth(dir, 'turn', hostile, { revision: REVISION }))
          )
        ).toBe(true);
        const text = yield* files.readFileString(path.join(dir, ...HEADER_FILE));
        expect(text.split('\n').filter((line) => line.startsWith('#'))).toEqual([
          '# rev 9 turn 3',
        ]);
        expect(text).toContain('run ning # rev 999 turn 999');
      }).pipe(Effect.orDie)
    )
  );
});
