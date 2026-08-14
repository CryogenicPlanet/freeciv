/**
 * The pregame catalog primitives.
 *
 * Ports `test_v2_start_sanitizes_a_label_and_picks_a_sex_deterministically`
 * (test_client.py:8762) and covers the pieces `command_start` composes but the
 * Python asserts only through it: the mirror re-read's fail-closed conditions,
 * the near-miss refusal text, the sorted deterministic draw, and the phase-aware
 * refusal that `legal` and `batch` will import from here.
 */
import { afterEach, describe, expect, test } from 'bun:test';
import { Effect, Either } from 'effect';
import { createHash } from 'node:crypto';
import type { BatchDisposition } from 'src/schema/batch';
import type { ReceiptState } from 'src/schema/receipt';
import { type PlayerError, V2ResponseError, playerError } from 'src/errors';
import {
  NOT_READIED_LINE,
  PREGAME_SHOWN_MAX,
  configureIntent,
  pyRepr,
  shownChoices,
  startJson,
  startingLine,
} from 'src/render/pregame';
import { CACHE_DIR, HEADER_FILE, mirrorDir, pageNote, tableText, writeMirror } from 'src/services/mirror';
import { PrivateFs } from 'src/services/private-fs';
import {
  V2_LEADER_MAX_BYTES,
  V2_PREGAME_CATALOGS,
  cachedStyleName,
  checkPregameArguments,
  defaultArguments,
  defaultSex,
  mirrorPregameCatalog,
  orderProperties,
  orderReceiptOk,
  phaseAwareRefusal,
  pregameChoice,
  pregameDefaultNation,
  sanitizedLeader,
  type PregameItem,
} from 'src/services/pregame';
import { FIXTURE_GAME_ID, scratchWorkspace, type Scratch } from 'test/_fixtures';
import { dispositionOf, rev } from 'test/_do-harness';
import { awaitTest } from 'test/_effect-test';
import { observedFirst, observedLast } from 'test/_expect';
import { path } from 'test/_test-platform';

const scratches: Scratch[] = [];

afterEach(() =>
  Promise.all(scratches.splice(0).map((scratch) => scratch.cleanup()))
);

const NATION = `nation_${'a'.repeat(32)}`;
const ZULU = `nation_${'c'.repeat(32)}`;
const STYLE = `style_${'b'.repeat(32)}`;

const item = (id: string, name: string, style?: string): PregameItem => {
  if (style === undefined) return { id, name };
  return { id, name, default_style_id: style };
};

const receiptDisposition = (state: ReceiptState): BatchDisposition =>
  dispositionOf('batch_receipt_truth_table', state, rev(4));

// ---------------------------------------------------------------------------
// _sanitized_leader
// ---------------------------------------------------------------------------

describe('sanitizedLeader', () => {
  test('a harness label survives as itself', () => {
    expect(sanitizedLeader('pi-gpt-5.5')).toBe('pi-gpt-5.5');
    expect(sanitizedLeader('codex-test-model')).toBe('codex-test-model');
  });

  test('a separator becomes a hyphen and runs of space collapse', () => {
    expect(sanitizedLeader('  codex/test  model  ')).toBe('codex-test model');
  });

  test('a label with nothing usable in it reduces to nothing, not to punctuation', () => {
    expect(sanitizedLeader('***')).toBe('');
    expect(sanitizedLeader('')).toBe('');
    expect(sanitizedLeader("  '-.  ")).toBe('');
  });

  test('an over-long label is cut to the byte budget and re-stripped', () => {
    const long = sanitizedLeader('A'.repeat(200));
    expect(new TextEncoder().encode(long).length).toBeLessThanOrEqual(V2_LEADER_MAX_BYTES);
    expect(long).toBe('A'.repeat(V2_LEADER_MAX_BYTES));
    // Truncation that lands on a hyphen must not leave the hyphen behind.
    const trailing = sanitizedLeader(`${'B'.repeat(V2_LEADER_MAX_BYTES)}-CCC`);
    expect(trailing).toBe('B'.repeat(V2_LEADER_MAX_BYTES));
  });

  test('a non-ASCII label is hyphenated before the byte budget is measured', () => {
    // Every non-ASCII code point is outside the accepted class, so the byte
    // count can never exceed the character count once the strip has run.
    const folded = sanitizedLeader('Boudicca ✦ the Brave');
    expect(folded).toBe('Boudicca - the Brave');
  });
});

// ---------------------------------------------------------------------------
// the deterministic sex
// ---------------------------------------------------------------------------

describe('defaultSex', () => {
  test('it is a pure function of the resolved leader name', () => {
    const expected =
      (createHash('sha256').update('codex-test-model', 'utf8').digest()[0] ?? 0) % 2 !== 0
        ? 'male'
        : 'female';
    expect(defaultSex('codex-test-model')).toBe(expected);
    expect(defaultSex('codex-test-model')).toBe(defaultSex('codex-test-model'));
  });
});

// ---------------------------------------------------------------------------
// _pregame_choice
// ---------------------------------------------------------------------------

const refusalOf = <A>(effect: Effect.Effect<A, PlayerError>): string => {
  const outcome = Effect.runSync(Effect.either(effect));
  if (Either.isRight(outcome)) throw new Error('expected a refusal');
  return outcome.left.message;
};

describe('pregameChoice', () => {
  const catalog = [item(NATION, 'English', STYLE), item(ZULU, 'Zulu', STYLE)];

  test('a name matches case-insensitively and exactly', () => {
    expect(Effect.runSync(pregameChoice(catalog, 'eNgLiSh', 'nation')).id).toBe(NATION);
  });

  test('an unmatched name is refused with the catalog quoted back', () => {
    const message = refusalOf(pregameChoice(catalog, 'Atlantean', 'nation'));
    expect(message).toBe("no nation named 'Atlantean' is offered; try one of: English Zulu");
  });

  test('a near miss narrows the suggestion to the near misses', () => {
    const message = refusalOf(pregameChoice(catalog, 'lish', 'nation'));
    expect(message).toBe("no nation named 'lish' is offered; try one of: English");
  });

  test('an empty catalog says so rather than trailing off', () => {
    expect(refusalOf(pregameChoice([], 'English', 'nation'))).toBe(
      "no nation named 'English' is offered; try one of: none were offered"
    );
  });

  test('a duplicated name is the lobby catalog being broken, not the input', () => {
    const doubled = [item(NATION, 'English'), item(ZULU, 'english')];
    expect(refusalOf(pregameChoice(doubled, 'English', 'nation'))).toBe(
      "nation 'English' is offered more than once; this lobby catalog is ambiguous"
    );
  });

  test('the suggestion list is capped, and sorted before it is cut', () => {
    const many = Array.from({ length: 30 }, (_index, position) =>
      item(`nation_${'z'.repeat(30)}${position}`, `N${String(position).padStart(2, '0')}`)
    );
    const message = refusalOf(pregameChoice(many, 'Atlantean', 'nation'));
    const shown = message.split('try one of: ')[1] ?? '';
    expect(shown.split(' ')).toHaveLength(PREGAME_SHOWN_MAX);
    expect(shown.startsWith('N00 N01')).toBe(true);
  });

  test('a repr keeps CPython’s quoting choices', () => {
    expect(pyRepr('Atlantean')).toBe("'Atlantean'");
    expect(pyRepr("N'Djamena")).toBe('"N\'Djamena"');
    expect(pyRepr('a\tb')).toBe("'a\\tb'");
    expect(shownChoices([])).toBe('none were offered');
  });
});

// ---------------------------------------------------------------------------
// _pregame_default_nation
// ---------------------------------------------------------------------------

describe('pregameDefaultNation', () => {
  test('the draw is made over the sorted catalog, so a seeded RNG reproduces it', () => {
    const offered = [item(ZULU, 'Zulu', STYLE), item(NATION, 'English', STYLE)];
    const seen: ReadonlyArray<string>[] = [];
    const picked = Effect.runSync(
      pregameDefaultNation(offered, (choices) => {
        seen.push(choices.map((choice) => choice.name));
        return observedLast(choices);
      })
    );
    expect(seen[0]).toEqual(['English', 'Zulu']);
    expect(picked.id).toBe(ZULU);
  });

  test('a row with no id or no name is never drawable', () => {
    const offered = [item('', 'Nameless'), item(ZULU, ''), item(NATION, 'English', STYLE)];
    const drawn = Effect.runSync(
      pregameDefaultNation(offered, (choices) => {
        expect(choices.map((choice) => choice.name)).toEqual(['English']);
        return observedFirst(choices);
      })
    );
    expect(drawn.id).toBe(NATION);
  });

  test('an empty lobby fails closed and names the catalog to read', () => {
    expect(
      refusalOf(
        pregameDefaultNation([], () => {
          throw new Error('the draw must not happen');
        })
      )
    ).toBe(
      'this lobby offers no selectable nation; read the catalog with ' +
        '`just state --section pregame_nations`'
    );
  });
});

// ---------------------------------------------------------------------------
// _mirror_pregame_catalog
// ---------------------------------------------------------------------------

const withMirror = <A, E>(
  write: (dir: string) => Effect.Effect<unknown, E, PrivateFs>,
  read: (sessionPath: string) => Effect.Effect<A, never, PrivateFs>
): Effect.Effect<A> =>
  Effect.gen(function* () {
    const scratch = scratchWorkspace();
    scratches.push(scratch);
    const sessionPath = path.join(scratch.workspace.stateRoot, FIXTURE_GAME_ID, 'codex-test.json');
    const dir = yield* Effect.orDie(mirrorDir(sessionPath));
    yield* Effect.provideService(Effect.orDie(write(dir)), PrivateFs, scratch.files);
    return yield* Effect.provideService(read(sessionPath), PrivateFs, scratch.files);
  });

const nationsTable = (
  rows: ReadonlyArray<ReadonlyArray<string>>,
  complete = true
): string =>
  tableText(
    { revision: 4, turn: 0 },
    [pageNote('nations', rows.length, rows.length, complete)],
    ['id', 'nation', 'default_style_id'],
    rows
  );

describe('mirrorPregameCatalog', () => {
  awaitTest('a complete projection is re-read rather than re-fetched', function* () {
    const items = yield* withMirror(
      (dir) =>
        writeMirror(
          dir,
          V2_PREGAME_CATALOGS.get('pregame_nations')?.mirror ?? [],
          nationsTable([
            [NATION, 'English', STYLE],
            [ZULU, 'Zulu', STYLE],
          ])
        ),
      (sessionPath) => mirrorPregameCatalog(sessionPath, 'pregame_nations')
    );
    expect(items).toEqual([
      { id: NATION, name: 'English', default_style_id: STYLE },
      { id: ZULU, name: 'Zulu', default_style_id: STYLE },
    ]);
  });

  awaitTest('a partial projection is refused whole; a half catalog is not a catalog', function* () {
    const items = yield* withMirror(
      (dir) =>
        writeMirror(dir, [CACHE_DIR, 'nations.tsv'], nationsTable([[NATION, 'English', STYLE]], false)),
      (sessionPath) => mirrorPregameCatalog(sessionPath, 'pregame_nations')
    );
    expect(items).toEqual([]);
  });

  awaitTest('a truncated opaque id poisons the whole projection', function* () {
    const items = yield* withMirror(
      (dir) =>
        writeMirror(
          dir,
          [CACHE_DIR, 'nations.tsv'],
          nationsTable([
            [NATION, 'English', STYLE],
            ['nation_cc…', 'Zulu', STYLE],
          ])
        ),
      (sessionPath) => mirrorPregameCatalog(sessionPath, 'pregame_nations')
    );
    expect(items).toEqual([]);
  });

  awaitTest('a projection whose columns moved is not guessed at', function* () {
    const items = yield* withMirror(
      (dir) =>
        writeMirror(
          dir,
          [CACHE_DIR, 'nations.tsv'],
          tableText(
            { revision: 4, turn: 0 },
            [pageNote('nations', 1, 1, true)],
            ['id', 'name'],
            [[NATION, 'English']]
          )
        ),
      (sessionPath) => mirrorPregameCatalog(sessionPath, 'pregame_nations')
    );
    expect(items).toEqual([]);
  });

  awaitTest('an absent projection is empty, not an error', function* () {
    const items = yield* withMirror(
      () => Effect.void,
      (sessionPath) => mirrorPregameCatalog(sessionPath, 'pregame_nations')
    );
    expect(items).toEqual([]);
  });

  awaitTest('a style is named from the mirror alone', function* () {
    const named = yield* withMirror(
      (dir) =>
        writeMirror(
          dir,
          [CACHE_DIR, 'styles.tsv'],
          tableText(
            { revision: 4, turn: 0 },
            [pageNote('styles', 1, 1, true)],
            ['id', 'style'],
            [[STYLE, 'European']]
          )
        ),
      (sessionPath) => cachedStyleName(sessionPath, STYLE)
    );
    expect(named).toBe('European');
    const missing = yield* withMirror(
      () => Effect.void,
      (sessionPath) => cachedStyleName(sessionPath, STYLE)
    );
    expect(missing).toBe('');
  });
});

// ---------------------------------------------------------------------------
// _phase_aware_refusal
// ---------------------------------------------------------------------------

const HEADER = (state: string, active: string): string =>
  `phase ${state} · turn 3 phase 1 · active ${active}\n`;

const underRefusal = (
  header: string | null,
  failure: PlayerError | V2ResponseError
): Effect.Effect<string> =>
  Effect.gen(function* () {
    const scratch = scratchWorkspace();
    scratches.push(scratch);
    const sessionPath = path.join(scratch.workspace.stateRoot, FIXTURE_GAME_ID, 'codex-test.json');
    if (header !== null) {
      const dir = yield* Effect.orDie(mirrorDir(sessionPath));
      yield* Effect.provideService(
        Effect.orDie(writeMirror(dir, HEADER_FILE, header)),
        PrivateFs,
        scratch.files
      );
    }
    const outcome = yield* Effect.provideService(
      Effect.either(phaseAwareRefusal(sessionPath, Effect.fail(failure))),
      PrivateFs,
      scratch.files
    );
    if (Either.isRight(outcome)) throw new Error('expected a refusal');
    return outcome.left.message;
  });

describe('phaseAwareRefusal', () => {
  awaitTest('a refusal raised after the phase ended leads with the phase fact', function* () {
    const message = yield* underRefusal(
      HEADER('inactive_done', 'no'),
      playerError('unknown or expired action ID')
    );
    expect(message).toBe(
      'your phase is not active (state inactive_done) — just wait\n' +
        'unknown or expired action ID'
    );
  });

  awaitTest('a live phase leaves the refusal exactly as it was', function* () {
    const message = yield* underRefusal(
      HEADER('awaiting_agent', 'yes'),
      playerError('unknown or expired action ID')
    );
    expect(message).toBe('unknown or expired action ID');
  });

  awaitTest('a refusal that already leads with the note is not double-prefixed', function* () {
    const note = 'your phase is not active (state ending) — just wait';
    const message = yield* underRefusal(HEADER('ending', 'no'), playerError(`${note}\nand more`));
    expect(message).toBe(`${note}\nand more`);
  });

  awaitTest('no mirror at all means no rewrite', function* () {
    expect(yield* underRefusal(null, playerError('cold cache'))).toBe('cold cache');
  });

  awaitTest('a validated wire refusal is rewritten too, exactly as CPython subclasses it', function* () {
    const message = yield* underRefusal(
      HEADER('phase_not_ready', 'no'),
      new V2ResponseError({ message: 'HTTP 409: no (conflict)', status: 409, payload: null })
    );
    expect(message).toBe(
      'your phase is not active (state phase_not_ready) — just wait\nHTTP 409: no (conflict)'
    );
  });
});

// ---------------------------------------------------------------------------
// argument checking
// ---------------------------------------------------------------------------

const CONFIGURE_SCHEMA = {
  type: 'object',
  properties: {
    nation_id: { type: 'string' },
    leader_name: { type: 'string' },
    is_male: { type: 'boolean' },
    style_id: { type: 'string' },
  },
  required: ['nation_id', 'leader_name', 'is_male', 'style_id'],
};

describe('checkPregameArguments', () => {
  const action = {
    action_id: 'action_1',
    kind: 'pregame.configure',
    argument_schema: CONFIGURE_SCHEMA,
  };
  const good = { nation_id: NATION, leader_name: 'Ada', is_male: false, style_id: STYLE };

  test('the arguments this workspace builds are accepted', () => {
    expect(Effect.runSync(Effect.either(checkPregameArguments(action, good)))).toEqual(
      Either.right(undefined)
    );
  });

  test('a schema that wants something else names the escape hatch', () => {
    const { nation_id: _dropped, ...missing } = good;
    expect(refusalOf(checkPregameArguments(action, missing))).toBe(
      'the enumerated pregame.configure action does not take the arguments ' +
        'this workspace builds; run `just legal --kind pregame.configure --all ' +
        '--json` and submit it with `just batch`'
    );
  });

  test('an argument the schema never declared is refused the same way', () => {
    expect(refusalOf(checkPregameArguments(action, { ...good, colour: 'red' }))).toContain(
      'does not take the arguments this workspace builds'
    );
  });

  test('required names the schema does not declare as properties are ignored', () => {
    const drifted = {
      action_id: 'action_1',
      kind: 'pregame.configure',
      argument_schema: { properties: { ready: { type: 'boolean' } }, required: ['ready', 'ghost'] },
    };
    expect(orderProperties(drifted).required).toEqual(['ready']);
  });
});

describe('defaultArguments and orderReceiptOk', () => {
  const ready = {
    action_id: 'action_2',
    kind: 'pregame.set_ready',
    argument_schema: {
      type: 'object',
      properties: { ready: { type: 'boolean', enum: [true] } },
      required: ['ready'],
    },
  };

  test('a single-valued enum fills itself', () => {
    expect(defaultArguments(ready)).toEqual({ ready: true });
  });

  test('a choice with more than one legal value is never guessed', () => {
    expect(
      defaultArguments({
        ...ready,
        argument_schema: {
          properties: { ready: { type: 'boolean', enum: [true, false] } },
          required: ['ready'],
        },
      })
    ).toBeNull();
  });

  test('only accepted and applied mean the seat was configured', () => {
    const withoutReceipt: BatchDisposition = { ...receiptDisposition('applied'), receipt: null };
    expect(orderReceiptOk(receiptDisposition('applied'))).toBe(true);
    expect(orderReceiptOk(receiptDisposition('accepted'))).toBe(true);
    expect(orderReceiptOk(receiptDisposition('rejected'))).toBe(false);
    expect(orderReceiptOk(receiptDisposition('ambiguous'))).toBe(false);
    expect(orderReceiptOk(withoutReceipt)).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// the printed shapes
// ---------------------------------------------------------------------------

describe('the start block', () => {
  test('an unnamed style reads as the nation default, not as an opaque id', () => {
    expect(startingLine('Zulu', 'codex-test-model', false, '')).toBe(
      'starting as Zulu — codex-test-model (female), style the nation default'
    );
    expect(startingLine('English', 'Ada', true, 'European')).toBe(
      'starting as English — Ada (male), style European'
    );
  });

  test('the intent names the seat the receipt belongs to', () => {
    expect(configureIntent('English', 'Ada', false)).toBe('configure English Ada female');
    expect(NOT_READIED_LINE).toBe('not readied: the configuration was not accepted');
  });

  test('--json carries the resolved choices and every disposition', () => {
    expect(
      startJson({
        nation: 'English',
        leader: 'Ada',
        male: false,
        styleId: STYLE,
        dispositions: [{ batch_id: 'batch_1' }],
      })
    ).toEqual({
      schema_version: 1,
      command: 'start',
      nation: 'English',
      leader: 'Ada',
      is_male: false,
      style_id: STYLE,
      dispositions: [{ batch_id: 'batch_1' }],
    });
  });
});
