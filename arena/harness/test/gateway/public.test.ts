/**
 * `src/gateway/public.ts` against its oracle.
 *
 * Two kinds of test, and the second is the one that matters:
 *
 * 1. **Behaviour** — the rules `agent_eval/replay_gateway.py`'s `_public_*`
 *    family states, written as assertions.  These cover the shapes a fixture
 *    cannot reach (lone surrogates, `__proto__` keys) and the ones the Python
 *    suite never exercises at all (`PUBLIC_SCORE_METRICS`, the `\x85` strip).
 * 2. **Differential** — the same input handed to CPython's real functions
 *    through `python3 -c`, with both sides canonicalized (`sort_keys=True`,
 *    `(",", ":")`, `ensure_ascii=False`) and compared as **text**.  That
 *    catches what an eyeballed expectation cannot: a `180` where Python wrote
 *    `180.0`, a key this port emits and Python omits, a different sort.
 *
 * The differential driver spawns one short-lived `python3` per call.  It reads
 * only `agent_eval/replay_gateway.py`, writes nothing, and touches no running
 * stack.
 *
 * @module
 */

import { describe, expect, it } from 'bun:test';
import { Either, Option } from 'effect';
import { canonicalText, CANON_UTF8, type CanonValue, decodeGameId, Gateway } from '@arena/wire';
import {
  isUntrusted,
  publicAiDifficulty,
  publicControlProtocol,
  publicCounts,
  publicEvent as publicEventOfCanon,
  publicEvents as publicEventsOfCanon,
  publicInt,
  publicNumber,
  publicPlaces,
  publicText,
  publicTiming,
  pythonStrip,
  untrustedField,
  untrustedFieldOr,
} from '../../src/gateway/public.ts';
import { parsePythonJson } from '../../src/gateway/python-json.ts';
import type { GameId } from '@arena/wire';

// ---------------------------------------------------------------------------
// The oracle
// ---------------------------------------------------------------------------

/** The checkout root: `test/gateway/` → `arena/harness/` → `arena/` → repo. */
const REPO_ROOT = new URL('../../../../', import.meta.url).pathname;

/**
 * One `python3 -c` program that dispatches on `op` and prints the canonical
 * JSON of `_public_*`'s answer.
 *
 * `default=str` is deliberately **absent**: if a projection ever returns
 * something `json.dumps` cannot spell, the driver must fail loudly rather than
 * stringify it into a passing comparison.
 */
const PUBLIC_DRIVER = `
import json, sys
sys.path.insert(0, ${JSON.stringify(REPO_ROOT)})
from agent_eval import replay_gateway as g

request = json.load(sys.stdin)
op, args = request["op"], request["args"]
if op == "public_text":
    value = g._public_text(args["value"], args["fallback"], args.get("limit", 160))
elif op == "public_int":
    value = g._public_int(args["value"], args.get("default", 0), args.get("minimum", 0))
elif op == "public_number":
    value = g._public_number(args["value"], args.get("default", 0.0))
elif op == "public_event":
    value = g._public_event(args["value"])
elif op == "public_counts":
    value = g._public_counts(args["value"])
elif op == "public_events":
    value = g._public_events(args["value"], args["game_id"])
elif op == "public_timing":
    value = g._public_timing(args["config"])
elif op == "public_control_protocol":
    value = g._public_control_protocol(args["config"], args["manifest"])
elif op == "public_ai_difficulty":
    value = g._public_ai_difficulty(args["level"])
elif op == "public_places":
    value = g._public_places(args["value"], args.get("manifest"))
else:
    raise SystemExit("unknown op: " + op)
sys.stdout.write(
    json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
)
`;

/** `_canonical(value).decode("utf-8")` for a value this port produced. */
const canonical = (value: CanonValue): string =>
  Either.getOrThrowWith(canonicalText(value, CANON_UTF8), (error) => new Error(String(error)));

/** Run the driver and return CPython's canonical answer. */
const oracle = (program: string, op: string, args: unknown): string => {
  const result = Bun.spawnSync(['python3', '-c', program], {
    cwd: REPO_ROOT,
    stdin: Buffer.from(JSON.stringify({ op, args }), 'utf-8'),
  });
  const stderr = result.stderr.toString();
  if (result.exitCode !== 0) throw new Error(`python3 ${op} failed: ${stderr}`);
  return result.stdout.toString();
};

/**
 * A JavaScript literal as CPython's `json.loads` reads the same JSON text.
 *
 * `_public_event` and `_public_events` are the two projections whose rejections
 * are `isinstance(..., int)` tests, so what they are handed has to carry the
 * int/float distinction: their real input is a derivation document read by
 * `src/gateway/python-json.ts`, and a `12` here means a Python `int` exactly as
 * the `12` the oracle receives over stdin does.  Round-tripping the fixture
 * through the same reader is what keeps the two sides comparing like with like
 * — writing `12n` by hand in every fixture would say the same thing and would
 * stop saying it the moment someone added a case.
 */
const asPython = (value: unknown): CanonValue =>
  Either.getOrThrowWith(
    parsePythonJson(JSON.stringify(value)),
    (error) => new Error(`fixture is not JSON: ${error.message}`),
  );

const publicEvent = (value: unknown): ReturnType<typeof publicEventOfCanon> =>
  publicEventOfCanon(asPython(value));

const publicEvents = (value: unknown, gameId: GameId): ReturnType<typeof publicEventsOfCanon> =>
  publicEventsOfCanon(asPython(value), gameId);

/** True when the oracle can run at all; the differential suites skip if not. */
const PYTHON_AVAILABLE = Bun.spawnSync(['python3', '-c', 'import sys']).exitCode === 0;

const GAME_ID = Either.getOrThrowWith(
  decodeGameId('game_ieTomdES08hpUmFRFzCOAVMo'),
  (error) => new Error(String(error)),
);

// ---------------------------------------------------------------------------
// _public_text
// ---------------------------------------------------------------------------

describe('publicText', () => {
  it('replaces a non-string, and a string that narrows to nothing, with the fallback', () => {
    expect(publicText(42, 'fb')).toBe('fb');
    expect(publicText(null, 'fb')).toBe('fb');
    expect(publicText(['a'], 'fb')).toBe('fb');
    expect(publicText('', 'fb')).toBe('fb');
    expect(publicText('   ', 'fb')).toBe('fb');
    // Control characters are deleted first, so this is empty, not "\x01".
    expect(publicText('\u0001\u0002', 'fb')).toBe('fb');
  });

  it('keeps the tab Python names explicitly and drops the rest of C0', () => {
    expect(publicText('a\tb', 'fb')).toBe('a\tb');
    expect(publicText('a\nb', 'fb')).toBe('ab');
    expect(publicText('a\u0000b', 'fb')).toBe('ab');
    expect(publicText('ab', 'fb')).toBe('ab');
  });

  it('strips what Python calls whitespace, not what ECMAScript does', () => {
    // \x85 (NEL): whitespace to `str.strip()`, not to `String.trim()`.
    expect(publicText('\u0085hi\u0085', 'fb')).toBe('hi');
    expect('\u0085hi\u0085'.trim()).toBe('\u0085hi\u0085');
    // U+FEFF: the reverse — `trim()` would eat it, Python keeps it.
    expect(publicText('\uFEFFhi', 'fb')).toBe('\uFEFFhi');
    expect('\uFEFFhi'.trim()).toBe('hi');
  });

  it('truncates by code point, so an astral character is never cut in half', () => {
    const rocket = '\u{1F680}';
    expect(publicText(rocket.repeat(4), 'fb', 2)).toBe(rocket.repeat(2));
    // The UTF-16 answer would be a lone surrogate, and an unencodable body.
    expect(rocket.repeat(4).slice(0, 2)).not.toBe(rocket.repeat(2));
  });

  it('is a strip, not a trim: pythonStrip leaves interior whitespace alone', () => {
    expect(pythonStrip('  a b　 ')).toBe('a b');
  });
});

// ---------------------------------------------------------------------------
// _public_int / _public_number
// ---------------------------------------------------------------------------

describe('publicInt', () => {
  it('answers a bigint so the canonical writer omits the ".0"', () => {
    expect(publicInt(7)).toBe(7n);
    expect(canonical({ turn: publicInt(7) })).toBe('{"turn":7}');
    expect(canonical({ turn: publicNumber(7) })).toBe('{"turn":7.0}');
  });

  it('rejects booleans, non-finite numbers and fractions rather than rounding', () => {
    expect(publicInt(true, 9n)).toBe(9n);
    expect(publicInt(false, 9n)).toBe(9n);
    expect(publicInt(Number.NaN, 9n)).toBe(9n);
    expect(publicInt(Number.POSITIVE_INFINITY, 9n)).toBe(9n);
    expect(publicInt(1.5, 9n)).toBe(9n);
    expect(publicInt(3.0, 9n)).toBe(3n);
    expect(publicInt('4', 9n)).toBe(9n);
  });

  it('clamps to the minimum, which _public_places drives to -1', () => {
    expect(publicInt(-5)).toBe(0n);
    expect(publicInt(-5, -1n, -1n)).toBe(-1n);
    expect(publicInt(2, -1n, -1n)).toBe(2n);
  });

  it('accepts a bigint, because a decoded loader payload already carries one', () => {
    expect(publicInt(12n)).toBe(12n);
    expect(publicInt(-12n)).toBe(0n);
  });
});

describe('publicNumber', () => {
  it('keeps a float a float and defaults everything unusable', () => {
    expect(publicNumber(1.5)).toBe(1.5);
    expect(publicNumber(true, 2)).toBe(2);
    expect(publicNumber(Number.NaN, 2)).toBe(2);
    expect(publicNumber(null, 2)).toBe(2);
    // Never null: a still-running disk row publishes 0.0, not null (`:1148`).
    expect(publicNumber(undefined)).toBe(0);
  });
});

// ---------------------------------------------------------------------------
// dict access
// ---------------------------------------------------------------------------

describe('untrustedField', () => {
  it('does not answer from Object.prototype where dict.get would say None', () => {
    expect(untrustedField({}, 'constructor')).toBeUndefined();
    expect(untrustedField({}, 'toString')).toBeUndefined();
    expect(untrustedField({ toString: 'mine' }, 'toString')).toBe('mine');
  });

  it('treats an array as not-a-mapping, as isinstance(_, Mapping) does', () => {
    expect(isUntrusted([1, 2])).toBe(false);
    expect(untrustedField([1, 2], '0')).toBeUndefined();
  });

  it('lets a present null state win over status, and an absent one not', () => {
    expect(untrustedFieldOr({ state: null, status: 'completed' }, 'state', 'status')).toBeNull();
    expect(untrustedFieldOr({ status: 'completed' }, 'state', 'status')).toBe('completed');
    expect(untrustedFieldOr({ state: 'failed', status: 'completed' }, 'state', 'status')).toBe(
      'failed',
    );
  });
});

// ---------------------------------------------------------------------------
// _public_event / _public_events
// ---------------------------------------------------------------------------

const EVENT = {
  turn: 12,
  kind: 'city_captured',
  summary: 'A captured B',
  weight: 66,
  actors: ['place-1', 'place-2'],
  data: { city: 'B' },
  token: 'must-not-leak',
};

describe('publicEvent', () => {
  it('publishes six keys and never the seventh', () => {
    const event = Option.getOrThrow(publicEvent(EVENT));
    expect(Object.keys(event).toSorted()).toEqual([
      'actors',
      'data',
      'kind',
      'summary',
      'turn',
      'weight',
    ]);
    expect(canonical(event)).not.toContain('must-not-leak');
    expect(event.turn).toBe(12n);
    expect(event.weight).toBe(66n);
  });

  it('drops a row whose weight is outside [1, 100] rather than clamping it', () => {
    expect(Option.isNone(publicEvent({ ...EVENT, weight: 0 }))).toBe(true);
    expect(Option.isNone(publicEvent({ ...EVENT, weight: 101 }))).toBe(true);
    expect(Option.isNone(publicEvent({ ...EVENT, weight: true }))).toBe(true);
    expect(Option.isSome(publicEvent({ ...EVENT, weight: 1 }))).toBe(true);
  });

  it('drops a row with no turn, no kind or no summary', () => {
    expect(Option.isNone(publicEvent({ ...EVENT, turn: null }))).toBe(true);
    expect(Option.isNone(publicEvent({ ...EVENT, turn: -1 }))).toBe(true);
    expect(Option.isNone(publicEvent({ ...EVENT, kind: '' }))).toBe(true);
    expect(Option.isNone(publicEvent({ ...EVENT, summary: '' }))).toBe(true);
    expect(Option.isNone(publicEvent('not a row'))).toBe(true);
  });

  it('caps actors at eight, keeps only strings, and empties a non-object data', () => {
    const event = Option.getOrThrow(
      publicEvent({
        ...EVENT,
        actors: [...Array.from({ length: 12 }, (_, index) => `a${String(index)}`), 7],
        data: ['not', 'a', 'dict'],
      }),
    );
    expect(event.actors).toHaveLength(8);
    expect(event.data).toEqual({});
  });
});

describe('publicEvents', () => {
  it('overrides schema_version and game_id with the gateway’s own values', () => {
    const value = publicEvents({ schema_version: 99, game_id: 'other', events: [] }, GAME_ID);
    expect(value.schema_version).toBe(1n);
    expect(value.game_id).toBe(GAME_ID);
  });

  it('recomputes total_events and min_included_weight over the survivors', () => {
    const value = publicEvents(
      {
        events: [EVENT, { ...EVENT, weight: 3 }, { ...EVENT, weight: 0 }],
        min_included_weight: 42,
      },
      GAME_ID,
    );
    expect(value.events).toHaveLength(2);
    expect(value.total_events).toBe(2n);
    expect(value.min_included_weight).toBe(3n);
  });

  it('falls back to the loader’s min_included_weight only when nothing survived', () => {
    expect(publicEvents({ events: [], min_included_weight: 42 }, GAME_ID).min_included_weight).toBe(
      42n,
    );
  });

  it('keeps a warning’s turn only when it is an int, and caps the list at 100', () => {
    const value = publicEvents(
      {
        event_warnings: [
          { turn: 3, message: 'a' },
          { turn: 'x', message: 'b' },
          { message: 'c' },
          { turn: 1 },
          ...Array.from({ length: 120 }, () => ({ turn: 1, message: 'z' })),
        ],
      },
      GAME_ID,
    );
    expect(value.event_warnings).toHaveLength(100);
    expect(value.event_warnings[0]).toEqual({ turn: 3n, message: 'a' });
    expect(value.event_warnings[1]).toEqual({ turn: null, message: 'b' });
    expect(value.event_warnings[2]).toEqual({ turn: null, message: 'c' });
  });
});

describe('publicCounts', () => {
  it('narrows both halves and collapses kinds that truncate together', () => {
    const long = 'k'.repeat(50);
    const counts = publicCounts({ [long]: 2, [`${long}!`]: 5, ok: 1.5 });
    expect(counts[`k`.repeat(40)]).toBe(5n);
    expect(counts['ok']).toBe(0n);
  });
});

// ---------------------------------------------------------------------------
// _public_timing / _public_control_protocol / _public_ai_difficulty
// ---------------------------------------------------------------------------

describe('publicTiming', () => {
  it('publishes both keys or neither', () => {
    expect(publicTiming({ timing_mode: 'blitz', action_timeout_s: 60 })).toEqual({
      timing_mode: 'blitz',
      action_timeout_s: 60,
    });
    expect(publicTiming({ timing_mode: 'infinite', action_timeout_s: null })).toEqual({
      timing_mode: 'infinite',
      action_timeout_s: null,
    });
    // `dict.get` answers `None` for an absent key too, and `infinite`
    // publishes on both spellings.
    expect(publicTiming({ timing_mode: 'infinite' })).toEqual({
      timing_mode: 'infinite',
      action_timeout_s: null,
    });
    expect(publicTiming({ timing_mode: 'custom', action_timeout_s: 7.5 })).toEqual({
      timing_mode: 'custom',
      action_timeout_s: 7.5,
    });
  });

  it('refuses an inconsistent pair, a legacy timeout, and an unknown mode', () => {
    expect(publicTiming({ timing_mode: 'default', action_timeout_s: 60 })).toEqual({});
    expect(publicTiming({ timing_mode: 'blitz', action_timeout_s: 180 })).toEqual({});
    expect(publicTiming({ timing_mode: 'infinite', action_timeout_s: 60 })).toEqual({});
    expect(publicTiming({ action_timeout_s: 180 })).toEqual({});
    expect(publicTiming({ timing_mode: 'deity', action_timeout_s: 180 })).toEqual({});
    expect(publicTiming({ timing_mode: 'custom', action_timeout_s: 0 })).toEqual({});
    expect(publicTiming({ timing_mode: 'custom', action_timeout_s: true })).toEqual({});
  });

  it('publishes 180.0 as a float, which is why the pair is a number here', () => {
    expect(canonical({ ...publicTiming({ timing_mode: 'default', action_timeout_s: 180 }) })).toBe(
      '{"action_timeout_s":180.0,"timing_mode":"default"}',
    );
  });
});

describe('publicControlProtocol', () => {
  it('prefers the config, falls back to the manifest, and never invents a value', () => {
    expect(publicControlProtocol({ control_protocol: 'full-control-v2' }, {})).toEqual({
      control_protocol: 'full-control-v2',
    });
    expect(publicControlProtocol({}, { control_protocol: 'strategic-v1' })).toEqual({
      control_protocol: 'strategic-v1',
    });
    expect(publicControlProtocol({ control_protocol: 'full-control-v9' }, {})).toEqual({});
    expect(publicControlProtocol({}, {})).toEqual({});
  });
});

describe('publicAiDifficulty', () => {
  it('narrows to the published levels and answers null for anything else', () => {
    expect(publicAiDifficulty('hard')).toBe('hard');
    expect(publicAiDifficulty('cheating')).toBe('cheating');
    expect(publicAiDifficulty('deity')).toBeNull();
    expect(publicAiDifficulty(null)).toBeNull();
    expect(publicAiDifficulty(2)).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// _public_places
// ---------------------------------------------------------------------------

const AGENT_PLACE = {
  place: 1,
  seat_id: 'place-1',
  player_name: 'AgentPlace1',
  player_color: '#0067A5',
  controller: 'agent',
  joined: true,
  controller_label: 'pi-gpt-5.5',
  controller_type: 'external',
  model: null,
  controller_metadata: { model: 'gpt-5.5', token: 'must-not-leak' },
  controller_fingerprint: 'c13b2ded092a7e60',
};

const NATIVE_PLACE = {
  place: 2,
  seat_id: 'place-2',
  player_name: 'NativePlace2',
  player_color: '#F38400',
  controller: 'native_classic_ai',
  joined: false,
};

describe('publicPlaces', () => {
  it('lifts the model out of controller_metadata and drops the metadata itself', () => {
    const [place] = publicPlaces([AGENT_PLACE]);
    expect(place?.model).toBe('gpt-5.5');
    expect(place?.controller_label).toBe('pi-gpt-5.5');
    expect(canonical(publicPlaces([AGENT_PLACE]))).not.toContain('must-not-leak');
    expect(canonical(publicPlaces([AGENT_PLACE]))).not.toContain('controller_fingerprint');
  });

  it('defaults a native place and inherits the game difficulty', () => {
    const [place] = publicPlaces([NATIVE_PLACE], { config: { difficulty: 'hard' } });
    expect(place?.controller_label).toBe(Gateway.NATIVE_CONTROLLER_LABEL);
    expect(place?.controller_type).toBe(Gateway.NATIVE_CONTROLLER_TYPE);
    expect(place?.model).toBe(Gateway.NATIVE_CONTROLLER_MODEL);
    expect(place?.ai_difficulty).toBe('hard');
  });

  it('omits ai_difficulty entirely when it does not resolve — null is a different claim', () => {
    const [inherited] = publicPlaces([NATIVE_PLACE], { config: { difficulty: 'deity' } });
    expect(inherited === undefined ? null : 'ai_difficulty' in inherited).toBe(false);
    const [own] = publicPlaces([{ ...NATIVE_PLACE, ai_difficulty: 'novice' }], {
      config: { difficulty: 'hard' },
    });
    expect(own?.ai_difficulty).toBe('novice');
    // A non-native place never carries the key, whatever the config says.
    const [agent] = publicPlaces([AGENT_PLACE], { config: { difficulty: 'hard' } });
    expect(agent === undefined ? null : 'ai_difficulty' in agent).toBe(false);
  });

  it('drops a row with no place, no seat id or no player name', () => {
    expect(publicPlaces([{ ...AGENT_PLACE, place: 0 }])).toEqual([]);
    expect(publicPlaces([{ ...AGENT_PLACE, place: 'first' }])).toEqual([]);
    expect(publicPlaces([{ ...AGENT_PLACE, seat_id: '  ' }])).toEqual([]);
    expect(publicPlaces([{ ...AGENT_PLACE, player_name: null }])).toEqual([]);
    expect(publicPlaces(['not a row', 7])).toEqual([]);
    expect(publicPlaces('not a list')).toEqual([]);
  });

  it('sorts by (place, seat_id) with the seat id compared by code point', () => {
    const rows = publicPlaces([
      { ...AGENT_PLACE, place: 2, seat_id: '\u{1F600}' },
      { ...AGENT_PLACE, place: 2, seat_id: 'ﬀ' },
      { ...AGENT_PLACE, place: 1, seat_id: 'b' },
    ]);
    expect(rows.map((row) => row.seat_id)).toEqual(['b', 'ﬀ', '\u{1F600}']);
    // UTF-16 order would have put the astral seat first: its first code unit
    // is a surrogate, which sorts below U+FB00.
    const [astral, ligature] = ['\u{1F600}', 'ﬀ'];
    expect(astral < ligature).toBe(true);
  });

  it('publishes a present-but-blank label when strip() finds a control character', () => {
    const [place] = publicPlaces([{ ...AGENT_PLACE, controller_label: '\u0001' }]);
    expect(place?.controller_label).toBe('');
  });
});

// ---------------------------------------------------------------------------
// Differential against CPython
// ---------------------------------------------------------------------------

describe.if(PYTHON_AVAILABLE)('differential: _public_* in CPython', () => {
  const check = (op: string, args: unknown, ours: CanonValue): void => {
    expect(canonical(ours)).toBe(oracle(PUBLIC_DRIVER, op, args));
  };

  it('agrees on _public_text for the whole edge-case corpus', () => {
    const cases: readonly (readonly [unknown, string, number])[] = [
      ['  hello  ', 'fb', 160],
      ['\u0085nel\u0085', 'fb', 160],
      ['\uFEFFbom', 'fb', 160],
      ['　ideographic　', 'fb', 160],
      ['tab\there', 'fb', 160],
      ['nul\u0000inside', 'fb', 160],
      ['\u0001', 'fb', 160],
      ['\u0001x', 'fb', 160],
      ['\u{1F680}\u{1F680}\u{1F680}\u{1F680}', 'fb', 2],
      ['café naïve 世界', 'fb', 8],
      ['', 'fb', 160],
      [42, 'fb', 160],
      [null, 'fb', 160],
      [['a'], 'fb', 160],
      [true, 'fb', 160],
    ];
    cases.forEach(([value, fallback, limit]) => {
      check('public_text', { value, fallback, limit }, publicText(value, fallback, limit));
    });
  });

  it('agrees on _public_int and _public_number, including the int/float spelling', () => {
    const values: readonly unknown[] = [7, -7, 0, 1.5, true, false, null, '4', [], {}];
    values.forEach((value) => {
      check('public_int', { value, default: 9, minimum: 0 }, publicInt(value, 9n, 0n));
      check('public_int', { value, default: -1, minimum: -1 }, publicInt(value, -1n, -1n));
      check('public_number', { value, default: 2.5 }, publicNumber(value, 2.5));
    });
  });

  it('agrees on _public_event, row by row', () => {
    const rows: readonly unknown[] = [
      EVENT,
      { ...EVENT, weight: 0 },
      { ...EVENT, weight: 100 },
      { ...EVENT, turn: 0 },
      { ...EVENT, kind: 'k'.repeat(60) },
      { ...EVENT, summary: 's'.repeat(300) },
      { ...EVENT, actors: [1, 'a', null, ...Array.from({ length: 10 }, () => 'x')] },
      { ...EVENT, data: [1, 2] },
      { ...EVENT, data: null },
      'not a row',
      [],
    ];
    rows.forEach((value) => {
      check(
        'public_event',
        { value },
        Option.match(publicEvent(value), { onNone: () => null, onSome: (row) => row }),
      );
    });
  });

  it('agrees on the whole _public_events envelope', () => {
    const payloads: readonly unknown[] = [
      { events: [EVENT, { ...EVENT, weight: 3 }, { ...EVENT, weight: 0 }], available: true },
      { events: [], min_included_weight: 42, truncated: true, omitted_counts: { city_founded: 4 } },
      {
        events: [EVENT],
        event_counts: { city_captured: 1, 5: 2 },
        total_events: 99,
        last_turn: 12,
        complete: true,
        event_warnings: [{ turn: 3, message: 'm' }, { message: 'n' }, 'skip'],
      },
      {},
      // No `'not a mapping'` case: `_public_events` is annotated `Mapping` and
      // *raises* `AttributeError` on anything else (`:406`).  The route only
      // ever calls it with a loader result it has already checked (`:1793`),
      // so the port's tolerance there is unreachable rather than divergent.
    ];
    payloads.forEach((value) => {
      check('public_events', { value, game_id: GAME_ID }, publicEvents(value, GAME_ID));
    });
  });

  it('agrees on _public_timing, _public_control_protocol and _public_ai_difficulty', () => {
    const configs: readonly unknown[] = [
      { timing_mode: 'default', action_timeout_s: 180 },
      { timing_mode: 'default', action_timeout_s: 600 },
      { timing_mode: 'blitz', action_timeout_s: 60 },
      { timing_mode: 'infinite', action_timeout_s: null },
      { timing_mode: 'infinite' },
      { timing_mode: 'custom', action_timeout_s: 7.5 },
      { timing_mode: 'custom', action_timeout_s: -1 },
      { timing_mode: 'nope', action_timeout_s: 1 },
      { action_timeout_s: 180 },
      {},
    ];
    configs.forEach((config) => {
      check('public_timing', { config }, { ...publicTiming(config) });
      check(
        'public_control_protocol',
        { config, manifest: { control_protocol: 'strategic-v1' } },
        { ...publicControlProtocol(config, { control_protocol: 'strategic-v1' }) },
      );
    });
    (['hard', 'deity', 'novice', null, 7] as const).forEach((level) => {
      check('public_ai_difficulty', { level }, publicAiDifficulty(level));
    });
  });

  it('agrees on _public_places for every branch of the controller switch', () => {
    const manifests: readonly unknown[] = [
      undefined,
      { config: { difficulty: 'hard' } },
      { config: { difficulty: 'deity' } },
      { config: 'not a dict' },
    ];
    const rows = [
      AGENT_PLACE,
      NATIVE_PLACE,
      { ...NATIVE_PLACE, place: 3, seat_id: 'place-3', ai_difficulty: 'novice' },
      { ...AGENT_PLACE, place: 4, seat_id: 'place-4', joined: false, controller_label: '  ' },
      { ...AGENT_PLACE, place: 5, seat_id: 'place-5', controller_label: 7, model: 'named' },
      { ...AGENT_PLACE, place: 0, seat_id: 'dropped' },
      'not a row',
    ];
    manifests.forEach((manifest) => {
      check('public_places', { value: rows, manifest }, publicPlaces(rows, manifest));
    });
  });
});
