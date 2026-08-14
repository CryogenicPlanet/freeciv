/**
 * The mirror's map and yields writers (U08).
 *
 * Ports `MapTests` from `play/tests/test_state_mirror.py` plus the write-side
 * half of `YieldOverlayTests` (the overlay renderer itself is U09's).  Every
 * golden below is the byte-exact text CPython wrote for the same input: the
 * fixtures were fed to `state_mirror.update_from_page` and the resulting files
 * captured verbatim, so a single space in a grid row is a failing assertion.
 *
 * Beyond CPython's own cases the suite pins the three shapes the brief calls
 * out because they have no CPython test and are pure byte surface: a partially
 * explored board (unproven squares inside the bounding box stay blank), a board
 * with negative coordinates (the `%5d` gutter and the `x -2..0` window), and a
 * wrap-around board whose grid spans the full width read back from
 * `state/overview.tsv`.
 */
import { afterEach, describe, expect, test } from 'bun:test';
import { Effect, Either } from 'effect';
import { renderMap, terrainLegendLine } from 'src/render/mirror/map';
import type { PrivateFs } from 'src/services/private-fs';
import {
  MAP_FILE,
  OVERVIEW_FILE,
  YIELD_FILE,
  mirrorDir,
  parseMap,
  writeMirror,
  type MirrorRevision,
} from 'src/services/mirror';
import { updateMap } from 'src/services/mirror/update-map';
import { updateYields, yieldRows } from 'src/services/mirror/update-yields';
import type { JsonObject } from 'src/schema/primitives';
import { scratchWorkspace, type Scratch } from 'test/_fixtures';
import { effectTest, provideTestLayer } from 'test/_effect-test';
import { path } from 'test/_test-platform';

const GAME_ID = 'game_12345678901234567890';
const CITY_A = `city_${'c'.repeat(32)}`;
const CITY_B = `city_${'e'.repeat(32)}`;
const TILE_A = `tile_${'1'.repeat(32)}`;
const TILE_B = `tile_${'2'.repeat(32)}`;
const TILE_C = `tile_${'3'.repeat(32)}`;

const rev = (revision: number, turn = 3): MirrorRevision => ({ turn, revision });

const byKey = (a: readonly [string, string], b: readonly [string, string]): number =>
  a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0;

const scratches: Scratch[] = [];
afterEach(() =>
  Effect.runPromise(
    Effect.forEach(scratches.splice(0), (scratch) => scratch.cleanup, { discard: true })
  )
);

interface Mirror {
  readonly dir: string;
  readonly run: <A, E>(effect: Effect.Effect<A, E, PrivateFs>) => Effect.Effect<A>;
  readonly attempt: <A, E>(
    effect: Effect.Effect<A, E, PrivateFs>
  ) => Effect.Effect<Either.Either<A, E>>;
  readonly read: (relative: ReadonlyArray<string>) => Effect.Effect<string>;
  readonly exists: (relative: ReadonlyArray<string>) => Effect.Effect<boolean>;
  readonly relative: (absolute: ReadonlyArray<string>) => ReadonlyArray<string>;
}

const freshMirror = (): Effect.Effect<Mirror> =>
  Effect.gen(function* () {
    const scratch = yield* scratchWorkspace();
    scratches.push(scratch);
    const sessionPath = path.join(scratch.workspace.stateRoot, GAME_ID, 'codex-test.json');
    const dir = yield* Effect.orDie(mirrorDir(sessionPath));
    const attempt = <A, E>(
      effect: Effect.Effect<A, E, PrivateFs>
    ): Effect.Effect<Either.Either<A, E>> =>
      Effect.either(provideTestLayer(effect, scratch.layer));
    return {
      dir,
      attempt,
      run: <A, E>(effect: Effect.Effect<A, E, PrivateFs>): Effect.Effect<A> =>
        Effect.orDie(provideTestLayer(effect, scratch.layer)),
      read: (relative) =>
        Effect.orDie(scratch.files.readText(path.join(dir, ...relative), 'mirror')),
      exists: (relative) => scratch.files.exists(path.join(dir, ...relative)),
      relative: (absolute) => absolute.map((item) => path.relative(dir, item)),
    };
  });

// ---------------------------------------------------------------------------
// Fixtures — the CPython test's own payloads
// ---------------------------------------------------------------------------

const tiles = (): ReadonlyArray<JsonObject> => [
  { id: 'tile_1', x: 30, y: 71, visibility: 'visible', terrain: 'Ocean' },
  { id: 'tile_2', x: 31, y: 71, visibility: 'remembered', terrain: 'Desert' },
  { id: 'tile_3', x: 32, y: 71, visibility: 'unknown' },
];

const citizens = (city: string = CITY_A): ReadonlyArray<JsonObject> => [
  {
    city_id: city,
    kind: 'tile',
    tile_id: TILE_A,
    worked: true,
    free_worked: true,
    can_work: true,
    yields: { food: 2, shields: 1, trade: 0, gold: 0, luxury: 0, science: 0 },
  },
  {
    city_id: city,
    kind: 'tile',
    tile_id: TILE_B,
    worked: false,
    free_worked: false,
    can_work: true,
    yields: { food: 1, shields: 0, trade: 3, gold: 0, luxury: 0, science: 0 },
  },
  {
    city_id: city,
    kind: 'specialist',
    id: 'specialist_1',
    name: 'Elvis',
    count: 0,
    counts_toward_population: true,
    can_use: true,
    is_default: true,
    yields: { food: 0, shields: 0, trade: 0, gold: 0, luxury: 2, science: 0 },
  },
];

const TILE_ALIASES = {
  [TILE_A]: 'T(30,71)',
  [TILE_B]: 'T(31,71)',
  [CITY_A]: 'c1',
};

/** The `state/overview.tsv` CPython wrote for an 80x50 wrapping board. */
const OVERVIEW_80x50 =
  '# rev 9 turn 3\n' +
  '# overview 1/1 complete\n' +
  'fact        \tvalue\n' +
  'client_state\trunning\n' +
  'turn        \t3\n' +
  'map         \t80x50\n' +
  'topology    \twrapx|iso\n' +
  'player      \t(none yet)\n' +
  'research    \t(none yet)\n';

// ---------------------------------------------------------------------------
// map.txt
// ---------------------------------------------------------------------------

describe('state/map.txt', () => {
  effectTest('fog renders as a question mark and the legend names what it shows', () => Effect.gen(function* () {
    const mirror = yield* freshMirror();
    const written = yield* mirror.run(updateMap(mirror.dir, 'state', rev(9), tiles()));
    expect(yield* mirror.read(MAP_FILE)).toBe(
      '# rev 9 turn 3\n' +
        '# map size unknown · 2 of 3 tiles known\n' +
        '# window x 30..32 y 71..71\n' +
        "# legend '?'=never seen · UPPERCASE=visible now · lowercase=remembered\n" +
        '# terrain D=Desert · O=Ocean\n' +
        '   71 |Od?\n'
    );
    expect(mirror.relative(written)).toEqual(['state/map.txt', 'state/delta.md']);
  }));

  effectTest('an unknown tile never reveals a volunteered terrain', () => Effect.gen(function* () {
    const mirror = yield* freshMirror();
    const items = tiles().map((item, index) =>
      index === 2 ? { ...item, terrain: 'Grassland' } : item
    );
    yield* mirror.run(updateMap(mirror.dir, 'state', rev(9), items));
    const text = yield* mirror.read(MAP_FILE);
    expect(text.split('\n').filter((line) => line.startsWith('   71'))[0]?.split('|')[1]).toBe(
      'Od?'
    );
    expect(text).not.toContain('Grassland');
  }));

  effectTest('later tiles merge into the grid and report progress', () => Effect.gen(function* () {
    const mirror = yield* freshMirror();
    yield* mirror.run(updateMap(mirror.dir, 'state', rev(9), tiles()));
    yield* mirror.run(
      updateMap(mirror.dir, 'state', rev(10), [
        { id: 'tile_3', x: 32, y: 71, visibility: 'visible', terrain: 'Forest' },
      ])
    );
    expect(yield* mirror.read(MAP_FILE)).toBe(
      '# rev 10 turn 3\n' +
        '# map size unknown · 3 of 3 tiles known\n' +
        '# window x 30..32 y 71..71\n' +
        "# legend '?'=never seen · UPPERCASE=visible now · lowercase=remembered\n" +
        '# terrain D=Desert · F=Forest · O=Ocean\n' +
        '   71 |OdF\n'
    );
    expect(yield* mirror.read(['state', 'delta.md'])).toContain('terrain known: 2 -> 3 tiles');
  }));

  effectTest('terrain codes never change meaning inside one grid', () => Effect.gen(function* () {
    const mirror = yield* freshMirror();
    yield* mirror.run(
      updateMap(mirror.dir, 'state', rev(9), [
        { id: 'tile_1', x: 30, y: 71, visibility: 'visible', terrain: 'Wasteland' },
      ])
    );
    expect(yield* mirror.read(MAP_FILE)).toContain('W=Wasteland');
    yield* mirror.run(
      updateMap(mirror.dir, 'state', rev(10), [
        { id: 'tile_2', x: 31, y: 71, visibility: 'visible', terrain: 'Wetland' },
      ])
    );
    // The legend keeps `W` for the terrain that claimed it; the newcomer takes
    // the first free fallback letter, and the row still opens with `W`.
    expect(yield* mirror.read(MAP_FILE)).toBe(
      '# rev 10 turn 3\n' +
        '# map size unknown · 2 of 2 tiles known\n' +
        '# window x 30..31 y 71..71\n' +
        "# legend '?'=never seen · UPPERCASE=visible now · lowercase=remembered\n" +
        '# terrain W=Wasteland · E=Wetland\n' +
        '   71 |WE\n'
    );
  }));

  effectTest('an empty tiles page says so instead of drawing a grid', () => Effect.gen(function* () {
    const mirror = yield* freshMirror();
    const written = yield* mirror.run(updateMap(mirror.dir, 'state', rev(9), []));
    expect(yield* mirror.read(MAP_FILE)).toBe('# rev 9 turn 3\n# no tiles known yet\n');
    // Nothing changed, so the digest is left alone entirely.
    expect(mirror.relative(written)).toEqual(['state/map.txt']);
    expect(yield* mirror.exists(['state', 'delta.md'])).toBe(false);
  }));

  effectTest('a page older than the mirror is ignored', () => Effect.gen(function* () {
    const mirror = yield* freshMirror();
    yield* mirror.run(updateMap(mirror.dir, 'state', rev(9), tiles()));
    const before = yield* mirror.read(MAP_FILE);
    const written = yield* mirror.run(
      updateMap(mirror.dir, 'state', rev(8), [
        { id: 'tile_9', x: 40, y: 71, visibility: 'visible', terrain: 'Forest' },
      ])
    );
    expect(written).toEqual([]);
    expect(yield* mirror.read(MAP_FILE)).toBe(before);
  }));

  effectTest('a partially explored board leaves unproven squares blank, not fogged', () => Effect.gen(function* () {
    const mirror = yield* freshMirror();
    yield* mirror.run(
      updateMap(mirror.dir, 'state', rev(4, 2), [
        { id: 't1', x: 10, y: 4, visibility: 'visible', terrain: 'Grassland' },
        { id: 't2', x: 13, y: 4, visibility: 'remembered', terrain: 'Hills' },
        { id: 't3', x: 11, y: 6, visibility: 'unknown', terrain: 'Mountains' },
        { id: 't4', x: 13, y: 6, visibility: 'visible', terrain: 'Deep Ocean' },
      ])
    );
    // Row 5 was never covered at all, so it is four spaces — and the line is
    // not right-stripped, which is what keeps column x=13 under column x=13.
    expect(yield* mirror.read(MAP_FILE)).toBe(
      '# rev 4 turn 2\n' +
        '# map size unknown · 3 of 4 tiles known\n' +
        '# window x 10..13 y 4..6\n' +
        "# legend '?'=never seen · UPPERCASE=visible now · lowercase=remembered\n" +
        '# terrain B=Deep Ocean · G=Grassland · H=Hills\n' +
        '    4 |G  h\n' +
        '    5 |    \n' +
        '    6 | ? B\n'
    );
  }));

  effectTest('negative coordinates keep their sign in the gutter and the window', () => Effect.gen(function* () {
    const mirror = yield* freshMirror();
    yield* mirror.run(
      updateMap(mirror.dir, 'state', rev(12, 7), [
        { id: 't1', x: -2, y: -1, visibility: 'visible', terrain: 'Tundra' },
        { id: 't2', x: -1, y: -1, visibility: 'remembered', terrain: 'Jungle' },
        { id: 't3', x: 0, y: 0, visibility: 'visible', terrain: 'Lake' },
        { id: 't4', x: -2, y: 0, visibility: 'unknown' },
      ])
    );
    expect(yield* mirror.read(MAP_FILE)).toBe(
      '# rev 12 turn 7\n' +
        '# map size unknown · 3 of 4 tiles known\n' +
        '# window x -2..0 y -1..0\n' +
        "# legend '?'=never seen · UPPERCASE=visible now · lowercase=remembered\n" +
        '# terrain J=Jungle · L=Lake · T=Tundra\n' +
        '   -1 |Tj \n' +
        '    0 |? L\n'
    );
  }));

  effectTest('a wrap-around board reads its size back out of the overview', () => Effect.gen(function* () {
    const mirror = yield* freshMirror();
    yield* mirror.run(writeMirror(mirror.dir, OVERVIEW_FILE, OVERVIEW_80x50));
    yield* mirror.run(
      updateMap(mirror.dir, 'state', rev(9), [
        { id: 't1', x: 79, y: 25, visibility: 'visible', terrain: 'Plains' },
        { id: 't2', x: 0, y: 25, visibility: 'visible', terrain: 'Swamp' },
      ])
    );
    // Two tiles either side of the seam bound an 80-wide window: the grid is a
    // bounding box in raw coordinates, it does not know the board wraps.
    expect(yield* mirror.read(MAP_FILE)).toBe(
      '# rev 9 turn 3\n' +
        '# map 80x50 · 2 of 2 tiles known\n' +
        '# window x 0..79 y 25..25\n' +
        "# legend '?'=never seen · UPPERCASE=visible now · lowercase=remembered\n" +
        '# terrain P=Plains · S=Swamp\n' +
        `   25 |S${' '.repeat(78)}P\n`
    );
  }));

  effectTest('a coordinate wider than the gutter pushes the grid right', () => Effect.gen(function* () {
    const mirror = yield* freshMirror();
    yield* mirror.run(
      updateMap(mirror.dir, 'state', rev(9), [
        { id: 'a', x: 100000, y: 123456, visibility: 'visible', terrain: 'Ocean' },
        { id: 'b', x: 100001, y: 123457, visibility: 'visible', terrain: 'Ocean' },
      ])
    );
    // CPython's `{y:>5}` pads, it never truncates.
    expect(yield* mirror.read(MAP_FILE)).toBe(
      '# rev 9 turn 3\n' +
        '# map size unknown · 2 of 2 tiles known\n' +
        '# window x 100000..100001 y 123456..123457\n' +
        "# legend '?'=never seen · UPPERCASE=visible now · lowercase=remembered\n" +
        '# terrain O=Ocean\n' +
        '123456 |O \n' +
        '123457 | O\n'
    );
  }));

  effectTest('an overview that carries no map row still says size unknown', () => Effect.gen(function* () {
    const mirror = yield* freshMirror();
    yield* mirror.run(
      writeMirror(
        mirror.dir,
        OVERVIEW_FILE,
        '# rev 9 turn 3\n# overview 1/1 complete\nfact        \tvalue\nclient_state\trunning\n'
      )
    );
    yield* mirror.run(
      updateMap(mirror.dir, 'state', rev(9), [
        { id: 'a', x: 1, y: 1, visibility: 'visible', terrain: 'Ocean' },
      ])
    );
    expect(yield* mirror.read(MAP_FILE)).toContain('# map size unknown · 1 of 1 tiles known');
  }));

  effectTest('a ruleset-invented terrain draws from the fallback alphabet in name order', () => Effect.gen(function* () {
    const mirror = yield* freshMirror();
    yield* mirror.run(
      updateMap(mirror.dir, 'state', rev(9), [
        { id: 'a', x: 0, y: 0, visibility: 'visible', terrain: 'deep water' },
        { id: 'b', x: 1, y: 0, visibility: 'visible', terrain: 'Deep Ocean' },
        { id: 'c', x: 2, y: 0, visibility: 'remembered', terrain: 'desert' },
        { id: 'd', x: 3, y: 0, visibility: 'visible', terrain: '123' },
        { id: 'e', x: 4, y: 0, visibility: 'visible', terrain: 'Ocean' },
      ])
    );
    // Names sort by code point — digits, then uppercase, then lowercase — and
    // `desert` cannot take `D` because `deep water` claimed it first.
    expect(yield* mirror.read(MAP_FILE)).toBe(
      '# rev 9 turn 3\n' +
        '# map size unknown · 5 of 5 tiles known\n' +
        '# window x 0..4 y 0..0\n' +
        "# legend '?'=never seen · UPPERCASE=visible now · lowercase=remembered\n" +
        '# terrain E=123 · B=Deep Ocean · O=Ocean · D=deep water · N=desert\n' +
        '    0 |DBnEO\n'
    );
  }));

  effectTest('the written grid parses back to the grid that was written', () => Effect.gen(function* () {
    const mirror = yield* freshMirror();
    yield* mirror.run(updateMap(mirror.dir, 'state', rev(12, 7), [
      { id: 't1', x: -2, y: -1, visibility: 'visible', terrain: 'Tundra' },
      { id: 't2', x: 0, y: 0, visibility: 'visible', terrain: 'Lake' },
    ]));
    const parsed = parseMap(yield* mirror.read(MAP_FILE));
    expect(parsed.revision).toEqual({ turn: 7, revision: 12 });
    expect([...parsed.grid.entries()].toSorted(byKey)).toEqual([
      ['-2,-1', 'T'],
      ['0,0', 'L'],
    ]);
    expect([...parsed.legend.entries()].toSorted(byKey)).toEqual([
      ['Lake', 'L'],
      ['Tundra', 'T'],
    ]);
  }));

  effectTest('a tile item without coordinates is a refusal, not a blank square', () => Effect.gen(function* () {
    const mirror = yield* freshMirror();
    const either = yield* mirror.attempt(
      updateMap(mirror.dir, 'state', rev(9), [
        { id: 'tile_1', visibility: 'visible', terrain: 'Ocean' },
      ])
    );
    expect(Either.isLeft(either) ? either.left.message : '').toBe(
      'state mirror: tile item carries no coordinates'
    );
    expect(yield* mirror.exists(MAP_FILE)).toBe(false);
  }));
});

describe('renderMap', () => {
  test('the legend is ordered by terrain name and filtered to what shows', () => {
    const legend = new Map([
      ['Ocean', 'O'],
      ['Desert', 'D'],
      ['Arctic', 'A'],
    ]);
    // `d` is Desert remembered through fog, so the filter compares uppercase.
    expect(terrainLegendLine(legend, new Map([['0,0', 'd'], ['1,0', 'O']]))).toBe(
      'D=Desert · O=Ocean'
    );
    expect(terrainLegendLine(legend, new Map([['0,0', '?']]))).toBe('');
  });

  effectTest('an empty grid renders the note and nothing else', () => Effect.gen(function* () {
    const mirror = yield* freshMirror();
    const text = yield* mirror.run(
      renderMap(mirror.dir, rev(9), { revision: null, legend: new Map(), grid: new Map() }, [])
    );
    expect(text).toBe('# rev 9 turn 3\n# no tiles known yet\n');
  }));
});

// ---------------------------------------------------------------------------
// yields.tsv
// ---------------------------------------------------------------------------

describe('state/yields.tsv', () => {
  effectTest('citizen pages price tiles and specialists are skipped', () => Effect.gen(function* () {
    const mirror = yield* freshMirror();
    const written = yield* mirror.run(
      updateYields(mirror.dir, 'state', rev(9), citizens(), TILE_ALIASES)
    );
    expect(mirror.relative(written)).toEqual(['state/yields.tsv', 'state/delta.md']);
    const text = yield* mirror.read(YIELD_FILE);
    expect(text).toBe(
      '# rev 9 turn 3\n' +
        '# yields 2/2 complete\n' +
        'tile    \tfood\tshields\ttrade\tworked\tcity\n' +
        'T(30,71)\t2   \t1      \t0    \tyes   \tc1\n' +
        'T(31,71)\t1   \t0      \t3    \tno    \tc1\n'
    );
    expect(text).not.toContain('Elvis');
    expect(yield* mirror.read(['state', 'delta.md'])).toContain('tile yields known for 2 tiles');
  }));

  effectTest('a tile with no coordinate alias keeps its opaque handle', () => Effect.gen(function* () {
    const mirror = yield* freshMirror();
    yield* mirror.run(updateYields(mirror.dir, 'state', rev(9), citizens(), { [CITY_A]: 'c1' }));
    expect(yield* mirror.read(YIELD_FILE)).toBe(
      '# rev 9 turn 3\n' +
        '# yields 2/2 complete\n' +
        'tile                                 \tfood\tshields\ttrade\tworked\tcity\n' +
        `${TILE_A}\t2   \t1      \t0    \tyes   \tc1\n` +
        `${TILE_B}\t1   \t0      \t3    \tno    \tc1\n`
    );
  }));

  effectTest("a second city's page adds to the overlay rather than replacing it", () => Effect.gen(function* () {
    const mirror = yield* freshMirror();
    yield* mirror.run(updateYields(mirror.dir, 'state', rev(9), citizens(), TILE_ALIASES));
    const second = citizens(CITY_B).map((item, index) =>
      index === 0
        ? { ...item, tile_id: TILE_C }
        : index === 1
          ? { ...item, tile_id: TILE_B, worked: true }
          : item
    );
    yield* mirror.run(
      updateYields(mirror.dir, 'state', rev(9), second, {
        [TILE_C]: 'T(40,10)',
        [TILE_B]: 'T(31,71)',
        [CITY_B]: 'c2',
      })
    );
    // T(30,71) survives from page one, T(31,71) is re-priced in place by page
    // two, and T(40,10) is appended — insertion order, not sorted order.
    expect(yield* mirror.read(YIELD_FILE)).toBe(
      '# rev 9 turn 3\n' +
        '# yields 3/3 complete\n' +
        'tile    \tfood\tshields\ttrade\tworked\tcity\n' +
        'T(30,71)\t2   \t1      \t0    \tyes   \tc1\n' +
        'T(31,71)\t1   \t0      \t3    \tyes   \tc2\n' +
        'T(40,10)\t2   \t1      \t0    \tyes   \tc2\n'
    );
  }));

  effectTest('a page that prices no tile writes nothing at all', () => Effect.gen(function* () {
    const mirror = yield* freshMirror();
    const written = yield* mirror.run(
      updateYields(mirror.dir, 'state', rev(9), [
        {
          city_id: CITY_A,
          kind: 'specialist',
          id: 'specialist_1',
          name: 'Elvis',
          count: 0,
          yields: { food: 0, shields: 0, trade: 0 },
        },
      ])
    );
    expect(written).toEqual([]);
    expect(yield* mirror.exists(YIELD_FILE)).toBe(false);
    expect(yield* mirror.exists(['state', 'delta.md'])).toBe(false);
  }));

  effectTest('a missing yield is a dash and only `worked: true` is `yes`', () => Effect.gen(function* () {
    const mirror = yield* freshMirror();
    yield* mirror.run(
      updateYields(
        mirror.dir,
        'state',
        rev(5, 2),
        [
          { city_id: CITY_A, kind: 'tile', tile_id: TILE_A, worked: true, yields: { food: 2 } },
          {
            city_id: null,
            kind: 'tile',
            tile_id: TILE_B,
            // A truthy string is not `True`: CPython compares by identity.
            worked: 'yes',
            yields: { food: 0, shields: 0, trade: 0 },
          },
          // Neither of these prices a tile: a non-string id and a non-object
          // `yields` are skipped in silence, not refused.
          { city_id: CITY_A, kind: 'tile', tile_id: 17, yields: { food: 1 } },
          { city_id: CITY_A, kind: 'tile', tile_id: 'tile_x', yields: 'nope' },
        ],
        { [TILE_A]: 'T(30,71)' }
      )
    );
    expect(yield* mirror.read(YIELD_FILE)).toBe(
      '# rev 5 turn 2\n' +
        '# yields 2/2 complete\n' +
        'tile                                 \tfood\tshields\ttrade\tworked\tcity\n' +
        `T(30,71)                             \t2   \t-      \t-    \tyes   \t${CITY_A}\n` +
        `${TILE_B}\t0   \t0      \t0    \tno    \t-\n`
    );
  }));

  effectTest('a page older than the overlay is ignored', () => Effect.gen(function* () {
    const mirror = yield* freshMirror();
    yield* mirror.run(updateYields(mirror.dir, 'state', rev(9), citizens(), TILE_ALIASES));
    const before = yield* mirror.read(YIELD_FILE);
    const stale = citizens().map((item, index) =>
      index === 0 ? { ...item, yields: { food: 99, shields: 1, trade: 0 } } : item
    );
    expect(yield* mirror.run(updateYields(mirror.dir, 'state', rev(8), stale, TILE_ALIASES))).toEqual([]);
    expect(yield* mirror.read(YIELD_FILE)).toBe(before);
  }));

  effectTest('a citizen item that is not an object is a refusal', () => Effect.gen(function* () {
    const mirror = yield* freshMirror();
    const either = yield* mirror.attempt(updateYields(mirror.dir, 'state', rev(9), ['oops']));
    expect(Either.isLeft(either) ? either.left.message : '').toBe(
      'state mirror: city citizen item is not an object'
    );
  }));

  effectTest('yieldRows is the projection decision on its own', () => Effect.gen(function* () {
    expect((yield* yieldRows(citizens(), TILE_ALIASES))).toEqual([
      ['T(30,71)', '2', '1', '0', 'yes', 'c1'],
      ['T(31,71)', '1', '0', '3', 'no', 'c1'],
    ]);
  }));

  effectTest('an id naming an Object.prototype member is not an alias', () => Effect.gen(function* () {
    // `tile_id`/`city_id` are unvalidated wire strings: CPython only asks
    // `isinstance(str)`, then looks the id up in a dict, which sees own keys
    // only.  A plain JS property read would walk the prototype chain and spell
    // `toString` as `function toString() { [native code] }` — different bytes,
    // a different column width, and a merge key `show map --yields` can no
    // longer match.
    const hostile = ['toString', 'constructor', '__proto__', 'valueOf', 'hasOwnProperty'];
    const items = hostile.map((id) => ({
      city_id: id,
      kind: 'tile',
      tile_id: id,
      worked: true,
      yields: { food: 1, shields: 1, trade: 1 },
    }));
    expect((yield* yieldRows(items, TILE_ALIASES))).toEqual(
      hostile.map((id) => [id, '1', '1', '1', 'yes', id])
    );
    // No aliases at all is the same answer, and an *own* key still resolves.
    expect((yield* yieldRows(items, null))).toEqual(
      hostile.map((id) => [id, '1', '1', '1', 'yes', id])
    );
    expect((yield* yieldRows(items, { toString: 'T(1,2)' }))).toEqual(
      hostile.map((id) =>
        id === 'toString'
          ? ['T(1,2)', '1', '1', '1', 'yes', 'T(1,2)']
          : [id, '1', '1', '1', 'yes', id]
      )
    );
  }));

  effectTest('a prototype-named tile survives the round trip into state/yields.tsv', () => Effect.gen(function* () {
    const mirror = yield* freshMirror();
    yield* mirror.run(
      updateYields(
        mirror.dir,
        'state',
        rev(9),
        [
          {
            city_id: 'constructor',
            kind: 'tile',
            tile_id: 'toString',
            worked: true,
            yields: { food: 2, shields: 1, trade: 0 },
          },
        ],
        TILE_ALIASES
      )
    );
    expect(yield* mirror.read(YIELD_FILE)).toBe(
      '# rev 9 turn 3\n' +
        '# yields 1/1 complete\n' +
        'tile    \tfood\tshields\ttrade\tworked\tcity\n' +
        'toString\t2   \t1      \t0    \tyes   \tconstructor\n'
    );
  }));
});

// ---------------------------------------------------------------------------
// Both writers against one mirror
// ---------------------------------------------------------------------------

describe('map and yields together', () => {
  effectTest('each writer files its own digest section at one revision', () => Effect.gen(function* () {
    const mirror = yield* freshMirror();
    yield* mirror.run(updateMap(mirror.dir, 'state', rev(9), tiles()));
    yield* mirror.run(updateYields(mirror.dir, 'state', rev(9), citizens(), TILE_ALIASES));
    expect(yield* mirror.read(['state', 'delta.md'])).toBe(
      '# rev 9 turn 3\n' +
        'no earlier mirror · last update: state\n' +
        '\n' +
        '## map\n' +
        '- terrain known: 0 -> 2 tiles\n' +
        '\n' +
        '## yields\n' +
        '- tile yields known for 2 tiles\n'
    );
  }));
});
