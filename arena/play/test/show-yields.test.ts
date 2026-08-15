/**
 * `just show map --yields` — the per-tile yield overlay.
 *
 * Ports `test_v2_show_map_yields_reads_two_local_files_and_no_socket` from
 * `play/tests/test_client.py`, plus the shapes `render_map_yields`
 * (`state_mirror.py:901-969`) has that the Python suite only reaches through a
 * live game: the degraded list for a window wider than 24, the "nothing priced
 * yet" remedy and the drifted-header case.
 *
 * Every `GOLDEN` entry is the verbatim stdout of `python3 client.py show map
 * --yields` run against the byte-identical mirror below.  Two local files in,
 * zero sockets: `runShow` requires `SessionStore | PrivateFs` and no more.
 */
import { afterEach, describe, expect, test } from 'bun:test';
import { Effect, Either, Layer } from 'effect';
import { runShow, type ShowOptions } from 'src/commands/show.cmd';
import { renderMapYields, YIELD_TILE_RE, YIELD_WINDOW_MAX } from 'src/render/mirror/yields-overlay';
import { v2StateSchema } from 'src/services/aliases';
import { type PrivateFs } from 'src/services/private-fs';
import { SessionStore, sessionStoreFor, type V2ClientState } from 'src/services/session-store';
import { FIXTURE_AGENT_ID, FIXTURE_GAME_ID, scratchWorkspace, sessionFile, type Scratch } from 'test/_fixtures';
import { captureEffect } from 'test/_capture';
import { effectTest, provideTestLayer } from 'test/_effect-test';
import { observedAt } from 'test/_expect';
import { path, withTestFileSystem } from 'test/_test-platform';

const YIELD_MIRROR: ReadonlyArray<readonly [string, string]> = [
  ["state/map.txt", "# rev 7 turn 3\n# map 64x64 · 12 of 12 tiles known\n# window x 30..33 y 71..73\n# legend '?'=never seen · UPPERCASE=visible now · lowercase=remembered\n# terrain F=Forest · G=Grassland · H=Hills · O=Ocean\n   71 |GGFH\n   72 |GGgh\n   73 |OOOO\n"],
  ["state/yields.tsv", "# rev 7 turn 3\n# yields 3/3 complete\ntile    \tfood\tshields\ttrade\tworked\tcity\nT(30,71)\t3   \t0      \t1    \tno    \t-\nT(31,72)\t2   \t1      \t0    \tyes   \tc1\nT(32,72)\t1   \t2      \t1    \tno    \t-\n"],
];

const WIDE_MIRROR: ReadonlyArray<readonly [string, string]> = [
  ["state/map.txt", "# rev 7 turn 3\n# map 64x64 · 12 of 12 tiles known\n# window x 30..60 y 71..73\n# legend '?'=never seen · UPPERCASE=visible now · lowercase=remembered\n# terrain F=Forest · G=Grassland · H=Hills · O=Ocean\n   71 |GGFHGGGGGGGGGGGGGGGGGGGGGGGGGGG\n   72 |GGgh\n   73 |OOOO\n"],
  ["state/yields.tsv", "# rev 7 turn 3\n# yields 2/2 complete\ntile    \tfood\tshields\ttrade\tworked\tcity\nT(30,71)\t3   \t0      \t1    \tno    \t-\nT(60,71)\t1   \t2      \t1    \tno    \t-\n"],
];

const OTHER_COLUMNS_MIRROR: ReadonlyArray<readonly [string, string]> = [
  ["state/map.txt", "# rev 7 turn 3\n# map 64x64 · 12 of 12 tiles known\n# window x 30..33 y 71..73\n# legend '?'=never seen · UPPERCASE=visible now · lowercase=remembered\n# terrain F=Forest · G=Grassland · H=Hills · O=Ocean\n   71 |GGFH\n   72 |GGgh\n   73 |OOOO\n"],
  ["state/yields.tsv", "# rev 7 turn 3\nalias\tname\nu1\tSettlers\n"],
];

const GOLDEN = {
  "current/yields_grep": {"code": 2, "stdout": "", "stderr": "error: --yields overlays the terrain grid; run `just show map --yields`\n"},
  "current/yields_map": {"code": 0, "stdout": "# rev 7 turn 3\n# yields · 3 tiles priced · window x 30..32 y 71..72\n# cell TERRAIN food/shields/trade · '?' = not read for that tile\n      30     31     32\n   71 |G3/0/1 G?     F?\n   72 |G?     G2/1/0 g1/2/1\n", "stderr": ""},
  "current/yields_map_json": {"code": 0, "stdout": "{\"command\":\"show\",\"lines\":[\"# rev 7 turn 3\",\"# yields \\u00b7 3 tiles priced \\u00b7 window x 30..32 y 71..72\",\"# cell TERRAIN food/shields/trade \\u00b7 '?' = not read for that tile\",\"      30     31     32\",\"   71 |G3/0/1 G?     F?\",\"   72 |G?     G2/1/0 g1/2/1\"],\"schema_version\":1,\"selection\":\"map --yields\"}\n", "stderr": ""},
  "current/yields_units": {"code": 2, "stdout": "", "stderr": "error: --yields overlays the terrain grid; run `just show map --yields`\n"},
  "empty/yields": {"code": 2, "stdout": "", "stderr": "error: this seat has no map projection yet; run `just turn` or `just state --section map_tiles` to write one\n"},
  "nomap/yields": {"code": 2, "stdout": "", "stderr": "error: this seat has no map projection yet; run `just turn` or `just state --section map_tiles` to write one\n"},
  "nopriced/yields": {"code": 0, "stdout": "# rev 7 turn 3\n# no tile yields read yet; price a city's tiles with `just state --section city_citizens --actor_id c1`\n", "stderr": ""},
  "othercolumns/yields": {"code": 0, "stdout": "# rev 7 turn 3\n# no tile yields read yet; price a city's tiles with `just state --section city_citizens --actor_id c1`\n", "stderr": ""},
  "stale/yields_map": {"code": 0, "stdout": "# rev 7 turn 3\n# yields · 3 tiles priced · window x 30..32 y 71..72\n# cell TERRAIN food/shields/trade · '?' = not read for that tile\n      30     31     32\n   71 |G3/0/1 G?     F?\n   72 |G?     G2/1/0 g1/2/1\n", "stderr": ""},
  "wide/yields": {"code": 0, "stdout": "# rev 7 turn 3\n# yields · 2 tiles priced · window x 30..60 y 71..71\n# cell TERRAIN food/shields/trade · '?' = not read for that tile\n30,71 G 3/0/1\n60,71 G 1/2/1\n", "stderr": ""},
} as const;

// ---------------------------------------------------------------------------

const scratches: Scratch[] = [];
afterEach(() =>
  Effect.runPromise(
    Effect.asVoid(Effect.all(scratches.splice(0).map((scratch) => scratch.cleanup)))
  )
);

const clientState = (revision: number): V2ClientState => ({
  schema_version: 5,
  game_id: FIXTURE_GAME_ID,
  agent_id: FIXTURE_AGENT_ID,
  last_revision: { revision, state_token: `token_3_${revision}`, turn: 3 },
  actions: {},
  pending_catalogs: {},
  batches: {},
  receipts: {},
  action_aliases: { state_revision: null, by_alias: {} },
  entity_aliases: {},
  tile_aliases: {},
  drained_actors: [],
});

interface Seat {
  readonly sessionPath: string;
  readonly mirror: string;
  readonly layer: Layer.Layer<SessionStore | PrivateFs>;
}

const seat = (
  files: ReadonlyArray<readonly [string, string]> = YIELD_MIRROR,
  revision = 7
): Effect.Effect<Seat> =>
  Effect.gen(function* () {
    const scratch = yield* scratchWorkspace();
    scratches.push(scratch);
    const home = path.join(scratch.workspace.stateRoot, FIXTURE_GAME_ID);
    const sessionPath = path.join(home, 'codex-test.json');
    const mirror = path.join(home, 'codex-test');
    yield* scratch.files.writeJson(sessionPath, sessionFile());
    yield* scratch.files.writeJson(
      path.join(home, 'codex-test.v2-state'),
      clientState(revision)
    );
    for (const [relative, text] of files) {
      yield* scratch.files.writeText(path.join(mirror, relative), text);
    }
    const store = sessionStoreFor(scratch.workspace, scratch.files, v2StateSchema, {});
    return {
      sessionPath,
      mirror,
      layer: Layer.merge(scratch.layer, Layer.succeed(SessionStore, store)),
    };
  }).pipe(Effect.orDie);

const show = (
  fixture: Seat,
  overrides: Partial<ShowOptions> = {}
): Effect.Effect<{ readonly stdout: string; readonly stderr: string }> =>
  Effect.map(
    captureEffect(
      Effect.either(
        provideTestLayer(
          runShow({
            session: fixture.sessionPath,
            name: 'map',
            grep: '',
            regex: false,
            yields: true,
            json: false,
            ...overrides,
          }),
          fixture.layer
        )
      )
    ),
    ({ value: result, captured }) => ({
      stdout: captured.out.length === 0 ? '' : `${captured.out.join('\n')}\n`,
      stderr: Either.isLeft(result) ? `error: ${result.left.message}\n` : '',
    })
  );

const golden = (
  key: keyof typeof GOLDEN,
  fixture: Effect.Effect<Seat>,
  overrides: Partial<ShowOptions> = {}
): Effect.Effect<void> =>
  Effect.flatMap(fixture, (ready) =>
    Effect.map(show(ready, overrides), (actual) => {
      const expected = GOLDEN[key];
      expect(actual.stdout).toBe(expected.stdout);
      expect(actual.stderr).toBe(expected.stderr);
    })
  );

const overlay = (fixture: Effect.Effect<Seat>): Effect.Effect<ReadonlyArray<string>> =>
  Effect.flatMap(fixture, (ready) =>
    provideTestLayer(renderMapYields(ready.mirror), ready.layer)
  );

const mirrorWithSpan = (span: number): ReadonlyArray<readonly [string, string]> => {
  const row = 'G'.repeat(span);
  const last = 30 + span - 1;
  return [
    ['state/map.txt', `# rev 7 turn 3\n# window x 30..${last} y 71..71\n   71 |${row}\n`],
    [
      'state/yields.tsv',
      '# rev 7 turn 3\ntile\tfood\tshields\ttrade\tworked\tcity\n' +
        `T(30,71)\t3\t0\t1\tno\t-\nT(${last},71)\t1\t2\t1\tno\t-\n`,
    ],
  ];
};

// ---------------------------------------------------------------------------

describe('the grid overlay', () => {
  effectTest('two local files become one priced grid', () => golden('current/yields_map', seat()));

  effectTest('the same lines come back under --json with `map --yields` as the selection', () =>
    golden('current/yields_map_json', seat(), { json: true })
  );

  effectTest('a tile inside the window that was never priced renders `?`, not a zero', () =>
    Effect.map(overlay(seat()), (lines) => {
      // (31,71) is inside the priced bounding box but carries no yields row.
      expect(lines[4]).toBe('   71 |G3/0/1 G?     F?');
      expect(lines.join('\n')).not.toContain('G0/0/0');
    })
  );

  effectTest('a remembered tile keeps its lowercase terrain character', () =>
    Effect.map(overlay(seat()), (lines) => {
      expect(lines[5]).toBe('   72 |G?     G2/1/0 g1/2/1');
    })
  );

  effectTest('the overlay never rewrites either file it read', () =>
    withTestFileSystem((platformFiles) =>
      Effect.gen(function* () {
        const fixture = yield* seat();
        const fixtureMapPath = path.join(fixture.mirror, 'state', 'map.txt');
        const before = yield* platformFiles.readFileString(fixtureMapPath);
        yield* golden('current/yields_map', Effect.succeed(fixture));
        expect(yield* platformFiles.readFileString(fixtureMapPath)).toBe(before);
      })
    ).pipe(Effect.orDie)
  );

  effectTest('the overlay is never prefixed with the staleness banner', () =>
    // `show map` at the same revision *is* banner-eligible; `--yields` returns
    // before the banner is ever computed, exactly like CPython.
    golden('stale/yields_map', seat(YIELD_MIRROR, 12))
  );
});

describe('the degraded shapes', () => {
  effectTest('a window wider than 24 columns prints one line per priced tile', () =>
    golden('wide/yields', seat(WIDE_MIRROR))
  );

  effectTest('the threshold is inclusive: exactly 24 columns still prints a grid', () =>
    Effect.gen(function* () {
      const inclusive = yield* overlay(seat(mirrorWithSpan(YIELD_WINDOW_MAX)));
      const degraded = yield* overlay(seat(mirrorWithSpan(YIELD_WINDOW_MAX + 1)));
      expect(inclusive[3]).toContain('  30 ');
      expect(degraded[3]).toBe('30,71 G 3/0/1');
    })
  );

  effectTest('a mirror with no yields file names the command that prices tiles', () =>
    golden('nopriced/yields', seat([observedAt(YIELD_MIRROR, 0)]))
  );

  effectTest('a yields table whose header drifted is ignored rather than misread', () =>
    golden('othercolumns/yields', seat(OTHER_COLUMNS_MIRROR))
  );

  effectTest('no map projection at all is a refusal that names both writers', () =>
    Effect.gen(function* () {
      yield* golden('nomap/yields', seat([observedAt(YIELD_MIRROR, 1)]));
      yield* golden('empty/yields', seat([]));
    })
  );

  effectTest('an empty overlay is an empty list, which is not an empty grid', () =>
    Effect.gen(function* () {
      expect(yield* overlay(seat([]))).toEqual([]);
      expect(yield* overlay(seat([observedAt(YIELD_MIRROR, 0)]))).toHaveLength(2);
    })
  );
});

describe('--yields anywhere else', () => {
  effectTest('it refuses on another section and on --grep', () =>
    Effect.gen(function* () {
      yield* golden('current/yields_units', seat(), { name: 'units' });
      yield* golden('current/yields_grep', seat(), { name: '', grep: 'Settlers' });
      yield* golden('current/yields_units', seat(), { name: '' });
    })
  );

  effectTest('`map` plus --grep is the --yields refusal, not the "not both" one', () =>
    golden('current/yields_grep', seat(), { name: '', grep: 'G' })
  );
});

describe('the tile handle', () => {
  test('only a well-formed `T(x,y)` with at most four digits is priced', () => {
    expect(YIELD_TILE_RE.test('T(31,72)')).toBe(true);
    expect(YIELD_TILE_RE.test('T(-1,-2)')).toBe(true);
    expect(YIELD_TILE_RE.test('T(12345,1)')).toBe(false);
    expect(YIELD_TILE_RE.test('t(1,2)')).toBe(false);
    expect(YIELD_TILE_RE.test('T(1, 2)')).toBe(false);
    expect(YIELD_TILE_RE.test('u1')).toBe(false);
  });
});
