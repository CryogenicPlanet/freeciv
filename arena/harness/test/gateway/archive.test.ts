/** Unit coverage for archive projections; route/repository suites own CPython differentials. */

import { readFileSync } from 'node:fs';
import { describe, expect, it } from 'bun:test';
import { Either, Option } from 'effect';
import {
  canonicalText,
  CANON_UTF8,
  type CanonValue,
  decodeFrameIndex,
  decodeGameId,
  type FrameIndex,
  type GameId,
  Gateway,
} from '@arena/wire';
import {
  type ArchivePng,
  archiveFrames,
  archiveOutcome,
  archivePpmPlayers,
  archiveReasons,
  archiveScoreOutcome,
  archiveStatus,
  archiveUrls,
  archiveVictory,
  asInterrupted,
  diskGameRow,
  diskRowsWithInterrupted,
  interruptedCandidates,
  liveGameIds,
  manifestState,
  pairArchiveFrames,
  selectFramePng,
  sortDiskGameRows,
  terminalArchiveView,
} from '../../src/gateway/archive.ts';
import {
  type Canonical,
  isUntrusted,
  publicPlaces,
  type Untrusted,
} from '../../src/gateway/public.ts';
import { parsePythonJson } from '../../src/gateway/python-json.ts';

// The route and repository suites own the CPython differentials.
const FIXTURES = new URL('../../../wire/test/fixtures/runs/', import.meta.url).pathname;

const canonical = (value: CanonValue): string =>
  Either.getOrThrowWith(canonicalText(value, CANON_UTF8), (error) => new Error(String(error)));

const gameId = (value: string): GameId =>
  Either.getOrThrowWith(decodeGameId(value), (error) => new Error(String(error)));

const record = <Value>(value: Value): Untrusted => {
  if (!isUntrusted(value)) throw new Error('fixture is not an object');
  return value;
};

/** A validated frame index, without an assertion. */
const frameIndex = (value: number): FrameIndex =>
  Either.getOrThrowWith(decodeFrameIndex(value), (error) => new Error(String(error)));

const parseJson = (text: string): Option.Option<unknown> =>
  Option.getRight(parsePythonJson(text));

// ---------------------------------------------------------------------------
// The corpus
// ---------------------------------------------------------------------------

const readFixture = (kind: 'manifest' | 'report', name: string): Untrusted =>
  record(JSON.parse(readFileSync(`${FIXTURES}/${kind}/${name}`, 'utf-8')));

const BASE = 'http://127.0.0.1:48261';
const REPO_ROOT = new URL('../../../../', import.meta.url).pathname;

/** CPython's file-backed PPM header projection for one in-memory test case. */
const ppmOracle = (text: string, manifest: Untrusted): string => {
  const source = `
import json, os, sys, tempfile
from pathlib import Path
sys.path.insert(0, ${JSON.stringify(REPO_ROOT)})
from agent_eval.replay_gateway import _archive_ppm_players, _canonical, _public_places
request = json.load(sys.stdin)
with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", newline="", delete=False) as stream:
    stream.write(request["text"])
    path = Path(stream.name)
try:
    places = _public_places(request["manifest"].get("resolved_places"), request["manifest"])
    sys.stdout.buffer.write(_canonical(_archive_ppm_players(path, places)))
finally:
    os.unlink(path)
`;
  const result = Bun.spawnSync(['python3', '-c', source], {
    stdin: Buffer.from(JSON.stringify({ text, manifest }), 'utf8'),
    stderr: 'pipe',
  });
  if (result.exitCode !== 0) {
    throw new Error(`PPM oracle failed: ${result.stderr.toString()}`);
  }
  return result.stdout.toString();
};

// ---------------------------------------------------------------------------
// Behaviour
// ---------------------------------------------------------------------------

describe('manifest state projection', () => {
  it('preserves an unfamiliar sanitized state like Python _public_text', () => {
    expect(manifestState({ state: ' paused\u0000 ' })).toBe('paused');
    expect(manifestState({ status: 'future-state' })).toBe('future-state');
    expect(manifestState({ state: 7 })).toBe('unknown');
  });
});

describe('archiveReasons', () => {
  it('redacts a reason that names a path or a secret, wholesale', () => {
    expect(archiveReasons(['/Users/me/runs/game_x failed'])).toEqual([
      Gateway.REDACTED_INVALID_REASON,
    ]);
    expect(archiveReasons(['owner_token expired'])).toEqual([Gateway.REDACTED_INVALID_REASON]);
    expect(archiveReasons(['SECRET leaked'])).toEqual([Gateway.REDACTED_INVALID_REASON]);
    expect(archiveReasons(['turn 322 timed out'])).toEqual(['turn 322 timed out']);
  });

  it('dedupes after the substitution, so many path reasons collapse to one', () => {
    expect(archiveReasons(['/a', '/b', 'clean', 'clean'])).toEqual([
      Gateway.REDACTED_INVALID_REASON,
      'clean',
    ]);
  });

  it('caps the input at 100 entries and drops what narrows to blank', () => {
    const many = Array.from({ length: 120 }, (_, index) => `reason ${String(index)}`);
    expect(archiveReasons(many)).toHaveLength(100);
    expect(archiveReasons(['', '   ', 7, null])).toEqual([]);
    expect(archiveReasons('not a list')).toEqual([]);
  });
});

/** One leaderboard row, already ranked — the outcome functions' only input. */
const entry = (score: bigint, rank: bigint, label: string): Canonical<Gateway.LeaderboardEntry> => ({
  rank,
  score,
  score_turn: 100n,
  place: rank,
  seat_id: `place-${String(rank)}`,
  player_name: `Player${String(rank)}`,
  player_color: '',
  controller_label: label,
  controller_type: 'external',
  model: null,
  ai_difficulty: null,
});

describe('archiveScoreOutcome', () => {
  it('reports no winner when there is no score snapshot at all', () => {
    expect(archiveScoreOutcome('completed', [])).toEqual({
      status: 'invalid',
      summary: 'No valid winner; no complete score snapshot is available',
      leaders: [],
      margin: null,
      score_turn: null,
    });
  });

  it('names the margin, and drops the suffix when the leader is the only row', () => {
    expect(archiveScoreOutcome('completed', [entry(1096n, 1n, 'winner')]).summary).toBe(
      'winner won',
    );
    expect(
      archiveScoreOutcome('completed', [entry(1096n, 1n, 'winner'), entry(0n, 2n, 'loser')]).summary,
    ).toBe('winner won by 1096');
  });

  it('reads a tie as tied only when the run completed', () => {
    const tied = [entry(9n, 1n, 'a'), { ...entry(9n, 1n, 'b'), place: 2n, seat_id: 'place-2' }];
    expect(archiveScoreOutcome('completed', tied)).toMatchObject({
      status: 'tied',
      summary: 'a and b finished tied',
      margin: 0n,
    });
    expect(archiveScoreOutcome('invalid', tied)).toMatchObject({
      status: 'invalid',
      summary: 'No valid winner; a and b were tied at the last complete score',
      margin: 0n,
    });
  });

  it('phrases a non-completed single leader as "led", never as "won"', () => {
    expect(
      archiveScoreOutcome('invalid', [entry(1096n, 1n, 'winner'), entry(0n, 2n, 'loser')]).summary,
    ).toBe('No valid winner; winner led by 1096 at the last complete score');
  });

  it('appends the victory label only to a won or tied outcome', () => {
    const victory = Option.getOrThrow(
      archiveVictory(Option.some({ victory: 'spacerace', winners: ['w'], turn: 200 })),
    );
    expect(victory.label).toBe('spaceship victory');
    expect(
      archiveOutcome(
        'completed',
        [entry(1096n, 1n, 'winner'), entry(0n, 2n, 'loser')],
        Option.some(victory),
      ).summary,
    ).toBe('winner won by 1096 (spaceship victory)');
    expect(
      archiveOutcome(
        'invalid',
        [entry(1096n, 1n, 'winner'), entry(0n, 2n, 'loser')],
        Option.some(victory),
      ),
    ).toMatchObject({
      summary: 'No valid winner; winner led by 1096 at the last complete score',
      victory,
    });
  });
});

describe('archiveVictory', () => {
  it('echoes an unknown code as its own label and labels turn_limit a score victory', () => {
    expect(
      Option.getOrThrow(archiveVictory(Option.some({ victory: 'brand_new' }))).label,
    ).toBe('brand_new');
    expect(
      Option.getOrThrow(archiveVictory(Option.some({ victory: 'turn_limit' }))).label,
    ).toBe('score victory');
  });

  it('ignores a record that is missing, malformed, or not a victory', () => {
    expect(Option.isNone(archiveVictory(Option.none()))).toBe(true);
    expect(Option.isNone(archiveVictory(parseJson('{not json')))).toBe(true);
    expect(Option.isNone(archiveVictory(Option.some(['not', 'a', 'dict'])))).toBe(true);
    expect(Option.isNone(archiveVictory(Option.some({ victory: '' })))).toBe(true);
    expect(Option.isNone(archiveVictory(Option.some({ victory: 7 })))).toBe(true);
  });

  it('relays turn and year unvalidated, and nulls them rather than omitting', () => {
    const odd = Option.getOrThrow(
      archiveVictory(Option.some({ victory: 'conquest', turn: { nested: true }, winners: [1, 'w'] })),
    );
    expect(odd.turn).toEqual({ nested: true });
    // `record.get("year")` is `None`, and `None` is a value, not an absence.
    expect('year' in odd).toBe(true);
    expect(odd.year).toBeNull();
    expect(odd.winners).toEqual(['w']);
  });

  it('preserves the int spelling the disk reader supplied', () => {
    const victory = Option.getOrThrow(
      archiveVictory(parseJson('{"victory":"turn_limit","turn":753,"year":1.5}')),
    );
    expect(victory.turn).toBe(753n);
    expect(victory.year).toBe(1.5);
    expect(canonical(victory)).toContain('"turn":753,');
  });
});

/** One `watch_frames/NNNNNN.png`, as the listing would report it. */
const png = (index: number): ArchivePng => ({
  index: frameIndex(index),
  name: `${String(index).padStart(6, '0')}.png`,
});

describe('archivePpmPlayers', () => {
  it('matches Python on header boundaries, escaping, and dynamic factions', () => {
    const manifest = readFixture('manifest', 'completed-strategic-v1-multiplayer.json');
    const places = publicPlaces(manifest['resolved_places'], manifest);
    const representative = [
      [
        'P3',
        '# playerno:0:color:( 0, 103,165):name:"AgentPlace1"',
        '# playerno:2:color:(  1,  2,  3):name:"Blackbeard"',
        '# playerno:3:color:(300,  1,  1):name:"OutOfRange"',
        '# playerno:4:color:(  1,  1,  1):name:""',
        '4 4',
        '# playerno:9:color:(1,1,1):name:"TooLate"',
      ].join('\n'),
      '# playerno:0:color:(1,2,3):name:"AgentPlace1"\nP3\n',
      'P3\nnot a comment\n# playerno:0:color:(1,2,3):name:"AgentPlace1"\n',
      '# playerno:0:color:(1,2,3):name:"He said \\"hi\\" \\\\ ok"\n',
      'P3\r\n\r\n# playerno:2:color:(9,9,9):name:"Blackbeard"\r\n',
      '',
      'nothing here at all',
    ] as const;

    representative.forEach((text, index) => {
      expect(canonical(archivePpmPlayers(text, places)), `PPM case ${String(index)}`).toBe(
        ppmOracle(text, manifest),
      );
    });
  });
});

describe('pairArchiveFrames', () => {
  it('pairs the nth PNG with the nth PPM by name, not by turn', () => {
    const pairs = pairArchiveFrames(
      [png(2), png(0), png(1)],
      [
        { turn: 5, name: 'turn-0005-M-b.map.ppm' },
        { turn: 1, name: 'turn-0001-M-a.map.ppm' },
      ],
    );
    expect(pairs.map((pair) => [Number(pair.index), pair.turn])).toEqual([
      [0, 1n],
      [1, 5n],
      [2, null],
    ]);
    expect(pairs[2]?.sourceName).toBe('000002.png');
    expect(pairs[2]?.ppmName).toBeNull();
    expect(pairs[0]?.sourceName).toBe('turn-0001-M-a.map.ppm');
  });

  it('publishes a null latest_png_url exactly when there are no frames', () => {
    const empty = archiveFrames(gameId('game_ieTomdES08hpUmFRFzCOAVMo'), BASE, []);
    expect(empty.latest_png_url).toBeNull();
    expect(empty.frames).toEqual([]);
  });
});

describe('selectFramePng', () => {
  const pngs: readonly ArchivePng[] = [
    { index: frameIndex(0), name: '000000.png' },
    { index: frameIndex(7), name: '000007.png' },
    { index: frameIndex(3), name: '000003.png' },
  ];

  it('resolves latest to the highest index, not the last listed', () => {
    expect(Either.getOrThrow(selectFramePng(pngs, Option.none())).name).toBe('000007.png');
  });

  it('distinguishes an empty archive from a missing index', () => {
    expect(selectFramePng([], Option.none())).toEqual(Either.left('noMapFrames'));
    expect(selectFramePng(pngs, Option.some(frameIndex(9)))).toEqual(
      Either.left('mapFrameDoesNotExist'),
    );
    expect(Either.getOrThrow(selectFramePng(pngs, Option.some(frameIndex(3)))).name).toBe(
      '000003.png',
    );
  });
});

describe('archiveUrls', () => {
  it('keeps watch_url root-relative unless a viewer origin was configured', () => {
    const id = gameId('game_ieTomdES08hpUmFRFzCOAVMo');
    expect(archiveUrls(BASE, id).watch_url).toBe(`/watch/${id}`);
    expect(archiveUrls(BASE, id, true).watch_url).toBe(`${BASE}/watch/${id}`);
    expect(archiveUrls(BASE, id).status_url).toBe(`${BASE}/v1/games/${id}/status`);
  });
});

describe('the interrupted relabel', () => {
  const manifest = readFixture('manifest', 'running-v2-multiplayer.json');
  const row = Option.getOrThrow(diskGameRow(manifest));

  it('drops a run with no recorded turn — a lobby husk has nothing to watch', () => {
    expect(Option.isNone(asInterrupted(row, Option.none()))).toBe(true);
  });

  it('takes the larger of the manifest turn and the replay tail, and quotes it', () => {
    const relabelled = Option.getOrThrow(asInterrupted(row, Option.some(596n)));
    expect(relabelled.state).toBe('interrupted');
    expect(relabelled.current_turn).toBe(596n);
    expect(relabelled.outcome.summary).toContain('turn 596');
    expect(relabelled.outcome.status).toBe('interrupted');
    expect(relabelled.leaderboard).toEqual([]);
    // The manifest's own turn wins when it got further than the telemetry,
    // and the summary quotes the result of the max, not the turn that was read.
    const ahead = Option.getOrThrow(
      diskGameRow({ ...manifest, current_turn: 700 }),
    );
    expect(ahead.current_turn).toBe(700n);
    const behind = Option.getOrThrow(asInterrupted(ahead, Option.some(1n)));
    expect(behind.current_turn).toBe(700n);
    expect(behind.outcome.summary).toContain('turn 700');
    // This fixture records no turn at all, so the telemetry's wins outright.
    expect(row.current_turn).toBeNull();
  });

  it('never relabels a row the supervisor still owns', () => {
    const live = liveGameIds([{ game_id: row.game_id }, 'junk', { game_id: 'short' }]);
    expect(live.has(row.game_id)).toBe(true);
    expect(live.size).toBe(1);
    expect(diskRowsWithInterrupted([row], live, new Map([[row.game_id, 42n]]))).toEqual([]);
    expect(interruptedCandidates([row], live)).toEqual([]);
    expect(interruptedCandidates([row], new Set())).toEqual([row.game_id]);
  });

  it('leaves a terminal row untouched and drops a turnless orphan', () => {
    const terminal = Option.getOrThrow(
      diskGameRow(readFixture('manifest', 'completed-strategic-v1-multiplayer.json')),
    );
    expect(diskRowsWithInterrupted([terminal], new Set(), new Map())).toEqual([terminal]);
    expect(diskRowsWithInterrupted([row], new Set(), new Map())).toEqual([]);
  });
});

describe('sortDiskGameRows', () => {
  it('sorts by (created_at, game_id) descending on both', () => {
    const rows = [
      { created_at: 2, game_id: 'a' },
      { created_at: 3, game_id: 'b' },
      { created_at: 2, game_id: 'c' },
    ];
    expect(sortDiskGameRows(rows).map((row) => row.game_id)).toEqual(['b', 'c', 'a']);
  });
});

describe('the archive schema version', () => {
  it('is wire’s ARCHIVE_SCHEMA_VERSION, in the bigint spelling', () => {
    const id = gameId('game_ieTomdES08hpUmFRFzCOAVMo');
    const view = Either.getOrThrow(
      terminalArchiveView(
        id,
        readFixture('manifest', 'completed-strategic-v1-multiplayer.json'),
        readFixture('report', 'completed-two-seats-full-score.json'),
        Option.none(),
      ),
    );
    expect(archiveStatus(view, BASE).schema_version === BigInt(Gateway.ARCHIVE_SCHEMA_VERSION)).toBe(
      true,
    );
  });
});
