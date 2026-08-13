/**
 * `src/gateway/archive.ts` against its oracle.
 *
 * The differential half is the point of this file.  Every archive payload is
 * built twice — once by this port from a parsed manifest, once by CPython from
 * a **real run directory** the driver materializes in a temp dir — and the two
 * are compared as canonical text (`sort_keys=True`, `(",", ":")`,
 * `ensure_ascii=False`), which is the byte form the gateway actually serves.
 * A `180` where Python wrote `180.0`, a key ordered differently inside a
 * nested object, an omitted `ai_difficulty`, a positional frame pairing that
 * is off by one: all of it fails here rather than in a parity run.
 *
 * The inputs are `@arena/wire`'s run corpus
 * (`arena/wire/test/fixtures/runs/{manifest,report}`) — bytes captured from
 * real matches, not hand-written expectations — plus synthetic runs for the
 * shapes the corpus does not contain (a tie, a dynamic PPM faction, a frame
 * with no autosave, an orphaned run).
 *
 * Every temp directory the driver creates is removed in its own `finally`.
 * Nothing here touches a running stack or the repo's own `.agent-eval/runs`.
 *
 * @module
 */

import { readdirSync, readFileSync } from 'node:fs';
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
  isTerminalRunState,
} from '@arena/wire';
import {
  type ArchiveFrameSource,
  type ArchivePng,
  type ArchivePpm,
  type ArchiveView,
  archiveFrames,
  archiveLeaderboard,
  archiveOutcome,
  archivePpmPlayers,
  archiveReasons,
  archiveResult,
  archiveScoreOutcome,
  archiveStatus,
  archiveUrls,
  archiveVictory,
  archiveWatch,
  asInterrupted,
  diskGameRow,
  diskRowsWithInterrupted,
  gamesIndex,
  interruptedCandidates,
  liveGameIds,
  manifestState,
  pairArchiveFrames,
  selectFramePng,
  sortDiskGameRows,
  terminalArchiveView,
} from '../../src/gateway/archive.ts';
import { type Canonical, isUntrusted, publicPlaces, type Untrusted } from '../../src/gateway/public.ts';
import { parsePythonJson } from '../../src/gateway/python-json.ts';

// ---------------------------------------------------------------------------
// The oracle
// ---------------------------------------------------------------------------

const REPO_ROOT = new URL('../../../../', import.meta.url).pathname;
const FIXTURES = `${REPO_ROOT}arena/wire/test/fixtures/runs`;

/**
 * One `python3 -c` program that materializes a run directory, calls the real
 * `_archive_*` functions against it, prints the canonical answer, and removes
 * the directory in a `finally`.
 *
 * `mkdtemp(...).resolve()` is load-bearing on macOS: `_read_manifest` compares
 * `run_root.resolve().parent` against `runs_root`, and `/var` is a symlink to
 * `/private/var`, so an unresolved temp root makes every lookup a 404.
 */
const ARCHIVE_DRIVER = `
import json, shutil, sys, tempfile
from pathlib import Path
sys.path.insert(0, ${JSON.stringify(REPO_ROOT)})
from agent_eval import replay_gateway as g


def canon(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def build(root, spec):
    run = root / spec["game_id"]
    (run / "watch_frames").mkdir(parents=True, exist_ok=True)
    (run / "saves").mkdir(parents=True, exist_ok=True)
    (run / "manifest.json").write_text(json.dumps(spec["manifest"]), encoding="utf-8")
    if spec.get("report") is not None:
        (run / "report.json").write_text(json.dumps(spec["report"]), encoding="utf-8")
    if spec.get("victory") is not None:
        (run / "victory.json").write_text(spec["victory"], encoding="utf-8")
    for name in spec.get("pngs") or []:
        (run / "watch_frames" / name).write_bytes(b"png")
    for name, text in (spec.get("ppms") or {}).items():
        (run / "saves" / name).write_text(text, encoding="utf-8")
    if spec.get("video"):
        (run / "game.mp4").write_bytes(b"video")
    if spec.get("replay") is not None:
        (run / "replay.jsonl").write_text(spec["replay"], encoding="utf-8")
    return run


request = json.load(sys.stdin)
op, args = request["op"], request["args"]
root = Path(tempfile.mkdtemp(prefix="arena-archive-oracle-")).resolve()
try:
    if op == "run":
        runs = root / "runs"
        runs.mkdir()
        build(runs, args)
        base, absolute = args["base"], args.get("absolute_watch", False)
        try:
            archive = g._terminal_archive(runs, args["game_id"])
        except g.GatewayProblem as exc:
            out = {"problem": str(exc), "status": int(exc.status)}
        else:
            out = {
                "status_body": g._archive_status(archive, base, absolute_watch=absolute),
                "frames": g._archive_frames(archive, base),
                "watch": g._archive_watch(archive, base, absolute_watch=absolute),
                "result": g._archive_result(archive, base, absolute_watch=absolute),
            }
        out["row"] = g._disk_game_row(args["manifest"])
        out["index"] = g._disk_games_index(runs)
    elif op == "index":
        runs = root / "runs"
        runs.mkdir()
        for spec in args["runs"]:
            build(runs, spec)
        out = g._disk_games_index(runs)
    elif op == "interrupted":
        runs = root / "runs"
        runs.mkdir()
        build(runs, args)
        row = g._disk_game_row(args["manifest"])
        out = {
            "turn": g._last_replay_turn(runs, args["game_id"]),
            "row": None if row is None else g._as_interrupted(runs, dict(row)),
        }
    elif op == "ppm":
        path = root / "turn-0001-M-test.map.ppm"
        path.write_text(args["text"], encoding="utf-8")
        out = g._archive_ppm_players(
            path, g._public_places(args.get("places"), args.get("manifest")),
        )
    elif op == "victory":
        if args.get("text") is not None:
            (root / "victory.json").write_text(args["text"], encoding="utf-8")
        out = g._archive_victory(root)
    elif op == "reasons":
        out = g._archive_reasons(args["value"])
    elif op == "leaderboard":
        places = g._public_places(args["manifest"].get("resolved_places"), args["manifest"])
        out = {
            "places": places,
            "leaderboard": g._archive_leaderboard(args["report"], places),
        }
    elif op == "outcome":
        places = g._public_places(args["manifest"].get("resolved_places"), args["manifest"])
        out = g._archive_score_outcome(
            args["state"], g._archive_leaderboard(args["report"], places),
        )
    else:
        raise SystemExit("unknown op: " + op)
    sys.stdout.write(canon(out))
finally:
    shutil.rmtree(root, ignore_errors=True)
`;

/** `_canonical(value).decode("utf-8")` for a value this port produced. */
const canonical = (value: CanonValue): string =>
  Either.getOrThrowWith(canonicalText(value, CANON_UTF8), (error) => new Error(String(error)));

/** Run the driver and return CPython's canonical answer. */
const oracle = (op: string, args: unknown): string => {
  const result = Bun.spawnSync(['python3', '-c', ARCHIVE_DRIVER], {
    cwd: REPO_ROOT,
    stdin: Buffer.from(JSON.stringify({ op, args }), 'utf-8'),
  });
  if (result.exitCode !== 0) {
    throw new Error(`python3 ${op} failed: ${result.stderr.toString()}`);
  }
  return result.stdout.toString();
};

const PYTHON_AVAILABLE = Bun.spawnSync(['python3', '-c', 'import sys']).exitCode === 0;

const gameId = (value: string): GameId =>
  Either.getOrThrowWith(decodeGameId(value), (error) => new Error(String(error)));

const record = (value: unknown): Untrusted => {
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

const fixtureNames = (kind: 'manifest' | 'report'): readonly string[] =>
  readdirSync(`${FIXTURES}/${kind}`)
    .filter((name) => name.endsWith('.json'))
    .toSorted();

/** Every captured report, paired with the manifest the gateway would read. */
const REPORT_RUNS = fixtureNames('report').map((name) => {
  const report = readFixture('report', name);
  const embedded = record(report['manifest']);
  const id = embedded['game_id'];
  const standalone = fixtureNames('manifest')
    .map((file) => readFixture('manifest', file))
    .find((manifest) => manifest['game_id'] === id);
  return { name, report, manifest: standalone ?? embedded, gameId: gameId(String(id)) };
});

const BASE = 'http://127.0.0.1:48261';

// ---------------------------------------------------------------------------
// The port's own composition of the five projections
// ---------------------------------------------------------------------------

/** What the driver's `run` op is handed, and what this port reproduces from. */
interface RunSpec {
  readonly game_id: string;
  readonly manifest: Untrusted;
  readonly report?: Untrusted | null;
  readonly victory?: string | null;
  readonly pngs?: readonly string[];
  readonly ppms?: Readonly<Record<string, string>>;
  readonly video?: boolean;
  readonly replay?: string;
  readonly base?: string;
  readonly absolute_watch?: boolean;
}

/** The archive listing, as the I/O layer would hand it over. */
const listing = (
  spec: RunSpec,
): { readonly pngs: readonly ArchivePng[]; readonly ppms: readonly ArchivePpm[] } => ({
  pngs: (spec.pngs ?? []).flatMap((name) =>
    Either.match(Gateway.decodeArchivePngName(name), {
      onLeft: (): readonly ArchivePng[] => [],
      onRight: (index: FrameIndex) => [{ index, name }],
    }),
  ),
  ppms: Object.keys(spec.ppms ?? {}).flatMap((name) =>
    Option.match(Gateway.archivePpmTurn(name), {
      onNone: (): readonly ArchivePpm[] => [],
      onSome: (turn) => [{ turn, name }],
    }),
  ),
});

/** `_terminal_archive` over a spec, victory record and all. */
const viewOf = (spec: RunSpec): Either.Either<ArchiveView, Gateway.GatewayProblemName> =>
  terminalArchiveView(
    gameId(spec.game_id),
    spec.manifest,
    spec.report ?? {},
    archiveVictory(
      spec.victory === undefined || spec.victory === null
        ? Option.none()
        : parseJson(spec.victory),
    ),
  );

/** The frame rows, PPM headers parsed exactly where the pairing says to. */
const framesOf = (spec: RunSpec, view: ArchiveView): readonly ArchiveFrameSource[] => {
  const { pngs, ppms } = listing(spec);
  return pairArchiveFrames(pngs, ppms).map((pairing) => ({
    ...pairing,
    mapPlayers:
      pairing.ppmName === null
        ? []
        : archivePpmPlayers(spec.ppms?.[pairing.ppmName] ?? '', view.places),
  }));
};

/**
 * `_disk_games_index` (`:1242`) composed from the pure parts.
 *
 * The composition is the contract: a row is built from the manifest, a
 * *terminal* row then has its `leaderboard` and `outcome` replaced from the
 * archive — and is **dropped entirely** when the archive refuses (`:1264`) —
 * and the survivors are sorted.  The I/O layer does the directory walk; this
 * is everything else.
 */
const indexOf = (specs: readonly RunSpec[]): Canonical<Gateway.GamesIndexResponse> => {
  const rows = specs.flatMap((spec) =>
    Option.match(diskGameRow(spec.manifest), {
      onNone: (): readonly Canonical<Gateway.GameRow>[] => [],
      onSome: (row) =>
        isTerminalRunState(row.state)
          ? Either.match(viewOf(spec), {
              onLeft: (): readonly Canonical<Gateway.GameRow>[] => [],
              onRight: (view) => [
                Object.assign({}, row, {
                  leaderboard: view.leaderboard,
                  outcome: view.outcome,
                }),
              ],
            })
          : [row],
    }),
  );
  return gamesIndex(sortDiskGameRows(rows));
};

/** Everything the driver prints for the `run` op, built by this port. */
const runProjections = (spec: RunSpec): CanonValue => {
  const base = spec.base ?? BASE;
  const absolute = spec.absolute_watch ?? false;
  const row = Option.getOrNull(diskGameRow(spec.manifest));
  const index = indexOf([spec]);
  return Either.match(viewOf(spec), {
    onLeft: (problem): CanonValue => ({
      problem: Gateway.GATEWAY_PROBLEM_MESSAGES[problem],
      // The status table is keyed by *message*, not by name.
      status: BigInt(Gateway.GATEWAY_PROBLEM_STATUS[Gateway.GATEWAY_PROBLEM_MESSAGES[problem]]),
      row,
      index,
    }),
    onRight: (view) => {
      const frames = framesOf(spec, view);
      return {
        status_body: archiveStatus(view, base, absolute),
        frames: archiveFrames(view.gameId, base, frames),
        watch: archiveWatch(view, base, frames, spec.video === true, absolute),
        result: archiveResult(view, base, absolute),
        row,
        index,
      };
    },
  });
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
    const view = Either.getOrThrow(
      viewOf({
        game_id: 'game_ieTomdES08hpUmFRFzCOAVMo',
        manifest: readFixture('manifest', 'completed-strategic-v1-multiplayer.json'),
        report: readFixture('report', 'completed-two-seats-full-score.json'),
      }),
    );
    expect(archiveStatus(view, BASE).schema_version === BigInt(Gateway.ARCHIVE_SCHEMA_VERSION)).toBe(
      true,
    );
  });
});

// ---------------------------------------------------------------------------
// Differential against CPython
// ---------------------------------------------------------------------------

/** A PPM header with two configured players and one faction Freeciv invented. */
const PPM_HEADER = [
  'P3',
  '# playerno:0:color:( 0, 103,165):name:"AgentPlace1"',
  '# playerno:1:color:(243,132,  0):name:"AgentPlace2"',
  '# playerno:2:color:(  1,  2,  3):name:"Blackbeard"',
  '# playerno:3:color:(300,  1,  1):name:"OutOfRange"',
  '# playerno:4:color:(  1,  1,  1):name:""',
  '4 4',
  '255',
  '0 0 0  0 0 0',
  '# playerno:9:color:(1,1,1):name:"TooLate"',
  '',
].join('\n');

describe.if(PYTHON_AVAILABLE)('differential: _archive_* in CPython', () => {
  it('agrees on every projection of every captured run', () => {
    REPORT_RUNS.forEach(({ name, report, manifest, gameId: id }) => {
      const spec: RunSpec = {
        game_id: id,
        manifest,
        report,
        base: BASE,
        absolute_watch: false,
      };
      expect(canonical(runProjections(spec)), `report fixture ${name}`).toBe(
        oracle('run', spec),
      );
    });
  });

  it('agrees with the viewer origin configured (absolute watch_url)', () => {
    REPORT_RUNS.forEach(({ name, report, manifest, gameId: id }) => {
      const spec: RunSpec = {
        game_id: id,
        manifest,
        report,
        base: 'https://freeciv.localhost',
        absolute_watch: true,
      };
      expect(canonical(runProjections(spec)), `absolute ${name}`).toBe(oracle('run', spec));
    });
  });

  it('agrees on a run with frames, autosaves, a video and a victory record', () => {
    const spec: RunSpec = {
      game_id: 'game_ieTomdES08hpUmFRFzCOAVMo',
      manifest: readFixture('manifest', 'completed-strategic-v1-multiplayer.json'),
      report: readFixture('report', 'completed-two-seats-full-score.json'),
      victory: JSON.stringify({ victory: 'spacerace', winners: ['AgentPlace1'], turn: 753 }),
      pngs: ['000000.png', '000001.png', '000002.png'],
      ppms: {
        'turn-0001-M-test.map.ppm': PPM_HEADER,
        'turn-0002-M-test.map.ppm': PPM_HEADER,
      },
      video: true,
      base: BASE,
    };
    expect(canonical(runProjections(spec))).toBe(oracle('run', spec));
  });

  it('agrees on a score row carrying private metrics, and publishes none of them', () => {
    const report = readFixture('report', 'completed-two-seats-full-score.json');
    const score = record(report['score']);
    const players = Array.isArray(score['players']) ? score['players'] : [];
    const doctored: Untrusted = {
      ...report,
      episode: '/Users/me/.agent-eval/runs/game_ieTomdES08hpUmFRFzCOAVMo',
      score: {
        ...score,
        players: players.map((player) => {
          const scored = record(player);
          return {
            ...scored,
            metrics: {
              ...record(scored['metrics']),
              secret_token: 999,
              owner_token: 'must-not-leak',
            },
          };
        }),
      },
    };
    const spec: RunSpec = {
      game_id: 'game_ieTomdES08hpUmFRFzCOAVMo',
      manifest: readFixture('manifest', 'completed-strategic-v1-multiplayer.json'),
      report: doctored,
      base: BASE,
    };
    const ours = canonical(runProjections(spec));
    expect(ours).toBe(oracle('run', spec));
    // `PUBLIC_SCORE_METRICS` is an allowlist, and `episode` names the run
    // directory: neither may survive into a spectator body.
    expect(ours).not.toContain('secret_token');
    expect(ours).not.toContain('must-not-leak');
    expect(ours).not.toContain('.agent-eval/runs');
    expect(ours).not.toContain('controller_fingerprint');
  });

  it('agrees on the refusals: a live run, and a report that names another game', () => {
    const live: RunSpec = {
      game_id: 'game_QAoITB7qSmKNSwsXX6LaZG8H',
      manifest: readFixture('manifest', 'running-v2-multiplayer.json'),
      report: readFixture('report', 'completed-two-seats-full-score.json'),
      base: BASE,
    };
    expect(canonical(runProjections(live))).toBe(oracle('run', live));

    const mismatched: RunSpec = {
      game_id: 'game_ieTomdES08hpUmFRFzCOAVMo',
      manifest: readFixture('manifest', 'completed-strategic-v1-multiplayer.json'),
      report: readFixture('report', 'three-players-ranked.json'),
      base: BASE,
    };
    expect(canonical(runProjections(mismatched))).toBe(oracle('run', mismatched));
  });

  it('agrees on a malformed or absent victory.json', () => {
    const texts: readonly (string | null)[] = [
      null,
      '',
      '{not json',
      '["not","a","dict"]',
      '{"victory": ""}',
      '{"victory": "turn_limit", "winners": ["a", 2], "turn": 500, "year": null}',
      '{"victory": "brand_new"}',
    ];
    texts.forEach((text) => {
      const ours = archiveVictory(text === null ? Option.none() : parseJson(text));
      expect(canonical(Option.getOrNull(ours)), `victory ${String(text)}`).toBe(
        oracle('victory', { text }),
      );
    });
  });

  it('agrees on _archive_reasons for the freetext corpus', () => {
    const manifest = readFixture('manifest', 'cancelled-strategic-v1-many-freetext-reasons.json');
    const values: readonly unknown[] = [
      manifest['invalid_reasons'],
      readFixture('manifest', 'invalid-strategic-v1-freetext-reasons.json')['invalid_reasons'],
      ['/Users/x/runs/game_y', 'token=abc', 'PASSWORD', 'plain', 'plain', 'r'.repeat(400)],
      [],
      'not a list',
      Array.from({ length: 120 }, (_, index) => `reason ${String(index)}`),
    ];
    values.forEach((value, index) => {
      expect(canonical(archiveReasons(value)), `reasons case ${String(index)}`).toBe(
        oracle('reasons', { value }),
      );
    });
  });

  it('agrees on the leaderboard join, sort and dense rank', () => {
    REPORT_RUNS.forEach(({ name, report, manifest }) => {
      const places = publicPlaces(manifest['resolved_places'], manifest);
      expect(
        canonical({ places, leaderboard: archiveLeaderboard(report, places) }),
        `leaderboard ${name}`,
      ).toBe(oracle('leaderboard', { manifest, report }));
    });
  });

  it('agrees on the outcome for every state, including the tie', () => {
    const states = ['completed', 'invalid', 'failed'] as const;
    REPORT_RUNS.forEach(({ name, report, manifest }) => {
      const places = publicPlaces(manifest['resolved_places'], manifest);
      const leaderboard = archiveLeaderboard(report, places);
      states.forEach((state) => {
        expect(
          canonical(archiveScoreOutcome(state, leaderboard)),
          `outcome ${name} as ${state}`,
        ).toBe(oracle('outcome', { manifest, report, state }));
      });
    });
  });

  it('agrees on the PPM header scan, matched and dynamic factions alike', () => {
    const manifest = readFixture('manifest', 'completed-strategic-v1-multiplayer.json');
    const places = manifest['resolved_places'];
    const texts: readonly string[] = [
      PPM_HEADER,
      // A header that starts with a player row rather than the magic.
      '# playerno:0:color:(1,2,3):name:"AgentPlace1"\nP3\n',
      // Junk before any player row stops the scan immediately.
      'P3\nnot a comment\n# playerno:0:color:(1,2,3):name:"AgentPlace1"\n',
      // Escaped quotes and backslashes in the name.
      '# playerno:0:color:(1,2,3):name:"He said \\"hi\\" \\\\ ok"\n',
      // Windows line endings, and a blank line before the players.
      'P3\r\n\r\n# playerno:2:color:(9,9,9):name:"Blackbeard"\r\n',
      '',
      'nothing here at all',
    ];
    texts.forEach((text, index) => {
      expect(canonical(archivePpmPlayers(text, publicPlaces(places, manifest))), `ppm ${String(index)}`).toBe(
        oracle('ppm', { text, places, manifest }),
      );
    });
  });

  it('agrees on the multi-run disk index, sort order and all', () => {
    const specs: readonly RunSpec[] = REPORT_RUNS.map(({ report, manifest, gameId: id }) => ({
      game_id: id,
      manifest,
      report,
    }));
    expect(canonical(indexOf(specs))).toBe(oracle('index', { runs: specs }));
  });

  it('agrees on the interrupted relabel, torn replay tail included', () => {
    const manifest = readFixture('manifest', 'running-v2-multiplayer.json');
    const id = String(manifest['game_id']);
    const replays: readonly (string | undefined)[] = [
      undefined,
      '',
      '{"schema_version":1,"turn":44}\n{"schema_version":1,"turn":9',
      '{"schema_version":1,"turn":596}\n',
      '{"schema_version":1,"turn":0}\n',
      '{"schema_version":1}\n{"schema_version":1,"turn":5}\n',
      'not json at all\n',
    ];
    replays.forEach((replay) => {
      const spec: RunSpec = {
        game_id: id,
        manifest,
        ...(replay === undefined ? {} : { replay }),
      };
      const expected = oracle('interrupted', spec);
      const turn = record(JSON.parse(expected))['turn'];
      const ours = {
        turn: typeof turn === 'number' ? BigInt(turn) : null,
        row: Option.getOrNull(
          Option.flatMap(diskGameRow(manifest), (row) =>
            asInterrupted(
              row,
              typeof turn === 'number' ? Option.some(BigInt(turn)) : Option.none(),
            ),
          ),
        ),
      };
      expect(canonical(ours), `replay ${String(replay)}`).toBe(expected);
    });
  });
});
