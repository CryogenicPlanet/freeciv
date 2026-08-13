/**
 * `RunsRepository` against a real `runs_root`, with CPython as the oracle.
 *
 * The tree is built from `arena/wire/test/fixtures/runs` — the same archived
 * manifests and reports the wire schemas were transcribed from — plus the
 * synthetic runs no capture contains: a symlinked run directory, a symlinked
 * `manifest.json`, a torn `replay.jsonl` tail, a lobby husk with an empty one,
 * and an oversize manifest.
 *
 * The differential is the point.  `python3 -c` imports
 * `agent_eval.replay_gateway` and calls `_disk_games_index`,
 * `_last_replay_turn`, `_read_manifest` and `_terminal_archive` on the *same
 * directory*, then canonicalizes with the gateway's own `_canonical`.  This
 * file compares those bytes to `canonicalText` over the rows this port builds,
 * so every field, every coercion and the sort order are pinned at once rather
 * than one `expect` at a time.  A projection this port gets subtly wrong —
 * `created_at` spelled `0` instead of `0.0`, a place key present instead of
 * omitted, a dense rank off by one — fails here and nowhere else.
 */
import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { Effect, Either, Option } from 'effect';
import {
  canonicalText,
  CANON_UTF8,
  type CanonValue,
  decodeJsonValueFromString,
  FrameIndex,
  isJsonObject,
  type JsonObject,
} from '@arena/wire';
import {
  constants,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  realpathSync,
  rmSync,
  symlinkSync,
  writeFileSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { gamesIndex } from 'src/gateway/archive';
import { MAX_PROXY_JSON_BYTES } from 'src/gateway/constants';
import {
  makeRunsRepository,
  O_CLOEXEC,
  O_NOFOLLOW,
  O_RDONLY,
  type RunsRepositoryApi,
} from 'src/gateway/services/runs';

// ---------------------------------------------------------------------------
// Fixture tree
// ---------------------------------------------------------------------------

const REPO_ROOT = resolve(import.meta.dir, '../../../..');
const FIXTURES = join(REPO_ROOT, 'arena/wire/test/fixtures/runs');

/** Ids that exist in the fixture corpus with a manifest *and* a matching report. */
const COMPLETED = 'game_ieTomdES08hpUmFRFzCOAVMo';
const FAILED = 'game_ssp8cKCX7WHao84FuVTVkmuQ';
const CANCELLED = 'game_agU2q3kM9-grVcw2FhcLbXfy';
const CANCELLED_RANKED = 'game_ZpZ7pkuWmi__HVqsPqHxhIvq';
const INVALID = 'game_Hsit9YEuBjKdJPPouFoGVYlk';
const RUNNING = 'game_QAoITB7qSmKNSwsXX6LaZG8H';
/** Terminal, but its `report.json` is missing: `_disk_games_index` drops it. */
const NO_REPORT = 'game_dtEjWTQyn61AJ4ua9kZj0mV8';
/** Synthetic ids — 20-80 chars of `[A-Za-z0-9_-]`, per `GAME_ID_RE`. */
const HUSK_EMPTY_REPLAY = 'game_huskEmptyReplay0001';
const HUSK_NO_REPLAY = 'game_huskNoReplayFile0001';
const BROKEN_MANIFEST = 'game_brokenManifest000001';
const SYMLINKED_RUN = 'game_symlinkedRunDir00001';
const OVERSIZE_MANIFEST = 'game_oversizeManifest0001';
const ID_MISMATCH = 'game_manifestIdMismatch01';
const SYMLINKED_MANIFEST = 'game_symlinkedManifest001';
const REPORT_MISMATCH = 'game_reportIdMismatch0001';

/** A fixture document, decoded without ever holding an `any`. */
const readFixture = (kind: string, name: string): JsonObject => {
  const document = decodeJsonValueFromString(
    readFileSync(join(FIXTURES, kind, `${name}.json`), 'utf8'),
  );
  if (Either.isLeft(document) || !isJsonObject(document.right)) {
    throw new Error(`fixture ${kind}/${name}.json is not a JSON object`);
  }
  return document.right;
};

const writeJson = (path: string, value: unknown): void => {
  writeFileSync(path, `${JSON.stringify(value)}\n`, 'utf8');
};

interface RunSpec {
  readonly id: string;
  readonly manifest: string;
  readonly report?: string;
  readonly reportGameId?: string;
  readonly replay?: string;
  readonly frames?: ReadonlyArray<string>;
  readonly video?: boolean;
}

const writeRun = (root: string, spec: RunSpec): void => {
  const directory = join(root, spec.id);
  mkdirSync(directory, { recursive: true });
  writeJson(join(directory, 'manifest.json'), {
    ...readFixture('manifest', spec.manifest),
    game_id: spec.id,
  });
  if (spec.report !== undefined) {
    const report = readFixture('report', spec.report);
    const manifest = report['manifest'];
    writeJson(join(directory, 'report.json'), {
      ...report,
      manifest: {
        ...(isJsonObject(manifest) ? manifest : {}),
        game_id: spec.reportGameId ?? spec.id,
      },
    });
  }
  if (spec.replay !== undefined) {
    writeFileSync(join(directory, 'replay.jsonl'), spec.replay, 'utf8');
  }
  if (spec.frames !== undefined) {
    const frames = join(directory, 'watch_frames');
    mkdirSync(frames, { recursive: true });
    spec.frames.forEach((name, index) =>
      writeFileSync(join(frames, name), `png-${String(index)}`, 'utf8'),
    );
  }
  if (spec.video === true) {
    writeFileSync(join(directory, 'game.mp4'), 'archive-video', 'utf8');
  }
};

/**
 * A torn tail: the last line is an unterminated write, so the scan must walk
 * back past it to turn 44 (`test_replay_gateway.py:950`).
 */
const TORN_REPLAY = [
  '{"schema_version":1,"turn":41,"kind":"turn"}',
  '',
  '{"schema_version":1,"turn":44,"kind":"turn"}',
  '{"schema_version":1,"turn":9',
].join('\n');

const buildTree = (): string => {
  // `realpath` because macOS's `/var/folders/...` is itself a symlink, and the
  // Python oracle is handed this path directly rather than through
  // `gateway_config`, which would have resolved it.
  const root = realpathSync(mkdtempSync(join(tmpdir(), 'arena-runs-')));
  writeRun(root, {
    id: COMPLETED,
    manifest: 'completed-strategic-v1-multiplayer',
    report: 'completed-two-seats-full-score',
    frames: ['000000.png', '000001.png', '000007.png'],
    video: true,
  });
  // A frame that is a link to a secret, and a frame that is empty: both are
  // invisible to `_archive_regular_files`, and `latest.png` stays 000007.
  writeFileSync(join(root, COMPLETED, 'auth.json'), '{"token":"must-not-leak"}', 'utf8');
  symlinkSync(
    join(root, COMPLETED, 'auth.json'),
    join(root, COMPLETED, 'watch_frames', '000002.png'),
  );
  writeFileSync(join(root, COMPLETED, 'watch_frames', '000003.png'), '', 'utf8');
  // The engine's game-over record: its label is appended to a `won` summary.
  writeJson(join(root, COMPLETED, 'victory.json'), {
    schema_version: 1,
    victory: 'spacerace',
    winners: ['AgentPlace1'],
    turn: 752,
    year: 1900,
  });
  writeRun(root, {
    id: FAILED,
    manifest: 'failed-strategic-v1-three-places',
    report: 'empty-score-no-recovery',
  });
  writeRun(root, {
    id: CANCELLED,
    manifest: 'cancelled-v2-never-started-recovery',
    report: 'empty-score-with-recovery',
  });
  writeRun(root, {
    id: CANCELLED_RANKED,
    manifest: 'cancelled-strategic-v1-many-freetext-reasons',
    report: 'three-players-ranked',
  });
  // An unrecognized code is echoed as its own label, and this run's outcome is
  // `invalid`, so the label is reported but never spliced into the summary.
  writeJson(join(root, CANCELLED_RANKED, 'victory.json'), {
    victory: 'brand_new',
    winners: ['AgentPlace1', 7],
    turn: 332,
  });
  writeRun(root, {
    id: INVALID,
    manifest: 'invalid-v2-score-snapshot-incomplete',
    report: 'dead-player-alive-false',
  });
  // A malformed record is silently no record at all.
  writeFileSync(join(root, INVALID, 'victory.json'), '{not json', 'utf8');
  writeRun(root, {
    id: RUNNING,
    manifest: 'running-v2-multiplayer',
    replay: TORN_REPLAY,
  });
  writeRun(root, { id: NO_REPORT, manifest: 'completed-strategic-v1-single-native-seat' });
  writeRun(root, {
    id: HUSK_EMPTY_REPLAY,
    manifest: 'running-v2-multiplayer',
    replay: '',
  });
  writeRun(root, { id: HUSK_NO_REPLAY, manifest: 'running-v2-multiplayer' });
  writeRun(root, {
    id: REPORT_MISMATCH,
    manifest: 'completed-strategic-v1-multiplayer',
    report: 'completed-two-seats-full-score',
    reportGameId: COMPLETED,
  });

  // A manifest that is not JSON at all.
  mkdirSync(join(root, BROKEN_MANIFEST), { recursive: true });
  writeFileSync(join(root, BROKEN_MANIFEST, 'manifest.json'), '{broken', 'utf8');

  // A manifest naming a different game than the directory it sits in.
  mkdirSync(join(root, ID_MISMATCH), { recursive: true });
  writeJson(join(root, ID_MISMATCH, 'manifest.json'), {
    ...readFixture('manifest', 'running-v2-multiplayer'),
    game_id: COMPLETED,
  });

  // A run whose `manifest.json` is a symlink: `O_NOFOLLOW` must refuse it.
  mkdirSync(join(root, SYMLINKED_MANIFEST), { recursive: true });
  symlinkSync(
    join(root, COMPLETED, 'manifest.json'),
    join(root, SYMLINKED_MANIFEST, 'manifest.json'),
  );

  // A manifest one byte over the 8 MiB ceiling.
  mkdirSync(join(root, OVERSIZE_MANIFEST), { recursive: true });
  writeFileSync(
    join(root, OVERSIZE_MANIFEST, 'manifest.json'),
    `{"game_id":"${OVERSIZE_MANIFEST}","config":{},"padding":"${'x'.repeat(
      MAX_PROXY_JSON_BYTES,
    )}"}`,
    'utf8',
  );

  // A run directory that is a symlink to another run.
  symlinkSync(join(root, COMPLETED), join(root, SYMLINKED_RUN));

  // A directory that is not a game id at all.
  mkdirSync(join(root, 'not-a-game-id'), { recursive: true });
  return root;
};

// ---------------------------------------------------------------------------
// The CPython oracle
// ---------------------------------------------------------------------------

const ORACLE = `
import json, os, sys
sys.path.insert(0, os.environ["ARENA_REPO_ROOT"])
from pathlib import Path
from agent_eval.replay_gateway import (
    GatewayProblem, TERMINAL_STATES, _archive_frame_path, _archive_video_path,
    _as_interrupted, _canonical, _disk_games_index, _last_replay_turn,
    _read_manifest, _terminal_archive,
)

runs = Path(os.environ["ARENA_RUNS_ROOT"])
live = set(json.loads(os.environ["ARENA_LIVE_IDS"]))
probe_ids = json.loads(os.environ["ARENA_PROBE_IDS"])


def rows_with_interrupted():
    rows = []
    for row in _disk_games_index(runs)["games"]:
        if row["game_id"] in live:
            continue
        if row["state"] not in TERMINAL_STATES:
            row = _as_interrupted(runs, row)
            if row is None:
                continue
        rows.append(row)
    return rows


def probe(reader, game_id):
    try:
        reader(runs, game_id)
    except GatewayProblem as exc:
        return {"status": int(exc.status), "message": str(exc)}
    return {"status": 200, "message": "ok"}


def probe_binary(game_id, index):
    try:
        archive = _terminal_archive(runs, game_id)
        path = (
            _archive_video_path(archive) if index == "video"
            else _archive_frame_path(archive, index)
        )
    except GatewayProblem as exc:
        return {"status": int(exc.status), "message": str(exc)}
    return {"status": 200, "message": str(path)}


sys.stdout.write(json.dumps({
    "index": _canonical(_disk_games_index(runs)).decode("utf-8"),
    "terminal_only": _canonical(
        _disk_games_index(runs, terminal_only=True)
    ).decode("utf-8"),
    "interrupted": _canonical(
        {"schema_version": 1, "games": rows_with_interrupted()}
    ).decode("utf-8"),
    "last_turn": {gid: _last_replay_turn(runs, gid) for gid in probe_ids},
    "manifest": {gid: probe(_read_manifest, gid) for gid in probe_ids},
    "archive": {gid: probe(_terminal_archive, gid) for gid in probe_ids},
    "binary": {
        f"{gid}:{index}": probe_binary(gid, None if index == "latest" else index)
        for gid, index in json.loads(os.environ["ARENA_BINARY_PROBES"])
    },
}))
`;

interface OracleProbe {
  readonly status: number;
  readonly message: string;
}

interface OracleResult {
  readonly index: string;
  readonly terminal_only: string;
  readonly interrupted: string;
  readonly last_turn: Record<string, number | null>;
  readonly manifest: Record<string, OracleProbe>;
  readonly archive: Record<string, OracleProbe>;
  readonly binary: Record<string, OracleProbe>;
}

/** `[game id, frame index | "latest" | "video"]`, as the oracle keys them. */
type BinaryProbe = readonly [string, number | 'latest' | 'video'];

const runOracle = (
  root: string,
  liveIds: ReadonlyArray<string>,
  probeIds: ReadonlyArray<string>,
  binaryProbes: ReadonlyArray<BinaryProbe> = [],
): OracleResult => {
  const result = Bun.spawnSync(['python3', '-c', ORACLE], {
    stdout: 'pipe',
    stderr: 'pipe',
    env: {
      ...process.env,
      ARENA_REPO_ROOT: REPO_ROOT,
      ARENA_RUNS_ROOT: root,
      ARENA_LIVE_IDS: JSON.stringify(liveIds),
      ARENA_PROBE_IDS: JSON.stringify(probeIds),
      ARENA_BINARY_PROBES: JSON.stringify(binaryProbes),
    },
  });
  if (result.exitCode !== 0) {
    throw new Error(`oracle failed: ${result.stderr.toString()}`);
  }
  // The one assertion in this file: a subprocess's stdout is `any` by
  // construction, and this is the boundary where it becomes typed.
  return JSON.parse(result.stdout.toString()) as OracleResult;
};

/**
 * A decoded row as a {@link CanonValue}.
 *
 * Absent optional keys are dropped — that is what "omitted, not null" means on
 * the wire — and anything this port should never produce becomes a poison
 * string so a differential fails loudly instead of comparing equal.
 */
const toCanon = (value: unknown): CanonValue => {
  if (
    value === null ||
    typeof value === 'boolean' ||
    typeof value === 'string' ||
    typeof value === 'number' ||
    typeof value === 'bigint'
  ) {
    return value;
  }
  if (Array.isArray(value)) {
    return value.map(toCanon);
  }
  if (typeof value === 'object') {
    return Object.fromEntries(
      Object.entries(value)
        .filter(([, entry]) => entry !== undefined)
        .map(([key, entry]) => [key, toCanon(entry)]),
    );
  }
  return '<not a JSON value>';
};

const canonical = (value: unknown): string =>
  Either.getOrElse(
    canonicalText(toCanon(value), CANON_UTF8),
    (error) => `<canon error: ${String(error)}>`,
  );

const run = <A, E>(effect: Effect.Effect<A, E>): A => Effect.runSync(effect);

const failure = <A>(effect: Effect.Effect<A, { readonly message: string; readonly status: number }>): {
  readonly status: number;
  readonly message: string;
} => {
  const result = Effect.runSync(Effect.either(effect));
  if (Either.isRight(result)) {
    return { status: 200, message: 'ok' };
  }
  return { status: result.left.status, message: result.left.message };
};

// ---------------------------------------------------------------------------

describe('RunsRepository', () => {
  const state: { root: string; repo: RunsRepositoryApi } = {
    root: '',
    repo: makeRunsRepository('/nonexistent'),
  };

  beforeAll(() => {
    state.root = buildTree();
    state.repo = makeRunsRepository(state.root);
  });

  afterAll(() => {
    if (state.root !== '') {
      rmSync(state.root, { recursive: true, force: true });
    }
  });

  describe('open(2) flags', () => {
    test('O_RDONLY and O_NOFOLLOW agree with node:fs on this platform', () => {
      expect(O_RDONLY).toBe(constants.O_RDONLY);
      expect(O_NOFOLLOW).toBe(constants.O_NOFOLLOW);
      expect(O_NOFOLLOW).toBe(process.platform === 'linux' ? 0x0002_0000 : 0x0000_0100);
    });

    test('O_CLOEXEC is hardcoded because Bun does not define it', () => {
      expect(Object.hasOwn(constants, 'O_CLOEXEC')).toBe(false);
      expect(O_CLOEXEC).toBe(process.platform === 'linux' ? 0x0008_0000 : 0x0100_0000);
    });
  });

  describe('readManifest', () => {
    test('reads the manifest of a real run', () => {
      const manifest = run(state.repo.readManifest(COMPLETED));
      expect(manifest['game_id']).toBe(COMPLETED);
      expect(manifest['state']).toBe('completed');
    });

    test('preserves a Python integer beyond Number.MAX_SAFE_INTEGER', () => {
      const root = realpathSync(mkdtempSync(join(tmpdir(), 'arena-manifest-int-')));
      const id = 'game_manifestLargeInt000001';
      mkdirSync(join(root, id), { recursive: true });
      writeFileSync(
        join(root, id, 'manifest.json'),
        `{"game_id":"${id}","counter":9007199254740993}\n`,
        'utf8',
      );
      const manifest = run(makeRunsRepository(root).readManifest(id));
      expect(manifest['counter']).toBe(9_007_199_254_740_993n);
      rmSync(root, { recursive: true, force: true });
    });

    test('decodeManifest runs the strict wire schema over the same bytes', () => {
      const manifest = Effect.runSync(
        Effect.either(state.repo.decodeManifest(COMPLETED)),
      );
      expect(Either.isRight(manifest)).toBe(true);
      if (Either.isRight(manifest)) {
        expect(String(manifest.right.game_id)).toBe(COMPLETED);
        expect(manifest.right.current_turn).toBe(752n);
      }
    });

    test('the four 404 gates and the two 503s', () => {
      expect(failure(state.repo.readManifest('short'))).toEqual({
        status: 404,
        message: 'game not found',
      });
      expect(failure(state.repo.readManifest(`${COMPLETED}%2Fstatus`))).toEqual({
        status: 404,
        message: 'game not found',
      });
      expect(failure(state.repo.readManifest(SYMLINKED_RUN))).toEqual({
        status: 404,
        message: 'game not found',
      });
      expect(failure(state.repo.readManifest(ID_MISMATCH))).toEqual({
        status: 404,
        message: 'game not found',
      });
      expect(failure(state.repo.readManifest('game_doesNotExistAtAll01'))).toEqual({
        status: 404,
        message: 'game not found',
      });
      expect(failure(state.repo.readManifest(BROKEN_MANIFEST))).toEqual({
        status: 503,
        message: 'game manifest is unavailable',
      });
      expect(failure(state.repo.readManifest(OVERSIZE_MANIFEST))).toEqual({
        status: 503,
        message: 'game manifest is unavailable',
      });
    });

    test('O_NOFOLLOW refuses a symlinked manifest.json', () => {
      // The link points at a manifest that would otherwise read perfectly;
      // without the flag this returns the linked run's document.
      expect(failure(state.repo.readManifest(SYMLINKED_MANIFEST))).toEqual({
        status: 404,
        message: 'game manifest not found',
      });
    });

    test('a run directory with no manifest.json is a 404, not a 503', () => {
      const empty = join(state.root, 'game_noManifestAtAll00001');
      mkdirSync(empty, { recursive: true });
      expect(failure(state.repo.readManifest('game_noManifestAtAll00001'))).toEqual({
        status: 404,
        message: 'game manifest not found',
      });
      rmSync(empty, { recursive: true, force: true });
    });
  });

  describe('terminalArchive', () => {
    test('a completed run carries its leaderboard and outcome', () => {
      const archive = run(state.repo.terminalArchive(COMPLETED));
      expect(archive.state).toBe('completed');
      expect(archive.benchmarkValid).toBe(true);
      expect(archive.leaderboard.length).toBe(2);
      expect(archive.leaderboard[0]?.rank).toBe(1n);
      expect(archive.outcome.status).toBe('won');
      expect(archive.places.map((place) => place.seat_id)).toEqual(['place-1', 'place-2']);
    });

    test('a live run has no archive', () => {
      expect(failure(state.repo.terminalArchive(RUNNING))).toEqual({
        status: 404,
        message: 'terminal archive not found',
      });
    });

    test('a missing report is a 404 naming the report', () => {
      expect(failure(state.repo.terminalArchive(NO_REPORT))).toEqual({
        status: 404,
        message: 'game report not found',
      });
    });

    test('a report naming another game is not this run archive', () => {
      expect(failure(state.repo.terminalArchive(REPORT_MISMATCH))).toEqual({
        status: 404,
        message: 'terminal archive not found',
      });
    });
  });

  describe('lastReplayTurn', () => {
    test('walks back past a torn final line', () => {
      expect(run(state.repo.lastReplayTurn(RUNNING))).toEqual(Option.some(44n));
    });

    test('an empty or absent replay.jsonl has no turn', () => {
      expect(run(state.repo.lastReplayTurn(HUSK_EMPTY_REPLAY))).toEqual(Option.none());
      expect(run(state.repo.lastReplayTurn(HUSK_NO_REPLAY))).toEqual(Option.none());
      expect(run(state.repo.lastReplayTurn('game_doesNotExistAtAll01'))).toEqual(
        Option.none(),
      );
    });

    test('the scan stops at the first line that parses', () => {
      const root = realpathSync(mkdtempSync(join(tmpdir(), 'arena-tail-')));
      const write = (id: string, body: string): void => {
        mkdirSync(join(root, id), { recursive: true });
        writeFileSync(join(root, id, 'replay.jsonl'), body, 'utf8');
      };
      const repo = makeRunsRepository(root);
      // A parseable row with no `turn` ends the scan: turn 7 is NOT found.
      write('game_stopsAtTurnlessRow01', '{"turn":7}\n{"kind":"end"}\n');
      // A non-object row ends it too.
      write('game_stopsAtNonObjectRow1', '{"turn":7}\n[1,2,3]\n');
      // Blank lines are skipped, not treated as unparseable.
      write('game_skipsBlankTrailing001', '{"turn":7}\n\n   \n');
      // Zero and negative turns are not turns.
      write('game_zeroTurnIsNotATurn01', '{"turn":0}\n');
      // Invalid UTF-8 in the last line is skipped like a torn write.
      mkdirSync(join(root, 'game_invalidUtf8Tail00001'), { recursive: true });
      writeFileSync(
        join(root, 'game_invalidUtf8Tail00001', 'replay.jsonl'),
        Buffer.concat([
          Buffer.from('{"turn":12}\n', 'utf8'),
          Buffer.from([0x7b, 0x22, 0xff, 0xfe, 0x22, 0x7d]),
        ]),
      );
      expect(run(repo.lastReplayTurn('game_stopsAtTurnlessRow01'))).toEqual(Option.none());
      expect(run(repo.lastReplayTurn('game_stopsAtNonObjectRow1'))).toEqual(Option.none());
      expect(run(repo.lastReplayTurn('game_skipsBlankTrailing001'))).toEqual(
        Option.some(7n),
      );
      expect(run(repo.lastReplayTurn('game_zeroTurnIsNotATurn01'))).toEqual(Option.none());
      expect(run(repo.lastReplayTurn('game_invalidUtf8Tail00001'))).toEqual(
        Option.some(12n),
      );
      rmSync(root, { recursive: true, force: true });
    });

    test('only the last 64 KiB is read', () => {
      const root = realpathSync(mkdtempSync(join(tmpdir(), 'arena-tail-')));
      const id = 'game_tailWindowIs64KiB001';
      mkdirSync(join(root, id), { recursive: true });
      // One early row, then enough filler to push it out of the window, then a
      // line that is only *partly* inside it: the scan must find neither.
      writeFileSync(
        join(root, id, 'replay.jsonl'),
        [
          '{"turn":1}',
          `{"padding":"${'p'.repeat(70000)}","turn":2}`,
          `{"padding":"${'q'.repeat(100)}"}`,
        ].join('\n'),
        'utf8',
      );
      expect(run(makeRunsRepository(root).lastReplayTurn(id))).toEqual(Option.none());
      rmSync(root, { recursive: true, force: true });
    });

    test('isinstance(turn, int): a float is refused, and a bool is the one gap left', () => {
      const root = realpathSync(mkdtempSync(join(tmpdir(), 'arena-tail-')));
      const write = (id: string, body: string): void => {
        mkdirSync(join(root, id), { recursive: true });
        writeFileSync(join(root, id, 'replay.jsonl'), body, 'utf8');
      };
      const repo = makeRunsRepository(root);
      // `isinstance(5.0, int)` is False, so CPython answers `None` and drops
      // the run from the index.  This used to answer `5n` — `JSON.parse` had
      // already erased the distinction — and published a row Python hides.
      write('game_floatTurnDivergence1', '{"turn":5.0}\n');
      write('game_intTurnAgrees00000001', '{"turn":5}\n');
      write('game_largeIntExact00000001', '{"turn":9007199254740993}\n');
      // The remaining gap, and it is the other direction: `isinstance(True,
      // int)` is True and `True > 0`, so CPython returns `True` and publishes
      // `"current_turn": true`.  `Gateway.GameRow` types that field as an
      // integer and this port may not edit wire's schema, so the row is
      // dropped here instead.  `save_replay` writes turns with `int`.
      write('game_boolTurnDivergence01', '{"turn":true}\n');
      expect(run(repo.lastReplayTurn('game_floatTurnDivergence1'))).toEqual(Option.none());
      expect(run(repo.lastReplayTurn('game_intTurnAgrees00000001'))).toEqual(Option.some(5n));
      expect(run(repo.lastReplayTurn('game_largeIntExact00000001'))).toEqual(
        Option.some(9_007_199_254_740_993n),
      );
      expect(run(repo.lastReplayTurn('game_boolTurnDivergence01'))).toEqual(Option.none());

      // The float half against CPython itself, not against a remembered claim.
      const oracle = Bun.spawnSync(
        [
          'python3',
          '-c',
          [
            'import json, os, sys',
            'sys.path.insert(0, os.environ["ARENA_REPO_ROOT"])',
            'from pathlib import Path',
            'from agent_eval.replay_gateway import _last_replay_turn',
            'root = Path(os.environ["ARENA_TAIL_ROOT"])',
            'print(json.dumps({',
            '  "float": _last_replay_turn(root, "game_floatTurnDivergence1"),',
            '  "int": _last_replay_turn(root, "game_intTurnAgrees00000001"),',
            '}))',
          ].join('\n'),
        ],
        {
          stdout: 'pipe',
          stderr: 'pipe',
          env: { ...process.env, ARENA_REPO_ROOT: REPO_ROOT, ARENA_TAIL_ROOT: root },
        },
      );
      expect(oracle.stderr.toString()).toBe('');
      expect(oracle.stdout.toString().trim()).toBe('{"float": null, "int": 5}');

      rmSync(root, { recursive: true, force: true });
    });
  });

  describe('diskGamesIndex', () => {
    test('never fails, even with no runs_root at all', () => {
      const index = run(makeRunsRepository('/nonexistent/runs/root').diskGamesIndex());
      expect(index).toEqual({ schema_version: 1n, games: [] });
    });

    test('drops every malformed run and keeps the rest', () => {
      const ids: ReadonlyArray<string> = run(state.repo.diskGamesIndex()).games.map(
        (row) => row.game_id,
      );
      expect(ids).toContain(COMPLETED);
      expect(ids).toContain(RUNNING);
      expect(ids).toContain(HUSK_EMPTY_REPLAY);
      // Terminal with no report, broken JSON, oversize, id mismatch, symlinked
      // run directory, and a directory that is not a game id.
      expect(ids).not.toContain(NO_REPORT);
      expect(ids).not.toContain(BROKEN_MANIFEST);
      expect(ids).not.toContain(OVERSIZE_MANIFEST);
      expect(ids).not.toContain(ID_MISMATCH);
      expect(ids).not.toContain(SYMLINKED_RUN);
      expect(ids).not.toContain(SYMLINKED_MANIFEST);
    });

    test('sorts by (created_at, game_id) descending', () => {
      const rows = run(state.repo.diskGamesIndex()).games;
      const keys = rows.map((row) => [row.created_at ?? 0, row.game_id] as const);
      const sorted = keys.toSorted(
        (left, right) =>
          right[0] - left[0] || (right[1] < left[1] ? -1 : right[1] > left[1] ? 1 : 0),
      );
      expect(keys).toEqual(sorted);
    });

    test('a terminal row carries the archive leaderboard, not the empty one', () => {
      const row = run(state.repo.diskGamesIndex()).games.find(
        (candidate) => candidate.game_id === COMPLETED,
      );
      expect(row?.leaderboard.length).toBe(2);
      expect(row?.outcome.status).toBe('won');
    });

    test('terminalOnly drops the live rows', () => {
      const ids: ReadonlyArray<string> = run(
        state.repo.diskGamesIndex({ terminalOnly: true }),
      ).games.map((row) => row.game_id);
      expect(ids).toContain(COMPLETED);
      expect(ids).not.toContain(RUNNING);
      expect(ids).not.toContain(HUSK_EMPTY_REPLAY);
    });
  });

  describe('diskRowsWithInterrupted', () => {
    test('relabels an orphan, hides a husk, and yields to a live id', () => {
      const rows = run(state.repo.diskRowsWithInterrupted(new Set()));
      const ids: ReadonlyArray<string> = rows.map((row) => row.game_id);
      expect(ids).toContain(RUNNING);
      expect(ids).not.toContain(HUSK_EMPTY_REPLAY);
      expect(ids).not.toContain(HUSK_NO_REPLAY);
      const orphan = rows.find((row) => row.game_id === RUNNING);
      expect(orphan?.state).toBe('interrupted');
      expect(orphan?.outcome.status).toBe('interrupted');
      expect(orphan?.outcome.summary).toContain('Interrupted at turn ');
      expect(orphan?.current_turn).not.toBeNull();

      const live: ReadonlyArray<string> = run(
        state.repo.diskRowsWithInterrupted(new Set([RUNNING])),
      ).map((row) => row.game_id);
      expect(live).not.toContain(RUNNING);
      // A terminal row is never relabelled, live or not.
      expect(live).toContain(COMPLETED);
    });

    test('current_turn is the max of the manifest and the replay tail', () => {
      const manifest = run(state.repo.readManifest(RUNNING));
      const manifestTurn = manifest['current_turn'];
      const rows = run(state.repo.diskRowsWithInterrupted(new Set()));
      const orphan = rows.find((row) => row.game_id === RUNNING);
      const expected =
        typeof manifestTurn === 'number' && manifestTurn > 44 ? BigInt(manifestTurn) : 44n;
      expect(orphan?.current_turn).toBe(expected);
      expect(orphan?.outcome.summary).toBe(
        `Interrupted at turn ${String(expected)} without a terminal result; the replay is available.`,
      );
    });
  });

  describe('frameFile and videoFile', () => {
    test('latest.png is the highest index on disk, not the last name read', () => {
      const archive = run(state.repo.terminalArchive(COMPLETED));
      expect(run(state.repo.frameFile(archive, Option.none()))).toBe(
        join(archive.runRoot, 'watch_frames', '000007.png'),
      );
      expect(run(state.repo.frameFile(archive, Option.some(FrameIndex.make(0))))).toBe(
        join(archive.runRoot, 'watch_frames', '000000.png'),
      );
      expect(failure(state.repo.frameFile(archive, Option.some(FrameIndex.make(3))))).toEqual({
        status: 404,
        message: 'map frame does not exist',
      });
      // Unbounded in Python; here it simply misses the map.
      expect(failure(state.repo.frameFile(archive, Option.some(FrameIndex.make(999_999))))).toEqual({
        status: 404,
        message: 'map frame does not exist',
      });
    });

    test('video.mp4 resolves, and is 404 when a run has none', () => {
      const archive = run(state.repo.terminalArchive(COMPLETED));
      expect(run(state.repo.videoFile(archive))).toBe(join(archive.runRoot, 'game.mp4'));
      const other = run(state.repo.terminalArchive(FAILED));
      expect(failure(state.repo.videoFile(other))).toEqual({
        status: 404,
        message: 'replay video not found',
      });
    });

    test('an archive with no watch_frames directory has no frames', () => {
      const archive = run(state.repo.terminalArchive(FAILED));
      expect(failure(state.repo.frameFile(archive, Option.none()))).toEqual({
        status: 404,
        message: 'archive data not found',
      });
    });

    test('symlinked and empty frames are invisible, and so is a symlinked directory', () => {
      const root = realpathSync(mkdtempSync(join(tmpdir(), 'arena-frames-')));
      const id = 'game_symlinkedFrame000001';
      writeRun(root, {
        id,
        manifest: 'completed-strategic-v1-multiplayer',
        report: 'completed-two-seats-full-score',
        frames: ['000001.png'],
      });
      const secret = join(root, id, 'auth.json');
      writeFileSync(secret, '{"token":"must-not-leak"}', 'utf8');
      symlinkSync(secret, join(root, id, 'watch_frames', '000000.png'));
      writeFileSync(join(root, id, 'watch_frames', '000002.png'), '', 'utf8');
      const repo = makeRunsRepository(root);
      const archive = run(repo.terminalArchive(id));
      expect(failure(repo.frameFile(archive, Option.some(FrameIndex.make(0))))).toEqual({
        status: 404,
        message: 'map frame does not exist',
      });
      expect(failure(repo.frameFile(archive, Option.some(FrameIndex.make(2))))).toEqual({
        status: 404,
        message: 'map frame does not exist',
      });
      expect(run(repo.frameFile(archive, Option.none()))).toBe(
        join(root, id, 'watch_frames', '000001.png'),
      );

      // The whole directory as a link out of the run: refused before listing.
      const linked = 'game_symlinkedFramesDir01';
      writeRun(root, {
        id: linked,
        manifest: 'completed-strategic-v1-multiplayer',
        report: 'completed-two-seats-full-score',
      });
      symlinkSync(join(root, id, 'watch_frames'), join(root, linked, 'watch_frames'));
      const linkedArchive = run(repo.terminalArchive(linked));
      expect(failure(repo.frameFile(linkedArchive, Option.none()))).toEqual({
        status: 404,
        message: 'archive data not found',
      });

      // A symlinked game.mp4 is refused the same way.
      symlinkSync(secret, join(root, linked, 'game.mp4'));
      expect(failure(repo.videoFile(linkedArchive))).toEqual({
        status: 404,
        message: 'replay video not found',
      });
      rmSync(root, { recursive: true, force: true });
    });
  });

  describe('differential against agent_eval.replay_gateway', () => {
    const probeIds = [
      COMPLETED,
      FAILED,
      CANCELLED,
      CANCELLED_RANKED,
      INVALID,
      RUNNING,
      NO_REPORT,
      HUSK_EMPTY_REPLAY,
      HUSK_NO_REPLAY,
      BROKEN_MANIFEST,
      SYMLINKED_RUN,
      SYMLINKED_MANIFEST,
      OVERSIZE_MANIFEST,
      ID_MISMATCH,
      REPORT_MISMATCH,
      'short',
      'game_doesNotExistAtAll01',
    ];

    test('the oracle answers with substance, so the comparisons are not vacuous', () => {
      const oracle = runOracle(state.root, [], probeIds);
      expect(oracle.index).toContain(COMPLETED);
      expect(oracle.index).toContain('"controller_label":"pi-gpt-5.6-sol"');
      expect(oracle.index).toContain('"rank":1');
      expect(oracle.interrupted).toContain('"state":"interrupted"');
      expect(oracle.index.length).toBeGreaterThan(2000);
      expect(run(state.repo.diskGamesIndex()).games.length).toBeGreaterThanOrEqual(6);
    });

    test('the comparison is sensitive to the two mistakes it exists to catch', () => {
      const oracle = runOracle(state.root, [], probeIds);
      const index = run(state.repo.diskGamesIndex());
      // A Python `int` spelled as a JS number canonicalizes as `5000.0`.
      const asFloat = {
        schema_version: 1n,
        games: index.games.map((row) => ({ ...row, turns: Number(row.turns) })),
      };
      expect(canonical(asFloat)).not.toBe(oracle.index);
      // An optional key present as `null` instead of omitted.
      const withNullKey = {
        schema_version: 1n,
        games: index.games.map((row) => ({ ...row, control_protocol: null })),
      };
      expect(canonical(withNullKey)).not.toBe(oracle.index);
    });

    test('_disk_games_index is byte-identical', () => {
      const oracle = runOracle(state.root, [], probeIds);
      expect(canonical(run(state.repo.diskGamesIndex()))).toBe(oracle.index);
    });

    test('_disk_games_index(terminal_only=True) is byte-identical', () => {
      const oracle = runOracle(state.root, [], probeIds);
      expect(canonical(run(state.repo.diskGamesIndex({ terminalOnly: true })))).toBe(
        oracle.terminal_only,
      );
    });

    test('_disk_rows_with_interrupted is byte-identical, with and without live ids', () => {
      const empty = runOracle(state.root, [], probeIds);
      expect(
        canonical(gamesIndex(run(state.repo.diskRowsWithInterrupted(new Set())))),
      ).toBe(empty.interrupted);

      const live = [RUNNING, COMPLETED];
      const withLive = runOracle(state.root, live, probeIds);
      expect(
        canonical(gamesIndex(run(state.repo.diskRowsWithInterrupted(new Set(live))))),
      ).toBe(withLive.interrupted);
    });

    test('_last_replay_turn agrees on every run', () => {
      const oracle = runOracle(state.root, [], probeIds);
      const mine = Object.fromEntries(
        probeIds.map((id) => [
          id,
          Option.match(run(state.repo.lastReplayTurn(id)), {
            onNone: (): number | null => null,
            onSome: (turn) => Number(turn),
          }),
        ]),
      );
      expect(mine).toEqual(oracle.last_turn);
    });

    test('_archive_frame_path and _archive_video_path resolve to the same file', () => {
      const probes: ReadonlyArray<BinaryProbe> = [
        [COMPLETED, 'latest'],
        [COMPLETED, 0],
        [COMPLETED, 1],
        // A symlinked frame, an empty frame, and one that was never written.
        [COMPLETED, 2],
        [COMPLETED, 3],
        [COMPLETED, 4],
        [COMPLETED, 999_999],
        [COMPLETED, 'video'],
        // A terminal run with no `watch_frames/` and no video at all.
        [FAILED, 'latest'],
        [FAILED, 0],
        [FAILED, 'video'],
        // Not a terminal archive: the failure comes from `_terminal_archive`.
        [RUNNING, 'latest'],
      ];
      const oracle = runOracle(state.root, [], probeIds, probes);
      const mine = Object.fromEntries(
        probes.map(([id, index]) => {
          const archive = Effect.runSync(Effect.either(state.repo.terminalArchive(id)));
          if (Either.isLeft(archive)) {
            return [
              `${id}:${String(index)}`,
              { status: archive.left.status, message: archive.left.message },
            ];
          }
          const resolved =
            index === 'video'
              ? state.repo.videoFile(archive.right)
              : state.repo.frameFile(
                  archive.right,
                  index === 'latest' ? Option.none() : Option.some(FrameIndex.make(index)),
                );
          const outcome = Effect.runSync(Effect.either(resolved));
          return [
            `${id}:${String(index)}`,
            Either.isLeft(outcome)
              ? { status: outcome.left.status, message: outcome.left.message }
              : { status: 200, message: outcome.right },
          ];
        }),
      );
      expect(mine).toEqual(oracle.binary);
      // Not vacuous: the happy paths really did resolve to a file.
      expect(oracle.binary[`${COMPLETED}:latest`]?.message).toContain('000007.png');
      expect(oracle.binary[`${COMPLETED}:video`]?.message).toContain('game.mp4');
    });

    test('the victory record reaches the summary exactly where Python puts it', () => {
      const rows = run(state.repo.diskGamesIndex()).games;
      const won = rows.find((row) => row.game_id === COMPLETED);
      expect(won?.outcome.status).toBe('won');
      expect(won?.outcome.victory?.code).toBe('spacerace');
      expect(won?.outcome.victory?.label).toBe('spaceship victory');
      expect(won?.outcome.summary).toContain('(spaceship victory)');
      // `turn` and `year` are passed through with their Python spelling: an
      // `int`, not the `752.0` a JS number would canonicalize to.
      expect(canonical(won?.outcome.victory ?? null)).toContain('"turn":752,');
      expect(canonical(won?.outcome.victory ?? null)).toContain('"year":1900');
      const unlabelled = rows.find((row) => row.game_id === CANCELLED_RANKED);
      // An unknown code is its own label, and a non-string winner is dropped.
      expect(unlabelled?.outcome.victory?.label).toBe('brand_new');
      expect(unlabelled?.outcome.victory?.winners).toEqual(['AgentPlace1']);
      expect(unlabelled?.outcome.status).toBe('invalid');
      expect(unlabelled?.outcome.summary).not.toContain('brand_new');
      const corrupt = rows.find((row) => row.game_id === INVALID);
      expect(corrupt?.outcome.victory).toBeNull();
    });

    test('_read_manifest and _terminal_archive agree on status and message', () => {
      const oracle = runOracle(state.root, [], probeIds);
      const manifests = Object.fromEntries(
        probeIds.map((id) => [id, failure(state.repo.readManifest(id))]),
      );
      const archives = Object.fromEntries(
        probeIds.map((id) => [id, failure(state.repo.terminalArchive(id))]),
      );
      expect(manifests).toEqual(oracle.manifest);
      expect(archives).toEqual(oracle.archive);
    });
  });
});
