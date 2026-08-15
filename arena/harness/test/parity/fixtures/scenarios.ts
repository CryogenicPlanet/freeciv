/**
 * The eight scenario classes in `./runs`, and what each one must do to the
 * games index.
 *
 * This is the *index-shaped* view of the fixture tree; `./request-cases.ts` is
 * the route-shaped view, and `./README.md` carries the provenance.  Values
 * here were measured by running CPython's own `_disk_games_index` +
 * `_as_interrupted` over `./runs` — see the README for the command.
 *
 * The load-bearing asymmetry: **four of the eight must be absent**, and they
 * are absent for four different reasons that a port can easily conflate — an
 * empty replay, an unparseable manifest, a directory/manifest id mismatch, and
 * a symlink.  Three of the four never reach the row builder at all.
 *
 * @module
 */

/**
 * The `runs_root` to hand a gateway.
 *
 * Absolute and already resolved — `import.meta.dir` is the loader's real path
 * — which matters more than it looks: the gateway does
 * `Path(runs_root).expanduser().resolve()` and then `_read_manifest` compares
 * `run_root.parent` to that resolved root *by identity*.  Hand over an
 * unresolved path and every lookup misses silently, with an empty index and no
 * error to explain it.
 *
 * The tree is read-only to both gateways.  A rig that wants to mutate a run
 * must copy it out first; nothing here is regenerated between runs.
 */
export const PARITY_RUNS_ROOT: string = `${import.meta.dir}/runs`;

/** Why a scenario is or is not in `/v1/games`. */
export type IndexPresence =
  | {
      readonly _tag: 'Present';
      /** `state` on the index row — synthesized for an interrupted run. */
      readonly state: 'completed' | 'cancelled' | 'interrupted';
      /** `benchmark_valid`: `true` only for a completed, valid run. */
      readonly benchmarkValid: boolean | null;
      /** `current_turn` after the interrupted reshape, when there is one. */
      readonly currentTurn: number;
      /** `outcome.status`. */
      readonly outcomeStatus: 'won' | 'invalid' | 'interrupted';
    }
  | {
      readonly _tag: 'Absent';
      /** The exact gate that dropped it, named after the Python that drops it. */
      readonly droppedBy:
        | '_as_interrupted: no recorded turns'
        | '_read_archive_json: manifest does not parse'
        | '_read_manifest: manifest game_id != directory name'
        | '_disk_games_index: candidate.is_symlink()';
    };

/** One fixture run directory. */
export interface ParityScenario {
  /** The directory name, which is also the manifest's `game_id` (except #06). */
  readonly gameId: string;
  /** The scenario class this directory exists to represent. */
  readonly scenario:
    | 'terminal-valid'
    | 'terminal-no-winner'
    | 'interrupted'
    | 'lobby-husk'
    | 'malformed-manifest'
    | 'wrong-game-id'
    | 'symlinked-run'
    | 'torn-tail';
  /** The run in `.agent-eval/runs` this was cut down from. */
  readonly provenance: string;
  /** What the index must do with it. */
  readonly presence: IndexPresence;
  /** What it proves that no other fixture proves. */
  readonly proves: string;
}

/**
 * Every scenario.  Not in index order — see {@link INDEX_ORDER} for that.
 */
export const PARITY_SCENARIOS: ReadonlyArray<ParityScenario> = [
  {
    gameId: 'game_parity_terminal_valid_01',
    scenario: 'terminal-valid',
    provenance: 'game_ieTomdES08hpUmFRFzCOAVMo',
    presence: {
      _tag: 'Present',
      state: 'completed',
      benchmarkValid: true,
      currentTurn: 752,
      outcomeStatus: 'won',
    },
    proves:
      'a terminal archive with a winner: leaderboard ranks, margin, victory record, ' +
      'a paired frame and an unpaired one, and the only two real autosaves in the tree',
  },
  {
    gameId: 'game_parity_terminal_nowin_02',
    scenario: 'terminal-no-winner',
    provenance: 'game_a8_dSs1WtX5NoDPHACckOKc4',
    presence: {
      _tag: 'Present',
      state: 'cancelled',
      benchmarkValid: null,
      currentTurn: 596,
      outcomeStatus: 'invalid',
    },
    proves:
      'terminal but not valid: benchmark_valid stays null (not false), the leader is a ' +
      'native AI that outscored the agent, and every frame is unpaired',
  },
  {
    gameId: 'game_parity_interrupted_03',
    scenario: 'interrupted',
    provenance: 'game_Hsit9YEuBjKdJPPouFoGVYlk',
    presence: {
      _tag: 'Present',
      state: 'interrupted',
      benchmarkValid: null,
      currentTurn: 4,
      outcomeStatus: 'interrupted',
    },
    proves:
      'an orphaned non-terminal run is relabeled, not dropped, and its current_turn is ' +
      'max(manifest 2, replay tail 4) — a port that takes either one alone is wrong',
  },
  {
    gameId: 'game_parity_lobby_husk_04',
    scenario: 'lobby-husk',
    provenance: 'game_uSO-nwaR-nO8QXbx45dwTyKd',
    presence: { _tag: 'Absent', droppedBy: '_as_interrupted: no recorded turns' },
    proves:
      'a zero-byte replay.jsonl has nothing to watch, so the row VANISHES — the one ' +
      'scenario where a readable manifest still produces no index row',
  },
  {
    gameId: 'game_parity_malformed_05',
    scenario: 'malformed-manifest',
    provenance: 'game_xMCbmQ67I89z0UjFTM8zyO9H',
    presence: {
      _tag: 'Absent',
      droppedBy: '_read_archive_json: manifest does not parse',
    },
    proves:
      'a truncated manifest is skipped by the index scan but is a 503 on the direct ' +
      'route: the index swallows the problem, the route reports it',
  },
  {
    gameId: 'game_parity_wrong_id_06',
    scenario: 'wrong-game-id',
    provenance: 'game_JCF3XsC4LhXSQQly_xLNjYj5',
    presence: {
      _tag: 'Absent',
      droppedBy: '_read_manifest: manifest game_id != directory name',
    },
    proves:
      'the directory name is the id; a manifest claiming game_parity_wrong_id_06_other ' +
      'is unreachable from both names, so the row never exists under either',
  },
  {
    gameId: 'game_parity_symlink_07',
    scenario: 'symlinked-run',
    provenance: 'symlink -> game_parity_terminal_valid_01',
    presence: { _tag: 'Absent', droppedBy: '_disk_games_index: candidate.is_symlink()' },
    proves:
      'the symlink check runs before resolve(), so even a link to a perfectly good run ' +
      'is refused — and the valid run it points at appears exactly once, not twice',
  },
  {
    gameId: 'game_parity_torn_tail_08',
    scenario: 'torn-tail',
    provenance: 'game_mEUltpqtzauPGfjI9IlhWJ5x',
    presence: {
      _tag: 'Present',
      state: 'interrupted',
      benchmarkValid: null,
      currentTurn: 3,
      outcomeStatus: 'interrupted',
    },
    proves:
      'the tail scan walks backwards past a half-written final line to the last line ' +
      'that parses: turn 3, from four lines of which the fourth is torn',
  },
] as const;

/**
 * The index order — `(created_at, game_id)` descending — for the four rows
 * that appear.  Pinned because a stable sort over floats is exactly where two
 * implementations drift apart quietly.
 */
export const INDEX_ORDER: ReadonlyArray<string> = [
  'game_parity_interrupted_03',
  'game_parity_terminal_nowin_02',
  'game_parity_torn_tail_08',
  'game_parity_terminal_valid_01',
];

/** The four that must never appear in `/v1/games`. */
export const ABSENT_FROM_INDEX: ReadonlyArray<string> = PARITY_SCENARIOS.filter(
  (scenario) => scenario.presence._tag === 'Absent',
).map((scenario) => scenario.gameId);

/**
 * The one fixture with real autosaves.  `replay_from_autosaves`,
 * `board_from_autosave` and `events_from_autosaves` all answer for it, so the
 * `ReplayDerivationPython` layer derives the *same* document for both
 * gateways; every other fixture answers `available: false` with no snapshots.
 */
export const DERIVABLE_GAME_ID = 'game_parity_terminal_valid_01';

/** The turns those two autosaves carry. */
export const DERIVABLE_TURNS: ReadonlyArray<number> = [1, 2];
