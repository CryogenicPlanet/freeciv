/**
 * The `_archive_*` and `_disk_*` families of `agent_eval/replay_gateway.py`,
 * as values.
 *
 * Every bare `:NNN` cites that file.  Where `./public.ts` sanitizes a *field*,
 * this module assembles a *document*: the four archive payloads (`/status`,
 * `/watch.json`, `/frames`, `/result`), the games-index row, and the
 * interrupted relabel that keeps an orphaned run visible.  Together they are
 * everything the gateway can serve when the supervisor is gone — which is the
 * whole reason the disk fallback exists.
 *
 * ## Pure by construction
 *
 * `replay_gateway.py` interleaves reading and shaping: `_archive_victory`
 * opens `victory.json`, `_archive_frames` lists two directories,
 * `_terminal_archive` reads `manifest.json` and `report.json`.  Here the two
 * are split.  Every function below takes what was read — a parsed value, a
 * directory listing, a PPM's text — and returns what to serve.  No `Effect`,
 * no service, no filesystem; the I/O module reads, this module decides.
 *
 * | Python | reads | this module |
 * |---|---|---|
 * | `_terminal_archive` (`:773`) | `manifest.json`, `report.json` | {@link terminalArchiveView} |
 * | `_archive_victory` (`:681`) | `victory.json` | {@link archiveVictory} |
 * | `_archive_frames` (`:968`) | `watch_frames/`, `saves/` | {@link pairArchiveFrames} + {@link archiveFrames} |
 * | `_archive_ppm_players` (`:906`) | one `.map.ppm` | {@link archivePpmPlayers} |
 * | `_archive_frame_path` (`:1007`) | `watch_frames/` | {@link selectFramePng} |
 * | `_as_interrupted` (`:1211`) | `replay.jsonl`'s tail | {@link asInterrupted} |
 *
 * Failures are values.  The two functions that can refuse answer
 * `Either.left(name)` with a `Gateway.GatewayProblemName`; the route layer
 * turns that name into its tagged error and the one response site renders it.
 * No message string is spelled here.
 *
 * ## The traps this port had to preserve
 *
 * - **`benchmark_valid` is not the manifest's** (`:790`): it is
 *   `state == "completed" AND manifest.benchmark_valid is True`, and the
 *   *outcome* is then computed against `"completed" if benchmark_valid else
 *   "invalid"` (`:800`) rather than against the real state.  A `completed` run
 *   with `benchmark_valid: false` therefore reports `state: "completed"` and
 *   `outcome.status: "invalid"` in the same body.
 * - **Frames pair positionally** (`:975`): the *n*-th PNG by index takes the
 *   *n*-th PPM by name.  One missing autosave relabels every later frame, and
 *   a PNG past the end of the PPM list gets `turn: null` and the PNG's own
 *   name as `source_name`.
 * - **`_disk_game_row` and `_archive_status` disagree about
 *   `benchmark_valid`**: the row publishes the manifest's value when it is a
 *   real bool and `null` otherwise (`:1132`); the status narrows to a strict
 *   bool.
 * - **The interrupted summary quotes the *post-`max`* turn** (`:1229`) — the
 *   larger of the manifest's `current_turn` and the replay tail's.
 * - **A redacted invalid reason is replaced whole** (`:616`), before the
 *   dedupe, so a run whose every reason named a path publishes exactly one.
 *
 * @module
 */

import { Either, Option } from 'effect';
import {
  type CanonValue,
  compareCodePoints,
  type FrameIndex,
  type GameId,
  Gateway,
  isGameId,
  isKnownRunState,
  isTerminalRunState,
  type RunState,
} from '@arena/wire';
import {
  type Canonical,
  isUntrusted,
  publicAiDifficulty,
  publicControlProtocol,
  publicInt,
  publicNumber,
  publicPlaces,
  publicText,
  publicTiming,
  pythonStrip,
  type Untrusted,
  untrustedField,
  untrustedFieldOr,
} from './public.ts';

// ---------------------------------------------------------------------------
// The archive, minus the parts that live on disk
// ---------------------------------------------------------------------------

/**
 * `TerminalArchive` (`:88`) with `run_root` removed — the directory is the
 * I/O layer's business and nothing in a *payload* is derived from it.
 *
 * `manifest` and `report` are kept whole because the three shaping functions
 * each reach for different parts of them, exactly as Python does.
 */
export interface ArchiveView {
  readonly gameId: GameId;
  readonly manifest: Untrusted;
  readonly report: Untrusted;
  readonly state: RunState;
  readonly benchmarkValid: boolean;
  readonly places: readonly Canonical<Gateway.GamePlace>[];
  readonly leaderboard: readonly Canonical<Gateway.LeaderboardEntry>[];
  readonly outcome: Canonical<Gateway.MatchOutcome>;
}

/**
 * `"schema_version": 1` as the `bigint` the canonical writer needs.
 *
 * `Gateway.ARCHIVE_SCHEMA_VERSION` is the same number in JSON spelling; the
 * two are pinned to each other by `test/gateway/archive.test.ts` rather than
 * derived, because `Gateway.SchemaVersion1`'s decoded type is the *literal*
 * `1n` and `BigInt(...)` of a `number` is not.
 */
const SCHEMA_VERSION = 1n;

/** Current-version state; malformed or legacy values project to `unknown`. */
export const manifestState = (manifest: unknown): RunState => {
  const state = publicText(untrustedFieldOr(manifest, 'state', 'status'), 'unknown', 32);
  return isKnownRunState(state) ? state : 'unknown';
};

// ---------------------------------------------------------------------------
// _archive_reasons
// ---------------------------------------------------------------------------

/** `_archive_reasons`' caps (`:602-620`). */
const REASON_LIMIT = 240;
const REASONS_MAX = 100;

/** The words that make a reason unpublishable (`:614`). */
const REASON_SECRET_WORDS = ['token', 'password', 'credential', 'secret'] as const;

/**
 * `_archive_reasons` (`:602`) — the invalid-reason list, redacted and
 * deduplicated.
 *
 * A reason that names a path (`/` or `\`) or contains one of
 * {@link REASON_SECRET_WORDS} is replaced *wholesale* by
 * `Gateway.REDACTED_INVALID_REASON`.  Only the first 100 raw entries are
 * considered (`:606`) — the cap is on the input, not the output — and the
 * dedupe runs after the substitution.
 */
export const archiveReasons = (value: unknown): readonly string[] => {
  const rows = (Array.isArray(value) ? value : []).slice(0, REASONS_MAX).flatMap((raw) => {
    const reason = publicText(raw, '', REASON_LIMIT);
    if (reason === '') return [];
    const lowered = reason.toLowerCase();
    const unsafe =
      reason.includes('/') ||
      reason.includes('\\') ||
      REASON_SECRET_WORDS.some((word) => lowered.includes(word));
    return [unsafe ? Gateway.REDACTED_INVALID_REASON : reason];
  });
  return [...new Set(rows)];
};

// ---------------------------------------------------------------------------
// _archive_leaderboard
// ---------------------------------------------------------------------------

const LEADERBOARD_SEAT_LIMIT = 80;
const LEADERBOARD_NAME_LIMIT = 80;

/**
 * A scored row before the dense rank is stamped on it (`:645-655`).
 *
 * Spelled out rather than `Omit<LeaderboardEntry, 'rank'>`: the wire type
 * leaves `model` and `ai_difficulty` *optional* because an upstream row omits
 * them, while this producer always writes both (`:653`).  Keeping them
 * required is what lets the ranked row below stay a {@link Canonical} value.
 */
interface UnrankedEntry {
  readonly score: bigint;
  readonly score_turn: bigint | null;
  readonly place: bigint;
  readonly seat_id: string;
  readonly player_name: string;
  readonly player_color: string;
  readonly controller_label: string;
  readonly controller_type: string;
  readonly model: string | null;
  readonly ai_difficulty: string | null;
}

/**
 * `_archive_leaderboard` (`:623`) — join `report.score.players[]` to the
 * resolved places, sort, then dense-rank.
 *
 * The join is by `seat_id` first and by `player_name` second (`:640`), and a
 * score row that matches neither is **dropped**: a scored player the manifest
 * never knew about does not reach the wire.  `score_turn` is the *game-wide*
 * `final_turn` with `0` collapsed to `null` (`:644`), not a per-player value.
 *
 * The rank is dense in the "ties share the lower rank, the next rank jumps to
 * the index" sense (`:657-663`): scores `9, 9, 4` rank `1, 1, 3`.  On a sorted
 * list that is exactly "the 1-based position of the first row with this
 * score", which is what the running `previous_score`/`rank` pair computes.
 */
export const archiveLeaderboard = (
  report: unknown,
  places: readonly Canonical<Gateway.GamePlace>[],
): readonly Canonical<Gateway.LeaderboardEntry>[] => {
  const score = untrustedField(report, 'score');
  const rawPlayers = isUntrusted(score) ? untrustedField(score, 'players') : undefined;
  if (!Array.isArray(rawPlayers)) return [];
  const finalTurn = publicInt(untrustedField(score, 'final_turn'));
  const bySeat = new Map(places.map((place) => [place.seat_id, place]));
  const byName = new Map(places.map((place) => [place.player_name, place]));
  const rows = rawPlayers.flatMap((raw): readonly UnrankedEntry[] => {
    if (!isUntrusted(raw)) return [];
    const seatId = publicText(untrustedField(raw, 'seat_id'), '', LEADERBOARD_SEAT_LIMIT);
    const playerName = publicText(untrustedField(raw, 'name'), '', LEADERBOARD_NAME_LIMIT);
    const place = bySeat.get(seatId) ?? byName.get(playerName);
    if (place === undefined) return [];
    return [
      {
        score: publicInt(untrustedField(raw, 'score')),
        score_turn: finalTurn === 0n ? null : finalTurn,
        place: place.place,
        seat_id: place.seat_id,
        player_name: place.player_name,
        player_color: place.player_color,
        controller_label: place.controller_label ?? Gateway.UNCLAIMED_CONTROLLER_LABEL,
        controller_type: place.controller_type ?? Gateway.EXTERNAL_CONTROLLER_TYPE,
        model: place.model ?? null,
        ai_difficulty: place.ai_difficulty ?? null,
      },
    ];
  });
  const sorted = rows.toSorted((left, right) =>
    left.score !== right.score
      ? left.score > right.score
        ? -1
        : 1
      : left.place !== right.place
        ? left.place < right.place
          ? -1
          : 1
        : compareCodePoints(left.seat_id, right.seat_id),
  );
  return sorted.map((row) => ({
    ...row,
    rank: BigInt(sorted.findIndex((peer) => peer.score === row.score) + 1),
  }));
};

// ---------------------------------------------------------------------------
// _archive_victory / _archive_outcome / _archive_score_outcome
// ---------------------------------------------------------------------------

/**
 * A relayed JSON value, with its integers spelled the way CPython read them.
 *
 * Disk readers use `parsePythonJson`, so Python integers already arrive as
 * `bigint` and integral floats remain `number`; no post-parse numeric guess is
 * permitted here because it would collapse a disk literal such as `753.0`.
 */
const relayedJson = (value: unknown): CanonValue => {
  if (value === null || typeof value === 'string' || typeof value === 'boolean') return value;
  if (typeof value === 'bigint') return value;
  if (typeof value === 'number') return value;
  if (Array.isArray(value)) return value.map(relayedJson);
  if (isUntrusted(value)) {
    return Object.fromEntries(
      Object.entries(value).map(([key, item]) => [key, relayedJson(item)]),
    );
  }
  return null;
};

/**
 * `_archive_victory` (`:681`) minus the `victory.json` read.
 *
 * Pass {@link Option.none} for a file that is missing, unreadable or not JSON
 * — Python swallows all three identically (`:689`).  A record that is not an
 * object, or whose `victory` is not a non-empty string, is equally ignored.
 *
 * `turn` and `year` are relayed with **no validation at all** (`:704`),
 * including when they hold objects or arrays, and they are **always present**:
 * an absent key becomes `null`, because the payload is built from
 * `record.get(...)`.  `winners` keeps its string elements and drops the rest.
 */
export const archiveVictory = (
  record: Option.Option<unknown>,
): Option.Option<Canonical<Gateway.MatchVictory>> => {
  const value = Option.getOrUndefined(record);
  if (!isUntrusted(value)) return Option.none();
  const code = value['victory'];
  if (typeof code !== 'string' || code === '') return Option.none();
  const winners = value['winners'];
  const turn = value['turn'];
  const year = value['year'];
  return Option.some({
    code,
    label: Gateway.victoryLabel(code),
    winners: (Array.isArray(winners) ? winners : []).filter(
      (name): name is string => typeof name === 'string',
    ),
    // Always present, `null` when the record had no such key: Python builds
    // the payload with `record.get("turn")` (`:704`), which puts `None` on
    // the wire rather than omitting the key.  `@arena/wire`'s `MatchVictory`
    // types both as *optional*; the differential oracle says they never are.
    turn: turn === undefined ? null : relayedJson(turn),
    year: year === undefined ? null : relayedJson(year),
  });
};

/**
 * `_archive_score_outcome` (`:721`) — who won, in one sentence.
 *
 * Four shapes, and the `state` argument decides between the last two:
 *
 * - no leaderboard → `invalid`, "no complete score snapshot is available";
 * - a shared best rank → `tied` when the state is `completed`, else `invalid`
 *   with the "were tied at the last complete score" phrasing, `margin: 0`;
 * - a single leader, `completed` → `won`, "X won by N";
 * - a single leader, anything else → `invalid`, "No valid winner; X led by N
 *   at the last complete score".
 *
 * `margin` is `null` — and the ` by N` suffix vanishes — when the leader is
 * the only row (`:749`).  Note the state passed in by
 * {@link terminalArchiveView} is derived from `benchmark_valid`, not from the
 * run's real state.
 */
export const archiveScoreOutcome = (
  state: string,
  leaderboard: readonly Canonical<Gateway.LeaderboardEntry>[],
): Canonical<Omit<Gateway.MatchOutcome, 'victory'>> => {
  const first = leaderboard[0];
  if (first === undefined) {
    return {
      status: 'invalid',
      summary: 'No valid winner; no complete score snapshot is available',
      leaders: [],
      margin: null,
      score_turn: null,
    };
  }
  const leaders = leaderboard.filter((row) => row.rank === first.rank);
  const labels = leaders.map((row) => row.controller_label);
  if (leaders.length > 1) {
    return {
      status: state === 'completed' ? 'tied' : 'invalid',
      summary:
        state === 'completed'
          ? `${labels.join(' and ')} finished tied`
          : `No valid winner; ${labels.join(' and ')} were tied at the last complete score`,
      leaders: labels,
      margin: 0n,
      score_turn: first.score_turn,
    };
  }
  const best = leaderboard
    .filter((row) => row !== first)
    .reduce<bigint | null>(
      (highest, row) => (highest === null || row.score > highest ? row.score : highest),
      null,
    );
  const margin = best === null ? null : first.score - best;
  const suffix = margin === null ? '' : ` by ${String(margin)}`;
  return {
    status: state === 'completed' ? 'won' : 'invalid',
    summary:
      state === 'completed'
        ? `${first.controller_label} won${suffix}`
        : `No valid winner; ${first.controller_label} led${suffix} at the last complete score`,
    leaders: labels,
    margin,
    score_turn: first.score_turn,
  };
};

/**
 * `_archive_outcome` (`:709`) — {@link archiveScoreOutcome} plus the engine's
 * game-over record.
 *
 * `victory` is always present and usually `null`.  Its label is appended to
 * the summary only when the match actually resolved (`won`/`tied`, `:716`), so
 * an invalid run never reads as a won one even though it still reports the
 * condition that fired.
 */
export const archiveOutcome = (
  state: string,
  leaderboard: readonly Canonical<Gateway.LeaderboardEntry>[],
  victory: Option.Option<Canonical<Gateway.MatchVictory>>,
): Canonical<Gateway.MatchOutcome> => {
  const outcome = archiveScoreOutcome(state, leaderboard);
  return Option.match(victory, {
    onNone: (): Canonical<Gateway.MatchOutcome> => ({ ...outcome, victory: null }),
    onSome: (record) => ({
      ...outcome,
      summary:
        outcome.status === 'won' || outcome.status === 'tied'
          ? `${outcome.summary} (${record.label})`
          : outcome.summary,
      victory: record,
    }),
  });
};

/**
 * `_terminal_archive` (`:773`) minus its two file reads.
 *
 * Answers `terminalArchiveNotFound` in the two cases Python raises it: a state
 * outside `TERMINAL_STATES` (`:779`), and a `report.json` whose embedded
 * manifest names a different game (`:787`).  The report's *own* readability —
 * `game report not found` / `is unavailable` — belongs to the reader, which is
 * why it is not modelled here.
 */
export const terminalArchiveView = (
  gameId: GameId,
  manifest: Untrusted,
  report: Untrusted,
  victory: Option.Option<Canonical<Gateway.MatchVictory>>,
): Either.Either<ArchiveView, Gateway.GatewayProblemName> => {
  const state = manifestState(manifest);
  if (!isTerminalRunState(state)) return Either.left('terminalArchiveNotFound');
  const reportManifest = untrustedField(report, 'manifest');
  if (!isUntrusted(reportManifest) || untrustedField(reportManifest, 'game_id') !== gameId) {
    return Either.left('terminalArchiveNotFound');
  }
  const places = publicPlaces(untrustedField(manifest, 'resolved_places'), manifest);
  const leaderboard = archiveLeaderboard(report, places);
  const benchmarkValid =
    state === 'completed' && untrustedField(manifest, 'benchmark_valid') === true;
  return Either.right({
    gameId,
    manifest,
    report,
    state,
    benchmarkValid,
    places,
    leaderboard,
    outcome: archiveOutcome(benchmarkValid ? 'completed' : 'invalid', leaderboard, victory),
  });
};

// ---------------------------------------------------------------------------
// _archive_urls / _archive_status
// ---------------------------------------------------------------------------

/** `/watch/{id}` — the viewer route, root-relative unless made absolute. */
const watchPath = (gameId: GameId): string => `/watch/${gameId}`;

/**
 * `_archive_urls` (`:809`) — the eight artifact URLs, all but one absolute.
 *
 * `watch_url` is the exception (`:817`): root-relative `/watch/{id}` unless
 * `--viewer-public-url` was configured, in which case the caller passes
 * `absoluteWatch` and the origin is prepended.  That switch is why a status
 * served through a tunnel links to the tunnel while one served locally links
 * to whatever host the browser already has.
 */
export const archiveUrls = (
  base: string,
  gameId: GameId,
  absoluteWatch = false,
): Canonical<Gateway.ArchiveUrls> => {
  const game = `${base}/v1/games/${gameId}`;
  return {
    join_url: `${game}/join`,
    status_url: `${game}/status`,
    result_url: `${game}/result`,
    watch_url: absoluteWatch ? `${base}${watchPath(gameId)}` : watchPath(gameId),
    watch_json_url: `${base}${Gateway.archiveWatchJsonPath(gameId)}`,
    replay_url: `${game}/replay.json`,
    frames_url: `${base}${Gateway.archiveFramesPath(gameId)}`,
    video_url: `${base}${Gateway.archiveVideoPath(gameId)}`,
  };
};

/** `config` if it is a dict, else `{}` (`:830`, `:1068`). */
const archiveConfig = (manifest: Untrusted): unknown => {
  const config = untrustedField(manifest, 'config');
  return isUntrusted(config) ? config : {};
};

/** `_public_int(manifest["current_turn"]) if it is not None else None` (`:849`). */
const currentTurn = (manifest: Untrusted): bigint | null => {
  const value = untrustedField(manifest, 'current_turn');
  return value === undefined || value === null ? null : publicInt(value);
};

/** `_public_text(config.get("mode"), "unknown", 32)` and its `state` twin. */
const TEXT_UNKNOWN = 'unknown';
const MODE_LIMIT = 32;
const OBJECTIVE_LIMIT = 240;

/**
 * `_archive_status` (`:827`) — the `/status` and bare-`/v1/games/{id}` body.
 *
 * Only reachable for a **terminal** run: `_terminal_archive` refuses anything
 * else, so a live game's status can only ever come from upstream.  Three
 * fields are worth naming:
 *
 * - `benchmark_valid` is the archive's strict bool, not the manifest's
 *   tri-state (contrast {@link diskGameRow});
 * - `error` is a constant — the real message never reaches a spectator
 *   (`:859`);
 * - `ai_difficulty` is present and possibly `null`, while a *place*'s
 *   `ai_difficulty` is omitted when it does not resolve.
 */
export const archiveStatus = (
  archive: ArchiveView,
  base: string,
  absoluteWatch = false,
): Canonical<Gateway.GameStatus> => {
  const config = archiveConfig(archive.manifest);
  return {
    schema_version: SCHEMA_VERSION,
    game_id: archive.gameId,
    state: archive.state,
    created_at: publicNumber(untrustedField(archive.manifest, 'created_at')),
    finished_at: publicNumber(untrustedField(archive.manifest, 'finished_at')),
    benchmark_valid: archive.benchmarkValid,
    mode: publicText(untrustedField(config, 'mode'), TEXT_UNKNOWN, MODE_LIMIT),
    ...publicControlProtocol(config, archive.manifest),
    ai_difficulty: publicAiDifficulty(untrustedField(config, 'difficulty')),
    places: publicInt(untrustedField(config, 'places')),
    max_agents: publicInt(untrustedField(config, 'max_agents')),
    joined_agents: publicInt(untrustedField(archive.manifest, 'joined_agents')),
    turns: publicInt(untrustedField(config, 'turns')),
    current_turn: currentTurn(archive.manifest),
    objective: publicText(
      untrustedField(config, 'objective'),
      Gateway.DEFAULT_OBJECTIVE,
      OBJECTIVE_LIMIT,
    ),
    ...publicTiming(config),
    error: untrustedField(archive.manifest, 'error') ? Gateway.ARCHIVE_ERROR_MESSAGE : null,
    invalid_reasons: archiveReasons(untrustedField(archive.manifest, 'invalid_reasons')),
    resolved_places: archive.places,
    leaderboard: archive.leaderboard,
    outcome: archive.outcome,
    ...archiveUrls(base, archive.gameId, absoluteWatch),
  };
};

// ---------------------------------------------------------------------------
// _archive_ppm_players
// ---------------------------------------------------------------------------

/**
 * `PPM_PLAYER_RE` (`:38`), transcribed.
 *
 * Not from `@arena/wire`: this matches a *savegame artifact*, not a payload,
 * so wire has no reason to own it.  Two known narrowings against Python's
 * `re`, neither reachable from a Freeciv-written header: `\d` here is ASCII
 * where Python's also accepts Unicode digits, and `\s` disagrees on `\x85`
 * and U+FEFF.
 */
const PPM_PLAYER_RE = /^#\s*playerno:(\d+):color:\(\s*(\d+),\s*(\d+),\s*(\d+)\):name:"(.*)"\s*$/u;

/** The header scan stops after this many lines (`:915`). */
const PPM_HEADER_LINES = 512;

/** The PPM magic, the only non-comment line tolerated before a player row. */
const PPM_MAGIC = 'P3';

const PPM_NAME_LIMIT = 80;
const PPM_COLOR_MAX = 255;

/** `f"#{red:02X}{green:02X}{blue:02X}"` (`:945`). */
const mapPlayerColor = (red: number, green: number, blue: number): string =>
  `#${[red, green, blue].map((part) => part.toString(16).toUpperCase().padStart(2, '0')).join('')}`;

/**
 * Python's universal-newline line iteration: `\r\n`, `\r` and `\n` all end a
 * line, and a trailing terminator does not produce an empty final line.
 */
const pythonLines = (text: string): readonly string[] => {
  const lines = text.split(/\r\n|\r|\n/u);
  return lines.at(-1) === '' ? lines.slice(0, -1) : lines;
};

/** One PPM header line, parsed and matched against the resolved places. */
const ppmRow = (
  line: string,
  identities: ReadonlyMap<string, Canonical<Gateway.GamePlace>>,
): Option.Option<Canonical<Gateway.MapPlayer>> => {
  const match = PPM_PLAYER_RE.exec(line);
  if (match === null) return Option.none();
  const red = Number(match[2]);
  const green = Number(match[3]);
  const blue = Number(match[4]);
  if ([red, green, blue].some((part) => part < 0 || part > PPM_COLOR_MAX)) return Option.none();
  const playerId = Number(match[1]);
  const playerName = publicText(
    (match[5] ?? '').replaceAll('\\"', '"').replaceAll('\\\\', '\\'),
    '',
    PPM_NAME_LIMIT,
  );
  if (playerName === '') return Option.none();
  const place = identities.get(playerName);
  const identity =
    place === undefined
      ? {
          seat_id: `${Gateway.DYNAMIC_SEAT_ID_PREFIX}${String(playerId)}`,
          place: null,
          controller_label: Gateway.DYNAMIC_CONTROLLER_LABEL,
          controller_type: Gateway.DYNAMIC_CONTROLLER_TYPE,
          scored: false,
        }
      : {
          seat_id: place.seat_id,
          place: place.place,
          controller_label: place.controller_label ?? null,
          controller_type: place.controller_type ?? null,
          model: place.model ?? null,
          ai_difficulty: place.ai_difficulty ?? null,
          scored: true,
        };
  return Option.some({
    player_id: BigInt(playerId),
    player_name: playerName,
    player_color: mapPlayerColor(red, green, blue),
    ...identity,
  });
};

/** The header scan's state: rows accepted so far, and whether it has stopped. */
interface PpmScan {
  readonly rows: readonly Canonical<Gateway.MapPlayer>[];
  readonly stopped: boolean;
}

/**
 * `_archive_ppm_players` (`:906`) minus the file read — hand it the decoded
 * text of one `*.map.ppm`.
 *
 * The scan is deliberately paranoid, because a PPM is mostly pixels: it reads
 * at most 513 lines and stops at the first non-comment line that is neither
 * the `P3` magic nor blank — or at the first non-comment line of any kind once
 * a player row has been accepted (`:918-923`).  Line 0 is matched even when it
 * is not a comment.
 *
 * A player whose name matches a resolved place inherits that place's identity
 * and is `scored`; one that does not is a faction Freeciv invented mid-game
 * and gets `dynamic-player-{n}`, a `null` place, no `model`/`ai_difficulty`
 * keys at all, and `scored: false`.  Rows sort by `(player_id, player_name)`.
 */
export const archivePpmPlayers = (
  text: string,
  places: readonly Canonical<Gateway.GamePlace>[],
): readonly Canonical<Gateway.MapPlayer>[] => {
  const identities = new Map(places.map((place) => [place.player_name, place]));
  const scan = pythonLines(text)
    .slice(0, PPM_HEADER_LINES + 1)
    .reduce<PpmScan>(
      (state, line, index) => {
        if (state.stopped) return state;
        if (index > 0 && !line.startsWith('#')) {
          const stripped = pythonStrip(line);
          return state.rows.length > 0 || (stripped !== '' && stripped !== PPM_MAGIC)
            ? { ...state, stopped: true }
            : state;
        }
        return Option.match(ppmRow(line, identities), {
          onNone: () => state,
          onSome: (row) => ({ ...state, rows: [...state.rows, row] }),
        });
      },
      { rows: [], stopped: false },
    );
  return scan.rows.toSorted((left, right) =>
    left.player_id === right.player_id
      ? compareCodePoints(left.player_name, right.player_name)
      : left.player_id < right.player_id
        ? -1
        : 1,
  );
};

// ---------------------------------------------------------------------------
// _archive_frames / _archive_frame_path
// ---------------------------------------------------------------------------

/** One `watch_frames/NNNNNN.png` the archive listing found (`ARCHIVE_PNG_RE`). */
export interface ArchivePng {
  readonly index: FrameIndex;
  readonly name: string;
}

/** One `saves/turn-NNNN-*.map.ppm` the archive listing found (`ARCHIVE_PPM_RE`). */
export interface ArchivePpm {
  readonly turn: number;
  readonly name: string;
}

/**
 * A PNG paired with the PPM that sits at its position, or with nothing.
 *
 * `ppmName` is what the I/O layer must read to fill `mapPlayers`; when it is
 * `null` there is no autosave for this frame and the row publishes
 * `turn: null`, `map_players: []` and the PNG's own name.
 */
export interface FramePairing {
  readonly index: FrameIndex;
  readonly turn: bigint | null;
  readonly sourceName: string;
  readonly ppmName: string | null;
}

/** A {@link FramePairing} whose PPM has been read and parsed. */
export interface ArchiveFrameSource extends FramePairing {
  readonly mapPlayers: readonly Canonical<Gateway.MapPlayer>[];
}

/**
 * The pairing half of `_archive_frames` (`:968-981`) — which PPM, if any,
 * belongs to each PNG.
 *
 * **Positional, not by turn.**  PNGs are taken in ascending index order and
 * PPMs in ascending *name* order, and the *n*-th of one is matched with the
 * *n*-th of the other.  Frame 7's label is whatever the eighth autosave says,
 * so a single missing autosave relabels every later frame — the reason
 * `@arena/wire`'s `legacyFrameTurn` exists.
 *
 * Run this first: it names exactly the PPMs the I/O layer needs to read.
 */
export const pairArchiveFrames = (
  pngs: readonly ArchivePng[],
  ppms: readonly ArchivePpm[],
): readonly FramePairing[] => {
  const sortedPpms = ppms.toSorted((left, right) => compareCodePoints(left.name, right.name));
  return pngs
    .toSorted((left, right) => left.index - right.index)
    .map((png) => {
      const ppm = sortedPpms[png.index];
      return ppm === undefined
        ? { index: png.index, turn: null, sourceName: png.name, ppmName: null }
        : { index: png.index, turn: BigInt(ppm.turn), sourceName: ppm.name, ppmName: ppm.name };
    });
};

/**
 * The `frames[]` rows shared by `/frames` and `watch.json` (`:976-994`).
 *
 * Split out because `watch.json` embeds the same array (`:1058`) and reading
 * it back off a built {@link archiveFrames} would lose the
 * {@link Canonical} guarantee the nested value carries.
 */
export const archiveFrameRows = (
  gameId: GameId,
  base: string,
  frames: readonly ArchiveFrameSource[],
): readonly Canonical<Gateway.ReplayFrame>[] =>
  frames.map((frame) => ({
    // `BigInt`, because the index is a Python `int` on the wire: CPython
    // writes `"index":0`, and a JavaScript number canonicalizes to `0.0`.
    index: BigInt(frame.index),
    turn: frame.turn,
    map_players: frame.mapPlayers,
    source_name: frame.sourceName,
    png_url: `${base}${Gateway.archiveFramePngPath(gameId, frame.index)}`,
  }));

/**
 * `_archive_frames` (`:968`) — the `/frames` listing.
 *
 * `latest_png_url` is `null`, never omitted, exactly when there are no frames
 * (`:1001`).  Every `png_url` is absolute against `base`, which is the
 * gateway's own origin unless `--viewer-public-url` replaced it.
 */
export const archiveFrames = (
  gameId: GameId,
  base: string,
  frames: readonly ArchiveFrameSource[],
): Canonical<Gateway.FrameManifest> => ({
  schema_version: SCHEMA_VERSION,
  game_id: gameId,
  label: Gateway.FRAME_MANIFEST_LABEL,
  frames: archiveFrameRows(gameId, base, frames),
  latest_png_url: frames.length === 0 ? null : `${base}${Gateway.archiveLatestFramePath(gameId)}`,
});

/**
 * The selection half of `_archive_frame_path` (`:1007`) — which PNG answers
 * `/frames/{n}.png` or `/frames/latest.png`.
 *
 * `latest` is the **highest index**, not the last file listed (`:1017`).  Two
 * different 404s: an archive with no frames at all is `noMapFrames`, and an
 * index that is simply not there is `mapFrameDoesNotExist`.
 */
export const selectFramePng = (
  pngs: readonly ArchivePng[],
  index: Option.Option<FrameIndex>,
): Either.Either<ArchivePng, Gateway.GatewayProblemName> => {
  const first = pngs[0];
  if (first === undefined) return Either.left('noMapFrames');
  const wanted = Option.getOrElse(index, () =>
    pngs.reduce((highest, png) => (png.index > highest ? png.index : highest), first.index),
  );
  const found = pngs.find((png) => png.index === wanted);
  return found === undefined ? Either.left('mapFrameDoesNotExist') : Either.right(found);
};

// ---------------------------------------------------------------------------
// _archive_watch
// ---------------------------------------------------------------------------

/**
 * `_archive_watch` (`:1033`) — `watch.json`, the one document the viewer
 * actually loads.
 *
 * It is `_archive_status` and `_archive_frames` in one envelope plus a
 * `timeline`, which is the frames' turns with the unpaired ones dropped
 * (`:1038-1042`).  `replay.available` is the constant `true`; `video` reports
 * whether `game.mp4` exists, which the I/O layer resolves — Python discovers
 * it by *catching* `_archive_video_path`'s 404 (`:1044-1048`).
 *
 * Note the two `url`s below come from the **relative-watch** URL set: the
 * `absoluteWatch` switch reaches `game.watch_url` and nothing else.
 */
export const archiveWatch = (
  archive: ArchiveView,
  base: string,
  frames: readonly ArchiveFrameSource[],
  videoAvailable: boolean,
  absoluteWatch = false,
): Canonical<Gateway.WatchResponse> => {
  const urls = archiveUrls(base, archive.gameId);
  const rows = archiveFrameRows(archive.gameId, base, frames);
  return {
    schema_version: SCHEMA_VERSION,
    label: Gateway.WATCH_LABEL,
    game: archiveStatus(archive, base, absoluteWatch),
    timeline: rows.flatMap((frame) => (frame.turn === null ? [] : [{ turn: frame.turn }])),
    frames: rows,
    replay: { available: true, url: urls.replay_url },
    video: { available: videoAvailable, url: urls.video_url, kind: Gateway.ARCHIVED_VIDEO_KIND },
  };
};

// ---------------------------------------------------------------------------
// _archive_result
// ---------------------------------------------------------------------------

const RESULT_SEAT_LIMIT = 80;
const RESULT_NAME_LIMIT = 80;

/**
 * `_archive_result` (`:1066`) — the curated `/result` document.
 *
 * Keyed `artifact_id`, not `game_id`, and it is *not* the upstream `/result`:
 * the whole `manifest`, `seat_stats`, `recovery` and — critically — the
 * `episode` path that names the run directory on disk are all absent.  Each
 * player's `metrics` is filtered to `Gateway.PUBLIC_SCORE_METRICS`, which is
 * what keeps a scorer's private counters off the wire.
 */
export const archiveResult = (
  archive: ArchiveView,
  base: string,
  absoluteWatch = false,
): Canonical<Gateway.ArchiveResult> => {
  const config = archiveConfig(archive.manifest);
  const rawScore = untrustedField(archive.report, 'score');
  const rawPlayers = isUntrusted(rawScore) ? untrustedField(rawScore, 'players') : [];
  const urls = archiveUrls(base, archive.gameId, absoluteWatch);
  return {
    schema_version: SCHEMA_VERSION,
    artifact_id: archive.gameId,
    state: archive.state,
    benchmark_valid: archive.benchmarkValid,
    ...publicTiming(config),
    invalid_reasons: archiveReasons(untrustedField(archive.manifest, 'invalid_reasons')),
    leaderboard: archive.leaderboard,
    outcome: archive.outcome,
    score: {
      final_turn: isUntrusted(rawScore) ? publicInt(untrustedField(rawScore, 'final_turn')) : null,
      players: (Array.isArray(rawPlayers) ? rawPlayers : []).flatMap((raw) => {
        if (!isUntrusted(raw)) return [];
        const metrics = untrustedField(raw, 'metrics');
        return [
          {
            seat_id: publicText(untrustedField(raw, 'seat_id'), '', RESULT_SEAT_LIMIT),
            player_id: publicInt(untrustedField(raw, 'player_id')),
            name: publicText(untrustedField(raw, 'name'), '', RESULT_NAME_LIMIT),
            rank: publicInt(untrustedField(raw, 'rank')),
            score: publicInt(untrustedField(raw, 'score')),
            metrics: isUntrusted(metrics)
              ? Object.fromEntries(
                  Object.entries(metrics)
                    .filter(([key]) => Gateway.isPublicScoreMetric(key))
                    .map(([key, value]) => [key, publicInt(value)]),
                )
              : {},
          },
        ];
      }),
    },
    artifact_urls: {
      status: urls.status_url,
      watch: urls.watch_url,
      replay: urls.replay_url,
      frames: urls.frames_url,
      video: urls.video_url,
    },
  };
};

// ---------------------------------------------------------------------------
// _disk_game_row / _as_interrupted / _disk_games_index
// ---------------------------------------------------------------------------

/** The three fixed summaries of a disk row's outcome (`:1136-1144`). */
const DISK_ROW_SUMMARIES = {
  complete: 'Terminal result available in this replay.',
  no_valid_winner: 'No valid winner.',
  pending: 'Match in progress.',
} as const satisfies Readonly<Record<(typeof Gateway.DISK_ROW_OUTCOME_STATUSES)[number], string>>;

/**
 * `_disk_game_row` (`:1123`) — one row of the games index, built from a
 * manifest alone.
 *
 * {@link Option.none} when the manifest has no valid `game_id` or no `config`
 * object; those two are the only hard requirements.  A terminal row is
 * enriched afterwards by the caller, which replaces `leaderboard` and
 * `outcome` from the archive (`:1268`) — this function always emits the empty
 * leaderboard and one of the three `DISK_ROW_OUTCOME_STATUSES`.
 *
 * `benchmark_valid` is tri-state here (`bool | null`), unlike
 * {@link archiveStatus}'s strict bool: a manifest that recorded no verdict
 * publishes `null` rather than `false`.
 */
export const diskGameRow = (manifest: Untrusted): Option.Option<Canonical<Gateway.GameRow>> => {
  const gameId = untrustedField(manifest, 'game_id');
  const config = untrustedField(manifest, 'config');
  if (typeof gameId !== 'string' || !isGameId(gameId)) return Option.none();
  if (!isUntrusted(config)) return Option.none();
  const state = manifestState(manifest);
  const rawValidity = untrustedField(manifest, 'benchmark_valid');
  const validity = typeof rawValidity === 'boolean' ? rawValidity : null;
  const status = isTerminalRunState(state)
    ? validity === true
      ? 'complete'
      : 'no_valid_winner'
    : 'pending';
  // Annotated rather than inlined into `Option.some`: the contextual type is
  // what makes TypeScript distribute the two conditional spreads below over
  // their union members instead of collapsing them into optional keys.
  const row: Canonical<Gateway.GameRow> = {
    game_id: gameId,
    state,
    created_at: publicNumber(untrustedField(manifest, 'created_at')),
    finished_at: publicNumber(untrustedField(manifest, 'finished_at')),
    current_turn: currentTurn(manifest),
    turns: publicInt(untrustedField(config, 'turns')),
    benchmark_valid: validity,
    mode: publicText(untrustedField(config, 'mode'), TEXT_UNKNOWN, MODE_LIMIT),
    ...publicControlProtocol(config, manifest),
    ai_difficulty: publicAiDifficulty(untrustedField(config, 'difficulty')),
    ...publicTiming(config),
    places: publicInt(untrustedField(config, 'places')),
    max_agents: publicInt(untrustedField(config, 'max_agents')),
    joined_agents: publicInt(untrustedField(manifest, 'joined_agents')),
    resolved_places: publicPlaces(untrustedField(manifest, 'resolved_places'), manifest),
    leaderboard: [],
    outcome: {
      status,
      summary: DISK_ROW_SUMMARIES[status],
      leaders: [],
      margin: null,
      score_turn: null,
      victory: null,
    },
    watch_path: watchPath(gameId),
  };
  return Option.some(row);
};

/**
 * `_as_interrupted` (`:1211`) — relabel an orphaned run, or drop it.
 *
 * `lastReplayTurn` is `_last_replay_turn`'s answer (`:1178`), which the I/O
 * layer produces by reading the last 64 KiB of `replay.jsonl` backwards and
 * stopping at the **first line that parses**: a parseable line with no
 * positive integer `turn` yields `None` rather than making the scan continue.
 * {@link Option.none} therefore means "a lobby husk with nothing to watch",
 * and such a row stays hidden from the index — the `game_a8` incident fix.
 *
 * `current_turn` becomes `max(existing, turn)` and the summary quotes that
 * result, not the turn that was read.  Everything else — `leaderboard`,
 * `benchmark_valid`, a `finished_at` of `0.0` that still says "never
 * finished" — is left exactly as it was.
 */
export const asInterrupted = (
  row: Canonical<Gateway.GameRow>,
  lastReplayTurn: Option.Option<bigint>,
): Option.Option<Canonical<Gateway.InterruptedGameRow>> =>
  Option.map(lastReplayTurn, (turn) => {
    const current = row.current_turn === null || row.current_turn < turn ? turn : row.current_turn;
    // `Object.assign`, not `{...row, ...}`: an object spread widens every
    // optional property of its source with `undefined`, so the copy would no
    // longer satisfy {@link Canonical} — the canonical writer refuses an
    // `undefined`-valued key.  `Object.assign` composes the types by
    // intersection instead, and the guarantee survives.  The target is a
    // fresh `{}`; the input row is never mutated, unlike Python's in-place
    // rewrite (`:1224-1238`).
    return Object.assign({}, row, {
      current_turn: current,
      state: Gateway.INTERRUPTED_STATUS,
      outcome: {
        status: Gateway.INTERRUPTED_STATUS,
        summary: Gateway.interruptedSummary(current),
        leaders: [],
        margin: null,
        score_turn: null,
        victory: null,
      },
    });
  });

/**
 * `live_ids` (`:1627`) — the ids the upstream index claims, so the disk view
 * never contradicts or duplicates a run the supervisor still owns.
 *
 * Tolerant by design: a non-object element, or one whose `game_id` is not a
 * well-formed id, contributes nothing to the set *and is still relayed* inside
 * `games` by the caller.
 */
export const liveGameIds = (upstreamRows: unknown): ReadonlySet<string> =>
  new Set(
    (Array.isArray(upstreamRows) ? upstreamRows : []).flatMap((row) => {
      const gameId = untrustedField(row, 'game_id');
      return typeof gameId === 'string' && isGameId(gameId) ? [gameId] : [];
    }),
  );

/**
 * `_disk_games_index`'s ordering (`:1274`): `(created_at, game_id)`
 * **descending on both**.
 *
 * Applied inside the disk index and *not* re-applied after the upstream merge,
 * so a merged response reads "upstream rows in their own order, then disk rows
 * in this one".
 */
export const sortDiskGameRows = <
  A extends { readonly created_at: number | null; readonly game_id: string },
>(
  rows: readonly A[],
): readonly A[] =>
  rows.toSorted((left, right) =>
    left.created_at === right.created_at
      ? compareCodePoints(right.game_id, left.game_id)
      : (right.created_at ?? 0) - (left.created_at ?? 0),
  );

/**
 * The ids {@link diskRowsWithInterrupted} will ask about — i.e. the only runs
 * whose `replay.jsonl` tail is worth reading.
 *
 * Python reads the tail inside the loop; splitting it out keeps this module
 * free of I/O without making the caller re-derive the filter.
 */
export const interruptedCandidates = (
  rows: readonly Canonical<Gateway.GameRow>[],
  liveIds: ReadonlySet<string>,
): readonly string[] =>
  rows
    .filter((row) => !liveIds.has(row.game_id) && !isTerminalRunState(row.state))
    .map((row) => row.game_id);

/**
 * `_disk_rows_with_interrupted` (`:1582`) — the disk index as the two
 * relabelling branches of `/v1/games` serve it.
 *
 * A row that is live upstream is skipped entirely (the upstream row wins,
 * unmodified); a terminal row passes through untouched; anything else is
 * {@link asInterrupted} or dropped.  `replayTurns` supplies
 * `_last_replay_turn`'s answer per game id — build it from
 * {@link interruptedCandidates}, which names exactly the rows that need one.
 *
 * **Not** applied on the upstream-404/405 branch (`:1650`), which serves the
 * raw disk index instead: lobby husks visible, `outcome.status` `"pending"`.
 */
export const diskRowsWithInterrupted = (
  rows: readonly Canonical<Gateway.GameRow>[],
  liveIds: ReadonlySet<string>,
  replayTurns: ReadonlyMap<string, bigint>,
): readonly Canonical<Gateway.GameRow>[] =>
  rows.flatMap((row): readonly Canonical<Gateway.GameRow>[] => {
    if (liveIds.has(row.game_id)) return [];
    if (isTerminalRunState(row.state)) return [row];
    return Option.match(asInterrupted(row, Option.fromNullable(replayTurns.get(row.game_id))), {
      onNone: (): readonly Canonical<Gateway.GameRow>[] => [],
      onSome: (relabelled) => [relabelled],
    });
  });

/** The `{schema_version, games}` envelope every index branch shares (`:1279`). */
export const gamesIndex = (
  rows: readonly Canonical<Gateway.GameRow>[],
): Canonical<Gateway.GamesIndexResponse> => ({
  schema_version: SCHEMA_VERSION,
  games: rows,
});
