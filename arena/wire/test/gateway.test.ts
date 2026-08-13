import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { describe, expect, test } from 'bun:test';
import { Either } from 'effect';
import { decodeWatchResponse } from 'src/gateway/archive';
import {
  decodeGamesIndexResponse,
  decodeGameStatus,
} from 'src/gateway/games';
import { decodeGatewayIdentity } from 'src/gateway/identity';
import { decodeManifest, decodeReport } from 'src/gateway/manifest';
import { decodeGatewayProblem } from 'src/gateway/problem';
import {
  decodeBoardResponse,
  decodeGameEventsResponse,
  decodeReplayResponse,
  decodeTechnologyCatalog,
} from 'src/gateway/replay';

const fixtures = join(import.meta.dir, 'fixtures');
const read = (path: string): unknown =>
  JSON.parse(readFileSync(join(fixtures, path), 'utf8'));
const accepts = (result: Either.Either<unknown, unknown>): boolean => Either.isRight(result);

const cases = [
  ['identity', decodeGatewayIdentity, 'live/gateway-health.json'],
  ['games index', decodeGamesIndexResponse, 'live/gateway-games-index.json'],
  ['manifest', decodeManifest, 'runs/manifest/running-v2-multiplayer.json'],
  ['report', decodeReport, 'runs/report/completed-two-seats-full-score.json'],
  ['watch', decodeWatchResponse, 'live/gateway-watch-terminal.json'],
  ['replay', decodeReplayResponse, 'live/gateway-replay-terminal-limit5.json'],
  ['board', decodeBoardResponse, 'live/gateway-board-turn1.json'],
  ['events', decodeGameEventsResponse, 'live/gateway-events.json'],
] as const;

const validCorpus = [
  ...[
    'cancelled-strategic-v1-many-freetext-reasons.json',
    'cancelled-strategic-v1-never-started.json',
    'cancelled-v2-benchmark-null.json',
    'cancelled-v2-never-started-recovery.json',
    'completed-strategic-v1-multiplayer.json',
    'completed-strategic-v1-single-native-seat.json',
    'failed-strategic-v1-three-places.json',
    'failed-v2-boundary-wedged-recovery.json',
    'failed-v2-sidecar-exited.json',
    'invalid-strategic-v1-freetext-reasons.json',
    'invalid-v2-score-snapshot-incomplete.json',
    'running-v2-multiplayer.json',
  ].map((name) => [decodeManifest, `runs/manifest/${name}`] as const),
  ...[
    'alive-null-legacy-players.json',
    'completed-two-seats-full-score.json',
    'dead-player-alive-false.json',
    'empty-score-no-recovery.json',
    'empty-score-with-recovery.json',
    'partial-seat-stats-with-recovery.json',
    'three-players-ranked.json',
    'tied-ranks-empty-seat-stats.json',
  ].map((name) => [decodeReport, `runs/report/${name}`] as const),
  [decodeTechnologyCatalog, 'runs/replay-catalog/tech-tree-with-depth-and-requires.json'],
  [decodeTechnologyCatalog, 'runs/replay-catalog/tech-tree-without-depth.json'],
  [decodeGamesIndexResponse, 'live/supervisor-games-index.json'],
  [decodeGameStatus, 'live/supervisor-status-running.json'],
  [decodeGameStatus, 'live/supervisor-status-terminal.json'],
  [decodeWatchResponse, 'live/supervisor-watch.json'],
] as const;

const invalidCorpus = [
  ...[
    'manifest-current-turn-fractional.json',
    'manifest-invalid-reasons-string.json',
    'manifest-missing-game-id.json',
    'manifest-schema-version-string.json',
    'manifest-seats-object-not-array.json',
    'manifest-status-unknown.json',
  ].map((name) => [decodeManifest, `invalid/${name}`] as const),
  [decodeTechnologyCatalog, 'invalid/replay-catalog-requires-names-not-ids.json'],
  [decodeTechnologyCatalog, 'invalid/replay-catalog-tech-missing-id.json'],
  [decodeReport, 'invalid/report-final-turn-string.json'],
  [decodeReport, 'invalid/report-missing-seat-stats.json'],
  [decodeReport, 'invalid/report-players-object-not-array.json'],
  [decodeGameStatus, 'invalid/status-outcome-status-unknown.json'],
  [decodeGameStatus, 'invalid/status-resolved-places-null.json'],
] as const;

describe('current gateway schemas', () => {
  for (const [name, decoder, path] of cases) {
    const decode = decoder as (input: unknown) => Either.Either<unknown, unknown>;
    test(`${name} accepts its captured v1 payload`, () => {
      expect(accepts(decode(read(path)))).toBe(true);
    });

    test(`${name} rejects fields outside its version`, () => {
      const value = read(path);
      expect(accepts(decode({ ...(value as object), future_field: true }))).toBe(false);
    });
  }

  test.each(validCorpus)('accepts captured fixture %s', (decoder, path) => {
    const decode = decoder as (input: unknown) => Either.Either<unknown, unknown>;
    expect(accepts(decode(read(path)))).toBe(true);
  });

  test.each(invalidCorpus)('rejects invalid fixture %s', (decoder, path) => {
    const decode = decoder as (input: unknown) => Either.Either<unknown, unknown>;
    expect(accepts(decode(read(path)))).toBe(false);
  });

  test('nested unknown fields and future versions are rejected with paths', () => {
    const manifest = read('runs/manifest/running-v2-multiplayer.json') as Record<string, unknown>;
    const config = manifest['config'] as Record<string, unknown>;
    const nested = decodeManifest({ ...manifest, config: { ...config, future_field: true } });
    expect(Either.isLeft(nested)).toBe(true);
    if (Either.isLeft(nested)) {
      expect(nested.left.issues.some((issue) => issue.path.join('.') === 'config.future_field')).toBe(true);
    }
    expect(accepts(decodeManifest({ ...manifest, schema_version: 2 }))).toBe(false);
  });

  test('problem bodies are exact', () => {
    expect(accepts(decodeGatewayProblem({ error: 'not found' }))).toBe(true);
    expect(accepts(decodeGatewayProblem({ error: 'not found', code: 'future' }))).toBe(false);
  });
});
