import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { describe, expect, test } from 'bun:test';
import { Either, ParseResult, Schema } from 'effect';
import { decodeWire, encodeWire } from 'src/codec';
import { decodeWatchResponse } from 'src/gateway/archive';
import {
  decodeArchiveResult,
  decodeGameResult,
  decodeGamesIndexResponse,
  decodeGameRow,
  decodeGameStatus,
  decodeUpstreamResult,
  encodeArchiveResult,
  encodeGameResult,
  encodeGamesIndexResponse,
  encodeGameStatus,
  encodeUpstreamResult,
} from 'src/gateway/games';
import { decodeGatewayIdentity, encodeGatewayIdentity } from 'src/gateway/identity';
import {
  decodeManifest,
  decodeReport,
  encodeManifest,
  encodeReport,
} from 'src/gateway/manifest';
import { decodeGatewayProblem } from 'src/gateway/problem';
import {
  decodeBoardResponse,
  decodeGameEventsResponse,
  decodeReplayResponse,
  decodeTechnologyCatalog,
  encodeBoardResponse,
  encodeGameEventsResponse,
  encodeReplayResponse,
  encodeTechnologyCatalog,
} from 'src/gateway/replay';

const fixtures = join(import.meta.dir, 'fixtures');
const read = (path: string): unknown =>
  JSON.parse(readFileSync(join(fixtures, path), 'utf8'));
const accepts = (result: Either.Either<unknown, unknown>): boolean => Either.isRight(result);
const right = <A>(result: Either.Either<A, unknown>): A => {
  if (Either.isLeft(result)) throw new Error('expected Right');
  return result.right;
};

const cases = [
  ['identity', decodeGatewayIdentity, 'live/gateway-health.json'],
  ['games index', decodeGamesIndexResponse, 'live/gateway-games-index.json'],
  ['manifest', decodeManifest, 'runs/manifest/running-v2-multiplayer.json'],
  ['report', decodeReport, 'runs/report/completed-two-seats-full-score.json'],
  ['watch', decodeWatchResponse, 'live/gateway-watch-terminal.json'],
  ['replay', decodeReplayResponse, 'live/gateway-replay-terminal-limit5.json'],
  ['board', decodeBoardResponse, 'live/gateway-board-turn1.json'],
  ['events', decodeGameEventsResponse, 'live/gateway-events.json'],
  ['archive result', decodeArchiveResult, 'live/gateway-result-terminal.json'],
  ['upstream result', decodeUpstreamResult, 'live/supervisor-result-terminal.json'],
  ['result union (archive)', decodeGameResult, 'live/gateway-result-terminal.json'],
  ['result union (upstream)', decodeGameResult, 'live/supervisor-result-terminal.json'],
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

const roundTripCases = [
  ['identity', decodeGatewayIdentity, encodeGatewayIdentity, 'live/gateway-health.json'],
  ['games index', decodeGamesIndexResponse, encodeGamesIndexResponse, 'live/gateway-games-index.json'],
  ['status', decodeGameStatus, encodeGameStatus, 'live/supervisor-status-terminal.json'],
  ['manifest', decodeManifest, encodeManifest, 'runs/manifest/running-v2-multiplayer.json'],
  ['report', decodeReport, encodeReport, 'runs/report/completed-two-seats-full-score.json'],
  ['replay', decodeReplayResponse, encodeReplayResponse, 'live/gateway-replay-terminal-limit5.json'],
  ['board', decodeBoardResponse, encodeBoardResponse, 'live/gateway-board-turn1.json'],
  ['events', decodeGameEventsResponse, encodeGameEventsResponse, 'live/gateway-events.json'],
  ['technology catalog', decodeTechnologyCatalog, encodeTechnologyCatalog, 'runs/replay-catalog/tech-tree-with-depth-and-requires.json'],
  ['archive result', decodeArchiveResult, encodeArchiveResult, 'live/gateway-result-terminal.json'],
  ['upstream result', decodeUpstreamResult, encodeUpstreamResult, 'live/supervisor-result-terminal.json'],
  ['archive result union', decodeGameResult, encodeGameResult, 'live/gateway-result-terminal.json'],
  ['upstream result union', decodeGameResult, encodeGameResult, 'live/supervisor-result-terminal.json'],
] as const;

const versionedCases = [
  ['identity', decodeGatewayIdentity, 'live/gateway-health.json'],
  ['games index', decodeGamesIndexResponse, 'live/gateway-games-index.json'],
  ['game status', decodeGameStatus, 'live/supervisor-status-running.json'],
  ['manifest', decodeManifest, 'runs/manifest/running-v2-multiplayer.json'],
  ['watch', decodeWatchResponse, 'live/gateway-watch-terminal.json'],
  ['replay', decodeReplayResponse, 'live/gateway-replay-terminal-limit5.json'],
  ['board', decodeBoardResponse, 'live/gateway-board-turn1.json'],
  ['events', decodeGameEventsResponse, 'live/gateway-events.json'],
  ['technology catalog', decodeTechnologyCatalog, 'runs/replay-catalog/tech-tree-with-depth-and-requires.json'],
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

  test.each(roundTripCases)('%s decodes and re-encodes its captured current shape', (_name, decoder, encoder, path) => {
    const decode = decoder as (input: unknown) => Either.Either<unknown, unknown>;
    const encode = encoder as (input: never) => Either.Either<unknown, unknown>;
    const input = read(path);
    expect(right(encode(right(decode(input)) as never))).toEqual(input);
  });

  test.each(validCorpus)('accepts captured fixture %s', (decoder, path) => {
    const decode = decoder as (input: unknown) => Either.Either<unknown, unknown>;
    expect(accepts(decode(read(path)))).toBe(true);
  });

  test.each(invalidCorpus)('rejects invalid fixture %s', (decoder, path) => {
    const decode = decoder as (input: unknown) => Either.Either<unknown, unknown>;
    expect(accepts(decode(read(path)))).toBe(false);
  });

  test.each(versionedCases)('%s requires the current integer schema version', (_name, decoder, path) => {
    const decode = decoder as (input: unknown) => Either.Either<unknown, unknown>;
    const value = read(path) as Record<string, unknown>;
    const { schema_version: _schemaVersion, ...missing } = value;
    for (const candidate of [
      missing,
      { ...value, schema_version: '1' },
      { ...value, schema_version: 0 },
      { ...value, schema_version: 2 },
    ]) {
      expect(accepts(decode(candidate))).toBe(false);
    }
  });

  test('timing fields must be both present or both absent', () => {
    const status = read('live/supervisor-status-terminal.json') as Record<string, unknown>;
    const { action_timeout_s: _timeout, ...withoutTimeout } = status;
    const { timing_mode: _mode, ...withoutMode } = status;
    expect(accepts(decodeGameStatus(withoutTimeout))).toBe(false);
    expect(accepts(decodeGameStatus(withoutMode))).toBe(false);

    const row = (read('live/gateway-games-index.json') as { games: readonly Record<string, unknown>[] }).games[0];
    expect(row).toBeDefined();
    const { timing_mode: _rowMode, action_timeout_s: _rowTimeout, ...withoutTiming } = row ?? {};
    expect(accepts(decodeGameRow({ ...withoutTiming, timing_mode: 'default' }))).toBe(false);
  });

  test('disk rows preserve sanitized states outside the known vocabulary', () => {
    const row = (read('live/gateway-games-index.json') as { games: readonly Record<string, unknown>[] }).games[0];
    expect(row).toBeDefined();
    const decoded = decodeGameRow({ ...row, state: 'paused' });
    expect(Either.isRight(decoded) && decoded.right.state).toBe('paused');
  });

  test('nested unknown fields are rejected with their path', () => {
    const manifest = read('runs/manifest/running-v2-multiplayer.json') as Record<string, unknown>;
    const config = manifest['config'] as Record<string, unknown>;
    const nested = decodeManifest({ ...manifest, config: { ...config, future_field: true } });
    expect(Either.isLeft(nested)).toBe(true);
    if (Either.isLeft(nested)) {
      expect(nested.left.issues.some((issue) => issue.path.join('.') === 'config.future_field')).toBe(true);
    }
  });

  test('problem bodies are exact', () => {
    expect(accepts(decodeGatewayProblem({ error: 'not found' }))).toBe(true);
    expect(accepts(decodeGatewayProblem({ error: 'not found', code: 'future' }))).toBe(false);
  });
});

describe('strict codec errors', () => {
  const Packet = Schema.Struct({ mode: Schema.Literal('v1') });
  const FailingEncodePacket = Schema.transformOrFail(
    Schema.Struct({ mode: Schema.String }),
    Packet,
    {
      strict: true,
      decode: () => ParseResult.succeed({ mode: 'v1' as const }),
      encode: (value, _options, ast) =>
        ParseResult.fail(new ParseResult.Type(ast, value, 'test encode failure')),
    },
  );
  const decodePacket = decodeWire(Packet, 'Packet');
  const encodePacket = encodeWire(Packet, 'Packet');
  const encodeFailingPacket = encodeWire(FailingEncodePacket, 'FailingEncodePacket');

  test('decode errors retain the schema name and excess-field path', () => {
    const result = decodePacket({ mode: 'v1', future: true });
    expect(Either.isLeft(result)).toBe(true);
    if (Either.isLeft(result)) {
      expect(result.left.schemaName).toBe('Packet');
      expect(result.left.issues.some((issue) => issue.path.join('.') === 'future')).toBe(true);
    }
  });

  test('encoding emits the current shape and returns typed failures', () => {
    expect(encodePacket({ mode: 'v1' })).toEqual(Either.right({ mode: 'v1' }));
    const result = encodeFailingPacket({ mode: 'v1' });
    expect(Either.isLeft(result)).toBe(true);
    if (Either.isLeft(result)) {
      expect(result.left._tag).toBe('WireEncodeError');
      expect(result.left.schemaName).toBe('FailingEncodePacket');
      expect(result.left.message).toContain('test encode failure');
    }
  });
});
