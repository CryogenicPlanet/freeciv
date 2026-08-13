import { describe, expect, test } from 'bun:test';
import { Data, Effect, Either, ParseResult, Schema } from 'effect';
import {
  decodeWire,
  encodeWire,
  type WireDecoder,
  type WireEncoder,
} from 'src/codec';
import {
  decodeJsonValue,
  decodeJsonValueFromString,
  isJsonArray,
  isJsonObject,
  jsonField,
  type JsonObject,
  type JsonValue,
} from 'src/json';
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

const fixtures = `${import.meta.dir}/fixtures`;

class FixtureError extends Data.TaggedError('FixtureError')<{
  readonly message: string;
  readonly cause?: unknown;
}> {}
const accepts = <A, E>(result: Either.Either<A, E>): boolean => Either.isRight(result);
const right = <A, E>(result: Either.Either<A, E>): A => {
  if (Either.isLeft(result)) throw new Error('expected Right');
  return result.right;
};
const read = (path: string): Effect.Effect<JsonValue, FixtureError> =>
  Effect.tryPromise({
    try: () => Bun.file(`${fixtures}/${path}`).text(),
    catch: (cause) => new FixtureError({ message: `failed to read fixture: ${path}`, cause }),
  }).pipe(
    Effect.flatMap((text) =>
      Either.match(decodeJsonValueFromString(text), {
        onLeft: (error) => Effect.fail(new FixtureError({ message: `invalid JSON fixture: ${path}: ${error.message}` })),
        onRight: (value) => Effect.succeed(value),
      })),
  );
const readObject = (path: string): Effect.Effect<JsonObject, FixtureError> =>
  Effect.flatMap(read(path), (value) =>
    isJsonObject(value)
      ? Effect.succeed(value)
      : Effect.fail(new FixtureError({ message: `expected object fixture: ${path}` })));
type WireAcceptance = (input: JsonValue) => boolean;

const acceptsWith = <A>(decoder: WireDecoder<A>): WireAcceptance =>
  (input): boolean => accepts(decoder(input));
const decoderCase = <A>(
  name: string,
  decoder: WireDecoder<A>,
  path: string,
): readonly [string, WireAcceptance, string] => [name, acceptsWith(decoder), path];
const fixtureCase = <A>(
  decoder: WireDecoder<A>,
  path: string,
): readonly [WireAcceptance, string] => [acceptsWith(decoder), path];
const roundTripCase = <A, I>(
  name: string,
  decoder: WireDecoder<A>,
  encoder: WireEncoder<A, I>,
  path: string,
): readonly [string, (input: JsonValue) => void, string] => [
  name,
  (input) => {
    const encoded = right(encoder(right(decoder(input))));
    expect(right(decodeJsonValue(encoded))).toEqual(input);
  },
  path,
];

const cases = [
  decoderCase('identity', decodeGatewayIdentity, 'live/gateway-health.json'),
  decoderCase('games index', decodeGamesIndexResponse, 'live/gateway-games-index.json'),
  decoderCase('manifest', decodeManifest, 'runs/manifest/running-v2-multiplayer.json'),
  decoderCase('report', decodeReport, 'runs/report/completed-two-seats-full-score.json'),
  decoderCase('watch', decodeWatchResponse, 'live/gateway-watch-terminal.json'),
  decoderCase('replay', decodeReplayResponse, 'live/gateway-replay-terminal-limit5.json'),
  decoderCase('board', decodeBoardResponse, 'live/gateway-board-turn1.json'),
  decoderCase('events', decodeGameEventsResponse, 'live/gateway-events.json'),
  decoderCase('archive result', decodeArchiveResult, 'live/gateway-result-terminal.json'),
  decoderCase('upstream result', decodeUpstreamResult, 'live/supervisor-result-terminal.json'),
  decoderCase('result union (archive)', decodeGameResult, 'live/gateway-result-terminal.json'),
  decoderCase('result union (upstream)', decodeGameResult, 'live/supervisor-result-terminal.json'),
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
  ].map((name) => fixtureCase(decodeManifest, `runs/manifest/${name}`)),
  ...[
    'alive-null-legacy-players.json',
    'completed-two-seats-full-score.json',
    'dead-player-alive-false.json',
    'empty-score-no-recovery.json',
    'empty-score-with-recovery.json',
    'partial-seat-stats-with-recovery.json',
    'three-players-ranked.json',
    'tied-ranks-empty-seat-stats.json',
  ].map((name) => fixtureCase(decodeReport, `runs/report/${name}`)),
  fixtureCase(decodeTechnologyCatalog, 'runs/replay-catalog/tech-tree-with-depth-and-requires.json'),
  fixtureCase(decodeTechnologyCatalog, 'runs/replay-catalog/tech-tree-without-depth.json'),
  fixtureCase(decodeGamesIndexResponse, 'live/supervisor-games-index.json'),
  fixtureCase(decodeGameStatus, 'live/supervisor-status-running.json'),
  fixtureCase(decodeGameStatus, 'live/supervisor-status-terminal.json'),
  fixtureCase(decodeWatchResponse, 'live/supervisor-watch.json'),
] as const;

const roundTripCases = [
  roundTripCase('identity', decodeGatewayIdentity, encodeGatewayIdentity, 'live/gateway-health.json'),
  roundTripCase('games index', decodeGamesIndexResponse, encodeGamesIndexResponse, 'live/gateway-games-index.json'),
  roundTripCase('status', decodeGameStatus, encodeGameStatus, 'live/supervisor-status-terminal.json'),
  roundTripCase('manifest', decodeManifest, encodeManifest, 'runs/manifest/running-v2-multiplayer.json'),
  roundTripCase('report', decodeReport, encodeReport, 'runs/report/completed-two-seats-full-score.json'),
  roundTripCase('replay', decodeReplayResponse, encodeReplayResponse, 'live/gateway-replay-terminal-limit5.json'),
  roundTripCase('board', decodeBoardResponse, encodeBoardResponse, 'live/gateway-board-turn1.json'),
  roundTripCase('events', decodeGameEventsResponse, encodeGameEventsResponse, 'live/gateway-events.json'),
  roundTripCase('technology catalog', decodeTechnologyCatalog, encodeTechnologyCatalog, 'runs/replay-catalog/tech-tree-with-depth-and-requires.json'),
  roundTripCase('archive result', decodeArchiveResult, encodeArchiveResult, 'live/gateway-result-terminal.json'),
  roundTripCase('upstream result', decodeUpstreamResult, encodeUpstreamResult, 'live/supervisor-result-terminal.json'),
  roundTripCase('archive result union', decodeGameResult, encodeGameResult, 'live/gateway-result-terminal.json'),
  roundTripCase('upstream result union', decodeGameResult, encodeGameResult, 'live/supervisor-result-terminal.json'),
] as const;

const versionedCases = [
  decoderCase('identity', decodeGatewayIdentity, 'live/gateway-health.json'),
  decoderCase('games index', decodeGamesIndexResponse, 'live/gateway-games-index.json'),
  decoderCase('game status', decodeGameStatus, 'live/supervisor-status-running.json'),
  decoderCase('manifest', decodeManifest, 'runs/manifest/running-v2-multiplayer.json'),
  decoderCase('watch', decodeWatchResponse, 'live/gateway-watch-terminal.json'),
  decoderCase('replay', decodeReplayResponse, 'live/gateway-replay-terminal-limit5.json'),
  decoderCase('board', decodeBoardResponse, 'live/gateway-board-turn1.json'),
  decoderCase('events', decodeGameEventsResponse, 'live/gateway-events.json'),
  decoderCase('technology catalog', decodeTechnologyCatalog, 'runs/replay-catalog/tech-tree-with-depth-and-requires.json'),
] as const;

const invalidCorpus = [
  ...[
    'manifest-current-turn-fractional.json',
    'manifest-invalid-reasons-string.json',
    'manifest-missing-game-id.json',
    'manifest-schema-version-string.json',
    'manifest-seats-object-not-array.json',
    'manifest-status-unknown.json',
  ].map((name) => fixtureCase(decodeManifest, `invalid/${name}`)),
  fixtureCase(decodeTechnologyCatalog, 'invalid/replay-catalog-requires-names-not-ids.json'),
  fixtureCase(decodeTechnologyCatalog, 'invalid/replay-catalog-tech-missing-id.json'),
  fixtureCase(decodeReport, 'invalid/report-final-turn-string.json'),
  fixtureCase(decodeReport, 'invalid/report-missing-seat-stats.json'),
  fixtureCase(decodeReport, 'invalid/report-players-object-not-array.json'),
  fixtureCase(decodeGameStatus, 'invalid/status-outcome-status-unknown.json'),
  fixtureCase(decodeGameStatus, 'invalid/status-resolved-places-null.json'),
] as const;

describe('current gateway schemas', () => {
  for (const [name, acceptsInput, path] of cases) {
    test(`${name} accepts its captured v1 payload`, () =>
      Effect.runPromise(Effect.map(read(path), (input) => {
        expect(acceptsInput(input)).toBe(true);
      })));

    test(`${name} rejects fields outside its version`, () =>
      Effect.runPromise(Effect.map(readObject(path), (input) => {
        expect(acceptsInput({ ...input, future_field: true })).toBe(false);
      })));
  }

  test.each(roundTripCases)('%s decodes and re-encodes its captured current shape', (_name, assertRoundTrip, path) =>
    Effect.runPromise(Effect.map(read(path), assertRoundTrip)));

  test.each(validCorpus)('accepts captured fixture %s', (acceptsInput, path) =>
    Effect.runPromise(Effect.map(read(path), (input) => {
      expect(acceptsInput(input)).toBe(true);
    })));

  test.each(invalidCorpus)('rejects invalid fixture %s', (acceptsInput, path) =>
    Effect.runPromise(Effect.map(read(path), (input) => {
      expect(acceptsInput(input)).toBe(false);
    })));

  test.each(versionedCases)('%s requires the current integer schema version', (_name, acceptsInput, path) =>
    Effect.runPromise(Effect.map(readObject(path), (value) => {
      const { schema_version: _schemaVersion, ...missing } = value;
      for (const candidate of [
        missing,
        { ...value, schema_version: '1' },
        { ...value, schema_version: 0 },
        { ...value, schema_version: 2 },
      ]) {
        expect(acceptsInput(candidate)).toBe(false);
      }
    })));

  test('timing fields must be both present or both absent', () =>
    Effect.runPromise(Effect.gen(function* () {
      const status = yield* readObject('live/supervisor-status-terminal.json');
      const { action_timeout_s: _timeout, ...withoutTimeout } = status;
      const { timing_mode: _mode, ...withoutMode } = status;
      expect(accepts(decodeGameStatus(withoutTimeout))).toBe(false);
      expect(accepts(decodeGameStatus(withoutMode))).toBe(false);

      const index = yield* readObject('live/gateway-games-index.json');
      const games = jsonField(index, 'games');
      if (!isJsonArray(games)) throw new FixtureError({ message: 'expected games fixture array' });
      const row = games[0];
      if (!isJsonObject(row)) throw new FixtureError({ message: 'expected game row fixture' });
      const { timing_mode: _rowMode, action_timeout_s: _rowTimeout, ...withoutTiming } = row;
      expect(accepts(decodeGameRow({ ...withoutTiming, timing_mode: 'default' }))).toBe(false);
    })));

  test('disk rows preserve sanitized states outside the known vocabulary', () =>
    Effect.runPromise(Effect.map(readObject('live/gateway-games-index.json'), (index) => {
      const games = jsonField(index, 'games');
      if (!isJsonArray(games)) throw new FixtureError({ message: 'expected games fixture array' });
      const row = games[0];
      if (!isJsonObject(row)) throw new FixtureError({ message: 'expected game row fixture' });
      const decoded = decodeGameRow({ ...row, state: 'paused' });
      expect(Either.isRight(decoded) && decoded.right.state).toBe('paused');
    })));

  test('nested unknown fields are rejected with their path', () =>
    Effect.runPromise(Effect.map(readObject('runs/manifest/running-v2-multiplayer.json'), (manifest) => {
      const config = jsonField(manifest, 'config');
      if (!isJsonObject(config)) throw new FixtureError({ message: 'expected manifest config fixture' });
      const nested = decodeManifest({ ...manifest, config: { ...config, future_field: true } });
      expect(Either.isLeft(nested)).toBe(true);
      if (Either.isLeft(nested)) {
        expect(nested.left.issues.some((issue) => issue.path.join('.') === 'config.future_field')).toBe(true);
      }
    })));

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
