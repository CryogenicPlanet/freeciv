import type { CanonRecord } from '@arena/wire';
import { Effect, type Layer } from 'effect';
import {
  DerivationArtifactsMissing,
  type DerivationError,
  type DerivationRequest,
  DerivationUnavailable,
  layerFromRunner,
  type ReplayDerivation,
} from '../../../src/gateway/services/derivation.ts';

export const derivationRequestKey = (request: DerivationRequest): string =>
  request.operation === 'replay'
    ? `replay:${request.gameId}:${String(request.afterTurn)}:${String(request.limit)}:${String(request.complete)}`
    : request.operation === 'board'
      ? `board:${request.gameId}:${String(request.turn)}`
      : `events:${request.gameId}:${String(request.complete)}`;

export type DerivationFixture = ReadonlyMap<
  string,
  Effect.Effect<CanonRecord, DerivationError>
>;

export const derivationFixture = (
  entries: Readonly<Record<string, CanonRecord>>,
): DerivationFixture =>
  new Map(Object.entries(entries).map(([key, value]) => [key, Effect.succeed(value)]));

export const ReplayDerivationFixture = (
  entries: DerivationFixture,
): Layer.Layer<ReplayDerivation> =>
  layerFromRunner((request) =>
    entries.get(derivationRequestKey(request)) ??
      Effect.fail(
        new DerivationArtifactsMissing({
          operation: request.operation,
          gameId: request.gameId,
          detail: `no fixture for ${derivationRequestKey(request)}`,
        }),
      ),
  );

export const ReplayDerivationUnavailable: Layer.Layer<ReplayDerivation> = layerFromRunner(
  (request) =>
    Effect.fail(
      new DerivationUnavailable({
        operation: request.operation,
        gameId: request.gameId,
        detail: 'no derivation backend is configured',
      }),
    ),
);
