import { Layer } from 'effect';
import {
  type FetchLike,
  make,
  UpstreamClient,
  type UpstreamClientOptions,
} from '../../../src/gateway/services/upstream.ts';

export const upstreamLayerTest = (
  options: UpstreamClientOptions & { readonly fetch: FetchLike },
): Layer.Layer<UpstreamClient> => Layer.succeed(UpstreamClient, make(options));
