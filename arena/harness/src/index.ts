/** Public harness identity and gateway embedding entry points. */
import { TELEMETRY_PACKAGE } from '@arena/telemetry';
import { WIRE_PACKAGE, WIRE_REVISION } from '@arena/wire';

export {
  GATEWAY_CLI_ERROR_EXIT_CODE,
  GATEWAY_CLI_NAME,
  type GatewayCliArgs,
  gatewayCommand,
  PYTHON_GATEWAY_PROG,
} from './gateway/cli.ts';

export {
  GatewayConfig,
  GatewayConfigError,
  type GatewayConfigInput,
  type GatewayConfigValues,
  gatewayConfigLayer,
  makeGatewayConfig,
} from './gateway/config.ts';

export {
  gatewayLayer,
  gatewayServeCommand,
  gatewayTeardown,
  type GatewayStartupError,
  runGatewayCli,
} from './gateway/main.ts';

export {
  GATEWAY_REQUEST_EVENT,
  gatewayApp,
  type GatewayHandle,
  type GatewayServeError,
  type GatewayServerServices,
  runGatewayForever,
  serveGateway,
} from './gateway/server.ts';

/** Identity of this package. */
export const HARNESS_PACKAGE = '@arena/harness' as const;

/** One arena package and the revision of the wire format it was built against. */
export interface StackEntry {
  readonly package: string;
  readonly wireRevision: number;
}

/**
 * The packages this harness is built from, in dependency order.
 *
 * The harness reports this on startup so a run's telemetry records which
 * wire revision produced it.
 */
export const stack = (): ReadonlyArray<StackEntry> =>
  [WIRE_PACKAGE, TELEMETRY_PACKAGE, HARNESS_PACKAGE].map((name) => ({
    package: name,
    wireRevision: WIRE_REVISION,
  }));
