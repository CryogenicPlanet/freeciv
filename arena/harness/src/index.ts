/**
 * Barrel for `@arena/harness`.
 *
 * Two things live here and nothing else:
 *
 * - the package's identity and the wire revision it was built against, which a
 *   run's telemetry records;
 * - the **gateway's entry points** — the command, the layer stack, the app and
 *   the startup sequence.  Deliberately a short list: the port's modules are
 *   deep and heavily cited, and a caller that needs `dispatch` or
 *   `RunsRepository` should import the module that owns it so the citation
 *   travels with the import.  What a *consumer* needs is how to start a
 *   gateway, and how to embed one in a process it already owns (the parity rig
 *   runs two at once, which is why {@link serveGateway} is exported as a value
 *   and not hidden behind `main`).
 *
 * The barrel re-exports no error classes and no message strings: those are
 * `@arena/wire`'s, and one of the port's rules is that they are declared once.
 */
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
  main as gatewayMain,
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
