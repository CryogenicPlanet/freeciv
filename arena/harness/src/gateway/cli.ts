/**
 * The gateway's command line — the port of `_parser()`
 * (`agent_eval/replay_gateway.py:2174-2185`).
 *
 * Nine flags, four of them required, no positionals, no subcommands.  The
 * Python declares them with bare `add_argument` calls and passes no `help=`
 * text to any of them, so there is no help copy to reproduce: what a user sees
 * is argparse's generated usage line, and the closest honest port is options
 * with no descriptions.
 *
 * ```
 * usage: python3 -m agent_eval.replay_gateway [-h] [--host HOST] [--port PORT]
 *        --service-url SERVICE_URL --runs-root RUNS_ROOT --cache-root CACHE_ROOT
 *        [--repo-root REPO_ROOT] --ready-file READY_FILE
 *        [--upstream-timeout-s UPSTREAM_TIMEOUT_S]
 *        [--viewer-public-url VIEWER_PUBLIC_URL]
 * ```
 *
 * ## `type=int` and `type=float` are Python's, not JavaScript's
 *
 * `--port` and `--upstream-timeout-s` are declared with `type=int` and
 * `type=float`, which means `--port 5_0` binds port 50, `--port ' 5 '` binds 5,
 * and `--upstream-timeout-s 1e3` is a thousand seconds.  Both go through
 * `pythonInt`/`pythonFloat` from `./config.ts` rather than `Options.integer` /
 * `Options.float`, whose parsers reject all three.  The rejection message is
 * argparse's — `argument --port: invalid int value: '0x10'` — because it is
 * the only part of the failure a caller can match on.
 *
 * ## Known, deliberate divergences from argparse
 *
 * - **`prog`.** argparse prints `python3 -m agent_eval.replay_gateway`; this
 *   command is named {@link GATEWAY_CLI_NAME} because a `@effect/cli` command
 *   name is also the token subcommands are matched against.  The Python's
 *   `prog` is kept as {@link PYTHON_GATEWAY_PROG} for anything that reports
 *   which implementation is running.
 * - **Prefix matching.** argparse accepts unambiguous abbreviations
 *   (`--serv=…`); `@effect/cli` requires the full flag.  Nothing in the repo
 *   abbreviates — `local_stack.py:540-548` spells every flag out.
 * - **Built-ins.** `@effect/cli` adds `--help`, `--version`, `--wizard` and
 *   `--completions`; argparse adds only `-h/--help`.  Extra built-ins cannot
 *   change how a valid invocation parses.
 *
 * `local_stack.py:540-548` passes every flag except `--upstream-timeout-s`, so
 * that default is the one that has to be right in practice.
 *
 * @module
 */

import { Command, HelpDoc, Options, ValidationError } from '@effect/cli';
import { Effect } from 'effect';
import type { Either, Option } from 'effect';
import {
  GatewayConfigError,
  gatewayConfigLayer,
  pythonFloat,
  pythonInt,
  pythonRepr,
} from './config.ts';
import type { GatewayConfigInput } from './config.ts';

/**
 * The command's name.  `@effect/cli` uses it for usage text and subcommand
 * matching, so it has to be a single token; see the module doc.
 */
export const GATEWAY_CLI_NAME = 'replay-gateway' as const;

/** `_parser()`'s `prog` (`:2175`), kept for provenance reporting. */
export const PYTHON_GATEWAY_PROG = 'python3 -m agent_eval.replay_gateway' as const;

/** `--host` default (`:2176`). */
export const DEFAULT_GATEWAY_HOST = '127.0.0.1' as const;

/** `--port` default (`:2177`), as the text the Python `int()` port parses. */
export const DEFAULT_GATEWAY_PORT = '0' as const;

/** `--upstream-timeout-s` default (`:2183`), as text for the `float()` port. */
export const DEFAULT_UPSTREAM_TIMEOUT_SECONDS = '10' as const;

/**
 * `--repo-root` default: `str(REPO_ROOT)` (`:2181`), where `REPO_ROOT` is
 * `Path(__file__).resolve().parent.parent` (`:34`) — the checkout that
 * contains `agent_eval/`.
 *
 * This module lives at `arena/harness/src/gateway/cli.ts`, so the same
 * directory is four levels up.  It is *not* resolved here: the Python's
 * `REPO_ROOT` was already resolved when the module loaded, and
 * `makeGatewayConfig` resolves whatever it is handed anyway.
 */
export const DEFAULT_REPO_ROOT: string = ((): string => {
  const directory = decodeURIComponent(new URL('../../../../', import.meta.url).pathname);
  return directory.length > 1 && directory.endsWith('/') ? directory.slice(0, -1) : directory;
})();

/**
 * Lift a Python-semantics parser into an option, failing the way argparse
 * fails: `argument {flag}: invalid {type} value: {repr(value)}`.
 */
const pythonTyped =
  <A>(
    flag: string,
    typeName: string,
    parse: (text: string) => Either.Either<A, GatewayConfigError>,
  ) =>
  (text: string): Effect.Effect<A, ValidationError.ValidationError> =>
    Effect.mapError(parse(text), () =>
      ValidationError.invalidValue(
        HelpDoc.p(`argument ${flag}: invalid ${typeName} value: ${pythonRepr(text)}`),
      ),
    );

/**
 * The nine flags of `_parser()`, in declaration order.
 *
 * Exported as the config object rather than as a finished command so that a
 * test — and `main.ts` — can build the command with whichever handler it
 * needs without re-deriving the flag surface.
 */
export const gatewayCliOptions = {
  /** `--host`, default `127.0.0.1` (`:2176`).  Validated as a loopback literal later. */
  host: Options.text('host').pipe(Options.withDefault(DEFAULT_GATEWAY_HOST)),
  /** `--port`, `type=int`, default `0` (`:2177`).  Range-checked later. */
  port: Options.text('port').pipe(
    Options.withDefault(DEFAULT_GATEWAY_PORT),
    Options.mapEffect(pythonTyped('--port', 'int', pythonInt)),
  ),
  /** `--service-url`, **required** (`:2178`). */
  serviceUrl: Options.text('service-url'),
  /** `--runs-root`, **required** (`:2179`). */
  runsRoot: Options.text('runs-root'),
  /** `--cache-root`, **required** (`:2180`). */
  cacheRoot: Options.text('cache-root'),
  /** `--repo-root`, default {@link DEFAULT_REPO_ROOT} (`:2181`). */
  repoRoot: Options.text('repo-root').pipe(Options.withDefault(DEFAULT_REPO_ROOT)),
  /** `--ready-file`, **required** (`:2182`). */
  readyFile: Options.text('ready-file'),
  /** `--upstream-timeout-s`, `type=float`, default `10` (`:2183`). */
  upstreamTimeoutSeconds: Options.text('upstream-timeout-s').pipe(
    Options.withDefault(DEFAULT_UPSTREAM_TIMEOUT_SECONDS),
    Options.mapEffect(pythonTyped('--upstream-timeout-s', 'float', pythonFloat)),
  ),
  /** `--viewer-public-url`, optional with no default — `None` (`:2184`). */
  viewerPublicUrl: Options.optional(Options.text('viewer-public-url')),
} as const;

/**
 * The parsed command line, before any validation — exactly what
 * `args` holds in `main` (`:2189`).
 *
 * Structurally identical to {@link GatewayConfigInput}; the alias exists so a
 * reader can tell "what the parser produced" from "what the config consumes"
 * at a glance, and so the assignment is checked by the compiler.
 */
export type GatewayCliArgs = GatewayConfigInput;

/**
 * The command itself, with no handler attached.
 *
 * `main.ts` — which this module deliberately does not contain — supplies the
 * handler that builds the layer and serves.  Splitting it that way keeps the
 * flag surface testable without starting a socket.
 */
export const gatewayCommand: Command.Command<
  typeof GATEWAY_CLI_NAME,
  never,
  never,
  GatewayCliArgs
> = Command.make(GATEWAY_CLI_NAME, gatewayCliOptions);

/**
 * The parsed command line, as a `Layer`.
 *
 * This is the seam between the two halves of the port: everything above it
 * deals in strings a user typed, everything below it deals in a validated
 * {@link GatewayConfigValues}.  Re-exported here (it is defined next to the
 * validation it performs) so a caller needs one import to get from `argv` to a
 * running gateway.
 */
export const gatewayConfigLayerFromCli: typeof gatewayConfigLayer = gatewayConfigLayer;

/**
 * `main`'s failure line (`:2206`): `print(f"error: {exc}", file=sys.stderr)`,
 * followed by exit code 2.
 */
export const formatStartupError = (error: GatewayConfigError): string => `error: ${error.message}`;

/** The exit code `main` uses for a rejected configuration (`:2207`). */
export const GATEWAY_CLI_ERROR_EXIT_CODE = 2 as const;

/**
 * The type of {@link gatewayCliOptions}' parse result, asserted against
 * {@link GatewayCliArgs}.
 *
 * `Options.optional` yields `Option<string>` and the two numeric flags yield
 * `bigint`/`number`, so this alias is the compile-time proof that the parser
 * and the config constructor agree — including that `--port` stays unbounded
 * until the range check runs.
 */
export type GatewayCliParsed = {
  readonly host: string;
  readonly port: bigint;
  readonly serviceUrl: string;
  readonly runsRoot: string;
  readonly cacheRoot: string;
  readonly repoRoot: string;
  readonly readyFile: string;
  readonly upstreamTimeoutSeconds: number;
  readonly viewerPublicUrl: Option.Option<string>;
};
