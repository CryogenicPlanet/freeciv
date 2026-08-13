/** Gateway CLI surface. Numeric flags deliberately use Python `int()`/`float()` semantics. */

import { Command, HelpDoc, Options, ValidationError } from '@effect/cli';
import { Effect, Option, Redacted } from 'effect';
import type { Either } from 'effect';
import {
  GatewayConfigError,
  pythonFloat,
  pythonInt,
  pythonRepr,
} from './config.ts';
import type { GatewayConfigInput, PostgresBackendInput } from './config.ts';

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

// ---------------------------------------------------------------------------
// The two TypeScript-only flags
// ---------------------------------------------------------------------------

/** `--backend`'s flag name, without its dashes (`Options.choice` takes it bare). */
export const GATEWAY_BACKEND_OPTION = 'backend' as const;

/** `--backend`, as a user types it. */
export const GATEWAY_BACKEND_FLAG = `--${GATEWAY_BACKEND_OPTION}` as const;

/** `--database-url`'s flag name, without its dashes. */
export const DATABASE_URL_OPTION = 'database-url' as const;

/** `--database-url`, as a user types it. */
export const DATABASE_URL_FLAG = `--${DATABASE_URL_OPTION}` as const;

/** `--materialize-root`'s flag name, without its dashes. */
export const MATERIALIZE_ROOT_OPTION = 'materialize-root' as const;

/** `--materialize-root`, as a user types it. */
export const MATERIALIZE_ROOT_FLAG = `--${MATERIALIZE_ROOT_OPTION}` as const;

/** The values `--backend` accepts; anything else is `@effect/cli`'s own error. */
export const GATEWAY_BACKENDS = ['fs', 'postgres'] as const;

/** One of {@link GATEWAY_BACKENDS}. */
export type GatewayBackendName = (typeof GATEWAY_BACKENDS)[number];

/**
 * `--backend`'s default: the filesystem, i.e. the gateway that already
 * exists.  An absent flag must not change a single byte of a response.
 */
export const DEFAULT_GATEWAY_BACKEND: GatewayBackendName = 'fs';

/**
 * `@effect/cli`'s own sentence for a required option that was not supplied
 * (`@effect/cli/internal/options.js:697`, `:990`, `:1064`).
 *
 * `--database-url` cannot be *declared* required — it is required only when
 * `--backend postgres` is, and `@effect/cli` has no conditional requirement —
 * so the requirement is enforced by hand and has to report itself in the same
 * words, through the same {@link ValidationError.missingValue}, as the four
 * flags the parser declares required.  Spelled once here so a change in
 * `@effect/cli`'s wording shows up as one failing assertion rather than as a
 * pair of messages that quietly drifted apart.
 */
export const missingOptionMessage = (flag: string): string =>
  `Expected to find option: '${flag}'`;

/** What a flag that no backend asked for is told. */
export const withoutPostgresMessage = (flag: string): string =>
  `${flag} is only used with ${GATEWAY_BACKEND_FLAG} postgres`;

/** What a `--database-url` that no backend asked for is told. */
export const DATABASE_URL_WITHOUT_POSTGRES: string = withoutPostgresMessage(DATABASE_URL_FLAG);

/** What a `--materialize-root` that no backend asked for is told. */
export const MATERIALIZE_ROOT_WITHOUT_POSTGRES: string =
  withoutPostgresMessage(MATERIALIZE_ROOT_FLAG);

/**
 * The three TypeScript-only flags, resolved into the one value
 * {@link GatewayConfigInput.backend} carries.
 *
 * They are parsed as a group because they are one decision, and the rejections
 * are the point: a `--database-url` or a `--materialize-root` typed without
 * `--backend postgres` is a *silent* fallback to the filesystem otherwise — an
 * operator who believed they were serving from Postgres would get
 * correct-looking answers from the wrong storage.  `--materialize-root` is kept
 * unresolved here; `makeGatewayConfig` resolves it and refuses one that nests
 * with `--cache-root`, because that check needs the resolved cache root.
 */
const selectBackend = (parsed: {
  readonly backend: GatewayBackendName;
  readonly databaseUrl: Option.Option<string>;
  readonly materializeRoot: Option.Option<string>;
}): Effect.Effect<PostgresBackendInput | undefined, ValidationError.ValidationError> =>
  parsed.backend === 'postgres'
    ? Option.match(parsed.databaseUrl, {
        onNone: () =>
          Effect.fail(
            ValidationError.missingValue(HelpDoc.p(missingOptionMessage(DATABASE_URL_FLAG))),
          ),
        onSome: (url) =>
          Effect.succeed<PostgresBackendInput>({
            _tag: 'Postgres',
            databaseUrl: Redacted.make(url),
            materializeRoot: Option.getOrUndefined(parsed.materializeRoot),
          }),
      })
    : Option.isSome(parsed.databaseUrl)
      ? Effect.fail(ValidationError.invalidValue(HelpDoc.p(DATABASE_URL_WITHOUT_POSTGRES)))
      : Option.isSome(parsed.materializeRoot)
        ? Effect.fail(ValidationError.invalidValue(HelpDoc.p(MATERIALIZE_ROOT_WITHOUT_POSTGRES)))
        : Effect.succeed(undefined);

/**
 * The nine flags of `_parser()`, in declaration order, plus the two
 * TypeScript-only ones the module doc scopes out of the parity claim.
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
  /**
   * `--backend fs|postgres`, `--database-url` and `--materialize-root`, as one
   * field.
   *
   * They are parsed together because none is meaningful alone: they are one
   * decision — which storage the repository reads, and where it stages the
   * files the python bridge needs — and resolving it here means `main.ts`
   * matches on a value that cannot be inconsistent, instead of re-deriving the
   * rule from three loose fields.  `fs` (and the absent flags) resolve to
   * `undefined`, so the record is unchanged for every caller that does not ask
   * for Postgres.
   */
  backend: Options.all({
    backend: Options.choice(GATEWAY_BACKEND_OPTION, GATEWAY_BACKENDS).pipe(
      Options.withDefault(DEFAULT_GATEWAY_BACKEND),
    ),
    databaseUrl: Options.optional(Options.text(DATABASE_URL_OPTION)),
    materializeRoot: Options.optional(Options.text(MATERIALIZE_ROOT_OPTION)),
  }).pipe(Options.mapEffect(selectBackend)),
} as const;

/**
 * The parsed command line, before any validation — exactly what
 * `args` holds in `main` (`:2189`).
 *
 * Structurally identical to {@link GatewayConfigInput}; the alias exists so a
 * reader can tell "what the parser produced" from "what the config consumes"
 * at a glance, and so the assignment is checked by the compiler.
 *
 * `backend` is the one field where "produced" and "consumed" are spelled
 * differently, and {@link GatewayCommandArgs} is where that is written down.
 */
export type GatewayCliArgs = GatewayConfigInput;

/**
 * What {@link gatewayCommand} actually yields.
 *
 * A parse always produces the `backend` key — `undefined` when no backend was
 * named — whereas {@link GatewayConfigInput} accepts it as optional, so that
 * the flag pair is additive for every caller that constructs a configuration
 * without going through argv.  The two are not interchangeable, because
 * `Command.Command` is invariant in its parsed type: this alias is the one the
 * command is annotated with, and it is assignable to {@link GatewayCliArgs},
 * which is what every consumer takes.
 */
export interface GatewayCommandArgs extends GatewayConfigInput {
  readonly backend: PostgresBackendInput | undefined;
}

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
  GatewayCommandArgs
> = Command.make(GATEWAY_CLI_NAME, gatewayCliOptions);

/**
 * `main`'s failure line (`:2206`): `print(f"error: {exc}", file=sys.stderr)`,
 * followed by exit code 2.
 */
export const formatStartupError = (error: GatewayConfigError): string => `error: ${error.message}`;

/** The exit code `main` uses for a rejected configuration (`:2207`). */
export const GATEWAY_CLI_ERROR_EXIT_CODE = 2 as const;

