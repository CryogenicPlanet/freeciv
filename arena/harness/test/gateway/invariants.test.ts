/** Narrow module-graph checks for one response site and scoped Effect execution. */
import { describe, expect, test } from 'bun:test';
import { readdirSync, readFileSync, statSync } from 'node:fs';
import { join, relative, resolve } from 'node:path';

const GATEWAY_ROOT = resolve(import.meta.dir, '../../src/gateway');

interface Module {
  /** Path relative to `src/gateway`, forward-slashed. */
  readonly name: string;
  readonly source: string;
}

const walk = (directory: string): readonly string[] =>
  readdirSync(directory).flatMap((entry) => {
    const path = join(directory, entry);
    return statSync(path).isDirectory()
      ? walk(path)
      : path.endsWith('.ts')
        ? [path]
        : [];
  });

const modulesUnder = (root: string): readonly Module[] =>
  walk(root)
    .map((path) => ({
      name: relative(root, path).replaceAll('\\', '/'),
      source: readFileSync(path, 'utf8'),
    }))
    .toSorted((left, right) => (left.name < right.name ? -1 : 1));

const MODULES: readonly Module[] = modulesUnder(GATEWAY_ROOT);

/** Source with every block and line comment removed — claims, not prose. */
const code = (source: string): string =>
  source.replaceAll(/\/\*[\s\S]*?\*\//g, '').replaceAll(/\/\/[^\n]*/g, '');

const importsSymbol = (module: Module, symbol: string): boolean =>
  new RegExp(`import[^;]*\\b${symbol}\\b[^;]*from`, 's').test(code(module.source));

// ---------------------------------------------------------------------------
// 1. One response site
// ---------------------------------------------------------------------------

/**
 * Every module allowed to construct an `HttpServerResponse`, and why.
 *
 * The point of the list is that it is short and that each entry names a
 * *success* shape or the socket edge.  Nothing on it builds an error body.
 */
const RESPONSE_BUILDERS = {
  'http/respond.ts': 'THE renderer: the only module that turns a failure into bytes',
  'http/json.ts': 'the two success serializers, `_json` and `_bounded_json`',
  'server.ts': "the pre-router stdlib 501, which is the socket edge and not a route",
  'http/routes/archive.ts': 'the proxied stream and the local file — success shapes',
  'http/routes/replay.ts': 'the relayed upstream 2xx — a success shape',
} satisfies Readonly<Record<string, string>>;

/**
 * Statuses a *route* may never write.  `respond.ts` owns every one of them.
 *
 * Spelled as the two shapes an author would reach for — `status: 404` and
 * `{ status: 404 }` — rather than as a bare number, so that an upstream status
 * relayed through a variable (which is how `archive.ts` and `replay.ts`
 * legitimately answer 404 and 500) does not trip it.
 */
const ERROR_STATUS_LITERAL = /\bstatus:\s*(4\d{2}|5\d{2})\b/;

/**
 * The one module allowed a bare error status: the socket edge.
 *
 * `server.ts` transcribes `http.server`'s `501 Unsupported method (…)` page,
 * which the stdlib emits *before* `do_GET` exists — so it is not a route
 * refusing a request, it is the layer below routing refusing a verb, and it
 * deliberately carries none of `_send_headers`' headers.
 */
const STATUS_LITERAL_EDGE = 'server.ts';

describe('one response site', () => {
  test('only the allowlisted modules import HttpServerResponse', () => {
    const importers = MODULES.filter((module) =>
      importsSymbol(module, 'HttpServerResponse'),
    ).map((module) => module.name);
    expect(importers.toSorted()).toEqual(Object.keys(RESPONSE_BUILDERS).toSorted());
  });

  test('no module outside http/respond.ts writes a 4xx or 5xx status literal', () => {
    const offenders = MODULES.filter(
      (module) =>
        module.name !== 'http/respond.ts' &&
        module.name !== STATUS_LITERAL_EDGE &&
        ERROR_STATUS_LITERAL.test(code(module.source)),
    ).map((module) => module.name);
    // `errors.ts` carries `readonly status = 404` on the taxonomy classes, which
    // is a *declaration* of what a failure means, not a response being built —
    // and it is spelled `status = `, not `status: `, so it is not matched.
    expect(offenders).toEqual([]);
  });

  test('nothing but respond.ts renders a problem body', () => {
    const renderers = MODULES.filter(
      (module) =>
        module.name !== 'http/respond.ts' &&
        (/\bgatewayProblemBytes\b/.test(code(module.source)) ||
          /\brenderProblemBody\b/.test(code(module.source)) ||
          /\btoResponse\b/.test(code(module.source))),
    ).map((module) => module.name);
    expect(renderers).toEqual([]);
  });
});

// ---------------------------------------------------------------------------
// 2. No Effect.run* outside the entrypoint
// ---------------------------------------------------------------------------

describe('Effect.run* belongs to the entrypoint', () => {
  test('no module under src/gateway runs an Effect at module scope', () => {
    const offenders = MODULES.filter((module) =>
      /\bEffect\.run(Sync|Promise|Fork|Callback|SyncExit|PromiseExit)\b/.test(
        code(module.source),
      ),
    ).map((module) => module.name);
    // `main.ts` is the CLI entrypoint and runs through `@effect/platform-bun`'s
    // runner rather than an `Effect.run*` of its own, so the list is empty
    // outright.  A `dlopen` used to run here, at *import* time, from
    // `services/ready-file.ts`.
    expect(offenders).toEqual([]);
  });
});
