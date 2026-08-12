/**
 * The port's structural invariants, checked against the source itself.
 *
 * Everything else under `test/gateway/` runs the gateway.  This file reads it,
 * because three of the claims the port is built on are properties of the
 * *module graph* and no request can observe them being broken until the day one
 * of them matters:
 *
 * 1. **One response site.**  `http/respond.ts:13` says "a route module that
 *    imports `HttpServerResponse` to build a 404 has broken the invariant even
 *    if the bytes happen to match", and `http/routes/health.ts` says it is "the
 *    reason nothing here imports `HttpServerResponse`".  Both were true and
 *    neither was enforced: a future route that answered
 *    `HttpServerResponse.json({error: …}, {status: 404})` for convenience would
 *    keep CI green while silently dropping the security-header pair, the
 *    `no-store`, and the canonical byte encoding `renderProblemBody`
 *    guarantees.  {@link RESPONSE_BUILDERS} is the allowlist, with a reason per
 *    entry, and the second half of the test is what makes it more than a list:
 *    no module outside `http/respond.ts` may name an error status.
 * 2. **`Effect.run*` belongs to the entrypoint.**  A `runSync` at module scope
 *    executes before argv is parsed, before a `Layer` exists and outside every
 *    `Scope`.  There was exactly one — a `dlopen` in `services/ready-file.ts`
 *    — and it is gone; this keeps it gone.
 * 3. **The service's vocabulary is spelled once.**  `constants.ts` documents
 *    itself as the module "so the dispatcher and its tests cannot disagree
 *    about a literal", and six of its exports were imported by nobody while
 *    live copies ran elsewhere: mutating them was unobservable.  Every export
 *    is now asserted to have a reader.
 *
 * A grep-shaped test is a blunt instrument and this one is deliberately narrow:
 * it reads imports and a small set of literal spellings, so a false positive is
 * a rename away and a false *negative* only happens if someone constructs a
 * response through an alias.  It is still strictly more than three docstrings.
 *
 * @module
 */
import { describe, expect, test } from 'bun:test';
import { readdirSync, readFileSync, statSync } from 'node:fs';
import { join, relative, resolve } from 'node:path';
import {
  isUpstreamFallbackStatus,
  UPSTREAM_FALLBACK_STATUSES,
} from 'src/gateway/constants.ts';

const GATEWAY_ROOT = resolve(import.meta.dir, '../../src/gateway');
const TEST_ROOT = resolve(import.meta.dir);

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
const TESTS: readonly Module[] = modulesUnder(TEST_ROOT);

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
const RESPONSE_BUILDERS: Readonly<Record<string, string>> = {
  'http/respond.ts': 'THE renderer: the only module that turns a failure into bytes',
  'http/json.ts': 'the two success serializers, `_json` and `_bounded_json`',
  'server.ts': "the pre-router stdlib 501, which is the socket edge and not a route",
  'http/routes/archive.ts': 'the proxied stream and the local file — success shapes',
  'http/routes/replay.ts': 'the relayed upstream 2xx — a success shape',
};

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

// ---------------------------------------------------------------------------
// 3. constants.ts is the vocabulary, not a museum
// ---------------------------------------------------------------------------

/** Every `export const NAME` in `constants.ts`. */
const constantNames = (): readonly string[] => {
  const source = MODULES.find((module) => module.name === 'constants.ts')?.source ?? '';
  return Array.from(code(source).matchAll(/export const ([A-Za-z0-9_]+)/g), (match) =>
    String(match[1]),
  );
};

/** Everything that could read a constant: the rest of the gateway, and the tests. */
const CONSTANT_READERS: readonly Module[] = [
  ...MODULES.filter((module) => module.name !== 'constants.ts'),
  ...TESTS,
];

/** Where a name is used inside `constants.ts` itself, past its own declaration. */
const usedWithinConstants = (name: string): boolean => {
  const source = MODULES.find((module) => module.name === 'constants.ts')?.source ?? '';
  return code(source).replace(new RegExp(`export const ${name}\\b`), '').includes(name);
};

describe('constants.ts has no dead exports', () => {
  test('every export has a reader — another module, a test, or a sibling export', () => {
    const orphans = constantNames().filter(
      (name) =>
        !usedWithinConstants(name) &&
        !CONSTANT_READERS.some((module) => importsSymbol(module, name)),
    );
    // Six of these were orphans while live re-declarations ran in
    // `services/upstream.ts` and `http/respond.ts`, so five separate mutations
    // of this file survived the entire suite — and no test file imported this
    // module at all.
    expect(orphans).toEqual([]);
  });

  test('the fallback set and its predicate are one fact', () => {
    // `isUpstreamFallbackStatus` used to hardcode `404 || 405` two lines below
    // the list, so widening the list to `[404, 405, 410]` changed nothing and
    // no test noticed.  Both halves are asserted here, and the predicate is
    // asserted to *agree* with the list over the whole status space.
    expect([...UPSTREAM_FALLBACK_STATUSES]).toEqual([404, 405]);
    const disagreements = Array.from({ length: 500 }, (_, index) => 100 + index).filter(
      (status) =>
        isUpstreamFallbackStatus(status) !==
        (UPSTREAM_FALLBACK_STATUSES as readonly number[]).includes(status),
    );
    expect(disagreements).toEqual([]);
  });

  test('the upstream client re-declares none of them', () => {
    const upstream = MODULES.find((module) => module.name === 'services/upstream.ts');
    expect(upstream).toBeDefined();
    if (upstream === undefined) return;
    const redeclared = constantNames().filter((name) =>
      new RegExp(`export const ${name}\\b`).test(code(upstream.source)),
    );
    expect(redeclared).toEqual([]);
  });
});
