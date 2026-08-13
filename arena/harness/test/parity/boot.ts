/**
 * Boot Python and TypeScript gateways from one flag builder and compare their
 * argv. Only `--ready-file` and `--cache-root` differ: the former is exclusively
 * locked, while sharing the latter could let one implementation answer from the
 * other's derived data.
 *
 * Both bind port 0 and publish the actual port atomically through private ready
 * files. Paths inside `.agent-eval` and service ports claimed by a running
 * stack are refused. Every spawned child is registered immediately so teardown
 * can kill setup failures and report orphans.
 */

import { isJsonValue } from '@arena/wire';
import { Either } from 'effect';
import { mkdirSync, mkdtempSync, readdirSync, readFileSync, realpathSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve, sep } from 'node:path';
import {
  type GatewayReadyRecord,
  parseGatewayReadyRecord,
  parseJsonObjectFromText,
  pidFromStackRecord,
  pipedStreamText,
  portsFromServiceUrl,
  portsFromStackRecord,
} from './json-boundary.ts';

// ---------------------------------------------------------------------------
// Where things are
// ---------------------------------------------------------------------------

/** The checkout that contains `agent_eval/` — the Python's `REPO_ROOT`. */
export const REPO_ROOT: string = resolve(import.meta.dir, '../../../..');

/** `arena/harness` — the TypeScript gateway's package root. */
export const HARNESS_ROOT: string = resolve(import.meta.dir, '../..');

/** The TypeScript gateway's entry point, absolute so cwd cannot change it. */
export const GATEWAY_MAIN_TS: string = join(HARNESS_ROOT, 'src/gateway/main.ts');

/**
 * The interpreter, overridable for a checkout whose `python3` is not the one
 * that owns `agent_eval`.
 */
export const PYTHON_BIN: string = process.env['ARENA_PARITY_PYTHON'] ?? 'python3';

/** The two launchers.  Everything after these is the one shared flags array. */
export const PYTHON_LAUNCHER: ReadonlyArray<string> = [
  PYTHON_BIN,
  '-m',
  'agent_eval.replay_gateway',
];
export const TYPESCRIPT_LAUNCHER: ReadonlyArray<string> = ['bun', GATEWAY_MAIN_TS];

/** Loopback only.  Both gateways refuse a non-loopback `--host` anyway. */
export const GATEWAY_HOST = '127.0.0.1' as const;

/** The state directory a running `local_stack` owns.  Never written to here. */
export const LIVE_STACK_DIR: string = join(REPO_ROOT, '.agent-eval');

/** Ready records published by running stacks, one level below state root. */
export const LIVE_STACK_RECORDS_DIR: string = join(LIVE_STACK_DIR, 'local-stack');

// ---------------------------------------------------------------------------
// The platform gate, and why it is announced rather than silent
// ---------------------------------------------------------------------------

/**
 * Ready-file locking has native bindings for Darwin and Linux. A different
 * platform cannot boot the complete pair and must not report partial parity.
 */
export const PARITY_PLATFORM_SUPPORTED: boolean =
  process.platform === 'darwin' || process.platform === 'linux';

/** `ARENA_REQUIRE_PARITY=1` turns every unsupported-platform skip into failure. */
export const PARITY_REQUIRED: boolean = process.env['ARENA_REQUIRE_PARITY'] === '1';

/** The loud banner a skipped rig prints, naming exactly what did not run. */
export const paritySkipWarning = (rig: string, whatDidNotRun: string): string =>
  `\n!! ${rig} DID NOT RUN: platform ${process.platform} is unsupported; expected linux or darwin.\n` +
  `!! ${whatDidNotRun} contributed ZERO assertions to this run.\n` +
  '!! ARENA_REQUIRE_PARITY=1 forbids this platform skip.\n';

// ---------------------------------------------------------------------------
// Upstream fixtures for a gateway that must find no upstream
// ---------------------------------------------------------------------------

/**
 * RFC 5737 TEST-NET-1 connect-timeout fixture. Unlike a released ephemeral
 * port it cannot be rebound by the gateway, which would create a self-proxy.
 */
export const UNROUTABLE_UPSTREAM_URL = 'http://192.0.2.1:9' as const;

/** One second.  Long enough to be a timeout, short enough to run ~50 legs. */
export const UNROUTABLE_UPSTREAM_TIMEOUT_S = 1;

/**
 * `127.0.0.1:1` — a well-known port nothing binds, so a connect is *refused*
 * immediately rather than timing out.
 *
 * A different branch from {@link UNROUTABLE_UPSTREAM_URL}, and worth exercising
 * separately: refusal is instant, timeout is not, and the two reach the
 * gateway's disk fallback through different error paths.  Port 1 lies outside
 * every ephemeral range, so it carries none of the reuse hazard.
 */
export const REFUSED_UPSTREAM_URL = 'http://127.0.0.1:1' as const;

// ---------------------------------------------------------------------------
// The flags array
// ---------------------------------------------------------------------------

/** The two implementations under comparison. */
export type Impl = 'python' | 'typescript';

/** The three gateway process slots: Python, TS/filesystem, and TS/Postgres. */
export type Slot = Impl | 'postgres';

/** The suffix each slot's private files carry. */
const SLOT = { python: 'py', typescript: 'ts', postgres: 'pg' } satisfies Readonly<
  Record<Slot, string>
>;

/**
 * The only flags whose values may differ between the two processes, and the
 * only two that *must*.  See the module doc.
 */
export const SLOT_SCOPED_FLAGS: ReadonlyArray<string> = ['--cache-root', '--ready-file'];

/** What a caller asks for.  Everything optional has a documented default. */
export interface BootSpec {
  /** Shared by both gateways: the disk must not be a variable. */
  readonly runsRoot: string;
  /** `--service-url`.  A stub's origin, or one of the down-fixtures above. */
  readonly serviceUrl: string;
  /**
   * Names this boot's private files — `<scratch>/<scenario>-py.ready.json`,
   * `<scratch>/cache-<scenario>-ts`, and so on.  One scenario, one fresh pair
   * of caches.
   */
  readonly scenario: string;
  /** Defaults to a fresh `mkdtemp`, owned and removed by `pair.cleanup()`. */
  readonly scratch?: string;
  /** `--repo-root`.  Defaults to {@link REPO_ROOT}. */
  readonly repoRoot?: string;
  /** `--upstream-timeout-s`.  Omitted from argv entirely when unset. */
  readonly upstreamTimeoutSeconds?: number;
  /** `--viewer-public-url`.  Omitted from argv entirely when unset. */
  readonly viewerPublicUrl?: string;
  /** Extra environment for both children, merged over `process.env`. */
  readonly env?: Readonly<Record<string, string | undefined>>;
  /** Overrides {@link DEFAULT_READY_TIMEOUT_MS}. */
  readonly readyTimeoutMs?: number;
}

/** A {@link BootSpec} with every default filled in — what {@link gatewayFlags} reads. */
export interface ResolvedBootSpec {
  readonly runsRoot: string;
  readonly serviceUrl: string;
  readonly scenario: string;
  readonly scratch: string;
  readonly ownsScratch: boolean;
  readonly repoRoot: string;
  readonly upstreamTimeoutSeconds: number | undefined;
  readonly viewerPublicUrl: string | undefined;
  readonly env: Readonly<Record<string, string | undefined>>;
  readonly readyTimeoutMs: number;
}

/** A `bun` start, a CPython start and a `--port 0` bind, with slack. */
export const DEFAULT_READY_TIMEOUT_MS = 40_000;

const READY_POLL_MS = 25;

const slotName = (scenario: string, suffix: string): string => `${scenario}-${suffix}`;

/**
 * The private-file names, keyed by a **suffix** rather than by {@link Impl}.
 *
 * The pair has one slot per implementation, so "which implementation" and
 * "whose files are these" were the same question and one parameter answered
 * both.  The trio breaks that: two of its three gateways are the *same*
 * TypeScript binary pointed at different backends, and they need different
 * ready files and different derivation caches for exactly the reasons the
 * module doc gives.  So the file naming is keyed by the slot suffix, and
 * {@link readyFileFor}/{@link cacheRootFor} stay the pair's spelling of it.
 */
export const readyFileForSuffix = (scratch: string, scenario: string, suffix: string): string =>
  join(scratch, `${slotName(scenario, suffix)}.ready.json`);

/** @see {@link readyFileForSuffix} */
export const cacheRootForSuffix = (scratch: string, scenario: string, suffix: string): string =>
  join(scratch, `cache-${slotName(scenario, suffix)}`);

export const readyFileFor = (scratch: string, scenario: string, impl: Impl): string =>
  readyFileForSuffix(scratch, scenario, SLOT[impl]);

export const cacheRootFor = (scratch: string, scenario: string, impl: Impl): string =>
  cacheRootForSuffix(scratch, scenario, SLOT[impl]);

// ---------------------------------------------------------------------------
// Slots — which launcher, whose files, and what argv it adds
// ---------------------------------------------------------------------------

/**
 * One gateway's slot in a fleet.
 *
 * Three fields, and the third is the whole reason this type exists: a slot may
 * append flags the other slots do not have.  Today exactly one does — the
 * Postgres-backed TypeScript gateway — and {@link PG_ONLY_FLAGS} is the closed
 * list of what it may append.  Everything before {@link GatewaySlot.extraFlags}
 * is still built by one function body from one spec, which is the structural
 * claim `argvParity` checks.
 */
export interface GatewaySlot {
  /** Which launcher runs it, and therefore which cwd it is spawned in. */
  readonly impl: Impl;
  /** Names its private files: `<scenario>-<suffix>.ready.json`, `cache-<scenario>-<suffix>`. */
  readonly suffix: string;
  /** Appended after the shared flags.  Empty for both members of a pair. */
  readonly extraFlags: ReadonlyArray<string>;
}

/** CPython, in the pair's own slot. */
export const PYTHON_SLOT: GatewaySlot = { impl: 'python', suffix: SLOT.python, extraFlags: [] };

/** The filesystem-backed TypeScript gateway, in the pair's own slot. */
export const TS_FS_SLOT: GatewaySlot = {
  impl: 'typescript',
  suffix: SLOT.typescript,
  extraFlags: [],
};

/** The third slot's file suffix — `<scenario>-pg.ready.json`, `cache-<scenario>-pg`. */
export const PG_SLOT_SUFFIX = 'pg' as const;

/**
 * The flags the Postgres slot adds, and the *only* ones it may.
 *
 * They are TS-only by construction: `agent_eval/replay_gateway.py`'s parser has
 * nine options and knows neither of these, so they can never be part of the
 * shared array {@link gatewayFlags} builds.  {@link SLOT_SCOPED_FLAGS} is not
 * the right home for them either — that list is about two processes disagreeing
 * on a *shared* flag's value, and this is a flag one process does not have.
 */
export const PG_ONLY_FLAGS: ReadonlyArray<string> = ['--backend', '--database-url'];

/** `--backend postgres --database-url <url>`, in the third slot. */
export const tsPgSlot = (databaseUrl: string): GatewaySlot => ({
  impl: 'typescript',
  suffix: PG_SLOT_SUFFIX,
  extraFlags: ['--backend', 'postgres', '--database-url', databaseUrl],
});

/**
 * The flags, built once per slot from one function body.
 *
 * The two calls differ in a single argument.  That is what makes the parity
 * claim structural rather than aspirational: there is no second place a flag
 * could be spelled differently, because there is no second place a flag is
 * spelled.
 *
 * `--upstream-timeout-s` and `--viewer-public-url` are *omitted* rather than
 * defaulted when the caller says nothing, which puts the two implementations'
 * own defaults inside what the rig compares — the same reason
 * `local_stack.py:540-548` passes eight flags and not ten.
 */
export const gatewayFlagsForSlot = (
  spec: ResolvedBootSpec,
  slot: GatewaySlot,
): ReadonlyArray<string> => [
  '--host', GATEWAY_HOST,
  '--port', '0',
  '--service-url', spec.serviceUrl,
  '--runs-root', spec.runsRoot,
  '--cache-root', cacheRootForSuffix(spec.scratch, spec.scenario, slot.suffix),
  '--repo-root', spec.repoRoot,
  '--ready-file', readyFileForSuffix(spec.scratch, spec.scenario, slot.suffix),
  ...(spec.upstreamTimeoutSeconds === undefined
    ? []
    : ['--upstream-timeout-s', String(spec.upstreamTimeoutSeconds)]),
  ...(spec.viewerPublicUrl === undefined ? [] : ['--viewer-public-url', spec.viewerPublicUrl]),
  ...slot.extraFlags,
];

/** The pair's spelling: one implementation, one slot, no extra flags. */
export const gatewayFlags = (spec: ResolvedBootSpec, impl: Impl): ReadonlyArray<string> =>
  gatewayFlagsForSlot(spec, impl === 'python' ? PYTHON_SLOT : TS_FS_SLOT);

// ---------------------------------------------------------------------------
// argv parity
// ---------------------------------------------------------------------------

/** `['--host','127.0.0.1',…]` → `[['--host','127.0.0.1'],…]`. */
const flagPairs = (flags: ReadonlyArray<string>): ReadonlyArray<readonly [string, string]> =>
  flags.flatMap((token, index): ReadonlyArray<readonly [string, string]> =>
    index % 2 === 0 ? [[token, flags[index + 1] ?? ''] as const] : [],
  );

export interface ArgvParity {
  /** Flag names in order, from the Python side. */
  readonly pythonOrder: ReadonlyArray<string>;
  /** Flag names in order, from the TypeScript side. */
  readonly typescriptOrder: ReadonlyArray<string>;
  /** True when both processes were given the same flags in the same order. */
  readonly sameFlagOrder: boolean;
  /** Flags whose *values* matched exactly. */
  readonly identical: ReadonlyArray<string>;
  /**
   * Flags whose values differed.  Must equal {@link SLOT_SCOPED_FLAGS};
   * anything else appearing here is drift, not a waiver.
   */
  readonly divergent: ReadonlyArray<string>;
}

/**
 * Two flag arrays, compared position by position.
 *
 * Split out of {@link argvParity} so the trio can ask the same question of a
 * pair that is not a {@link GatewayPair} — the two *TypeScript* gateways — with
 * one comparison rule rather than two.
 */
export const flagsParity = (
  pythonFlags: ReadonlyArray<string>,
  typescriptFlags: ReadonlyArray<string>,
): ArgvParity => {
  const py = flagPairs(pythonFlags);
  const ts = flagPairs(typescriptFlags);
  const pythonOrder = py.map(([name]) => name);
  const typescriptOrder = ts.map(([name]) => name);
  const paired = py.map(([name, value], index) => {
    const other = ts[index];
    return { name, same: other !== undefined && other[0] === name && other[1] === value };
  });
  return {
    pythonOrder,
    typescriptOrder,
    sameFlagOrder:
      pythonOrder.length === typescriptOrder.length &&
      pythonOrder.every((name, index) => typescriptOrder[index] === name),
    identical: paired.flatMap(({ name, same }) => (same ? [name] : [])),
    divergent: paired.flatMap(({ name, same }) => (same ? [] : [name])),
  };
};

/** Read the two argvs back off a booted pair and report where they differ. */
export const argvParity = (pair: GatewayPair): ArgvParity =>
  flagsParity(pair.python.flags, pair.typescript.flags);

// ---------------------------------------------------------------------------
// The child registry
// ---------------------------------------------------------------------------

/**
 * Every child this module has spawned, registered at `Bun.spawn` and never
 * later.
 *
 * The gateway smoke rig learned this one the hard way: a registry populated
 * only once a process had proved itself healthy left two gateways running past
 * the end of a suite whose `beforeAll` threw halfway — observed, with `ps`.
 * There is no window here in which a live process is unknown to the teardown.
 *
 * A closure rather than a module-level binding, so the array can neither be
 * reassigned nor reached except through its three accessors.
 */
interface BootRegistry {
  readonly add: (child: Bun.Subprocess) => Bun.Subprocess;
  readonly killAll: () => Promise<ReadonlyArray<number>>;
}

const registry = ((): BootRegistry => {
  const children: Bun.Subprocess[] = [];
  return {
    add: (child) => {
      children.push(child);
      return child;
    },
    killAll: async () => {
      const killed = await Promise.all(
        children.map(async (child): Promise<ReadonlyArray<number>> => {
          const wasAlive = child.exitCode === null && child.signalCode === null;
          if (wasAlive) child.kill('SIGKILL');
          await child.exited;
          return wasAlive ? [child.pid] : [];
        }),
      );
      return killed.flat();
    },
  };
})();

/**
 * Put a child *another* file spawned under the same teardown, at the
 * `Bun.spawn` call.
 *
 * The registry's claim — "there is no window here in which a live process is
 * unknown to the teardown" — was true of this module and false of the
 * directory: `hunt-state-cache-fuzz.test.ts` spawns four gateways of its own
 * for the ready-file contention probes, and they were reachable only through
 * that file's own SIGINT-and-await.  That is exactly the path a `beforeAll`
 * that throws (or a setup timeout) skips, which is the case the registry exists
 * for.  Wrapping the spawn closes it; `killAllBooted()` in an `afterAll` then
 * covers every child this directory has ever started.
 *
 * Returns the child, so the call site reads `registerBooted(Bun.spawn(…))`.
 */
export const registerBooted = (child: Bun.Subprocess): Bun.Subprocess => registry.add(child);

/**
 * The unconditional safety net, for an `afterAll`.
 *
 * Returns the pids it had to `SIGKILL`.  Empty is the healthy result; anything
 * else names a leak that {@link GatewayPair.stop} did not reach.
 */
export const killAllBooted = (): Promise<ReadonlyArray<number>> => registry.killAll();

/** The pids of `pids` still visible to `ps`.  The orphan check, as a value. */
export const aliveProcesses = (pids: ReadonlyArray<number>): ReadonlyArray<number> =>
  pids.filter((pid) => {
    const probe = Bun.spawnSync(['ps', '-p', String(pid), '-o', 'pid=']);
    return probe.stdout.toString().trim() !== '';
  });

// ---------------------------------------------------------------------------
// The live stack, as read from its own ready records
// ---------------------------------------------------------------------------

/**
 * A throwing stdlib call as a value.
 *
 * Every call site below is "the file may not exist, or may be half-written" —
 * expected states of a directory another process owns, not exceptions this rig
 * should propagate.
 */
const orNull = <A>(thunk: () => A): A | null => {
  const outcome = Either.try(thunk);
  return Either.isRight(outcome) ? outcome.right : null;
};

/**
 * One `local_stack` ready record, reduced to what a safety guard needs.
 *
 * `pid` is `null` when the record does not carry one, which is treated as *not
 * provably alive* — a record with no pid cannot be aged out and would otherwise
 * claim its ports forever.
 */
export interface LiveStackRecord {
  readonly file: string;
  readonly pid: number | null;
  readonly ports: ReadonlyArray<number>;
}

/**
 * Every ready record in {@link LIVE_STACK_RECORDS_DIR}, live or stale.
 *
 * Not a port scan: the ready file *is* the stack's published identity
 * (`identity_payload`, `replay_gateway.py:1301`; `_write_ready`,
 * `local_stack.py:491`), written atomically under an exclusive lock, and
 * reading it cannot disturb anything.
 *
 * **Three keys, not one.**  A gateway record carries `port`, `url` and
 * `upstream_service_url`; a *supervisor* record carries no `port` at all — its
 * address is `internal_service_url` — and the supervisor is exactly what
 * `--service-url` points at.  A guard that looked only for a `port` key could
 * therefore never match the one record it most needs to.
 */
export const liveStackRecords = (): ReadonlyArray<LiveStackRecord> =>
  (orNull(() => readdirSync(LIVE_STACK_RECORDS_DIR)) ?? [])
    .filter((name) => name.endsWith('.json'))
    .flatMap((name): ReadonlyArray<LiveStackRecord> => {
      const file = join(LIVE_STACK_RECORDS_DIR, name);
      const text = orNull(() => readFileSync(file, 'utf8'));
      if (text === null) return [];
      const parsed = parseJsonObjectFromText(text);
      if (parsed === null) return [];
      return [{ file, pid: pidFromStackRecord(parsed), ports: portsFromStackRecord(parsed) }];
    });

/**
 * The ports a **running** `local_stack` currently claims.
 *
 * Liveness is the second half of the guard and it is not optional: these
 * records are removed on a clean shutdown and left behind on a crash, so this
 * directory accumulates months of dead stacks — 13 files on this checkout, 4 of
 * them live.  Without the `ps` test the rig would refuse ports nothing holds
 * (and, worse, would report "a local_stack is RUNNING" in its banner when none
 * is), and with it the refusal names only sockets that actually exist.
 */
export const liveStackPorts = (): ReadonlyArray<number> => {
  const records = liveStackRecords().filter((record) => record.pid !== null);
  const alive = new Set(aliveProcesses(records.flatMap((record) => (record.pid === null ? [] : [record.pid]))));
  return Array.from(
    new Set(
      records.flatMap((record) => (record.pid !== null && alive.has(record.pid) ? record.ports : [])),
    ),
  ).toSorted((left, right) => left - right);
};

/**
 * Refuse a `--service-url` that names a port the running stack holds.
 *
 * {@link insideLiveStack} guards the two *paths*; this guards the one
 * *address*, and until it existed nothing did — a caller could hand
 * {@link bootGatewayPair} the user's live supervisor and the rig would proxy a
 * real match through two test gateways.
 */
const liveStackServiceUrl = (serviceUrl: string): ReadonlyArray<number> => {
  const port = portsFromServiceUrl(serviceUrl)[0];
  return port === undefined ? [] : liveStackPorts().filter((claimed) => claimed === port);
};

// ---------------------------------------------------------------------------
// Booting one gateway
// ---------------------------------------------------------------------------

/** One running gateway, and everything a leg or an assertion needs from it. */
export interface BootedGateway {
  readonly impl: Impl;
  /** The full argv, launcher included. */
  readonly argv: ReadonlyArray<string>;
  /** The shared-shape flags — what {@link argvParity} compares. */
  readonly flags: ReadonlyArray<string>;
  readonly process: Bun.Subprocess;
  readonly pid: number;
  /** The port the kernel actually handed out, read from the ready file. */
  readonly port: number;
  /** `http://127.0.0.1:<port>` — what `wire-client.ts` connects to. */
  readonly origin: string;
  readonly readyFile: string;
  readonly cacheRoot: string;
  readonly readyRecord: GatewayReadyRecord;
  /** The child's stdout, once it has exited.  Consume once. */
  readonly stdout: () => Promise<string>;
  readonly stderr: () => Promise<string>;
}

/** Why a gateway never reached a published ready record. */
export interface BootFailure {
  readonly _tag: 'BootFailed';
  readonly impl: Impl;
  readonly reason: string;
  readonly argv: ReadonlyArray<string>;
  readonly exitCode: number | null;
  readonly stderr: string;
}

export type BootResult = { readonly _tag: 'Booted'; readonly gateway: BootedGateway } | BootFailure;

/** The outcome of waiting on a ready file — tagged, so `reason` cannot collide
 * with a field of the record itself. */
export type ReadyOutcome =
  | { readonly _tag: 'Ready'; readonly record: GatewayReadyRecord }
  | { readonly _tag: 'NotReady'; readonly reason: string };

const readReadyRecord = async (path: string): Promise<GatewayReadyRecord | null> => {
  const file = Bun.file(path);
  if (!(await file.exists())) return null;
  // `.then(ok, fail)` rather than a `try` block: an absent or half-written
  // record is an expected poll result, not an exception.
  const parsed = await file.json().then(
    (value) => (isJsonValue(value) ? value : null),
    () => null,
  );
  return parsed === null ? null : parseGatewayReadyRecord(parsed);
};

/**
 * Poll for the ready record, giving up early when the child dies.
 *
 * The early exit matters more than the deadline does: a gateway that refuses
 * its configuration exits 2 in milliseconds, and waiting the full forty seconds
 * to then report "ready file never appeared" would bury the `error: …` line
 * that says exactly what was wrong.
 */
export const awaitReady = async (
  child: Bun.Subprocess,
  path: string,
  deadlineMs: number,
): Promise<ReadyOutcome> => {
  const deadline = Date.now() + deadlineMs;
  const poll = async (): Promise<ReadyOutcome> => {
    const record = await readReadyRecord(path);
    if (record !== null) {
      return { _tag: 'Ready', record };
    }
    if (child.exitCode !== null) {
      return { _tag: 'NotReady', reason: `exited ${String(child.exitCode)} before publishing ${path}` };
    }
    if (Date.now() > deadline) {
      return { _tag: 'NotReady', reason: `ready file never appeared within ${deadlineMs}ms: ${path}` };
    }
    await Bun.sleep(READY_POLL_MS);
    return poll();
  };
  return poll();
};

const bootGateway = async (spec: ResolvedBootSpec, slot: GatewaySlot): Promise<BootResult> => {
  const impl = slot.impl;
  const flags = gatewayFlagsForSlot(spec, slot);
  const argv = [...(impl === 'python' ? PYTHON_LAUNCHER : TYPESCRIPT_LAUNCHER), ...flags];
  const readyFile = readyFileForSuffix(spec.scratch, spec.scenario, slot.suffix);
  const cacheRoot = cacheRootForSuffix(spec.scratch, spec.scenario, slot.suffix);
  const child = registry.add(
    Bun.spawn(argv, {
      // CPython needs the checkout on `sys.path` for `-m agent_eval…`; the
      // TypeScript entry point is absolute but still resolves `node_modules`
      // from its package.
      cwd: impl === 'python' ? spec.repoRoot : HARNESS_ROOT,
      stdout: 'pipe',
      stderr: 'pipe',
      env: {
        ...process.env,
        // The gateways must not inherit the rig's telemetry configuration: the
        // Python has none, so a corpus directory appearing beside one of them
        // would be a filesystem difference the parity claim does not want.
        ARENA_GATEWAY_TELEMETRY_DIR: undefined,
        ...spec.env,
      },
    }),
  );
  const stdout = (): Promise<string> => pipedStreamText(child.stdout);
  const stderr = (): Promise<string> => pipedStreamText(child.stderr);
  const abandon = async (reason: string): Promise<BootFailure> => {
    if (child.exitCode === null) child.kill('SIGKILL');
    await child.exited;
    return { _tag: 'BootFailed', impl, reason, argv, exitCode: child.exitCode, stderr: await stderr() };
  };

  const outcome = await awaitReady(child, readyFile, spec.readyTimeoutMs);
  if (outcome._tag === 'NotReady') return abandon(outcome.reason);
  const port = outcome.record.port;
  const url = outcome.record.url;
  return {
    _tag: 'Booted',
    gateway: {
      impl,
      argv,
      flags,
      process: child,
      pid: child.pid,
      port,
      origin: url,
      readyFile,
      cacheRoot,
      readyRecord: outcome.record,
      stdout,
      stderr,
    },
  };
};

// ---------------------------------------------------------------------------
// The pair
// ---------------------------------------------------------------------------

/** A value keyed by implementation, which is most of what a rig reports. */
export type ByImpl<A> = Readonly<Record<Impl, A>>;

const byImpl = <A>(python: A, typescript: A) => ({ python, typescript }) satisfies ByImpl<A>;

export interface GatewayPair {
  readonly scenario: string;
  readonly scratch: string;
  readonly runsRoot: string;
  readonly serviceUrl: string;
  readonly python: BootedGateway;
  readonly typescript: BootedGateway;
  /** Both, in a fixed order, for a rig that iterates rather than names. */
  readonly both: readonly [BootedGateway, BootedGateway];
  /**
   * Empty both derivation caches, making the *next* request cold on both sides.
   *
   * A cold/warm leg pair is `await pair.freshCaches()` and then the same request
   * twice: the first pays for `python3 -m agent_eval.replay_derive_cli`, the
   * second reads what it wrote.  Safe to call while both gateways serve —
   * neither process holds a descriptor inside `cache_root` (the gateway creates
   * the directory at startup and never reads or writes within it; only the
   * derivation subprocess does), so removing and recreating it races nothing.
   *
   * Returns the two cache roots, now existing and empty.
   */
  readonly freshCaches: () => Promise<ReadonlyArray<string>>;
  /** SIGINT both, wait for both, and report.  Idempotent. */
  readonly stop: () => Promise<StopReport>;
  /** Remove the scratch directory, if this module created it. */
  readonly cleanup: () => void;
}

export type PairResult =
  | { readonly _tag: 'Booted'; readonly pair: GatewayPair }
  /** The rig refused to run at all — see {@link insideLiveStack}. */
  | { readonly _tag: 'Refused'; readonly reason: string; readonly cleanup: () => void }
  | {
      readonly _tag: 'BootFailed';
      readonly failures: ReadonlyArray<BootFailure>;
      readonly cleanup: () => void;
    };

/** What a stopped pair leaves behind, all of it assertable. */
export interface StopReport {
  readonly scenario: string;
  readonly exitCodes: ByImpl<number | null>;
  readonly stdout: ByImpl<string>;
  readonly stderr: ByImpl<string>;
  /** Both gateways unlink their own ready record on a clean exit. */
  readonly readyFilesRemoved: ByImpl<boolean>;
  /** Pids still visible to `ps` after both were waited on.  Must be empty. */
  readonly orphans: ReadonlyArray<number>;
}

/**
 * Refuse anything under the live stack's state directory.
 *
 * Not paranoia: a running `local_stack` keeps its gateway's ready file, its
 * derivation cache and the runs of a *live game* in `.agent-eval/`, and a rig
 * that pointed `--cache-root` there could poison a real match's replay.  The
 * check is on the resolved path, so `..` cannot walk into it.
 */
const insideLiveStack = (path: string): boolean => {
  const resolved = resolve(path);
  return resolved === LIVE_STACK_DIR || resolved.startsWith(LIVE_STACK_DIR + sep);
};

const resolveSpec = (spec: BootSpec): ResolvedBootSpec => {
  const ownsScratch = spec.scratch === undefined;
  // `realpath` because macOS's `/var/folders/…` is itself a symlink and both
  // gateways resolve their path flags: an unresolved scratch would make the two
  // `/health` bodies disagree for reasons having nothing to do with the port.
  const scratch =
    spec.scratch ?? realpathSync(mkdtempSync(join(tmpdir(), `arena-parity-${spec.scenario}-`)));
  return {
    runsRoot: spec.runsRoot,
    serviceUrl: spec.serviceUrl,
    scenario: spec.scenario,
    scratch,
    ownsScratch,
    repoRoot: spec.repoRoot ?? REPO_ROOT,
    upstreamTimeoutSeconds: spec.upstreamTimeoutSeconds,
    viewerPublicUrl: spec.viewerPublicUrl,
    env: spec.env ?? {},
    readyTimeoutMs: spec.readyTimeoutMs ?? DEFAULT_READY_TIMEOUT_MS,
  };
};

/**
 * Spawn both gateways with the same flags and wait for both ready records.
 *
 * Never throws.  A gateway that will not start comes back as a
 * {@link BootFailure} carrying its argv, its exit code and its stderr — which
 * is the whole diagnosis, in the return type.
 */
export const bootGatewayPair = async (spec: BootSpec): Promise<PairResult> => {
  const resolved = resolveSpec(spec);
  const cleanup = (): void => {
    if (resolved.ownsScratch) rmSync(resolved.scratch, { recursive: true, force: true });
  };
  const forbidden = [resolved.scratch, resolved.runsRoot].filter(insideLiveStack);
  if (forbidden.length > 0) {
    return {
      _tag: 'Refused',
      reason: `refusing to run inside the live stack state directory: ${forbidden.join(', ')}`,
      cleanup,
    };
  }
  const claimed = liveStackServiceUrl(resolved.serviceUrl);
  if (claimed.length > 0) {
    return {
      _tag: 'Refused',
      reason:
        `refusing to proxy ${resolved.serviceUrl}: a RUNNING local_stack claims port ` +
        `${claimed.join(', ')} — the user's stack and any live game are never touched`,
      cleanup,
    };
  }
  mkdirSync(resolved.scratch, { recursive: true });
  const python = await bootGateway(resolved, PYTHON_SLOT);
  const typescript = await bootGateway(resolved, TS_FS_SLOT);
  if (python._tag !== 'Booted' || typescript._tag !== 'Booted') {
    // One of them may have started; it must not outlive the other's failure.
    await Promise.all(
      [python, typescript].map(async (result) => {
        if (result._tag !== 'Booted') return;
        result.gateway.process.kill('SIGKILL');
        await result.gateway.process.exited;
      }),
    );
    return {
      _tag: 'BootFailed',
      failures: [python, typescript].flatMap((result) =>
        result._tag === 'BootFailed' ? [result] : [],
      ),
      cleanup,
    };
  }
  return { _tag: 'Booted', pair: makePair(resolved, python.gateway, typescript.gateway, cleanup) };
};

interface PairStopState {
  report: StopReport | null;
}

const makePair = (
  spec: ResolvedBootSpec,
  python: BootedGateway,
  typescript: BootedGateway,
  cleanup: () => void,
): GatewayPair => {
  // One cell, consulted by `stop`, so a second call — an `afterAll` net after a
  // test already stopped the pair — reports the first call's findings rather
  // than waiting on already-consumed streams.
  const state: PairStopState = { report: null };
  const both = [python, typescript] as const;

  const stop = async (): Promise<StopReport> => {
    if (state.report !== null) return state.report;
    // SIGINT, the way `local_stack` stops a gateway: `KeyboardInterrupt` is a
    // *clean* exit on both sides, so a non-zero code here is a real finding.
    both.forEach((gateway) => {
      if (gateway.process.exitCode === null) gateway.process.kill('SIGINT');
    });
    await Promise.all(both.map((gateway) => gateway.process.exited));
    const report: StopReport = {
      scenario: spec.scenario,
      exitCodes: byImpl(python.process.exitCode, typescript.process.exitCode),
      stdout: byImpl(await python.stdout(), await typescript.stdout()),
      stderr: byImpl(await python.stderr(), await typescript.stderr()),
      readyFilesRemoved: byImpl(
        !(await Bun.file(python.readyFile).exists()),
        !(await Bun.file(typescript.readyFile).exists()),
      ),
      // Asked only after both have been waited on, which is what makes `ps` a
      // question with a right answer rather than a race.
      orphans: aliveProcesses([python.pid, typescript.pid]),
    };
    state.report = report;
    return report;
  };

  return {
    scenario: spec.scenario,
    scratch: spec.scratch,
    runsRoot: spec.runsRoot,
    serviceUrl: spec.serviceUrl,
    python,
    typescript,
    both,
    freshCaches: () => {
      const roots = both.map((gateway) => gateway.cacheRoot);
      roots.forEach((root) => {
        rmSync(root, { recursive: true, force: true });
        mkdirSync(root, { recursive: true });
      });
      return Promise.resolve(roots);
    },
    stop,
    cleanup,
  };
};

/**
 * The sharp edge, for a caller that wants one.
 *
 * {@link bootGatewayPair} returns failures as values because a boot failure is
 * itself comparison material — but a `beforeAll` that cannot boot has nothing
 * to compare, and `bun:test`'s failure channel is an exception.  This is the
 * only place in these two modules that throws, and it throws the whole
 * diagnosis.
 */
export const unwrapPair = (result: PairResult): GatewayPair => {
  if (result._tag === 'Booted') return result.pair;
  result.cleanup();
  throw new Error(
    result._tag === 'Refused'
      ? result.reason
      : result.failures
          .map(
            (failure) =>
              `${failure.impl} gateway did not boot: ${failure.reason}\n` +
              `  argv: ${failure.argv.join(' ')}\n` +
              `  stderr: ${failure.stderr.trim()}`,
          )
          .join('\n'),
  );
};

/**
 * Boot, use, stop — teardown guaranteed on the failure path too.
 *
 * `.then(onOk, onError)` rather than `try`/`finally`: the outcome is held as a
 * value long enough to run the teardown, and only then is a failure re-raised.
 */
export const withGatewayPair = async <A>(
  spec: BootSpec,
  use: (pair: GatewayPair) => Promise<A>,
): Promise<A> => {
  const pair = unwrapPair(await bootGatewayPair(spec));
  const outcome = await use(pair).then(
    (value) => ({ ok: true, value }) as const,
    (error) => ({ ok: false, error }) as const,
  );
  await pair.stop();
  pair.cleanup();
  return outcome.ok ? outcome.value : Promise.reject(outcome.error);
};

// ---------------------------------------------------------------------------
// The trio — CPython, the fs-backed port, and the pg-backed port
// ---------------------------------------------------------------------------

/**
 * Three gateways over **one** run archive, two of which are the same binary.
 *
 * The pair's premise — "one flags array, two processes" — survives intact and
 * gains one clause.  `ts-pg` is handed the *same* shared flags as `ts-fs`
 * (`gatewayFlagsForSlot` is one function body, called three times) and appends
 * {@link PG_ONLY_FLAGS}; `--runs-root` in particular is byte-identical across
 * all three, because on the pg backend it is the logical scope key every row
 * carries rather than a directory to read, and `/health` publishes it verbatim.
 * A rig that gave the pg gateway a *different* `--runs-root` would compare two
 * archives and call the result parity.
 *
 * What the trio does **not** own is the database.  It takes a URL and assumes
 * the rows are already there: creating an ephemeral database, migrating it and
 * ingesting the fixture tree are the caller's, because they are the operations
 * with the safety rules attached.
 */
export type TrioSide = 'python' | 'tsFs' | 'tsPg';

/** A value keyed by trio side.  The three-way analogue of {@link ByImpl}. */
export type BySide<A> = Readonly<Record<TrioSide, A>>;

const bySide = <A>(python: A, tsFs: A, tsPg: A): BySide<A> => ({ python, tsFs, tsPg });

/** The three sides, in a fixed order, for a rig that iterates rather than names. */
export const TRIO_SIDES: ReadonlyArray<TrioSide> = ['python', 'tsFs', 'tsPg'];

/** A {@link BootSpec} plus the one thing only the third gateway needs. */
export interface TrioSpec extends BootSpec {
  /**
   * `--database-url`.  Handed to the pg slot only, and never compared, logged
   * or published: a database URL may carry credentials, which is why
   * `config.ts` holds it `Redacted` and `/health` does not report it.
   */
  readonly databaseUrl: string;
}

/** What a stopped trio leaves behind.  {@link StopReport}, three-sided. */
export interface TrioStopReport {
  readonly scenario: string;
  readonly exitCodes: BySide<number | null>;
  readonly stdout: BySide<string>;
  readonly stderr: BySide<string>;
  readonly readyFilesRemoved: BySide<boolean>;
  readonly orphans: ReadonlyArray<number>;
}

export interface GatewayTrio {
  readonly scenario: string;
  readonly scratch: string;
  readonly runsRoot: string;
  readonly serviceUrl: string;
  readonly python: BootedGateway;
  readonly tsFs: BootedGateway;
  readonly tsPg: BootedGateway;
  /** All three, in {@link TRIO_SIDES} order. */
  readonly all: readonly [BootedGateway, BootedGateway, BootedGateway];
  /**
   * Empty every derivation cache — and the pg gateway's materialization
   * directory — so the next request is cold on all three sides.
   *
   * The third removal is the one that is easy to forget.  On the pg backend the
   * python bridge does not read `--runs-root`; it reads a directory the gateway
   * materializes the saves into, whose default is `<cache-root>-saves`
   * (a *sibling*, because `save_replay._cache_directory` refuses a cache root
   * that nests with the saves directory).  Leaving it populated would make the
   * "cold" leg skip the work whose parity this rig exists to check, and the
   * skip would be invisible — a warm answer is byte-identical to a cold one,
   * which is precisely what makes the omission dangerous rather than merely
   * wrong.
   */
  readonly freshCaches: () => Promise<ReadonlyArray<string>>;
  readonly stop: () => Promise<TrioStopReport>;
  readonly cleanup: () => void;
}

export type TrioResult =
  | { readonly _tag: 'Booted'; readonly trio: GatewayTrio }
  | { readonly _tag: 'Refused'; readonly reason: string; readonly cleanup: () => void }
  | {
      readonly _tag: 'BootFailed';
      readonly failures: ReadonlyArray<BootFailure>;
      readonly cleanup: () => void;
    };

/**
 * The materialization directory the pg gateway derives from a cache root.
 *
 * Spelled here because the rig has to *remove* it and the gateway has to
 * *create* it, and a rig that guessed a different name would silently stop
 * making cold legs cold.
 */
export const materializeRootFor = (cacheRoot: string): string => `${cacheRoot}-saves`;

const makeTrio = (
  spec: ResolvedBootSpec,
  python: BootedGateway,
  tsFs: BootedGateway,
  tsPg: BootedGateway,
  cleanup: () => void,
): GatewayTrio => {
  const state: { report: TrioStopReport | null } = { report: null };
  const all = [python, tsFs, tsPg] as const;

  const stop = async (): Promise<TrioStopReport> => {
    if (state.report !== null) return state.report;
    all.forEach((gateway) => {
      if (gateway.process.exitCode === null) gateway.process.kill('SIGINT');
    });
    await Promise.all(all.map((gateway) => gateway.process.exited));
    const report: TrioStopReport = {
      scenario: spec.scenario,
      exitCodes: bySide(python.process.exitCode, tsFs.process.exitCode, tsPg.process.exitCode),
      stdout: bySide(await python.stdout(), await tsFs.stdout(), await tsPg.stdout()),
      stderr: bySide(await python.stderr(), await tsFs.stderr(), await tsPg.stderr()),
      readyFilesRemoved: bySide(
        !(await Bun.file(python.readyFile).exists()),
        !(await Bun.file(tsFs.readyFile).exists()),
        !(await Bun.file(tsPg.readyFile).exists()),
      ),
      orphans: aliveProcesses(all.map((gateway) => gateway.pid)),
    };
    state.report = report;
    return report;
  };

  return {
    scenario: spec.scenario,
    scratch: spec.scratch,
    runsRoot: spec.runsRoot,
    serviceUrl: spec.serviceUrl,
    python,
    tsFs,
    tsPg,
    all,
    freshCaches: () => {
      const roots = all.map((gateway) => gateway.cacheRoot);
      roots.forEach((root) => {
        rmSync(root, { recursive: true, force: true });
        rmSync(materializeRootFor(root), { recursive: true, force: true });
        mkdirSync(root, { recursive: true });
      });
      return Promise.resolve(roots);
    },
    stop,
    cleanup,
  };
};

/**
 * Spawn all three and wait for all three ready records.
 *
 * Never throws; the failure shape is {@link bootGatewayPair}'s, so a pg gateway
 * that refuses its configuration comes back carrying the `error: …` line it
 * printed rather than as a forty-second timeout.
 */
export const bootGatewayTrio = async (spec: TrioSpec): Promise<TrioResult> => {
  const resolved = resolveSpec(spec);
  const cleanup = (): void => {
    if (resolved.ownsScratch) rmSync(resolved.scratch, { recursive: true, force: true });
  };
  const forbidden = [resolved.scratch, resolved.runsRoot].filter(insideLiveStack);
  if (forbidden.length > 0) {
    return {
      _tag: 'Refused',
      reason: `refusing to run inside the live stack state directory: ${forbidden.join(', ')}`,
      cleanup,
    };
  }
  const claimed = liveStackServiceUrl(resolved.serviceUrl);
  if (claimed.length > 0) {
    return {
      _tag: 'Refused',
      reason:
        `refusing to proxy ${resolved.serviceUrl}: a RUNNING local_stack claims port ` +
        `${claimed.join(', ')} — the user's stack and any live game are never touched`,
      cleanup,
    };
  }
  mkdirSync(resolved.scratch, { recursive: true });
  const python = await bootGateway(resolved, PYTHON_SLOT);
  const tsFs = await bootGateway(resolved, TS_FS_SLOT);
  const tsPg = await bootGateway(resolved, tsPgSlot(spec.databaseUrl));
  const results = [python, tsFs, tsPg];
  if (results.some((result) => result._tag !== 'Booted')) {
    await Promise.all(
      results.map(async (result) => {
        if (result._tag !== 'Booted') return;
        result.gateway.process.kill('SIGKILL');
        await result.gateway.process.exited;
      }),
    );
    return {
      _tag: 'BootFailed',
      failures: results.flatMap((result) => (result._tag === 'BootFailed' ? [result] : [])),
      cleanup,
    };
  }
  return python._tag === 'Booted' && tsFs._tag === 'Booted' && tsPg._tag === 'Booted'
    ? {
        _tag: 'Booted',
        trio: makeTrio(resolved, python.gateway, tsFs.gateway, tsPg.gateway, cleanup),
      }
    : { _tag: 'BootFailed', failures: [], cleanup };
};

/** {@link unwrapPair}, for the trio.  The only place this section throws. */
export const unwrapTrio = (result: TrioResult): GatewayTrio => {
  if (result._tag === 'Booted') return result.trio;
  result.cleanup();
  throw new Error(
    result._tag === 'Refused'
      ? result.reason
      : result.failures
          .map(
            (failure) =>
              `${failure.impl} gateway did not boot: ${failure.reason}\n` +
              `  argv: ${failure.argv.join(' ')}\n` +
              `  stderr: ${failure.stderr.trim()}`,
          )
          .join('\n'),
  );
};

/** What the trio's three argvs agree and disagree on. */
export interface TrioArgvParity {
  /** CPython against the fs-backed port — {@link argvParity}, unchanged. */
  readonly pair: ArgvParity;
  /** Flags `ts-pg` carries that `ts-fs` does not.  Must be {@link PG_ONLY_FLAGS}. */
  readonly pgOnly: ReadonlyArray<string>;
  /** Shared flags whose values matched between the two TypeScript gateways. */
  readonly pgSharedIdentical: ReadonlyArray<string>;
  /**
   * Shared flags whose values differed between the two TypeScript gateways.
   * Must be {@link SLOT_SCOPED_FLAGS} — and for the same reasons: a shared
   * ready file is an exclusive `flock` one of them loses, and a shared
   * derivation cache would let whichever answered first answer for the other,
   * which on *this* rig would fabricate the exact parity it is measuring.
   */
  readonly pgSharedDivergent: ReadonlyArray<string>;
}

/** Read the three argvs back off a booted trio and report where they differ. */
export const trioArgvParity = (trio: GatewayTrio): TrioArgvParity => {
  const fs = flagPairs(trio.tsFs.flags);
  const pg = flagPairs(trio.tsPg.flags);
  const fsNames = fs.map(([name]) => name);
  const shared = pg.filter(([name]) => fsNames.includes(name));
  const paired = shared.map(([name, value]) => ({
    name,
    same: fs.some(([other, otherValue]) => other === name && otherValue === value),
  }));
  return {
    pair: flagsParity(trio.python.flags, trio.tsFs.flags),
    pgOnly: pg.flatMap(([name]) => (fsNames.includes(name) ? [] : [name])),
    pgSharedIdentical: paired.flatMap(({ name, same }) => (same ? [name] : [])),
    pgSharedDivergent: paired.flatMap(({ name, same }) => (same ? [] : [name])),
  };
};

