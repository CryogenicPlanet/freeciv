/**
 * Boot both gateways from **one** flags array.
 *
 * The parity rig's first assertion is not about a response body — it is about
 * the command line.  Two implementations answering identically to two
 * *different* configurations proves nothing, and the way that happens in
 * practice is not malice but drift: a flag renamed on one side, a default that
 * moved, an argument order that stopped mattering.  So this module builds the
 * flags **once**, from one function body, and hands the result to two binaries:
 *
 * ```
 * python3 -m agent_eval.replay_gateway   <flags>
 * bun     src/gateway/main.ts            <flags>
 * ```
 *
 * {@link argvParity} then reads the two argvs back off the booted pair and
 * reports which flags differ.  Exactly two may: {@link SLOT_SCOPED_FLAGS}.
 *
 * ## Why `--cache-root` and `--ready-file` *must* differ
 *
 * They are the only two flags naming a resource a second process cannot share:
 *
 * - **`--ready-file`** is held under an exclusive `flock` for the process
 *   lifetime (`services/ready-file.ts`).  A shared one is not a subtle bug: the
 *   second gateway refuses to start and exits 2.
 * - **`--cache-root`** is where the derivation subprocess writes
 *   `replay.json`/`board.json`/`events.json`, keyed by game and turn.  Sharing
 *   it would let whichever gateway ran first *answer for the other one*, and
 *   the rig would then report perfect parity on a body only one implementation
 *   ever produced.  That is precisely the failure a parity rig exists to not
 *   have.
 *
 * Every other flag — host, port, service URL, runs root, repo root, and the two
 * optional ones — is byte-identical between the two processes.
 *
 * ## Ports come from the ready files, not from a guess
 *
 * Both are spawned with `--port 0`, so the kernel picks.  The bound port is
 * published in the ready record (`identity_payload`, `replay_gateway.py:1301`),
 * written atomically under the lock *before* the socket serves.
 * {@link awaitReady} polls for it with a deadline and gives up early when the
 * child exits first, so a gateway that refuses its configuration is reported as
 * "exited 2, stderr: …" instead of as a forty-second timeout.
 *
 * ## The user's stack is never touched
 *
 * Every process here binds `--port 0` and writes under a private `mkdtemp`
 * scratch.  {@link bootGatewayPair} additionally *refuses* two things:
 *
 * 1. a runs root or a scratch inside the repo's `.agent-eval/` — that directory
 *    belongs to a running `local_stack`, and a rig writing a cache or a ready
 *    file into it could disturb a live game;
 * 2. a `--service-url` whose port a **running** stack claims
 *    ({@link liveStackPorts}), which is the only thing standing between a
 *    mistyped `PARITY_SERVICE_URL` and a real match's supervisor.
 *
 * Teardown kills only the pids this module spawned, and
 * {@link StopReport.orphans} is the proof that it did.
 *
 * Run `bun test/parity/boot.ts` for the module's self-test.
 *
 * @module
 */

import { Either } from 'effect';
import { mkdirSync, mkdtempSync, readdirSync, readFileSync, realpathSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve, sep } from 'node:path';
import { describeOutcome, isWireResponse, wireGet } from './wire-client.ts';
import type { WireOutcome } from './wire-client.ts';

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

/**
 * Where a running `local_stack` publishes its ready records — one directory
 * *below* {@link LIVE_STACK_DIR}.
 *
 * `agent_eval/local_stack.py:491-492` builds `stack_root = state_dir /
 * "local-stack"` and writes `supervisor-{pid}-{token}.json` and
 * `gateway-{pid}-{token}.json` into it.  Neither name ends in `.ready.json`,
 * and neither file is in `.agent-eval/` itself — where the only `*.ready.json`
 * files are hand-made leftovers from other rigs.  Reading the wrong directory
 * with the wrong pattern is not a near miss: it finds every dead file and none
 * of the live ones, which is a guard that reports "safe" precisely when it is
 * wrong.  Measured on this checkout before the fix: `[57922, 57922]` (a gateway
 * that exited on 2 August) while four records of two *running* stacks sat
 * unread.
 */
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
 * RFC 5737 TEST-NET-1 — reserved for documentation, guaranteed unroutable.
 *
 * The correct "upstream is down" fixture, and the reason is a measured trap: a
 * *released ephemeral port* is not one.  Bind `127.0.0.1:0`, read the port,
 * close the socket and hand that URL to a gateway, and the kernel is free to
 * hand the same port to the next listener — which, often enough, is the gateway
 * itself.  It then proxies to itself, and the rig either deadlocks or reports a
 * fabricated success.  An address that cannot be routed cannot be reused.
 *
 * Pair it with {@link UNROUTABLE_UPSTREAM_TIMEOUT_S}: a connect here hangs until
 * *someone's* timeout fires, and it must be the gateway's.
 *
 * ## What this fixture is *for*, measured
 *
 * A refused connect and a hung connect are different branches, and only this
 * one exercises `--upstream-timeout-s`.  Against `GET /v1/games`, with the
 * gateways booted by this module:
 *
 * | upstream | `-s` | Python | TypeScript |
 * |---|---|---|---|
 * | {@link REFUSED_UPSTREAM_URL} | default | `200` in 3ms | `200` in 4ms |
 * | {@link REFUSED_UPSTREAM_URL} | 1 | `200` in 1ms | `200` in 4ms |
 * | this | 1 | `200` in 1007ms | `200` in 1030ms |
 * | this | 3 | `200` in 3006ms | `200` in ~3020ms |
 *
 * Both honour the flag and then serve the disk fallback.  That third row is
 * what this fixture exists for, and it is the row that was **red** when the rig
 * was first run: the TypeScript side answered *nothing* and the connection was
 * closed at 12s, whatever `-s` said, because
 * `HttpApp.toHandled` runs a handler uninterruptibly and an `Effect.timeout`
 * inside an uninterruptible region waits for the arm it just cancelled (see
 * `src/gateway/server.ts#gatewayApp`).  A rig that only ever used a *refused*
 * upstream (as the earlier gateway smoke rig did) reports parity here, because
 * a refusal is instant on both sides and never reaches the timeout path at all.
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

/** The suffix each implementation's private files carry. */
const SLOT: Readonly<Record<Impl, string>> = { python: 'py', typescript: 'ts' };

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

const slotName = (scenario: string, impl: Impl): string => `${scenario}-${SLOT[impl]}`;

export const readyFileFor = (scratch: string, scenario: string, impl: Impl): string =>
  join(scratch, `${slotName(scenario, impl)}.ready.json`);

export const cacheRootFor = (scratch: string, scenario: string, impl: Impl): string =>
  join(scratch, `cache-${slotName(scenario, impl)}`);

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
export const gatewayFlags = (spec: ResolvedBootSpec, impl: Impl): ReadonlyArray<string> => [
  '--host', GATEWAY_HOST,
  '--port', '0',
  '--service-url', spec.serviceUrl,
  '--runs-root', spec.runsRoot,
  '--cache-root', cacheRootFor(spec.scratch, spec.scenario, impl),
  '--repo-root', spec.repoRoot,
  '--ready-file', readyFileFor(spec.scratch, spec.scenario, impl),
  ...(spec.upstreamTimeoutSeconds === undefined
    ? []
    : ['--upstream-timeout-s', String(spec.upstreamTimeoutSeconds)]),
  ...(spec.viewerPublicUrl === undefined ? [] : ['--viewer-public-url', spec.viewerPublicUrl]),
];

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

/** Read the two argvs back off a booted pair and report where they differ. */
export const argvParity = (pair: GatewayPair): ArgvParity => {
  const py = flagPairs(pair.python.flags);
  const ts = flagPairs(pair.typescript.flags);
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
const registry = ((): {
  readonly add: (child: Bun.Subprocess) => Bun.Subprocess;
  readonly pids: () => ReadonlyArray<number>;
  readonly killAll: () => Promise<ReadonlyArray<number>>;
} => {
  const children: Bun.Subprocess[] = [];
  return {
    add: (child) => {
      children.push(child);
      return child;
    },
    pids: () => children.map((child) => child.pid),
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

/** Every pid this module has spawned in this process, healthy or not. */
export const bootedPids = (): ReadonlyArray<number> => registry.pids();

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

/** The `port` of a `http://127.0.0.1:NNNNN` service URL, when it has one. */
const urlPort = (value: unknown): ReadonlyArray<number> => {
  if (typeof value !== 'string' || !URL.canParse(value)) return [];
  const port = Number(new URL(value).port);
  return Number.isInteger(port) && port > 0 ? [port] : [];
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
      const parsed: unknown = text === null ? null : orNull(() => JSON.parse(text) as unknown);
      // A predicate rather than an assertion: the value came off a directory
      // another process owns, so nothing about its shape is guaranteed.
      if (!isRecord(parsed)) return [];
      const port = parsed['port'];
      const ports = [
        ...(typeof port === 'number' ? [port] : []),
        ...urlPort(parsed['url']),
        ...urlPort(parsed['internal_service_url']),
        ...urlPort(parsed['upstream_service_url']),
      ];
      const pid = parsed['pid'];
      return [{ file, pid: typeof pid === 'number' ? pid : null, ports }];
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
  const port = urlPort(serviceUrl)[0];
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
  readonly readyRecord: Readonly<Record<string, unknown>>;
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
  | { readonly _tag: 'Ready'; readonly record: Readonly<Record<string, unknown>> }
  | { readonly _tag: 'NotReady'; readonly reason: string };

/**
 * A predicate rather than an assertion: the value came off the disk, and
 * claiming a shape for it with `as` would make every downstream read of the
 * record unchecked.  The two fields that matter — `port` and `url` — are still
 * `typeof`-tested individually below.
 */
const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === 'object' && value !== null && !Array.isArray(value);

const readReadyRecord = async (path: string): Promise<Readonly<Record<string, unknown>> | null> => {
  const file = Bun.file(path);
  if (!(await file.exists())) return null;
  // `.then(ok, fail)` rather than a `try` block: an absent or half-written
  // record is an expected poll result, not an exception.
  const parsed: unknown = await file.json().then(
    (value: unknown) => value,
    () => null,
  );
  return isRecord(parsed) ? parsed : null;
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
    if (record !== null && typeof record['url'] === 'string') {
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

/**
 * `Bun.Subprocess`'s streams are typed as a union over every `stdio` option;
 * `'pipe'` — the one asked for below — is the `ReadableStream` arm, and
 * `instanceof` is how that is established rather than asserted.
 */
const streamText = (stream: unknown): Promise<string> =>
  stream instanceof ReadableStream ? new Response(stream).text() : Promise.resolve('');

const bootGateway = async (spec: ResolvedBootSpec, impl: Impl): Promise<BootResult> => {
  const flags = gatewayFlags(spec, impl);
  const argv = [...(impl === 'python' ? PYTHON_LAUNCHER : TYPESCRIPT_LAUNCHER), ...flags];
  const readyFile = readyFileFor(spec.scratch, spec.scenario, impl);
  const cacheRoot = cacheRootFor(spec.scratch, spec.scenario, impl);
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
  const stdout = (): Promise<string> => streamText(child.stdout);
  const stderr = (): Promise<string> => streamText(child.stderr);
  const abandon = async (reason: string): Promise<BootFailure> => {
    if (child.exitCode === null) child.kill('SIGKILL');
    await child.exited;
    return { _tag: 'BootFailed', impl, reason, argv, exitCode: child.exitCode, stderr: await stderr() };
  };

  const outcome = await awaitReady(child, readyFile, spec.readyTimeoutMs);
  if (outcome._tag === 'NotReady') return abandon(outcome.reason);
  const port = outcome.record['port'];
  const url = outcome.record['url'];
  if (typeof port !== 'number' || typeof url !== 'string') {
    return abandon(`ready record lacks a numeric port and a string url: ${readyFile}`);
  }
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

const byImpl = <A>(python: A, typescript: A): ByImpl<A> => ({ python, typescript });

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
  const python = await bootGateway(resolved, 'python');
  const typescript = await bootGateway(resolved, 'typescript');
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

const makePair = (
  spec: ResolvedBootSpec,
  python: BootedGateway,
  typescript: BootedGateway,
  cleanup: () => void,
): GatewayPair => {
  // One cell, consulted by `stop`, so a second call — an `afterAll` net after a
  // test already stopped the pair — reports the first call's findings rather
  // than waiting on already-consumed streams.
  const state: { report: StopReport | null } = { report: null };
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
    (error: unknown) => ({ ok: false, error }) as const,
  );
  await pair.stop();
  pair.cleanup();
  return outcome.ok ? outcome.value : Promise.reject(outcome.error);
};

// ---------------------------------------------------------------------------
// Self-test
// ---------------------------------------------------------------------------

/**
 * What `bun test/parity/boot.ts` checks, as a value — so a `.test.ts` in this
 * directory can assert on it without re-deriving the rig.
 */
export interface SelfTestReport {
  readonly readyFilesAppeared: ByImpl<boolean>;
  readonly ports: ByImpl<number>;
  readonly portsDiffer: boolean;
  /** The `/health` status, or the failure tag when there was no response. */
  readonly health: ByImpl<number | string>;
  /** Which framing rule ended each read — `content-length` is the law working. */
  readonly healthCompletedBy: ByImpl<string>;
  readonly argv: ArgvParity;
  /** Both cache roots exist and are empty after `freshCaches()`. */
  readonly cachesFresh: boolean;
  readonly stop: StopReport;
  /** Pids the safety net had to `SIGKILL`.  Must be empty. */
  readonly killedByNet: ReadonlyArray<number>;
  readonly ok: boolean;
}

/** A `/health` status, or the failure tag when there was no response at all. */
const outcomeStatus = (outcome: WireOutcome): number | string =>
  isWireResponse(outcome) ? outcome.status : outcome._tag;

/** Which framing rule ended the read — `content-length` is the law working. */
const outcomeFraming = (outcome: WireOutcome): string =>
  isWireResponse(outcome) ? outcome.completedBy : describeOutcome(outcome);

/**
 * Boot both against one empty fixture root, hit `/health` on each, and report.
 *
 * The upstream is the unroutable fixture with a one-second timeout: `/health`
 * is served locally by both gateways and never proxies, so no stub is needed
 * and no port is bound beyond the two `--port 0` listeners this function owns
 * and tears down.
 */
export const selfTest = async (): Promise<SelfTestReport> => {
  const runsRoot = realpathSync(mkdtempSync(join(tmpdir(), 'arena-parity-runs-')));
  const pair = unwrapPair(
    await bootGatewayPair({
      runsRoot,
      serviceUrl: UNROUTABLE_UPSTREAM_URL,
      upstreamTimeoutSeconds: UNROUTABLE_UPSTREAM_TIMEOUT_S,
      scenario: 'selftest',
    }),
  );
  const readyFilesAppeared = byImpl(
    await Bun.file(pair.python.readyFile).exists(),
    await Bun.file(pair.typescript.readyFile).exists(),
  );
  const pyHealth = await wireGet(pair.python.origin, '/health');
  const tsHealth = await wireGet(pair.typescript.origin, '/health');

  const cacheRoots = await pair.freshCaches();
  const cachesFresh = cacheRoots.every((root) => readdirSync(root).length === 0);
  const argv = argvParity(pair);
  const ports = byImpl(pair.python.port, pair.typescript.port);
  const stop = await pair.stop();
  pair.cleanup();
  rmSync(runsRoot, { recursive: true, force: true });
  const killedByNet = await killAllBooted();

  return {
    readyFilesAppeared,
    ports,
    portsDiffer: ports.python !== ports.typescript,
    health: byImpl(outcomeStatus(pyHealth), outcomeStatus(tsHealth)),
    healthCompletedBy: byImpl(outcomeFraming(pyHealth), outcomeFraming(tsHealth)),
    argv,
    cachesFresh,
    stop,
    killedByNet,
    ok:
      readyFilesAppeared.python &&
      readyFilesAppeared.typescript &&
      ports.python !== ports.typescript &&
      outcomeStatus(pyHealth) === 200 &&
      outcomeStatus(tsHealth) === 200 &&
      argv.sameFlagOrder &&
      argv.divergent.join(',') === SLOT_SCOPED_FLAGS.join(',') &&
      cachesFresh &&
      stop.exitCodes.python === 0 &&
      stop.exitCodes.typescript === 0 &&
      stop.readyFilesRemoved.python &&
      stop.readyFilesRemoved.typescript &&
      stop.orphans.length === 0 &&
      killedByNet.length === 0,
  };
};

if (import.meta.main) {
  const report = await selfTest();
  // This file's evidence has to reach a terminal, and there is no Logger in a
  // bare `bun` script.
  // oxlint-disable-next-line effecttsgo/global-console
  console.log(JSON.stringify(report, null, 2));
  process.exit(report.ok ? 0 : 1);
}
