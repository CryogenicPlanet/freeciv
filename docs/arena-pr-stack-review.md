# Arena TypeScript migration — PR stack review

**Reviewed stack:** PRs [#7](https://github.com/CryogenicPlanet/freeciv/pull/7) → [#8](https://github.com/CryogenicPlanet/freeciv/pull/8) → [#9](https://github.com/CryogenicPlanet/freeciv/pull/9) → [#10](https://github.com/CryogenicPlanet/freeciv/pull/10)

**Review axes:**

1. Behavioral parity with the Python server and client.
2. Effect TypeScript style and architecture.
3. Migration scaffolding or code paths that should not remain in the final artifact.

**Scope note:** The original review covered committed PR diffs only. Uncommitted PostgreSQL work was excluded and remains outside this document's update.

## Fix status update (PR #10)

The PR #10 follow-up is implemented in the parity-rig worktree:

- **Resolved:** the full parity boot gates now support both Linux and Darwin. `ARENA_REQUIRE_PARITY=1` makes an unsupported-platform skip fail.
- **Resolved:** raw `/v1/games/A/../B/...` is an ordinary equality leg after the PR #9 Node raw-target fix; both gateways refuse it. The referred disposition and exclusion were removed.
- **Resolved:** the redundant header/method hunt wrapper and the query hunt's historical `DIVERGENT_AT_MEASUREMENT` registry/meta-tests were removed. Unique fixed, waiver, traversal, and derive seeds remain, including waiver self-invalidation.
- **Remeasured:** self-invalidation removed the now-equal duplicate-`Content-Length` and binary framing waivers; their request/frame seeds are ordinary parity assertions now. The invented-verb runtime waiver was updated from Bun's old closed connection to Node's measured bare `400`.
- **Still tracked elsewhere:** findings not assigned to this PR #10 cleanup remain historical review findings below; this update does not claim that all PR #7–#9 concerns are closed.

Verification results for this update are recorded at the end of the document.

## Executive summary

The stack has strong Effect fundamentals and a serious differential parity rig. The two PR #10 acceptance blockers identified by the original review—Darwin-only parity gates and the referred cross-game raw-path divergence—are now resolved. Remaining stack-wide release findings below retain their original severity unless explicitly marked resolved.

## Release blockers

### 1. The required ready-file implementation was Darwin-only — resolved

**PR:** #9  
**Status:** Resolved before/with the PR #10 follow-up

- `arena/harness/src/gateway/services/ready-file.ts:102-110`
- `arena/harness/src/gateway/services/ready-file.ts:308-324`
- `arena/harness/src/gateway/main.ts:160-170`
- Python reference: `agent_eval/replay_gateway.py:243-253`

The locking implementation hardcodes:

- `libSystem.B.dylib`
- Darwin's `__error` ABI
- Darwin `EWOULDBLOCK = 35`
- Darwin `O_CLOEXEC`

The original implementation could not run the normal TypeScript gateway on Linux. The current implementation selects Linux/Darwin libc, errno access, `EWOULDBLOCK`, and `O_CLOEXEC` values, and the parity gates now admit both supported platforms.

**Implemented direction:**

- Implement platform-specific Darwin and Linux bindings/constants, or replace the FFI implementation with a portable locking abstraction.
- Preserve interoperability with Python `fcntl.flock`.
- Run ready-file contention tests in both directions on Linux.
- Make the full gateway and parity suite mandatory in Linux CI.
- Do not silently disable ready-file publication on Linux; it is part of the launcher contract.

### 2. Cross-game dot-segment normalization served another game — resolved

**PR:** #10  
**Status:** Resolved

- `arena/harness/test/parity/hunt-query-fuzz.test.ts:723-736`
- `arena/harness/test/parity/hunt-query-fuzz.test.ts:928-941`

A request shaped like:

```text
/v1/games/A/../B/board.json?turn=1
```

produces:

- Python: `400`
- TypeScript: `200`, serving game B

The PR #9 Node server edge now reads `IncomingMessage.url`, preserving literal dot segments for dispatch instead of routing the adapter-normalized URL. The PR #10 query corpus treats the target as ordinary byte equality and asserts that both gateways refuse it; no referred branch, disposition, or traversal exclusion remains.

### 3. TypeScript adds a fixed 120-second derivation timeout

**PR:** #9  
**Severity:** High

- `arena/harness/src/gateway/services/derivation.ts:384-415`
- `arena/harness/src/gateway/services/derivation.ts:610-624`
- Python reference: `agent_eval/replay_gateway.py:264-324`, `:1700-1719`

Replay, board, and event derivation subprocesses are killed after 120 seconds. Python holds the derivation mutex without a wall-clock limit.

A valid large cold derivation can therefore return TypeScript `503` while Python eventually succeeds and populates the cache.

**Required direction:**

- Default to no derivation timeout for Python parity.
- If an operational timeout is desired, make it an explicit configuration option and document it as a behavior change rather than a parity default.

### 4. Disk JSON can lose arbitrary-precision integers

**PR:** #9  
**Severity:** High

- `arena/harness/src/gateway/services/runs.ts:287-305`
- `arena/harness/src/gateway/services/runs.ts:384-460`
- `arena/harness/src/gateway/archive.ts:267-301`
- Python reference: `agent_eval/replay_gateway.py:573-599`, `:681-705`

Manifest, report, and victory files are read through ordinary JavaScript-number JSON decoding. A later `BigInt(value)` conversion cannot recover precision already lost by `JSON.parse`.

For example, `9007199254740993` can first become `9007199254740992`.

The stack already contains `python-json.ts` to preserve Python's `int`/`float` distinction, but it is not used at every Python-authored disk boundary.

**Required direction:**

- Use the integer-preserving parser for manifest, report, victory, and all other Python-authored disk JSON.
- Add values around `2^53` to disk-to-response parity tests.

### 5. Readiness is removed before the listener closes

**PR:** #9  
**Severity:** High

- `arena/harness/src/gateway/server.ts:455-483`
- Python reference: `agent_eval/replay_gateway.py:2163-2170`

TypeScript scope finalization removes the ready file before closing the HTTP listener. Python closes the server first, then removes readiness and releases its lock.

A launcher observing file removal can begin replacement while the old listener remains bound. TypeScript also suppresses owned-ready-file unlink failures, potentially reporting clean shutdown while leaving stale readiness.

**Required direction:**

1. Stop accepting requests and close the listener.
2. Remove the owned ready record.
3. Release the ready lock.
4. Surface cleanup failures without masking the original shutdown cause.

### 6. A clean checkout skips real derivation parity

**PRs:** #9 and #10  
**Severity:** High

- `arena/harness/test/gateway/derivation.test.ts:470-490`
- `arena/harness/test/gateway/derivation.test.ts:548-576`
- `arena/harness/test/gateway/smoke-live.test.ts:169-175`

The bridge differential searches local `.agent-eval/runs`. With no suitable local run, the real loader parity tests skip. The committed save fixtures are not wired into the suite.

Observed committed-tip test result:

```text
2135 pass
5 skip
0 fail
```

The skipped cases included byte parity against the real Python save loaders.

**Required direction:**

- Build a deterministic synthetic run directory around committed `.sav.gz` fixtures.
- Require replay, board, and events differential tests in CI.
- Treat parity-suite skips as failures in the required CI job.

## Additional correctness findings

### 7. Competing technology-catalogue schemas disagree on valid IDs

**PR:** #7  
**Severity:** High

- `arena/wire/src/gateway/replay.ts:128-157`
- `arena/wire/src/gateway/manifest.ts:687-715`
- Python reference: `agent_eval/supervisor.py:424-434`

Two exported schemas model effectively the same catalogue:

- `Technology.id` accepts any non-negative integer.
- `TechnologyEntry.id` enforces `0..511`.

A caller using `decodeTechnologyCatalog` can accept `512`, while `decodeReplayCatalog` rejects it.

**Required direction:** consolidate the types into one authoritative schema and reuse it for both embedded and on-disk catalogues.

### 8. OTLP failure is falsely reported as a dropped event

**PR:** #8  
**Severity:** High

- `arena/telemetry/src/observability.ts:125-137`
- `arena/telemetry/src/middleware.ts:122-152`

The authoritative NDJSON line is written before OTLP mirroring. If mirroring fails, `record()` fails and the middleware logs:

```text
@arena/telemetry dropped a wide event
```

The event was not dropped; it was persisted locally and failed only to mirror.

**Required direction:** distinguish emit/write loss from post-persistence export failure.

### 9. Synchronous request-path filesystem work can stall the gateway

**PR:** #9  
**Severity:** Medium

- `arena/harness/src/gateway/services/runs.ts:255-305`, `:384-407`, `:742-828`
- `arena/harness/src/gateway/http/routes/archive.ts:169-193`
- Python reference: `agent_eval/replay_gateway.py:1280-1299`

The TypeScript request path performs synchronous filesystem calls on Bun's event loop. Python uses `ThreadingHTTPServer` and serializes only derivations.

A slow disk or archive read can stall unrelated health and proxy requests.

**Required direction:** use asynchronous scoped filesystem effects or isolate blocking calls on an appropriate blocking executor.

### 10. Derivation transport failures are silently degraded

**PR:** #9  
**Severity:** Medium

- `arena/harness/src/gateway/services/derivation.ts:477-492`
- `arena/harness/src/gateway/services/derivation.ts:541-559`
- Python bridge: `agent_eval/replay_derive_cli.py:97-107`, `:168-172`

Current behavior includes:

- swallowing stdin write/end errors through `Effect.ignore`
- converting reader rejection into EOF
- converting `child.exited` rejection into exit code `-1`

A failed stdin write can leave the child deriving with incomplete input or waiting until timeout.

**Required direction:** map each transport failure immediately to `DerivationUnavailable` and close stdin in a finalizer.

### 11. Startup ordering and bind-error reporting differ from Python

**PR:** #9  
**Severity:** Medium

- `arena/harness/src/gateway/server.ts:421-475`
- `arena/harness/src/gateway/main.ts:315-324`
- Python reference: `agent_eval/replay_gateway.py:2092-2094`, `:2205-2209`

TypeScript binds before creating `cache_root`; Python creates the directory first. Bind failures can also become defects rendered with `Cause.pretty` instead of Python's uniform `error: …`, exit 2 contract.

**Required direction:** validate/create the cache before binding and translate bind failures into a tagged startup error.

### 12. Upstream JSON rejects Python-accepted non-finite constants

**PR:** #9  
**Severity:** Medium

- `arena/harness/src/gateway/python-json.ts:59-65`, `:225-235`
- `arena/harness/src/gateway/http/routes/games.ts:129-145`
- Python reference: `agent_eval/replay_gateway.py:123-126`, `:1613-1639`

`parsePythonJson` deliberately rejects `NaN` and infinities. Python's default `json.loads` and `json.dumps` accept and re-emit them.

An upstream games response containing `NaN` can therefore be Python `200` but TypeScript `502`.

**Required direction:** either reproduce CPython constant handling or harden both implementations together and document the contract change.

### 13. PPM metadata parsing adds a 512 KiB cutoff

**PR:** #9  
**Severity:** Medium

- `arena/harness/src/gateway/http/routes/archive.ts:145-193`, `:234-243`
- Python reference: `agent_eval/replay_gateway.py:906-925`

TypeScript reads a 512 KiB prefix. Python reads at most 513 complete logical lines. A very long early line can hide valid metadata from TypeScript.

**Required direction:** stream a bounded number of complete lines to match Python's rule.

### 14. The ready-lock descriptor leaks if `chmod` fails

**PR:** #9  
**Severity:** Medium

- `arena/harness/src/gateway/services/ready-file.ts:383-399`
- `arena/harness/src/gateway/services/ready-file.ts:449-455`
- Python reference: `agent_eval/replay_gateway.py:243-253`

`openLockFd` opens the descriptor and then performs `chmod` through `Effect.tap`. If `chmod` fails, the descriptor has not yet reached `acquireRelease` and is not closed.

**Required direction:** bracket open plus chmod as one acquisition and close on every post-open failure.

### 15. Wire decoding is intentionally less strict than the Python client

**PR:** #7  
**Severity:** Medium / contract decision

- `arena/wire/src/tolerant.ts:31-44`
- `arena/wire/src/agent/health.ts:579-580`
- Python reference: `play/client.py:2091-2099`

Wire decoders generally preserve excess keys. Python `_exact` rejects them in many client envelopes; unknown `sidecar` keys are one concrete example.

This may be a valid forward-compatibility policy, but it is not drop-in Python validation parity.

**Required direction:** expose clearly named strict decoders for Python-client parity, or document tolerant decoding as an intentional protocol change.

### 16. Core parity was Darwin-only and optional by default — resolved

**PR:** #10  
**Status:** Resolved

- `arena/harness/test/parity/boot.ts:133-159`
- `arena/harness/test/parity/diff.test.ts:1030`
- `arena/harness/test/parity/hunt-state-cache-fuzz.test.ts:1012`

The core matrix, state/cache fuzz, and live smoke rig now run on Linux and Darwin. Other platforms remain explicitly gated, and `ARENA_REQUIRE_PARITY=1` turns that unsupported-platform skip into a failure.

## Effect TypeScript assessment

### Strengths

The production source has strong Effect discipline:

- no raw `try/catch` in TypeScript production source
- no `Effect.run*` in production source
- typed `Data.TaggedError` failures
- `Context.Tag` and `Layer` for gateway dependencies
- scoped listener, lock, stream, and subprocess lifecycles
- pure projection code remains pure
- telemetry uses `FiberRef`, `Ref`, `Clock`, `Random`, `onExit`, and layers appropriately
- the module-global `FiberRef` is per-fiber context, not ordinary mutable global state
- imperative boundaries are generally fenced with `Effect.try` or `Effect.tryPromise`

### Style and organization concerns

The implementation is over-commented. Approximate source comment density:

| Area | Comment percentage |
|---|---:|
| wire | 49% |
| telemetry | 54% |
| gateway | 45% |
| parity rig | 29% |

Keep comments that explain observable Python oddities, security invariants, or surprising resource ordering. Remove comments that preserve implementation history, adversarial-hunt narratives, rejected alternatives, or references to uncommitted “dossier” sections.

Several files are too large:

| File | Lines |
|---|---:|
| `arena/harness/src/gateway/archive.ts` | 1131 |
| `arena/wire/src/gateway/games.ts` | 1020 |
| `arena/wire/src/agent/health.ts` | 1003 |
| `arena/harness/src/gateway/config.ts` | 993 |
| `arena/harness/src/gateway/services/upstream.ts` | 893 |
| `arena/harness/src/gateway/services/runs.ts` | 851 |
| `arena/harness/src/gateway/services/ready-file.ts` | 742 |

Suggested splits:

- `config.ts`: Python numeric parsing, URL parsing, path resolution, config assembly
- `archive.ts`: status/result/watch/frame/index projections
- `health.ts`: sidecar, phase, recovery, envelope
- `runs.ts`: secure filesystem primitives versus repository API

Naming is otherwise generally strong. The clearest naming problem is the competing `TechnologyCatalog` and `ReplayCatalog` concepts.

### Lint signal

Production-source lint results:

- wire: 0 warnings
- telemetry: 0 warnings
- harness: 12 warnings

The full harness suite reports 406 warnings, mostly from parity and spike tests using Promise/async and Node APIs. “Zero lint errors” is accurate, but the stack is not warning-clean Effect code.

## Test assessment

The final stack adds approximately 73,453 net lines. Test-to-source ratios:

| Package | Test/source ratio |
|---|---:|
| wire | 1.49× |
| telemetry | 0.95× |
| harness | 2.49× |

### Retain

- core byte-level parity matrix
- self-invalidating waiver checks
- `/health` normalizer and dead-normalization alarms
- raw wire client
- state/cache concurrency tests
- representative real-run fixtures
- canonical JSON differential tests
- Effect result-preservation tests
- symlink and `O_NOFOLLOW` security tests

### Consolidate or remove

#### Historical hunt wrappers — PR #10 cleanup complete

`hunt-header-method-fuzz.test.ts` was redundant with the matrix and has been deleted. `hunt-query-fuzz.test.ts` remains because it owns unique parser-boundary, traversal, derive, and waiver seeds, but its historical `DIVERGENT_AT_MEASUREMENT` registry and disposition meta-tests were removed.

#### Duplicate native-schema fixture

- `arena/wire/test/native-schema-fixture.ts`
- `arena/harness/test/spikes/s2-native-schema-fixture.ts`

They are about 3,400 lines each and differ only slightly. Keep one authoritative fixture.

#### Redundant telemetry exit matrices

`middleware.test.ts` and `result-preserved.test.ts` both exercise success, failure, defect, and interruption across multiple backends. Keep the stronger result-preservation matrix and reduce the middleware suite to emission/outcome behavior.

#### Migration-time meta-tests

Review whether these should remain after the Python migration ends:

- `arena/wire/test/citations.test.ts`
- `arena/wire/test/schema-shape.test.ts`
- `arena/wire/test/parity-constants.test.ts`
- `arena/harness/test/gateway/invariants.test.ts`

The gateway invariant test provides lasting architectural value. Python line-number citation validation and very large schema snapshots are more likely to become maintenance burdens once Python is no longer co-maintained.

## Migration stragglers

### Definitely temporary

#### Python derivation bridge

- `agent_eval/replay_derive_cli.py`
- `ReplayDerivationPython` in `arena/harness/src/gateway/services/derivation.ts`
- bridge-specific tests and comments

These explicitly describe themselves as interim and scheduled for deletion when native derivation lands.

#### Deferred corpus command

- `agent_eval/corpus_record.py:1963-1986`
- parser registration around `agent_eval/corpus_record.py:2051`

The `live` subcommand always exits with a deferral error. Implement it or remove it before presenting the CLI as final.

#### Historical migration prose

- `DIVERGENT_AT_MEASUREMENT` — removed from the PR #10 query hunt
- `OPEN_FINDINGS` narratives
- references to uncommitted dossiers
- long fixture README migration histories

Convert accepted behavior into concise contract documentation and focused regression tests.

### Likely permanent

- `local_stack --ts-gateway`
- core parity matrix
- closed waiver list
- real save and binary fixtures
- golden corpus recorder, while supervisor/sidecar ports still need it

The corpus recorder's monkeypatching of `v2_control.secrets` and `v2_control.time` remains explicit debt. Injectable entropy and clock dependencies would be safer.

## Per-PR recommendation

| PR | Recommendation |
|---|---|
| #7 wire | Changes requested: consolidate technology schemas and decide strict-versus-tolerant client semantics. |
| #8 telemetry/corpus | Changes requested: fix false dropped-event reporting and remove or hide the deferred `live` command. |
| #9 gateway | Not ready as a replacement: portable Linux locking, derivation timeout, integer-preserving disk JSON, lifecycle ordering, and transport failure handling need resolution. |
| #10 parity rig | **Follow-up complete:** Linux/Darwin gates enabled, cross-game raw-path parity restored, and historical hunt scaffolding trimmed without dropping unique seeds or waiver self-invalidation. |

## Original proposed fix order

Approval was subsequently given for the scoped PR #10 follow-up. Items 1 and 6, plus the PR #10 hunt cleanup, are reflected in the status update above. The remaining order is retained as historical planning context.

1. Portable Linux/Darwin ready-file locking and Linux CI. **Implemented.**
2. Correct listener/readiness acquisition and release order.
3. Mandatory clean-checkout derivation parity fixtures.
4. Remove or make configurable the derivation timeout.
5. Integer-preserving parsing at every Python-authored JSON boundary.
6. Resolve raw-path/dot-segment routing behavior. **Implemented.**
7. Type derivation transport failures.
8. Fix telemetry export-versus-drop reporting.
9. Resolve remaining medium parity differences.
10. Consolidate schemas, large files, hunt tests, fixtures, and migration prose.

## Verification

### Original review baseline

On an archive of the then-committed stack tip, with workspace dependencies linked read-only:

- typecheck: passed
- wire tests: **1230 passed**
- telemetry tests: **79 passed**
- harness tests: **2135 passed, 5 skipped**
- lint: zero errors; 406 harness warnings

### PR #10 follow-up on macOS

- `bun run typecheck`: passed for wire, telemetry, and harness.
- `bun run lint`: passed with **0 errors** (415 existing-style warnings).
- `ARENA_REQUIRE_PARITY=1 bun test arena/harness/test/parity/hunt-query-fuzz.test.ts`: **265 passed, 0 failed**.
- `ARENA_REQUIRE_PARITY=1 bun test arena/harness/test/parity arena/harness/test/gateway/smoke-live.test.ts`: **1307 passed, 0 failed**.
- `ARENA_REQUIRE_PARITY=1 bun test arena/harness/test/gateway/server.test.ts arena/harness/test/gateway/ready-file.test.ts`: **91 passed, 0 failed**.
- `python3 -B -W error::ResourceWarning -m unittest agent_eval.tests.test_local_stack -v`: **19 passed**.
- `git diff --check`: passed.

Linux is enabled by the same platform gates and native ready-lock selection, but this follow-up verification was executed on macOS; Linux CI/runtime execution remains the cross-platform confirmation.
