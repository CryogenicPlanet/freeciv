# Arena TypeScript migration — PR stack review

**Reviewed stack:** PRs [#7](https://github.com/CryogenicPlanet/freeciv/pull/7) → [#8](https://github.com/CryogenicPlanet/freeciv/pull/8) → [#9](https://github.com/CryogenicPlanet/freeciv/pull/9) → [#10](https://github.com/CryogenicPlanet/freeciv/pull/10)

**Review axes:**

1. Behavioral parity with the Python server and client.
2. Effect TypeScript style and architecture.
3. Migration scaffolding or code paths that should not remain in the final artifact.

**Scope note:** The original review covered committed PR diffs only. Uncommitted PostgreSQL work was excluded and remains outside this document's update.

## Final fix status

The full #7–#10 stack was repaired and restacked parent-first:

- **PR #7:** reduced to canonical JSON and strict, explicitly versioned gateway schemas. Speculative Agent/FNV surfaces, tolerant excess-field preservation, generated snapshots, hunts, and migration meta-tests were removed.
- **PR #8:** OTLP mirror failure is distinguished from authoritative event loss; the deferred corpus `live` command was removed.
- **PR #9:** Linux locking, listener/readiness ordering, typed startup and derivation transport failures, integer-preserving Python JSON, unbounded-by-default derivation, logical PPM scanning, raw request targets, and committed derivation fixtures are implemented.
- **PR #10:** Linux/Darwin parity gates, raw dot-segment refusal, Python non-finite JSON parity, platform-aware waivers, and historical hunt cleanup are implemented.

Verification results are recorded at the end of the document.

## Executive summary

The original release blockers and correctness findings are resolved unless a section explicitly says otherwise. The remaining medium concern is synchronous filesystem work in request handling; it is bounded and securely wrapped, but can still stall unrelated requests on a slow filesystem. The final wire package is gateway-focused and rejects unsupported versions and unknown fields.

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

### 3. TypeScript adds a fixed 120-second derivation timeout — resolved

**PR:** #9  
**Status:** Resolved

- `arena/harness/src/gateway/services/derivation.ts:384-415`
- `arena/harness/src/gateway/services/derivation.ts:610-624`
- Python reference: `agent_eval/replay_gateway.py:264-324`, `:1700-1719`

Replay, board, and event derivation subprocesses are killed after 120 seconds. Python holds the derivation mutex without a wall-clock limit.

A valid large cold derivation can therefore return TypeScript `503` while Python eventually succeeds and populates the cache.

**Implemented direction:** omitted timeout now means no wall-clock limit, matching Python; callers may opt into an explicit timeout.

### 4. Disk JSON can lose arbitrary-precision integers — resolved

**PR:** #9  
**Status:** Resolved

- `arena/harness/src/gateway/services/runs.ts:287-305`
- `arena/harness/src/gateway/services/runs.ts:384-460`
- `arena/harness/src/gateway/archive.ts:267-301`
- Python reference: `agent_eval/replay_gateway.py:573-599`, `:681-705`

Manifest, report, and victory files are read through ordinary JavaScript-number JSON decoding. A later `BigInt(value)` conversion cannot recover precision already lost by `JSON.parse`.

For example, `9007199254740993` can first become `9007199254740992`.

The stack already contains `python-json.ts` to preserve Python's `int`/`float` distinction, but it is not used at every Python-authored disk boundary.

**Implemented direction:** Python-authored disk JSON and derivation output use `parsePythonJson`, preserving integer/float distinctions and integers beyond `2^53`; boundary tests cover the behavior.

### 5. Readiness is removed before the listener closes — resolved

**PR:** #9  
**Status:** Resolved

- `arena/harness/src/gateway/server.ts:455-483`
- Python reference: `agent_eval/replay_gateway.py:2163-2170`

TypeScript scope finalization removes the ready file before closing the HTTP listener. Python closes the server first, then removes readiness and releases its lock.

A launcher observing file removal can begin replacement while the old listener remains bound. TypeScript also suppresses owned-ready-file unlink failures, potentially reporting clean shutdown while leaving stale readiness.

**Implemented direction:** the listener has a child scope registered after ready resources, so LIFO finalization closes it before readiness removal and lock release.

### 6. A clean checkout skips real derivation parity — resolved

**PRs:** #9 and #10  
**Status:** Resolved

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

**Implemented direction:** `derivation.test.ts` builds a deterministic run around a committed save and requires replay, board, and events bridge parity in a clean checkout. `ARENA_REQUIRE_PARITY=1` prevents platform gating from silently skipping required parity.

## Additional correctness findings

### 7. Competing technology-catalogue schemas disagree on valid IDs — resolved

**PR:** #7  
**Status:** Resolved

- `arena/wire/src/gateway/replay.ts:128-157`
- `arena/wire/src/gateway/manifest.ts:687-715`
- Python reference: `agent_eval/supervisor.py:424-434`

The original stack exported two schemas for the same catalogue with different ID bounds. One `TechnologyCatalog` schema now owns the embedded and on-disk shape, with technology IDs bounded to `0..511`.

### 8. OTLP failure is falsely reported as a dropped event — resolved

**PR:** #8  
**Status:** Resolved

- `arena/telemetry/src/observability.ts:125-137`
- `arena/telemetry/src/middleware.ts:122-152`

The authoritative NDJSON line is written before OTLP mirroring. If mirroring fails, `record()` fails and the middleware logs:

```text
@arena/telemetry dropped a wide event
```

The event was not dropped; it was persisted locally and failed only to mirror.

**Implemented direction:** persistence and mirroring have distinct outcomes; OTLP failure no longer claims that the authoritative corpus event was dropped.

### 9. Synchronous request-path filesystem work can stall the gateway

**PR:** #9  
**Severity:** Medium

- `arena/harness/src/gateway/services/runs.ts:255-305`, `:384-407`, `:742-828`
- `arena/harness/src/gateway/http/routes/archive.ts:169-193`
- Python reference: `agent_eval/replay_gateway.py:1280-1299`

The TypeScript request path performs synchronous filesystem calls on Bun's event loop. Python uses `ThreadingHTTPServer` and serializes only derivations.

A slow disk or archive read can stall unrelated health and proxy requests.

**Required direction:** use asynchronous scoped filesystem effects or isolate blocking calls on an appropriate blocking executor.

### 10. Derivation transport failures are silently degraded — resolved

**PR:** #9  
**Status:** Resolved

- `arena/harness/src/gateway/services/derivation.ts:477-492`
- `arena/harness/src/gateway/services/derivation.ts:541-559`
- Python bridge: `agent_eval/replay_derive_cli.py:97-107`, `:168-172`

Current behavior includes:

- swallowing stdin write/end errors through `Effect.ignore`
- converting reader rejection into EOF
- converting `child.exited` rejection into exit code `-1`

A failed stdin write can leave the child deriving with incomplete input or waiting until timeout.

**Implemented direction:** stdin, reader, and child-exit failures map to typed `DerivationUnavailable`; stdin and process cleanup are scoped.

### 11. Startup ordering and bind-error reporting differ from Python — resolved

**PR:** #9  
**Status:** Resolved

- `arena/harness/src/gateway/server.ts:421-475`
- `arena/harness/src/gateway/main.ts:315-324`
- Python reference: `agent_eval/replay_gateway.py:2092-2094`, `:2205-2209`

TypeScript binds before creating `cache_root`; Python creates the directory first. Bind failures can also become defects rendered with `Cause.pretty` instead of Python's uniform `error: …`, exit 2 contract.

**Implemented direction:** cache creation precedes binding and bind failures flow through the typed startup error/reporting contract.

### 12. Upstream JSON rejects Python-accepted non-finite constants — resolved

**PR:** #9  
**Status:** Resolved

- `arena/harness/src/gateway/python-json.ts:59-65`, `:225-235`
- `arena/harness/src/gateway/http/routes/games.ts:129-145`
- Python reference: `agent_eval/replay_gateway.py:123-126`, `:1613-1639`

`parsePythonJson` deliberately rejects `NaN` and infinities. Python's default `json.loads` and `json.dumps` accept and re-emit them.

An upstream games response containing `NaN` can therefore be Python `200` but TypeScript `502`.

**Implemented direction:** the Python-compatible parser accepts `NaN` and infinities, and gateway rendering emits CPython-compatible spellings.

### 13. PPM metadata parsing adds a 512 KiB cutoff — resolved

**PR:** #9  
**Status:** Resolved

- `arena/harness/src/gateway/http/routes/archive.ts:145-193`, `:234-243`
- Python reference: `agent_eval/replay_gateway.py:906-925`

TypeScript reads a 512 KiB prefix. Python reads at most 513 complete logical lines. A very long early line can hide valid metadata from TypeScript.

**Implemented direction:** the reader streams at most 513 complete logical lines, including coverage for a metadata row after a line larger than 512 KiB.

### 14. The ready-lock descriptor leaks if `chmod` fails — resolved

**PR:** #9  
**Status:** Resolved

- `arena/harness/src/gateway/services/ready-file.ts:383-399`
- `arena/harness/src/gateway/services/ready-file.ts:449-455`
- Python reference: `agent_eval/replay_gateway.py:243-253`

`openLockFd` opens the descriptor and then performs `chmod` through `Effect.tap`. If `chmod` fails, the descriptor has not yet reached `acquireRelease` and is not closed.

**Implemented direction:** a failed post-open `chmod` closes the descriptor before propagating the typed I/O error.

### 15. Wire decoding is intentionally less strict than the Python client — resolved

**PR:** #7  
**Status:** Resolved

The speculative Agent protocol package and tolerant preservation layer were removed from this gateway stack. Current gateway decoders use `onExcessProperty: 'error'`, require their supported schema version, reject future versions, and encode only the current shape.

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
| `arena/wire/src/gateway/games.ts` | 995 |
| `arena/harness/src/gateway/config.ts` | 993 |
| `arena/harness/src/gateway/services/upstream.ts` | 893 |
| `arena/harness/src/gateway/services/runs.ts` | 851 |
| `arena/harness/src/gateway/services/ready-file.ts` | 742 |

Suggested splits:

- `config.ts`: Python numeric parsing, URL parsing, path resolution, config assembly
- `archive.ts`: status/result/watch/frame/index projections
- `runs.ts`: secure filesystem primitives versus repository API

Naming is otherwise generally strong. The competing catalog types were consolidated into `TechnologyCatalog`.

### Lint signal

The final validation reports zero lint errors. Warnings remain, concentrated in parity/spike tests using Promise/async and Node APIs; “zero lint errors” does not mean warning-clean Effect code.

## Test assessment

The wire cleanup deleted 21,714 lines from its earlier implementation and left a focused 216-test package. The full stack retains the high-value parity and lifecycle suites below.

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

#### Duplicate native-schema fixture — resolved

The wire copy was removed; only the harness spike fixture remains.

#### Redundant telemetry exit matrices

`middleware.test.ts` and `result-preserved.test.ts` both exercise success, failure, defect, and interruption across multiple backends. Keep the stronger result-preservation matrix and reduce the middleware suite to emission/outcome behavior.

#### Migration-time meta-tests — wire cleanup complete

Wire citation, schema-shape, parity-constant, barrel, snapshot, hunt, and fixture-coverage meta-suites were removed. The gateway invariant test remains because it provides lasting architectural value.

## Migration stragglers

### Definitely temporary

#### Python derivation bridge

- `agent_eval/replay_derive_cli.py`
- `ReplayDerivationPython` in `arena/harness/src/gateway/services/derivation.ts`
- bridge-specific tests and comments

These explicitly describe themselves as interim and scheduled for deletion when native derivation lands.

#### Deferred corpus command — resolved

The non-functional `live` subcommand and its parser registration were removed.

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
| #7 wire | **Ready:** gateway-focused, strict/versioned, consolidated technology schema, speculative protocol machinery removed. |
| #8 telemetry/corpus | **Ready:** persistence/export semantics corrected and deferred CLI scaffolding removed. |
| #9 gateway | **Ready with noted performance caveat:** parity/lifecycle/transport/Linux blockers resolved; synchronous request-path filesystem work remains a follow-up. |
| #10 parity rig | **Ready:** Linux/Darwin gates enabled, raw-path parity restored, and historical hunt scaffolding trimmed without dropping unique seeds or waiver self-invalidation. |

## Original proposed fix order

Approval was subsequently given for the scoped PR #10 follow-up. Items 1 and 6, plus the PR #10 hunt cleanup, are reflected in the status update above. The remaining order is retained as historical planning context.

1. Portable Linux/Darwin ready-file locking and Linux CI. **Implemented.**
2. Correct listener/readiness acquisition and release order. **Implemented.**
3. Mandatory clean-checkout derivation parity fixtures. **Implemented.**
4. Remove or make configurable the derivation timeout. **Implemented.**
5. Integer-preserving parsing at every Python-authored JSON boundary. **Implemented.**
6. Resolve raw-path/dot-segment routing behavior. **Implemented.**
7. Type derivation transport failures. **Implemented.**
8. Fix telemetry export-versus-drop reporting. **Implemented.**
9. Resolve remaining medium parity differences. **Implemented except the request-path synchronous-I/O performance follow-up.**
10. Consolidate schemas, large files, hunt tests, fixtures, and migration prose. **Implemented for the reviewed stack scope.**

## Verification

### Original review baseline

On an archive of the then-committed stack tip, with workspace dependencies linked read-only:

- typecheck: passed
- wire tests: **1230 passed**
- telemetry tests: **79 passed**
- harness tests: **2135 passed, 5 skipped**
- lint: zero errors; 406 harness warnings

### Final slim-stack verification

macOS:

- `bun run typecheck`: passed for wire, telemetry, and harness.
- `bun test arena/wire`: **243 passed, 0 failed** after the final `/result`, encode, barrel, version, state, and timing-contract coverage.
- PR #9 harness suite: **931 passed, 0 failed**.
- `ARENA_REQUIRE_PARITY=1 bun test` at PR #10: **2082 passed, 0 failed**.
- Python local-stack tests: **19 passed**.
- lint: **0 errors**; `git diff --check`: passed.

Linux (`freeciv-port.exe.xyz`, Python 3.14):

- workspace typecheck: passed.
- wire suite before the final test-only matrix: **205 passed, 0 failed**.
- `ARENA_REQUIRE_PARITY=1 bun test`: **2081 passed, 0 failed**.
- Python local-stack tests: **19 passed**.

The one-test platform count difference is expected from a Darwin-only non-parity spike; required parity itself runs on both supported platforms.
