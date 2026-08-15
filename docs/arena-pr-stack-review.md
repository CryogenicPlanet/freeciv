# Arena TypeScript migration — PR stack review

**Reviewed stack:** PRs [#7](https://github.com/CryogenicPlanet/freeciv/pull/7) → [#8](https://github.com/CryogenicPlanet/freeciv/pull/8) → [#9](https://github.com/CryogenicPlanet/freeciv/pull/9) → [#10](https://github.com/CryogenicPlanet/freeciv/pull/10)

## Current status

The release blockers found during review are resolved. The stack now provides:

- strict, explicitly versioned gateway schemas and canonical JSON;
- ordered NDJSON telemetry writes with explicit live, test, and no-op layers;
- Linux and Darwin ready-file locking compatible with Python `fcntl.flock`;
- listener-before-readiness teardown ordering;
- typed startup and derivation transport failures;
- integer-preserving Python JSON, including non-finite constants where CPython accepts them;
- logical PPM metadata scanning without a byte-prefix cutoff;
- raw request-target handling, including refusal of cross-game dot segments;
- unbounded-by-default derivation, matching Python;
- committed real-save derivation fixtures and required parity gates;
- an opt-in TypeScript gateway in `local_stack`, with Python remaining the default.

The remaining medium concern is synchronous filesystem work in the TypeScript request path. It is bounded and securely wrapped, but a slow filesystem can stall unrelated requests. Moving those reads to asynchronous scoped filesystem effects remains follow-up work.

## Resolved correctness findings

| Finding | Resolution |
|---|---|
| Darwin-only ready-file implementation | Selects Linux/Darwin libc, errno access, `EWOULDBLOCK`, and `O_CLOEXEC`; parity runs on both platforms. |
| Cross-game `A/../B` normalization | Dispatch uses the raw incoming target and both gateways refuse the request. |
| Fixed 120-second derivation timeout | An omitted timeout means no wall-clock limit; callers may opt in. |
| Precision loss in Python-authored JSON | Disk and derivation boundaries use the Python-compatible parser. |
| Ready file removed before listener close | Scoped LIFO teardown closes the listener first. |
| Clean checkout skipped real derivation parity | Committed autosaves require replay, board, and events bridge parity. |
| Competing technology catalogue schemas | One bounded `TechnologyCatalog` schema owns both shapes. |
| Speculative OTLP mirror without a consumer | Removed; the retained backend records ordered NDJSON or captures events in memory for tests. |
| Derivation transport failures swallowed | Stdin, reader, and child-exit failures map to typed failures with scoped cleanup. |
| Startup order and bind reporting drift | Cache creation precedes binding and startup errors use the typed exit-2 contract. |
| Python non-finite JSON rejected | Compatible parsing and rendering preserve CPython spellings. |
| PPM metadata hidden beyond 512 KiB | The reader scans at most 513 complete logical lines. |
| Ready-lock descriptor leak on failed `chmod` | The descriptor closes before the typed I/O error propagates. |
| Core parity optional/Darwin-only | Linux and Darwin run it; `ARENA_REQUIRE_PARITY=1` forbids unsupported-platform skips. |

## Architecture assessment

Production TypeScript keeps Effect boundaries explicit: typed `Data.TaggedError` failures, `Context.Tag`/`Layer` dependencies, and scoped listeners, locks, streams, and subprocesses. Pure projections remain pure; imperative boundaries are wrapped in `Effect.try` or `Effect.tryPromise`.

Keep comments that explain observable Python behavior, security invariants, resource ordering, or a live waiver. Historical hunt narratives and migration dossiers are not part of the runtime contract.

## Required parity assets

The final stack retains:

- the core byte-level diff matrix;
- query/parser and state/cache/concurrency matrices with unique seeds;
- self-invalidating, measured waivers;
- `/health` normalization with dead-normalization alarms;
- the raw socket client for targets and malformed framing that `fetch` cannot express;
- Linux/Darwin platform gates;
- negative run-directory fixtures;
- real autosaves, PNG frames, PPM headers, victory data, and reports;
- symlink and no-follow security coverage;
- canonical JSON differentials and Effect result-preservation tests.

The standalone duplicate-`Content-Length` waiver oracle was removed after its waiver disappeared; the request remains a normal core-matrix leg.

## Migration boundary

`agent_eval/replay_derive_cli.py` and `ReplayDerivationPython` remain an explicit bridge until native derivation lands. The `local_stack --ts-gateway` switch is expected to remain during migration and is opt-in; the default gateway is Python.

## Recommendation

| PR | Recommendation |
|---|---|
| #7 wire | Ready: gateway-focused, strict/versioned schemas. |
| #8 telemetry/corpus | Ready: ordered NDJSON delivery, explicit no-op/test layers, and create-only corpus v3. |
| #9 gateway | Ready with the synchronous request-path filesystem caveat above. |
| #10 parity rig | Ready: mandatory Linux/Darwin parity, raw-path safety, and retained high-value matrices/fixtures. |

## Verification

- macOS harness typecheck passed;
- macOS harness lint: **0 errors**;
- macOS full harness: **2034 passed, 0 failed** before the final query-fuzz platform-gate-only change; exact-final query fuzz then passed **266 tests**;
- macOS Python gateway/local-stack suites: **59 passed**;
- Linux harness typecheck passed under CPython 3.14;
- exact-final Linux `ARENA_REQUIRE_PARITY=1 bun test arena/harness/test`: **2034 passed, 0 failed**;
- Linux Python gateway/local-stack suites: **59 passed**.

The one-test platform difference is a Darwin-only non-parity spike; required parity runs on both supported platforms.
