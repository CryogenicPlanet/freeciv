# Parity fixtures

A committed `runs_root` for the Python/TypeScript gateway parity rig. It contains eight scenario classes plus the bad/boundary request table. Tests use the tree read-only and compare status, headers, and body bytes over real sockets.

```
runs/
  game_parity_terminal_valid_01/    completed winner; real saves, PNGs and PPMs
  game_parity_terminal_nowin_02/    completed without a valid winner
  game_parity_interrupted_03/       orphaned non-terminal run
  game_parity_lobby_husk_04/        empty replay; absent from the index
  game_parity_malformed_05/         truncated manifest; absent
  game_parity_wrong_id_06/          directory/manifest id mismatch; absent
  game_parity_symlink_07 ->         symlinked run; absent
  game_parity_torn_tail_08/         replay ending in a partial line
request-cases.ts                    raw and boundary request matrix
scenarios.ts                        index expectations for all eight classes
```

Pass an absolute, resolved root. The Python gateway resolves `runs_root` and checks run ancestry by identity; an unresolved macOS `/tmp` path can otherwise produce an empty index. `PARITY_RUNS_ROOT` in `scenarios.ts` is ready to use.

## Index contract

The four visible rows are ordered by `(created_at, game_id)` descending:

| # | `game_id` | state | `benchmark_valid` | turn | outcome |
|---|---|---|---:|---:|---|
| 1 | `game_parity_interrupted_03` | interrupted | `null` | 4 | interrupted |
| 2 | `game_parity_terminal_nowin_02` | cancelled | `null` | 596 | invalid, margin 2157 |
| 3 | `game_parity_torn_tail_08` | interrupted | `null` | 3 | interrupted |
| 4 | `game_parity_terminal_valid_01` | completed | `true` | 752 | won, margin 1096 |

The other four disappear for distinct reasons, so direct-route legs also pin their behavior rather than relying only on index absence:

- lobby husk: readable manifest but no replay turn;
- malformed manifest: direct status is `503 game manifest is unavailable`;
- wrong id: neither directory nor claimed id is reachable;
- symlink: rejected before resolution.

## Load-bearing real artifacts

`game_parity_terminal_valid_01` keeps two unmodified Freeciv autosaves. They exercise replay, board, and event derivation through the real Python loaders on cold, re-cold, and warm caches.

| file | bytes | sha256 |
|---|---:|---|
| `saves/turn-0001-auto.sav.gz` | 28552 | `d23c9c73dd7a04e6a6eb8f2f6bfdbaf3fa57eb0bdfe53a6776b463d683cd875c` |
| `saves/turn-0002-auto.sav.gz` | 28675 | `2872d4e24e59d3ff5a1929bf7377324f0a370d9d94a50852685c6ae60957cca1` |

The same run keeps two real PNG frames, two compact PPM headers, a report, and synthetic `victory.json`. Together they cover paired/unpaired frame metadata, binary responses, leaderboard/victory projection, and the victory-label join.

`game_parity_terminal_nowin_02` keeps three real PNG frames, one PPM header, and a report. Its frames are intentionally unpaired. Runs without saves exercise the successful `available: false` derivation path.

The torn replay has three complete lines followed by a truncated fourth line without a newline. The tail reader must walk backward to turn 3. The interrupted fixture intentionally disagrees between manifest turn 2 and replay turn 4, pinning the `max(manifest, replay)` reshape.

## Fixture edits and security

The source run ids were rewritten consistently; `game_parity_wrong_id_06` deliberately claims a different id. Absolute `report.episode` paths were replaced. PPMs were reduced to the comment/header lines consumed by both readers. Non-terminal states and `victory.json` were synthesized because the source checkout had no natural examples of those branches. Replay files were reduced to the lines needed by the tests.

No authentication, sidecar, receipt, decision, log, command, or video files were copied. Savegames and committed JSON/JSONL/PPM/PNG content were checked for credentials and local paths. `controller_fingerprint` values remain because they are token-free hashes used for seat-stat matching.

Git stores `game_parity_symlink_07` as a symlink. A checkout that materializes it as a regular file has broken the fixture.

## Re-measuring safely

Use a private gateway on port 0, private cache/ready paths outside `.agent-eval`, and an RFC 5737 unroutable service URL:

```sh
python3 -m agent_eval.replay_gateway \
  --host 127.0.0.1 --port 0 \
  --service-url http://192.0.2.1:1 --upstream-timeout-s 1 \
  --runs-root arena/harness/test/parity/fixtures/runs \
  --cache-root "$SCRATCH/cache" --ready-file "$SCRATCH/parity.ready.json"
```

Do not use a released ephemeral port as the down fixture: the kernel can reuse it for the gateway itself. Send `request-cases.ts` through the raw client and complete reads according to HTTP framing; `fetch` rewrites several targets and cannot send malformed framing.

`//v1/games` is intentionally in the matrix: CPython collapses a leading slash run before dispatch, so the TypeScript edge must match it. Interior doubled slashes remain distinct. The matrix also distinguishes router-level `not found` from accepted-id lookup `game not found`; both status and message are observable.

`test/gateway/runs.test.ts` remains complementary in-process repository coverage. These fixtures provide the committed HTTP-level, two-process counterpart.
