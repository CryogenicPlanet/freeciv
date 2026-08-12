# Parity fixtures

A committed `runs_root` for the two-gateway parity rig: eight run directories,
one per scenario class, plus the bad/boundary request table both gateways are
replayed against.

Point either gateway at `./runs` and it must answer identically — same status,
same bytes, same rows in the same order. `runs/` is 324 KB across 27 files and
one symlink; nothing here is generated at test time, so a divergence is always
a code change and never a fixture race.

```
runs/
  game_parity_terminal_valid_01/   terminal, valid, a winner  (+ the only real autosaves)
  game_parity_terminal_nowin_02/   terminal, no valid winner
  game_parity_interrupted_03/      non-terminal manifest, orphaned
  game_parity_lobby_husk_04/       non-terminal, empty replay.jsonl   -> must VANISH
  game_parity_malformed_05/        manifest.json truncated mid-object -> absent
  game_parity_wrong_id_06/         dir name != manifest game_id       -> absent
  game_parity_symlink_07 ->        symlink into ...terminal_valid_01  -> absent
  game_parity_torn_tail_08/        replay.jsonl ends mid-line
request-cases.ts                   bad/boundary GameId + frame-index request table
scenarios.ts                       the same eight, as data, with their index expectations
```

`runs_root` is `expanduser().resolve()`'d by the gateway
(`replay_gateway.py:203`) and `_read_manifest` compares `run_root.parent` to it
by identity — so a rig that hands over an unresolved path (a `mkdtemp` under
`/tmp`, which is a symlink to `/private/tmp` on macOS) gets an empty index and
no error. Resolve before you pass it, or take `PARITY_RUNS_ROOT` from
`scenarios.ts`, which is already absolute and resolved.

## What each scenario proves, and the index row it must produce

The measured index — `_disk_games_index` plus the `_as_interrupted` reshape the
`/v1/games` handler applies to orphaned rows — is **4817 bytes**, sha256
`3e6d74056a340b2e985a32a1f427244f23444a3a1cef14691b54fd63c12ef6df`, and
contains four rows in this order (`(created_at, game_id)` descending):

| # | `game_id` | `state` | `benchmark_valid` | `current_turn` | `outcome.status` | `outcome.margin` |
|---|---|---|---|---|---|---|
| 1 | `game_parity_interrupted_03` | `interrupted` | `null` | 4 | `interrupted` | `null` |
| 2 | `game_parity_terminal_nowin_02` | `cancelled` | `null` | 596 | `invalid` | 2157 |
| 3 | `game_parity_torn_tail_08` | `interrupted` | `null` | 3 | `interrupted` | `null` |
| 4 | `game_parity_terminal_valid_01` | `completed` | `true` | 752 | `won` | 1096 |

**Four of the eight are absent, for four different reasons.** That asymmetry is
the point of the tree: "absent" is easy to get right by accident, and a port
that reaches it down the wrong path passes an index comparison while answering
the direct route wrong.

### 01 · terminal-valid — the only fixture with real autosaves

The one archive that fully answers. `state: completed`, `benchmark_valid:
true`, a two-row leaderboard (`place-1` 1856, `place-2` 760, dense ranks 1 and
2), so `outcome.status` is `won` with `margin: 1096`.

- **Frames pair, and then stop pairing.** `watch_frames/` holds `000000.png`
  and `000752.png`; `saves/` holds two `.map.ppm` files. `_archive_frames`
  indexes PPMs *positionally* against the sorted PNG indices, so frame 0 pairs
  with `turn-0001-...map.ppm` (`turn: 1`, two `map_players` parsed out of the
  PPM comment header, colours `#0067A5` / `#F38400`) and frame 752 pairs with
  nothing (`turn: null`, `map_players: []`, `source_name` falls back to the PNG
  name). One fixture, both branches.
- **`victory.json` is synthetic** — see "Departures from the source runs".
  It makes `outcome.summary` read `pi-gpt-5.6-sol won by 1096 (score victory)`,
  which is the only place the `VICTORY_LABELS` join is exercised at all.
- **This is the `ReplayDerivationPython` fixture.** Two real, unmodified
  autosaves:

  | file | bytes | sha256 |
  |---|---|---|
  | `saves/turn-0001-auto.sav.gz` | 28552 | `d23c9c73dd7a04e6a6eb8f2f6bfdbaf3fa57eb0bdfe53a6776b463d683cd875c` |
  | `saves/turn-0002-auto.sav.gz` | 28675 | `2872d4e24e59d3ff5a1929bf7377324f0a370d9d94a50852685c6ae60957cca1` |

  57 KB buys all three derivations for both gateways. Measured:
  `--op replay --after-turn 0 --limit 50 --complete` → 11088 bytes,
  `available: true`, `complete: true`, `has_more: false`, `next_after_turn: 2`,
  snapshots for turns 1 and 2, no `replay_warnings`; `--op board --turn 2` →
  59484 bytes; `--op events --complete` → 238 bytes. Byte-identical across a
  cold cache, a second cold cache and a warm one, which is what makes it usable
  as a two-gateway oracle. The cache lands where Python caches it —
  `<cache_root>/game_parity_terminal_valid_01/turn-0000000001-b2eb2020b2b2.json`,
  `turn-0000000002-2c30a7a00247.json`, `events.json` — so
  `derivationTurnCacheName` can be asserted against real names.

### 02 · terminal-no-winner

`state: cancelled`, and `benchmark_valid` is **`null`, not `false`** — the
manifest carries `null` and `_disk_game_row` only coerces a `bool`. The
leaderboard is real and inverted: the native Freeciv AI (`place-2`, 2276) beat
the agent (`place-1`, 119), so `outcome.status` is `invalid` with the summary
`No valid winner; Freeciv Classic AI led by 2157 at the last complete score`.

Its three frames (`000591.png`, `000592.png`, `000593.png`) are **all
unpaired** — one PPM ships, and index 591 is far past it. Complementary to 01:
here `turn` is `null` on every frame and `frames/0.png` is a
`map frame does not exist`, because indices are file names and not positions.

No `.sav.gz`, so all three derivations answer `available: false` with no
snapshots and **exit 0** — the "no artifacts" path is not an error here.

### 03 · interrupted

A non-terminal manifest with no live supervisor. `_as_interrupted` relabels the
row `interrupted` rather than dropping it, and sets
`current_turn = max(manifest 2, replay tail 4) = 4`. The manifest and the
replay deliberately disagree so a port that reads only one of them fails.

Being in the index is not a promise of an archive: `/status` on this id is a
404 `terminal archive not found`.

### 04 · lobby-husk — must vanish

Same shape as 03 with a **zero-byte** `replay.jsonl`. `_last_replay_turn`
returns `None`, `_as_interrupted` returns `None`, and the row disappears — the
only scenario where a perfectly readable manifest yields no index row. Its
`/status` is still `terminal archive not found` (404), not `game not found`:
the run exists, it just has nothing to watch.

### 05 · malformed-manifest

`manifest.json` is the first 200 bytes of a valid document, cut mid-object.
The index scan swallows the `GatewayProblem` and skips the directory; the
direct route does not — `/status` is **503 `game manifest is unavailable`**.
Status *and* message both matter: 404 would claim the run does not exist.

### 06 · wrong-game-id

The directory is `game_parity_wrong_id_06`; the manifest (and the report's
embedded manifest) claim `game_parity_wrong_id_06_other`. `_read_manifest`
rejects the mismatch, so the run is unreachable from **both** names — 404
`game not found` either way — and never reaches the row builder.

### 07 · symlinked-run

A symlink to `game_parity_terminal_valid_01`. `_disk_games_index` checks
`candidate.is_symlink()` *before* `is_dir()`, and `_read_manifest` re-checks
before `resolve()`, so a link to a perfectly good run is still refused: 404
`game not found`. The run it points at appears in the index exactly once.

Git stores this as a symlink; if a checkout materializes it as a regular file
containing the target name, the fixture is broken, not the gateway.

### 08 · torn-tail

Four replay lines, the fourth truncated to 120 bytes with no trailing newline —
what a supervisor killed mid-write leaves behind. The tail scan reads the last
64 KiB, splits, walks **backwards**, drops the line that will not parse, and
takes turn 3 from the line before it. `current_turn` is `max(1, 3) = 3`.

## Provenance

Every fixture is cut down from a real run in this checkout's `.agent-eval/runs`
(the wire corpus at `arena/wire/test/fixtures/runs` holds bare manifests and
reports, not run directories, so it cannot serve as a `runs_root`).

| fixture | source run | kept |
|---|---|---|
| `..._terminal_valid_01` | `game_ieTomdES08hpUmFRFzCOAVMo` | manifest, report, 3 replay lines, 2 PNGs, 2 autosaves, 2 PPM headers |
| `..._terminal_nowin_02` | `game_a8_dSs1WtX5NoDPHACckOKc4` | manifest, report, 3 replay lines, 3 PNGs, 1 PPM header |
| `..._interrupted_03` | `game_Hsit9YEuBjKdJPPouFoGVYlk` | manifest, 4 replay lines |
| `..._lobby_husk_04` | `game_uSO-nwaR-nO8QXbx45dwTyKd` | manifest, empty replay |
| `..._malformed_05` | `game_xMCbmQ67I89z0UjFTM8zyO9H` | manifest, truncated to 200 bytes |
| `..._wrong_id_06` | `game_JCF3XsC4LhXSQQly_xLNjYj5` | manifest, report |
| `..._symlink_07` | — | a symlink |
| `..._torn_tail_08` | `game_mEUltpqtzauPGfjI9IlhWJ5x` | manifest, 3 lines + a torn 4th |

### Departures from the source runs

Recorded here because a fixture that hides its edits is worse than no fixture.

1. **Ids were rewritten.** Every occurrence of the source `game_id` became the
   fixture id, in the manifest, in the report's embedded manifest and in the
   replay rows. `config.name` was regenerated by the supervisor's own rule
   (`"session-" + game_id[:13]`) so it does not point at a run that is not
   here. `game_parity_wrong_id_06` is the deliberate exception: its documents
   were rewritten to the *claimed* id, which is what makes it the fixture.
2. **`report.episode` was replaced** with `.agent-eval/runs/<fixture id>`. The
   source run recorded an absolute path containing a home directory. The
   gateway never reads this field.
3. **`.map.ppm` files were truncated to their first 8 lines.** Both readers
   (`_archive_ppm_players` and `save_replay._ppm_players`) stop at the first
   non-`#` line after the magic, so the header is the whole input and the parse
   is bit-identical — but a full PPM is 2.4 MB and the header is ~200 bytes.
   These files are never served; they exist to be paired with and parsed.
4. **States were forced non-terminal** on 03, 04 and 08 (`running`, `lobby`,
   `running`), with `finished_at`, `returncode`, `error` and `benchmark_valid`
   cleared to match. Every run archived in this checkout is terminal, so the
   orphan path had no natural source. `current_turn` was set to 2 (03), `null`
   (04) and 1 (08) to make the `max(manifest, replay)` reshape observable.
5. **`victory.json` in fixture 01 is synthetic**
   (`{"victory": "turn_limit", "winners": ["AgentPlace1"], "turn": 753,
   "year": 1995}`). No run in this checkout ever emitted one, and without it
   `_archive_victory` and the `VICTORY_LABELS` join are dead code in both
   gateways.
6. **`replay.jsonl` was truncated** to 3–4 lines everywhere. Nothing reads more
   than the tail.

### Secrets

`auth.json` — the only file in a run directory that holds credential material,
and it holds `*_token_sha256` digests rather than tokens — is **not copied**,
and neither are `sidecars/`, `v2-receipts/`, `v2-ambiguity-trace/`,
`decisions.jsonl`, `phase-events.jsonl`, `bridge-status.jsonl`, `server.log`,
`server.stdout.log`, `server.commands`, `score.log` or `game.mp4`.

Scanned before commit, across every committed file:

- no file named `auth.json`, `*token*`, `*secret*`, `*credential*`, `*.env`,
  `*.key`, `*.pem`;
- `token|secret|password|credential|api[-_]?key|authorization|bearer|sk-…|ghp_|xoxb|/Users/|/home/|@gmail`
  over all JSON, JSONL and PPM content — the only hits are the metric names
  `input_tokens` and `output_tokens` in `report.json`;
- the same sweep over both **decompressed** savegames — clean, and their only
  identifying content is the ruleset, the engine version and the two player
  names `AgentPlace1` / `AgentPlace2`;
- `strings` over all five PNGs — pure IDAT, no `tEXt` chunks, no paths.

`controller_fingerprint` values are kept deliberately. They are
`sha256(json({id, type, model, instructions, base_url, options}))`
(`agent_eval/config.py:271`) — no token is an input — and they are load-bearing
for `seat_stats` matching.

## Re-measuring

Everything above was measured with CPython 3.14.6 as the oracle, not derived
from reading `replay_gateway.py`. To reproduce the index:

```python
from pathlib import Path
from agent_eval import replay_gateway as rg
root = Path("arena/harness/test/parity/fixtures/runs").resolve()   # resolve()!
rows = []
for row in rg._disk_games_index(root)["games"]:
    if row["state"] not in rg.TERMINAL_STATES:
        row = rg._as_interrupted(root, row)
        if row is None:
            continue
    rows.append(row)
print(len(rg._canonical({"schema_version": 1, "games": rows})))
```

To reproduce the request table, start a **private** gateway — the developer's
own stack must never be touched, so bind port 0, keep the ready file out of
`.agent-eval/`, and point upstream at an unroutable RFC 5737 address so every
route falls through to the disk archive:

```
python3 -m agent_eval.replay_gateway \
  --host 127.0.0.1 --port 0 \
  --service-url http://192.0.2.1:1 --upstream-timeout-s 1 \
  --runs-root arena/harness/test/parity/fixtures/runs \
  --cache-root "$SCRATCH/cache" --ready-file "$SCRATCH/parity.ready.json"
```

It prints its `{host, port}` identity on stdout. Do **not** use a
just-released ephemeral port as the "upstream is down" fixture: the kernel
reuses it and the gateway ends up proxying to itself.

Send the targets from `request-cases.ts` on a raw socket, and **complete each
read on `Content-Length`** — waiting for EOF against a keep-alive server costs
a phantom 12 s per request. Rows flagged `rawSocketOnly` cannot go through
`fetch`, which rewrites them before they reach the wire.

### The trap that table exists for

`//v1/games` returns the games index, byte-for-byte identical to `/v1/games`
(4817 bytes, same sha256). Not because of anything in `replay_gateway.py`:
CPython's `BaseHTTPRequestHandler.parse_request` collapses a *leading* run of
slashes to a single `/` before `do_GET` ever sees `self.path` — the `gh-87389`
open-redirect fix. `///v1/games` collapses too. Interior slashes do not:
`/v1/games/<id>//frames/0.png` is a 404.

A port that routes on `new URL(request.url).pathname` keeps both slashes and
404s. That is a divergence on a path a browser reaches by resolving a
protocol-relative link, it is not on the accepted-waiver list, and it is a bug
in the port rather than a fixture to retire.

The second trap is quieter: the two 404 messages are different.
`not found` means the router refused the id (19 chars, 81 chars, a `.`, a
`%2F`); `game not found` means the router accepted it and the disk lookup
missed (20 chars, 80 chars, a symlink, an id mismatch). A comparison that only
checks status passes while the viewer — which renders `payload.error`
verbatim — is told the wrong story.

## Relationship to `test/gateway/runs.test.ts`

That suite builds a *temporary* tree at run time from the wire corpus and
covers overlapping ground (a symlinked run, a torn tail, a lobby husk) against
`RunsRepository` in-process. This directory is the committed, HTTP-level
counterpart: two live gateways, real savegames, real PNGs, one `runs_root`
neither of them may mutate. Keep both — a divergence that only appears over a
socket does not show up in the first, and vice versa.
