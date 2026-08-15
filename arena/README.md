# Arena

This directory contains the Freeciv Agent Arena code and its supporting material.
The Freeciv engine remains at the repository root so this fork can continue to
track upstream cleanly.

## Active TypeScript packages

- `play/` — Effect/Bun player CLI
- `harness/` — arena harness and replay gateway
- `db/` — Postgres persistence and ingestion
- `wire/` — shared protocol schemas and canonical JSON
- `telemetry/` — observability and wide-event support
- `viewer/` — browser replay viewer
- `video/` — offline replay renderer

## Supporting directories

- `docs/` — Arena architecture, protocol, gameplay, and operator documentation
- `tools/` — workspace-only development tooling
- `reference/` — read-only vendored source used for implementation reference
- `archive/` — the legacy Python harness and player client retained while the
  TypeScript migration is completed; new implementation work should not land
  there

The Bun workspace is intentionally rooted at the repository top level. Run
`bun install`, `bun run typecheck`, `bun run lint`, and `bun test` from there.
The root `bunfig.toml` enables Bun's isolated linker and global virtual store.
Root `just` recipes continue to orchestrate the Arena and the Freeciv engine.
