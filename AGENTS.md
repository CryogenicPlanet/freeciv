# Agent notes

## Vendored reference source (`arena/reference/`)

`arena/reference/effect/` contains the full source of the Effect monorepo, pinned to the
version installed in `arena/play/` (`effect@3.22.1`). It is **read-only reference
material**: when working with Effect APIs in `arena/play/`, inspect
`arena/reference/effect/packages/effect/src/` for the real implementation and idiomatic
usage instead of guessing from documentation. Never edit or import from
`arena/reference/` — it is not part of any build.

To update it, delete the directory and re-clone the tag matching the installed
version:

```sh
git clone --depth 1 --branch "effect@<version>" https://github.com/Effect-TS/effect.git arena/reference/effect
rm -rf arena/reference/effect/.git
```

## Arena TypeScript toolchain

- Runtime/tests: `bun` (`bun test`), Effect for all app code.
- Typecheck: `bun run typecheck` (TypeScript 7, native).
- Lint: `bun run lint` — oxlint in type-aware mode with the Effect rules from
  `@effect/tsgo`. After any dependency install, the `prepare` script must
  re-apply `effect-tsgo patch --oxlint`; `oxlint` versions must stay on one the
  installed `@effect/tsgo` ships artifacts for.

New implementation work belongs under `arena/`. Treat `arena/archive/` as
migration-only legacy code; change it only to preserve compatibility or parity
while active behavior moves to TypeScript.
