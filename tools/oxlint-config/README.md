# `@freeciv/oxlint-config`

Shared Oxlint configuration for the repository's Effect and React TypeScript projects.

The package owns:

- the common correctness and suspicious categories;
- unused-disable and consistent-type-import checks;
- Effect-specific and React-specific rule presets;
- the single vendored copy of the anti-slop rules.

Consumers keep only a thin `oxlint.config.ts` that calls either
`createEffectConfig` or `createReactConfig`. Oxlint loads the TypeScript source
directly through the `@freeciv/oxlint-config/anti-slop` export.

## Vendored anti-slop rules

The source under `src/anti-slop/` is vendored from
[`dmmulroy/anti-slop`](https://github.com/dmmulroy/anti-slop) at commit
`9b80d9a5c317d3af94d88a577bdbde4d9a45f7be` and may be adapted locally.

MIT License

Copyright (c) 2026 Dillon Mulroy

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
