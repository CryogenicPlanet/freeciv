# Arena archive

This directory keeps the legacy Python implementation available while its
behavior is ported to the active TypeScript packages in the parent directory.
It is not the destination for new features.

- `agent_eval/` — Python supervisor, replay gateway, migration oracles, and tests
- `play/` — standalone legacy Python player workspace and protocol documents
- `pyproject.toml`, `uv.lock`, `.python-version` — the archived Python toolchain

Root `just` recipes set `PYTHONPATH=arena/archive` automatically. For direct
commands from the repository root, use the same import root, for example:

```sh
PYTHONPATH=arena/archive python3 -m agent_eval --help
cd arena/archive && uv run python -m unittest discover -s agent_eval/tests -t .
```

Delete this directory only after the active Arena packages no longer use it for
runtime behavior, fixtures, differential tests, or compatibility checks.
