/**
 * The private-state sandbox.
 *
 * Ports `_state_relative_path` (client.py:311-341), `_open_state_directory`
 * (344-405), `_write_private_text` (408-470), `_write_private_json` (473-475),
 * `_read_private_text` (478-509), `_load_private_object` (512-520),
 * `_append_private_text` (523-566) and `_state_root` / `_state_regular_file`
 * (762-800).
 *
 * DIVERGENCE (NOTES.md §3): CPython walks the workspace with `openat` +
 * `O_NOFOLLOW` through directory file descriptors; Effect Platform does not
 * expose the `*at` family. The port refuses symlinks before every directory and
 * final-file access, creates directories with mode 0700, writes through an
 * exclusive temp file and atomic rename, and enforces mode 0600 on reads. The
 * difference is TOCTOU width, not intended reachability.
 */
import * as os from 'node:os';
import type { FileSystem } from '@effect/platform';
import { Context, Effect, Either, Layer, Schema } from 'effect';
import { type PlayerError, playerError } from 'src/errors';
import {
  JsonObjectSchema,
  type JsonObject,
  type JsonValueInput,
} from 'src/schema/primitives';
import { indentedJson } from 'src/services/json-output';
import { fileSystem, path } from 'src/services/platform';

// ---------------------------------------------------------------------------
// Workspace
// ---------------------------------------------------------------------------

export interface WorkspacePaths {
  /** The player workspace — CPython's `ROOT`, the directory `client.py` sits in. */
  readonly root: string;
  /** `PLAY_STATE_DIR`, resolved and proved to be inside {@link root}. */
  readonly stateRoot: string;
}

export class Workspace extends Context.Tag('Workspace')<Workspace, WorkspacePaths>() {}

export const expandUser = (value: string): string =>
  value === '~' || value.startsWith('~/') ? path.join(os.homedir(), value.slice(1)) : value;

/**
 * `Path.resolve()` — resolve symlinks without requiring the path to exist, by
 * realpath-ing the longest existing prefix and re-appending the rest.
 */
export const resolveExisting = (target: string): Effect.Effect<string> => {
  const absolute = path.resolve(expandUser(target));
  const walk = (head: string, tail: ReadonlyArray<string>): Effect.Effect<string> =>
    Effect.orElse(
      Effect.map(fileSystem.realPath(head), (resolved) =>
        path.join(resolved, ...[...tail].toReversed())
      ),
      () => {
        const parent = path.dirname(head);
        return parent === head
          ? Effect.succeed(absolute)
          : walk(parent, [...tail, path.basename(head)]);
      }
    );
  return walk(absolute, []);
};

const isInside = (parent: string, child: string): boolean =>
  child === parent || child.startsWith(parent + path.sep);

/** Build the workspace paths from `PLAY_ROOT` and `PLAY_STATE_DIR`. */
export const workspacePaths = (
  environment: Readonly<Record<string, string | undefined>> = process.env,
  cwd: string = process.cwd()
): Effect.Effect<WorkspacePaths, PlayerError> =>
  Effect.gen(function* () {
    const root = path.resolve(expandUser(environment['PLAY_ROOT'] ?? cwd));
    const configured = environment['PLAY_STATE_DIR'] ?? '.sessions';
    const expanded = expandUser(configured);
    const candidate = path.isAbsolute(expanded) ? expanded : path.join(root, expanded);
    const resolvedRoot = yield* resolveExisting(root);
    const stateRoot = yield* resolveExisting(candidate);
    return isInside(resolvedRoot, stateRoot)
      ? { root: resolvedRoot, stateRoot }
      : yield* playerError('PLAY_STATE_DIR must stay inside the player workspace');
  });

export const WorkspaceLive = Layer.effect(Workspace, workspacePaths());

/** A fixed workspace, for tests and for the offline byte-diff harness. */
export const workspaceLayer = (root: string, stateDir = '.sessions'): Layer.Layer<Workspace> =>
  Layer.succeed(Workspace, {
    root: path.resolve(root),
    stateRoot: path.isAbsolute(stateDir)
      ? path.resolve(stateDir)
      : path.join(path.resolve(root), stateDir),
  });

// ---------------------------------------------------------------------------
// Path containment
// ---------------------------------------------------------------------------

export interface StatePath {
  /** The absolute destination inside `stateRoot`. */
  readonly destination: string;
  /** Its components relative to `stateRoot`, at least one long. */
  readonly relative: readonly [string, ...ReadonlyArray<string>];
}

const BAD_PARTS: ReadonlySet<string> = new Set(['', '.', '..']);

export const stateRelativePath = (
  workspace: WorkspacePaths,
  target: string
): Effect.Effect<StatePath, PlayerError> =>
  Effect.suspend(() => {
    const lexical = path.resolve(expandUser(target));
    const lexicalWorkspace = path.resolve(workspace.root);
    const destination = isInside(lexicalWorkspace, lexical)
      ? path.join(workspace.root, path.relative(lexicalWorkspace, lexical))
      : lexical;
    if (!isInside(workspace.stateRoot, destination)) {
      return Effect.fail(playerError('private state files must stay inside PLAY_STATE_DIR'));
    }
    const relative = path
      .relative(workspace.stateRoot, destination)
      .split(path.sep)
      .filter((part) => part !== '');
    const [first, ...rest] = relative;
    if (first === undefined || relative.some((part) => BAD_PARTS.has(part))) {
      return Effect.fail(playerError('private state path is invalid'));
    }
    return Effect.succeed({ destination, relative: [first, ...rest] });
  });

// ---------------------------------------------------------------------------
// Symlink-safe directory walk
// ---------------------------------------------------------------------------

const REAL_DIR_ERROR =
  'private state directories must be real directories inside PLAY_STATE_DIR';

interface PathInspection {
  readonly symbolicLink: boolean;
  readonly info: FileSystem.File.Info | null;
}

const inspectPath = (target: string): Effect.Effect<PathInspection> =>
  Effect.gen(function* () {
    const link = yield* Effect.either(fileSystem.readLink(target));
    if (Either.isRight(link)) return { symbolicLink: true, info: null };
    const info = yield* Effect.either(fileSystem.stat(target));
    return Either.isRight(info)
      ? { symbolicLink: false, info: info.right }
      : { symbolicLink: false, info: null };
  });

const realDirectory = (target: string): Effect.Effect<boolean> =>
  Effect.map(
    inspectPath(target),
    ({ symbolicLink, info }) => !symbolicLink && info?.type === 'Directory'
  );

/** Walk into a state directory, refusing every symlinked component. */
export const openStateDirectory = (
  workspace: WorkspacePaths,
  parts: ReadonlyArray<string>,
  options: { readonly create: boolean }
): Effect.Effect<string, PlayerError> =>
  Effect.gen(function* () {
    const rootParts = path
      .relative(workspace.root, workspace.stateRoot)
      .split(path.sep)
      .filter((part) => part !== '');
    if (!isInside(workspace.root, workspace.stateRoot)) {
      return yield* playerError('PLAY_STATE_DIR must stay inside the player workspace');
    }
    if (!(yield* realDirectory(workspace.root))) {
      return yield* playerError('the player workspace is not a safe directory');
    }
    const descend = (parent: string, part: string): Effect.Effect<string, PlayerError> =>
      Effect.gen(function* () {
        if (BAD_PARTS.has(part)) return yield* playerError('private state path is invalid');
        const child = path.join(parent, part);
        const inspected = yield* inspectPath(child);
        if (inspected.symbolicLink) return yield* playerError(REAL_DIR_ERROR);
        if (inspected.info !== null) {
          return inspected.info.type === 'Directory'
            ? child
            : yield* playerError(REAL_DIR_ERROR);
        }
        if (!options.create) {
          return yield* playerError('private state directory does not exist');
        }
        yield* Effect.ignore(fileSystem.makeDirectory(child, { mode: 0o700 }));
        return (yield* realDirectory(child))
          ? child
          : yield* playerError(REAL_DIR_ERROR);
      });
    return yield* Effect.reduce([...rootParts, ...parts], workspace.root, descend);
  });

// ---------------------------------------------------------------------------
// The service
// ---------------------------------------------------------------------------

export interface PrivateFsApi {
  readonly workspace: WorkspacePaths;
  readonly resolve: (target: string) => Effect.Effect<StatePath, PlayerError>;
  readonly writeText: (target: string, text: string) => Effect.Effect<string, PlayerError>;
  readonly writeJson: (
    target: string,
    value: JsonValueInput
  ) => Effect.Effect<string, PlayerError>;
  readonly readText: (target: string, label: string) => Effect.Effect<string, PlayerError>;
  readonly loadObject: (target: string, label: string) => Effect.Effect<JsonObject, PlayerError>;
  readonly appendText: (target: string, text: string) => Effect.Effect<string, PlayerError>;
  /** `_state_regular_file` — the state-relative path of an existing regular file. */
  readonly regularFile: (target: string, label: string) => Effect.Effect<string, PlayerError>;
  readonly exists: (target: string) => Effect.Effect<boolean>;
  readonly openDirectory: (
    parts: ReadonlyArray<string>,
    options: { readonly create: boolean }
  ) => Effect.Effect<string, PlayerError>;
}

export class PrivateFs extends Context.Tag('PrivateFs')<PrivateFs, PrivateFsApi>() {}

const randomSuffix = (): string =>
  [...crypto.getRandomValues(new Uint8Array(6))]
    .map((byte) => byte.toString(16).padStart(2, '0'))
    .join('');

const leafOf = (relative: StatePath['relative']): string => relative.at(-1) ?? relative[0];
const bytes = (text: string): Uint8Array => new TextEncoder().encode(text);

const readAll = (opened: FileSystem.File) =>
  Effect.gen(function* () {
    const chunks: Uint8Array[] = [];
    for (;;) {
      const chunk = yield* opened.readAlloc(64 * 1024);
      if (chunk._tag === 'None') break;
      chunks.push(chunk.value);
    }
    return Buffer.concat(chunks.map((chunk) => Buffer.from(chunk)));
  });

const makeApi = (workspace: WorkspacePaths): PrivateFsApi => {
  const resolve = (target: string): Effect.Effect<StatePath, PlayerError> =>
    stateRelativePath(workspace, target);

  const parentOf = (
    relative: ReadonlyArray<string>,
    create: boolean
  ): Effect.Effect<string, PlayerError> =>
    openStateDirectory(workspace, relative.slice(0, -1), { create });

  const writeText = (target: string, text: string): Effect.Effect<string, PlayerError> =>
    Effect.gen(function* () {
      const { destination, relative } = yield* resolve(target);
      const parent = yield* parentOf(relative, true);
      const name = leafOf(relative);
      const temporary = path.join(parent, `.${name}.${randomSuffix()}.tmp`);
      const write = Effect.scoped(
        Effect.gen(function* () {
          const opened = yield* fileSystem.open(temporary, { flag: 'wx', mode: 0o600 });
          yield* opened.writeAll(bytes(text));
          yield* opened.sync;
        })
      ).pipe(
        Effect.flatMap(() => fileSystem.rename(temporary, path.join(parent, name))),
        Effect.as(destination),
        Effect.mapError(() =>
          playerError('cannot safely write private state inside PLAY_STATE_DIR')
        )
      );
      return yield* Effect.onError(write, () =>
        Effect.ignore(fileSystem.remove(temporary, { force: true }))
      );
    });

  const readText = (target: string, label: string): Effect.Effect<string, PlayerError> =>
    Effect.gen(function* () {
      const { relative } = yield* resolve(target);
      const parent = yield* parentOf(relative, false);
      const file = path.join(parent, leafOf(relative));
      const inspected = yield* inspectPath(file);
      if (inspected.symbolicLink) {
        return yield* playerError(`cannot safely read private ${label}`);
      }
      const readFailure = playerError(`cannot safely read private ${label}`);
      return yield* Effect.scoped(
        Effect.gen(function* () {
          const opened = yield* Effect.mapError(fileSystem.open(file), () => readFailure);
          const info = yield* Effect.mapError(opened.stat, () => readFailure);
          if (info.type !== 'File' || (info.mode & 0o777) !== 0o600) {
            return yield* playerError(`private ${label} must be a mode-0600 file`);
          }
          const content = yield* Effect.mapError(readAll(opened), () => readFailure);
          return new TextDecoder('utf-8', { fatal: false, ignoreBOM: true }).decode(content);
        })
      );
    });

  const appendText = (target: string, text: string): Effect.Effect<string, PlayerError> =>
    Effect.gen(function* () {
      const { destination, relative } = yield* resolve(target);
      const parent = yield* parentOf(relative, true);
      const file = path.join(parent, leafOf(relative));
      const inspected = yield* inspectPath(file);
      if (inspected.symbolicLink) {
        return yield* playerError('cannot safely append to private state');
      }
      if (inspected.info === null) {
        yield* Effect.ignore(
          fileSystem.writeFile(file, new Uint8Array(), { flag: 'wx', mode: 0o600 })
        );
        const created = yield* inspectPath(file);
        if (created.symbolicLink) {
          return yield* playerError('cannot safely append to private state');
        }
      }
      const appendFailure = playerError('cannot safely append to private state');
      return yield* Effect.scoped(
        Effect.gen(function* () {
          const opened = yield* Effect.mapError(
            fileSystem.open(file, { flag: 'a', mode: 0o600 }),
            () => appendFailure
          );
          const info = yield* Effect.mapError(opened.stat, () => appendFailure);
          if (info.type !== 'File') return yield* playerError('a private log must be a regular file');
          yield* Effect.mapError(fileSystem.chmod(file, 0o600), () => appendFailure);
          yield* Effect.mapError(opened.writeAll(bytes(text)), () => appendFailure);
          return destination;
        })
      );
    });

  return {
    workspace,
    resolve,
    writeText,
    writeJson: (target, value) => writeText(target, `${indentedJson(value)}\n`),
    readText,
    loadObject: (target, label) =>
      Effect.gen(function* () {
        const text = yield* readText(target, label);
        const decoded = yield* Effect.mapError(
          Schema.decodeUnknown(Schema.parseJson(Schema.Unknown))(text),
          () => playerError(`cannot read ${label}: invalid JSON`)
        );
        return yield* Effect.mapError(
          Schema.decodeUnknown(JsonObjectSchema)(decoded),
          () => playerError(`${label} must contain a JSON object`)
        );
      }),
    appendText,
    regularFile: (target, label) =>
      Effect.gen(function* () {
        const { relative } = yield* resolve(target);
        const parent = yield* parentOf(relative, false);
        const file = path.join(parent, leafOf(relative));
        const inspected = yield* inspectPath(file);
        return !inspected.symbolicLink && inspected.info?.type === 'File'
          ? relative.join(path.sep)
          : yield* playerError(`the ${label} must be a real file inside PLAY_STATE_DIR`);
      }),
    exists: (target) =>
      Effect.map(
        inspectPath(path.resolve(expandUser(target))),
        (inspected) => !inspected.symbolicLink && inspected.info?.type === 'File'
      ),
    openDirectory: (parts, options) => openStateDirectory(workspace, parts, options),
  };
};

export const PrivateFsLive: Layer.Layer<PrivateFs, never, Workspace> = Layer.effect(
  PrivateFs,
  Effect.map(Workspace, makeApi)
);

/** Build the API directly, for tests that do not want a Layer. */
export const privateFsFor = (workspace: WorkspacePaths): PrivateFsApi => makeApi(workspace);
