/**
 * A throwaway player workspace on disk.
 *
 * The private-state sandbox refuses anything outside `PLAY_STATE_DIR`, so a
 * test that touches state needs a real directory tree. On macOS `/tmp` reaches
 * the canonical private temporary root through the same symlink as the system
 * temporary directory, keeping that containment case in the fixture.
 */
import { Clock, Config, Effect, Layer } from 'effect';
import {
  PrivateFs,
  type Workspace,
  privateFsFor,
  workspaceLayer,
  type PrivateFsApi,
  type WorkspacePaths,
} from 'src/services/private-fs';
import { path, withTestFileSystem } from 'test/_test-platform';

export interface Scratch {
  readonly workspace: WorkspacePaths;
  readonly files: PrivateFsApi;
  readonly layer: Layer.Layer<Workspace | PrivateFs>;
  readonly cleanup: Effect.Effect<void>;
}

const tempRoot = Config.string('PLAY_CLI_TEST_TMP').pipe(
  Config.withDefault('/tmp'),
  Effect.flatMap((configured) => withTestFileSystem((files) => files.realPath(configured))),
  Effect.orDie
);
let workspaceSequence = 0;

export const scratchWorkspace = (stateDir = '.sessions'): Effect.Effect<Scratch> =>
  Effect.gen(function* () {
    const resolvedTempRoot = yield* tempRoot;
    const bootstrapWorkspace: WorkspacePaths = {
      root: resolvedTempRoot,
      stateRoot: resolvedTempRoot,
    };
    const bootstrapFiles = privateFsFor(bootstrapWorkspace);
    const now = yield* Clock.currentTimeNanos;
    const sequence = yield* Effect.sync(() => workspaceSequence++);
    const directory = `play-cli-${now}-${sequence}`;
    const root = path.join(resolvedTempRoot, directory);
    const stateRoot = path.join(root, stateDir);
    yield* Effect.orDie(
      bootstrapFiles.openDirectory([directory, stateDir], { create: true })
    );
    const workspace: WorkspacePaths = { root, stateRoot };
    const files = privateFsFor(workspace);
    return {
      workspace,
      files,
      layer: Layer.merge(workspaceLayer(root, stateDir), Layer.succeed(PrivateFs, files)),
      cleanup: withTestFileSystem((platformFiles) =>
        platformFiles.remove(root, { recursive: true, force: true })
      ).pipe(Effect.orDie),
    };
  });

/** Run one effect against a fresh workspace and always clean it up. */
export const withScratchWorkspace = <A, E>(
  body: (scratch: Scratch) => Effect.Effect<A, E>
): Effect.Effect<A, E> =>
  Effect.acquireUseRelease(scratchWorkspace(), body, (scratch) => scratch.cleanup);
