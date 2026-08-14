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
  readonly cleanup: () => Promise<void>;
}

const configuredTempRoot = Effect.runSync(
  Config.string('PLAY_CLI_TEST_TMP').pipe(Config.withDefault('/tmp'))
);
const tempRoot = await Effect.runPromise(
  withTestFileSystem((files) => files.realPath(configuredTempRoot)).pipe(Effect.orDie)
);
let workspaceSequence = 0;

export const scratchWorkspace = (stateDir = '.sessions'): Scratch => {
  const bootstrapWorkspace: WorkspacePaths = { root: tempRoot, stateRoot: tempRoot };
  const bootstrapFiles = privateFsFor(bootstrapWorkspace);
  const directory = `play-cli-${Effect.runSync(Clock.currentTimeNanos)}-${workspaceSequence}`;
  workspaceSequence += 1;
  const root = path.join(tempRoot, directory);
  const stateRoot = path.join(root, stateDir);
  Effect.runSync(
    bootstrapFiles.openDirectory([directory, stateDir], { create: true })
  );
  const workspace: WorkspacePaths = { root, stateRoot };
  const files = privateFsFor(workspace);
  return {
    workspace,
    files,
    layer: Layer.merge(workspaceLayer(root, stateDir), Layer.succeed(PrivateFs, files)),
    cleanup: () =>
      Effect.runPromise(
        withTestFileSystem((platformFiles) =>
          platformFiles.remove(root, { recursive: true, force: true })
        )
      ),
  };
};

/** Run one effect against a fresh workspace and always clean it up. */
export const withScratchWorkspace = <A, E>(
  body: (scratch: Scratch) => Effect.Effect<A, E>
): Effect.Effect<A, E> =>
  Effect.acquireUseRelease(
    Effect.sync(() => scratchWorkspace()),
    body,
    (scratch) => Effect.promise(scratch.cleanup)
  );
