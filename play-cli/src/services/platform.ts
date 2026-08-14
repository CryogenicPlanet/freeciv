import { FileSystem, Path } from '@effect/platform';
import * as BunFileSystem from '@effect/platform-bun/BunFileSystem';
import * as BunPath from '@effect/platform-bun/BunPath';
import { Context, Effect, Layer } from 'effect';

const platformContext = Effect.runSync(
  Effect.scoped(Layer.build(Layer.merge(BunFileSystem.layer, BunPath.layer)))
);

/** The official Effect Platform filesystem used by every source-owned I/O boundary. */
export const fileSystem: FileSystem.FileSystem = Context.get(
  platformContext,
  FileSystem.FileSystem
);

/** The official POSIX path service. The CLI is POSIX-only because it uses flock(2). */
export const path: Path.Path = Context.get(platformContext, Path.Path);
