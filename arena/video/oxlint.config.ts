import { createReactConfig } from "@freeciv/oxlint-config";

export default createReactConfig({
  ignorePatterns: ["dist", "node_modules", "out"],
});
