import { createEffectConfig } from "@freeciv/oxlint-config";
import recommended from "./node_modules/@effect/tsgo/oxlint-presets/recommended.json" with { type: "json" };

export default createEffectConfig(recommended);
