const antiSlopRules = {
  "anti-slop/no-chained-type-assertions": "error",
  "anti-slop/no-conditional-empty-object-spread": "error",
  "anti-slop/no-known-value-widening": "error",
  "anti-slop/no-module-mocking": "error",
  "anti-slop/no-object-parameters": "error",
  "anti-slop/no-reflect-apply": "error",
  "anti-slop/no-reflect-get": "error",
  "anti-slop/no-runtime-typeof": "error",
  "anti-slop/no-shape-in-symbol-names": "error",
  "anti-slop/no-unknown-parameters": "error",
  "anti-slop/no-unknown-returns": "error",
  "anti-slop/no-unknown-type-aliases": "error",
  "anti-slop/no-unsafe-dictionary-type": "error",
  "anti-slop/no-widen-then-assert": "error",
  "anti-slop/require-safety-comment-for-type-assertion": "error",
};

const commonConfig = {
  jsPlugins: [
    { name: "anti-slop", specifier: "@freeciv/oxlint-config/anti-slop" },
  ],
  options: { reportUnusedDisableDirectives: "deny" },
  categories: {
    correctness: "deny",
    suspicious: "warn",
  },
};

const consistentTypeImports = [
  "warn",
  { prefer: "type-imports", fixStyle: "inline-type-imports" },
];

export const createEffectConfig = (recommended) => ({
  ...recommended,
  ...commonConfig,
  options: { ...recommended.options, ...commonConfig.options },
  plugins: ["unicorn", "typescript", "oxc", ...recommended.plugins],
  ignorePatterns: ["dist", "node_modules", "oxlint.config.ts"],
  rules: {
    ...recommended.rules,
    "typescript/no-explicit-any": "deny",
    "typescript/consistent-type-imports": consistentTypeImports,
    "eslint/no-underscore-dangle": "off",
    "typescript/no-misused-spread": [
      "deny",
      { allow: [{ from: "lib", name: "string" }] },
    ],
    "typescript/require-array-sort-compare": [
      "deny",
      { ignoreStringArrays: true },
    ],
    ...antiSlopRules,
  },
});

export const createReactConfig = ({ ignorePatterns = ["dist", "node_modules"] } = {}) => ({
  ...commonConfig,
  ignorePatterns: [...ignorePatterns, "oxlint.config.ts"],
  plugins: ["typescript", "unicorn", "oxc", "react", "vitest"],
  rules: {
    "eslint/no-unused-vars": [
      "deny",
      { argsIgnorePattern: "^_", varsIgnorePattern: "^_" },
    ],
    "typescript/consistent-type-imports": consistentTypeImports,
    "typescript/no-explicit-any": "deny",
    "typescript/no-floating-promises": "deny",
    "typescript/await-thenable": "deny",
    "typescript/restrict-template-expressions": "deny",
    "typescript/require-await": "off",
    ...antiSlopRules,
  },
});
