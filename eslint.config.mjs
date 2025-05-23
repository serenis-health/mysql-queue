// @ts-check

import eslint from "@eslint/js";
import { globalIgnores } from "eslint/config";
import tseslint from "typescript-eslint";

export default tseslint.config(eslint.configs.recommended, tseslint.configs.recommended, globalIgnores(["**/dist/**"]), {
  rules: {
    "@typescript-eslint/no-unused-vars": [
      "error",
      {
        argsIgnorePattern: "^_",
        caughtErrorsIgnorePattern: "^_",
        varsIgnorePattern: "^_",
      },
    ],
    "func-style": ["error", "declaration"],
    "object-shorthand": ["error"],
    "sort-imports": ["error", { ignoreCase: true }],
    "sort-keys": ["error", "asc", { caseSensitive: true, natural: true }],
  },
});
