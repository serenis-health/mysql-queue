// @ts-check

import eslint from "@eslint/js";
import tseslint from "typescript-eslint";
import { globalIgnores } from "eslint/config";

export default tseslint.config(
  eslint.configs.recommended,
  tseslint.configs.recommended,
  globalIgnores(["**/dist/**"]),
  {
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
  },
);
