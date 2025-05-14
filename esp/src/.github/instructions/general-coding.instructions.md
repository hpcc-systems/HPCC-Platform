---
applyTo: '**/*.ts'
---
# General coding instructions

- Follow TypeScript best practices, including strict typing and avoiding the use of `any`.
- Use modern ES6+ syntax (e.g., `const`, `let`, arrow functions, destructuring).
- Ensure code is modular and reusable by following component-based architecture.
- Use meaningful variable and function names that reflect their purpose.
- Write comments for complex logic or non-obvious code.
- Follow a consistent coding style (e.g., ESLint rules).
- Avoid hardcoding values; use environment variables or configuration files.
- Ensure proper error handling and avoid unhandled promises.
- Write unit tests for all components and functions.
- Optimize for performance and accessibility (e.g., lazy loading, ARIA attributes).

# Language localisation

- Any english language strings should be extracted to nlsHPCC for localization.
- Localization files are located in the src/nls directory.
- Use `nlsHPCC` to localize strings in the code.
- Localization structures are sorted alphabetically by key.
- New localization keys should be added alphabetically by key.
- New localization keys should not replace existing keys.
- Do not change other localisation keys in the same file.