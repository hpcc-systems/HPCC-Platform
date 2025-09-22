---
applyTo: '**/*.tsx'
---
# Project coding standards for TypeScript and React

Apply the [general coding guidelines](./general-coding.instructions.md) to all code.

## TypeScript Guidelines
- Use TypeScript for all new code and avoid `any` types; leverage strict typing and generics.
- Prefer functional programming principles (pure functions, no side effects, immutability).
- Use interfaces and type aliases for data structures and props.
- Prefer immutable data (`const`, `readonly`, spread operators for updates).
- Use optional chaining (`?.`) and nullish coalescing (`??`) for safe property access.
- Organize types and interfaces in dedicated files for reusability.
- Use type-safe utility libraries when possible.

## React Guidelines
- Use functional components with hooks (avoid class components).
- Follow the React hooks rules (no conditional or nested hooks).
- Use `React.FunctionComponent` type for components with children, and define prop types explicitly.
- Prefer `useStyles` over inline styles for consistency and performance.
- Keep components small, focused, and reusable; follow single-responsibility principle.
- Use CSS modules or CSS-in-JS for component-level styling; avoid global styles.
- Use `useCallback`, `useMemo`, and `React.memo` for performance optimization.
- Prefer controlled components for forms; use libraries like `formik` or `react-hook-form` for complex forms.
- Use context and custom hooks for shared state and logic.
- Write unit tests for all components and hooks (e.g., with Jest, React Testing Library).
- Ensure accessibility (ARIA attributes, keyboard navigation, semantic HTML).

## Key Frameworks and Libraries

- [React](https://reactjs.org/): Core UI library for building components.
- [TypeScript](https://www.typescriptlang.org/): Static typing for safer, more maintainable code.
- [Fluent UI](https://react.fluentui.dev/?path=/docs/concepts-introduction--page): Microsoft’s React component library for consistent, accessible UI.
- [Formik](https://formik.org/): For building and managing complex forms.
- [React Hook Form](https://react-hook-form.com/): Lightweight form validation and management.
- [react-reflex](https://github.com/leefsmp/Re-Flex): For resizable split views.
- [react-singleton-hook](https://github.com/alfonsogarciacaro/react-singleton-hook): For singleton React hooks.
- [react-sizeme](https://github.com/ctrlplusb/react-sizeme): For responsive components based on container size.
- [@hpcc-js/*](https://www.npmjs.com/org/hpcc-js): HPCC Systems visualization and utility libraries (charts, graphs, layouts, maps, etc.).
- [d3-dsv](https://github.com/d3/d3-dsv): For parsing and formatting delimited text (CSV, TSV).
- [octokit](https://github.com/octokit/octokit.js): GitHub API client.
- [clipboard](https://github.com/zenorocha/clipboard.js): For copying content to the clipboard.
- [query-string](https://github.com/sindresorhus/query-string): For parsing and stringifying URL query strings.

> See `package.json` for the full list of dependencies and their versions.
