---
applyTo: '**'
---
# GitHub Copilot Instructions for ECL Watch (TypeScript/React Project)

## Project Overview

This is the **ECL Watch** web interface for the HPCC Platform, located in the `esp/src/` directory. This is a **separate TypeScript/React project** from the main C++ codebase.

## Build System
- **Build tool**: npm
- **Package manager**: npm (with package-lock.json)
- **Bundler**: Webpack
- **TypeScript**: Yes, with tsconfig.json
- **Testing**: Playwright

## Build Commands

```bash
# Navigate to the project root first
cd esp/src

# Install dependencies
npm ci

# Development build (with watch)
npm run build-dev

# Production build
npm run build

# Individual commands
npm run copy-redux     # Copy static resources
npm run compile        # TypeScript compilation
npm run bundle         # Webpack bundling
npm run lint           # ESLint
npm run test           # Playwright tests
```

## Project Structure

### Key Directories
- `eclwatch/` - Legacy Dojo-based interface
- `src/` - Modern TypeScript source code shared with dojo and react
- `src-react/` - React components and modern UI
- `tests/` - Playwright test files

### Key Files
- `package.json` - npm configuration and scripts
- `tsconfig.json` - TypeScript configuration
- `webpack.config.js` - Webpack bundling configuration
- `eslint.config.mjs` - ESLint configuration
- `playwright.config.ts` - Playwright test configuration

## Technology Stack

### Core Technologies
- **TypeScript** - Primary language
- **React 17.0.2** - UI framework
- **Dojo 1.17.3** - Legacy UI framework (being migrated from)
- **Webpack 5** - Module bundler
- **ESLint** - Code linting
- **Playwright** - End-to-end testing

### UI Libraries
- **@fluentui/react** - Microsoft Fluent UI components
- **@hpcc-js/** - HPCC-specific visualization and utility libraries
- **formik** - Form handling
- **react-hook-form** - Modern form handling

### Visualization
- **@hpcc-js/chart** - Charting components
- **@hpcc-js/graph** - Graph visualization
- **@hpcc-js/map** - Mapping components
- **d3-dsv** - Data manipulation

## Development Workflow

### Code Quality
```bash
# Lint code
npm run lint

# Fix linting issues
npm run lint-fix
```

### Testing
```bash
# Run tests
npm test

# Run tests in interactive mode
npm run test-interactive

# Generate test code
npm run test-codegen
```

## Code Style
- Follow TypeScript best practices
- Use ESLint configuration in `eslint.config.mjs`
- React components should use hooks pattern
- Prefer functional components over class components

## Key npm Scripts
- `watch` - Start development with watch mode
- `build` - Full production build
- `compile` - TypeScript compilation only
- `bundle` - Webpack bundling only
- `test` - Run Playwright tests
- `lint` - Run ESLint

## Migration Notes
This project is in the process of migrating from Dojo to React. When working on the codebase:
- New features should use React (`src-react/`)
- Legacy Dojo code exists in `eclwatch/`
- Prefer modern TypeScript patterns

# Language localisation
- Any english language strings should be extracted to nlsHPCC for localization.
- Localization files are located in the src/nls directory.
- Use `nlsHPCC` to localize strings in the code.
- Localization structures are sorted alphabetically by key.
- New localization keys should be added alphabetically by key.
- New localization keys should not replace existing keys.
- Do not change other localisation keys in the same file.
