# Script API Reference

Complete reference for `check_pr_actions.js` functions and utilities.

## Command-Line Interface

### Synopsis
```bash
node check_pr_actions.js [OPTIONS]
```

### Options

| Option | Type | Description | Default |
|--------|------|-------------|---------|
| `--branch <name>` | string | Override branch name detection | Current git branch |
| `--pr <number>` | number | Check specific PR by number | Auto-detect from branch |
| `--repo <owner/repo>` | string | Repository to check | Current git repository |
| `--output <dir>` | string | Output directory for artifacts | `tmp-gh-pr-status` |

### Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success (all runs passed or no runs found) |
| 1 | Failures detected (artifacts downloaded) |
| 2 | Error (auth, git, invalid arguments, etc.) |

## Internal Functions

### Authentication & Git

#### `requireGhAuth()`
Verifies GitHub CLI authentication status.

**Throws**: Error if not authenticated
**Example**:
```javascript
requireGhAuth();
// Proceeds if authenticated, throws otherwise
```

#### `getRepoRoot()`
Gets the git repository root directory.

**Returns**: `string` - Absolute path to repository root
**Throws**: Error if not in a git repository
**Example**:
```javascript
const root = getRepoRoot();
// "/Users/name/projects/my-repo"
```

#### `getCurrentBranch()`
Gets the current git branch name.

**Returns**: `string` - Branch name
**Throws**: Error if in detached HEAD state
**Example**:
```javascript
const branch = getCurrentBranch();
// "feature/new-feature"
```

### PR Operations

#### `listPrsForBranch(branch, repo)`
Lists all PRs for a given branch.

**Parameters**:
- `branch` (string) - Branch name (e.g., "main", "feature/new-feature")
- `repo` (string|undefined) - Repository in "owner/name" format (optional)

**Returns**: `Array<Object>` - Array of PR objects
```javascript
[
  {
    number: 123,
    title: "Add new feature",
    url: "https://github.com/owner/repo/pull/123",
    updatedAt: "2024-01-15T10:30:00Z",
    headRefName: "feature/new-feature",
    baseRefName: "main"
  }
]
```

**Example**:
```javascript
const prs = listPrsForBranch("feature/new-feature");
const prs2 = listPrsForBranch("main", "hpcc-systems/HPCC-Platform");
```

#### `selectLatestPr(prs)`
Selects the most recently updated PR from an array.

**Parameters**:
- `prs` (Array<Object>) - Array of PR objects with `updatedAt` field

**Returns**: `Object` - Most recently updated PR
**Example**:
```javascript
const prs = listPrsForBranch("feature/new-feature");
const latest = selectLatestPr(prs);
```

#### `getPrInfo(prNumber, repo)`
Gets detailed information about a specific PR.

**Parameters**:
- `prNumber` (number) - PR number
- `repo` (string|undefined) - Repository in "owner/name" format (optional)

**Returns**: `Object` - PR information
```javascript
{
  headRefOid: "abc123def456...",
  headRefName: "feature/new-feature"
}
```

**Example**:
```javascript
const info = getPrInfo(123);
const info2 = getPrInfo(456, "hpcc-systems/HPCC-Platform");
```

### Workflow Run Operations

#### `listRunsForPr(prNumber, repo)`
Lists all GitHub Actions workflow runs for a PR.

**Parameters**:
- `prNumber` (number) - PR number
- `repo` (string|undefined) - Repository in "owner/name" format (optional)

**Returns**: `Array<Object>` - Array of workflow run objects
```javascript
[
  {
    databaseId: 123456789,
    status: "completed",
    conclusion: "success",
    name: "CI",
    url: "https://github.com/owner/repo/actions/runs/123456789",
    updatedAt: "2024-01-15T10:30:00Z"
  }
]
```

**Status values**: `queued`, `in_progress`, `completed`
**Conclusion values**: `success`, `failure`, `cancelled`, `skipped`, `timed_out`, `action_required`, `neutral`, `stale`

**Example**:
```javascript
const runs = listRunsForPr(123);
const completed = runs.filter(r => r.status === "completed");
const failed = completed.filter(r => r.conclusion !== "success");
```

### Artifact Operations

#### `downloadArtifacts(runId, outputDir, repo)`
Downloads all artifacts for a workflow run.

**Parameters**:
- `runId` (number) - Workflow run ID
- `outputDir` (string) - Directory to download artifacts to
- `repo` (string|undefined) - Repository in "owner/name" format (optional)

**Returns**: `boolean` - `true` if successful, `false` otherwise
**Side effects**: Creates `outputDir` if it doesn't exist

**Example**:
```javascript
const success = downloadArtifacts(123456789, "./artifacts/run-123456789");
if (success) {
  console.log("Downloaded successfully");
} else {
  console.log("No artifacts or download failed");
}
```

#### `extractZip(zipPath)`
Extracts a ZIP file to a directory with the same name.

**Parameters**:
- `zipPath` (string) - Path to ZIP file

**Returns**: `boolean` - `true` if extraction succeeded
**Side effects**: 
- Creates extraction directory
- Deletes original ZIP after successful extraction
- Uses PowerShell on Windows, unzip on Unix

**Example**:
```javascript
const extracted = extractZip("/path/to/artifacts.zip");
// Creates /path/to/artifacts/ directory
// Deletes /path/to/artifacts.zip
```

#### `extractZipFiles(outputDir)`
Recursively finds and extracts all ZIP files in a directory.

**Parameters**:
- `outputDir` (string) - Root directory to search

**Side effects**: 
- Extracts all found ZIP files
- Deletes original ZIPs after successful extraction

**Example**:
```javascript
extractZipFiles("./tmp-gh-pr-status/run-123456789");
// Finds and extracts all .zip files in the tree
```

### Utility Functions

#### `run(cmd, args, options)`
Executes a shell command synchronously.

**Parameters**:
- `cmd` (string) - Command to execute
- `args` (Array<string>) - Command arguments
- `options` (Object) - Options
  - `check` (boolean) - Throw on non-zero exit (default: `true`)
  - Other options passed to `child_process.spawnSync`

**Returns**: `Object` - spawnSync result
```javascript
{
  status: 0,
  stdout: "command output",
  stderr: "",
  // ... other properties from spawnSync
}
```

**Throws**: Error if `check: true` and exit code is non-zero

**Example**:
```javascript
// Throws on failure
const result = run("git", ["status"]);

// Returns result without throwing
const result2 = run("git", ["status"], { check: false });
if (result2.status !== 0) {
  console.error("Failed");
}
```

#### `commandExists(command)`
Checks if a command exists in PATH.

**Parameters**:
- `command` (string) - Command name (e.g., "git", "gh", "unzip")

**Returns**: `boolean` - `true` if command exists
**Example**:
```javascript
if (commandExists("unzip")) {
  // Use unzip
} else {
  // Fall back to another method
}
```

#### `parseArgs(argv)`
Parses command-line arguments.

**Parameters**:
- `argv` (Array<string>) - Arguments array (typically `process.argv.slice(2)`)

**Returns**: `Object` - Parsed options
```javascript
{
  branch: undefined,
  pr: undefined,
  repo: undefined,
  output: "tmp-gh-pr-status"
}
```

**Example**:
```javascript
const args = parseArgs(["--pr", "123", "--output", "./results"]);
// { branch: undefined, pr: 123, repo: undefined, output: "./results" }
```

## Data Structures

### PR Object
```typescript
interface PR {
  number: number;
  title: string;
  url: string;
  updatedAt: string; // ISO 8601 datetime
  headRefName: string;
  baseRefName: string;
}
```

### Workflow Run Object
```typescript
interface WorkflowRun {
  databaseId: number;
  status: "queued" | "in_progress" | "completed";
  conclusion: "success" | "failure" | "cancelled" | "skipped" | 
              "timed_out" | "action_required" | "neutral" | "stale" | null;
  name: string;
  url: string;
  updatedAt: string; // ISO 8601 datetime
}
```

### PR Info Object
```typescript
interface PRInfo {
  headRefOid: string; // Git commit SHA
  headRefName: string; // Branch name
}
```

## Error Handling

### Common Errors

**"GitHub CLI is not authenticated"**
- Cause: `gh auth status` fails
- Fix: Run `gh auth login`

**"Command failed: git rev-parse --show-toplevel"**
- Cause: Not in a git repository
- Fix: Navigate to repository root

**"Detached HEAD. Provide --branch or --pr."**
- Cause: Git is in detached HEAD state
- Fix: Checkout a branch or use `--pr` option

**"No PR found for branch 'X'"**
- Cause: No PR exists for the branch
- Fix: Create a PR or use `--pr` option

## Examples

### Basic Usage
```javascript
// Check current branch
node check_pr_actions.js

// Check specific PR
node check_pr_actions.js --pr 123

// Custom output directory
node check_pr_actions.js --output ./my-artifacts
```

### Programmatic Usage
If you want to use functions from another script:

```javascript
const { requireGhAuth, listPrsForBranch, listRunsForPr } = require('./check_pr_actions.js');

requireGhAuth();
const prs = listPrsForBranch('main');
const runs = listRunsForPr(prs[0].number);
const failed = runs.filter(r => r.status === 'completed' && r.conclusion !== 'success');

console.log(`Found ${failed.length} failed runs`);
```

Note: The script is designed to be run as a CLI tool, so some refactoring would be needed for clean programmatic usage.
