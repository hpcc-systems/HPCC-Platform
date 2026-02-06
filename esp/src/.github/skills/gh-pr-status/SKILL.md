---
name: gh-pr-status
description: Check GitHub Actions status for pull requests and download failed workflow artifacts. Use when users ask to verify CI/CD status, check test results, find failing workflows, investigate build failures, or collect failure artifacts for the current branch or a specific PR number. For esp/src workspace changes, pay special attention to the "Test ECL Watch" workflow.
---

# GitHub Actions PR Checker

Automates checking GitHub Actions workflow status and collecting failure artifacts for pull requests.

## ECL Watch Specific Workflow

**Important**: For changes in the `esp/src` workspace, the critical workflow is:
- **Test ECL Watch (test-eclwatch.yml)** - Runs Playwright tests for the ECL Watch UI
- Uploads `eclwatch-test-results` artifact on failure
- Tests against a running HPCC-Platform instance

When checking CI for ECL Watch changes, prioritize failures in this workflow as they directly test your code.

## Quick Start

Run from repository root:
```bash
npx tsx .github/skills/gh-pr-status/scripts/check_pr_actions.ts
```

This checks the PR for your current branch, reports status, and downloads artifacts from failed runs to `tmp-gh-pr-status/`.

## When to Use

- User asks to "check CI", "verify tests", "check build status", "what's failing in CI"
- User wants to investigate test failures or download test reports
- User mentions GitHub Actions, workflows, or CI/CD status
- After making changes to verify all checks pass
- **Especially important after ECL Watch (esp/src) changes** to check Playwright test results

## Script Options

**Basic usage:**
```bash
# Check current branch
npx tsx .github/skills/gh-pr-status/scripts/check_pr_actions.ts

# Check specific PR
npx tsx .github/skills/gh-pr-status/scripts/check_pr_actions.ts --pr 12345

# Check different branch
npx tsx .github/skills/gh-pr-status/scripts/check_pr_actions.ts --branch feature/my-branch

# Custom output directory
npx tsx .github/skills/gh-pr-status/scripts/check_pr_actions.ts --output ./test-failures

# Different repository
npx tsx .github/skills/gh-pr-status/scripts/check_pr_actions.ts --repo owner/repo-name
```

**Combined options:**
```bash
npx tsx .github/skills/gh-pr-status/scripts/check_pr_actions.ts --pr 12345 --output ./pr-12345-results
```

## How It Works

1. **Authenticate**: Verifies GitHub CLI is authenticated
2. **Identify PR**: Finds PR matching current branch or uses specified PR number
3. **Check Runs**: Lists all workflow runs for the PR's head commit
4. **Report Status**: Shows completed (success/failed) and in-progress runs
5. **Download Artifacts**: For failed runs, downloads artifacts to output directory
6. **Extract**: Auto-extracts any ZIP files in downloaded artifacts

**Failure criteria**: Any completed run with conclusion ≠ `success` (includes `failure`, `cancelled`, `timed_out`, etc.)

## Requirements

- **GitHub CLI**: Install from https://cli.github.com/
- **Authentication**: Run `gh auth login` if not already authenticated
- **Git repository**: Must be run from within a git repository

## Output Structure

```
tmp-gh-pr-status/
├── run-123456/
│   ├── test-results/
│   │   └── results.xml
│   └── playwright-report/
│       ├── index.html
│       └── ...
└── run-789012/
    └── build-logs/
        └── output.log
```

Each failed run gets its own directory named `run-<runId>`.

## Common Scenarios

**Scenario 1: Check CI status (ECL Watch changes)**
```bash
npx tsx .github/skills/gh-pr-status/scripts/check_pr_actions.ts
```
Output: Lists all runs, highlights failures including "Test ECL Watch" workflow, downloads Playwright test results to `tmp-gh-pr-status/run-*/eclwatch-test-results/`

**Scenario 2: No PR exists yet**
Error: "No PR found for branch 'my-branch'."
→ Create a PR first or use `--branch` with an existing PR's branch.

**Scenario 3: Runs still in progress**
Output: "X workflow run(s) are still in progress."
→ Wait for completion or check downloaded artifacts from already-failed runs.

**Scenario 4: All checks passing**
Output: "All completed workflow runs succeeded."
→ No artifacts downloaded, exit code 0.

## Troubleshooting

**"GitHub CLI is not authenticated"**
→ Run: `gh auth login`

**"Detached HEAD. Provide --branch or --pr."**
→ Checkout a branch or specify `--pr <number>` explicitly.

**"No artifacts found or download failed"**
→ The workflow run may not have uploaded artifacts, or they've expired (default 90 days).

## Exit Codes

- `0`: All runs succeeded (or no runs found)
- `1`: One or more runs failed (artifacts downloaded)
- `2`: Error occurred (authentication, git, etc.)

## Additional Resources

For advanced usage and detailed information:
- **[API Reference](references/API.md)** - Complete function documentation and data structures
- **[Advanced Usage](references/ADVANCED.md)** - Error handling, customization, and optimization
- **[Practical Examples](references/EXAMPLES.md)** - Real-world workflows and automation scripts
