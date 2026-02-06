# Scripts

## check_pr_actions.ts

TypeScript script to check GitHub Actions workflow status for pull requests and download failed workflow artifacts.

### Prerequisites

- **GitHub CLI**: Install from https://cli.github.com/
- **Authentication**: Run `gh auth login` if not already authenticated
- **tsx**: For running TypeScript (use `npx tsx` - no install needed)

### Usage

```bash
# Check current branch
npx tsx check_pr_actions.ts

# Check specific PR
npx tsx check_pr_actions.ts --pr 12345

# Check different branch
npx tsx check_pr_actions.ts --branch feature/my-branch

# Custom output directory
npx tsx check_pr_actions.ts --output ./test-failures

# Different repository
npx tsx check_pr_actions.ts --repo owner/repo-name

# Get help
npx tsx check_pr_actions.ts --help
```

### Features

- Automatically detects PR from current branch
- Reports workflow run status (success/failed/in-progress)
- Highlights ECL Watch test failures
- Downloads artifacts from failed runs
- Auto-extracts ZIP files
- Provides direct links to Playwright test reports

### Output

Downloads artifacts to `tmp-gh-pr-status/` by default:
```
tmp-gh-pr-status/
├── run-123456/
│   └── eclwatch-test-results/
│       ├── playwright-report/
│       └── test-results/
└── run-789012/
    └── other-artifacts/
```

### Exit Codes

- `0` - All runs succeeded (or no runs found)
- `1` - One or more runs failed (artifacts downloaded)
- `2` - Error occurred (authentication, git, etc.)

### ECL Watch Integration

The script automatically prioritizes ECL Watch test failures and provides direct paths to Playwright reports when available.

For more information, see the [main SKILL.md](../SKILL.md).
