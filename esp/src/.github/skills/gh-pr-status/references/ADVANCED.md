# Advanced Usage and Error Handling

## Understanding Workflow Run States

### Run Status
- `queued`: Waiting to start
- `in_progress`: Currently executing
- `completed`: Finished (check conclusion for result)

### Run Conclusion (for completed runs)
- `success`: All jobs passed
- `failure`: One or more jobs failed
- `cancelled`: Manually cancelled
- `skipped`: Skipped (e.g., duplicate run)
- `timed_out`: Exceeded timeout limit
- `action_required`: Requires manual intervention
- `neutral`: Completed but marked neutral
- `stale`: No longer relevant

**Script behavior**: Any conclusion other than `success` triggers artifact download.

## Multiple PRs for Same Branch

When multiple PRs exist for a branch (e.g., targeting different base branches), the script selects the most recently updated PR.

**Override behavior:**
```bash
# List all PRs for branch
gh pr list --head my-branch --state all

# Check specific PR
npx tsx .github/skills/gh-pr-status/scripts/check_pr_actions.ts --pr 12345
```

## Artifact Considerations

### Artifact Retention
GitHub Actions artifacts expire based on repository settings (default: 90 days). Expired artifacts cannot be downloaded.

### Artifact Size
Large artifacts may take time to download. The script doesn't show progress bars but will complete the download.

### No Artifacts
Workflows may not upload artifacts for all runs. Common cases:
- Workflow failed before artifact upload step
- Workflow doesn't upload artifacts on failure
- Artifacts expired

**Output**: "No artifacts found or download failed for run X."

## Working with Downloaded Artifacts

### Playwright Test Results
```bash
# View Playwright HTML report
open tmp-gh-pr-status/run-123456/playwright-report/index.html

# Or on Windows
start tmp-gh-pr-status/run-123456/playwright-report/index.html
```

### Test Result XMLs
```bash
# Find all test result files
find tmp-gh-pr-status -name "*.xml" -o -name "*results*.json"

# View with less
less tmp-gh-pr-status/run-123456/test-results/results.xml
```

### Build Logs
Check individual artifact directories for log files, typically named:
- `build.log`
- `output.log`
- `console.log`
- `stderr.txt`

## Script Integration

### Use in CI/CD
```bash
#!/bin/bash
# Wait for PR checks and download failures if any

npx tsx .github/skills/gh-pr-status/scripts/check_pr_actions.ts
EXIT_CODE=$?

if [ $EXIT_CODE -eq 1 ]; then
  echo "Failures detected. Artifacts in tmp-gh-pr-status/"
  # Process artifacts, send notifications, etc.
elif [ $EXIT_CODE -eq 0 ]; then
  echo "All checks passed!"
else
  echo "Error occurred"
fi
```

### Scheduled Checks
```bash
# Check PR periodically until all succeed
while true; do
  npx tsx .github/skills/gh-pr-status/scripts/check_pr_actions.ts
  [ $? -eq 0 ] && break
  echo "Waiting 60s for in-progress runs..."
  sleep 60
done
```

## Customizing the Script

### Add Filtering by Workflow Name
Modify `listRunsForPr()` to filter specific workflows:

```javascript
const runs = listRunsForPr(prNumber, args.repo);
const filtered = runs.filter(run => 
  run.name === "CI" || run.name === "Tests"
);
```

### Change Failure Criteria
To only treat actual failures (not cancellations) as failures:

```javascript
const failed = completed.filter((run) => 
  run.conclusion === "failure" || run.conclusion === "timed_out"
);
```

### Custom Output Organization
Group by workflow name instead of run ID:

```javascript
const runDir = path.join(outputDir, runInfo.name.replace(/[^a-z0-9]/gi, '-'), `run-${runId}`);
```

## Performance Optimization

### Parallel Downloads
The current script downloads artifacts sequentially. For multiple failures, consider parallel downloads:

```javascript
await Promise.all(failed.map(runInfo => 
  downloadArtifactsAsync(runInfo.databaseId, ...)
));
```

### Selective Downloads
To download only specific artifact types:

```bash
# List artifacts first
gh run view <run-id>

# Download specific artifact
gh run download <run-id> -n "test-results"
```

## GitHub API Rate Limiting

The GitHub CLI uses the authenticated API which has higher rate limits (5000 req/hour). If you hit limits:

```bash
# Check rate limit status
gh api rate_limit

# Wait or use different authentication
```

## Cross-Repository Checks

Check PRs in different repositories:

```bash
npx tsx .github/skills/gh-pr-status/scripts/check_pr_actions.ts \
  --repo hpcc-systems/HPCC-Platform \
  --pr 12345
```

Useful for:
- Monorepo setups with separate PR workflows
- Dependent repositories
- Fork-based workflows
