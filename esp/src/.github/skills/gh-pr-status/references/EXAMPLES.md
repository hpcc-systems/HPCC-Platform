# Practical Examples

Real-world usage patterns and workflows for the GitHub Actions PR Checker.

## ECL Watch Development Workflow

### After Making UI Changes
```bash
# Check if Playwright tests pass
npx tsx .github/skills/gh-pr-status/scripts/check_pr_actions.ts

# The script will highlight "Test ECL Watch" workflow status
# If failed, Playwright reports will be downloaded to tmp-gh-pr-status/
```

### View Playwright Test Results
```bash
# After downloading artifacts
# Find the Playwright HTML report
find tmp-gh-pr-status -name "index.html" -path "*/playwright-report/*"

# Open the report (Windows)
start tmp-gh-pr-status/run-*/eclwatch-test-results/playwright-report/index.html

# Open the report (Linux/WSL)
xdg-open tmp-gh-pr-status/run-*/eclwatch-test-results/playwright-report/index.html

# Open the report (macOS)
open tmp-gh-pr-status/run-*/eclwatch-test-results/playwright-report/index.html
```

### Reproduce ECL Watch Test Failure Locally
```bash
# Download failure artifacts
npx tsx .github/skills/gh-pr-status/scripts/check_pr_actions.ts

# Review which tests failed in the Playwright report
# Then run the specific test locally
npm test -- --grep "specific test name"

# Or run all tests
npm run test-ci
```

## Daily Development Workflow

### Before Starting Work
```bash
# Check if your PR has any failing tests
npx tsx .github/skills/gh-pr-status/scripts/check_pr_actions.ts

# If failures found, artifacts are in tmp-gh-pr-status/
# Review test results before making changes
```

### After Pushing Changes
```bash
# Wait a moment for CI to start, then check
sleep 30
npx tsx .github/skills/gh-pr-status/scripts/check_pr_actions.ts

# Monitor until complete
while npx tsx .github/skills/gh-pr-status/scripts/check_pr_actions.ts | grep -q "in progress"; do
  echo "Waiting for CI..."
  sleep 60
done
```

### Before Requesting Review
```bash
# Verify all checks pass
npx tsx .github/skills/gh-pr-status/scripts/check_pr_actions.ts
EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
  echo "✓ All checks passing - ready for review!"
  gh pr ready  # Mark PR as ready for review
else
  echo "✗ Failures detected - check tmp-gh-pr-status/"
fi
```

## CI Debugging Workflows

### Investigate Flaky Test
```bash
# Download artifacts from last 5 runs
for run_id in $(gh run list --branch my-branch --limit 5 --json databaseId -q '.[].databaseId'); do
  npx tsx .github/skills/gh-pr-status/scripts/check_pr_actions.ts --pr <pr-number>
done

# Compare test results across runs
diff tmp-gh-pr-status/run-123456/test-results/results.xml \
     tmp-gh-pr-status/run-789012/test-results/results.xml
```

### Compare with Main Branch
```bash
# Get artifacts from your PR
npx tsx .github/skills/gh-pr-status/scripts/check_pr_actions.ts --output ./pr-artifacts

# Get artifacts from main branch's latest PR
npx tsx .github/skills/gh-pr-status/scripts/check_pr_actions.ts \
  --branch main \
  --output ./main-artifacts

# Compare results
diff -r ./pr-artifacts ./main-artifacts
```

### Reproduce Failure Locally
```bash
# Download failure artifacts
npx tsx .github/skills/gh-pr-status/scripts/check_pr_actions.ts

# Check Playwright traces
ls tmp-gh-pr-status/run-*/playwright-report/

# Open HTML report
open tmp-gh-pr-status/run-*/playwright-report/index.html

# Extract test names that failed
grep -r "failed" tmp-gh-pr-status/run-*/test-results/ | grep -o "test-[^.]*"

# Run specific test locally
npm test -- --grep "specific test name"
```

## Multi-Repository Workflows

### Check Dependencies
If your PR depends on changes in another repository:

```bash
# Check this repo
npx tsx .github/skills/gh-pr-status/scripts/check_pr_actions.ts

# Check dependency repo
npx tsx .github/skills/gh-pr-status/scripts/check_pr_actions.ts \
  --repo hpcc-systems/dependency-repo \
  --pr 456 \
  --output ./dependency-artifacts

# Verify both pass before merging
```

### Monorepo Pattern
```bash
# Check specific subproject workflows
npx tsx .github/skills/gh-pr-status/scripts/check_pr_actions.ts | \
  grep -E "frontend|backend|api"
```

## Team Collaboration

### Share Failure Artifacts
```bash
# Download artifacts
npx tsx .github/skills/gh-pr-status/scripts/check_pr_actions.ts

# Create zip for sharing
zip -r pr-failures.zip tmp-gh-pr-status/

# Upload to shared location or attach to ticket
# Teammate can extract and review without running script
```

### PR Review Helper
```bash
# Reviewer checks PR status
gh pr checkout 123
npx tsx .github/skills/gh-pr-status/scripts/check_pr_actions.ts

# If failures exist, review artifacts
ls -R tmp-gh-pr-status/

# Add review comment with findings
gh pr review --comment -b "Found failures in tmp-gh-pr-status/run-123456"
```

## Automation Scripts

### Pre-Push Hook
`.git/hooks/pre-push`:
```bash
#!/bin/bash

echo "Checking CI status before push..."
npx tsx .github/skills/gh-pr-status/scripts/check_pr_actions.ts

if [ $? -ne 0 ]; then
  echo ""
  echo "Warning: CI has failures. Push anyway? (y/N)"
  read -r response
  if [[ ! "$response" =~ ^[Yy]$ ]]; then
    echo "Push cancelled"
    exit 1
  fi
fi
```

### Scheduled Health Check
```bash
#!/bin/bash
# check-all-prs.sh - Run daily to check all open PRs

for pr in $(gh pr list --state open --json number -q '.[].number'); do
  echo "Checking PR #$pr..."
  npx tsx .github/skills/gh-pr-status/scripts/check_pr_actions.ts \
    --pr "$pr" \
    --output "./reports/pr-$pr"
  
  if [ $? -ne 0 ]; then
    # Send notification
    echo "PR #$pr has failures" | mail -s "CI Failure Alert" team@example.com
  fi
done
```

### Slack/Discord Notification
```bash
#!/bin/bash
# notify-failures.sh

OUTPUT=$(npx tsx .github/skills/gh-pr-status/scripts/check_pr_actions.ts 2>&1)
EXIT_CODE=$?

if [ $EXIT_CODE -ne 0 ]; then
  # Extract run URLs from output
  FAILURES=$(echo "$OUTPUT" | grep -o 'https://github.com/[^ ]*')
  
  # Send to Slack webhook
  curl -X POST -H 'Content-type: application/json' \
    --data "{\"text\":\"CI Failures detected:\n$FAILURES\"}" \
    "$SLACK_WEBHOOK_URL"
fi
```

## Advanced Filtering

### Filter by Workflow Name
Modify the script or post-process output:

```bash
# Download all artifacts
npx tsx .github/skills/gh-pr-status/scripts/check_pr_actions.ts

# Only care about Playwright test failures
find tmp-gh-pr-status -name "playwright-report" -type d | while read dir; do
  echo "Playwright report: $dir"
  open "$dir/index.html"
done
```

### Filter by Time Range
```bash
# Get runs from last 24 hours
gh run list --limit 50 --json databaseId,updatedAt \
  | jq -r '.[] | select(.updatedAt > (now - 86400 | todate)) | .databaseId'
```

### Only Download Specific Artifacts
```bash
# List available artifacts first
gh run view <run-id>

# Download only test results
gh run download <run-id> -n "test-results" -D ./test-results

# Or download only Playwright reports
gh run download <run-id> -n "playwright-report" -D ./playwright-report
```

## Performance Testing

### Benchmark CI Times
```bash
# Get timing data
gh run list --limit 20 --json conclusion,name,updatedAt,createdAt | \
  jq -r '.[] | select(.conclusion == "success") | 
    "\(.name): \(((.updatedAt | fromdateiso8601) - (.createdAt | fromdateiso8601)) / 60) minutes"'

# Average time for successful runs
gh run list --limit 100 --json conclusion,updatedAt,createdAt | \
  jq '[.[] | select(.conclusion == "success") | 
    ((.updatedAt | fromdateiso8601) - (.createdAt | fromdateiso8601))] | 
    add / length / 60'
```

## Integration with Other Tools

### With Playwright
```bash
# After downloading artifacts
npx tsx .github/skills/gh-pr-status/scripts/check_pr_actions.ts

# Merge Playwright reports from multiple runs
npx playwright merge-reports tmp-gh-pr-status/*/playwright-report

# Compare screenshots from failed tests
for report in tmp-gh-pr-status/*/playwright-report; do
  echo "Report: $report"
  ls "$report"/data/*.png
done
```

### With JUnit/XML Parsers
```bash
# Download artifacts
npx tsx .github/skills/gh-pr-status/scripts/check_pr_actions.ts

# Parse JUnit XML results
find tmp-gh-pr-status -name "*.xml" | while read xml; do
  echo "Processing $xml"
  xmllint --xpath "//testcase[@status='failed']/@name" "$xml" 2>/dev/null
done
```

### With Docker
```bash
# Run check in container
docker run --rm -v "$(pwd):/workspace" -w /workspace \
  node:18 \
  npx tsx .github/skills/gh-pr-status/scripts/check_pr_actions.ts
```

## Troubleshooting Workflows

### No Artifacts Downloaded
```bash
# Check if artifacts exist
gh run view <run-id>

# List artifacts manually
gh api "/repos/{owner}/{repo}/actions/runs/<run-id>/artifacts"

# Check artifact retention
gh api "/repos/{owner}/{repo}" | jq '.artifact_retention_days'
```

### Rate Limiting
```bash
# Check rate limit
gh api rate_limit

# If limited, wait or use different auth
export GH_TOKEN="different-token"
npx tsx .github/skills/gh-pr-status/scripts/check_pr_actions.ts
```

### Large Artifact Downloads
```bash
# Download artifacts to tmpfs for speed (Linux)
sudo mount -t tmpfs -o size=2G tmpfs /tmp/fast-artifacts
npx tsx .github/skills/gh-pr-status/scripts/check_pr_actions.ts \
  --output /tmp/fast-artifacts

# Or use ramdisk (macOS)
diskutil erasevolume HFS+ "RAMDisk" `hdiutil attach -nomount ram://4194304`
npx tsx .github/skills/gh-pr-status/scripts/check_pr_actions.ts \
  --output /Volumes/RAMDisk/artifacts
```

## CI/CD Pipeline Integration

### GitHub Actions Workflow
```yaml
name: Download Failure Artifacts

on:
  workflow_run:
    workflows: ["CI"]
    types: [completed]

jobs:
  download-failures:
    runs-on: ubuntu-latest
    if: ${{ github.event.workflow_run.conclusion == 'failure' }}
    steps:
      - uses: actions/checkout@v3
      
      - name: Setup Node
        uses: actions/setup-node@v3
        with:
          node-version: 18
      
      - name: Download artifacts
        run: |
          npx tsx .github/skills/gh-pr-status/scripts/check_pr_actions.ts \
            --pr ${{ github.event.workflow_run.pull_requests[0].number }}
      
      - name: Upload combined artifacts
        uses: actions/upload-artifact@v3
        with:
          name: failure-analysis
          path: tmp-gh-pr-status/
```

### Jenkins Pipeline
```groovy
pipeline {
  agent any
  stages {
    stage('Check PR Status') {
      steps {
        script {
          def exitCode = sh(
            script: 'npx tsx .github/skills/gh-pr-status/scripts/check_pr_actions.ts',
            returnStatus: true
          )
          if (exitCode != 0) {
            currentBuild.result = 'UNSTABLE'
            archiveArtifacts artifacts: 'tmp-gh-pr-status/**/*', allowEmptyArchive: true
          }
        }
      }
    }
  }
}
```
