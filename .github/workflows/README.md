# GitHub Workflows

This directory contains GitHub Actions workflows for the HPCC Platform project.

## Security Workflows

### helm-security-scan.yml

**Purpose:** Scans Helm charts for security vulnerabilities and misconfigurations.

**Triggers:**
- Pull requests that modify `Chart.yaml` or `requirements.yaml` files in the `helm/` directory
- Pull requests targeting `master` or `candidate-*` branches

**What it scans:**
- Kubernetes misconfigurations in Helm templates
- Security best practices violations
- Known CVEs in 3rd party Helm chart dependencies

**Tool used:** [Trivy](https://github.com/aquasecurity/trivy) by Aqua Security (open-source)

**Output:**
- SARIF format results uploaded to GitHub Security tab
- Table format results in workflow logs
- Workflow summary with scanned charts

**Related issue:** HPCC-35847

**Charts with 3rd party dependencies:**
- `helm/managed/logging/loki-stack/` - Grafana Loki stack for HPCC logs (7 dependencies)
- `helm/managed/metrics/prometheus/` - Prometheus for HPCC metrics (1 dependency)
- `helm/managed/observability/eck/` - Elastic Cloud on Kubernetes (2 dependencies)
- `helm/managed/logging/elastic/` - Elastic Stack for HPCC logs (3 dependencies)

### codeql-eclwatch.yml

**Purpose:** CodeQL security scanning for ECL Watch JavaScript/TypeScript code and GitHub Actions.

**Triggers:**
- Push to `master` or `candidate-*` branches
- Pull requests
- Weekly schedule (Mondays at 7:00 AM)

## Build Workflows

### build-docker-*.yml

**Purpose:** Build Docker images for different HPCC Platform variants.

### build-eclwatch.yml

**Purpose:** Build the ECL Watch web interface.

### build-clienttools-*.yml

**Purpose:** Build client tools for various platforms (macOS, Windows).

## Test Workflows

### test-helm.yml

**Purpose:** Run Helm chart linting and validation tests.

**Triggers:**
- Pull requests that modify Helm charts

**What it does:**
- Validates Helm templates using `kube-score`
- Checks for changes in Helm template output compared to base branch

### test-unit-gh_runner.yml

**Purpose:** Run unit tests for the HPCC Platform.

### test-smoke-gh_runner.yml

**Purpose:** Run smoke tests for the HPCC Platform.

### test-regression-suite-k8s.yml

**Purpose:** Run regression tests on Kubernetes.

## Other Workflows

### jirabot.yml / jirabot-merge.yml

**Purpose:** Automated Jira integration for issue tracking.

### pr-title-check.yml

**Purpose:** Validate pull request titles follow the required format.

### cleanup-artifacts.yml

**Purpose:** Clean up old workflow artifacts to save storage.

---

For more information about specific workflows, see the individual workflow files.
