# Tagging new versions

## General

The file tools/git/aliases.sh contains various git aliases which are useful when using git, and may be used by the merge scripts.

The file env.sh.example contains some example environment variable settings.  Copy that locally to env.sh and modify it to match your local setup.

Before running any of the other scripts, process the contents of that file as a source file

```
. env.sh
```

to initialize the common environment variables.

## Pre-requisites

The following tools are required:

* **git**
* **helm**

The following repositories should be checked out in a directory reserved for merging and tagging (default for scripts is ~/git):

```
git clone git@github.com:hpcc-systems/eclide.git
git clone git@github.com:hpcc-systems/hpcc4j.git HPCC-JAPIs
git clone git@github.com:hpcc-systems/LN.git ln
git clone git@github.com:hpcc-systems/HPCC-Platform.git hpcc
git clone git@github.com:hpcc-systems/helm-chart.git
```

The following are required for builds prior to 8.12.x
```
git clone git@github.com:hpcc-systems/nagios-monitoring.git
git clone git@github.com:hpcc-systems/ganglia-monitoring.git
```

The files git-fixversion and git-unupmerge can copied so they are on your default path, and then they will be available as git commands.

## Tagging new versions

### Automated branch maintenance

The root [branch-policy.json](../../branch-policy.json) defines the ordered
upmerge chain and repository enablement. The automation runs only from the
default branch of `risk-hsy/HPCC-Platform`.

Three Actions entry points are available:

* [Branch Upmerge](../../.github/workflows/branch-upmerge.yml): independently
  dispatch an internal upmerge, with `dry-run` (default) or `apply` and a
  repository selection.
* [Branch Synchronization](../../.github/workflows/branch-sync.yml): independently
  dispatch the existing OSS synchronization with the same mode/repository inputs.
* [Daily Branch Maintenance](../../.github/workflows/branch-maintenance.yml):
  runs upmerge then sync at **23:30 UTC**, and can also be dispatched manually
  with separate upmerge/sync modes.

All three entry points share a concurrency group; reusable operation jobs do not
acquire a nested lock. A cleanly aborted upmerge conflict blocks that repository's
later upmerge edges, not independent repositories or the subsequent sync. Failed
operations remain visible in the final run status and Actions output/summary.
Issue/email/chat notifications are deferred.

The policy's `paused` flag prevents apply operations; dry-run remains available.
`upmerge_enabled: false` skips sync-only repositories. A repository with one
applicable branch has no upmerge edges. Multiple applicable branches must contain
the complete configured chain; missing refs fail that repository without writing.

The internal App needs Contents: write and Workflows: write for upmerge apply.
Dry-run requests read-only Contents access. Existing app bypass exceptions allow
ordinary pushes; force pushes remain forbidden. New commits are signed off,
but not cryptographically signed. No pre-push build/test/PR gate is introduced.
The normalizer retains target version.cmake/POM content and patches only the HPCC
chart's `Chart.yaml` version/appVersion and the `helmVersion` emitted by
`hpcc.generateHelmVersion`. Component and Kubernetes API version fields are left
to Git's ordinary three-way merge, preserving source-only component upgrades.
Release-version conflicts in those two metadata files are normalized before
merging, without executing the legacy helper's `eval` parser.

Local CLI equivalents:

```bash
bash tools/tagging/upmerge-policy-branches.sh --mode dry-run --repository platform
bash tools/tagging/sync-oss-branches.sh --mode dry-run --repository platform
```

For authorized local repair/apply, pause scheduled writers and wait for any
active run to finish. `BRANCH_APP_TOKEN_INTERNAL` supplies internal authentication;
sync additionally needs `BRANCH_APP_TOKEN_OSS`. Do not put tokens in command
arguments or repository URLs. Actions inherits the existing
`BRANCH_APP_ID_INTERNAL`, `BRANCH_APP_PRIVATE_KEY_INTERNAL`, `BRANCH_APP_ID_OSS`,
and `BRANCH_APP_PRIVATE_KEY_OSS` secrets. Fresh sync tokens are minted after
upmerge so a long upmerge does not consume their lifetime.

Run local fixture tests without accessing GitHub:

```bash
bash tools/tagging/tests/test-upmerge-policy-branches.sh
bash tools/tagging/tests/test-sync-oss-branches.sh
```

### Manual tagging process

The following process should be followed when tagging a new set of versions.

1. Upmerge all changes between candidate branches for the different versions

You can set the `all` environment variable to a subset of the projects (e.g. `export all=hpcc`) if there are no changes in the other repositories.  The only effect for projects that are upmerged with no changes will be that they gain an empty merge transaction.  If multiple people are merging PRs to different repositories it may be safer to upmerge all projects.

For example:
```
./upmerge A.a.x candidate-A.b.x
./upmerge A.b.x candidate-A.c.x
./upmerge A.b.x candidate-B.0.x
./upmerge B.0.x master
```

2. Create new point-release candidate branches:

```
./gorc.sh A.a.x
./gorc.sh A.b.x
./gorc.sh A.c.x
```

## Taking a build gold:

Go gold with each of the explicit versions

```
./gogold.sh 7.8.76
./gogold.sh 7.10.50
```

If you have merged changes onto a point-release branch you would normally create a new rc before going gold.  If the change was trivial (e.g. removing an unwanted file) then you can use the --ignore option to skip that step.

## Creating a new rc for an existing point release:

This normally happens after cherry-picking a late fix for a particular version, which has already been merged into the .x candidate branch.

```
./gorc.sh A.a.<n>
```

## Create a new minor/major branch

A new minor branch is created from the current master...

```
./gominor.sh
```
