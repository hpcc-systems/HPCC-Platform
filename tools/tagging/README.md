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

Four Actions entry points are available:

* [Branch Upmerge](../../.github/workflows/branch-upmerge.yml): independently
  dispatch an internal upmerge, with `dry-run` (default) or `apply` and a
  repository selection.
* [Branch Synchronization](../../.github/workflows/branch-sync.yml): independently
  dispatch the existing OSS synchronization with the same mode/repository inputs.
* [Daily Branch Maintenance](../../.github/workflows/branch-maintenance.yml):
  runs upmerge, then [automated gold tagging](#automated-gold-releases), then
  sync at **23:30 UTC**, and can also be dispatched manually with separate
  upmerge/tag/sync modes.
* [Branch Release](../../.github/workflows/branch-release.yml): dispatch a gold
  release on demand for one branch (optionally forced) or for every changed branch.

All four entry points share a concurrency group; reusable operation jobs do not
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
bash tools/tagging/tests/test-release-policy-branches.sh
```

### Automated gold releases

[release-policy-branches.sh](release-policy-branches.sh) splits and tags new
gold point releases. It runs after upmerge in the nightly maintenance run, and is
skipped if upmerge failed. For each branch in `tagging.branches` of
[branch-policy.json](../../branch-policy.json) (see the
[design notes](../../devdoc/AutomatedGoldTagging.md) for the rationale and
failure analysis):

* If any repository with `tag_triggers: true` has changed since the
  `Split off A.B.N` commit of the previous release (empty upmerge merges do not
  count), every repository with `tag_enabled: true` gets a new release.
* Each release is a `gorc.sh`-style split: `candidate-A.B.x` moves to the next odd
  closedown point, and the new `candidate-A.B.N` branch is tagged gold directly
  (`<project>_A.B.N-1`, or `hpcc4j_A.B.N-1-release`). The helm charts are
  published to `helm-chart`, as `gogold.sh` does.
* Currently eclide, ln and platform trigger releases. hpcc4j is released with
  them, but a change only in hpcc4j does not trigger a release.

Repositories are processed in policy order. `platform` must be the last tagged
repository, because its tag starts the builds. All branches and repositories are
prepared locally with `go_rc.sh --gold --no-push` and checked before anything is
pushed. Then each repository is pushed with one atomic push of the `.x` branch,
the new branch and its tags, followed by `helm-chart`. The first failed push stops
all later pushes, the job fails and the failure detail is posted to
`CICD_WEBHOOK_URL`. Rerunning the workflow resumes the release:

* If some repositories were released, the rest are released at the same version
  without change detection.
* If the charts were not published, they are republished from the released branch.
* Anything else unexpected (a missing branch, versions that disagree, or a release
  this process did not make) is an internal error. Nothing is pushed until it is
  resolved by hand.

To release a branch on demand, dispatch **Branch Release**, choosing the branch and
`force` if nothing has changed. It upmerges first and synchronizes afterwards by
default. Set `paused` in the policy before tagging by hand, so the automation does
not race with you. Existing rc branches are ignored; the next automated release
uses the next even point.

The internal App also needs permission (a ruleset bypass where needed) to create
`candidate-*` branches and tags in each tagged repository and to push `helm-chart`
master. The runner needs `helm` (installed by the workflow) and `mvn` (used for
hpcc4j).

```bash
bash tools/tagging/release-policy-branches.sh --mode dry-run
bash tools/tagging/release-policy-branches.sh --mode dry-run --branch candidate-10.4.x --force
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

To split and tag a point release gold in one step, as the automation does, use
`--gold`. Every repository is prepared before anything is pushed. Each one is then
pushed atomically in order, followed by helm-chart:

```
./gorc.sh A.b.x --gold
```

Gold mode currently supports only the `origin` remote. It rejects `--remote` or
`-r` overrides rather than risk fetching from one remote and pushing the release
to another.

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
