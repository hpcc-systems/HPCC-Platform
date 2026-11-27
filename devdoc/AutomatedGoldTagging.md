# Automated gold tagging – design

Status: implemented. The scripts are described in [tools/tagging/README.md](../tools/tagging/README.md).

## 0. Decisions

| Topic | Decision |
|---|---|
| Approval | None. Tag automatically when a change is detected. |
| RC step | Skipped. A gorc-style split is tagged gold (`A.B.N-1`) directly. |
| Manual fallback | `gorc.sh` and `gogold.sh` stay. |
| Notifications | Existing build notifications on success. Tagging failures go to `CICD_WEBHOOK_URL`. |
| Verifying tagged builds | Out of scope. |
| hpcc4j | Is tagged, but a change in hpcc4j alone does not trigger a release. |
| Upmerge failure | Automatic tagging is skipped for the whole run. |
| Repository missing a tagged branch | Abort: internal error. |
| Cadence | Nightly, whenever something changed. |
| Outstanding RC branches (e.g. 10.4.62-rc1) | Resolved manually. Ignored by automation. |
| Order | Manifest `repositories` order. Branches in `tagging.branches` order. No submodules. |
| Tag signing | Unsigned annotated tags are fine. |
| Failure | Prepare everything locally, then push. On the first push failure, stop all further pushes, notify, and wait for a rerun. The rerun must resume correctly. |

## 1. How the current process works

The repositories are `eclide`, `HPCC-JAPIs` (hpcc4j), `ln` and `hpcc`, set in `$all` in `env.sh.example`. `helm-chart` is updated as a side effect of tagging `hpcc`.

### `gorc.sh A.B.x`

This wrapper runs `cmake_modules/go_rc.sh` in each repository on `candidate-A.B.x`. It waits for a key press.

`go_rc.sh` on a `closedown` branch with odd point P:

1. Creates `candidate-A.B.(P+1)` from the `.x` HEAD. This is the split.
2. On `.x`, bumps the version files to `closedown`, point P+2: `version.cmake`, `pom.xml`, `Chart.yaml`, and the `helmVersion` in `_helpers.tpl`. It commits this as `Split off A.B.(P+1)` and pushes.
3. On the new branch, sets `rc`, point P+1, sequence 1. It commits, pushes, and creates a tag for each entry in `HPCC_PROJECT`, e.g. `community_A.B.(P+1)-rc1`.
4. In the platform repository only, it packages the hpcc chart into `../helm-chart/docs`, re-indexes, and pushes `helm-chart` master.

### `gogold.sh A.B.P`

`go_gold.sh` requires `rc` maturity, an even point, and HEAD equal to the rc tag. It sets `release`, sequence 1, commits, and tags `<project>_A.B.P-1`. It publishes the hpcc chart plus the `examples/*` and `managed/*` charts.

### Other relevant pieces

- `update_version_file` also writes `HPCC_TAG_TIMESTAMP`. Preparing the same release twice therefore produces different commit SHAs, which matters for resuming (section 6).
- hpcc4j uses `parse_hpcc_pom.sh`, which reads `<version>` in the form `A.B.P-SEQ-maturity` from `pom.xml`. The same `go_*.sh` logic applies.
- `branch-operations.yml` is a reusable workflow that runs upmerge, then sync, under a shared concurrency group. When the job fails, its `notify-failure` job already posts to `CICD_WEBHOOK_URL`.
- `sync-oss-branches.sh` already syncs new `candidate-*` branches and any tag at a branch tip, in manifest order. **Sync needs no change.**
- `build-assets.yml` is triggered by the platform `community_*` tag. It checks out LN and ECLIDE at refs derived from that tag (`eclide_…`, `internal_…`). So the **platform tag must be pushed last**, after ln and eclide. The current manifest order (eclide, hpcc4j, helm-chart, ln, platform) already does this.

## 2. Target behaviour

For each branch in `tagging.branches`, in order (e.g. 10.2.x → 10.8.x):

1. **Check the release state** of every tagged repository (section 6). If a release is part-done, finish it before doing anything else.
2. **Detect changes** in each trigger repository since its last split (section 3).
3. **Decide.** If any trigger repository changed, or the run was forced, release the next point in **every** tagged repository. For example, an `eclide` fix on 10.4.x releases 10.4.(P+1) in eclide, hpcc4j, ln and platform.
4. **Prepare** all repositories locally. Nothing is pushed in this step.
5. **Push** (section 5).

Upmerge runs first, so a 10.2.x fix also releases 10.4, 10.6 and 10.8. `master` is never tagged automatically.

## 3. Change detection

```
P     = point on origin/candidate-A.B.x                     (must be odd, closedown)
last  = origin/candidate-A.B.(P-1)                           (previous split; must exist)
base  = git merge-base origin/candidate-A.B.x last
split = first first-parent commit in base..origin/candidate-A.B.x
        (must be "Split off A.B.(P-1)")
changed = ! git diff --quiet split origin/candidate-A.B.x
```

This compares file contents (trees) rather than counting commits. Upmerges that bring in nothing are common and leave the tree unchanged. This was verified on 10.4.x, where the split was followed by such an upmerge.

If `last` is missing or the commit after the base is not a split commit, abort with an internal error. A forced dispatch doesn't override this, because the version can't be trusted.

## 4. Manifest changes (`branch-policy.json`)

Add these per-repository flags:

| id (manifest order) | `tag_enabled` | `tag_triggers` |
|---|---|---|
| eclide | true | true |
| hpcc4j | true | false |
| helm-chart | false (published with platform) | false |
| ln | true | true |
| platform | true | true |

Also add a top-level section:

```json
"tagging": {
  "branches": ["candidate-10.2.x", "candidate-10.4.x", "candidate-10.6.x", "candidate-10.8.x"],
  "helm_chart_repository": "helm-chart"
}
```

`helm_chart_repository` names the repository entry that receives the packaged charts.

- `paused: true` blocks tagging in `apply` mode.
- `schema_version` stays 1, because the new fields are optional for the existing validators.
- Validation:
  - Every `tag_enabled` repository must have every branch in `tagging.branches`.
  - The platform must be the last `tag_enabled` repository, because the build trigger depends on it (section 1).

## 5. Prepare, then push

### Prepare (local only, all branches, all repositories)

For each branch, then for each tagged repository in manifest order, run `go_rc.sh --gold --no-push`. The `--no-push` option records the refs to push instead of pushing them:

- `.x` gets the commit `Split off A.B.(P+1)`, bumping it to P+2 `closedown`.
- `candidate-A.B.(P+1)` gets the release commit (`release`, sequence 1).
- Tags are created for each `HPCC_PROJECT` entry, e.g. `community_A.B.(P+1)-1`.

For the platform, the charts are packaged into the local `helm-chart` clone in the same step. All branches stack their commits on one local `helm-chart` master.

Then check the plan **before any push**:

- Every new tag points at the tip of its new branch.
- The new branch's parent is the `.x` commit that was fetched.
- The `.x` parent is the fetched tip.
- All repositories produced the same version.
- `git push --dry-run --atomic` succeeds for each repository.

### Push (strict order, stop on the first failure)

```
for branch in tagging.branches:
  for repo in manifest order (tag_enabled):
    git push --atomic origin \
      <new .x sha>:refs/heads/candidate-A.B.x \
      <release sha>:refs/heads/candidate-A.B.(P+1) \
      refs/tags/<each tag>
  (after all branches) git push origin helm-chart master
```

- `--atomic` means each repository is either fully released or untouched. The `.x` update is a fast-forward check against the fetched tip. If someone merged a PR in the meantime, the push is rejected rather than losing that work. Creating the branch or tag fails if it already exists.
- On the first failure:
  - Stop all further pushes, including later repositories, later branches and `helm-chart`.
  - Write the state (which repositories are released and which are pending) to the step summary.
  - Fail the job, so the existing `notify-failure` job posts to `CICD_WEBHOOK_URL`. Extend that message with the branch, repository and git error.
- If tagging fails, **skip sync for this run.** Otherwise OSS gets a part-done release and the build-trigger order is broken. The nightly run, or a rerun, syncs after it completes.

## 6. Resuming after a failure: per-repository release state

With per-repository atomic pushes, each tagged repository on a branch is in one of these states:

| State | `.x` | `candidate-A.B.(P+1)` | Tags |
|---|---|---|---|
| **pending** | point P, `closedown`, no split commit for P+1 | absent | absent |
| **released** | point P+2; first-parent history contains `Split off A.B.(P+1)` | exists; at its tip `release` P+1 sequence 1 | every project tag `…A.B.(P+1)-1` at the tip of the new branch |
| anything else | | | → **inconsistent**: internal error, abort, manual repair |

For each branch, take the state of all tagged repositories:

- **All pending:** normal detection (section 3).
- **All released:** nothing to resume. Run normal detection from the new baseline.
- **Mixed:** this is a part-done release. Its target is P+1, where P is the point of the pending repositories. All of these must hold, or the run aborts:
  - Every released repository is at exactly P+1, with `.x` at P+2.
  - Every pending repository is at P.
  - The released repositories form a **prefix** of the manifest order. A gap means something other than this process pushed.

  Then prepare and push only the pending repositories. **Change detection is not run**: they must be completed even if nothing in them changed.
- **helm-chart:** for every platform repository in the released state, check that `hpcc-A.B.(P+1).tgz` is in `helm-chart/docs`. If not, republish it from the `candidate-A.B.(P+1)` branch with `cmake_modules/publish_helm.sh`. This covers a failure after the platform push.

A rerun is the GitHub "Re-run failed jobs" button, or simply the next nightly run. Both work, because the state comes only from the remote repositories, not from the earlier run.

## 7. Potential problems, especially versions

1. **New SHAs on every prepare.** `HPCC_TAG_TIMESTAMP` and commit times change, so pending repositories are rebuilt from scratch on a rerun. This is fine because nothing of theirs was pushed. **Never** re-prepare a released repository: existing tags must not be moved. Sync also refuses to overwrite tags that differ.

2. **Commits landing between the failure and the rerun.** These are included in the resumed release of the pending repositories, but not in the repositories already released. Each repository's release is still a consistent snapshot of that repository. This is acceptable because the repositories are released independently today too, but the release is not a snapshot from one moment in time.

3. **Nightly upmerge running against a part-done release.** In a released repository, `.x` is now at P+2 and gets upmerged into higher branches. The normalizer keeps the target's version, so this is safe. Upmerge never touches point branches.

4. **A higher branch released after a lower branch failed.** The push order prevents this within one run. Across runs, the lower branch is resumed first because branches are processed in order. Still, nightly upmerge can carry the lower branch's changes upward first. Agreed policy: the run stops at the first failure.

5. **Repositories disagreeing on the version.** This happens if someone ran `gorc` or `gogold` for only some repositories, or after a manual repair. It shows up as inconsistent or as a gap in the prefix, and the run aborts. It is never guessed. The manual fallback `gorc.sh --gold` should therefore use the same prepare-then-atomic-push path, so it can't leave states the automation can't classify. Document: **set `paused` before running anything manually.**

6. **Point parity and old RC branches.** `.x` must be odd and `closedown`. Old RC branches (e.g. 10.4.62 at rc1) are ignored. Detection uses `candidate-A.B.(P-1)`, whether it is at rc or release, as the baseline, so an un-golded 10.4.62 doesn't block 10.4.64.
   - However, if someone manually golds 10.4.62 later, `gogold.sh` pushes a commit to `candidate-10.4.62`. The merge base with `.x` is unchanged, so detection is unaffected.
   - If an RC point branch is created manually with `gorc.sh` (not `--gold`) while automation is active, the next automatic release still works. It just skips one point.

7. **The manifest doesn't match the repositories.** Examples: a new `.x` branch is added to `tagging.branches` before its first split exists, or a repository lacks the branch. Both abort with an internal error, as agreed. After `gominor.sh`, add the new branch only after its first split.

8. **Several tags per repository.** `HPCC_PROJECT` may list more than one project, and `do_tag` loops over them. The state check and the atomic push must include **all** of them, otherwise a repository could be classified as released while one tag is missing.

9. **Partial `helm-chart` failure.** `helm package --dependency-update` for the examples needs external chart repositories. If that fails **during prepare**, nothing is pushed. A `helm-chart` **push** failure happens after the platform tag is pushed: the release is out but the chart isn't. The resume check in section 6 republishes it. `helm repo index` rewrites `index.yaml`, so a concurrent push to `helm-chart` master causes a non-fast-forward failure, which is fixed by rerunning.

10. **OSS sync order.** Sync also pushes in manifest order, so the platform tag reaches OSS after ln and eclide. If sync fails partway, the platform tag may be pushed without an ln tag, or not at all. This problem exists today and is not made worse.

11. **Concurrency.** The tagging step runs inside the shared `branch-operations` concurrency group, after upmerge, and fetches fresh refs after upmerge rather than reusing upmerge's temporary clones. A human pushing directly is caught by the atomic fast-forward check.

12. **`eval` in `doit`.** Only validated policy and input values (version numbers, `candidate-` names) may reach `go_*.sh`.

13. **Rerun with old code.** "Re-run failed jobs" reuses the workflow commit from the original run. If the failure was a script bug, the fix needs a new run, i.e. the next nightly run or a new dispatch, not a rerun.

## 8. Changes required

### `cmake_modules/parse_cmake.sh`

- Add `--no-push`. `doit` push commands record the refs instead of running.
- Add a shared helm-publish function that publishes the hpcc, examples and managed charts.
- New `cmake_modules/publish_helm.sh` republishes the charts for the current checkout.

### `cmake_modules/go_rc.sh`

- Add `--gold`: release, sequence 1, the gold commit message, and gold helm publishing.
- Without `--gold`, behaviour is unchanged.

### `cmake_modules/go_gold.sh`

- Use the shared helm-publish function.

### `tools/tagging/gorc.sh`

- Pass `--gold` through.
- Under `--gold`, use prepare-then-atomic-push for each repository.

### New `tools/tagging/release-policy-branches.sh`

- Options: `--policy`, `--mode dry-run|apply`, `--branch auto|candidate-A.B.x`, `--force`.
- Steps:
  1. Validate the policy.
  2. Clone the tagged repositories and `helm-chart` side by side, without submodules.
  3. Classify the state (section 6).
  4. Detect changes (section 3).
  5. Prepare (section 5).
  6. Check the plan.
  7. Push.
- App-token authentication through `GIT_CONFIG_*` environment variables, which child git commands inherit. Set the bot identity with `user.name` and `user.email`.
- Exit codes:
  - 0: nothing to do, or done.
  - 1: a push failed. This should trigger the webhook.
  - 2: internal error or inconsistent state.
- `dry-run` runs everything except the push and reports the plan.

### New tests: `tools/tagging/tests/test-release-policy-branches.sh`

Use local bare repositories. Cover:

- no change
- an empty upmerge only
- a real change
- a change only in a non-trigger repository
- a change in one trigger repository → all repositories released
- forced release
- missing branch → abort
- version disagreement → abort
- the push for the 2nd repository fails (simulated with a pre-receive hook): no later pushes, rerun resumes, and no change detection is needed
- `helm-chart` push failure → resumed on rerun
- gap in the released prefix → abort
- a PR merged to `.x` between prepare and push → atomic rejection

### `.github/workflows/branch-operations.yml`

- New inputs: `run_tag`, `tag_mode`, `tag_branch`, `tag_force`.
- Tagging step after upmerge: install helm, mint a write token (contents and workflows), run the script.
- Tagging only runs when the operation covers all repositories (`repository == all`).
- Automatic tagging is skipped if upmerge failed. `tag_force` doesn't override this.
- Sync is skipped if tagging failed.
- Extend the `notify-failure` message with the tagging summary.

### `.github/workflows/branch-maintenance.yml`

- Nightly: upmerge → tag (`auto`, apply) → sync.

### New `.github/workflows/branch-release.yml`

- Dispatch only. Inputs:
  - `branch` (choice: `auto` or one of the `.x` branches)
  - `mode`
  - `force`: release even if nothing changed. It is still subject to the state and consistency checks.
  - `upmerge` and `sync`: whether to run those steps around the tagging (both default to on).
- Runs upmerge → tag → sync through the reusable workflow.

### Docs

- `tools/tagging/README.md`: describe the automatic path, the manual dispatch, recovery, and "pause before manual tagging".

### Repository settings

- The App needs a ruleset bypass to create `candidate-*` branches and tags in all four tagged repositories.
