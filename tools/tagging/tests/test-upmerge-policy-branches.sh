#!/usr/bin/env bash
set -euo pipefail

TEST_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SCRIPT="$TEST_DIR/../upmerge-policy-branches.sh"
TEMP_DIR=$(mktemp -d "${TMPDIR:-/tmp}/upmerge-test.XXXXXX")
trap 'rm -rf "$TEMP_DIR"' EXIT
export GIT_CONFIG_NOSYSTEM=1 GIT_CONFIG_GLOBAL=/dev/null
export GIT_AUTHOR_NAME="Upmerge Test" GIT_AUTHOR_EMAIL="upmerge@example.invalid"
export GIT_COMMITTER_NAME="$GIT_AUTHOR_NAME" GIT_COMMITTER_EMAIL="$GIT_AUTHOR_EMAIL"
export UPMERGE_ALLOW_LOCAL_REMOTES=1
mkdir "$TEMP_DIR/tmp"
export TMPDIR="$TEMP_DIR/tmp"

fail() { echo "FAIL: $*" >&2; exit 1; }
expect_failure() {
    if "$@" > "$TEMP_DIR/failure.log" 2>&1; then
        fail "Expected failure: $*"
    fi
}

make_repo() {
    local name=$1 conflict=${2:-false}
    local work="$TEMP_DIR/$name-work" remote="$TEMP_DIR/$name.git"
    git init -q --bare --initial-branch=master "$remote"
    git init -q --initial-branch=base "$work"
    echo base > "$work/common.txt"
    mkdir -p "$work/helm/hpcc/templates" "$work/module"
    cat > "$work/version.cmake" <<'EOF'
set ( HPCC_MAJOR 10 )
set ( HPCC_MINOR 0 )
set ( HPCC_POINT 0 )
set ( HPCC_MATURITY "rc" )
set ( HPCC_SEQUENCE 1 )
EOF
    printf 'version: 10.0.0-rc1\nappVersion: 10.0.0-rc1\n' > "$work/helm/hpcc/Chart.yaml"
    printf '{{- define "hpcc.generateHelmVersion" -}}\nhelmVersion: 10.0.0-rc1\n{{- end -}}\n' > "$work/helm/hpcc/templates/_helpers.tpl"
    printf 'setting: base\n' > "$work/helm/hpcc/values.yaml"
    printf 'plugin:\n    version: 20.0.0-rc1\n' >> "$work/helm/hpcc/values.yaml"
    echo '<project>base</project>' > "$work/module/pom.xml"
    git -C "$work" add .
    git -C "$work" commit -qm base
    git -C "$work" remote add origin "$remote"
    local branch minor
    for branch in candidate-10.2.x candidate-10.4.x master; do
        case "$branch" in candidate-10.2.x) minor=2;; candidate-10.4.x) minor=4;; master) minor=6;; esac
        git -C "$work" checkout -qb "$branch" base
        sed -i "s/HPCC_MINOR 0/HPCC_MINOR $minor/" "$work/version.cmake"
        sed -i "s/10.0.0/10.$minor.0/g" "$work/helm/hpcc/Chart.yaml" "$work/helm/hpcc/templates/_helpers.tpl"
        echo "<project>$branch</project>" > "$work/module/pom.xml"
        echo "$branch" > "$work/$branch.txt"
        if [[ "$conflict" == true ]]; then echo "$branch" > "$work/common.txt"; fi
        if [[ "$branch" == candidate-10.2.x ]]; then
            sed -i 's/setting: base/setting: propagated/' "$work/helm/hpcc/values.yaml"
        fi
        git -C "$work" add .
        git -C "$work" commit -qm "$branch"
        git -C "$work" push -q origin "$branch"
    done
}

policy() {
    jq -n --arg source "$TEMP_DIR/$1.git" --argjson paused "${2:-false}" '
      {schema_version:1, paused:$paused,
       upmerge_order:["candidate-10.2.x","candidate-10.4.x","master"],
       repositories:[{id:"platform",source:$source,upmerge_enabled:true}]}
    ' > "$TEMP_DIR/policy.json"
}

make_identical_tree_repo() {
    local name=$1
    local work="$TEMP_DIR/$name-work" remote="$TEMP_DIR/$name.git" base_sha
    git init -q --bare --initial-branch=master "$remote"
    git init -q --initial-branch=base "$work"
    echo base > "$work/common.txt"
    git -C "$work" add common.txt
    git -C "$work" commit -qm base
    base_sha=$(git -C "$work" rev-parse HEAD)

    git -C "$work" checkout -qb candidate-10.2.x "$base_sha"
    echo shared > "$work/common.txt"
    git -C "$work" commit -qam 'Source change'
    SOURCE_CHANGE_SHA=$(git -C "$work" rev-parse HEAD)

    git -C "$work" checkout -qb candidate-10.4.x "$base_sha"
    echo shared > "$work/common.txt"
    git -C "$work" commit -qam 'Independent identical change'
    git -C "$work" checkout -qb master "$base_sha"
    git -C "$work" remote add origin "$remote"
    git -C "$work" push -q origin candidate-10.2.x candidate-10.4.x master
    IDENTICAL_WORK=$work
}

snapshot() { git --git-dir="$TEMP_DIR/$1.git" show-ref; }
run() { bash "$SCRIPT" --policy "$TEMP_DIR/policy.json" "$@"; }

make_repo normal
policy normal
BEFORE=$(snapshot normal)
run --mode dry-run > "$TEMP_DIR/dry-run.log" 2>&1 || { cat "$TEMP_DIR/dry-run.log"; fail dry-run; }
[[ "$(snapshot normal)" == "$BEFORE" ]] || fail "Dry-run changed remote refs"
[[ "$(grep -c '^dry-run' "$TEMP_DIR/dry-run.log")" == 2 ]] || fail "Dry-run did not exercise both edges"
run --mode apply > "$TEMP_DIR/apply.log" 2>&1 || { cat "$TEMP_DIR/apply.log"; fail apply; }
REMOTE="$TEMP_DIR/normal.git"
git --git-dir="$REMOTE" merge-base --is-ancestor candidate-10.2.x master || fail "Oldest change did not reach master"
git --git-dir="$REMOTE" cat-file -e master:candidate-10.2.x.txt
git --git-dir="$REMOTE" show master:version.cmake | grep -q 'HPCC_MINOR 6' || fail "Master version changed"
git --git-dir="$REMOTE" show master:module/pom.xml | grep -q '<project>master' || fail "POM changed"
git --git-dir="$REMOTE" show master:helm/hpcc/Chart.yaml | grep -q 'version: 10.6.0-rc1' || fail "Chart version changed"
git --git-dir="$REMOTE" show master:helm/hpcc/templates/_helpers.tpl | grep -q 'helmVersion: 10.6.0-rc1' || fail "Generated Helm version changed"
git --git-dir="$REMOTE" show master:helm/hpcc/values.yaml | grep -q 'setting: propagated' || fail "Non-version Helm change lost"
git --git-dir="$REMOTE" show master:helm/hpcc/values.yaml | grep -q 'version: 20.0.0-rc1' || fail "Unrelated target version changed"
AFTER=$(snapshot normal)
run --mode apply > "$TEMP_DIR/no-op.log" 2>&1
[[ "$(snapshot normal)" == "$AFTER" ]] || fail "Rerun created extra commits"
[[ "$(grep -c '^no-op' "$TEMP_DIR/no-op.log")" == 2 ]] || fail "Completed edges not skipped"

make_repo source-plugin
SOURCE_PLUGIN_WORK="$TEMP_DIR/source-plugin-work"
git -C "$SOURCE_PLUGIN_WORK" checkout -q candidate-10.2.x
sed -i 's/20.0.0-rc1/20.1.0-rc1/' "$SOURCE_PLUGIN_WORK/helm/hpcc/values.yaml"
git -C "$SOURCE_PLUGIN_WORK" add .
git -C "$SOURCE_PLUGIN_WORK" commit -qm 'Upgrade plugin version'
git -C "$SOURCE_PLUGIN_WORK" push -q origin candidate-10.2.x
git -C "$SOURCE_PLUGIN_WORK" checkout -q candidate-10.4.x
policy source-plugin
run --mode apply > "$TEMP_DIR/source-plugin.log" 2>&1 ||
    { cat "$TEMP_DIR/source-plugin.log"; fail source-only-plugin-version; }
git --git-dir="$TEMP_DIR/source-plugin.git" show candidate-10.4.x:helm/hpcc/values.yaml |
    grep -q '    version: 20.1.0-rc1' || fail "Source-only plugin upgrade was lost"
git --git-dir="$TEMP_DIR/source-plugin.git" show candidate-10.4.x:helm/hpcc/Chart.yaml |
    grep -q 'version: 10.4.0-rc1' || fail "Target HPCC chart version changed"

make_identical_tree_repo identical
policy identical
[[ "$(git -C "$IDENTICAL_WORK" rev-parse candidate-10.2.x^{tree})" == "$(git -C "$IDENTICAL_WORK" rev-parse candidate-10.4.x^{tree})" ]] || fail "Identical-tree fixture setup failed"
run --mode apply > "$TEMP_DIR/identical.log" 2>&1 || { cat "$TEMP_DIR/identical.log"; fail identical-tree-upmerge; }
git --git-dir="$TEMP_DIR/identical.git" merge-base --is-ancestor "$SOURCE_CHANGE_SHA" candidate-10.4.x || fail "Identical-tree merge ancestry was not recorded"
git -C "$IDENTICAL_WORK" checkout -q candidate-10.2.x
git -C "$IDENTICAL_WORK" revert --no-edit "$SOURCE_CHANGE_SHA" >/dev/null
git -C "$IDENTICAL_WORK" push -q origin candidate-10.2.x
run --mode apply > "$TEMP_DIR/revert.log" 2>&1 || { cat "$TEMP_DIR/revert.log"; fail source-revert-upmerge; }
[[ "$(git --git-dir="$TEMP_DIR/identical.git" show candidate-10.4.x:common.txt)" == base ]] || fail "Source revert was lost at candidate target"
[[ "$(git --git-dir="$TEMP_DIR/identical.git" show master:common.txt)" == base ]] || fail "Source revert was lost at master"

make_repo conflict true
make_repo healthy
jq --arg conflict "$TEMP_DIR/conflict.git" --arg healthy "$TEMP_DIR/healthy.git" '
 .repositories=[{id:"conflict",source:$conflict,upmerge_enabled:true},
                {id:"healthy",source:$healthy,upmerge_enabled:true}]
' "$TEMP_DIR/policy.json" > "$TEMP_DIR/multi.json"
CONFLICT_BEFORE=$(snapshot conflict)
expect_failure bash "$SCRIPT" --policy "$TEMP_DIR/multi.json" --mode apply
[[ "$(snapshot conflict)" == "$CONFLICT_BEFORE" ]] || fail "Conflict changed remote"
grep -q '^blocked.*conflict' "$TEMP_DIR/failure.log" || fail "Dependent edge not reported blocked"
git --git-dir="$TEMP_DIR/healthy.git" merge-base --is-ancestor candidate-10.2.x master || fail "Independent repository did not continue"
[[ -z "$(find "$TMPDIR" -mindepth 1 -print -quit)" ]] || fail "Owned workspace not cleaned"

make_repo helm-conflict
for branch in candidate-10.2.x candidate-10.4.x; do
    git -C "$TEMP_DIR/helm-conflict-work" checkout -q "$branch"
    sed -i "s/setting: .*/setting: $branch/" "$TEMP_DIR/helm-conflict-work/helm/hpcc/values.yaml"
    git -C "$TEMP_DIR/helm-conflict-work" add .
    git -C "$TEMP_DIR/helm-conflict-work" commit -qm "Conflicting Helm content"
    git -C "$TEMP_DIR/helm-conflict-work" push -q origin "$branch"
done
policy helm-conflict
HELM_BEFORE=$(snapshot helm-conflict)
expect_failure run --mode apply
[[ "$(snapshot helm-conflict)" == "$HELM_BEFORE" ]] || fail "Real Helm conflict was auto-resolved"
grep -q 'Unresolved in: helm/hpcc/values.yaml' "$TEMP_DIR/failure.log" || fail "Helm content conflict not reported"

policy conflict true
run --mode apply > "$TEMP_DIR/paused.log" 2>&1
[[ "$(snapshot conflict)" == "$CONFLICT_BEFORE" ]] || fail "Paused apply wrote"
expect_failure run --mode dry-run
grep -q '^conflict' "$TEMP_DIR/failure.log" || fail "Paused dry-run did not exercise merges"
policy normal
jq '.upmerge_order += ["master"]' "$TEMP_DIR/policy.json" > "$TEMP_DIR/invalid.json"
expect_failure bash "$SCRIPT" --policy "$TEMP_DIR/invalid.json" --mode apply
[[ "$(snapshot normal)" == "$AFTER" ]] || fail "Invalid topology wrote"
expect_failure run --repository unknown
make_repo missing
git --git-dir="$TEMP_DIR/missing.git" update-ref -d refs/heads/candidate-10.4.x
policy missing
MISSING_BEFORE=$(snapshot missing)
expect_failure run --mode apply
[[ "$(snapshot missing)" == "$MISSING_BEFORE" ]] || fail "Incomplete chain performed writes"
grep -q 'Configured upmerge branches are missing' "$TEMP_DIR/failure.log" || fail "Missing branch not reported"

git init -q --bare --initial-branch=master "$TEMP_DIR/single.git"
git --git-dir="$REMOTE" push -q "$TEMP_DIR/single.git" master
policy single
run --mode apply > "$TEMP_DIR/single.log" 2>&1
grep -q '^not-applicable' "$TEMP_DIR/single.log" || fail "Single branch not skipped"
jq '.repositories[0].upmerge_enabled=false' "$TEMP_DIR/policy.json" > "$TEMP_DIR/disabled.json"
bash "$SCRIPT" --policy "$TEMP_DIR/disabled.json" --mode apply > "$TEMP_DIR/disabled.log" 2>&1
grep -q '^disabled' "$TEMP_DIR/disabled.log" || fail "Disabled repository not skipped"

make_repo partial
policy partial
cat > "$TEMP_DIR/partial.git/hooks/pre-receive" <<'EOF'
#!/usr/bin/env bash
while read -r old new ref; do
    [[ "$ref" != refs/heads/master ]] || exit 1
done
EOF
chmod +x "$TEMP_DIR/partial.git/hooks/pre-receive"
OLD_MASTER=$(git --git-dir="$TEMP_DIR/partial.git" rev-parse master)
expect_failure run --mode apply
[[ "$(git --git-dir="$TEMP_DIR/partial.git" rev-parse master)" == "$OLD_MASTER" ]] || fail "Rejected push changed master"
git --git-dir="$TEMP_DIR/partial.git" merge-base --is-ancestor candidate-10.2.x candidate-10.4.x || fail "Successful prior push lost"
rm "$TEMP_DIR/partial.git/hooks/pre-receive"
run --mode apply > "$TEMP_DIR/resume.log" 2>&1
grep -q '^no-op' "$TEMP_DIR/resume.log" || fail "Resume repeated successful edge"
git --git-dir="$TEMP_DIR/partial.git" merge-base --is-ancestor candidate-10.2.x master || fail "Resume failed"

make_repo race
policy race
mkdir "$TEMP_DIR/bin"
REAL_GIT=$(command -v git)
export REAL_GIT RACE_REMOTE="$TEMP_DIR/race.git" RACE_WORK="$TEMP_DIR/race-work" RACE_TRIGGER="$TEMP_DIR/race-trigger"
cat > "$TEMP_DIR/bin/git" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
if [[ "$*" == *"ls-remote --refs origin refs/heads/candidate-10.4.x"* && ! -f "$RACE_TRIGGER" ]]; then
    touch "$RACE_TRIGGER"
    "$REAL_GIT" -C "$RACE_WORK" checkout -q candidate-10.4.x
    echo concurrent > "$RACE_WORK/concurrent.txt"
    "$REAL_GIT" -C "$RACE_WORK" add concurrent.txt
    "$REAL_GIT" -C "$RACE_WORK" commit -qm "Concurrent target update"
    "$REAL_GIT" -C "$RACE_WORK" push -q "$RACE_REMOTE" candidate-10.4.x
fi
exec "$REAL_GIT" "$@"
EOF
chmod +x "$TEMP_DIR/bin/git"
expect_failure env PATH="$TEMP_DIR/bin:$PATH" bash "$SCRIPT" --policy "$TEMP_DIR/policy.json" --mode apply
grep -q 'Target moved' "$TEMP_DIR/failure.log" || fail "Concurrent update not detected"
git --git-dir="$TEMP_DIR/race.git" cat-file -e candidate-10.4.x:concurrent.txt || fail "Concurrent change overwritten"
if git --git-dir="$TEMP_DIR/race.git" merge-base --is-ancestor candidate-10.2.x candidate-10.4.x; then
    fail "Stale merge pushed after target moved"
fi
run --mode apply > "$TEMP_DIR/race-resume.log" 2>&1
git --git-dir="$TEMP_DIR/race.git" cat-file -e master:concurrent.txt || fail "Replan lost concurrent change"

echo "PASS: dry-run, chain propagation, versions/POM/Helm, no-op, real conflicts, isolation, cleanup, pause, invalid/missing refs, single/disabled repos, partial-push recovery and remote races"
