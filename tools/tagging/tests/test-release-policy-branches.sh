#!/usr/bin/env bash
set -euo pipefail

TEST_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SCRIPT="$TEST_DIR/../release-policy-branches.sh"
CMAKE_MODULES=$(cd "$TEST_DIR/../../../cmake_modules" && pwd)
TEMP_DIR=$(mktemp -d "${TMPDIR:-/tmp}/release-test.XXXXXX")
trap '[[ -n "${KEEP_TEMP:-}" ]] || rm -rf "$TEMP_DIR"' EXIT
export GIT_CONFIG_NOSYSTEM=1 GIT_CONFIG_GLOBAL=/dev/null
export GIT_AUTHOR_NAME="Release Test" GIT_AUTHOR_EMAIL="release@example.invalid"
export GIT_COMMITTER_NAME="$GIT_AUTHOR_NAME" GIT_COMMITTER_EMAIL="$GIT_AUTHOR_EMAIL"
export RELEASE_ALLOW_LOCAL_REMOTES=1 HPCC_TAGGING_NO_MAVEN=1
mkdir "$TEMP_DIR/tmp" "$TEMP_DIR/bin" "$TEMP_DIR/remotes" "$TEMP_DIR/work"
export TMPDIR="$TEMP_DIR/tmp"
REMOTES="$TEMP_DIR/remotes"
WORK="$TEMP_DIR/work"
REPOS=(eclide hpcc4j ln platform)

# Minimal helm: package writes <name>-<version>.tgz, repo index lists the packages.
cat > "$TEMP_DIR/bin/helm" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
case "$1" in
    package)
        chart=${2%/}/Chart.yaml
        echo package > "$(sed -n 's/^name: *//p' "$chart" | head -1)-$(sed -n 's/^version: *//p' "$chart" | head -1).tgz";;
    repo)
        { echo "entries:"; for f in *.tgz; do echo "  - $5/$f"; done; } > index.yaml;;
    *) exit 1;;
esac
EOF
chmod +x "$TEMP_DIR/bin/helm"
export PATH="$TEMP_DIR/bin:$PATH"

fail() { echo "FAIL: $*" >&2; exit 1; }
expect_failure() {
    local expected=$1
    shift
    local status=0
    "$@" > "$TEMP_DIR/failure.log" 2>&1 || status=$?
    [[ "$status" == "$expected" ]] || { cat "$TEMP_DIR/failure.log"; fail "Expected exit $expected, got $status: $*"; }
}
remote() { git --git-dir="$REMOTES/$1.git" "${@:2}"; }
has_ref() { remote "$1" show-ref --verify --quiet "$2"; }
snapshot() { local id; for id in "${REPOS[@]}" helm-chart; do remote "$id" show-ref; done; }
run() { bash "$SCRIPT" --policy "$TEMP_DIR/policy.json" "$@"; }
run_ok() {
    local log=$1
    shift
    run "$@" > "$TEMP_DIR/$log.log" 2>&1 || { cat "$TEMP_DIR/$log.log"; fail "$log"; }
}

cmake_version() {
    local project=$1 minor=$2 point=$3
    if [[ "$project" == ln ]]; then
        cat <<EOF
if ("\${BUILD_LEVEL}" STREQUAL "INTERNAL")
    set ( HPCC_PROJECT "internal")
    set ( HPCC_PROJECT_ABBR  "LN")
elseif ("\${BUILD_LEVEL}" STREQUAL "ENTERPRISE")
    set ( HPCC_PROJECT "enterprise")
    set ( HPCC_PROJECT_ABBR  "EE" )
endif()
set ( HPCC_NAME "Enterprise Edition")
EOF
    else
        printf 'set ( HPCC_NAME "%s" )\nset ( HPCC_PROJECT "%s" )\n' "$project" "$project"
    fi
    cat <<EOF
set ( HPCC_MAJOR 10 )
set ( HPCC_MINOR $minor )
set ( HPCC_POINT $point )
set ( HPCC_MATURITY "closedown" )
set ( HPCC_SEQUENCE 0 )
set ( HPCC_TAG_TIMESTAMP "2026-01-01T00:00:00Z" )
EOF
}

make_repository() {
    local id=$1 work="$WORK/$1" minor point
    git init -q --bare --initial-branch=master "$REMOTES/$id.git"
    git init -q --initial-branch=master "$work"
    git -C "$work" remote add origin "$REMOTES/$id.git"
    echo "$id" > "$work/README"
    git -C "$work" add README
    git -C "$work" commit -qm base
    for minor in 2 4; do
        point=$([[ $minor == 2 ]] && echo 81 || echo 57)
        git -C "$work" checkout -qb "candidate-10.$minor.x" master
        if [[ "$id" == hpcc4j ]]; then
            printf '<project>\n  <artifactId>hpcc4j</artifactId>\n  <version>10.%s.%s-0-SNAPSHOT</version>\n</project>\n' \
                "$minor" "$point" > "$work/pom.xml"
        else
            cmake_version "$id" "$minor" "$point" > "$work/version.cmake"
        fi
        if [[ "$id" == platform ]]; then
            mkdir -p "$work/helm/hpcc/templates" "$work/helm/examples/demo" "$work/helm/managed/logging"
            printf 'name: hpcc\nversion: 10.%s.%s-closedown0\nappVersion: 10.%s.%s-closedown0\n' \
                "$minor" "$point" "$minor" "$point" > "$work/helm/hpcc/Chart.yaml"
            printf 'helmVersion: 10.%s.%s-closedown0\n' "$minor" "$point" > "$work/helm/hpcc/templates/_helpers.tpl"
            printf 'name: demo\nversion: 1.0.%s\n' "$minor" > "$work/helm/examples/demo/Chart.yaml"
            printf 'name: logging\nversion: 2.0.%s\n' "$minor" > "$work/helm/managed/logging/Chart.yaml"
        fi
        git -C "$work" add .
        git -C "$work" commit -qm "candidate-10.$minor.x"
    done
    git -C "$work" push -q origin master candidate-10.2.x candidate-10.4.x
}

for id in "${REPOS[@]}"; do make_repository "$id"; done
git init -q --bare --initial-branch=master "$REMOTES/helm-chart.git"
git init -q --initial-branch=master "$WORK/helm-chart"
mkdir "$WORK/helm-chart/docs"
echo "entries:" > "$WORK/helm-chart/docs/index.yaml"
git -C "$WORK/helm-chart" add .
git -C "$WORK/helm-chart" commit -qm base
git -C "$WORK/helm-chart" remote add origin "$REMOTES/helm-chart.git"
git -C "$WORK/helm-chart" push -q origin master

jq -n --arg r "$REMOTES" --argjson paused "${PAUSED:-false}" '
  {schema_version:1, paused:$paused, control_ref:"refs/heads/master",
   upmerge_order:["candidate-10.2.x","candidate-10.4.x","master"],
   tagging:{branches:["candidate-10.2.x","candidate-10.4.x"], helm_chart_repository:"helm-chart"},
   repositories:[
     {id:"eclide",source:"\($r)/eclide.git",tag_enabled:true,tag_triggers:true},
     {id:"hpcc4j",source:"\($r)/hpcc4j.git",tag_enabled:true,tag_triggers:false},
     {id:"helm-chart",source:"\($r)/helm-chart.git",tag_enabled:false,tag_triggers:false},
     {id:"ln",source:"\($r)/ln.git",tag_enabled:true,tag_triggers:true},
     {id:"platform",source:"\($r)/platform.git",tag_enabled:true,tag_triggers:true}]}
' > "$TEMP_DIR/policy.json"

# Bring a work clone up to date with the remote branch.
refresh() {
    local id=$1 branch=$2
    git -C "$WORK/$id" fetch -q origin
    git -C "$WORK/$id" checkout -q -B "$branch" "origin/$branch"
}
change() {
    local id=$1 branch=$2 name=${3:-change}
    refresh "$id" "$branch"
    echo "$name" >> "$WORK/$id/$name.txt"
    git -C "$WORK/$id" add "$name.txt"
    git -C "$WORK/$id" commit -qm "$name in $id"
    git -C "$WORK/$id" push -q origin "$branch"
}
go_gold() {
    local id=$1 branch=$2
    refresh "$id" "$branch"
    (cd "$WORK/$id" && bash "$CMAKE_MODULES/go_rc.sh" --gold) >> "$TEMP_DIR/setup.log" 2>&1 ||
        { cat "$TEMP_DIR/setup.log"; fail "go_rc.sh --gold for $id $branch"; }
}
check_released() {
    local version=$1 next=$2 id tip
    for id in "${REPOS[@]}"; do
        has_ref "$id" "refs/heads/candidate-$version" || fail "$id: candidate-$version not pushed"
        tip=$(remote "$id" rev-parse "candidate-$version")
        case "$id" in
            hpcc4j) tags=("hpcc4j_$version-1-release")
                    remote "$id" show "$tip:pom.xml" | grep -q "<version>$version-1</version>" || fail "$id: not gold";;
            ln) tags=("internal_$version-1" "enterprise_$version-1");;
            *) tags=("${id}_$version-1");;
        esac
        for tag in "${tags[@]}"; do
            [[ "$(remote "$id" rev-parse "$tag^{commit}" 2>/dev/null)" == "$tip" ]] || fail "$id: tag $tag missing or misplaced"
        done
        if [[ "$id" != hpcc4j ]]; then
            remote "$id" show "$tip:version.cmake" | grep -q 'HPCC_MATURITY "release"' || fail "$id: candidate-$version not release"
            remote "$id" show "candidate-${version%.*}.x:version.cmake" | grep -q "HPCC_POINT $next )" ||
                fail "$id: .x not moved to $next"
        fi
        [[ "$(remote "$id" log -1 --format=%s "candidate-${version%.*}.x")" == "Split off $version" ]] ||
            fail "$id: .x tip is not the split"
    done
    remote helm-chart cat-file -e "master:docs/hpcc-$version.tgz" || fail "Chart $version not published"
    remote helm-chart show master:docs/index.yaml | grep -q "/hpcc-$version.tgz" || fail "Chart $version not indexed"
}

# Previous releases (10.2.82 and 10.4.58), created by the manual tooling with pushes.
for branch in candidate-10.2.x candidate-10.4.x; do
    for id in "${REPOS[@]}"; do go_gold "$id" "$branch"; done
done
check_released 10.2.82 83
check_released 10.4.58 59
remote helm-chart log -1 --format=%s master | grep -qx 'HPCC Helm Charts 10.4.58-1' || fail "Gold chart message"
remote helm-chart cat-file -e master:docs/demo-1.0.4.tgz || fail "Gold did not publish example charts"
remote helm-chart cat-file -e master:docs/logging-2.0.4.tgz || fail "Gold did not publish managed charts"

# No changes, an empty upmerge-style commit, and a non-trigger change do not release.
BEFORE=$(snapshot)
run_ok nothing --mode apply
grep -q 'Nothing to release' "$TEMP_DIR/nothing.log" || fail "No-change run did not report nothing"
[[ "$(grep -c '^unchanged' "$TEMP_DIR/nothing.log")" == 6 ]] || fail "Trigger repositories not all checked"
grep -q '^unchanged.*hpcc4j' "$TEMP_DIR/nothing.log" && fail "Non-trigger repository was checked"
refresh platform candidate-10.4.x
git -C "$WORK/platform" commit -q --allow-empty -m "Upmerge with no changes"
git -C "$WORK/platform" push -q origin candidate-10.4.x
change hpcc4j candidate-10.4.x java-fix
BEFORE=$(snapshot)
run_ok non-trigger --mode apply
[[ "$(snapshot)" == "$BEFORE" ]] || fail "Empty or non-trigger change released"

# A change to one trigger repository releases every tagged repository on that branch only.
change ln candidate-10.4.x ln-fix
BEFORE=$(snapshot)
run_ok dry-run --mode dry-run
[[ "$(snapshot)" == "$BEFORE" ]] || fail "Dry-run changed remotes"
[[ "$(grep -c '^prepared' "$TEMP_DIR/dry-run.log")" == 5 ]] || fail "Dry-run did not prepare all repositories and helm-chart"
jq '.paused = true' "$TEMP_DIR/policy.json" > "$TEMP_DIR/paused.json"
bash "$SCRIPT" --policy "$TEMP_DIR/paused.json" --mode apply > "$TEMP_DIR/paused.log" 2>&1 || fail "Paused apply failed"
[[ "$(snapshot)" == "$BEFORE" ]] || fail "Paused apply changed remotes"
run_ok release --mode apply
grep -q '^changed.*candidate-10.4.x.*ln' "$TEMP_DIR/release.log" || fail "Change not reported"
check_released 10.4.60 61
remote hpcc4j cat-file -e candidate-10.4.60:java-fix.txt || fail "Non-trigger change not included in release"
has_ref platform refs/heads/candidate-10.2.84 && fail "Unchanged branch released"
AFTER=$(snapshot)
run_ok rerun --mode apply
[[ "$(snapshot)" == "$AFTER" ]] || fail "Rerun released again"

# Forced release of an unchanged branch.
expect_failure 2 run --mode apply --force
run_ok force --mode apply --branch candidate-10.2.x --force
grep -q '^forced' "$TEMP_DIR/force.log" || fail "Force not reported"
check_released 10.2.84 85
expect_failure 2 run --mode apply --branch candidate-10.6.x

# A configured branch that does not exist is an internal error.
jq '.tagging.branches += ["candidate-10.6.x"]' "$TEMP_DIR/policy.json" > "$TEMP_DIR/missing.json"
change eclide candidate-10.4.x pending-change
BEFORE=$(snapshot)
expect_failure 2 bash "$SCRIPT" --policy "$TEMP_DIR/missing.json" --mode apply
grep -q 'branch candidate-10.6.x does not exist' "$TEMP_DIR/failure.log" || fail "Missing branch not reported"
[[ "$(snapshot)" == "$BEFORE" ]] || fail "Missing branch run changed remotes"

# A rejected push stops all later pushes; a rerun completes the partial release.
cat > "$REMOTES/hpcc4j.git/hooks/pre-receive" <<'EOF'
#!/usr/bin/env bash
exit 1
EOF
chmod +x "$REMOTES/hpcc4j.git/hooks/pre-receive"
expect_failure 1 run --mode apply
grep -q '^push-failed.*hpcc4j' "$TEMP_DIR/failure.log" || fail "Push failure not recorded"
grep -q '^not-pushed.*platform' "$TEMP_DIR/failure.log" || fail "Later push not recorded as skipped"
has_ref eclide refs/heads/candidate-10.4.62 || fail "Earlier repository not pushed"
for id in hpcc4j ln platform; do
    has_ref "$id" refs/heads/candidate-10.4.62 && fail "$id pushed after an earlier failure"
done
remote helm-chart cat-file -e master:docs/hpcc-10.4.62.tgz 2>/dev/null && fail "Chart pushed after failure"
rm "$REMOTES/hpcc4j.git/hooks/pre-receive"
run_ok resume --mode apply
grep -q '^resume' "$TEMP_DIR/resume.log" || fail "Resume not reported"
grep -q '^released.*eclide.*already released' "$TEMP_DIR/resume.log" || fail "Released repository not skipped"
check_released 10.4.62 63

# A failed chart push is republished by the next run without a new release.
change platform candidate-10.4.x platform-fix
cat > "$REMOTES/helm-chart.git/hooks/pre-receive" <<'EOF'
#!/usr/bin/env bash
exit 1
EOF
chmod +x "$REMOTES/helm-chart.git/hooks/pre-receive"
expect_failure 1 run --mode apply
grep -q '^push-failed.*helm-chart' "$TEMP_DIR/failure.log" || fail "Chart push failure not recorded"
has_ref platform refs/heads/candidate-10.4.64 || fail "Platform not released before chart push"
rm "$REMOTES/helm-chart.git/hooks/pre-receive"
run_ok republish --mode apply
grep -q '^republish.*hpcc-10.4.64.tgz' "$TEMP_DIR/republish.log" || fail "Republish not reported"
check_released 10.4.64 65
has_ref platform refs/heads/candidate-10.4.66 && fail "Republish created a new release"
AFTER=$(snapshot)
run_ok after-republish --mode apply
[[ "$(snapshot)" == "$AFTER" ]] || fail "Republished chart published again"

# Inconsistent versions are internal errors.
refresh eclide candidate-10.2.x
ECLIDE_X=$(git -C "$WORK/eclide" rev-parse HEAD)
sed -i 's/HPCC_POINT 85 )/HPCC_POINT 87 )/' "$WORK/eclide/version.cmake"
git -C "$WORK/eclide" commit -qam "Unexpected version"
git -C "$WORK/eclide" push -q origin candidate-10.2.x
expect_failure 2 run --mode apply
grep -q 'eclide: .* is not a complete gold release' "$TEMP_DIR/failure.log" || fail "Version disagreement not reported"
sed -i 's/HPCC_POINT 87 )/HPCC_POINT 89 )/' "$WORK/eclide/version.cmake"
git -C "$WORK/eclide" commit -qam "Unexpected version"
git -C "$WORK/eclide" push -q origin candidate-10.2.x
expect_failure 2 run --mode apply
grep -q 'inconsistent points: 85 89' "$TEMP_DIR/failure.log" || fail "Point gap not reported"
git -C "$WORK/eclide" push -q -f origin "$ECLIDE_X:refs/heads/candidate-10.2.x"

# A release that skips an earlier repository was not made by this process.
go_gold hpcc4j candidate-10.2.x
expect_failure 2 run --mode apply
grep -q 'hpcc4j: released 10.2.86 after a pending repository' "$TEMP_DIR/failure.log" || fail "Prefix gap not reported"
remote hpcc4j update-ref -d refs/heads/candidate-10.2.86
remote hpcc4j tag -d hpcc4j_10.2.86-1-release >/dev/null
remote hpcc4j update-ref refs/heads/candidate-10.2.x candidate-10.2.x^

# A branch that moves after analysis fails the preflight before anything is pushed.
change eclide candidate-10.4.x race-fix
REAL_GIT=$(command -v git)
export REAL_GIT RACE_TRIGGER="$TEMP_DIR/race-trigger" RACE_WORK="$WORK/eclide"
mkdir "$TEMP_DIR/race-bin"
cat > "$TEMP_DIR/race-bin/git" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
if [[ "$*" == *"push -q --dry-run"* && ! -f "$RACE_TRIGGER" ]]; then
    touch "$RACE_TRIGGER"
    echo concurrent > "$RACE_WORK/concurrent.txt"
    "$REAL_GIT" -C "$RACE_WORK" add concurrent.txt
    "$REAL_GIT" -C "$RACE_WORK" commit -qm "Concurrent change"
    "$REAL_GIT" -C "$RACE_WORK" push -q origin candidate-10.4.x
fi
exec "$REAL_GIT" "$@"
EOF
chmod +x "$TEMP_DIR/race-bin/git"
expect_failure 1 env PATH="$TEMP_DIR/race-bin:$PATH" bash "$SCRIPT" --policy "$TEMP_DIR/policy.json" --mode apply
grep -q 'Preflight push failed for eclide' "$TEMP_DIR/failure.log" || fail "Moved branch not detected"
for id in "${REPOS[@]}"; do
    has_ref "$id" refs/heads/candidate-10.4.66 && fail "$id pushed after a failed preflight"
done
run_ok race-rerun --mode apply
check_released 10.4.66 67
remote eclide cat-file -e candidate-10.4.66:concurrent.txt || fail "Concurrent change not released"

# A point branch appearing after preflight must not be fast-forwarded by the real push.
change ln candidate-10.4.x point-branch-race
export RACE_BRANCH_TRIGGER="$TEMP_DIR/point-branch-trigger"
export RACE_BRANCH_REMOTE="$REMOTES/eclide.git"
export RACE_BRANCH_SHA="$(remote eclide rev-parse refs/heads/candidate-10.4.x)"
export RACE_BRANCH_NAME=candidate-10.4.68
cat > "$TEMP_DIR/race-bin/git" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
if [[ "$*" == *"push -q --dry-run"* && ! -f "$RACE_BRANCH_TRIGGER" ]]; then
    "$REAL_GIT" "$@"
    "$REAL_GIT" --git-dir="$RACE_BRANCH_REMOTE" update-ref "refs/heads/$RACE_BRANCH_NAME" "$RACE_BRANCH_SHA"
    touch "$RACE_BRANCH_TRIGGER"
    exit 0
fi
exec "$REAL_GIT" "$@"
EOF
expect_failure 1 env PATH="$TEMP_DIR/race-bin:$PATH" bash "$SCRIPT" --policy "$TEMP_DIR/policy.json" --mode apply
grep -q 'Push failed for eclide' "$TEMP_DIR/failure.log" || fail "Concurrent point branch was not rejected by the push lease"
[[ "$(remote eclide rev-parse "$RACE_BRANCH_NAME")" == "$RACE_BRANCH_SHA" ]] ||
    fail "Concurrent point branch was overwritten"
for id in hpcc4j ln platform; do
    has_ref "$id" "refs/heads/$RACE_BRANCH_NAME" && fail "$id pushed after the leased push failed"
done
remote eclide update-ref -d "refs/heads/$RACE_BRANCH_NAME"
run_ok point-branch-rerun --mode apply
check_released 10.4.68 69

# Without --gold, go_rc.sh still creates an rc; --gold rejects an rc branch.
BEFORE=$(snapshot)
refresh platform candidate-10.2.x
git -C "$WORK/helm-chart" fetch -q origin && git -C "$WORK/helm-chart" checkout -q -B master origin/master
(cd "$WORK/platform" && bash "$CMAKE_MODULES/go_rc.sh" --no-push) > "$TEMP_DIR/rc.log" 2>&1 || { cat "$TEMP_DIR/rc.log"; fail "go_rc.sh rc"; }
git -C "$WORK/platform" show candidate-10.2.86:version.cmake | grep -q 'HPCC_MATURITY "rc"' || fail "rc maturity"
git -C "$WORK/platform" rev-parse -q --verify refs/tags/platform_10.2.86-rc1 >/dev/null || fail "rc tag"
git -C "$WORK/helm-chart" log -1 --format=%s | grep -qx 'HPCC Helm Charts 10.2.86-rc1 Release Candidate 1' || fail "rc chart message"
(cd "$WORK/platform" && bash "$CMAKE_MODULES/go_rc.sh" --gold --no-push) > "$TEMP_DIR/rc-gold.log" 2>&1 && fail "--gold accepted an rc branch"
[[ "$(snapshot)" == "$BEFORE" ]] || fail "--no-push changed remotes"
[[ -z "$(find "$TMPDIR" -mindepth 1 -print -quit)" ]] || fail "Workspace not cleaned"

echo "PASS: no-change/empty/non-trigger detection, release of all repositories, dry-run, pause, force, missing branch, push failure resume, chart republish, version and prefix errors, moved-branch and point-branch races, rc regression, cleanup"
