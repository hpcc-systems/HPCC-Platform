#!/usr/bin/env bash
set -euo pipefail

TEST_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SYNC_SCRIPT="$TEST_DIR/../sync-oss-branches.sh"
TEMP_DIR=$(mktemp -d "${TMPDIR:-/tmp}/sync-oss-test.XXXXXX")
trap 'rm -rf "$TEMP_DIR"' EXIT

export GIT_CONFIG_NOSYSTEM=1
export GIT_CONFIG_GLOBAL=/dev/null
export GIT_AUTHOR_NAME="Branch Sync Test"
export GIT_AUTHOR_EMAIL="branch-sync@example.invalid"
export GIT_COMMITTER_NAME="$GIT_AUTHOR_NAME"
export GIT_COMMITTER_EMAIL="$GIT_AUTHOR_EMAIL"
export SYNCOSS_ALLOW_LOCAL_REMOTES=1

expect_failure() {
    local log_file=$1
    shift
    if "$@" >"$log_file" 2>&1; then
        echo "Expected command to fail: $*" >&2
        exit 1
    fi
}

make_pair() {
    local name=$1
    local source="$TEMP_DIR/$name-source.git"
    local target="$TEMP_DIR/$name-oss.git"
    local work="$TEMP_DIR/$name-seed"

    git init --quiet --bare --initial-branch=master "$source"
    git init --quiet --bare --initial-branch=master "$target"
    git init --quiet --initial-branch=master "$work"
    git -C "$work" config user.name "$GIT_AUTHOR_NAME"
    git -C "$work" config user.email "$GIT_AUTHOR_EMAIL"
    printf 'base\n' > "$work/data.txt"
    git -C "$work" add data.txt
    git -C "$work" commit --quiet -m "Seed $name"
    git -C "$work" remote add origin "$source"
    git -C "$work" remote add oss "$target"
    git -C "$work" push --quiet origin master
    git -C "$work" push --quiet oss master
    printf '%s\n%s\n' "$source" "$target"
}

add_source_branch() {
    local source=$1
    local branch=$2
    local content=$3
    local source_name=${source##*/}
    local work="$TEMP_DIR/${source_name%.git}-source-work-$branch"
    git clone --quiet "$source" "$work"
    git -C "$work" config user.name "$GIT_AUTHOR_NAME"
    git -C "$work" config user.email "$GIT_AUTHOR_EMAIL"
    if git -C "$work" show-ref --verify --quiet "refs/heads/$branch"; then
        git -C "$work" checkout --quiet "$branch"
    else
        git -C "$work" checkout --quiet -b "$branch" origin/master
    fi
    printf '%s\n' "$content" > "$work/$branch.txt"
    git -C "$work" add "$branch.txt"
    git -C "$work" commit --quiet -m "Source $branch"
    git -C "$work" push --quiet origin "$branch"
}

add_target_branch() {
    local target=$1
    local branch=$2
    local content=$3
    local target_name=${target##*/}
    local work="$TEMP_DIR/${target_name%.git}-target-work-$branch"
    git clone --quiet "$target" "$work"
    git -C "$work" config user.name "$GIT_AUTHOR_NAME"
    git -C "$work" config user.email "$GIT_AUTHOR_EMAIL"
    git -C "$work" checkout --quiet -b "$branch" origin/master
    printf '%s\n' "$content" > "$work/target-$branch.txt"
    git -C "$work" add "target-$branch.txt"
    git -C "$work" commit --quiet -m "Target-only $branch"
    git -C "$work" push --quiet origin "$branch"
}

add_source_tag() {
    local source=$1
    local branch=$2
    local tag=$3
    local kind=$4
    local source_name=${source##*/}
    local work="$TEMP_DIR/${source_name%.git}-tag-work-$tag"
    git clone --quiet "$source" "$work"
    git -C "$work" config user.name "$GIT_AUTHOR_NAME"
    git -C "$work" config user.email "$GIT_AUTHOR_EMAIL"
    git -C "$work" checkout --quiet "$branch"
    if [[ "$kind" == "annotated" ]]; then
        git -C "$work" tag -a "$tag" -m "Release $tag" "$branch"
    else
        git -C "$work" tag "$tag" "$branch"
    fi
    git -C "$work" push --quiet origin "refs/tags/$tag"
}

add_target_tag() {
    local target=$1
    local tag=$2
    local target_name=${target##*/}
    local work="$TEMP_DIR/${target_name%.git}-tag-work-$tag"
    git clone --quiet "$target" "$work"
    git -C "$work" config user.name "$GIT_AUTHOR_NAME"
    git -C "$work" config user.email "$GIT_AUTHOR_EMAIL"
    git -C "$work" tag -a "$tag" -m "Existing conflicting tag $tag" master
    git -C "$work" push --quiet origin "refs/tags/$tag"
}

read_tip() {
    git --git-dir="$1" rev-parse --verify "$2" 2>/dev/null || true
}

read_remote_tip() {
    git ls-remote --heads "$1" "refs/heads/$2" | awk 'NR == 1 { print $1 }'
}

PAIR_OUTPUT=$(make_pair platform)
PLATFORM_SOURCE=$(printf '%s\n' "$PAIR_OUTPUT" | sed -n '1p')
PLATFORM_OSS=$(printf '%s\n' "$PAIR_OUTPUT" | sed -n '2p')
PAIR_OUTPUT=$(make_pair ln)
LN_SOURCE=$(printf '%s\n' "$PAIR_OUTPUT" | sed -n '1p')
LN_OSS=$(printf '%s\n' "$PAIR_OUTPUT" | sed -n '2p')

add_source_branch "$PLATFORM_SOURCE" candidate-10.2.x "platform forward update"
add_source_branch "$PLATFORM_SOURCE" candidate-10.4.x "platform source side"
add_source_branch "$PLATFORM_SOURCE" candidate-10.6.x "platform branch creation"
add_source_branch "$PLATFORM_SOURCE" candidate-11.0.x "new candidate branch not listed in policy"
add_source_branch "$PLATFORM_SOURCE" main "main must not be synchronized"
add_source_branch "$PLATFORM_SOURCE" feature-test "feature branch must not be synchronized"
add_source_tag "$PLATFORM_SOURCE" candidate-10.2.x release-10.2.1 annotated
add_source_tag "$PLATFORM_SOURCE" candidate-10.4.x diverged-branch-release annotated
add_source_tag "$PLATFORM_SOURCE" candidate-11.0.x release-11.0.0 lightweight
add_source_tag "$PLATFORM_SOURCE" feature-test feature-tag annotated
add_source_tag "$PLATFORM_SOURCE" candidate-10.2.x conflicting-release annotated
add_target_branch "$PLATFORM_OSS" candidate-10.4.x "platform target side"
add_target_tag "$PLATFORM_OSS" conflicting-release
add_source_branch "$LN_SOURCE" candidate-10.2.x "ln forward update"
add_source_branch "$LN_SOURCE" candidate-10.4.x "ln forward update"
add_source_branch "$LN_SOURCE" candidate-10.6.x "ln forward update"
add_source_branch "$LN_SOURCE" candidate-11.0.x "new candidate branch discovered"
add_source_branch "$LN_SOURCE" master "ln master update"

POLICY_FILE="$TEMP_DIR/branch-policy.json"
cat > "$POLICY_FILE" <<EOF
{
  "schema_version": 1,
  "paused": false,
  "repositories": [
        {"id":"platform","source":"$PLATFORM_SOURCE","destination":"$PLATFORM_OSS","sync_enabled":true},
        {"id":"ln","source":"$LN_SOURCE","destination":"$LN_OSS","sync_enabled":true}
  ]
}
EOF

PLATFORM_BEFORE=$(read_remote_tip "$PLATFORM_OSS" candidate-10.2.x)
expect_failure "$TEMP_DIR/dry-run.log" bash "$SYNC_SCRIPT" --policy "$POLICY_FILE" --mode dry-run
[[ "$(read_remote_tip "$PLATFORM_OSS" candidate-10.2.x)" == "$PLATFORM_BEFORE" ]]
grep -q "diverged.*platform.*candidate-10.4.x" "$TEMP_DIR/dry-run.log"
grep -q "ready.*platform.*candidate-10.2.x" "$TEMP_DIR/dry-run.log"
grep -q "ready.*platform.*candidate-11.0.x.*creation of OSS branch" "$TEMP_DIR/dry-run.log"
grep -q "ready.*platform.*tag:release-10.2.1" "$TEMP_DIR/dry-run.log"
grep -q "ready.*platform.*tag:release-11.0.0" "$TEMP_DIR/dry-run.log"
grep -q "blocked.*platform.*tag:diverged-branch-release" "$TEMP_DIR/dry-run.log"
grep -q "diverged.*platform.*tag:conflicting-release" "$TEMP_DIR/dry-run.log"
! grep -q "platform.*main" "$TEMP_DIR/dry-run.log" || exit 1
! grep -q "platform.*feature-test" "$TEMP_DIR/dry-run.log" || exit 1
! grep -q "feature-tag" "$TEMP_DIR/dry-run.log" || exit 1

expect_failure "$TEMP_DIR/apply.log" bash "$SYNC_SCRIPT" --policy "$POLICY_FILE" --mode apply
[[ -n "$(read_remote_tip "$PLATFORM_OSS" candidate-10.2.x)" ]]
[[ -n "$(read_remote_tip "$PLATFORM_OSS" candidate-10.6.x)" ]]
[[ "$(read_remote_tip "$PLATFORM_OSS" candidate-11.0.x)" == "$(git --git-dir="$PLATFORM_SOURCE" rev-parse refs/heads/candidate-11.0.x)" ]]
[[ "$(read_tip "$PLATFORM_OSS" refs/tags/release-10.2.1)" == "$(read_tip "$PLATFORM_SOURCE" refs/tags/release-10.2.1)" ]]
[[ "$(read_tip "$PLATFORM_OSS" refs/tags/release-11.0.0)" == "$(read_tip "$PLATFORM_SOURCE" refs/tags/release-11.0.0)" ]]
[[ -z "$(read_tip "$PLATFORM_OSS" refs/tags/diverged-branch-release)" ]]
[[ "$(read_tip "$PLATFORM_OSS" refs/tags/conflicting-release)" != "$(read_tip "$PLATFORM_SOURCE" refs/tags/conflicting-release)" ]]
[[ -z "$(read_tip "$PLATFORM_OSS" refs/tags/feature-tag)" ]]
[[ -z "$(read_remote_tip "$PLATFORM_OSS" main)" ]]
[[ -z "$(read_remote_tip "$PLATFORM_OSS" feature-test)" ]]
[[ "$(read_remote_tip "$PLATFORM_OSS" candidate-10.4.x)" != "$(git --git-dir="$PLATFORM_SOURCE" rev-parse refs/heads/candidate-10.4.x)" ]]
[[ "$(read_remote_tip "$LN_OSS" master)" == "$(git --git-dir="$LN_SOURCE" rev-parse refs/heads/master)" ]]
[[ "$(read_remote_tip "$LN_OSS" candidate-11.0.x)" == "$(git --git-dir="$LN_SOURCE" rev-parse refs/heads/candidate-11.0.x)" ]]
grep -q "synced.*ln.*master" "$TEMP_DIR/apply.log"
grep -q "synced.*platform.*branch:candidate-11.0.x.*Created OSS branch" "$TEMP_DIR/apply.log"

cp "$POLICY_FILE" "$TEMP_DIR/unpaused-policy.json"
sed 's/"paused": false/"paused": true/' "$POLICY_FILE" > "$TEMP_DIR/paused-policy.json"
PLATFORM_MASTER_BEFORE=$(read_remote_tip "$PLATFORM_OSS" master)
bash "$SYNC_SCRIPT" --policy "$TEMP_DIR/paused-policy.json" --mode apply > "$TEMP_DIR/paused.log"
[[ "$(read_remote_tip "$PLATFORM_OSS" master)" == "$PLATFORM_MASTER_BEFORE" ]]
grep -q "paused" "$TEMP_DIR/paused.log"

expect_failure "$TEMP_DIR/invalid-repo.log" bash "$SYNC_SCRIPT" --policy "$POLICY_FILE" --repository unknown
jq '.repositories[0].sync_enabled = false' "$POLICY_FILE" > "$TEMP_DIR/disabled-policy.json"
bash "$SYNC_SCRIPT" --policy "$TEMP_DIR/disabled-policy.json" --mode apply --repository platform > "$TEMP_DIR/disabled.log"
grep -q "disabled.*platform" "$TEMP_DIR/disabled.log"

echo "sync-oss-branches tests passed"