#!/usr/bin/env bash
set -uo pipefail

# Defaults: inspect all configured repositories without changing remote refs.
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
POLICY_FILE="$SCRIPT_DIR/../../branch-policy.json"
MODE=dry-run
SELECTED_REPOSITORY=all
FAILURES=0

usage() {
    cat <<'EOF'
Usage: upmerge-policy-branches.sh [--policy FILE] [--mode dry-run|apply] [--repository ID]

Upmerge adjacent branches in branch-policy.json's upmerge_order for repositories
with upmerge_enabled=true. Default dry-run performs the full local merge chain,
including version normalization and commits, without pushing any refs.
Apply pushes ordinary merge commits only when the policy is unpaused.
Conflicts stop dependent edges, not independent repositories. Reruns use Git
ancestry to skip completed merges. Signing and pre-push builds are not required.

BRANCH_APP_TOKEN_INTERNAL authenticates risk-hsy reads and writes.
Local bare-repository tests may set UPMERGE_ALLOW_LOCAL_REMOTES=1; never set this
in Actions. Requires git and jq.
EOF
}

fatal() { echo "ERROR: $*" >&2; exit 2; }

# Parse CLI overrides before validating the policy or contacting any remote.
while [[ $# -gt 0 ]]; do
    case "$1" in
        --policy|--mode|--repository)
            [[ $# -ge 2 ]] || fatal "Missing value for $1"
            case "$1" in
                --policy) POLICY_FILE=$2;;
                --mode) MODE=$2;;
                --repository) SELECTED_REPOSITORY=$2;;
            esac
            shift 2;;
        -h|--help) usage; exit 0;;
        *) usage >&2; fatal "Unknown option: $1";;
    esac
done

# Reject invalid topology, duplicate repositories and unsupported branch names
# before creating a workspace or performing any repository operations.
[[ "$MODE" == dry-run || "$MODE" == apply ]] || fatal "Mode must be dry-run or apply"
for tool in git jq; do command -v "$tool" >/dev/null || fatal "$tool is required"; done

[[ -f "$POLICY_FILE" ]] || fatal "Policy not found: $POLICY_FILE"
jq -e '
    .schema_version == 1 and (.paused | type == "boolean") and
    (.upmerge_order | type == "array" and length > 0 and
       all(.[]; type == "string" and length > 0)) and
    (.upmerge_order | length == (unique | length)) and
    (.repositories | type == "array" and length > 0 and all(.[];
       (.id | type == "string" and test("^[A-Za-z0-9_-]+$")) and
       (.source | type == "string" and length > 0) and
       (.upmerge_enabled | type == "boolean"))) and
    (.repositories | map(.id) | length == (unique | length)) and
    (.repositories | map(.source | ascii_downcase) | length == (unique | length))
' "$POLICY_FILE" >/dev/null || fatal "Invalid policy or duplicate branch/repository"
mapfile -t ORDER < <(jq -r '.upmerge_order[]' "$POLICY_FILE")
for branch in "${ORDER[@]}"; do
    [[ "$branch" == master || "$branch" == candidate-* ]] || fatal "Unapproved upmerge branch: $branch"
    git check-ref-format "refs/heads/$branch" || fatal "Invalid branch: $branch"
done
[[ "$SELECTED_REPOSITORY" == all ]] ||
    jq -e --arg id "$SELECTED_REPOSITORY" '.repositories | any(.[]; .id == $id)' "$POLICY_FILE" >/dev/null ||
    fatal "Unknown repository: $SELECTED_REPOSITORY"

# Restrict production remotes to the internal organization. Local paths are
# accepted only through an explicit test-only opt-in.
source_url() {
    local slug=$1
    if [[ "$slug" =~ ^([A-Za-z0-9_.-]+)/([A-Za-z0-9_.-]+)$ ]]; then
        [[ "${BASH_REMATCH[1],,}" == risk-hsy ]] || return 1
        printf 'https://github.com/%s.git' "$slug"
    elif [[ "${UPMERGE_ALLOW_LOCAL_REMOTES:-0}" == 1 && ( "$slug" == /* || "$slug" == file://* ) ]]; then
        printf '%s' "$slug"
    else
        return 1
    fi
}
while IFS= read -r source; do
    source_url "$source" >/dev/null || fatal "Invalid internal source repository"
done < <(jq -r '.repositories[] | select(.upmerge_enabled) | .source' "$POLICY_FILE")

# Pause blocks apply before fetching; dry-run remains available for diagnosis.
if [[ "$MODE" == apply && "$(jq -r '.paused' "$POLICY_FILE")" == true ]]; then
    echo "Policy is paused; no remotes were fetched or changed."
    if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
        echo "## Internal upmerge: paused (no writes)" >> "$GITHUB_STEP_SUMMARY"
    fi
    exit 0
fi

# Own all temporary repositories and diagnostics, and summarize/clean them on
# normal exit or cancellation. Committed remote state, not these files, is recovery authority.
WORK_DIR=$(mktemp -d "${TMPDIR:-/tmp}/upmerge-work.XXXXXX") || fatal "Cannot create owned workspace"
RESULTS="$WORK_DIR/results"
cleanup() {
    local result=$?
    trap - EXIT
    if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
        {
            printf '## Internal upmerge: %s\n\n' "$MODE"
            printf '| Result | Repository | Edge | Detail |\n|---|---|---|---|\n'
            while IFS=$'\t' read -r status repo edge detail; do
                printf '| %s | `%s` | `%s` | %s |\n' "$status" "$repo" "$edge" "$detail"
            done < "$RESULTS"
        } >> "$GITHUB_STEP_SUMMARY" || { echo "ERROR: Could not write upmerge summary" >&2; result=1; }
    fi
    rm -rf "$WORK_DIR" || { echo "ERROR: Could not remove owned upmerge workspace" >&2; result=1; }
    exit "$result"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
touch "$RESULTS" || fatal "Cannot initialize operation results"
export GIT_TERMINAL_PROMPT=0

# Accumulate operation outcomes while allowing independent repositories to continue.
record() {
    local status=$1 repo=$2 edge=$3 detail=$4
    local indicator
    case "$status" in
        merged|dry-run) indicator="✓";;
        no-op) indicator="→";;
        conflict|failed) indicator="✗"; FAILURES=$((FAILURES + 1));;
        blocked) indicator="⊘";;
        disabled) indicator="○";;
        *) indicator="-";;
    esac
    printf '%s\t%s\t%s\t%s\n' "$status" "$repo" "$edge" "$detail" >> "$RESULTS" ||
        fatal "Cannot record upmerge outcome"
    printf '%-14s %-16s %-44s %s %s\n' "$status" "$repo" "$edge" "$indicator" "$detail"
}

# Supply authentication for this remote command only; do not persist credentials
# in Git configuration or embed them in remote URLs.
git_remote() {
    local directory=$1
    shift
    local url auth
    url=$(git -C "$directory" remote get-url origin) || return 1
    if [[ "$url" == https://github.com/* ]]; then
        if [[ -z "${BRANCH_APP_TOKEN_INTERNAL:-}" ]]; then
            if command -v gh >/dev/null; then
                BRANCH_APP_TOKEN_INTERNAL=$(gh auth token) || {
                    echo "ERROR: Failed to retrieve GitHub token via gh auth token" >&2
                    return 1
                }
            else
                echo "ERROR: BRANCH_APP_TOKEN_INTERNAL not set and gh CLI is not available" >&2
                return 1
            fi
        fi
        auth=$(printf 'x-access-token:%s' "$BRANCH_APP_TOKEN_INTERNAL" | base64 | tr -d '\r\n') || return 1
        GIT_CONFIG_COUNT=1 GIT_CONFIG_KEY_0="http.$url.extraheader" \
            GIT_CONFIG_VALUE_0="AUTHORIZATION: basic $auth" git -C "$directory" "$@"
    else
        git -C "$directory" "$@"
    fi
}

# Patch only the HPCC release-version fields that are authoritative in this chart:
# Chart.yaml package metadata and the generated manifest's helmVersion helper.
patch_helm_release_version() {
    local file=$1 input=$2 output=$3
    case "$file" in
        helm/hpcc/Chart.yaml)
            sed -E -e "s/^version: .*$/version: $version/" \
                -e "s/^appVersion: .*$/appVersion: $version/" "$input" > "$output"
            ;;
        helm/hpcc/templates/_helpers.tpl)
            sed -E -e "s/^helmVersion: .*$/helmVersion: $version/" "$input" > "$output"
            ;;
    esac
}

# Preserve target release metadata while merging source changes to Helm content.
normalize_versions() {
    local directory=$1 file field value version stage
    # Keep release metadata from the target branch: restore its top-level
    # version.cmake and POM files, plus POMs up to the legacy helper's depth.
    # Read only files tracked by HEAD so source-only or untracked paths are not
    # restored; treat all paths as data rather than executing them with eval.
    while IFS= read -r -d '' file; do
        if [[ "$file" == version.cmake || "$file" == pom.xml ||
              ( "$file" == */pom.xml && "$file" != */*/*/*/*/pom.xml ) ]]; then
            git -C "$directory" restore --source=HEAD --staged --worktree -- "$file" || return 1
        fi
    done < <(git -C "$directory" ls-tree -rz --name-only HEAD)

    # Repositories without the HPCC Helm chart have no chart versions to normalize.
    [[ -d "$directory/helm/hpcc" ]] || return 0

    # Require a regular target version file; do not read a symlink or accept a
    # chart merge where the target's version metadata is unavailable.
    [[ -f "$directory/version.cmake" && ! -L "$directory/version.cmake" ]] ||
        { echo "ERROR: Helm normalization requires a target version.cmake" >&2; return 1; }

    # Extract the five target release fields as plain text; version.cmake is data,
    # not shell input, so branch content cannot execute during normalization.
    local -a values=()
    for field in HPCC_MAJOR HPCC_MINOR HPCC_POINT HPCC_MATURITY HPCC_SEQUENCE; do
        value=$(sed -nE "s/^[[:space:]]*[sS][eE][tT][[:space:]]*\\([[:space:]]*$field[[:space:]]+\"?([^\" )]+)\"?[[:space:]]*\\).*$/\\1/p" "$directory/version.cmake")
        # A missing or multiline value is ambiguous and must not produce a chart version.
        [[ -n "$value" && "$value" != *$'\n'* ]] || { echo "ERROR: Cannot read $field" >&2; return 1; }
        values+=("$value")
    done

    # Validate the extracted values before interpolating them into Helm files.
    [[ "${values[0]}.${values[1]}.${values[2]}" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ &&
       "${values[3]}" =~ ^[A-Za-z]+$ && "${values[4]}" =~ ^[0-9]+$ ]] ||
        { echo "ERROR: Invalid target version fields" >&2; return 1; }

    # Format release versions without a maturity label; retain labels such as rc
    # for prereleases, followed by the target branch's sequence number.
    version="${values[0]}.${values[1]}.${values[2]}-"
    [[ "${values[3]}" == release ]] || version+="${values[3]}"
    version+="${values[4]}"

    # Only these two tracked files contain HPCC release-version metadata:
    # Chart.yaml defines the package/app versions and _helpers.tpl defines the
    # helmVersion label rendered into workloads. Other version fields belong to
    # components or Kubernetes APIs and must remain part of the ordinary merge.
    while IFS= read -r -d '' file; do
        [[ "$file" == helm/hpcc/Chart.yaml || "$file" == helm/hpcc/templates/_helpers.tpl ]] || continue
        [[ -f "$directory/$file" && ! -L "$directory/$file" ]] ||
            { echo "ERROR: Unsafe or missing Helm file: $file" >&2; return 1; }

        # If Git left conflict stages, normalize only the authoritative release
        # fields in each stage; all other source/target content still uses Git's
        # normal three-way conflict detection.
        if [[ -n "$(git -C "$directory" ls-files -u -- "$file")" ]]; then
            # Read the merge base, target, and source versions of the conflicted
            # file from the index. A missing base is valid for an added file;
            # missing target/source means delete/modify and needs manual handling.
            for stage in 1 2 3; do
                if git -C "$directory" cat-file -e ":$stage:$file" 2>/dev/null; then
                    git -C "$directory" show ":$stage:$file" > "$WORK_DIR/original-$stage" || return 1
                elif [[ "$stage" == 1 ]]; then
                    : > "$WORK_DIR/original-1"
                else
                    echo "Unresolved Helm delete/modify conflict: $file" >&2
                    return 1
                fi
            done

            # Use explicit output files so empty stages (for example, an added
            # file with no merge base) cannot retain data from a previous file.
            patch_helm_release_version "$file" "$WORK_DIR/original-1" "$WORK_DIR/stage-1" || return 1
            patch_helm_release_version "$file" "$WORK_DIR/original-2" "$WORK_DIR/stage-2" || return 1
            patch_helm_release_version "$file" "$WORK_DIR/original-3" "$WORK_DIR/stage-3" || return 1

            # Merge the normalized files with target as ours, the true merge base,
            # and normalized source as theirs. Non-version content conflicts fail.
            if ! git merge-file -p "$WORK_DIR/stage-2" "$WORK_DIR/stage-1" "$WORK_DIR/stage-3" > "$WORK_DIR/normalized"; then
                echo "Unresolved Helm content conflict: $file" >&2
                return 1
            fi
        else
            # For a file already cleanly merged by Git, use its merged content.
            cp "$directory/$file" "$WORK_DIR/normalized" || return 1
        fi

        # Ensure release metadata uses the target version while leaving every
        # unrelated version field in the merged chart unchanged.
        patch_helm_release_version "$file" "$WORK_DIR/normalized" "$directory/$file" || return 1
        git -C "$directory" add -- "$file" || return 1
    # Use NUL delimiters for safe paths; only tracked files are considered.
    done < <(git -C "$directory" ls-files -z helm/hpcc/Chart.yaml helm/hpcc/templates/_helpers.tpl)
}

# Fetch one repository into an isolated workspace and build local branches for
# its ordered chain. A failed repository must not stop the outer repository loop.
process_repository() {
    local id=$1 source=$2 directory="$WORK_DIR/$1" url branch
    local -a branches=()
    url=$(source_url "$source") || return 1
    mkdir "$directory" &&
        git -C "$directory" init -q &&
        git -C "$directory" config user.name "HPCC Branch Upmerge" &&
        git -C "$directory" config user.email "branch-upmerge@users.noreply.github.com" &&
        git -C "$directory" config commit.gpgsign false &&
        git -C "$directory" remote add origin "$url" ||
        { record failed "$id" "-" "Cannot initialize workspace"; return 1; }
    git_remote "$directory" fetch --no-tags origin '+refs/heads/*:refs/remotes/origin/*' ||
        { record failed "$id" "-" "Cannot fetch internal branches"; return 1; }
    for branch in "${ORDER[@]}"; do
        if git -C "$directory" show-ref --verify --quiet "refs/remotes/origin/$branch"; then
            branches+=("$branch")
            git -C "$directory" branch "$branch" "refs/remotes/origin/$branch" >/dev/null || return 1
        fi
    done
    # Single-branch repositories have no edges; otherwise require the complete
    # configured chain before attempting any merge or push.
    if [[ ${#branches[@]} == 1 ]]; then
        record not-applicable "$id" "-" "Only one applicable branch; no upmerge edges"
        return 0
    fi
    [[ ${#branches[@]} == ${#ORDER[@]} ]] ||
        { record failed "$id" "-" "Configured upmerge branches are missing"; return 1; }
    local index source_branch target edge source_sha target_sha current merged merge_status aborted=false
    local -a commit_args
    # Process adjacent edges in order. Local source tips include preceding
    # successful merges, including simulated commits when running in dry-run.
    for ((index=1; index<${#branches[@]}; index++)); do
        source_branch=${branches[index-1]}
        target=${branches[index]}
        edge="$source_branch -> $target"
        if [[ "$aborted" == true ]]; then record blocked "$id" "$edge" "Prior edge failed"; continue; fi
        source_sha=$(git -C "$directory" rev-parse "refs/heads/$source_branch") || return 1
        target_sha=$(git -C "$directory" rev-parse "refs/heads/$target") || return 1
        # Ancestry makes reruns idempotent, including recovery after partial pushes.
        if git -C "$directory" merge-base --is-ancestor "$source_sha" "$target_sha"; then
            record no-op "$id" "$edge" "Source $source_sha already merged"
            continue
        fi
        git -C "$directory" checkout -q "$target" || return 1
        # Keep a merge in progress even for fast-forward candidates so target
        # version normalization happens before creating the merge commit.
        merge_status=0
        git -C "$directory" merge --no-commit --no-ff "$source_sha" >/dev/null 2>&1 || merge_status=$?
        if [[ "$merge_status" -gt 1 ]] || ! git -C "$directory" rev-parse --verify MERGE_HEAD >/dev/null 2>&1; then
            record failed "$id" "$edge" "Git merge failed ($merge_status)"
            aborted=true
        elif ! normalize_versions "$directory" ||
             [[ -n "$(git -C "$directory" ls-files -u)" ]]; then
            unresolved=$(git -C "$directory" diff --name-only --diff-filter=U | tr '\n' ', ' | sed 's/,$//')
            record conflict "$id" "$edge" "Unresolved in: $unresolved"
            aborted=true
        else
            commit_args=(--no-gpg-sign -s -m "Upmerge $source_branch into $target" -m "Source: $source_sha")
            if [[ -z "$(git -C "$directory" diff --cached --name-only)" ]]; then
                # Preserve ancestry even when independent changes produce the same tree.
                commit_args=(--allow-empty "${commit_args[@]}")
            fi
            if ! git -C "$directory" commit "${commit_args[@]}"; then
                record failed "$id" "$edge" "Cannot commit merge"
                aborted=true
            fi
        fi
        # Discard unresolved local merge state and report later dependent edges
        # as blocked; other repositories and the workflow's sync phase may continue.
        if [[ "$aborted" == true ]]; then
            if git -C "$directory" rev-parse --verify MERGE_HEAD >/dev/null 2>&1; then
                git -C "$directory" merge --abort ||
                    { record failed "$id" "$edge" "Cannot abort merge; owned workspace will be discarded"; return 1; }
            fi
            continue
        fi
        merged=$(git -C "$directory" rev-parse HEAD) || return 1
        # Retain simulated local commits for the next edge, but never push in dry-run.
        if [[ "$MODE" == dry-run ]]; then
            record dry-run "$id" "$edge" "Simulated $merged; no push"
            continue
        fi
        # Check for target movement, push without force, then verify remote ancestry.
        # Any uncertain result stops dependent edges; the next run refetches remote truth.
        current=$(git_remote "$directory" ls-remote --refs origin "refs/heads/$target") || current=""
        if [[ "${current%%$'\t'*}" != "$target_sha" ]]; then
            record failed "$id" "$edge" "Target moved or cannot be read; re-run against current remote state"
            aborted=true
        elif ! git_remote "$directory" push origin "$merged:refs/heads/$target"; then
            record failed "$id" "$edge" "Push rejected or transport failed; re-run to reconcile remote state"
            aborted=true
        elif ! git_remote "$directory" fetch --no-tags origin "+refs/heads/$target:refs/remotes/origin/$target" ||
             ! git -C "$directory" merge-base --is-ancestor "$merged" "refs/remotes/origin/$target"; then
            record failed "$id" "$edge" "Cannot verify pushed merge; re-run to reconcile remote state"
            aborted=true
        else
            record merged "$id" "$edge" "Verified $merged (source $source_sha)"
        fi
    done
}

# Apply repository selection/enablement and aggregate failures without stopping
# healthy repositories. Return failure overall if any selected operation failed.
while IFS= read -r entry; do
    id=$(jq -r '.id' <<< "$entry")
    [[ "$SELECTED_REPOSITORY" == all || "$SELECTED_REPOSITORY" == "$id" ]] || continue
    if [[ "$(jq -r '.upmerge_enabled' <<< "$entry")" != true ]]; then
        record disabled "$id" "-" "upmerge_enabled is false"
        continue
    fi
    before=$FAILURES
    if ! process_repository "$id" "$(jq -r '.source' <<< "$entry")"; then
        [[ $FAILURES -gt $before ]] || record failed "$id" "-" "Repository operation failed"
    fi
done < <(jq -c '.repositories[]' "$POLICY_FILE")

# Print clear final status
echo ""
if [[ $FAILURES == 0 ]]; then
    echo "✓ SUCCESS: All upmerges completed without conflicts or errors"
    exit 0
else
    echo "✗ FAILED: $FAILURES merge operation(s) had conflicts or errors (see above)"
    exit 1
fi
