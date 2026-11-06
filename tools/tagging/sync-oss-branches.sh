#!/usr/bin/env bash
set -uo pipefail

usage() {
    cat <<'EOF'
Usage: sync-oss-branches.sh [--policy FILE] [--mode dry-run|apply] [--repository ID] [--tracematch]

Reads the repository list and sync_enabled flags from the root
branch-policy.json. For each enabled repository, a single local Git repository
has two remotes: origin (risk-hsy source) and oss (hpcc-systems destination).
Both remotes are fetched, then master and candidate-* source branches are
selectively considered for synchronization. Every eligible push is dry-run
checked first. Tags pointing exactly at a selected internal branch tip are also
considered. Divergent branches and conflicting tags are never force-updated.

Modes:
  dry-run  Validate/fetch all selected refs and dry-run eligible pushes. Default.
  apply    Do the same preflight, then push eligible refs if policy is unpaused.

Options:
  --policy FILE       Policy JSON (default: repository-root branch-policy.json)
  --mode MODE         dry-run or apply
  --repository ID     One policy repository ID; default is all configured repos
  --tracematch        Trace match operations (branch and tag matching)
  -h, --help          Show this help

Local bare-repository tests may set SYNCOSS_ALLOW_LOCAL_REMOTES=1. The Actions
workflow must never set that variable.
EOF
}

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/../.." && pwd)
POLICY_FILE="$REPO_ROOT/branch-policy.json"
MODE="dry-run"
SELECTED_REPOSITORY="all"
TRACEMATCH=0
INTERNAL_TOKEN=${BRANCH_APP_TOKEN_INTERNAL:-}
OSS_TOKEN=${BRANCH_APP_TOKEN_OSS:-}
OUTCOME="not-started"
FAILURES=0
SELECTED_COUNT=0
RESULTS_FILE=$(mktemp "${TMPDIR:-/tmp}/sync-oss-results.XXXXXX") || exit 2
PLAN_FILE=$(mktemp "${TMPDIR:-/tmp}/sync-oss-plan.XXXXXX") || exit 2
WORK_DIR=$(mktemp -d "${TMPDIR:-/tmp}/sync-oss-work.XXXXXX") || exit 2

cleanup() {
    local result=$?
    trap - EXIT
    write_summary
    rm -f "$RESULTS_FILE" "$PLAN_FILE"
    rm -rf "$WORK_DIR"
    exit "$result"
}

record() {
    local status=$1
    local repository=$2
    local branch=$3
    local detail=$4

    printf '%s\t%s\t%s\t%s\n' "$status" "$repository" "$branch" "$detail" >> "$RESULTS_FILE"
    printf '%-14s %-16s %-28s %s\n' "$status" "$repository" "$branch" "$detail"
    case "$status" in
        failed|diverged|blocked) FAILURES=$((FAILURES + 1)) ;;
    esac
}

write_summary() {
    [[ -n "${GITHUB_STEP_SUMMARY:-}" ]] || return 0
    {
        printf '## OSS branch synchronization: %s\n\n' "$OUTCOME"
        printf -- '- Mode: `%s`\n' "$MODE"
        printf -- '- Policy: `%s`\n' "$POLICY_FILE"
        printf -- '- Repository selection: `%s`\n' "$SELECTED_REPOSITORY"
        printf -- '- Repositories inspected: `%s`\n\n' "$SELECTED_COUNT"
        printf '| Result | Repository | Branch | Detail |\n|---|---|---|---|\n'
        # Render each recorded result row in the Actions summary table.
        while IFS=$'\t' read -r status repository branch detail; do
            [[ -n "$status" ]] || continue
            printf '| %s | `%s` | `%s` | %s |\n' "$status" "$repository" "$branch" "$detail"
        done < "$RESULTS_FILE"
    } >> "$GITHUB_STEP_SUMMARY"
}

fatal() {
    OUTCOME="failed"
    echo "ERROR: $*" >&2
    exit 2
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --policy)
            [[ $# -ge 2 ]] || { usage >&2; exit 2; }
            POLICY_FILE=$2
            shift 2
            ;;
        --mode)
            [[ $# -ge 2 ]] || { usage >&2; exit 2; }
            MODE=$2
            shift 2
            ;;
        --repository)
            [[ $# -ge 2 ]] || { usage >&2; exit 2; }
            SELECTED_REPOSITORY=$2
            shift 2
            ;;
        --tracematch)
            TRACEMATCH=1
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1" >&2
            usage >&2
            exit 2
            ;;
    esac
done

trap cleanup EXIT

[[ "$MODE" == "dry-run" || "$MODE" == "apply" ]] || fatal "Mode must be dry-run or apply"
[[ -f "$POLICY_FILE" ]] || fatal "Policy file not found: $POLICY_FILE"
command -v jq >/dev/null 2>&1 || fatal "jq is required to read the policy"
command -v git >/dev/null 2>&1 || fatal "git is required"
jq -e '
    .schema_version == 1 and
    (.paused | type == "boolean") and
    (.repositories | type == "array" and length > 0) and
    (.repositories | all(.[];
        (.id | type == "string" and length > 0) and
        (.source | type == "string" and length > 0) and
        (.destination | type == "string" and length > 0) and
        (.sync_enabled | type == "boolean")
    ))
' "$POLICY_FILE" >/dev/null || fatal "Policy JSON or required fields are invalid"

PAUSED=$(jq -r '.paused' "$POLICY_FILE")
if [[ "$MODE" == "apply" && "$PAUSED" == "true" ]]; then
    OUTCOME="paused"
    echo "Policy is paused; no remotes were fetched or changed."
    exit 0
fi

repo_url() {
    local value=$1
    local expected_owner=$2

    if [[ "$value" =~ ^([A-Za-z0-9_.-]+)/([A-Za-z0-9_.-]+)$ ]]; then
        [[ "${BASH_REMATCH[1],,}" == "$expected_owner" ]] || return 1
        printf 'https://github.com/%s/%s.git' "${BASH_REMATCH[1]}" "${BASH_REMATCH[2]}"
        return 0
    fi

    if [[ "${SYNCOSS_ALLOW_LOCAL_REMOTES:-0}" == "1" && ( "$value" == /* || "$value" == file://* ) ]]; then
        printf '%s' "$value"
        return 0
    fi
    return 1
}

git_auth() {
    local repository_dir=$1
    local remote_name=$2
    local token=$3
    shift 3

    local url
    url=$(git -C "$repository_dir" remote get-url "$remote_name") || return 1
    if [[ "$url" == https://github.com/* ]]; then
        [[ -n "$token" ]] || return 1
        local auth
        auth=$(printf 'x-access-token:%s' "$token" | base64 | tr -d '\r\n') || return 1
        GIT_CONFIG_COUNT=1 \
        GIT_CONFIG_KEY_0='http.https://github.com/.extraheader' \
        GIT_CONFIG_VALUE_0="AUTHORIZATION: basic $auth" \
            git -C "$repository_dir" "$@"
    else
        git -C "$repository_dir" "$@"
    fi
}

remote_ref_sha() {
    local repository_dir=$1
    local remote_name=$2
    local token=$3
    local ref=$4
    local output

    output=$(git_auth "$repository_dir" "$remote_name" "$token" ls-remote --refs "$remote_name" "$ref") || return 2
    if [[ -z "$output" ]]; then
        return 1
    fi
    printf '%s\n' "$output" | awk 'NR == 1 { print $1 }'
}

fetch_remote() {
    local repository_dir=$1
    local remote_name=$2
    local token=$3
    git_auth "$repository_dir" "$remote_name" "$token" fetch --prune --no-tags "$remote_name" \
        "+refs/heads/*:refs/remotes/$remote_name/*" \
        "+refs/tags/*:refs/tags/$remote_name/*"
}

# Process each repository entry from the policy JSON.
while IFS= read -r entry; do
    repo_id=$(jq -r '.id' <<< "$entry")
    source_slug=$(jq -r '.source' <<< "$entry")
    destination_slug=$(jq -r '.destination' <<< "$entry")
    sync_enabled=$(jq -r '.sync_enabled' <<< "$entry")

    [[ "$repo_id" =~ ^[A-Za-z0-9_-]+$ ]] || fatal "Invalid repository ID in policy"
    if [[ "$SELECTED_REPOSITORY" != "all" && "$SELECTED_REPOSITORY" != "$repo_id" ]]; then
        continue
    fi
    SELECTED_COUNT=$((SELECTED_COUNT + 1))
    if [[ "$sync_enabled" != "true" ]]; then
        record "disabled" "$repo_id" "-" "sync_enabled is false"
        continue
    fi
    source_url=$(repo_url "$source_slug" "risk-hsy") || fatal "Invalid internal source repository for $repo_id"
    destination_url=$(repo_url "$destination_slug" "hpcc-systems") || fatal "Invalid OSS destination repository for $repo_id"

    repository_dir="$WORK_DIR/$repo_id"
    mkdir -p "$repository_dir" || fatal "Cannot create work directory for $repo_id"
    git -C "$repository_dir" init --quiet || fatal "Cannot initialize work repository for $repo_id"
    git -C "$repository_dir" remote add origin "$source_url" || fatal "Cannot configure origin remote for $repo_id"
    git -C "$repository_dir" remote add oss "$destination_url" || fatal "Cannot configure oss remote for $repo_id"

    if ! fetch_remote "$repository_dir" origin "$INTERNAL_TOKEN" >/dev/null 2>&1; then
        record "failed" "$repo_id" "-" "Could not fetch internal repository"
        continue
    fi
    if ! fetch_remote "$repository_dir" oss "$OSS_TOKEN" >/dev/null 2>&1; then
        record "failed" "$repo_id" "-" "Could not fetch OSS repository"
        continue
    fi

    branch_tips_file="$repository_dir/selected-branch-tips"
    syncable_branches_file="$repository_dir/syncable-branches"
    : > "$branch_tips_file"
    : > "$syncable_branches_file"
    branch_count=0
    # Inspect fetched internal branches, considering only master and candidate-*.
    while IFS= read -r branch; do
        case "$branch" in
            master|candidate-*) ;;
            *) continue ;;
        esac
        branch_count=$((branch_count + 1))

        source_sha=$(git -C "$repository_dir" rev-parse --verify "refs/remotes/origin/$branch^{commit}") || {
            record "failed" "$repo_id" "$branch" "Fetched source did not resolve to a commit"
            continue
        }
        printf '%s\t%s\n' "$source_sha" "$branch" >> "$branch_tips_file"

        target_sha=""
        # Test the fetched OSS remote-tracking ref; If it exists, record its commit SHA.
        if git -C "$repository_dir" show-ref --verify --quiet "refs/remotes/oss/$branch"; then
            target_sha=$(git -C "$repository_dir" rev-parse --verify "refs/remotes/oss/$branch^{commit}") || {
                record "failed" "$repo_id" "$branch" "Fetched destination did not resolve to a commit"
                continue
            }
        fi

        if [[ -n "$target_sha" ]]; then
            if [[ "$source_sha" == "$target_sha" ]]; then
                [[ "$TRACEMATCH" -eq 1 ]] && \
                    record "no-op" "$repo_id" "$branch" "Internal and OSS refs already match"
                printf '%s\t%s\n' "$source_sha" "$branch" >> "$syncable_branches_file"
                continue
            fi
            if ! git -C "$repository_dir" merge-base "$target_sha" "$source_sha" >/dev/null 2>&1 || \
               ! git -C "$repository_dir" merge-base --is-ancestor "$target_sha" "$source_sha"; then
                record "diverged" "$repo_id" "$branch" "OSS tip is not an ancestor of the internal tip; no force push"
                continue
            fi
        fi

        if ! git_auth "$repository_dir" oss "$OSS_TOKEN" push --dry-run oss "$source_sha:refs/heads/$branch" >/dev/null 2>&1; then
            record "failed" "$repo_id" "$branch" "OSS push dry-run failed"
            continue
        fi
        printf 'branch\t%s\t%s\t%s\t%s\t%s\t%s\n' \
            "$repo_id" "$branch" "refs/remotes/origin/$branch" "refs/heads/$branch" \
            "$source_sha" "${target_sha:-missing}" >> "$PLAN_FILE"
        printf '%s\t%s\n' "$source_sha" "$branch" >> "$syncable_branches_file"
        if [[ -z "$target_sha" ]]; then
            record "ready" "$repo_id" "$branch" "Dry-run creation of OSS branch accepted for $source_sha"
        else
            record "ready" "$repo_id" "$branch" "Dry-run fast-forward accepted for $source_sha"
        fi
    done < <(git -C "$repository_dir" for-each-ref --format='%(refname:strip=3)' refs/remotes/origin | sort)

    if [[ "$branch_count" -eq 0 ]]; then
        record "no-matching-branches" "$repo_id" "-" "No master or candidate-* source branches found"
        continue
    fi

    # Select internal tags whose peeled commit matches a selected branch tip.
    while IFS= read -r tag; do
        source_ref="refs/tags/origin/$tag"
        source_oid=$(git -C "$repository_dir" rev-parse --verify "$source_ref") || {
            record "failed" "$repo_id" "tag:$tag" "Fetched source tag did not resolve"
            continue
        }
        tag_commit=$(git -C "$repository_dir" rev-parse --verify "$source_ref^{}" 2>/dev/null) || continue
        if ! awk -F '\t' -v commit="$tag_commit" '$1 == commit { found = 1 } END { exit !found }' "$branch_tips_file"; then
            continue
        fi
        associated_branch=$(awk -F '\t' -v commit="$tag_commit" '$1 == commit { print $2; exit }' "$syncable_branches_file")
        if [[ -z "$associated_branch" ]]; then
            record "blocked" "$repo_id" "tag:$tag" "Associated source branch is not eligible for synchronization"
            continue
        fi

        destination_ref="refs/tags/$tag"
        destination_source_ref="refs/tags/oss/$tag"
        destination_oid=""
        if git -C "$repository_dir" show-ref --verify --quiet "$destination_source_ref"; then
            destination_oid=$(git -C "$repository_dir" rev-parse --verify "$destination_source_ref") || {
                record "failed" "$repo_id" "tag:$tag" "Fetched OSS tag did not resolve"
                continue
            }
            if [[ "$source_oid" == "$destination_oid" ]]; then
                [[ "$TRACEMATCH" -eq 1 ]] && \
                    record "no-op" "$repo_id" "tag:$tag" "Associated tag objects already match"
                continue
            fi
            record "diverged" "$repo_id" "tag:$tag" "OSS tag exists with a different object; tag will not be overwritten"
            continue
        fi

        if ! git_auth "$repository_dir" oss "$OSS_TOKEN" push --dry-run oss "$source_ref:$destination_ref" >/dev/null 2>&1; then
            record "failed" "$repo_id" "tag:$tag" "OSS tag push dry-run failed"
            continue
        fi
        printf 'tag\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
            "$repo_id" "$tag" "$source_ref" "$destination_ref" "$source_oid" "missing" "$associated_branch" >> "$PLAN_FILE"
        record "ready" "$repo_id" "tag:$tag" "Associated tag push accepted for $source_oid"
    done < <(git -C "$repository_dir" for-each-ref --format='%(refname:strip=3)' refs/tags/origin | sort)
done < <(jq -c '.repositories[]' "$POLICY_FILE")

[[ "$SELECTED_COUNT" -gt 0 ]] || fatal "No policy repository matched selection: $SELECTED_REPOSITORY"

if [[ "$MODE" == "apply" ]]; then
    # Apply each planned ref update and verify the resulting OSS object ID.
    while IFS=$'\t' read -r kind repo_id name source_ref destination_ref source_oid target_oid associated_branch; do
        [[ -n "$repo_id" ]] || continue
        repository_dir="$WORK_DIR/$repo_id"
        if [[ "$kind" == "tag" ]]; then
            tag_commit=$(git -C "$repository_dir" rev-parse --verify "$source_ref^{}") || {
                record "failed" "$repo_id" "$kind:$name" "Source tag no longer resolves to a commit"
                continue
            }
            associated_branch_sha=""
            if ! associated_branch_sha=$(remote_ref_sha "$repository_dir" oss "$OSS_TOKEN" "refs/heads/$associated_branch"); then
                record "blocked" "$repo_id" "$kind:$name" "Associated OSS branch is missing or unavailable; tag not pushed"
                continue
            fi
            if [[ "$associated_branch_sha" != "$tag_commit" ]]; then
                record "blocked" "$repo_id" "$kind:$name" "Associated OSS branch is not at the tagged source tip; tag not pushed"
                continue
            fi
        fi
        if ! git_auth "$repository_dir" oss "$OSS_TOKEN" push oss "$source_ref:$destination_ref" >/dev/null 2>&1; then
            record "failed" "$repo_id" "$kind:$name" "Push failed; current remote state must be reconciled before retry"
            continue
        fi

        remote_sha=""
        if ! remote_sha=$(remote_ref_sha "$repository_dir" oss "$OSS_TOKEN" "$destination_ref"); then
            record "failed" "$repo_id" "$kind:$name" "Push returned success but remote verification failed"
            continue
        fi
        if [[ "$remote_sha" != "$source_oid" ]]; then
            record "failed" "$repo_id" "$kind:$name" "Post-push object mismatch; expected $source_oid, found $remote_sha"
            continue
        fi
        if [[ "$kind" == "branch" && "$target_oid" == "missing" ]]; then
            record "synced" "$repo_id" "$kind:$name" "Created OSS branch at $remote_sha"
        else
            record "synced" "$repo_id" "$kind:$name" "Verified OSS object $remote_sha"
        fi
    done < "$PLAN_FILE"
fi

if [[ "$FAILURES" -gt 0 ]]; then
    OUTCOME="partial-failure"
    exit 1
fi
if [[ "$MODE" == "apply" ]]; then
    OUTCOME="complete"
else
    OUTCOME="dry-run-complete"
fi
exit 0