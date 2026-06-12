#!/bin/bash

# Stop on errors
set -e

# NOTE: Do NOT use Bash associative arrays (declare -A) in this script.
# macOS ships with Bash 3.2 by default, which does not support associative
# arrays. Requiring Bash 4+ would break portability for users on macOS and
# some other systems where an older /bin/bash is the default. To keep this
# script widely usable we rely on temporary files, awk and grep for mappings
# and lookups instead of in-memory associative arrays.

# Default values
DRY_RUN=0
ORIGIN_USER=""
OSS_USER=""
CURRENT_USER=""

require_value() {
    local flag=$1
    local count=$2
    local value=$3
    if [[ "$count" -lt 2 || -z "$value" ]]; then
        echo "Error: $flag requires a value" >&2
        exit 1
    fi
}


# Parse arguments
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --dry-run|--dryrun) DRY_RUN=1; shift ;;
        --originuser)
            require_value "$1" "$#" "$2"
            ORIGIN_USER="$2"
            shift 2
            ;;
        --ossuser)
            require_value "$1" "$#" "$2"
            OSS_USER="$2"
            shift 2
            ;;
        *) echo "Unknown parameter passed: $1"; exit 1 ;;
    esac
done

switch_auth() {
    local target_user=$1
    if [[ -n "$target_user" && "$CURRENT_USER" != "$target_user" ]]; then
        echo "Switching authentication to $target_user..."
        gh auth switch --user "$target_user"
        CURRENT_USER="$target_user"
    fi
}

echo "Fetching from origin..."
switch_auth "$ORIGIN_USER"
git fetch origin

echo "Fetching from oss..."
switch_auth "$OSS_USER"
git fetch oss

# Fetch all remote refs locally for faster comparisons
echo "Gathering remote refs..."
ORIGIN_REFS_FILE=$(mktemp "${TMPDIR:-/tmp}/syncoss-origin-refs.XXXXXX")
OSS_REFS_FILE=$(mktemp "${TMPDIR:-/tmp}/syncoss-oss-refs.XXXXXX")
ORIGIN_BRANCH_SHAS_FILE=$(mktemp "${TMPDIR:-/tmp}/syncoss-origin-branch-shas.XXXXXX")
trap 'rm -f "$ORIGIN_REFS_FILE" "$OSS_REFS_FILE" "$ORIGIN_BRANCH_SHAS_FILE"' EXIT

git ls-remote origin > "$ORIGIN_REFS_FILE"
git ls-remote oss > "$OSS_REFS_FILE"

# Precompute a merged refs file with columns: origin_sha <tab> oss_sha <tab> ref
# This lets us iterate once over origin refs and have the corresponding OSS sha
# available without rescanning the full OSS refs file on every lookup.
MERGED_REFS_FILE=$(mktemp "${TMPDIR:-/tmp}/syncoss-merged-refs.XXXXXX")
TAG_ACTIONS_FILE=$(mktemp "${TMPDIR:-/tmp}/syncoss-tag-actions.XXXXXX")
trap 'rm -f "$ORIGIN_REFS_FILE" "$OSS_REFS_FILE" "$ORIGIN_BRANCH_SHAS_FILE" "$MERGED_REFS_FILE" "$TAG_ACTIONS_FILE"' EXIT

# Build merged refs: read OSS refs into a map, then print origin entries with the
# matching oss sha (empty if missing). Using awk keeps this O(N).
awk 'NR==FNR { oss[$2]=$1; next } { printf "%s\t%s\t%s\n", $1, ( $2 in oss ? oss[$2] : "" ), $2 }' "$OSS_REFS_FILE" "$ORIGIN_REFS_FILE" > "$MERGED_REFS_FILE"

# Build set of branch SHAs from origin (unchanged)
while read -r sha ref; do
    if [[ "$ref" == refs/heads/* ]]; then
        branch="${ref#refs/heads/}"
        if [[ "$branch" == "main" || "$branch" == "master" || "$branch" == candidate-* ]]; then
            echo "$sha" >> "$ORIGIN_BRANCH_SHAS_FILE"
        fi
    fi
done < "$ORIGIN_REFS_FILE"
push_to_oss() {
    local src_ref=$1
    local dest_ref=$2
    local label=$3

    if [[ $DRY_RUN -eq 1 ]]; then
        echo "$label: Would execute: git push oss $src_ref:$dest_ref"
    else
        echo "$label: Pushing to oss..."
        git push oss "$src_ref:$dest_ref"
    fi
}

echo "Synchronizing branches..."
# Read merged refs: origin_sha <tab> oss_sha <tab> ref
while IFS=$'\t' read -r origin_sha oss_sha ref; do
    if [[ "$ref" == refs/heads/* ]]; then
        branch="${ref#refs/heads/}"

        # Only synchronize specific branches: main, master, or ones starting with candidate-
        if [[ "$branch" == "main" || "$branch" == "master" || "$branch" == candidate-* ]]; then
            # Proceed only if the branch on oss is out of sync with origin
            if [[ "$origin_sha" != "$oss_sha" ]]; then

                # Check if the branch is new to oss, or if oss can be fast-forwarded from origin
                if [[ -z "$oss_sha" ]] || git merge-base --is-ancestor "$oss_sha" "$origin_sha" 2>/dev/null; then
                    push_to_oss "$origin_sha" "$ref" "Branch $branch"
                else
                    echo "Branch $branch: Push would be rejected (oss is not an ancestor of origin). Skipping."
                fi
            fi
        fi
    fi
done < "$MERGED_REFS_FILE"

echo "Synchronizing tags..."
# Precompute tag push actions in one AWK pass over the merged refs and the
# origin branch SHAs. Output lines: origin_sha <tab> ref
awk -F"\t" -v branches_file="$ORIGIN_BRANCH_SHAS_FILE" '
    BEGIN { while ((getline < branches_file) > 0) branch_shas[$1]=1 }
    {
        origin=$1; oss=$2; ref=$3
        refs[ref]=origin
        ossmap[ref]=oss
        order[++n]=ref
    }
    END {
        for (i=1;i<=n;i++) {
            ref=order[i]
            if (ref ~ /^refs\/tags\// && ref !~ /\^\{\}$/) {
                peel = ref "^{}"
                commit = (peel in refs) ? refs[peel] : refs[ref]
                if (commit != "" && (commit in branch_shas)) {
                    if (refs[ref] != ossmap[ref]) print refs[ref] "\t" ref
                }
            }
        }
    }' "$MERGED_REFS_FILE" > "$TAG_ACTIONS_FILE"

# Now perform the tag pushes
while read -r origin_sha ref; do
    tag="${ref#refs/tags/}"
    push_to_oss "$origin_sha" "$ref" "Tag $tag"
done < "$TAG_ACTIONS_FILE"

echo "Synchronization complete."
