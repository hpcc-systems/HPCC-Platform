#!/bin/bash

# Stop on errors
set -e

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
declare -A ORIGIN_REFS
declare -A ORIGIN_BRANCH_SHAS
while read -r sha ref; do
    ORIGIN_REFS["$ref"]="$sha"
    if [[ "$ref" == refs/heads/* ]]; then
        ORIGIN_BRANCH_SHAS["$sha"]=1
    fi
done < <(git ls-remote origin)

declare -A OSS_REFS
while read -r sha ref; do
    OSS_REFS["$ref"]="$sha"
done < <(git ls-remote oss)


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
for ref in "${!ORIGIN_REFS[@]}"; do
    if [[ "$ref" == refs/heads/* ]]; then
        branch="${ref#refs/heads/}"
        
        # Only synchronize specific branches: main, master, or ones starting with candidate-
        if [[ "$branch" == "main" || "$branch" == "master" || "$branch" == candidate-* ]]; then
            origin_sha="${ORIGIN_REFS[$ref]}"
            oss_sha="${OSS_REFS[$ref]}"

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
done

echo "Synchronizing tags..."
for ref in "${!ORIGIN_REFS[@]}"; do
    # Only process tags, and exclude dereferenced annotated tags (ending with ^{})
    if [[ "$ref" == refs/tags/* && ! "$ref" == *^{} ]]; then
        tag="${ref#refs/tags/}"
        origin_sha="${ORIGIN_REFS[$ref]}"
        peel_ref="${ref}^{}"
        commit_sha="${ORIGIN_REFS[$peel_ref]:-$origin_sha}"
        oss_sha="${OSS_REFS[$ref]}"

        # Only push the tag if it corresponds to an existing branch SHA we care about,
        # and if the tag doesn't already exist with the same SHA on oss.
        # For annotated tags, use the peeled commit SHA (^{}) to check branch membership.
        if [[ -n "${ORIGIN_BRANCH_SHAS[$commit_sha]}" && "$origin_sha" != "$oss_sha" ]]; then
            push_to_oss "$origin_sha" "$ref" "Tag $tag"
        fi
    fi
done

echo "Synchronization complete."
