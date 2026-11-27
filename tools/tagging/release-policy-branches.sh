#!/usr/bin/env bash
set -uo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/../.." && pwd)
CMAKE_MODULES="$REPO_ROOT/cmake_modules"
POLICY_FILE="$REPO_ROOT/branch-policy.json"
MODE=dry-run
SELECTED_BRANCH=auto
FORCE=false
OUTCOME=not-started
FAILURE_DETAIL=
WORK_DIR=
RESULTS=

usage() {
    cat <<'EOF'
Usage: release-policy-branches.sh [--policy FILE] [--mode dry-run|apply]
                                  [--branch auto|candidate-A.B.x] [--force]

Tag new gold point releases for the branches in branch-policy.json's
tagging.branches. For each branch, if any repository with tag_triggers=true has
changed since its last "Split off" commit, every repository with
tag_enabled=true is split and tagged gold (A.B.N-1) using go_rc.sh --gold.

Everything is prepared locally first. In apply mode each repository is then
pushed with one atomic push (the .x branch, the new point branch and its tags),
branches in policy order and repositories in manifest order, followed by the
helm-chart repository. The first failed push stops all further pushes.

A rerun derives the state from the remote repositories only. A partly pushed
release (a prefix of repositories released, the rest untouched) is completed
without change detection. Any other inconsistency is an internal error.

Options:
  --policy FILE    Policy JSON (default: repository-root branch-policy.json)
  --mode MODE      dry-run (default): prepare and report, never push
                   apply: push, unless the policy is paused
  --branch NAME    auto (default) for all tagging.branches, or one of them
  --force          Release even if nothing changed (requires --branch NAME)

Exit status: 0 nothing to do or completed, 1 push or fetch failure (rerun to
resume), 2 internal error or inconsistent state (needs manual attention).

BRANCH_APP_TOKEN_INTERNAL authenticates risk-hsy reads and writes. Local
bare-repository tests may set RELEASE_ALLOW_LOCAL_REMOTES=1; never set this in
Actions. Requires git, jq and helm.
EOF
}

write_summary() {
    if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
        {
            printf '## Automated gold tagging: %s (%s)\n\n' "$OUTCOME" "$MODE"
            [[ -z "$FAILURE_DETAIL" ]] || printf '**%s**\n\n' "$FAILURE_DETAIL"
            if [[ -n "$RESULTS" && -s "$RESULTS" ]]; then
                printf '| Result | Branch | Repository | Detail |\n|---|---|---|---|\n'
                while IFS=$'\t' read -r status branch repo detail; do
                    printf '| %s | `%s` | `%s` | %s |\n' "$status" "$branch" "$repo" "$detail"
                done < "$RESULTS"
            fi
        } >> "$GITHUB_STEP_SUMMARY" || echo "ERROR: Could not write tagging summary" >&2
    fi
    if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
        # Single line for the failure notification
        printf 'failure=%s\n' "$(tr '\n' ' ' <<< "$FAILURE_DETAIL")" >> "$GITHUB_OUTPUT" ||
            echo "ERROR: Could not write tagging output" >&2
    fi
}

cleanup() {
    local result=$?
    trap - EXIT
    write_summary
    [[ -z "$WORK_DIR" ]] || rm -rf "$WORK_DIR" || { echo "ERROR: Could not remove workspace" >&2; result=2; }
    exit "$result"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

fatal() {
    OUTCOME=internal-error
    FAILURE_DETAIL="Internal error: $*"
    echo "ERROR: $*" >&2
    exit 2
}

retryable() {
    OUTCOME=failed
    FAILURE_DETAIL="$*"
    echo "ERROR: $*" >&2
    exit 1
}

record() {
    local status=$1 branch=$2 repo=$3 detail=$4
    printf '%s\t%s\t%s\t%s\n' "$status" "$branch" "$repo" "$detail" >> "$RESULTS"
    printf '%-16s %-20s %-12s %s\n' "$status" "$branch" "$repo" "$detail"
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --policy|--mode|--branch)
            [[ $# -ge 2 ]] || fatal "Missing value for $1"
            case "$1" in
                --policy) POLICY_FILE=$2;;
                --mode) MODE=$2;;
                --branch) SELECTED_BRANCH=$2;;
            esac
            shift 2;;
        --force) FORCE=true; shift;;
        -h|--help) usage; exit 0;;
        *) usage >&2; fatal "Unknown option: $1";;
    esac
done

[[ "$MODE" == dry-run || "$MODE" == apply ]] || fatal "Mode must be dry-run or apply"
for tool in git jq; do command -v "$tool" >/dev/null || fatal "$tool is required"; done
[[ -f "$POLICY_FILE" ]] || fatal "Policy not found: $POLICY_FILE"

# Validate the structure first so the detailed checks below cannot fail on types.
jq -e '
    .schema_version == 1 and (.paused | type == "boolean") and
    (.repositories | type == "array" and length > 0 and all(.[];
        (.id | type == "string" and test("^[A-Za-z0-9_-]+$")) and
        (.source | type == "string" and length > 0) and
        ((.tag_enabled // false) | type == "boolean") and
        ((.tag_triggers // false) | type == "boolean"))) and
    (.repositories | map(.id) | length == (unique | length)) and
    (.tagging | type == "object") and
    (.tagging.branches | type == "array" and length > 0 and all(.[]; type == "string")) and
    (.tagging.helm_chart_repository | type == "string")
' "$POLICY_FILE" >/dev/null || fatal "Invalid policy structure (repositories/tagging)"

POLICY_ERRORS=$(jq -r '
    (.tagging.helm_chart_repository) as $chart |
    [.repositories[] | select(.tag_enabled == true)] as $tagged |
    (if any(.tagging.branches[]; test("^candidate-[0-9]+\\.[0-9]+\\.x$") | not)
        then "tagging.branches must be candidate-A.B.x branches" else empty end),
    (if (.tagging.branches | length) != (.tagging.branches | unique | length)
        then "tagging.branches contains duplicates" else empty end),
    (if any(.repositories[]; (.tag_triggers == true) and (.tag_enabled != true))
        then "tag_triggers requires tag_enabled" else empty end),
    (if ($tagged | length) == 0 then "No repository has tag_enabled" else empty end),
    (if any($tagged[]; .tag_triggers == true) | not then "No repository has tag_triggers" else empty end),
    (if any(.repositories[]; .id == $chart and (.tag_enabled != true)) | not
        then "tagging.helm_chart_repository must name a repository without tag_enabled" else empty end),
    (if any($tagged[]; .id == "helm-chart")
        then "A tagged repository cannot have id helm-chart" else empty end),
    (if any($tagged[]; .id == "platform") and ($tagged[-1].id != "platform")
        then "platform must be the last tagged repository (its tag triggers the build)" else empty end)
' "$POLICY_FILE") || fatal "Cannot validate policy"
[[ -z "$POLICY_ERRORS" ]] || fatal "Invalid policy: $(tr '\n' ';' <<< "$POLICY_ERRORS")"

mapfile -t POLICY_BRANCHES < <(jq -r '.tagging.branches[]' "$POLICY_FILE")
if [[ "$SELECTED_BRANCH" == auto ]]; then
    [[ "$FORCE" == false ]] || fatal "--force requires an explicit --branch"
    BRANCHES=("${POLICY_BRANCHES[@]}")
else
    printf '%s\n' "${POLICY_BRANCHES[@]}" | grep -Fxq -- "$SELECTED_BRANCH" ||
        fatal "Branch $SELECTED_BRANCH is not in tagging.branches"
    BRANCHES=("$SELECTED_BRANCH")
fi

# Restrict production remotes to the internal organization. Local paths are
# accepted only through an explicit test-only opt-in.
source_url() {
    local slug=$1
    if [[ "$slug" =~ ^([A-Za-z0-9_.-]+)/([A-Za-z0-9_.-]+)$ ]]; then
        [[ "${BASH_REMATCH[1],,}" == risk-hsy ]] || return 1
        printf 'https://github.com/%s.git' "$slug"
    elif [[ "${RELEASE_ALLOW_LOCAL_REMOTES:-0}" == 1 && ( "$slug" == /* || "$slug" == file://* ) ]]; then
        printf '%s' "$slug"
    else
        return 1
    fi
}

declare -A SOURCE=() TRIGGER=()
TAGGED=()
while IFS=$'\t' read -r id source enabled triggers; do
    SOURCE[$id]=$source
    TRIGGER[$id]=$triggers
    [[ "$enabled" != true ]] || TAGGED+=("$id")
done < <(jq -r '.repositories[] | [.id, .source, (.tag_enabled // false), (.tag_triggers // false)] | @tsv' "$POLICY_FILE")
CHART_ID=$(jq -r '.tagging.helm_chart_repository' "$POLICY_FILE")
for id in "${TAGGED[@]}" "$CHART_ID"; do
    source_url "${SOURCE[$id]}" >/dev/null || fatal "Invalid internal source repository for $id"
done

if [[ "$MODE" == apply && "$(jq -r '.paused' "$POLICY_FILE")" == true ]]; then
    OUTCOME=paused
    echo "Policy is paused; no remotes were fetched or changed."
    exit 0
fi

WORK_DIR=$(mktemp -d "${TMPDIR:-/tmp}/release-work.XXXXXX") || fatal "Cannot create workspace"
RESULTS="$WORK_DIR/results"
: > "$RESULTS" || fatal "Cannot initialize results"
mkdir "$WORK_DIR/repos" || fatal "Cannot create workspace"
export GIT_TERMINAL_PROMPT=0

# Supply authentication for this remote command only; do not persist credentials
# in Git configuration or embed them in remote URLs.
git_remote() {
    local directory=$1
    shift
    local url auth
    url=$(git -C "$directory" remote get-url origin) || return 1
    if [[ "$url" == https://github.com/* ]]; then
        if [[ -z "${BRANCH_APP_TOKEN_INTERNAL:-}" ]]; then
            command -v gh >/dev/null || { echo "ERROR: BRANCH_APP_TOKEN_INTERNAL not set and gh CLI is not available" >&2; return 1; }
            BRANCH_APP_TOKEN_INTERNAL=$(gh auth token) || { echo "ERROR: gh auth token failed" >&2; return 1; }
        fi
        auth=$(printf 'x-access-token:%s' "$BRANCH_APP_TOKEN_INTERNAL" | base64 | tr -d '\r\n') || return 1
        GIT_CONFIG_COUNT=1 GIT_CONFIG_KEY_0="http.$url.extraheader" \
            GIT_CONFIG_VALUE_0="AUTHORIZATION: basic $auth" git -C "$directory" "$@"
    else
        git -C "$directory" "$@"
    fi
}

repo_dir() { printf '%s/repos/%s' "$WORK_DIR" "$1"; }

clone_repository() {
    local id=$1 directory url
    directory=$(repo_dir "$id")
    url=$(source_url "${SOURCE[$id]}") || return 1
    mkdir "$directory" &&
        git -C "$directory" init -q &&
        git -C "$directory" config user.name "HPCC Release Automation" &&
        git -C "$directory" config user.email "branch-release@users.noreply.github.com" &&
        git -C "$directory" config commit.gpgsign false &&
        git -C "$directory" config tag.gpgSign false &&
        git -C "$directory" remote add origin "$url" || fatal "Cannot initialize workspace for $id"
    git_remote "$directory" fetch -q --no-tags origin \
        '+refs/heads/*:refs/remotes/origin/*' '+refs/tags/*:refs/tags/*' ||
        retryable "Cannot fetch $id; rerun"
}

# Read release metadata from a committed file without executing it.
cmake_values() {
    sed -nE "s/^[[:space:]]*[sS][eE][tT][[:space:]]*\\([[:space:]]*$1[[:space:]]+\"?([^\" )]+)\"?[[:space:]]*\\).*$/\\1/p"
}

# Sets V_KIND V_MAJOR V_MINOR V_POINT V_MATURITY V_SEQUENCE V_PROJECTS, mirroring
# the parsing in cmake_modules/parse_cmake.sh and parse_hpcc_pom.sh.
read_version() {
    local directory=$1 ref=$2 content field value project
    V_KIND='' V_MAJOR='' V_MINOR='' V_POINT='' V_MATURITY='' V_SEQUENCE='' V_PROJECTS=''
    if git -C "$directory" cat-file -e "$ref:pom.xml" 2>/dev/null; then
        content=$(git -C "$directory" show "$ref:pom.xml") || return 1
        V_KIND=pom
        V_PROJECTS=$(grep -m1 '<artifactId>' <<< "$content" | sed -E 's/^[^>]*>([^<]*)<.*$/\1/')
        value=$(grep -m1 '<version>' <<< "$content" | sed -E 's/^[^>]*>([^<]*)<.*$/\1/')
        [[ "$value" =~ ^([0-9]+)\.([0-9]+)\.([0-9]+)-([0-9]+)(-([A-Za-z]+))?$ ]] || return 1
        V_MAJOR=${BASH_REMATCH[1]} V_MINOR=${BASH_REMATCH[2]} V_POINT=${BASH_REMATCH[3]}
        V_SEQUENCE=${BASH_REMATCH[4]} V_MATURITY=${BASH_REMATCH[6]}
        if [[ -z "$V_MATURITY" ]]; then
            V_MATURITY=release
        elif [[ "$V_MATURITY" == SNAPSHOT ]]; then
            if (( V_MINOR % 2 == 1 )); then V_MATURITY=trunk
            elif (( V_POINT % 2 == 0 )); then V_MATURITY=rc
            else V_MATURITY=closedown
            fi
        else
            return 1
        fi
    elif git -C "$directory" cat-file -e "$ref:version.cmake" 2>/dev/null; then
        content=$(git -C "$directory" show "$ref:version.cmake") || return 1
        V_KIND=cmake
        for field in MAJOR MINOR POINT MATURITY SEQUENCE; do
            value=$(cmake_values "HPCC_$field" <<< "$content")
            [[ -n "$value" && "$value" != *$'\n'* ]] || return 1
            printf -v "V_$field" '%s' "$value"
        done
        V_PROJECTS=$(cmake_values HPCC_PROJECT <<< "$content" | tr '\n' ' ')
        V_PROJECTS=${V_PROJECTS% }
    else
        return 1
    fi
    [[ "$V_MAJOR.$V_MINOR.$V_POINT.$V_SEQUENCE" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ &&
       "$V_MATURITY" =~ ^[a-z]+$ && -n "$V_PROJECTS" ]] || return 1
    for project in $V_PROJECTS; do
        [[ "$project" =~ ^[A-Za-z0-9_.-]+$ ]] || return 1
    done
}

# The gold tags go_rc.sh --gold creates for point N (see set_tag in parse_*.sh).
expected_tags() {
    local kind=$1 projects=$2 version=$3 project
    for project in $projects; do
        if [[ "$kind" == pom ]]; then
            printf '%s_%s-1-release\n' "$project" "$version"
        else
            printf '%s_%s-1\n' "$project" "$version"
        fi
    done | sort
}

remote_branch_exists() { git -C "$1" show-ref --verify --quiet "refs/remotes/origin/$2"; }
tag_exists() { git -C "$1" show-ref --verify --quiet "refs/tags/$2"; }

# Most recent first-parent commit of ref with exactly this subject.
find_split() {
    local directory=$1 ref=$2 subject=$3
    git -C "$directory" log --first-parent --format='%H%x09%s' "$ref" |
        awk -F '\t' -v s="$subject" '$2 == s { print $1; exit }'
}

# Returns success if repository id has released A.B.N gold from branch X:
# point branch with release N-1 at its tip, all tags there, and the split on .x.
is_released() {
    local id=$1 x=$2 version=$3 directory tip tag
    directory=$(repo_dir "$id")
    remote_branch_exists "$directory" "candidate-$version" || return 1
    tip=$(git -C "$directory" rev-parse "refs/remotes/origin/candidate-$version") || return 1
    read_version "$directory" "$tip" || return 1
    [[ "$V_MAJOR.$V_MINOR.$V_POINT" == "$version" && "$V_MATURITY" == release && "$V_SEQUENCE" == 1 ]] || return 1
    while IFS= read -r tag; do
        tag_exists "$directory" "$tag" || return 1
        [[ "$(git -C "$directory" rev-parse "refs/tags/$tag^{commit}")" == "$tip" ]] || return 1
    done < <(expected_tags "$V_KIND" "$V_PROJECTS" "$version")
    [[ -n "$(find_split "$directory" "refs/remotes/origin/$x" "Split off $version")" ]]
}

# Returns success if nothing for A.B.N exists yet in repository id (branch X).
is_pending() {
    local id=$1 version=$2 x=$3 directory tag
    directory=$(repo_dir "$id")
    remote_branch_exists "$directory" "candidate-$version" && return 1
    read_version "$directory" "refs/remotes/origin/$x" || return 1
    while IFS= read -r tag; do
        tag_exists "$directory" "$tag" && return 1
    done < <(expected_tags "$V_KIND" "$V_PROJECTS" "$version")
    return 0
}

# Returns success if a trigger repository changed since "Split off A.B.(P-1)".
# Any unexpected history is an internal error.
has_changed() {
    local id=$1 x=$2 previous=$3 directory split
    directory=$(repo_dir "$id")
    remote_branch_exists "$directory" "candidate-$previous" ||
        fatal "$id: previous point branch candidate-$previous does not exist"
    split=$(find_split "$directory" "refs/remotes/origin/$x" "Split off $previous")
    [[ -n "$split" ]] || fatal "$id: no 'Split off $previous' commit on $x"
    git -C "$directory" merge-base --is-ancestor "$split^" "refs/remotes/origin/candidate-$previous" ||
        fatal "$id: candidate-$previous was not branched from the commit before 'Split off $previous'"
    ! git -C "$directory" diff --quiet "$split" "refs/remotes/origin/$x" --
}

# Name of the chart package helm creates from a commit, or empty if there is no chart.
chart_package() {
    local directory=$1 ref=$2 chart name version
    chart=$(git -C "$directory" show "$ref:helm/hpcc/Chart.yaml" 2>/dev/null) || return 0
    name=$(sed -nE 's/^name: *//p' <<< "$chart" | head -1)
    version=$(sed -nE 's/^version: *//p' <<< "$chart" | head -1)
    [[ "$name-$version" =~ ^[A-Za-z0-9_.-]+$ ]] || return 1
    printf '%s-%s.tgz' "$name" "$version"
}

chart_published() {
    local package=$1 directory
    directory=$(repo_dir helm-chart)
    # Let grep consume the complete index so pipefail cannot turn grep's early exit into SIGPIPE.
    git -C "$directory" cat-file -e "refs/heads/master:docs/$package" 2>/dev/null &&
        git -C "$directory" show "refs/heads/master:docs/index.yaml" 2>/dev/null | grep -F "/$package" >/dev/null
}

for id in "${TAGGED[@]}"; do clone_repository "$id"; done
SOURCE[helm-chart]=${SOURCE[$CHART_ID]}
clone_repository helm-chart
git -C "$(repo_dir helm-chart)" checkout -q -B master refs/remotes/origin/master ||
    fatal "helm-chart has no master branch"
HELM_BASE=$(git -C "$(repo_dir helm-chart)" rev-parse HEAD)

# Analysis: decide per branch what to prepare, before anything is changed.
declare -A PLAN_REPOS=() PLAN_VERSION=() REPUBLISH=()
for x in "${BRANCHES[@]}"; do
    major_minor=${x#candidate-}
    major_minor=${major_minor%.x}
    declare -A POINT=()
    points=()
    chart_repo=
    for id in "${TAGGED[@]}"; do
        directory=$(repo_dir "$id")
        remote_branch_exists "$directory" "$x" || fatal "$id: branch $x does not exist"
        read_version "$directory" "refs/remotes/origin/$x" || fatal "$id: cannot read version on $x"
        [[ "$V_MAJOR.$V_MINOR" == "$major_minor" && "$V_MATURITY" == closedown ]] && (( V_POINT % 2 == 1 )) ||
            fatal "$id: $x is at $V_MAJOR.$V_MINOR.$V_POINT $V_MATURITY; expected $major_minor.<odd> closedown"
        POINT[$id]=$V_POINT
        points+=("$V_POINT")
        if git -C "$directory" cat-file -e "refs/remotes/origin/$x:helm/hpcc/Chart.yaml" 2>/dev/null; then
            [[ -z "$chart_repo" ]] || fatal "$x: more than one tagged repository has a helm chart"
            chart_repo=$id
        fi
    done
    [[ -z "$chart_repo" || "$chart_repo" == "${TAGGED[-1]}" ]] ||
        fatal "$chart_repo publishes the helm chart and triggers the build, so it must be the last tagged repository"
    mapfile -t distinct < <(printf '%s\n' "${points[@]}" | sort -nu)

    if [[ ${#distinct[@]} == 1 ]]; then
        point=${distinct[0]}
        version="$major_minor.$((point + 1))"
        for id in "${TAGGED[@]}"; do
            is_pending "$id" "$version" "$x" ||
                fatal "$id: candidate-$version or its tags already exist, but $x is still at $point"
        done
        # Recover a release whose chart publish failed after the platform push.
        previous="$major_minor.$((point - 1))"
        if [[ -n "$chart_repo" ]] && is_released "$chart_repo" "$x" "$previous"; then
            ref=$(git -C "$(repo_dir "$chart_repo")" rev-parse "refs/remotes/origin/candidate-$previous")
            package=$(chart_package "$(repo_dir "$chart_repo")" "$ref") || fatal "$chart_repo: invalid chart on candidate-$previous"
            if [[ -n "$package" ]] && ! chart_published "$package"; then
                REPUBLISH[$x]=$ref
                record republish "$x" helm-chart "$package missing; republishing from candidate-$previous"
            fi
        fi
        changed=()
        for id in "${TAGGED[@]}"; do
            [[ "${TRIGGER[$id]}" == true ]] || continue
            if has_changed "$id" "$x" "$previous"; then
                changed+=("$id")
                record changed "$x" "$id" "Changed since Split off $previous"
            else
                record unchanged "$x" "$id" "No change since Split off $previous"
            fi
        done
        if [[ ${#changed[@]} -gt 0 || "$FORCE" == true ]]; then
            PLAN_REPOS[$x]="${TAGGED[*]}"
            PLAN_VERSION[$x]=$version
            [[ ${#changed[@]} -gt 0 ]] || record forced "$x" "-" "Release forced without changes"
        fi
    elif [[ ${#distinct[@]} == 2 && ${distinct[1]} == $((distinct[0] + 2)) ]]; then
        # A previous run pushed some repositories and failed. Complete it.
        point=${distinct[0]}
        version="$major_minor.$((point + 1))"
        pending=()
        seen_pending=false
        for id in "${TAGGED[@]}"; do
            if [[ "${POINT[$id]}" == "$point" ]]; then
                is_pending "$id" "$version" "$x" || fatal "$id: inconsistent partial release of $version"
                pending+=("$id")
                seen_pending=true
            else
                [[ "$seen_pending" == false ]] ||
                    fatal "$id: released $version after a pending repository; not pushed by this process"
                is_released "$id" "$x" "$version" || fatal "$id: $x is at ${POINT[$id]} but $version is not a complete gold release"
                record released "$x" "$id" "$version already released"
            fi
        done
        PLAN_REPOS[$x]="${pending[*]}"
        PLAN_VERSION[$x]=$version
        record resume "$x" "-" "Completing partial release of $version (no change detection)"
    else
        fatal "$x: repositories are at inconsistent points: ${distinct[*]}"
    fi
    unset POINT
done

if [[ ${#PLAN_REPOS[@]} == 0 && ${#REPUBLISH[@]} == 0 ]]; then
    OUTCOME=no-changes
    echo "Nothing to release."
    exit 0
fi
command -v helm >/dev/null || fatal "helm is required"

# Prepare: all branches and repositories locally, nothing is pushed.
declare -A PUSH_SPECS=()
declare -A PUSH_BASE=()
PUSH_ORDER=()
for x in "${BRANCHES[@]}"; do
    if [[ -n "${REPUBLISH[$x]:-}" ]]; then
        directory=$(repo_dir "${TAGGED[-1]}")
        git -C "$directory" clean -qfdx && git -C "$directory" checkout -q --detach "${REPUBLISH[$x]}" ||
            fatal "Cannot check out ${REPUBLISH[$x]} for chart republish"
        (cd "$directory" && env -u BRANCH_APP_TOKEN_INTERNAL bash "$CMAKE_MODULES/publish_helm.sh" --no-push) ||
            fatal "Republishing the helm chart for $x failed"
    fi
    [[ -n "${PLAN_REPOS[$x]:-}" ]] || continue
    version=${PLAN_VERSION[$x]}
    for id in ${PLAN_REPOS[$x]}; do
        directory=$(repo_dir "$id")
        base=$(git -C "$directory" rev-parse "refs/remotes/origin/$x")
        git -C "$directory" clean -qfdx && git -C "$directory" checkout -q -B "$x" "$base" ||
            fatal "$id: cannot check out $x"
        ! git -C "$directory" show-ref --verify --quiet "refs/heads/candidate-$version" ||
            fatal "$id: local candidate-$version already exists"
        echo "=== Preparing $id $version from $x"
        # Preparation is local-only; never expose the write token to legacy helper scripts.
        (cd "$directory" && env -u BRANCH_APP_TOKEN_INTERNAL bash "$CMAKE_MODULES/go_rc.sh" --gold --no-push) ||
            fatal "$id: go_rc.sh --gold failed on $x"

        # Check the plan before anything is pushed.
        x_sha=$(git -C "$directory" rev-parse "refs/heads/$x")
        new_sha=$(git -C "$directory" rev-parse "refs/heads/candidate-$version" 2>/dev/null) ||
            fatal "$id: go_rc.sh did not create candidate-$version"
        [[ "$(git -C "$directory" rev-parse "$x_sha^")" == "$base" && "$(git -C "$directory" log -1 --format=%s "$x_sha")" == "Split off $version" ]] ||
            fatal "$id: unexpected $x commit after preparing"
        read_version "$directory" "$x_sha" && [[ "$V_MATURITY" == closedown && "$V_POINT" == $((${version##*.} + 1)) ]] ||
            fatal "$id: unexpected $x version after preparing"
        [[ "$(git -C "$directory" rev-parse "$new_sha^")" == "$base" ]] ||
            fatal "$id: candidate-$version is not a single commit on $x"
        read_version "$directory" "$new_sha" &&
            [[ "$V_MAJOR.$V_MINOR.$V_POINT" == "$version" && "$V_MATURITY" == release && "$V_SEQUENCE" == 1 ]] ||
            fatal "$id: candidate-$version is not $version gold"
        expected=$(expected_tags "$V_KIND" "$V_PROJECTS" "$version")
        actual=$(git -C "$directory" tag --points-at "$new_sha" | sort)
        [[ "$actual" == "$expected" ]] ||
            fatal "$id: tags at candidate-$version ($(tr '\n' ' ' <<< "$actual")) differ from expected ($(tr '\n' ' ' <<< "$expected"))"
        specs="$x_sha:refs/heads/$x $new_sha:refs/heads/candidate-$version"
        for tag in $actual; do specs+=" refs/tags/$tag:refs/tags/$tag"; done
        PUSH_SPECS["$x|$id"]=$specs
        PUSH_BASE["$x|$id"]=$base
        PUSH_ORDER+=("$x|$id")
        record prepared "$x" "$id" "$version: $(tr '\n' ' ' <<< "$actual")"
    done
done

HELM_DIR=$(repo_dir helm-chart)
HELM_SHA=$(git -C "$HELM_DIR" rev-parse refs/heads/master)
if [[ "$HELM_SHA" != "$HELM_BASE" ]]; then
    git -C "$HELM_DIR" merge-base --is-ancestor "$HELM_BASE" "$HELM_SHA" || fatal "helm-chart master was rewritten"
    [[ -z "$(git -C "$HELM_DIR" status --porcelain --untracked-files=no)" ]] || fatal "helm-chart has uncommitted changes"
    record prepared "-" helm-chart "$(git -C "$HELM_DIR" rev-list --count "$HELM_BASE..$HELM_SHA") chart commit(s)"
fi

if [[ "$MODE" == dry-run ]]; then
    OUTCOME=dry-run
    echo "Dry-run: nothing was pushed."
    exit 0
fi

# Preflight every push, so a moved branch or existing tag fails before any write.
push_release() {
    local key=$1 dry_run=$2 x=${1%|*} id=${1#*|} version directory
    local -a specs options dry_run_option
    directory=$(repo_dir "$id")
    version=${PLAN_VERSION[$x]}
    read -r -a specs <<< "${PUSH_SPECS[$key]}"
    options=(
        "--force-with-lease=refs/heads/$x:${PUSH_BASE[$key]}"
        "--force-with-lease=refs/heads/candidate-$version:"
    )
    [[ "$dry_run" != true ]] || dry_run_option+=(--dry-run)
    git_remote "$directory" push -q "${dry_run_option[@]}" --atomic "${options[@]}" origin "${specs[@]}"
}

# Explicit leases reject a moved .x ref and require the new release branch to remain absent.
for key in "${PUSH_ORDER[@]}"; do
    push_release "$key" true ||
        retryable "Preflight push failed for ${key#*|} on ${key%|*}; the remote changed. Nothing was pushed; rerun."
done
if [[ "$HELM_SHA" != "$HELM_BASE" ]]; then
    git_remote "$HELM_DIR" push -q --dry-run origin "$HELM_SHA:refs/heads/master" ||
        retryable "Preflight push failed for helm-chart. Nothing was pushed; rerun."
fi

# Push in order; the first failure stops every later push.
pushed=0
for key in "${PUSH_ORDER[@]}"; do
    x=${key%|*} id=${key#*|}
    if ! push_release "$key" false; then
        record push-failed "$x" "$id" "Atomic push rejected; this repository is unchanged"
        for rest in "${PUSH_ORDER[@]:$((pushed + 1))}"; do
            record not-pushed "${rest%|*}" "${rest#*|}" "Stopped after an earlier failure"
        done
        [[ "$HELM_SHA" == "$HELM_BASE" ]] || record not-pushed "-" helm-chart "Stopped after an earlier failure"
        retryable "Push failed for $id on $x after $pushed successful push(es). Rerun to resume the release."
    fi
    record released "$x" "$id" "${PLAN_VERSION[$x]}"
    pushed=$((pushed + 1))
done
if [[ "$HELM_SHA" != "$HELM_BASE" ]]; then
    git_remote "$HELM_DIR" push origin "$HELM_SHA:refs/heads/master" || {
        record push-failed "-" helm-chart "Chart push rejected"
        retryable "helm-chart push failed after all releases were pushed. Rerun to republish the charts."
    }
    record released "-" helm-chart "Charts published"
fi
OUTCOME=released
echo "Release completed."
