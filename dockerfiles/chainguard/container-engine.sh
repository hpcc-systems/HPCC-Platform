#!/bin/bash

# Container and host-OS operations used by the build scripts. Keep runtime- and
# OS-specific command syntax here so another backend can be added without changing callers.
#
# A backend NAME is listed in CONTAINER_BACKENDS and may define:
#   NAME_available  auto-detection test (default: the NAME CLI exists and `NAME info` succeeds)
#   NAME_init       set CONTAINER_CLI and check prerequisites (default: CONTAINER_CLI=NAME)
#   NAME_<op>       replacement for any oci_<op> whose Docker-compatible syntax does not fit
# A Docker-compatible engine therefore only needs adding to CONTAINER_BACKENDS.

ENV_FILE="${ENV_FILE:-$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." >/dev/null 2>&1 && pwd)/.env}"
if [[ -f "${ENV_FILE}" ]]; then
    set -a
    # shellcheck disable=SC1090
    source "${ENV_FILE}" || { echo "Failed to parse ${ENV_FILE}; it is sourced by bash, so quote values containing special characters" >&2; exit 1; }
    set +a
fi

# Auto-detection order; override with CONTAINER_RUNTIME. "apple" is Apple's `container` CLI on macOS,
# "wslc" is WSL containers on Windows driven from Git Bash.
CONTAINER_BACKENDS=(docker podman apple wslc)

host_is_windows() { [[ "$(uname -s)" == MINGW* || "$(uname -s)" == MSYS* || "$(uname -s)" == CYGWIN* ]]; }

# Windows CLIs (wslc.exe, podman.exe) run from Git Bash take Windows paths and must not have MSYS rewrite
# container paths such as /tmp/... in their arguments.
win_host_path() {
    cygpath -w "$1"
}

# Rewrite the source of a bind --mount spec to a Windows path.
win_mount_spec() {
    local spec="$1" field out=() fields
    [[ ",${spec}," == *,type=volume,* ]] && { printf '%s\n' "${spec}"; return; }
    IFS=, read -r -a fields <<< "${spec}"
    for field in "${fields[@]}"; do
        case "${field}" in
            source=*|src=*) field="${field%%=*}=$(win_host_path "${field#*=}")" ;;
        esac
        out+=("${field}")
    done
    (IFS=,; printf '%s\n' "${out[*]}")
}

# Rewrite build arguments' Dockerfile and final context path to Windows paths.
win_build_args() {
    WIN_BUILD_ARGS=()
    while [[ $# -gt 0 ]]; do
        case "$1" in
            -f|--file) WIN_BUILD_ARGS+=("$1" "$(win_host_path "$2")"); shift ;;
            *) WIN_BUILD_ARGS+=("$1") ;;
        esac
        shift
    done
    WIN_BUILD_ARGS[${#WIN_BUILD_ARGS[@]}-1]="$(win_host_path "${WIN_BUILD_ARGS[${#WIN_BUILD_ARGS[@]}-1]}")"
}

# Windows file handles can briefly block directory renames over a VM file share (vcpkg extraction/install),
# so keep each VOLUME TARGET pair in a named volume instead.
volume_scratch_mount() {
    while (( $# >= 2 )); do
        "${CONTAINER_CLI}" volume inspect "$1" >/dev/null 2>&1 || "${CONTAINER_CLI}" volume create "$1" >/dev/null || return
        CONTAINER_SCRATCH_MOUNT+=(--mount "type=volume,source=$1,target=$2")
        shift 2
    done
}

# Portable `realpath -m` (macOS realpath has no -m): resolve the existing prefix, normalise the rest lexically.
resolve_path() {
    local path="$1" rest=() part out
    [[ "${path}" == /* ]] || path="${PWD}/${path}"
    while [[ ! -e "${path}" ]]; do
        rest=("$(basename "${path}")" ${rest[@]+"${rest[@]}"})
        path="$(dirname "${path}")"
    done
    out="$(cd "${path}" && pwd -P)"
    for part in ${rest[@]+"${rest[@]}"}; do
        case "${part}" in
            .|'') ;;
            ..) out="$(dirname "${out}")" ;;
            *) out="${out%/}/${part}" ;;
        esac
    done
    printf '%s\n' "${out}"
}

# melange packages APKs inside bubblewrap, which needs unprivileged user namespaces on a Linux host
# (Ubuntu 23.10+ restricts them via AppArmor). Fail now rather than after the full build.
host_check_unprivileged_userns() {
    # VM-backed runtimes (Apple, wslc) run on non-Linux hosts, which have no such sysctl.
    [[ "$(cat /proc/sys/kernel/apparmor_restrict_unprivileged_userns 2>/dev/null)" == 1 ]] || return 0
    echo "APK packaging needs unprivileged user namespaces for melange's bubblewrap runner." >&2
    echo "Run: sudo sysctl -w kernel.apparmor_restrict_unprivileged_userns=0" >&2
    return 1
}

# ---- Docker-compatible defaults, run against ${CONTAINER_CLI} ----

# A CLI without a running daemon (e.g. Docker Desktop stopped) should not shadow the other backends.
oci_available() { command -v "$1" >/dev/null 2>&1 && command "$1" info >/dev/null 2>&1; }
oci_init() { CONTAINER_CLI="${CONTAINER_RUNTIME}"; }
oci_login() { "${CONTAINER_CLI}" login --username "$2" --password-stdin "$1"; }
oci_build() { "${CONTAINER_CLI}" build "$@"; }
oci_run() { "${CONTAINER_CLI}" run "$@"; }
oci_pull() { "${CONTAINER_CLI}" pull --platform "$1" "$2"; }
oci_image_exists() { "${CONTAINER_CLI}" image inspect "$1" >/dev/null 2>&1; }
oci_image_tags() {
    local images
    images="$("${CONTAINER_CLI}" image ls --format '{{.Repository}} {{.Tag}}' --filter "reference=$1:*")" || return
    printf '%s\n' "${images}" | awk -v repository="$1" \
        'BEGIN { sub(/^docker.io\//, "", repository) }
         {
             image_repository = $1
             sub(/^docker.io\//, "", image_repository)
             if (image_repository == repository || image_repository == "localhost/" repository) {
                 print $2
             }
         }'
}
oci_image_environment() { "${CONTAINER_CLI}" image inspect --format '{{range .Config.Env}}{{println .}}{{end}}' "$1"; }
oci_image_label() { "${CONTAINER_CLI}" image inspect --format "{{index .Config.Labels \"$2\"}}" "$1"; }
oci_image_save() { "${CONTAINER_CLI}" save --output "$2" "$1"; }
oci_exists() { "${CONTAINER_CLI}" container inspect "$1" >/dev/null 2>&1; }
oci_remove() { "${CONTAINER_CLI}" container rm --force "$1"; }
oci_exec() { "${CONTAINER_CLI}" exec "$@"; }
oci_commit() { "${CONTAINER_CLI}" commit "$@"; }
oci_tag() { "${CONTAINER_CLI}" tag "$@"; }
oci_volume() { "${CONTAINER_CLI}" volume "$@"; }
oci_cpus() { "${CONTAINER_CLI}" info --format '{{.NCPU}}'; }
oci_scratch_mount() { :; }

# ---- podman: Podman on Linux, or podman.exe with a Podman machine on Windows (driven from Git Bash) ----

podman_cli() {
    if host_is_windows; then
        MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL='*' command podman "$@"
    else
        command podman "$@"
    fi
}

podman_init() {
    CONTAINER_CLI=podman_cli
    command -v podman >/dev/null 2>&1 || { echo "podman not found" >&2; exit 1; }
    if host_is_windows; then
        command -v cygpath >/dev/null 2>&1 || { echo "The podman backend on Windows must be run from Git Bash (cygpath not found)" >&2; exit 1; }
        PODMAN_ROOTLESS=false
    else
        PODMAN_ROOTLESS="$(podman_cli info --format '{{.Host.Security.Rootless}}' 2>/dev/null || echo false)"
    fi
}

podman_cpus() { podman_cli info --format '{{.Host.CPUs}}'; }

podman_image_save() {
    local archive="$2"
    if host_is_windows; then
        archive="$(win_host_path "${archive}")" || return
    fi
    podman_cli save --output "${archive}" "$1"
}

podman_build() {
    if host_is_windows; then
        win_build_args "$@"
        podman_cli build "${WIN_BUILD_ARGS[@]}"
    else
        podman_cli build "$@"
    fi
}

podman_run() {
    local args=() arg host_user=0
    for arg in "$@"; do
        case "${arg}" in
            --user|-u|--user=*|-u=*) host_user=1 ;;
        esac
    done
    # Keep the image's configured identity unless this run explicitly supplies a host user.
    [[ "${PODMAN_ROOTLESS}" == true && ${host_user} -eq 1 ]] && args+=(--userns=keep-id)
    if ! host_is_windows; then
        podman_cli run ${args[@]+"${args[@]}"} "$@"
        return
    fi
    # The Podman machine's Windows file share shows host files as owned by the machine user, so a host --user
    # (a Git Bash UID) could not write them.
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --user=*) ;;
            --user|-u) shift ;;
            --mount) args+=("$1" "$(win_mount_spec "$2")"); shift ;;
            --mount=*) args+=("--mount=$(win_mount_spec "${1#--mount=}")") ;;
            *) args+=("$1") ;;
        esac
        shift
    done
    podman_cli run "${args[@]}"
}

podman_scratch_mount() {
    if host_is_windows; then
        volume_scratch_mount "$@"
    fi
}

# ---- apple: Apple's `container` CLI (macOS) ----

apple_available() { [[ "$(uname -s)" == Darwin ]] && command -v container >/dev/null 2>&1; }

apple_init() {
    CONTAINER_CLI=container
    command -v container >/dev/null 2>&1 || { echo "Apple container CLI not found; install it from https://github.com/apple/container" >&2; exit 1; }
    command -v jq >/dev/null 2>&1 || { echo "jq is required for the Apple container backend" >&2; exit 1; }
}

apple_login() { command container registry login --username "$2" --password-stdin "$1"; }
apple_pull() { command container image pull --platform "$1" "$2"; }

apple_run() {
    # Each Apple container is its own VM, so --privileged is unnecessary (and unsupported).
    local args=() arg dns
    # Apple defaults to 4 CPUs/1 GiB per container VM, far too little for C++ builds.
    args+=(--cpus "${CONTAINER_CPUS:-$(sysctl -n hw.ncpu)}")
    args+=(--memory "${CONTAINER_MEMORY:-$(( $(sysctl -n hw.memsize) * 3 / 4 / 1048576 ))M}")
    # The vmnet gateway resolver often fails behind a VPN, so use the host's resolvers (override with CONTAINER_DNS).
    for dns in ${CONTAINER_DNS:-$(scutil --dns 2>/dev/null | awk '/nameserver\[[0-9]+\]/ && !seen[$3]++ {print $3}')}; do
        args+=(--dns "${dns}")
    done
    for arg in "$@"; do
        case "${arg}" in
            --privileged) ;;
            *) args+=("${arg}") ;;
        esac
    done
    command container run "${args[@]}"
}

apple_image_environment() {
    command container image inspect "$1" \
        | jq -r '[.. | objects | select(has("Env")) | .Env // [] | .[]] | unique | .[]'
}

apple_image_label() {
    command container image inspect "$1" \
        | jq -r --arg label "$2" 'first(.. | objects | select(has("Labels")) | .Labels // {} | .[$label] // empty) // ""'
}

apple_exists() { command container list --all --quiet 2>/dev/null | grep -qxF -- "$1"; }
apple_remove() { command container delete --force "$1"; }

# ---- wslc: WSL containers (Windows, driven from Git Bash) ----

wslc() {
    MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL='*' command wslc.exe "$@"
}

wslc_available() { command -v wslc.exe >/dev/null 2>&1; }

wslc_init() {
    CONTAINER_CLI=wslc
    command -v wslc.exe >/dev/null 2>&1 || { echo "wslc.exe not found; run 'wsl --update' (WSL 2.9.3 or later)" >&2; exit 1; }
    command -v cygpath >/dev/null 2>&1 || { echo "The wslc backend must be run from Git Bash (cygpath not found)" >&2; exit 1; }
}

wslc_build() {
    # wslc only builds for the host architecture and has no --platform option.
    local args=()
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --platform) shift ;;
            --platform=*) ;;
            *) args+=("$1") ;;
        esac
        shift
    done
    win_build_args "${args[@]}"
    wslc build "${WIN_BUILD_ARGS[@]}"
}

wslc_run() {
    # Each wslc session is its own VM; it has no --privileged, --init or --platform options.
    # virtiofs shows files without Linux metadata as root-owned, so a host --user could not chmod them.
    local args=() dns
    # The session VM (settings.yaml) caps every container; these only narrow it when set.
    [[ -n "${CONTAINER_CPUS:-}" ]] && args+=(--cpus "${CONTAINER_CPUS}")
    [[ -n "${CONTAINER_MEMORY:-}" ]] && args+=(--memory "${CONTAINER_MEMORY}")
    # Consommé resolves through Windows (VPN-aware), so DNS servers are only passed when overridden.
    for dns in ${CONTAINER_DNS:-}; do
        args+=(--dns "${dns}")
    done
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --privileged|--init|--platform=*|--user=*) ;;
            --platform|--user|-u) shift ;;
            --mount) args+=("$1" "$(win_mount_spec "$2")"); shift ;;
            --mount=*) args+=("--mount=$(win_mount_spec "${1#--mount=}")") ;;
            *) args+=("$1") ;;
        esac
        shift
    done
    wslc run "${args[@]}"
}

# wslc has no --format; it prints Docker-style indented JSON, one array element per line.
wslc_image_environment() {
    wslc image inspect "$1" | tr -d '\r' | awk '
        /^ *"Env": \[/ { inenv = 1; next }
        inenv && /^ *\]/ { inenv = 0 }
        inenv { sub(/^ *"/, ""); sub(/",?$/, ""); gsub(/\\"/, "\""); gsub(/\\\\/, "\\"); print }'
}

wslc_image_label() {
    wslc image inspect "$1" | tr -d '\r' | awk -v key="\"$2\": " '
        /^ *"Labels": \{/ { inlabels = 1; next }
        inlabels && /^ *\}/ { exit }
        inlabels && index($0, key) {
            value = substr($0, index($0, key) + length(key))
            sub(/^"/, "", value); sub(/",?$/, "", value)
            gsub(/\\"/, "\"", value); gsub(/\\\\/, "\\", value); print value; exit }'
}

wslc_remove() { wslc container remove --force "$1"; }
wslc_pull() { wslc pull "$2"; } # host architecture only

wslc_scratch_mount() { volume_scratch_mount "$@"; }

# ---- Public API used by the build scripts ----

# Run BACKEND_OP if the backend defines it, else the Docker-compatible oci_OP.
container_backend_call() {
    local backend="$1" op="$2"
    shift 2
    if declare -F "${backend}_${op}" >/dev/null; then
        "${backend}_${op}" "$@"
    else
        "oci_${op}" "$@"
    fi
}

container_login() { container_backend_call "${CONTAINER_RUNTIME}" login "$@"; }                         # REGISTRY USERNAME, password on stdin
container_build() { container_backend_call "${CONTAINER_RUNTIME}" build "$@"; }                         # docker build args
container_run() { container_backend_call "${CONTAINER_RUNTIME}" run "$@"; }                             # docker run args
container_pull() { container_backend_call "${CONTAINER_RUNTIME}" pull "$@"; }                           # PLATFORM IMAGE
container_image_exists() { container_backend_call "${CONTAINER_RUNTIME}" image_exists "$@"; }           # IMAGE
container_image_tags() { container_backend_call "${CONTAINER_RUNTIME}" image_tags "$@"; }               # REPOSITORY -> tag lines
container_image_environment() { container_backend_call "${CONTAINER_RUNTIME}" image_environment "$@"; } # IMAGE -> KEY=VALUE lines
container_image_label() { container_backend_call "${CONTAINER_RUNTIME}" image_label "$@"; }             # IMAGE LABEL -> value
container_image_save() { container_backend_call "${CONTAINER_RUNTIME}" image_save "$@"; }               # IMAGE ARCHIVE
container_exists() { container_backend_call "${CONTAINER_RUNTIME}" exists "$@"; }                       # NAME
container_remove() { container_backend_call "${CONTAINER_RUNTIME}" remove "$@"; }                       # NAME
container_exec() { container_backend_call "${CONTAINER_RUNTIME}" exec "$@"; }                           # exec args
container_commit() { container_backend_call "${CONTAINER_RUNTIME}" commit "$@"; }                       # CONTAINER IMAGE
container_tag() { container_backend_call "${CONTAINER_RUNTIME}" tag "$@"; }                             # SOURCE TARGET
container_volume() { container_backend_call "${CONTAINER_RUNTIME}" volume "$@"; }                       # volume subcommand and args
container_cpus() { container_backend_call "${CONTAINER_RUNTIME}" cpus "$@"; }                           # engine CPU count

# Sets CONTAINER_SCRATCH_MOUNT to mount args that keep each VOLUME TARGET pair off the host bind mount, or to nothing.
container_scratch_mount() {
    CONTAINER_SCRATCH_MOUNT=()
    container_backend_call "${CONTAINER_RUNTIME}" scratch_mount "$@"
}

# ---- Backend selection ----

if [[ -z "${CONTAINER_RUNTIME:-}" ]]; then
    CONTAINER_RUNTIME=docker
    for _backend in "${CONTAINER_BACKENDS[@]}"; do
        if container_backend_call "${_backend}" available "${_backend}"; then
            CONTAINER_RUNTIME="${_backend}"
            break
        fi
    done
    unset _backend
fi
if [[ " ${CONTAINER_BACKENDS[*]} " != *" ${CONTAINER_RUNTIME} "* ]]; then
    echo "Unsupported CONTAINER_RUNTIME: ${CONTAINER_RUNTIME} (expected one of: ${CONTAINER_BACKENDS[*]})" >&2
    exit 1
fi
container_backend_call "${CONTAINER_RUNTIME}" init
echo "Container runtime: ${CONTAINER_RUNTIME}" >&2