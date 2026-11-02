#!/bin/bash
set -euo pipefail

# All command-line arguments belong to CMake. Proxy settings come from the environment.
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
source "${SCRIPT_DIR}/container-engine.sh"
source "${SCRIPT_DIR}/toolchain-baselines.sh"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." >/dev/null 2>&1 && pwd)"
WORKSPACE_ROOT="${CHAINGUARD_WORKSPACE_ROOT:-$(dirname "${REPO_ROOT}")}"
[[ -d "${WORKSPACE_ROOT}" ]] || { echo "Workspace directory does not exist: ${WORKSPACE_ROOT}" >&2; exit 1; }
WORKSPACE_ROOT="$(cd "${WORKSPACE_ROOT}" && pwd -P)"
IMAGE="${CHAINGUARD_IMAGE:-}"
GLIBC="${CHAINGUARD_GLIBC:-2.34}"
GCC="${CHAINGUARD_GCC:-default}"
ARCH="${CHAINGUARD_ARCH:-$(uname -m)}"
JFROG_REGISTRY="${JFROG_REGISTRY:-useast.jfrog.lexisnexisrisk.com}"
JFROG_NUGET_REPOSITORY="${JFROG_NUGET_REPOSITORY:-hpccpl-nuget-local}"
NUGET_MODE="${NUGET_MODE:-read}"

[[ "${CHAINGUARD_NO_PULL:-0}" =~ ^[01]$ ]] || { echo "CHAINGUARD_NO_PULL must be 0 or 1" >&2; exit 1; }
[[ "${CHAINGUARD_PACKAGING:-0}" =~ ^[01]$ ]] || { echo "CHAINGUARD_PACKAGING must be 0 or 1" >&2; exit 1; }
[[ -z "${IMAGE}" || ( -z "${CHAINGUARD_GLIBC:-}" && -z "${CHAINGUARD_GCC:-}" ) ]] || {
    echo "CHAINGUARD_IMAGE cannot be combined with CHAINGUARD_GLIBC or CHAINGUARD_GCC" >&2; exit 1;
}
[[ "${NUGET_MODE}" =~ ^(off|read|write|readwrite)$ ]] || { echo "NUGET_MODE must be off, read, write, or readwrite" >&2; exit 1; }
[[ "${JFROG_NUGET_REPOSITORY}" =~ ^[A-Za-z0-9._-]+$ ]] || { echo "Invalid JFROG_NUGET_REPOSITORY: ${JFROG_NUGET_REPOSITORY}" >&2; exit 1; }
[[ "${JFROG_REGISTRY}" =~ ^[A-Za-z0-9.-]+(:[0-9]+)?$ ]] || { echo "Invalid JFROG_REGISTRY: ${JFROG_REGISTRY}" >&2; exit 1; }
case "${ARCH}" in
    x64|amd64|x86_64) CONTAINER_PLATFORM=linux/amd64; BUILDER_ARCH=amd64 ;;
    arm64|aarch64) CONTAINER_PLATFORM=linux/arm64; BUILDER_ARCH=arm64 ;;
    *) echo "Unsupported architecture: ${ARCH}" >&2; exit 1 ;;
esac
if [[ -z "${IMAGE}" ]]; then
    chainguard_select_baseline "${GLIBC}" || { echo "Unsupported glibc baseline: ${GLIBC}" >&2; exit 1; }
    [[ "${GCC}" == default ]] && GCC="${CHAINGUARD_DEFAULT_GCC}"
    [[ "${GCC}" =~ ^[0-9]+$ ]] || { echo "Invalid GCC major version: ${GCC}" >&2; exit 1; }
    IMAGE_PREFIX="useast.jfrog.lexisnexisrisk.com/hpccpl-docker-misc-local/chainguard/build-images/hpcc-chainguard"
    IMAGE="${IMAGE_PREFIX}-glibc${GLIBC}-gcc${GCC}-dev:latest-${BUILDER_ARCH}"
    PULL_ARGS=()
    [[ "${CHAINGUARD_NO_PULL:-0}" == 1 ]] && PULL_ARGS=(--no-pull)
    "${SCRIPT_DIR}/create-build-image.sh" --glibc "${GLIBC}" --gcc "${GCC}" \
        --arch "${BUILDER_ARCH}" --tag-prefix "${IMAGE_PREFIX}" ${PULL_ARGS[@]+"${PULL_ARGS[@]}"} >&2
fi
PACKAGING_ARGS=()
if [[ "${CHAINGUARD_PACKAGING:-0}" == 1 ]]; then
    PACKAGING_ARGS=(--privileged)
    PACKAGER_IMAGE="hpcc-chainguard-packager:${BUILDER_ARCH}"
    container_build --platform "${CONTAINER_PLATFORM}" --build-arg BASE_IMAGE="${IMAGE}" \
        -f "${SCRIPT_DIR}/containers/Dockerfile.packager" -t "${PACKAGER_IMAGE}" "${SCRIPT_DIR}/containers" >&2
    IMAGE="${PACKAGER_IMAGE}"
fi

mkdir -p "${HOME}/.cache/vcpkg/chainguard" "${HOME}/.ccache"
MOUNTS=(--mount "type=bind,source=${WORKSPACE_ROOT},target=${WORKSPACE_ROOT}")
case "${PWD}/" in
    "${WORKSPACE_ROOT}/"*) ;;
    *) MOUNTS+=(--mount "type=bind,source=${PWD},target=${PWD}") ;;
esac
# Additional Docker-style mount specifications, one per line.
while IFS= read -r mount; do
    [[ -z "${mount}" ]] || MOUNTS+=(--mount "${mount}")
done <<< "${CHAINGUARD_EXTRA_MOUNTS:-}"

BASELINE_ENV=()
SYSROOT="$(container_image_environment "${IMAGE}" | sed -n 's/^HPCC_SYSROOT=//p')"
if [[ -n "${SYSROOT}" ]]; then
    GLIBC_BASELINE="$(container_image_label "${IMAGE}" org.hpccsystems.glibc-baseline)"
    BASELINE_ENV=(-e "HPCC_GLIBC_BASELINE=${GLIBC_BASELINE}")
fi

NUGET_ENABLED=0
NUGET_SETUP=""
if [[ "${NUGET_MODE}" != off && -n "${JFROG_USERNAME:-}" && -n "${JFROG_PASSWORD:-}" ]]; then
    NUGET_ENABLED=1
    NUGET_SOURCE="https://${JFROG_REGISTRY}/artifactory/api/nuget/v3/${JFROG_NUGET_REPOSITORY}/index.json"
    NUGET_EXE="mono /opt/nuget/nuget.exe"
    # Credentials come from a mounted file, never the host command line.
    NUGET_SETUP="if command -v mono >/dev/null && [[ -f /opt/nuget/nuget.exe ]]; then \
source /tmp/hpcc-nuget/credentials; \
${NUGET_EXE} sources add -Name JFrog -Source ${NUGET_SOURCE} -StorePasswordInClearText -Username \"\${JFROG_USERNAME}\" -Password \"\${JFROG_PASSWORD}\" >/dev/null; \
${NUGET_EXE} setapikey \"\${JFROG_USERNAME}:\${JFROG_PASSWORD}\" -Source ${NUGET_SOURCE} >/dev/null; \
unset JFROG_USERNAME JFROG_PASSWORD; \
export VCPKG_BINARY_SOURCES='clear;default,readwrite;nuget,JFrog,${NUGET_MODE}'; \
else echo 'Builder image has no mono/nuget.exe; using only the local vcpkg binary cache' >&2; fi; "
    echo "vcpkg NuGet binary cache: ${NUGET_SOURCE} (${NUGET_MODE})" >&2
elif [[ "${NUGET_MODE}" != off ]]; then
    echo "JFROG_USERNAME/JFROG_PASSWORD not set; using only the local vcpkg binary cache" >&2
fi

# Package builds use a per-build-directory lock and per-invocation container names.
CONTAINER_SCRATCH_MOUNT=()
BUILD_LOCK_DIR=""
NUGET_SECRETS_DIR=""
CONTAINER_NAME="hpcc-cmake-$$-${RANDOM}"
trap 'trap "" INT TERM; container_remove "${CONTAINER_NAME}" >/dev/null 2>&1 || true; if [[ -n "${NUGET_SECRETS_DIR}" ]]; then rm -f "${NUGET_SECRETS_DIR}/credentials"; rmdir "${NUGET_SECRETS_DIR}"; fi; if [[ -n "${BUILD_LOCK_DIR}" ]]; then rmdir "${BUILD_LOCK_DIR}"; fi' EXIT
if [[ -n "${CHAINGUARD_BUILD_DIR:-}" ]]; then
    BUILD_DIR="$(resolve_path "${CHAINGUARD_BUILD_DIR}")"
    case "${BUILD_DIR}" in "${WORKSPACE_ROOT}"/*) ;; *) echo "Build directory must be within the workspace: ${WORKSPACE_ROOT}" >&2; exit 1 ;; esac
    BUILD_KEY="$(printf '%s' "${BUILD_DIR#${WORKSPACE_ROOT}/}" | tr -c 'A-Za-z0-9_.-' '-')"
    LOCK_CANDIDATE="${BUILD_DIR}.chainguard-lock"
    if ! mkdir "${LOCK_CANDIDATE}" 2>/dev/null; then
        echo "A build is already running in ${BUILD_DIR} (lock ${LOCK_CANDIDATE}); stop it before retrying" >&2
        exit 1
    fi
    BUILD_LOCK_DIR="${LOCK_CANDIDATE}"
    LEGACY_CONTAINER_NAME="hpcc-build-${BUILD_KEY}"
    if container_exists "${LEGACY_CONTAINER_NAME}"; then
        echo "A build is already running in ${BUILD_DIR} (container ${LEGACY_CONTAINER_NAME}); stop it before retrying" >&2
        exit 1
    fi
    CONTAINER_NAME="hpcc-build-${BUILD_KEY}-$$-${RANDOM}"
    container_scratch_mount \
        "${LEGACY_CONTAINER_NAME}-vcpkg-buildtrees" "${BUILD_DIR}/vcpkg-files/vcpkg_buildtrees" \
        "${LEGACY_CONTAINER_NAME}-vcpkg-packages" "${BUILD_DIR}/vcpkg-files/vcpkg_packages"
fi
NUGET_MOUNT=()
if [[ ${NUGET_ENABLED} -eq 1 ]]; then
    NUGET_SECRETS_DIR="$(mktemp -d)"
    chmod 700 "${NUGET_SECRETS_DIR}"
    printf 'JFROG_USERNAME=%q\nJFROG_PASSWORD=%q\n' "${JFROG_USERNAME}" "${JFROG_PASSWORD}" > "${NUGET_SECRETS_DIR}/credentials"
    NUGET_MOUNT=(--mount "type=bind,source=${NUGET_SECRETS_DIR},target=/tmp/hpcc-nuget,readonly")
fi

# Positional parameters preserve every CMake argument without shell interpretation.
container_run --rm --init -e TINI_KILL_PROCESS_GROUP=1 --name "${CONTAINER_NAME}" \
    ${PACKAGING_ARGS[@]+"${PACKAGING_ARGS[@]}"} --platform "${CONTAINER_PLATFORM}" --user "$(id -u):$(id -g)" --entrypoint bash \
    --workdir "${PWD}" "${MOUNTS[@]}" \
    --mount "type=bind,source=${HOME}/.cache/vcpkg/chainguard,target=/tmp/vcpkg-cache" \
    --mount "type=bind,source=${HOME}/.ccache,target=/tmp/ccache" \
    ${NUGET_MOUNT[@]+"${NUGET_MOUNT[@]}"} ${CONTAINER_SCRATCH_MOUNT[@]+"${CONTAINER_SCRATCH_MOUNT[@]}"} ${BASELINE_ENV[@]+"${BASELINE_ENV[@]}"} \
    -e PATH="/usr/bin:/bin:${PATH}" \
    -e HOME=/tmp/hpcc-home -e XDG_CACHE_HOME=/tmp/hpcc-home/.cache \
    -e VCPKG_DEFAULT_BINARY_CACHE=/tmp/vcpkg-cache -e CCACHE_DIR=/tmp/ccache \
    "${IMAGE}" -c "set -euo pipefail; ${NUGET_SETUP}exec cmake \"\$@\"" cmake "$@"
