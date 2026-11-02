#!/bin/bash
set -euo pipefail

usage() {
    printf '%s\n' \
        "Usage: $0 [options] [-- <extra cmake options>]" \
        "" \
        "Options:" \
        "  -s, --hpcc-source-dir <dir>  HPCC-Platform source directory (default: this checkout)" \
        "  --source-root <dir>         CMake project root (default: HPCC-Platform source directory)" \
        "  -g, --glibc <version>       glibc baseline for the builder image (default: 2.34)" \
        "  -c, --gcc <major>           GCC major version (default: baseline's paired version)" \
        "  -a, --arch, --aarch <arch>  Target architecture: x64, amd64, arm64, or aarch64" \
        "                              (default: host architecture)" \
        "  -b <dir>                    Build directory (default: ${CHAINGUARD_BUILD_ROOT:-${REPO_ROOT}/build}/${CHAINGUARD_BUILD_PREFIX:-chainguard-hpcc}," \
        "                              with baseline, architecture, and --containerized suffixes as needed)" \
        "  -o <dir>                    Package output directory (default: build directory)" \
        "  -i <image>                  Use this builder image instead of selecting one by baseline" \
        "  -k <file>                   Sign packages with this signing key" \
        "  -f <formats>                Package formats: apk, deb, rpm (comma-separated), or all" \
        "                              (default: all)" \
        "  --containerized             Enable containerized CMake configuration" \
        "  --no-pull                   Do not pull the selected builder image" \
        "  -h, --help                  Show this help" \
        "" \
        "Use -i without -g or -c. Arguments after -- are passed to CMake." >&2
}

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
source "${SCRIPT_DIR}/container-engine.sh"
source "${SCRIPT_DIR}/toolchain-baselines.sh"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." >/dev/null 2>&1 && pwd)"
WORKSPACE_ROOT="${CHAINGUARD_WORKSPACE_ROOT:-$(dirname "${REPO_ROOT}")}"
[[ -d "${WORKSPACE_ROOT}" ]] || { echo "Workspace directory does not exist: ${WORKSPACE_ROOT}" >&2; exit 1; }
WORKSPACE_ROOT="$(cd "${WORKSPACE_ROOT}" && pwd -P)"
SOURCE_DIR="${REPO_ROOT}"
PROJECT_DIR=""
BUILD_DIR=""
OUT_DIR=""
IMAGE=""
GLIBC=2.34
GCC=default
IMAGE_COMPONENTS_SET=0
SIGNING_KEY=""
ARCH=""
FORMATS=all
CONTAINERIZED=0
NO_PULL=0
CMAKE_OPTIONS=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        -s|--hpcc-source-dir|--source-root|-b|-o|-i|-g|--glibc|-c|--gcc|-k|-a|--arch|--aarch|-f)
            [[ $# -ge 2 && -n "$2" ]] || { echo "Missing value for $1" >&2; usage; exit 1; }
            case "$1" in
                -s|--hpcc-source-dir) SOURCE_DIR="$2" ;;
                --source-root) PROJECT_DIR="$2" ;;
                -b) BUILD_DIR="$2" ;;
                -o) OUT_DIR="$2" ;;
                -i) IMAGE="$2" ;;
                -g|--glibc) GLIBC="$2"; IMAGE_COMPONENTS_SET=1 ;;
                -c|--gcc) GCC="$2"; IMAGE_COMPONENTS_SET=1 ;;
                -k) SIGNING_KEY="$2" ;;
                -a|--arch|--aarch) ARCH="$2" ;;
                -f) FORMATS="$2" ;;
            esac
            shift 2 ;;
        --containerized) CONTAINERIZED=1; shift ;;
        --no-pull) NO_PULL=1; shift ;;
        --) shift; CMAKE_OPTIONS=("$@"); break ;;
        -h|--help) usage; exit 0 ;;
        *) usage; exit 1 ;;
    esac
done

[[ -f "${SOURCE_DIR}/CMakeLists.txt" ]] || { echo "-s must name the HPCC-Platform checkout" >&2; exit 1; }
PROJECT_DIR="${PROJECT_DIR:-${SOURCE_DIR}}"
[[ -f "${PROJECT_DIR}/CMakeLists.txt" ]] || { echo "--source-root must name a CMake project directory" >&2; exit 1; }
[[ "${FORMATS}" =~ ^(all|(apk|deb|rpm)(,(apk|deb|rpm))*)$ ]] || { echo "-f must be a comma-separated list of apk, deb, rpm, or all" >&2; exit 1; }
[[ "${GLIBC}" =~ ^[0-9]+\.[0-9]+$ ]] || { echo "Invalid glibc version: ${GLIBC}" >&2; exit 1; }
[[ "${GCC}" == default || "${GCC}" =~ ^[0-9]+$ ]] || { echo "Invalid GCC major version: ${GCC}" >&2; exit 1; }
[[ "${CHAINGUARD_BUILD_PREFIX:-chainguard-hpcc}" =~ ^[A-Za-z0-9][A-Za-z0-9_.-]*$ ]] || { echo "CHAINGUARD_BUILD_PREFIX must be a single directory name" >&2; exit 1; }
[[ -z "${IMAGE}" || ${IMAGE_COMPONENTS_SET} -eq 0 ]] || { echo "-i cannot be combined with --glibc or --gcc" >&2; exit 1; }
[[ -n "${ARCH}" ]] || ARCH="$(uname -m)"
case "${ARCH}" in
    x64|amd64|x86_64) BUILDER_ARCH=amd64 ;;
    arm64|aarch64) BUILDER_ARCH=arm64 ;;
    *) echo "Unsupported architecture: ${ARCH}" >&2; exit 1 ;;
esac
if [[ "${FORMATS}" == all || ",${FORMATS}," == *,apk,* ]]; then
    host_check_unprivileged_userns || exit 1
fi
if [[ -z "${IMAGE}" ]]; then
    chainguard_select_baseline "${GLIBC}" || { echo "Unsupported glibc baseline: ${GLIBC}" >&2; exit 1; }
    [[ "${GCC}" == default ]] && GCC="${CHAINGUARD_DEFAULT_GCC}"
    export CHAINGUARD_GLIBC="${GLIBC}" CHAINGUARD_GCC="${GCC}"
    unset CHAINGUARD_IMAGE
    IMAGE_DESCRIPTION="glibc${GLIBC}-gcc${GCC}-dev"
else
    export CHAINGUARD_IMAGE="${IMAGE}"
    unset CHAINGUARD_GLIBC CHAINGUARD_GCC
    IMAGE_DESCRIPTION="${IMAGE}"
fi

if [[ -z "${BUILD_DIR}" ]]; then
    BUILD_DIR="${CHAINGUARD_BUILD_ROOT:-${SOURCE_DIR}/build}/${CHAINGUARD_BUILD_PREFIX:-chainguard-hpcc}"
    [[ "${IMAGE_DESCRIPTION}" =~ (glibc[0-9.]+-gcc[0-9]+)-dev ]] && BUILD_DIR+="-${BASH_REMATCH[1]}"
    BUILD_DIR+="-${BUILDER_ARCH}"
    [[ ${CONTAINERIZED} -eq 1 ]] && BUILD_DIR+="-containerized"
fi

BUILD_DIR="$(resolve_path "${BUILD_DIR}")"
OUT_DIR="$(resolve_path "${OUT_DIR:-${BUILD_DIR}}")"
SOURCE_DIR="$(realpath "${SOURCE_DIR}")"
PROJECT_DIR="$(realpath "${PROJECT_DIR}")"
case "${BUILD_DIR}" in "${WORKSPACE_ROOT}"/*) ;; *) echo "Build directory must be within the workspace: ${WORKSPACE_ROOT}" >&2; exit 1 ;; esac
case "${OUT_DIR}" in "${WORKSPACE_ROOT}"/*) ;; *) echo "Output directory must be within the workspace: ${WORKSPACE_ROOT}" >&2; exit 1 ;; esac
EXTRA_MOUNTS="${CHAINGUARD_EXTRA_MOUNTS:-}"
SOURCE_ROOTS=("${REPO_ROOT}")
[[ "${SOURCE_DIR}" == "${REPO_ROOT}" ]] || SOURCE_ROOTS+=("${SOURCE_DIR}")
[[ "${PROJECT_DIR}" == "${REPO_ROOT}" || "${PROJECT_DIR}" == "${SOURCE_DIR}" ]] || SOURCE_ROOTS+=("${PROJECT_DIR}")
for source_root in "${SOURCE_ROOTS[@]}"; do
    case "${source_root}" in
        "${WORKSPACE_ROOT}"/*) ;;
        *) EXTRA_MOUNTS+="${EXTRA_MOUNTS:+$'\n'}type=bind,source=${source_root},target=${source_root}" ;;
    esac
done
if [[ -n "${SIGNING_KEY}" ]]; then
    [[ -f "${SIGNING_KEY}" ]] || { echo "Signing key must name a regular file" >&2; exit 1; }
    SIGNING_KEY="$(realpath "${SIGNING_KEY}")"
    EXTRA_MOUNTS+="${EXTRA_MOUNTS:+$'\n'}type=bind,source=${SIGNING_KEY},target=${SIGNING_KEY},readonly"
fi
mkdir -p "${BUILD_DIR}" "${OUT_DIR}"

# This function runs inside the proxy's container, where the image supplies HPCC_SYSROOT.
build_packages() {
    local PROJECT_DIR="$1" SOURCE_DIR="$2" BUILD_DIR="$3" OUT_DIR="$4"
    local FORMATS="$5" SIGNING_KEY="$6" BUILDER_ARCH="$7" CONTAINERIZED_CMAKE="$8"
    local TOOLING_DIR="$9"
    shift 9
    local CONTAINER_CMD=(cmake --fresh -S "${PROJECT_DIR}" -B "${BUILD_DIR}" -G Ninja
        -DHPCC_SOURCE_DIR="${SOURCE_DIR}" -DCMAKE_BUILD_TYPE=RelWithDebInfo
        -DCONTAINERIZED="${CONTAINERIZED_CMAKE}" -DCPACK_STRIP_FILES=ON -DINCLUDE_PLUGINS=ON -DPLATFORM=ON
        -DVCPKG_FILES_DIR="${BUILD_DIR}/vcpkg-files" -DVCPKG_USE_NINJA=OFF
        -DCPACK_THREADS=0 -DUSE_OPTIONAL=OFF -DSUPPRESS_REMBED=ON -DSUPPRESS_V8EMBED=ON)

    local SYSROOT="${HPCC_SYSROOT:-}" VCPKG_TRIPLET SYSROOT_LINK_FLAGS
    if [[ -n "${SYSROOT}" ]]; then
        [[ "${SYSROOT}" == /opt/hpcc-sysroot ]] || { echo "The vcpkg sysroot triplets expect /opt/hpcc-sysroot, not ${SYSROOT}" >&2; exit 1; }
        # HPCC_GLIBC_BASELINE keys vcpkg's cached binaries per baseline.
        VCPKG_TRIPLET="$([[ ${BUILDER_ARCH} == arm64 ]] && echo arm64 || echo x64)-linux-dynamic-sysroot"
        # GCC needs explicit startfile/library paths; libtool drops --sysroot, so ld also needs rpath-link.
        # Prefer vcpkg's libraries over same-named, older sysroot copies.
        SYSROOT_LINK_FLAGS="--sysroot=${SYSROOT} -B${SYSROOT}/usr/lib -L${SYSROOT}/usr/lib -Wl,-rpath-link,\"${BUILD_DIR}/vcpkg-files/vcpkg_installed/${VCPKG_TRIPLET}/lib:${SYSROOT}/usr/lib\""
        # py3embed must use the sysroot distro's interpreter, headers, and library.
        CONTAINER_CMD+=('-UPython3_*' '-U_Python3_*' -C "${SYSROOT}/hpcc-python.cmake" -DUSE_PYTHON3=ON
            -DCMAKE_SYSROOT="${SYSROOT}"
            -DVCPKG_TARGET_TRIPLET="${VCPKG_TRIPLET}" -DVCPKG_HOST_TRIPLET="${VCPKG_TRIPLET}"
            -DCMAKE_EXE_LINKER_FLAGS="${SYSROOT_LINK_FLAGS}" -DCMAKE_SHARED_LINKER_FLAGS="${SYSROOT_LINK_FLAGS}"
            -DCMAKE_MODULE_LINKER_FLAGS="${SYSROOT_LINK_FLAGS}")
    fi
    CONTAINER_CMD+=("$@")
    "${CONTAINER_CMD[@]}"
    cmake --build "${BUILD_DIR}" --parallel
    local SIGNING_ARGS=()
    [[ -z "${SIGNING_KEY}" ]] || SIGNING_ARGS=(-k "${SIGNING_KEY}")
    # Empty-array guards support macOS's bash 3.2 with set -u.
    "${TOOLING_DIR}/containers/package-install-tree.sh" \
        -b "${BUILD_DIR}" -o "${OUT_DIR}" -f "${FORMATS}" \
        --hpcc-source-dir "${SOURCE_DIR}" ${SIGNING_ARGS[@]+"${SIGNING_ARGS[@]}"}
}

CONTAINERIZED_CMAKE=OFF
[[ ${CONTAINERIZED} -eq 1 ]] && CONTAINERIZED_CMAKE=ON
export CHAINGUARD_ARCH="${BUILDER_ARCH}" CHAINGUARD_NO_PULL="${NO_PULL}" CHAINGUARD_PACKAGING=1
export CHAINGUARD_BUILD_DIR="${BUILD_DIR}" CHAINGUARD_EXTRA_MOUNTS="${EXTRA_MOUNTS}"
exec "${SCRIPT_DIR}/cmake-chainguard.sh" -E env bash -c \
    "set -euo pipefail; $(declare -f build_packages); build_packages \"\$@\"" build-packages \
    "${PROJECT_DIR}" "${SOURCE_DIR}" "${BUILD_DIR}" "${OUT_DIR}" "${FORMATS}" "${SIGNING_KEY}" \
    "${BUILDER_ARCH}" "${CONTAINERIZED_CMAKE}" "${SCRIPT_DIR}" ${CMAKE_OPTIONS[@]+"${CMAKE_OPTIONS[@]}"}