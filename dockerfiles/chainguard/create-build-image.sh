#!/bin/bash
set -euo pipefail

usage() {
    echo "Usage: $0 -g <glibc>[,<glibc>...]|all [-c <gcc-major>[,<gcc-major>...]|default] [-a|--arch|--aarch amd64|arm64] [-t image-prefix] [-i base-image] [--no-pull]" >&2
    echo "  glibc: 2.34 through 2.44" >&2
    echo "  gcc:   Wolfi GCC major version(s); 'default' uses each baseline's paired GCC (2.34-2.37=11, 2.38-2.41=13, 2.42-2.43=15, 2.44=16)" >&2
    echo "  --no-pull: with a registry-qualified prefix, use a local image or build one instead of pulling the published image" >&2
}

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
source "${SCRIPT_DIR}/container-engine.sh"
source "${SCRIPT_DIR}/toolchain-baselines.sh"

GLIBC=""
GCC=default
ARCH=""
IMAGE_PREFIX=hpcc-chainguard
JFROG_REGISTRY="${JFROG_REGISTRY:-useast.jfrog.lexisnexisrisk.com}"
BASE_IMAGE="${BASE_IMAGE:-${JFROG_REGISTRY}/glb-oci-virtual/lexisnexisrisk.com/gcc-glibc:latest-dev}"
PULL=1

while [[ $# -gt 0 ]]; do
    case "$1" in
        -g|--glibc) GLIBC="$2"; shift 2 ;;
        -c|--gcc) GCC="$2"; shift 2 ;;
        -a|--arch|--aarch) ARCH="$2"; shift 2 ;;
        -t|--tag-prefix) IMAGE_PREFIX="$2"; shift 2 ;;
        -i|--base-image) BASE_IMAGE="$2"; shift 2 ;;
        --no-pull) PULL=0; shift ;;
        -h|--help) usage; exit 0 ;;
        *) usage; exit 1 ;;
    esac
done

if [[ ${GLIBC} == all ]]; then
    GLIBCS=(2.34 2.35 2.36 2.37 2.38 2.39 2.40 2.41 2.42 2.43 2.44)
else
    IFS=, read -r -a GLIBCS <<< "${GLIBC}"
fi
if [[ ${#GLIBCS[@]} -eq 0 ]]; then
    usage; exit 1
fi
for glibc in "${GLIBCS[@]}"; do
    case "${glibc}" in
        2.34|2.35|2.36|2.37|2.38|2.39|2.40|2.41|2.42|2.43|2.44) ;;
        *) echo "Unsupported glibc baseline: ${glibc}" >&2; usage; exit 1 ;;
    esac
done

IFS=, read -r -a GCCS <<< "${GCC}"
if [[ ${#GCCS[@]} -eq 0 ]]; then
    usage; exit 1
fi
for gcc in "${GCCS[@]}"; do
    if [[ ${gcc} != default && ! ${gcc} =~ ^[0-9]+$ ]]; then
        echo "Invalid GCC major version: ${gcc}" >&2; usage; exit 1
    fi
done

if [[ -z "${ARCH}" ]]; then
    ARCH="$(uname -m)"
fi
case "${ARCH}" in
    amd64|x86_64) ARCH=amd64; CONTAINER_PLATFORM=linux/amd64 ;;
    arm64|aarch64) ARCH=arm64; CONTAINER_PLATFORM=linux/arm64 ;;
    *) echo "Unsupported architecture: ${ARCH}" >&2; exit 1 ;;
esac

REGISTRY_AUTHENTICATED=0
jfrog_login() {
    [[ ${REGISTRY_AUTHENTICATED} -eq 0 ]] || return 0
    [[ -n "${JFROG_USERNAME:-}" && -n "${JFROG_PASSWORD:-}" ]] || return 1
    # Called from conditionals, where set -e does not apply.
    printf '%s' "${JFROG_PASSWORD}" | container_login "${JFROG_REGISTRY}" "${JFROG_USERNAME}" >/dev/null \
        || { echo "Login to ${JFROG_REGISTRY} failed" >&2; return 1; }
    REGISTRY_AUTHENTICATED=1
}

# Only a registry-qualified prefix (first component names a host) has a published image to pull.
REGISTRY_PREFIX=0
[[ "${IMAGE_PREFIX}" == */* && ( "${IMAGE_PREFIX%%/*}" == *[.:]* || "${IMAGE_PREFIX%%/*}" == localhost ) ]] && REGISTRY_PREFIX=1

for glibc in "${GLIBCS[@]}"; do
    # Each baseline pairs an Ubuntu libc with the newest GCC its libstdc++ supports.
    chainguard_select_baseline "${glibc}"
    default_major="${CHAINGUARD_DEFAULT_GCC}"
    sysroot_image="${CHAINGUARD_SYSROOT_IMAGE}"
    for gcc in "${GCCS[@]}"; do
        major="${gcc}"
        [[ ${major} == default ]] && major="${default_major}"
        if (( major > default_major )); then
            echo "Warning: GCC ${major} is newer than the glibc ${glibc} sysroot's libstdc++ (GCC ${default_major}); links may fail" >&2
        fi
        image="${IMAGE_PREFIX}-glibc${glibc}-gcc${major}-dev:latest-${ARCH}"
        # Pulling keeps a local "latest" tag current; a failed pull falls back to the local image or a build.
        if [[ ${PULL} -eq 1 && ${REGISTRY_PREFIX} -eq 1 ]]; then
            [[ "${IMAGE_PREFIX}" == "${JFROG_REGISTRY}/"* ]] && { jfrog_login || true; }
            if container_pull "${CONTAINER_PLATFORM}" "${image}"; then
                echo "Using published image ${image}"
                continue
            fi
            echo "Could not pull ${image}; using a local image or building one" >&2
        fi
        if container_image_exists "${image}"; then
            echo "Using existing image ${image}"
            continue
        fi
        if [[ "${BASE_IMAGE}" == "${JFROG_REGISTRY}/"* ]] && ! jfrog_login; then
            echo "Valid JFROG_USERNAME and JFROG_PASSWORD are required in ${ENV_FILE} to use the JFrog base image" >&2
            exit 1
        fi
        container_build --platform "${CONTAINER_PLATFORM}" \
            --build-arg BASE_IMAGE="${BASE_IMAGE}" \
            --build-arg GCC_MAJOR="${major}" \
            --build-arg SYSROOT_IMAGE="${sysroot_image}" \
            --build-arg GLIBC_BASELINE="${glibc}" \
            -f "${SCRIPT_DIR}/containers/Dockerfile.toolchain" \
            -t "${image}" \
            "${SCRIPT_DIR}/containers"
        container_run --rm --platform "${CONTAINER_PLATFORM}" --entrypoint /bin/sh "${image}" -lc \
            "set -e; gcc --version >/dev/null; g++ --version >/dev/null; cat \"\${HPCC_SYSROOT}/hpcc-baseline\"; test -x \"\$(command -v melange)\"; test -x \"\$(command -v ninja)\"; mono /opt/nuget/nuget.exe help >/dev/null"
    done
done
