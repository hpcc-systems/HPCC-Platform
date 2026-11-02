#!/bin/bash
set -euo pipefail

usage() {
    echo "Usage: $0 -b <build-dir> [-o <out-dir>] [-s <staging-dir>] [-k <signing-key.rsa>] [-f apk,deb,rpm|all] [--hpcc-source-dir <dir>]" >&2
}

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
BUILD_DIR=""
OUT_DIR=""
STAGING_DIR=""
SIGNING_KEY=""
FORMATS=apk
HPCC_SOURCE_DIR=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        -b) BUILD_DIR="$2"; shift 2 ;;
        -o) OUT_DIR="$2"; shift 2 ;;
        -s) STAGING_DIR="$2"; shift 2 ;;
        -k) SIGNING_KEY="$2"; shift 2 ;;
        -f) FORMATS="$2"; shift 2 ;;
        --hpcc-source-dir) HPCC_SOURCE_DIR="$2"; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) usage; exit 1 ;;
    esac
done

[[ -n "${BUILD_DIR}" && -f "${BUILD_DIR}/CMakeCache.txt" ]] || {
    echo "A configured CMake build directory is required" >&2
    exit 1
}
[[ "${FORMATS}" == all ]] && FORMATS=apk,deb,rpm
if [[ -z "${HPCC_SOURCE_DIR}" ]]; then
    HPCC_SOURCE_DIR="$(sed -n 's/^HPCC_SOURCE_DIR:[A-Z]*=//p' "${BUILD_DIR}/CMakeCache.txt")"
fi
if [[ -z "${HPCC_SOURCE_DIR}" ]]; then
    HPCC_SOURCE_DIR="$(sed -n 's/^CMAKE_HOME_DIRECTORY:INTERNAL=//p' "${BUILD_DIR}/CMakeCache.txt")"
fi
[[ -d "${HPCC_SOURCE_DIR}/cmake_modules/dependencies" ]] || {
    echo "HPCC dependency metadata not found; supply --hpcc-source-dir <HPCC-Platform checkout>" >&2
    exit 1
}
BUILD_APK=0
BUILD_DEB=0
BUILD_RPM=0
IFS=, read -ra FORMAT_LIST <<< "${FORMATS}"
for format in "${FORMAT_LIST[@]}"; do
    case "${format}" in
        apk) BUILD_APK=1 ;;
        deb) BUILD_DEB=1 ;;
        rpm) BUILD_RPM=1 ;;
        *) echo "Unsupported package format: ${format}" >&2; exit 1 ;;
    esac
done

if [[ ${BUILD_APK} -eq 1 ]]; then
    command -v melange >/dev/null || { echo "melange is required" >&2; exit 1; }
fi
if [[ ${BUILD_DEB} -eq 1 || ${BUILD_RPM} -eq 1 ]]; then
    command -v nfpm >/dev/null || { echo "nfpm is required" >&2; exit 1; }
fi
command -v syft >/dev/null || { echo "syft is required" >&2; exit 1; }

OUT_DIR="${OUT_DIR:-${BUILD_DIR}}"
STAGING_DIR="${STAGING_DIR:-${BUILD_DIR}/staging}"
mkdir -p "${OUT_DIR}" "${STAGING_DIR}"

extract_cpack_var() {
    awk -v key="$1" '$0 ~ "^set\\(" key " " && match($0, /"[^"]+"/) { print substr($0, RSTART + 1, RLENGTH - 2); exit }' "${BUILD_DIR}/CPackConfig.cmake"
}

PKG_NAME="$(extract_cpack_var CPACK_PACKAGE_NAME || true)"
PKG_VERSION="$(extract_cpack_var CPACK_RPM_PACKAGE_VERSION || true)"
PKG_RELEASE="$(extract_cpack_var CPACK_RPM_PACKAGE_RELEASE || true)"
PKG_MAINTAINER="$(extract_cpack_var CPACK_PACKAGE_CONTACT || true)"
PKG_NAME="${PKG_NAME:-hpccsystems-platform}"
PKG_VERSION="${PKG_VERSION:-0.0.0}"
PKG_VERSION="${PKG_VERSION//_/.}"
PKG_RELEASE="${PKG_RELEASE:-1}"
DEB_VERSION="${PKG_VERSION}"
DEB_RELEASE="${PKG_RELEASE}"
if [[ "${PKG_RELEASE}" =~ ^rc[0-9]+$ ]]; then
    # Debian sorts "~rcN" before the gold release; RPM and APK keep their original release.
    DEB_VERSION+="~${PKG_RELEASE}"
    DEB_RELEASE=1
fi
# APK releases (-rN) must be numeric, so pre-release maturities become version suffixes that apk orders
# before the gold release: trunk -> _alpha, closedown -> _beta, rc -> _rc.
APK_STAGE="${PKG_RELEASE%Debug}"
if [[ "${APK_STAGE}" =~ ^[0-9]+$ ]]; then
    APK_VERSION="${PKG_VERSION}"; APK_EPOCH="${APK_STAGE}"
elif [[ "${APK_STAGE}" =~ ^(rc|closedown|trunk)([0-9]+)$ ]]; then
    case "${BASH_REMATCH[1]}" in rc) suffix=rc ;; closedown) suffix=beta ;; trunk) suffix=alpha ;; esac
    APK_VERSION="${PKG_VERSION}_${suffix}${BASH_REMATCH[2]}"; APK_EPOCH=0
else
    echo "Unsupported package release for APK versioning: ${PKG_RELEASE}" >&2; exit 1
fi
APK_FULL_VERSION="${APK_VERSION}-r${APK_EPOCH}"
PKG_MAINTAINER="${PKG_MAINTAINER:-HPCCSystems <ossdevelopment@lexisnexis.com>}"

case "$(uname -m)" in
    x86_64) APK_ARCH=x86_64; NFPM_ARCH=amd64 ;;
    aarch64|arm64) APK_ARCH=aarch64; NFPM_ARCH=arm64 ;;
    *) echo "Unsupported architecture: $(uname -m)" >&2; exit 1 ;;
esac

STAGE_ROOT="${STAGING_DIR}/stage-rootfs"
# Keep Melange's workspace outside its source tree to avoid recursively copying it.
WORK_DIR="${STAGING_DIR}.melange-work"
MELANGE_CONFIG="${STAGING_DIR}/melange.generated.yaml"
MELANGE_KEYRING="/etc/apk/keys/wolfi-signing.rsa.pub"
rm -rf "${STAGE_ROOT}" "${WORK_DIR}" "${MELANGE_CONFIG}"
mkdir -p "${STAGE_ROOT}" "${WORK_DIR}"

CPACK_STRIP_FILES="$(sed -n 's/^CPACK_STRIP_FILES:[^=]*=//p' "${BUILD_DIR}/CMakeCache.txt")"
INSTALL_ARGS=()
[[ "${CPACK_STRIP_FILES}" =~ ^(ON|TRUE|1)$ ]] && INSTALL_ARGS+=(--strip)
DESTDIR="${STAGE_ROOT}" cmake --install "${BUILD_DIR}" ${INSTALL_ARGS[@]+"${INSTALL_ARGS[@]}"}
if [[ -f "${HPCC_SYSROOT:-}/hpcc-baseline" ]]; then
    mkdir -p "${STAGE_ROOT}/var/lib/HPCCSystems"
    cp "${HPCC_SYSROOT}/hpcc-baseline" "${STAGE_ROOT}/var/lib/HPCCSystems/"
fi
mkdir -p "${STAGE_ROOT}/opt/HPCCSystems/lib"
SONAME_LINKS="${STAGE_ROOT}/opt/HPCCSystems/lib/soname-links.txt"
: > "${SONAME_LINKS}"
while IFS= read -r -d '' library; do
    soname="$(readelf -d "${library}" 2>/dev/null | awk -F'[][]' '/SONAME/ {print $2; exit}' || true)"
    [[ -n "${soname}" && "$(basename "${library}")" != "${soname}" ]] && printf '%s %s\n' "${soname}" "$(basename "${library}")" >> "${SONAME_LINKS}"
done < <(find "${STAGE_ROOT}/opt/HPCCSystems" -type f -name '*.so*' -print0)

RUNTIME_SBOM="${OUT_DIR}/${PKG_NAME}-${APK_FULL_VERSION}.runtime.spdx.json"
syft scan "dir:${STAGE_ROOT}" --source-name "${PKG_NAME}" --source-version "${APK_FULL_VERSION}" --output spdx-json > "${RUNTIME_SBOM}"
VCPKG_PACKAGES_DIR="${BUILD_DIR}/vcpkg-files/vcpkg_packages"
[[ -d "${VCPKG_PACKAGES_DIR}" ]] || VCPKG_PACKAGES_DIR="${BUILD_DIR}/vcpkg_packages"
[[ -d "${VCPKG_PACKAGES_DIR}" ]] || { echo "vcpkg package provenance directory not found" >&2; exit 1; }
RUNTIME_VCPKG_SBOM="${OUT_DIR}/${PKG_NAME}-${APK_FULL_VERSION}.runtime-vcpkg.spdx.json"
python3 "${SCRIPT_DIR}/generate-vcpkg-sbom.py" --stage-root "${STAGE_ROOT}" --vcpkg-packages-dir "${VCPKG_PACKAGES_DIR}" --hpcc-package-name "${PKG_NAME}" --hpcc-package-version "${APK_FULL_VERSION}" --output "${RUNTIME_VCPKG_SBOM}"
mkdir -p "${STAGE_ROOT}/var/lib/db/sbom"
cp "${RUNTIME_SBOM}" "${RUNTIME_VCPKG_SBOM}" "${STAGE_ROOT}/var/lib/db/sbom/"

ELF_INFO="${WORK_DIR}/elf-deps.txt"
{ find "${STAGE_ROOT}" -type f \( -name '*.so*' -o -perm -u+x \) -print0 | xargs -0 -r readelf -dV --wide 2>/dev/null || true; } | awk '
    /\(NEEDED\)/ && match($0, /\[[^]]+\]/) { print "NEEDED " substr($0, RSTART + 1, RLENGTH - 2) }
    /\(SONAME\)/ && match($0, /\[[^]]+\]/) { print "PROVIDED " substr($0, RSTART + 1, RLENGTH - 2) }
    /Version: [0-9]+ +File: / { for (i = 1; i <= NF; i++) if ($i == "File:") lib = $(i + 1) }
    /Name: [^ ]+ +Flags: / { for (i = 1; i <= NF; i++) if ($i == "Name:") print "VERNEED " lib " " $(i + 1) }
' | sort -u > "${ELF_INFO}"

GLIBC_BASELINE=""
if [[ -f "${HPCC_SYSROOT:-}/hpcc-baseline" ]]; then
    GLIBC_BASELINE="$(sed -n 's/^GLIBC=//p' "${HPCC_SYSROOT}/hpcc-baseline")"
    GLIBCXX_BASELINE="$(sed -n 's/^GLIBCXX=//p' "${HPCC_SYSROOT}/hpcc-baseline")"
    too_new="$(awk -v glibc="${GLIBC_BASELINE}" -v glibcxx="${GLIBCXX_BASELINE}" '
        function newer(v, max,   a, b, i) {
            split(v, a, "."); split(max, b, ".")
            for (i = 1; i <= 3; i++) if ((a[i] + 0) != (b[i] + 0)) return (a[i] + 0) > (b[i] + 0)
            return 0
        }
        $1 == "VERNEED" && $3 ~ /^GLIBC_[0-9]/ { v = $3; sub(/^GLIBC_/, "", v); if (newer(v, glibc)) print $2 " " $3 }
        $1 == "VERNEED" && $3 ~ /^GLIBCXX_[0-9]/ { v = $3; sub(/^GLIBCXX_/, "", v); if (newer(v, glibcxx)) print $2 " " $3 }
    ' "${ELF_INFO}")"
    if [[ -n "${too_new}" ]]; then
        echo "Binaries require symbols newer than the glibc ${GLIBC_BASELINE} / GLIBCXX ${GLIBCXX_BASELINE} baseline:" >&2
        echo "${too_new}" >&2
        exit 1
    fi
fi

# File-name label: glibc baseline and CPack's _k8s marker for containerised builds.
PKG_LABEL="${GLIBC_BASELINE:+glibc${GLIBC_BASELINE}}"
if grep -qE '^CONTAINERIZED:BOOL=(ON|TRUE|1)$' "${BUILD_DIR}/CMakeCache.txt"; then
    PKG_LABEL+="${PKG_LABEL:+_}k8s"
fi

yaml_list() {
    local item
    for item in "$@"; do
        printf '    - "%s"\n' "${item}"
    done
}

build_native_packages() {
    local elf_info="${ELF_INFO}"
    local nfpm_config="${STAGING_DIR}/nfpm.generated.yaml"
    local nfpm_deb_config="${STAGING_DIR}/nfpm-deb.generated.yaml"
    local install_root="${STAGE_ROOT}/opt/HPCCSystems"

    # Plugin runtimes (Java, Python) are optional on the target host, matching the per-distro packages.
    local external_libs
    mapfile -t external_libs < <(comm -23 \
        <(awk '$1 == "NEEDED" { print $2 }' "${elf_info}" | sort -u) \
        <({ awk '$1 == "PROVIDED" { print $2 }' "${elf_info}"; find "${STAGE_ROOT}" -name '*.so*' | sed 's#.*/##'; } | sort -u) \
        | grep -Ev '^(libjvm\.so|libpython[0-9.]+\.so)' || true)

    local glibc_min gcc_major lib
    glibc_min="$(awk '$1 == "VERNEED" && $3 ~ /^GLIBC_[0-9]/ { sub(/^GLIBC_/, "", $3); print $3 }' "${elf_info}" | sort -V | tail -1)"
    gcc_major="$(gcc -dumpversion | cut -d. -f1)"

    local deb_depends=() rpm_depends=()
    for metadata in noble el8; do
        [[ -f "${HPCC_SOURCE_DIR}/cmake_modules/dependencies/${metadata}.cmake" ]] || {
            echo "Missing dependency metadata: ${HPCC_SOURCE_DIR}/cmake_modules/dependencies/${metadata}.cmake" >&2
            exit 1
        }
    done
    read -ra deb_depends <<< "$(sed -n 's/^SET_DEPENDENCIES *( *CPACK_DEBIAN_PACKAGE_DEPENDS \(.*\))\s*$/\1/p' "${HPCC_SOURCE_DIR}/cmake_modules/dependencies/noble.cmake")"
    read -ra rpm_depends <<< "$(sed -n 's/^SET_DEPENDENCIES *( *CPACK_RPM_PACKAGE_REQUIRES \(.*\))\s*$/\1/p' "${HPCC_SOURCE_DIR}/cmake_modules/dependencies/el8.cmake")"

    for lib in "${external_libs[@]}"; do
        case "${lib}" in
            libc.so.*|libm.so.*|libdl.so.*|libpthread.so.*|librt.so.*|libresolv.so.*|ld-linux*) deb_depends+=("libc6 (>= ${glibc_min})") ;;
            libstdc++.so.*) deb_depends+=("libstdc++6 (>= ${gcc_major})") ;;
            libgcc_s.so.*) deb_depends+=(libgcc-s1) ;;
            libuuid.so.*) deb_depends+=(libuuid1) ;;
            libcrypt.so.*) deb_depends+=(libcrypt1) ;;
            *) echo "No Debian package mapping for external library ${lib}" >&2; exit 1 ;;
        esac
        rpm_depends+=("${lib}()(64bit)")
    done
    mapfile -t deb_depends < <(printf '%s\n' "${deb_depends[@]}" | awk '!seen[$0]++')
    mapfile -t -O "${#rpm_depends[@]}" rpm_depends < <(awk -v libs=" ${external_libs[*]} " \
        '$1 == "VERNEED" && index(libs, " " $2 " ") { print $2 "(" $3 ")(64bit)" }' "${elf_info}")

    local scripts_dir="${BUILD_DIR}/oss/initfiles"
    [[ -d "${scripts_dir}" ]] || scripts_dir="${BUILD_DIR}/initfiles"

    {
        cat <<EOF
name: ${PKG_NAME}
arch: ${NFPM_ARCH}
platform: linux
version: ${PKG_VERSION}
release: ${PKG_RELEASE}
version_schema: none
maintainer: "${PKG_MAINTAINER}"
vendor: HPCC Systems
description: HPCC Systems platform package built from local CMake artifacts
license: Apache-2.0
homepage: https://hpccsystems.com
deb:
  compression: zstd
rpm:
  compression: zstd
EOF
        if [[ -f "${scripts_dir}/bash/sbin/deb/postinst" ]]; then
            cat <<EOF
scripts:
  postinstall: "${scripts_dir}/bash/sbin/deb/postinst"
  preremove: "${scripts_dir}/sbin/prerm"
  postremove: "${scripts_dir}/bash/sbin/deb/postrm"
EOF
        fi
        cat <<EOF
contents:
  - src: "${install_root}"
    dst: /opt/HPCCSystems
    type: tree
EOF
        local path soname library
        while IFS= read -r -d '' path; do
            printf '  - src: "%s"\n    dst: "/%s"\n' "${path}" "${path#"${STAGE_ROOT}"/}"
        done < <(find "${STAGE_ROOT}" -path "${install_root}" -prune -o \( -type f -o -type l \) -print0)
        while IFS=' ' read -r soname library; do
            [[ -e "${install_root}/lib/${soname}" ]] && continue
            printf '  - src: "%s"\n    dst: "/opt/HPCCSystems/lib/%s"\n    type: symlink\n' "${library}" "${soname}"
        done < <(sort -u "${SONAME_LINKS}")
        echo "overrides:"
        echo "  deb:"
        echo "    depends:"
        yaml_list "${deb_depends[@]}"
        echo "  rpm:"
        echo "    depends:"
        yaml_list "${rpm_depends[@]}"
    } > "${nfpm_config}"

    local deb_target="${OUT_DIR}/${PKG_NAME}_${DEB_VERSION}-${DEB_RELEASE}${PKG_LABEL:+_${PKG_LABEL}}_${NFPM_ARCH}.deb"
    local rpm_target="${OUT_DIR}/${PKG_NAME}-${PKG_VERSION}-${PKG_RELEASE}${PKG_LABEL:+.${PKG_LABEL}}.${APK_ARCH}.rpm"
    if [[ ${BUILD_DEB} -eq 1 ]]; then
        sed -e "s|^version: .*|version: ${DEB_VERSION}|" \
            -e "s|^release: .*|release: ${DEB_RELEASE}|" \
            "${nfpm_config}" > "${nfpm_deb_config}"
        nfpm package --config "${nfpm_deb_config}" --packager deb --target "${deb_target}"
    fi
    [[ ${BUILD_RPM} -eq 1 ]] && nfpm package --config "${nfpm_config}" --packager rpm --target "${rpm_target}"
    return 0
}

if [[ ${BUILD_DEB} -eq 1 || ${BUILD_RPM} -eq 1 ]]; then
    build_native_packages
fi

[[ ${BUILD_APK} -eq 1 ]] || exit 0

cat > "${MELANGE_CONFIG}" <<EOF
package:
  name: ${PKG_NAME}
  version: ${APK_VERSION}
  epoch: ${APK_EPOCH}
  description: HPCC Systems platform package built from local CMake artifacts
  copyright:
    - license: Apache-2.0
environment:
  contents:
    repositories:
      - https://packages.wolfi.dev/os
    packages:
      - busybox
pipeline:
  - runs: |
      mkdir -p "\${{targets.destdir}}"
      cp -a stage-rootfs/. "\${{targets.destdir}}"/
update:
  enabled: false
EOF

mkdir -p "${XDG_CACHE_HOME:-${HOME}/.cache}"
APK_REPO="${STAGING_DIR}/apk-repo"
MELANGE_ARGS=(build "${MELANGE_CONFIG}" --arch "${APK_ARCH}" --source-dir "${STAGING_DIR}" --out-dir "${APK_REPO}" --workspace-dir "${WORK_DIR}" --keyring-append "${MELANGE_KEYRING}")
[[ -n "${SIGNING_KEY}" ]] && MELANGE_ARGS+=(--signing-key "${SIGNING_KEY}")
melange "${MELANGE_ARGS[@]}"
cp "${APK_REPO}/${APK_ARCH}/${PKG_NAME}-${APK_FULL_VERSION}.apk" \
    "${OUT_DIR}/${PKG_NAME}-${APK_FULL_VERSION}${PKG_LABEL:+.${PKG_LABEL}}.${APK_ARCH}.apk"