#!/bin/bash
# Runs inside an Ubuntu image (packages installed by Dockerfile.toolchain) to capture its libc/libstdc++ development files as a GCC sysroot.
set -euo pipefail

SYSROOT="${1:?sysroot path is required}"
EXPECTED_GLIBC="${2:?expected glibc version is required}"

glibc="$(getconf GNU_LIBC_VERSION)"
glibc="${glibc#glibc }"
[[ "${glibc}" == "${EXPECTED_GLIBC}" ]] || { echo "Sysroot image provides glibc ${glibc}, expected ${EXPECTED_GLIBC}" >&2; exit 1; }

mkdir -p "${SYSROOT}/usr/lib"
cp -a /usr/include "${SYSROOT}/usr/"
multiarch_dir="$(ls -d /usr/lib/*-linux-gnu 2>/dev/null | head -1 || true)"
if [[ -n "${multiarch_dir}" ]]; then
    multiarch="${multiarch_dir##*/}"
    cp -a "${multiarch_dir}/." "${SYSROOT}/usr/lib/"
    cp -a "/usr/include/${multiarch}/." "${SYSROOT}/usr/include/"
    # Debian linker scripts reference the multiarch directories by absolute path.
    ln -s . "${SYSROOT}/usr/lib/${multiarch}"
else
    cp -a /usr/lib64/. "${SYSROOT}/usr/lib/"
fi
ln -s lib "${SYSROOT}/usr/lib64"
ln -s usr/lib "${SYSROOT}/lib"
ln -s usr/lib "${SYSROOT}/lib64"

python="$(python3 -c 'import sys; print("%d.%d" % sys.version_info[:2])')"
cp -a /usr/lib/python3* "${SYSROOT}/usr/lib/"
mkdir -p "${SYSROOT}/usr/bin"
cp -a "/usr/bin/python${python}" "${SYSROOT}/usr/bin/"

find "${SYSROOT}" -type l -print0 | while IFS= read -r -d '' link; do
    target="$(readlink "${link}")"
    if [[ "${target}" == /* ]]; then
        ln -sfn "${SYSROOT}${target}" "${link}"
    fi
done
# Development symlinks normally live in the distro GCC's private directory.
[[ -e "${SYSROOT}/usr/lib/libstdc++.so" ]] || ln -s libstdc++.so.6 "${SYSROOT}/usr/lib/libstdc++.so"
[[ -e "${SYSROOT}/usr/lib/libgcc_s.so" ]] || ln -s libgcc_s.so.1 "${SYSROOT}/usr/lib/libgcc_s.so"
# Debian-family distros only ship libcrypt.so.1; RHEL-family provides it via libxcrypt-compat.
ln -sfn libcrypt.so.1 "${SYSROOT}/usr/lib/libcrypt.so"
# libtool drops --sysroot when linking, so linker scripts must name their inputs relative to -L <sysroot>/usr/lib.
while IFS= read -r script; do
    sed -i -E 's#/[^ ()]*/##g' "${script}"
done < <(find "${SYSROOT}/usr/lib" -maxdepth 1 -type f -name '*.so' -size -4k -exec grep -lIs 'GROUP' {} + || true)
# pkgconf's PKG_CONFIG_SYSROOT_DIR would also prefix vcpkg's own paths, so sysroot .pc files carry absolute sysroot paths.
find "${SYSROOT}/usr/lib/pkgconfig" -type f -name '*.pc' -exec sed -i -E "s#^([A-Za-z_]+=)/usr#\1${SYSROOT}/usr#" {} +

glibcxx="$(grep -aoE 'GLIBCXX_3\.4\.[0-9]+' "${SYSROOT}/usr/lib/libstdc++.so.6" | sort -Vu | tail -1)"
printf 'GLIBC=%s\nGLIBCXX=%s\nPYTHON=%s\n' "${glibc}" "${glibcxx#GLIBCXX_}" "${python}" > "${SYSROOT}/hpcc-baseline"

# The distro's Python runs on Wolfi through the sysroot's own loader, so CMake sees a matching interpreter and library.
loader="$(ls "${SYSROOT}"/usr/lib/ld-linux*.so.* | head -1)"
cat > "${SYSROOT}/usr/bin/hpcc-python" <<EOF
#!/bin/sh
PYTHONHOME="${SYSROOT}/usr" exec "${loader}" --library-path "${SYSROOT}/usr/lib" "${SYSROOT}/usr/bin/python${python}" "\$@"
EOF
chmod 0755 "${SYSROOT}/usr/bin/hpcc-python"
"${SYSROOT}/usr/bin/hpcc-python" -c 'import sysconfig' 
cat > "${SYSROOT}/hpcc-python.cmake" <<EOF
set(Python3_EXECUTABLE "${SYSROOT}/usr/bin/hpcc-python" CACHE FILEPATH "")
set(Python3_INCLUDE_DIR "${SYSROOT}/usr/include/python${python}" CACHE PATH "")
set(Python3_LIBRARY "${SYSROOT}/usr/lib/libpython${python}.so" CACHE FILEPATH "")
EOF
test -f "${SYSROOT}/usr/include/python${python}/Python.h"
test -e "${SYSROOT}/usr/lib/libpython${python}.so"
