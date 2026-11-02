set(VCPKG_TARGET_ARCHITECTURE x64)
set(VCPKG_CRT_LINKAGE dynamic)
set(VCPKG_LIBRARY_LINKAGE dynamic)

set(VCPKG_CMAKE_SYSTEM_NAME Linux)

set(VCPKG_FIXUP_ELF_RPATH ON)
set(VCPKG_BUILD_TYPE release)

# Chainguard glibc-baseline builds: tracking HPCC_GLIBC_BASELINE keeps cached binaries separate per baseline.
if(NOT DEFINED ENV{HPCC_GLIBC_BASELINE})
    message(FATAL_ERROR "HPCC_GLIBC_BASELINE must be set for the ${TARGET_TRIPLET} triplet")
endif()
set(VCPKG_ENV_PASSTHROUGH HPCC_GLIBC_BASELINE)
set(VCPKG_CHAINLOAD_TOOLCHAIN_FILE "${CMAKE_CURRENT_LIST_DIR}/linux-sysroot-toolchain.cmake")
set(VCPKG_C_FLAGS "--sysroot=/opt/hpcc-sysroot")
set(VCPKG_CXX_FLAGS "--sysroot=/opt/hpcc-sysroot")
# vcpkg's lib precedes the sysroot so its libraries (e.g. OpenSSL) win over same-named, older sysroot copies.
set(VCPKG_LINKER_FLAGS "--sysroot=/opt/hpcc-sysroot -B/opt/hpcc-sysroot/usr/lib -L/opt/hpcc-sysroot/usr/lib -Wl,-rpath-link,${CURRENT_INSTALLED_DIR}/lib:/opt/hpcc-sysroot/usr/lib")
