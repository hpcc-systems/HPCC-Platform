include("${CMAKE_CURRENT_LIST_DIR}/../vcpkg/scripts/toolchains/linux.cmake")
# Ports may only find headers, libraries and packages in the sysroot or vcpkg's tree; tools still come from the host.
set(CMAKE_SYSROOT "/opt/hpcc-sysroot")
set(CMAKE_FIND_ROOT_PATH_MODE_PROGRAM NEVER)
set(CMAKE_FIND_ROOT_PATH_MODE_INCLUDE ONLY)
set(CMAKE_FIND_ROOT_PATH_MODE_LIBRARY ONLY)
set(CMAKE_FIND_ROOT_PATH_MODE_PACKAGE ONLY)
# pkg-config must not fall back to the host's .pc files (vcpkg's own are still found via PKG_CONFIG_PATH).
set(ENV{PKG_CONFIG_LIBDIR} "${CMAKE_SYSROOT}/usr/lib/pkgconfig")
