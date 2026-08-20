# Not sure how well this will work in GH Runners?
vcpkg_from_git(
    OUT_SOURCE_PATH SOURCE_PATH
    URL "https://github.com/risk-hsy/protrace.git"
    REF 4d652d642e968c91cf585a33bb471c65a65fd70f
    HEAD_REF master
)

vcpkg_cmake_configure(
    SOURCE_PATH "${SOURCE_PATH}"
    OPTIONS
        -DUSER_BUILD_TESTS=OFF
        -DPROTRACE_STANDALONE=ON
        -DBUILD_PROTRACE_KERNEL=OFF
        -DUSER_THREAD_MEM=4194304
        -DUSER_LOCK_MEM=16777216
)

vcpkg_cmake_install()

# Upstream installs multiple CMake package configs; fix up each so vcpkg toolchain
# can discover them via find_package(... CONFIG REQUIRED).
vcpkg_cmake_config_fixup(
    PACKAGE_NAME protrace
    CONFIG_PATH lib/cmake/protrace
    DO_NOT_DELETE_PARENT_CONFIG_PATH
)
vcpkg_cmake_config_fixup(
    PACKAGE_NAME protrace_user
    CONFIG_PATH lib/cmake/protrace_user
    DO_NOT_DELETE_PARENT_CONFIG_PATH
)
vcpkg_cmake_config_fixup(
    PACKAGE_NAME protrace_translate
    CONFIG_PATH lib/cmake/protrace_translate
    DO_NOT_DELETE_PARENT_CONFIG_PATH
)

# vcpkg config fixup rewrites imported executable targets to tools/${PORT}.
# Ensure protrace_translate exists at that location.
vcpkg_copy_tools(
    TOOL_NAMES protrace_translate
    AUTO_CLEAN
)

file(INSTALL "${SOURCE_PATH}/LICENSE.txt" DESTINATION "${CURRENT_PACKAGES_DIR}/share/${PORT}" RENAME copyright)
