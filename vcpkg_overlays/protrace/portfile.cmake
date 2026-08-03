# Not sure how well this will work in GH Runners?
vcpkg_from_git(
    OUT_SOURCE_PATH SOURCE_PATH
    URL "https://github.com/risk-hsy/protrace.git"
    REF 119f4cd15c3f85705aa6d31406ca0ea8d0a3f613
    HEAD_REF master
)

vcpkg_cmake_configure(
    SOURCE_PATH "${SOURCE_PATH}"
    OPTIONS
        -DUSER_BUILD_TESTS=OFF
        -DPROTRACE_STANDALONE=ON
        -DBUILD_PROTRACE_KERNEL=OFF
)

vcpkg_cmake_install()
vcpkg_cmake_config_fixup(PACKAGE_NAME protrace_user CONFIG_PATH lib/cmake/protrace_user)
file(INSTALL "${SOURCE_PATH}/LICENSE.txt" DESTINATION "${CURRENT_PACKAGES_DIR}/share/${PORT}" RENAME copyright)
