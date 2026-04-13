vcpkg_from_github(
    OUT_SOURCE_PATH SOURCE_PATH
    REPO numactl/numactl
    REF "v${VERSION}"
    SHA512 a9aa93bdc6333b620c10ff3573d6ff645ab54beece75e67be8cdddb27d062cc56cea34db342005a171877f85f05eb1d24e43f8466be907ba3b7c8b1f897cd954
    HEAD_REF master
    PATCHES
        pkgconfig.diff
)

vcpkg_make_configure(
    SOURCE_PATH "${SOURCE_PATH}"
    AUTORECONF
)
vcpkg_make_install()

# Normalize usr/local-prefixed install trees into vcpkg package roots.
set(_numactl_usr_local "${CURRENT_PACKAGES_DIR}/usr/local")
if(IS_DIRECTORY "${_numactl_usr_local}")
    foreach(_dir bin include lib share)
        if(IS_DIRECTORY "${_numactl_usr_local}/${_dir}")
            file(MAKE_DIRECTORY "${CURRENT_PACKAGES_DIR}/${_dir}")
            file(COPY "${_numactl_usr_local}/${_dir}/" DESTINATION "${CURRENT_PACKAGES_DIR}/${_dir}")
            file(REMOVE_RECURSE "${_numactl_usr_local}/${_dir}")
        endif()
    endforeach()
endif()

# Some autotools installs place pkg-config files under usr/local.
# Relocate them to the canonical vcpkg pkgconfig locations.
set(_numactl_pc_rel_path "lib/pkgconfig/numa.pc")
if(EXISTS "${CURRENT_PACKAGES_DIR}/usr/local/${_numactl_pc_rel_path}")
    file(MAKE_DIRECTORY "${CURRENT_PACKAGES_DIR}/lib/pkgconfig")
    file(RENAME "${CURRENT_PACKAGES_DIR}/usr/local/${_numactl_pc_rel_path}" "${CURRENT_PACKAGES_DIR}/${_numactl_pc_rel_path}")
endif()

if(NOT VCPKG_BUILD_TYPE AND EXISTS "${CURRENT_PACKAGES_DIR}/debug/usr/local/${_numactl_pc_rel_path}")
    file(MAKE_DIRECTORY "${CURRENT_PACKAGES_DIR}/debug/lib/pkgconfig")
    file(RENAME "${CURRENT_PACKAGES_DIR}/debug/usr/local/${_numactl_pc_rel_path}" "${CURRENT_PACKAGES_DIR}/debug/${_numactl_pc_rel_path}")
endif()

vcpkg_fixup_pkgconfig()

file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/debug/include" "${CURRENT_PACKAGES_DIR}/debug/share")

vcpkg_install_copyright(FILE_LIST "${SOURCE_PATH}/README.md" "${SOURCE_PATH}/LICENSE.LGPL2.1" "${SOURCE_PATH}/LICENSE.GPL2")
vcpkg_replace_string("${CURRENT_PACKAGES_DIR}/share/${PORT}/copyright" ".*# License" "# License" REGEX)
