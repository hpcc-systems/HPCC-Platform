vcpkg_download_distfile(ARCHIVE
    URLS "https://www.openldap.org/software/download/OpenLDAP/openldap-release/openldap-${VERSION}.tgz"
         "https://mirror.eu.oneandone.net/software/openldap/openldap-release/openldap-${VERSION}.tgz"
    FILENAME "openldap-${VERSION}.tgz"
    SHA512 18129ad9a385457941e3203de5f130fe2571701abf24592c5beffb01361aae3182c196b2cd48ffeecb792b9b0e5f82c8d92445a7ec63819084757bdedba63b20
)

vcpkg_extract_source_archive(
    SOURCE_PATH
    ARCHIVE "${ARCHIVE}"
    PATCHES
        android.diff
        cyrus-sasl.diff
        openssl.patch
        subdirs.patch
)

vcpkg_list(SET FEATURE_OPTIONS)
if("tools" IN_LIST FEATURES)
    vcpkg_list(APPEND FEATURE_OPTIONS --enable-tools)
endif()

if("cyrus-sasl" IN_LIST FEATURES)
    vcpkg_list(APPEND FEATURE_OPTIONS --with-cyrus-sasl)
else()
    vcpkg_list(APPEND FEATURE_OPTIONS --without-cyrus-sasl)
endif()

if(VCPKG_TARGET_IS_ANDROID)
    vcpkg_list(APPEND FEATURE_OPTIONS --with-yielding_select=yes)
elseif(VCPKG_TARGET_IS_EMSCRIPTEN)
    vcpkg_list(APPEND FEATURE_OPTIONS --with-yielding_select=no)
endif()

# Disable build environment details in binaries
set(ENV{SOURCE_DATE_EPOCH} "1659614616")

vcpkg_make_configure(
    SOURCE_PATH "${SOURCE_PATH}"
    AUTORECONF
    OPTIONS
        ${FEATURE_OPTIONS}
        --disable-cleartext
        --disable-mdb
        --disable-relay
        --disable-slapd
        --disable-syncprov
        --with-tls=openssl
        --without-systemd
        --without-fetch
        --without-argon2
        ac_cv_lib_iodbc_SQLDriverConnect=no
        ac_cv_lib_odbc_SQLDriverConnect=no
        ac_cv_lib_odbc32_SQLDriverConnect=no
)

vcpkg_make_install(TARGETS depend install)

# Normalize usr/local-prefixed install trees into vcpkg package roots.
set(_openldap_usr_local "${CURRENT_PACKAGES_DIR}/usr/local")
if(IS_DIRECTORY "${_openldap_usr_local}")
    foreach(_dir bin include lib share)
        if(IS_DIRECTORY "${_openldap_usr_local}/${_dir}")
            file(MAKE_DIRECTORY "${CURRENT_PACKAGES_DIR}/${_dir}")
            file(COPY "${_openldap_usr_local}/${_dir}/" DESTINATION "${CURRENT_PACKAGES_DIR}/${_dir}")
            file(REMOVE_RECURSE "${_openldap_usr_local}/${_dir}")
        endif()
    endforeach()
endif()

# Some autotools installs place pkg-config files under usr/local.
# Move them to locations vcpkg_fixup_pkgconfig() checks.
set(_openldap_pc_dir "${CURRENT_PACKAGES_DIR}/usr/local/lib/pkgconfig")
if(IS_DIRECTORY "${_openldap_pc_dir}")
    file(MAKE_DIRECTORY "${CURRENT_PACKAGES_DIR}/lib/pkgconfig")
    file(GLOB _openldap_pc_files "${_openldap_pc_dir}/*.pc")
    if(_openldap_pc_files)
        file(COPY ${_openldap_pc_files} DESTINATION "${CURRENT_PACKAGES_DIR}/lib/pkgconfig")
        file(REMOVE ${_openldap_pc_files})
    endif()
endif()

if(NOT VCPKG_BUILD_TYPE)
    set(_openldap_pc_dir_dbg "${CURRENT_PACKAGES_DIR}/debug/usr/local/lib/pkgconfig")
    if(IS_DIRECTORY "${_openldap_pc_dir_dbg}")
        file(MAKE_DIRECTORY "${CURRENT_PACKAGES_DIR}/debug/lib/pkgconfig")
        file(GLOB _openldap_pc_files_dbg "${_openldap_pc_dir_dbg}/*.pc")
        if(_openldap_pc_files_dbg)
            file(COPY ${_openldap_pc_files_dbg} DESTINATION "${CURRENT_PACKAGES_DIR}/debug/lib/pkgconfig")
            file(REMOVE ${_openldap_pc_files_dbg})
        endif()
    endif()
endif()

vcpkg_fixup_pkgconfig()
vcpkg_copy_pdbs()

file(REMOVE_RECURSE
    "${CURRENT_PACKAGES_DIR}/debug/include"
    "${CURRENT_PACKAGES_DIR}/debug/share"
)

vcpkg_install_copyright(FILE_LIST "${SOURCE_PATH}/LICENSE")
