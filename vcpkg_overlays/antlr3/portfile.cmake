vcpkg_download_distfile(LIB_C
    URLS "https://github.com/antlr/website-antlr3/raw/refs/heads/gh-pages/download/C/libantlr3c-${VERSION}.tar.gz"
    FILENAME "libantlr3c-${VERSION}.tar.gz"
    SHA512 8edb243d745ff5bf3b15940f124d1255a9ca965cb656a73a558aed7fa07effcd7620f23dc692e5d5169a03200254836dd57af3ce444ba225281a5b721497e211
)

vcpkg_extract_source_archive(SOURCE_PATH
    ARCHIVE "${LIB_C}"
)

set(ARM_FLAG "")
if (LINUX)
    execute_process(COMMAND uname -m OUTPUT_VARIABLE ARCHITECTURE)
    string(STRIP ${ARCHITECTURE} ARCHITECTURE)
    if(ARCHITECTURE MATCHES "arm" OR ARCHITECTURE MATCHES "aarch64")
        set(ARM_FLAG "--build=aarch64-unknown-linux")
    endif()
endif()

vcpkg_configure_make(
    SOURCE_PATH "${SOURCE_PATH}"
    COPY_SOURCE
    OPTIONS
        --enable-64bit
        ${ARM_FLAG}
)
vcpkg_install_make()
vcpkg_fixup_pkgconfig()
vcpkg_copy_pdbs()

vcpkg_download_distfile(COMPLETE_JAR
    URLS "https://github.com/antlr/website-antlr3/raw/refs/heads/gh-pages/download/antlr-${VERSION}-complete.jar"
    FILENAME "antlr-${VERSION}-complete.jar"
    SHA512 04be4dfba3a21f3ab9d9e439a64958bd8e844a9f151b798383bd9e0dd6ebc416783ae7cb1d1dbb27fb7288ab9756b13b8338cdb8ceb41a10949c852ad45ab1f2
)

vcpkg_download_distfile(RUNTIME_JAR
    URLS "https://github.com/antlr/website-antlr3/raw/refs/heads/gh-pages/download/antlr-runtime-${VERSION}.jar"
    FILENAME "antlr-runtime-${VERSION}.jar"
    SHA512 1786aff2df4664483adcb319e64be7b69b643ac9508c3f11796b5aa45b9072b46f53f0a21b2ff7291162afe81506de16161746273e4532ebad75adbd81203f0d
)

file(INSTALL 
    DESTINATION "${CURRENT_PACKAGES_DIR}/share/antlr3"
    TYPE FILE
    FILES ${COMPLETE_JAR} ${RUNTIME_JAR}
)

vcpkg_install_copyright(FILE_LIST "${SOURCE_PATH}/COPYING")
