vcpkg_from_github(
    OUT_SOURCE_PATH SOURCE_PATH
    REPO antlr/antlr3
    HEAD_REF master
    REF release-${VERSION}
    SHA512 9608026867eef8d33cc61146d805e92bd27cdbe049c3597811acd93d3bfbed3adb1815c392a68a8fa22220ce39a585f860dff6d255f80a107a27f700ffed5c85
)

set (JAR_VERSION ${VERSION})

file(DOWNLOAD 
    "https://github.com/antlr/website-antlr3/raw/refs/heads/gh-pages/download/antlr-${JAR_VERSION}-complete.jar" 
    "${SOURCE_PATH}/antlr-${JAR_VERSION}-complete.jar"
)

file(DOWNLOAD 
    "https://github.com/antlr/website-antlr3/raw/refs/heads/gh-pages/download/antlr-runtime-${JAR_VERSION}.jar"
    "${SOURCE_PATH}/antlr-runtime-${JAR_VERSION}.jar"
)

set(WORKING_DIR "${SOURCE_PATH}/runtime/C")

vcpkg_configure_make(
    SOURCE_PATH "${WORKING_DIR}"
    COPY_SOURCE
)
vcpkg_install_make()

file(INSTALL 
    DESTINATION "${CURRENT_PACKAGES_DIR}/share/antlr3"
    TYPE FILE
    FILES "${SOURCE_PATH}/antlr-${JAR_VERSION}-complete.jar" "${SOURCE_PATH}/antlr-runtime-${JAR_VERSION}.jar"
)

vcpkg_copy_pdbs()

vcpkg_install_copyright(FILE_LIST "${WORKING_DIR}/COPYING")
