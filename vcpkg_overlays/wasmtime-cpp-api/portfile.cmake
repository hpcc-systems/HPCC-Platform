vcpkg_download_distfile(ARCHIVE
    URLS "https://github.com/bytecodealliance/wasmtime-cpp/archive/refs/tags/v${VERSION}.tar.gz"
    FILENAME "v${VERSION}.tar.gz"
    SHA512 6440472084198572b2f00f455e100c9cc0f8a6c76f5f6278432756335f4a340e1af347d6a88ad2e06e0d22a5b84f240a210a9ecfcab1699c1b0fa21cedb8574d
)

vcpkg_extract_source_archive_ex(
    OUT_SOURCE_PATH SOURCE_PATH
    ARCHIVE ${ARCHIVE}
)

file(COPY ${SOURCE_PATH}/include/. DESTINATION ${CURRENT_PACKAGES_DIR}/include/wasmtime-cpp-api)

# Handle copyright
file(INSTALL ${SOURCE_PATH}/LICENSE DESTINATION ${CURRENT_PACKAGES_DIR}/share/wasmtime-cpp-api RENAME copyright)

