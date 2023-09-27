if (WIN32)
    vcpkg_download_distfile(ARCHIVE
        URLS "https://github.com/bytecodealliance/wasmtime/releases/download/v${VERSION}/wasmtime-v${VERSION}-x86_64-windows-c-api.zip"
        FILENAME "wasmtime-v${VERSION}-x86_64-windows-c-api.zip"
        SHA512 419a11498f3853764e9285650ef12bbf597445f1e97980ab2b7634a28ad7c81c78502320be23ca4f25302d3408161a9a712ad4a088eb57052cfa21da885e369f
    )
elseif (APPLE)
    vcpkg_download_distfile(ARCHIVE
        URLS "https://github.com/bytecodealliance/wasmtime/releases/download/v${VERSION}/wasmtime-v${VERSION}-x86_64-macos-c-api.tar.xz"
        FILENAME "wasmtime-v${VERSION}-x86_64-macos-c-api.tar.xz"
        SHA512 aed34569c3063b44b5a7acda53b274851d2ef8597b7819221d1a550ee2c408b51315bba95ffd9f495feceab3c3825db9ef53c432bdf565cfacfe31c76328f795
    )
elseif (LINUX)
    vcpkg_download_distfile(ARCHIVE
        URLS "https://github.com/bytecodealliance/wasmtime/releases/download/v${VERSION}/wasmtime-v${VERSION}-x86_64-linux-c-api.tar.xz"
        FILENAME "wasmtime-v${VERSION}-x86_64-linux-c-api.tar.xz"
        SHA512 ad590bceeb6b20520275f96a8cdb737792502f581d4ae3c152f9a6c2602090e5d6aa80f27c77b707e31169d12f6daf176cb7260d16af31b4a83afa7ed6991ded
    )
endif()

vcpkg_extract_source_archive_ex(
    OUT_SOURCE_PATH SOURCE_PATH
    ARCHIVE ${ARCHIVE}
)

file(COPY ${SOURCE_PATH}/include/. DESTINATION ${CURRENT_PACKAGES_DIR}/include/wasmtime-c-api)
if (WIN32)
    file(COPY ${SOURCE_PATH}/lib/. DESTINATION ${CURRENT_PACKAGES_DIR}/debug/bin)
    file(COPY ${SOURCE_PATH}/lib/. DESTINATION ${CURRENT_PACKAGES_DIR}/bin)
else ()
    file(COPY ${SOURCE_PATH}/lib/. DESTINATION ${CURRENT_PACKAGES_DIR}/debug/lib)
    file(COPY ${SOURCE_PATH}/lib/. DESTINATION ${CURRENT_PACKAGES_DIR}/lib)
endif ()

# Handle copyright
file(INSTALL ${SOURCE_PATH}/LICENSE DESTINATION ${CURRENT_PACKAGES_DIR}/share/wasmtime-c-api RENAME copyright)

