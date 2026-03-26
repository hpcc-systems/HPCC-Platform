if (WIN32)
    vcpkg_download_distfile(ARCHIVE
        URLS "https://github.com/bytecodealliance/wasmtime/releases/download/v${VERSION}/wasmtime-v${VERSION}-x86_64-windows-c-api.zip"
        FILENAME "wasmtime-v${VERSION}-x86_64-windows-c-api.zip"
        SHA512 dc96b8908ae1a4eb2bc502cebdfac314240202d02962adabe4763c9dd5aec10f356ed456f1b989aefef2fa0b8dc3b32f80dea817f2c0b01f94d28c7c03e8fc40
    )
elseif (APPLE)
    if (VCPKG_TARGET_ARCHITECTURE STREQUAL "arm64")
        vcpkg_download_distfile(ARCHIVE
            URLS "https://github.com/bytecodealliance/wasmtime/releases/download/v${VERSION}/wasmtime-v${VERSION}-aarch64-macos-c-api.tar.xz"
            FILENAME "wasmtime-v${VERSION}-aarch64-macos-c-api.tar.xz"
            SHA512 fde0238f8d025456543ddad030b1cbf8aec2523d1410648f4a87df838c321edb99099919b54a4bb485e6b43751a8e4b49c832cbb81240b48a1d41ff0d6a0d60d
        )
    elseif (VCPKG_TARGET_ARCHITECTURE STREQUAL "x64")
        vcpkg_download_distfile(ARCHIVE
            URLS "https://github.com/bytecodealliance/wasmtime/releases/download/v${VERSION}/wasmtime-v${VERSION}-x86_64-macos-c-api.tar.xz"
            FILENAME "wasmtime-v${VERSION}-x86_64-macos-c-api.tar.xz"
            SHA512 4c4da5a46d2ddd1f2749c7fe439fdb61c50d0b917e7edd06bc8770ee953bf118b22ada4e025cbef46ebf88a48d7b191516cc9f5c66c012adfacab490842b5db6
        )
    else()
        message(FATAL_ERROR "Unsupported macOS target architecture for wasmtime-c-api: ${VCPKG_TARGET_ARCHITECTURE}")
    endif()
elseif (LINUX)
    execute_process(COMMAND uname -m OUTPUT_VARIABLE ARCHITECTURE)
    string(STRIP ${ARCHITECTURE} ARCHITECTURE)
    if(ARCHITECTURE MATCHES "arm" OR ARCHITECTURE MATCHES "aarch64")
        vcpkg_download_distfile(ARCHIVE
            URLS "https://github.com/bytecodealliance/wasmtime/releases/download/v${VERSION}/wasmtime-v${VERSION}-aarch64-linux-c-api.tar.xz"
            FILENAME "wasmtime-v${VERSION}-aarch64-linux-c-api.tar.xz"
            SHA512 81f45cc5cf37b5709f52b6c7b826b4f7164661cb633182cbc335e3e61bafc9367d7274d6f79304561287581570d1f6f077422c262dcbd41ca154523972b57620
        )
    else()
        vcpkg_download_distfile(ARCHIVE
            URLS "https://github.com/bytecodealliance/wasmtime/releases/download/v${VERSION}/wasmtime-v${VERSION}-x86_64-linux-c-api.tar.xz"
            FILENAME "wasmtime-v${VERSION}-x86_64-linux-c-api.tar.xz"
            SHA512 1ca3029e56d1c780162e9caf93aa6c3e0b44327f5c1fef94a9a325f1a3e980b9193b3d7008ee67f0899d5e59ad12595eb4d51b84eabad41eab9b3d68bb43d63b
        )
    endif()
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

