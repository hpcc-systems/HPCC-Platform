if ("${VCPKG_DONE}" STREQUAL "")
  set (VCPKG_DONE 1)

# Verify vcpkg submodule is properly initialized and up-to-date
if(NOT EXISTS "${HPCC_SOURCE_DIR}/vcpkg/.git")
    message(FATAL_ERROR "vcpkg submodule is not initialized. Please run: git submodule update --init --recursive vcpkg")
endif()

# Check vcpkg submodule version
# Simple approach: Try local checkout first, give clear manual instructions if it fails
# IMPORTANT: Auto-update should be disabled in CI environments to avoid concurrency issues (set -DVCPKG_AUTO_UPDATE_SUBMODULE=OFF)
# WARNING: In multi-user or shared checkouts, concurrent CMake configure operations may cause race conditions during auto-update
# SECURITY: Always verify vcpkg-configuration.json changes in pull requests - malicious modifications could check out compromised commits
# Read expected baseline from vcpkg-configuration.json
if(EXISTS "${HPCC_SOURCE_DIR}/vcpkg-configuration.json")
    file(READ "${HPCC_SOURCE_DIR}/vcpkg-configuration.json" VCPKG_CONFIG_CONTENT)
    
    # Use CMake's built-in JSON support (available since 3.19, HPCC requires 3.22.1+)
    string(JSON EXPECTED_VCPKG_COMMIT ERROR_VARIABLE JSON_ERROR GET "${VCPKG_CONFIG_CONTENT}" "default-registry" "baseline")
    
    if(JSON_ERROR)
        message(WARNING "Could not read baseline from vcpkg-configuration.json: ${JSON_ERROR}")
        message(WARNING "Skipping vcpkg submodule version check")
    elseif(EXPECTED_VCPKG_COMMIT AND NOT EXPECTED_VCPKG_COMMIT MATCHES ".*-NOTFOUND")
        # Validate commit hash format (7-40 hexadecimal characters for Git SHA-1)
        if(NOT EXPECTED_VCPKG_COMMIT MATCHES "^[a-fA-F0-9]{7,40}$")
            message(WARNING "Invalid baseline commit format in vcpkg-configuration.json: ${EXPECTED_VCPKG_COMMIT}")
            message(WARNING "Expected 7-40 hexadecimal characters. Skipping vcpkg submodule version check")
        else()
            # Get current vcpkg commit
            execute_process(
                COMMAND git -C "${HPCC_SOURCE_DIR}/vcpkg" rev-parse HEAD
                OUTPUT_VARIABLE CURRENT_VCPKG_COMMIT
                OUTPUT_STRIP_TRAILING_WHITESPACE
                RESULT_VARIABLE REV_PARSE_RESULT
                ERROR_VARIABLE REV_PARSE_ERROR
                ERROR_QUIET
            )
            
            if(NOT REV_PARSE_RESULT EQUAL 0 OR "${CURRENT_VCPKG_COMMIT}" STREQUAL "")
                message(FATAL_ERROR "Failed to determine current vcpkg commit. Please run: git submodule update --init --recursive vcpkg\nError: ${REV_PARSE_ERROR}")
            endif()
            
            # Compare commits
            if(NOT "${CURRENT_VCPKG_COMMIT}" STREQUAL "${EXPECTED_VCPKG_COMMIT}")
            set(VCPKG_MISMATCH_MSG "vcpkg submodule is not at expected baseline")
            set(VCPKG_DETAILS_MSG "Expected: ${EXPECTED_VCPKG_COMMIT}, Current: ${CURRENT_VCPKG_COMMIT}")
            
            if(VCPKG_AUTO_UPDATE_SUBMODULE)
                # Check for uncommitted changes and untracked files before auto-update
                execute_process(
                    COMMAND git -C "${HPCC_SOURCE_DIR}/vcpkg" status --porcelain
                    OUTPUT_VARIABLE VCPKG_STATUS
                    OUTPUT_STRIP_TRAILING_WHITESPACE
                    RESULT_VARIABLE STATUS_RESULT
                    ERROR_QUIET
                )
                
                if(NOT "${VCPKG_STATUS}" STREQUAL "")
                    message(FATAL_ERROR "${VCPKG_MISMATCH_MSG}\n${VCPKG_DETAILS_MSG}\nvcpkg directory has local changes or untracked files. Please clean up first:\n  cd vcpkg && git status")
                endif()
                
                # Try simple checkout first (works if commit exists locally)
                message(STATUS "${VCPKG_MISMATCH_MSG}")
                message(STATUS "${VCPKG_DETAILS_MSG}")
                message(STATUS "Attempting to update vcpkg submodule...")
                
                # Check if expected commit exists locally to support offline builds
                execute_process(
                    COMMAND git -C "${HPCC_SOURCE_DIR}/vcpkg" cat-file -e "${EXPECTED_VCPKG_COMMIT}^{commit}"
                    RESULT_VARIABLE COMMIT_EXISTS_RESULT
                    OUTPUT_QUIET
                    ERROR_QUIET
                )
                
                if(NOT COMMIT_EXISTS_RESULT EQUAL 0)
                    # Commit not found locally, need to fetch from remote
                    message(STATUS "Expected commit not found locally, fetching from remote...")
                    execute_process(
                        COMMAND git -C "${HPCC_SOURCE_DIR}/vcpkg" fetch origin
                        RESULT_VARIABLE FETCH_RESULT
                        OUTPUT_QUIET
                        ERROR_VARIABLE FETCH_ERROR
                    )
                    
                    if(NOT FETCH_RESULT EQUAL 0)
                        message(FATAL_ERROR "Failed to fetch vcpkg submodule updates: ${FETCH_ERROR}\nYou may be offline or the remote is unavailable.\nPlease update vcpkg manually:\n  cd vcpkg\n  git fetch origin\n  git checkout ${EXPECTED_VCPKG_COMMIT}\n\nOr disable auto-update: cmake -DVCPKG_AUTO_UPDATE_SUBMODULE=OFF ..")
                    endif()
                endif()
                
                execute_process(
                    COMMAND git -C "${HPCC_SOURCE_DIR}/vcpkg" checkout "${EXPECTED_VCPKG_COMMIT}"
                    RESULT_VARIABLE CHECKOUT_RESULT
                    OUTPUT_QUIET
                    ERROR_VARIABLE CHECKOUT_ERROR
                )
                
                if(NOT CHECKOUT_RESULT EQUAL 0)
                    # Checkout failed - give clear instructions
                    message(FATAL_ERROR "${VCPKG_MISMATCH_MSG}\n${VCPKG_DETAILS_MSG}\nFailed to checkout vcpkg baseline commit: ${CHECKOUT_ERROR}\nPlease update vcpkg manually:\n  cd vcpkg\n  git fetch origin\n  git checkout ${EXPECTED_VCPKG_COMMIT}\n\nOr disable auto-update: cmake -DVCPKG_AUTO_UPDATE_SUBMODULE=OFF ..")
                endif()
                
                message(STATUS "Successfully updated vcpkg submodule to baseline: ${EXPECTED_VCPKG_COMMIT}")
                message(STATUS "NOTE: vcpkg submodule is now in detached HEAD state at commit ${EXPECTED_VCPKG_COMMIT}")
                message(STATUS "NOTE: The parent repository will show vcpkg as modified. This is normal for submodule updates.")
                message(STATUS "NOTE: If you wish to work on a branch in vcpkg, check out the desired branch in the vcpkg directory.")
            else()
                message(FATAL_ERROR "${VCPKG_MISMATCH_MSG}\n${VCPKG_DETAILS_MSG}\nPlease update vcpkg manually:\n  cd vcpkg && git checkout ${EXPECTED_VCPKG_COMMIT}\n\nOr enable auto-update: cmake -DVCPKG_AUTO_UPDATE_SUBMODULE=ON ..")
            endif()
        else()
            message(STATUS "vcpkg submodule is at expected baseline: ${CURRENT_VCPKG_COMMIT}")
        endif()
        endif()
    else()
        message(WARNING "Could not read baseline from vcpkg-configuration.json default-registry section")
    endif()
else()
    message(WARNING "vcpkg-configuration.json not found, cannot verify submodule version")
endif()

set(VCPKG_FILES_DIR "${CMAKE_BINARY_DIR}" CACHE STRING "Folder for vcpkg download, build and installed files")
set(CMAKE_TOOLCHAIN_FILE ${HPCC_SOURCE_DIR}/vcpkg/scripts/buildsystems/vcpkg.cmake)
set(VCPKG_ROOT ${HPCC_SOURCE_DIR}/vcpkg)
set(VCPKG_INSTALLED_DIR "${VCPKG_FILES_DIR}/vcpkg_installed")
set(VCPKG_INSTALL_OPTIONS "--x-abi-tools-use-exact-versions;--downloads-root=${VCPKG_FILES_DIR}/vcpkg_downloads;--x-buildtrees-root=${VCPKG_FILES_DIR}/vcpkg_buildtrees;--x-packages-root=${VCPKG_FILES_DIR}/vcpkg_packages")

if(WIN32)
    set(VCPKG_HOST_TRIPLET "x64-windows" CACHE STRING "host triplet")
    set(VCPKG_TARGET_TRIPLET "x64-windows" CACHE STRING "target triplet")
elseif(APPLE)
  if (CMAKE_OSX_ARCHITECTURES MATCHES "x86_64")
    set(VCPKG_HOST_TRIPLET "x64-osx" CACHE STRING "host triplet")
    set(VCPKG_TARGET_TRIPLET "x64-osx" CACHE STRING "target triplet")
  else()
    set(VCPKG_HOST_TRIPLET "arm64-osx" CACHE STRING "host triplet")
    set(VCPKG_TARGET_TRIPLET "arm64-osx" CACHE STRING "target triplet")
  endif()
elseif(UNIX)
  execute_process(COMMAND uname -m OUTPUT_VARIABLE ARCHITECTURE)
  string(STRIP ${ARCHITECTURE} ARCHITECTURE)
  if (EMSCRIPTEN)
    if(ARCHITECTURE MATCHES "arm" OR ARCHITECTURE MATCHES "aarch64")
      set(VCPKG_HOST_TRIPLET "arm64-linux-dynamic" CACHE STRING "host triplet")
    else()
      set(VCPKG_HOST_TRIPLET "x64-linux-dynamic" CACHE STRING "host triplet")
    endif()
    set(VCPKG_TARGET_TRIPLET "wasm32-emscripten")
    if(DEFINED ENV{EMSDK})
      set(VCPKG_CHAINLOAD_TOOLCHAIN_FILE "$ENV{EMSDK}/upstream/emscripten/cmake/Modules/Platform/Emscripten.cmake")
    else()
      message(FATAL_ERROR "EMSDK environment variable is not set. Please set EMSDK to your Emscripten SDK path.")
    endif()
  else()
    if(ARCHITECTURE MATCHES "arm" OR ARCHITECTURE MATCHES "aarch64")
      set(VCPKG_HOST_TRIPLET "arm64-linux-dynamic" CACHE STRING "host triplet")
      set(VCPKG_TARGET_TRIPLET "arm64-linux-dynamic" CACHE STRING "target triplet")
    else()
      set(VCPKG_HOST_TRIPLET "x64-linux-dynamic" CACHE STRING "host triplet")
      set(VCPKG_TARGET_TRIPLET "x64-linux-dynamic" CACHE STRING "target triplet")
    endif()
  endif()
endif()

message("-- vcpkg settings:")
message("---- VCPKG_FILES_DIR: ${VCPKG_FILES_DIR}")
message("---- CMAKE_TOOLCHAIN_FILE: ${CMAKE_TOOLCHAIN_FILE}")
message("---- VCPKG_ROOT: ${VCPKG_ROOT}")
message("---- VCPKG_INSTALLED_DIR: ${VCPKG_INSTALLED_DIR}")
message("---- VCPKG_INSTALL_OPTIONS: ${VCPKG_INSTALL_OPTIONS}")
message("---- VCPKG_HOST_TRIPLET: ${VCPKG_HOST_TRIPLET}")
message("---- VCPKG_TARGET_TRIPLET: ${VCPKG_TARGET_TRIPLET}")

#  Create a catalog of the vcpkg dependencies ---
file(GLOB VCPKG_PACKAGES ${VCPKG_FILES_DIR}/vcpkg_packages/*/CONTROL)
list(APPEND VCPKG_PACKAGE_LIST "-----------------\n")
foreach(VCPKG_PACKAGE ${VCPKG_PACKAGES})
    file(READ ${VCPKG_PACKAGE} VCPKG_PACKAGE_CONTENTS)
    list(APPEND VCPKG_PACKAGE_LIST ${VCPKG_PACKAGE_CONTENTS})
    list(APPEND VCPKG_PACKAGE_LIST "-----------------\n")
endforeach()
file(WRITE ${CMAKE_BINARY_DIR}/vcpkg-catalog.txt ${VCPKG_PACKAGE_LIST})
if (INSTALL_VCPKG_CATALOG)
    install(FILES ${CMAKE_BINARY_DIR}/vcpkg-catalog.txt DESTINATION "." COMPONENT Runtime)
endif()

#  Check if vcpkg needs a bootstrap ---
if (WIN32)
    set(VCPKG_BOOTSTRAP_FILE "bootstrap-vcpkg.bat")
else ()
    set(VCPKG_BOOTSTRAP_FILE "./bootstrap-vcpkg.sh")
endif ()
file(SHA256 "${VCPKG_ROOT}/scripts/vcpkg-tool-metadata.txt" VCPKG_SCRIPT_HASH)
set(VCPKG_HASH_FILE "${VCPKG_FILES_DIR}/bootstrap-vcpkg-hash.txt")

set(SAVED_VCPKG_SCRIPT_HASH "")
if (EXISTS "${VCPKG_HASH_FILE}")
    file(READ "${VCPKG_HASH_FILE}" SAVED_VCPKG_SCRIPT_HASH)
endif()

if (NOT "${VCPKG_SCRIPT_HASH}" STREQUAL "${SAVED_VCPKG_SCRIPT_HASH}")
    message( "Hash mismatch or hash file missing. Running ${VCPKG_BOOTSTRAP_FILE}..." )
    execute_process(
        COMMAND "${CMAKE_COMMAND}" -E chdir "${VCPKG_ROOT}" "${VCPKG_BOOTSTRAP_FILE}"
        RESULT_VARIABLE BOOTSTRAP_RESULT
    )
    if (NOT BOOTSTRAP_RESULT EQUAL 0)
        message(FATAL_ERROR "Failed to execute ${VCPKG_BOOTSTRAP_FILE}")
    endif()
    file(WRITE "${VCPKG_HASH_FILE}" "${VCPKG_SCRIPT_HASH}")
endif()

endif ()
