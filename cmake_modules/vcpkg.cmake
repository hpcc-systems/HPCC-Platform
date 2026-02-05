if ("${VCPKG_DONE}" STREQUAL "")
  set (VCPKG_DONE 1)

# vcpkg submodule management (skip entirely if disabled)
if(VCPKG_AUTO_UPDATE_SUBMODULE)
    # Find git executable for submodule operations
    find_package(Git REQUIRED)

    if(NOT EXISTS "${HPCC_SOURCE_DIR}/vcpkg/.git")
        message(FATAL_ERROR "vcpkg submodule not initialized.")
    endif()

    # Check if vcpkg submodule is out of sync using git native commands
    execute_process(
        COMMAND ${GIT_EXECUTABLE} submodule status vcpkg
        OUTPUT_VARIABLE SUBMODULE_STATUS
        OUTPUT_STRIP_TRAILING_WHITESPACE
        WORKING_DIRECTORY "${HPCC_SOURCE_DIR}"
        ERROR_QUIET
    )
    
    # Git submodule status output format:
    # " " (space) = in sync
    # "+" = checked out to different commit than recorded
    # "-" = not initialized/checked out
    # "U" = merge conflicts
    if(SUBMODULE_STATUS MATCHES "^[+-U]")
        message(STATUS "vcpkg submodule is out of sync: ${SUBMODULE_STATUS}")

        message(STATUS "Updating vcpkg submodule...")

        # Try a shallow update first (faster, less data). Add a timeout to avoid
        # long blocking operations on slow networks. If shallow update fails or
        # times out, fall back to a full update and report detailed output.
        set(VCPKG_SHALLOW_UPDATE_TIMEOUT 120)
        execute_process(
            COMMAND ${GIT_EXECUTABLE} submodule update --init --recursive --depth 1 vcpkg
            RESULT_VARIABLE SUBMODULE_UPDATE_RESULT
            OUTPUT_VARIABLE SUBMODULE_UPDATE_OUTPUT
            ERROR_VARIABLE SUBMODULE_UPDATE_ERROR
            WORKING_DIRECTORY "${HPCC_SOURCE_DIR}"
            TIMEOUT ${VCPKG_SHALLOW_UPDATE_TIMEOUT}
        )

        # Check for timeout vs other failures with distinct messages
        set(SHALLOW_UPDATE_FAILED FALSE)
        if(SUBMODULE_UPDATE_RESULT MATCHES ".*-NOTFOUND$")
            message(WARNING "Shallow update of vcpkg submodule timed out after ${VCPKG_SHALLOW_UPDATE_TIMEOUT} seconds.")
            set(SHALLOW_UPDATE_FAILED TRUE)
        elseif(NOT SUBMODULE_UPDATE_RESULT EQUAL 0)
            message(WARNING "Shallow update of vcpkg submodule failed with error code ${SUBMODULE_UPDATE_RESULT}. Output: ${SUBMODULE_UPDATE_OUTPUT} Error: ${SUBMODULE_UPDATE_ERROR}")
            set(SHALLOW_UPDATE_FAILED TRUE)
        endif()
        
        if(SHALLOW_UPDATE_FAILED)
            message(STATUS "Attempting full vcpkg submodule update (this may take longer)...")

            set(VCPKG_FULL_UPDATE_TIMEOUT 600)
            execute_process(
                COMMAND ${GIT_EXECUTABLE} submodule update --init --recursive vcpkg
                RESULT_VARIABLE SUBMODULE_UPDATE_RESULT_FULL
                OUTPUT_VARIABLE SUBMODULE_UPDATE_OUTPUT_FULL
                ERROR_VARIABLE SUBMODULE_UPDATE_ERROR_FULL
                WORKING_DIRECTORY "${HPCC_SOURCE_DIR}"
                TIMEOUT ${VCPKG_FULL_UPDATE_TIMEOUT}
            )

            # Check for timeout or other failures in full update with detailed messages
            if(SUBMODULE_UPDATE_RESULT_FULL MATCHES ".*-NOTFOUND$")
                message(FATAL_ERROR "Full vcpkg submodule update timed out after ${VCPKG_FULL_UPDATE_TIMEOUT} seconds.\nManual fix: git submodule update --init --recursive")
            elseif(NOT SUBMODULE_UPDATE_RESULT_FULL EQUAL 0)
                message(FATAL_ERROR "Full vcpkg submodule update failed with error code ${SUBMODULE_UPDATE_RESULT_FULL}.\nOutput: ${SUBMODULE_UPDATE_OUTPUT_FULL}\nError: ${SUBMODULE_UPDATE_ERROR_FULL}\nManual fix: git submodule update --init --recursive")
            endif()
        endif()

        # Get the updated commit hash for security reporting
        execute_process(
            COMMAND ${GIT_EXECUTABLE} rev-parse HEAD
            OUTPUT_VARIABLE VCPKG_UPDATED_COMMIT
            OUTPUT_STRIP_TRAILING_WHITESPACE
            WORKING_DIRECTORY "${HPCC_SOURCE_DIR}/vcpkg"
            ERROR_QUIET
        )

        message(STATUS "Successfully updated vcpkg submodule")
        message(WARNING "Updated to commit: ${VCPKG_UPDATED_COMMIT} - verify source")
    else()
        message(STATUS "vcpkg submodule is in sync")
    endif()
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
# If vcpkg is not present (scripts missing) and automatic submodule updates are
# disabled, fail early with a clear message instead of attempting a long
# bootstrap/download which would block the configure step.
if(NOT EXISTS "${VCPKG_ROOT}/scripts/vcpkg-tool-metadata.txt")
  if(NOT VCPKG_AUTO_UPDATE_SUBMODULE)
    message(FATAL_ERROR "vcpkg appears to be missing or uninitialized (missing: ${VCPKG_ROOT}/scripts/vcpkg-tool-metadata.txt).\nEither enable automatic submodule updates with -DVCPKG_AUTO_UPDATE_SUBMODULE=ON or run: git submodule update --init --recursive vcpkg")
  endif()
endif()
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
