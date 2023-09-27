project(secure-enclave)

set(CMAKE_CXX_STANDARD 20)

find_path(WASMTIME_CPP_API_INCLUDE_DIRS "wasmtime-cpp-api/wasmtime.hh"
    PATHS ${VCPKG_INSTALLED_DIR}/${VCPKG_TARGET_TRIPLET}
)
if (WIN32)
    find_library(WASMTIME_LIB NAMES wasmtime.dll
        PATHS ${VCPKG_INSTALLED_DIR}/${VCPKG_TARGET_TRIPLET}
    )
else()
    find_library(WASMTIME_LIB NAMES wasmtime
        PATHS ${VCPKG_INSTALLED_DIR}/${VCPKG_TARGET_TRIPLET}
    )
endif()

include_directories(
    ${WASMTIME_CPP_API_INCLUDE_DIRS}/wasmtime-c-api
    ${WASMTIME_CPP_API_INCLUDE_DIRS}/wasmtime-cpp-api
    ./../../../system/include
    ./../../../rtl/eclrtl
    ./../../../system/jlib
)

add_definitions(-D_USRDLL -DSECUREENCLAVE_EXPORTS)

add_library(secure-enclave SHARED
    secure-enclave.cpp
)

target_link_libraries(secure-enclave PRIVATE
    ${WASMTIME_LIB}
)

install(
    TARGETS secure-enclave 
    DESTINATION plugins
    CALC_DEPS
)

