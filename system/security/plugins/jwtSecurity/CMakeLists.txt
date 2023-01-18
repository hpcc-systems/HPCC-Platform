################################################################################
#    HPCC SYSTEMS software Copyright (C) 2020 HPCC Systems®.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
################################################################################

project(jwtSecurity)

# configmgr directives
add_subdirectory(configxml)

# https://github.com/Thalhammer/jwt-cpp; disable tests and examples
option(BUILD_TESTS "Configure CMake to build tests (or not)" ON) # Copy from jwt-cpp/CMakeLists.txt
set(BUILD_TESTS OFF)
option(BUILD_EXAMPLES "Configure CMake to build examples (or not)" ON) # Copy from jwt-cpp/CMakeLists.txt
set(BUILD_EXAMPLES OFF)

# Required installed libraries
find_package(CURL REQUIRED)
find_path(JWT_CPP_INCLUDE_DIRS "jwt-cpp/base.h" HINTS "${VCPKG_INSTALLED_DIR}")

set(SRCS
    "${CMAKE_CURRENT_SOURCE_DIR}/jwtSecurity.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/jwtCache.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/jwtEndpoint.cpp"
    "${HPCC_SOURCE_DIR}/system/security/shared/authmap.cpp"
    "${HPCC_SOURCE_DIR}/esp/services/ws_store/espstorelib/daliKVStore.cpp"
    "${JWT_CPP_INCLUDE_DIRS}/jwt-cpp/jwt.h")

add_definitions(-DJWTSECURITY_EXPORTS -D_USRDLL)

include_directories("${JWT_CPP_INCLUDE_DIRS}"
                    "${HPCC_SOURCE_DIR}/dali/base"
                    "${HPCC_SOURCE_DIR}/dali/server"
                    "${HPCC_SOURCE_DIR}/esp/services/ws_store/espstorelib"
                    "${HPCC_SOURCE_DIR}/esp/smc/SMCLib"
                    "${HPCC_SOURCE_DIR}/system/include"
                    "${HPCC_SOURCE_DIR}/system/mp"
                    "${HPCC_SOURCE_DIR}/system/security/shared"
                    "${HPCC_SOURCE_DIR}/system/jlib"
                    "${CURL_INCLUDE_DIRS}")

HPCC_ADD_LIBRARY(jwtSecurity SHARED ${SRCS})

install(TARGETS jwtSecurity
        RUNTIME DESTINATION "${EXEC_DIR}"
        LIBRARY DESTINATION "${LIB_DIR}"
        CALC_DEPS)

target_link_libraries(jwtSecurity
                      PRIVATE jlib dalibase
                      PRIVATE CURL::libcurl)
