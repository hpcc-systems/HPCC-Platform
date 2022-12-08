################################################################################
#    HPCC SYSTEMS software Copyright (C) 2016 HPCC Systems®.
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

# Component: couchbaseembed

#####################################################
# Description:
# ------------
#    Cmake Input File for couchbaseembed
#####################################################

project(couchbaseembed)

IF ( COUCHBASEEMBED )

    find_path(LIBCOUCHBASE_CXX_INCLUDE_DIRS "libcouchbase/couchbase++.h")
    find_library(LIBCOUCHBASE_LIB NAMES couchbase)

    set(
        SRCS
        couchbaseembed.cpp
        )

    INCLUDE_DIRECTORIES (
        ${LIBCOUCHBASE_CXX_INCLUDE_DIRS}
        ${HPCC_SOURCE_DIR}/esp/platform #for token serialize/deserialize
        ${HPCC_SOURCE_DIR}/system/include
        ${HPCC_SOURCE_DIR}/rtl/eclrtl
        ${HPCC_SOURCE_DIR}/rtl/include
        ${HPCC_SOURCE_DIR}/rtl/nbcd
        ${HPCC_SOURCE_DIR}/common/deftype
        ${HPCC_SOURCE_DIR}/system/jlib
        ${HPCC_SOURCE_DIR}/roxie/roxiemem
        )

    ADD_DEFINITIONS(-D_USRDLL -DCOUCHBASEEMBED_EXPORTS)
    HPCC_ADD_LIBRARY(couchbaseembed SHARED ${SRCS})
    install(CODE "set(ENV{LD_LIBRARY_PATH} \"\$ENV{LD_LIBRARY_PATH}:${PROJECT_BINARY_DIR}:${PROJECT_BINARY_DIR}/libcouchbase-build/lib\")")
    install(
        TARGETS couchbaseembed
        DESTINATION plugins
        CALC_DEPS)

    target_link_libraries(
        couchbaseembed
        ${LIBCOUCHBASE_LIB}
        eclrtl
        jlib)
endif()

if(PLATFORM OR CLIENTTOOLS_ONLY)
    install(
        FILES ${CMAKE_CURRENT_SOURCE_DIR}/couchbase.ecllib
        DESTINATION plugins
        COMPONENT Runtime)
endif()
