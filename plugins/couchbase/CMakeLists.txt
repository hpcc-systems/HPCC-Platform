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

    if(APPLE)
        set(LIBCOUCHBASE_LIB ${CMAKE_CURRENT_BINARY_DIR}/libcouchbase-build/lib/libcouchbase.so)
        set(LIBCOUCHBASE_LIB_SONAME ${CMAKE_CURRENT_BINARY_DIR}/libcouchbase-build/lib/libcouchbase.so.2)
        set(LIBCOUCHBASE_LIB_REAL ${CMAKE_CURRENT_BINARY_DIR}/libcouchbase-build/lib/libcouchbase.so.2.0.59)
    else()
        set(LIBCOUCHBASE_LIB ${CMAKE_CURRENT_BINARY_DIR}/libcouchbase-build/lib/libcouchbase.so)
        set(LIBCOUCHBASE_LIB_SONAME ${CMAKE_CURRENT_BINARY_DIR}/libcouchbase-build/lib/libcouchbase.so.2)
        set(LIBCOUCHBASE_LIB_REAL ${CMAKE_CURRENT_BINARY_DIR}/libcouchbase-build/lib/libcouchbase.so.2.0.59)
    endif()

    set(SSL_INC "")
    if (OPENSSL_INCLUDE_DIR)
       set (SSL_INC "-DOPENSSL_INCLUDE_DIR=${OPENSSL_INCLUDE_DIR}")
    endif()

    set(SSL_LIB "")
    if (OPENSSL_LIBRARIES)
       set (SSL_LIB "-DOPENSSL_LIBRARIES=${OPENSSL_LIBRARIES}")
    endif()

    set(SSL2_LIB "")
    if (OPENSSL_SSL_LIBRARY)
       set (SSL2_LIB "-DOPENSSL_SSL_LIBRARY=${OPENSSL_SSL_LIBRARY}")
    endif()

    set(CYP_LIB "")
    if (OPENSSL_CRYPTO_LIBRARY)
       set (CYP_LIB "-DOPENSSL_CRYPTO_LIBRARY=${OPENSSL_CRYPTO_LIBRARY}")
    endif()

    add_custom_command(
        OUTPUT ${LIBCOUCHBASE_LIB}
        COMMAND mkdir -p ${CMAKE_CURRENT_BINARY_DIR}/libcouchbase-build
        COMMAND cd ${CMAKE_CURRENT_BINARY_DIR}/libcouchbase-build && ${CMAKE_COMMAND} ${SSL2_LIB} ${SSL_INC} ${SSL_LIB}  ${CMAKE_CURRENT_SOURCE_DIR}/libcouchbase && ${CMAKE_MAKE_PROGRAM}
        COMMENT Building libcouchbase for import)
    add_custom_target(libcouchbase-build ALL DEPENDS ${LIBCOUCHBASE_LIB})
    add_library(libcouchbase SHARED IMPORTED)
    set_property(TARGET libcouchbase PROPERTY IMPORTED_LOCATION ${LIBCOUCHBASE_LIB})
    add_dependencies(libcouchbase libcouchbase-build)

    set(
        SRCS
        couchbaseembed.cpp
        )

    INCLUDE_DIRECTORIES (
        ${PROJECT_SOURCE_DIR}/libcouchbase/include
        ${PROJECT_BINARY_DIR}/libcouchbase-build/generated
        ${PROJECT_SOURCE_DIR}/libcouchbase-cxx/include
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
        DESTINATION plugins)

    install(
        FILES ${LIBCOUCHBASE_LIB_REAL}
        DESTINATION ${LIB_DIR}
        PERMISSIONS OWNER_WRITE OWNER_READ OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
        COMPONENT Runtime)

    install(
        FILES ${LIBCOUCHBASE_LIB} ${LIBCOUCHBASE_LIB_SONAME}
        DESTINATION ${LIB_DIR}
        COMPONENT Runtime)

    target_link_libraries(
        couchbaseembed
        libcouchbase
        eclrtl
        jlib)
endif()

if(PLATFORM OR CLIENTTOOLS_ONLY)
    install(
        FILES ${CMAKE_CURRENT_SOURCE_DIR}/couchbase.ecllib
        DESTINATION plugins
        COMPONENT Runtime)
endif()
