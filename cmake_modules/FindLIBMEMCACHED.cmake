################################################################################
#    HPCC SYSTEMS software Copyright (C) 2014 HPCC Systems®.
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

# - Try to find the libmemcached library
# Once done this will define
#
#  LIBMEMCACHED_FOUND - system has the libmemcached library
#  LIBMEMCACHED_INCLUDE_DIR - the libmemcached include directory(s)
#  LIBMEMCACHED_LIBRARIES - The libraries needed to use libmemcached

#  If the memcached libraries are found on the system, we assume they exist natively and dependencies
#  can be handled through package management.  If the libraries are not found, and if
#  MEMCACHED_USE_EXTERNAL_LIBRARY is ON, we will fetch, build, and include a copy of the neccessary
#  Libraries.


IF (NOT LIBMEMCACHED_FOUND)
    option(MEMCACHED_USE_EXTERNAL_LIBRARY "Pull and build source from external location if local is not found" ON)
    if(NOT LIBMEMCACHED_VERSION)
        set(LIBMEMCACHED_VERSION "${LIBMEMCACHED_FIND_VERSION}")
    endif()

    # Search for native library to build against
    IF (WIN32)
        SET (libmemcached_lib "libmemcached")
        SET (libmemcachedUtil_lib "libmemcachedutil")
    ELSE()
        SET (libmemcached_lib "memcached")
        SET (libmemcachedUtil_lib "memcachedutil")
    ENDIF()

    FIND_PATH(LIBMEMCACHED_INCLUDE_DIR libmemcached/memcached.hpp PATHS /usr/include /usr/share/include /usr/local/include PATH_SUFFIXES libmemcached)

    FIND_LIBRARY (LIBMEMCACHEDCORE_LIBRARY NAMES ${libmemcached_lib} PATHS /usr/lib usr/lib/libmemcached /usr/share /usr/lib64 /usr/local/lib /usr/local/lib64)
    FIND_LIBRARY (LIBMEMCACHEDUTIL_LIBRARY NAMES ${libmemcachedUtil_lib} PATHS /usr/lib /usr/share /usr/lib64 /usr/local/lib /usr/local/lib64)

    #IF (LIBMEMCACHED_LIBRARY STREQUAL "LIBMEMCACHED_LIBRARY-NOTFOUND")
    #  SET (LIBMEMCACHEDCORE_LIBRARY "")    # Newer versions of libmemcached are header-only, with no associated library.
    #ENDIF()

    SET (LIBMEMCACHED_LIBRARIES ${LIBMEMCACHEDCORE_LIBRARY} ${LIBMEMCACHEDUTIL_LIBRARY})

    IF(LIBMEMCACHED_INCLUDE_DIR)
        FILE (STRINGS "${LIBMEMCACHED_INCLUDE_DIR}/libmemcached-1.0/configure.h" version REGEX "#define LIBMEMCACHED_VERSION_STRING")
        STRING(REGEX REPLACE "#define LIBMEMCACHED_VERSION_STRING " "" version "${version}")
        STRING(REGEX REPLACE "\"" "" version "${version}")
        SET (LIBMEMCACHED_VERSION_STRING ${version})
        IF ("${LIBMEMCACHED_VERSION_STRING}" VERSION_EQUAL "${LIBMEMCACHED_FIND_VERSION}" OR "${LIBMEMCACHED_VERSION_STRING}" VERSION_GREATER "${LIBMEMCACHED_FIND_VERSION}")
            SET (LIBMEMCACHED_VERSION_OK 1)
            SET (MSG "${DEFAULT_MSG}")
        ELSE()
            SET (LIBMEMCACHED_VERSION_OK 0)
            SET(MSG "libmemcached version '${LIBMEMCACHED_VERSION_STRING}' incompatible with min version>=${LIBMEMCACHED_FIND_VERSION}")
        ENDIF()
    ENDIF()

    include(FindPackageHandleStandardArgs)
    find_package_handle_standard_args(libmemcached ${MSG}
        LIBMEMCACHED_LIBRARIES
        LIBMEMCACHED_INCLUDE_DIR
        LIBMEMCACHED_VERSION_OK
        )

    if(NOT LIBMEMCACHED_FOUND AND MEMCACHED_USE_EXTERNAL_LIBRARY)
        # Currently libmemcached versions are not sufficient on ubuntu 12.04 and 14.04 LTS
        # until then, we build the required libraries from source
        set(LIBMEMCACHED_URL https://launchpad.net/libmemcached/1.0/${LIBMEMCACHED_VERSION}/+download/libmemcached-${LIBMEMCACHED_VERSION}.tar.gz)
        if(NOT EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/libmemcached-${LIBMEMCACHED_VERSION}.tar.gz)
            file(DOWNLOAD
                ${LIBMEMCACHED_URL} ${CMAKE_CURRENT_SOURCE_DIR}/libmemcached-${LIBMEMCACHED_VERSION}.tar.gz
                STATUS libmemcached_status
                LOG libmemcached_log
                TIMEOUT 30
                TLS_VERIFY ON)
            list(GET libmemcached_status 0 status_code)
            list(GET libmemcached_status 1 status_msg)
            if(NOT status_code EQUAL 0)
                if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/libmemcached-${LIBMEMCACHED_VERSION}.tar.gz")
                    file(REMOVE ${CMAKE_CURRENT_SOURCE_DIR}/libmemcached-${LIBMEMCACHED_VERSION}.tar.gz)
                endif()
                message(FATAL_ERROR "Fatal Error: download of ${LIBMEMCACHED_URL} failed
                status_code: ${status_code}
                status_msg: ${status_msg}
                log: ${libmemcached_log}\n")
            else()
                message(STATUS "Download of external libmemcached ${LIBMEMCACHED_VERSION} library successful")
            endif()
        endif()
        add_custom_command(
            OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/libmemcached-${LIBMEMCACHED_VERSION}/libmemcached/.libs/libmemcached.so.11.0.0
            COMMAND ${CMAKE_COMMAND} -E tar xzf ${CMAKE_CURRENT_SOURCE_DIR}/libmemcached-${LIBMEMCACHED_VERSION}.tar.gz -C ${CMAKE_CURRENT_BINARY_DIR}/libmemcached-${LIBMEMCACHED_VERSION}
            COMMAND ${CMAKE_COMMAND} -E chdir ${CMAKE_CURRENT_BINARY_DIR}/libmemcached-${LIBMEMCACHED_VERSION} ${CMAKE_CURRENT_BINARY_DIR}/libmemcached-${LIBMEMCACHED_VERSION}/configure --prefix=\"${INSTALL_DIR}\" LDFLAGS=\"-L${LIB_PATH}\" > /dev/null
            COMMAND ${CMAKE_COMMAND} -E chdir ${CMAKE_CURRENT_BINARY_DIR}/libmemcached-${LIBMEMCACHED_VERSION} ${CMAKE_MAKE_PROGRAM} LDFLAGS=\"-Wl,-rpath-link,${LIB_PATH}\" > /dev/null
            DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/libmemcached-${LIBMEMCACHED_VERSION}.tar.gz
            WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
            COMMENT "building libmemcached-${LIBMEMCACHED_VERSION}")
        add_custom_target(generate-libmemcached
            DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/libmemcached-${LIBMEMCACHED_VERSION}/libmemcached/.libs/libmemcached.so.11.0.0)

        add_library(libmemcached SHARED IMPORTED)
        add_library(libmemcachedutil SHARED IMPORTED)
        set_property(TARGET libmemcached
            PROPERTY IMPORTED_LOCATION ${CMAKE_CURRENT_BINARY_DIR}/libmemcached-${LIBMEMCACHED_VERSION}/libmemcached/.libs/libmemcached.so.11.0.0)
        set_property(TARGET libmemcachedutil
            PROPERTY IMPORTED_LOCATION ${CMAKE_CURRENT_BINARY_DIR}/libmemcached-${LIBMEMCACHED_VERSION}/libmemcached/.libs/libmemcachedutil.so.2.0.0)
        set_property(TARGET libmemcached
            PROPERTY IMPORTED_LINK_DEPENDENT_LIBRARIES libmemcachedutil)
        add_dependencies(libmemcached generate-libmemcached)
        add_dependencies(libmemcachedutil generate-libmemcached)

        install(CODE "set(ENV{LD_LIBRARY_PATH} \"\$ENV{LD_LIBRARY_PATH}:${PROJECT_BINARY_DIR}:${PROJECT_BINARY_DIR}/libmemcached-${LIBMEMCACHED_VERSION}/libmemcached/.libs\")")
        install(PROGRAMS
            ${CMAKE_CURRENT_BINARY_DIR}/libmemcached-${LIBMEMCACHED_VERSION}/libmemcached/.libs/libmemcached.so
            ${CMAKE_CURRENT_BINARY_DIR}/libmemcached-${LIBMEMCACHED_VERSION}/libmemcached/.libs/libmemcached.so.11
            ${CMAKE_CURRENT_BINARY_DIR}/libmemcached-${LIBMEMCACHED_VERSION}/libmemcached/.libs/libmemcached.so.11.0.0
            ${CMAKE_CURRENT_BINARY_DIR}/libmemcached-${LIBMEMCACHED_VERSION}/libmemcached/.libs/libmemcachedutil.so
            ${CMAKE_CURRENT_BINARY_DIR}/libmemcached-${LIBMEMCACHED_VERSION}/libmemcached/.libs/libmemcachedutil.so.2
            ${CMAKE_CURRENT_BINARY_DIR}/libmemcached-${LIBMEMCACHED_VERSION}/libmemcached/.libs/libmemcachedutil.so.2.0.0
            DESTINATION lib)

        set(LIBMEMCACHED_LIBRARIES $<TARGET_FILE:libmemcached> $<TARGET_FILE:libmemcachedutil>)
        set(LIBMEMCACHED_INCLUDE_DIR ${CMAKE_CURRENT_BINARY_DIR}/libmemcached-${LIBMEMCACHED_VERSION})
        IF ("${LIBMEMCACHED_VERSION}" VERSION_EQUAL "${LIBMEMCACHED_FIND_VERSION}" OR "${LIBMEMCACHED_VERSION}" VERSION_GREATER "${LIBMEMCACHED_FIND_VERSION}")
            SET (LIBMEMCACHED_VERSION_OK 1)
            SET (MSG "${DEFAULT_MSG}")
        ELSE()
            SET (LIBMEMCACHED_VERSION_OK 0)
            SET(MSG "libmemcached version '${LIBMEMCACHED_VERSION}' incompatible with min version>=${LIBMEMCACHED_FIND_VERSION}")
        ENDIF()

        include(FindPackageHandleStandardArgs)
        find_package_handle_standard_args(libmemcached ${MSG}
            LIBMEMCACHED_LIBRARIES
            LIBMEMCACHED_INCLUDE_DIR
            LIBMEMCACHED_VERSION_OK
            )

    endif()

    MARK_AS_ADVANCED(LIBMEMCACHED_INCLUDE_DIRS LIBMEMCACHED_LIBRARIES)
ENDIF()

