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

# - Try to find the libmemcached library # Once done this will define #
#  LIBMEMCACHED_FOUND - system has the libmemcached library
#  LIBMEMCACHED_INCLUDE_DIR - the libmemcached include directory(s)
#  LIBMEMCACHED_LIBRARIES - The libraries needed to use libmemcached

#  If the memcached libraries are found on the system, we assume they exist natively and dependencies
#  can be handled through package management.  If the libraries are not found, and if
#  MEMCACHED_USE_EXTERNAL_LIBRARY is ON, we will fetch, build, and include a copy of the neccessary
#  Libraries.


if(NOT LIBMEMCACHED_FOUND)
    option(MEMCACHED_USE_EXTERNAL_LIBRARY "Pull and build source from external location if local is not found" ON)
    if(NOT LIBMEMCACHED_VERSION)
        set(LIBMEMCACHED_VERSION "${LIBMEMCACHED_FIND_VERSION}")
    endif()

    # Search for native library to build against
    if(WIN32)
        set(libmemcached_lib "libmemcached")
        set(libmemcachedUtil_lib "libmemcachedutil")
    else()
        set(libmemcached_lib "memcached")
        set(libmemcachedUtil_lib "memcachedutil")
    endif()

    find_path(LIBMEMCACHED_INCLUDE_DIR libmemcached/memcached.hpp PATHS /usr/include /usr/share/include /usr/local/include PATH_SUFFIXES libmemcached)

    find_library(LIBMEMCACHEDCORE_LIBRARY NAMES ${libmemcached_lib} PATHS /usr/lib usr/lib/libmemcached /usr/share /usr/lib64 /usr/local/lib /usr/local/lib64)
    find_library(LIBMEMCACHEDUTIL_LIBRARY NAMES ${libmemcachedUtil_lib} PATHS /usr/lib /usr/share /usr/lib64 /usr/local/lib /usr/local/lib64)

    set(LIBMEMCACHED_LIBRARIES ${LIBMEMCACHEDCORE_LIBRARY} ${LIBMEMCACHEDUTIL_LIBRARY})

    if(EXISTS "${LIBMEMCACHED_INCLUDE_DIR}/libmemcached-1.0/configure.h")
        file(STRINGS "${LIBMEMCACHED_INCLUDE_DIR}/libmemcached-1.0/configure.h" version REGEX "#define LIBMEMCACHED_VERSION_STRING")
        string(REGEX REPLACE "#define LIBMEMCACHED_VERSION_STRING " "" version "${version}")
        string(REGEX REPLACE "\"" "" version "${version}")
        set(LIBMEMCACHED_VERSION_STRING ${version})
        if("${LIBMEMCACHED_VERSION_STRING}" VERSION_EQUAL "${LIBMEMCACHED_FIND_VERSION}" OR "${LIBMEMCACHED_VERSION_STRING}" VERSION_GREATER "${LIBMEMCACHED_FIND_VERSION}")
            set(LIBMEMCACHED_VERSION_OK 1)
            set(MSG "${DEFAULT_MSG}")
        else()
            set(LIBMEMCACHED_VERSION_OK 0)
            set(MSG "libmemcached version '${LIBMEMCACHED_VERSION_STRING}' incompatible with min version>=${LIBMEMCACHED_FIND_VERSION}")
        endif()
    endif()

    include(FindPackageHandleStandardArgs)
    find_package_handle_standard_args(libmemcached ${MSG}
        LIBMEMCACHED_LIBRARIES
        LIBMEMCACHED_INCLUDE_DIR
        LIBMEMCACHED_VERSION_OK)

    if(NOT LIBMEMCACHED_FOUND AND MEMCACHED_USE_EXTERNAL_LIBRARY)
        # Currently libmemcached versions are not sufficient on ubuntu 12.04 and 14.04 LTS
        # until then, we build the required libraries from source
        set(LIBMEMCACHED_URL https://launchpad.net/libmemcached/1.0/${LIBMEMCACHED_VERSION}/+download/libmemcached-${LIBMEMCACHED_VERSION}.tar.gz)
        include(ExternalProject)
        ExternalProject_Add(
            generate-libmemcached
            URL ${LIBMEMCACHED_URL}
            DOWNLOAD_NO_PROGRESS 1
            TIMEOUT 15
            PREFIX "ext"
            CONFIGURE_COMMAND "${CMAKE_CURRENT_BINARY_DIR}/ext/src/generate-libmemcached/configure" --prefix=${INSTALL_DIR} LDFLAGS=-L${LIB_PATH}
            BUILD_COMMAND ${CMAKE_MAKE_PROGRAM} LDFLAGS=-Wl,-rpath-link,${LIB_PATH}
            BINARY_DIR ${CMAKE_CURRENT_BINARY_DIR}
            INSTALL_COMMAND "")
        add_library(libmemcached SHARED IMPORTED)
        add_library(libmemcachedutil SHARED IMPORTED)
        set_property(TARGET libmemcached
            PROPERTY IMPORTED_LOCATION ${CMAKE_CURRENT_BINARY_DIR}/libmemcached/.libs/libmemcached.so.11.0.0)
        set_property(TARGET libmemcachedutil
            PROPERTY IMPORTED_LOCATION ${CMAKE_CURRENT_BINARY_DIR}/libmemcached/.libs/libmemcachedutil.so.2.0.0)
        set_property(TARGET libmemcached
            PROPERTY IMPORTED_LINK_DEPENDENT_LIBRARIES libmemcachedutil)
        add_dependencies(libmemcached generate-libmemcached)
        add_dependencies(libmemcachedutil generate-libmemcached)

        install(CODE "set(ENV{LD_LIBRARY_PATH} \"\$ENV{LD_LIBRARY_PATH}:${PROJECT_BINARY_DIR}:${PROJECT_BINARY_DIR}/libmemcached/.libs\")")
        install(PROGRAMS
            ${CMAKE_CURRENT_BINARY_DIR}/libmemcached/.libs/libmemcached.so
            ${CMAKE_CURRENT_BINARY_DIR}/libmemcached/.libs/libmemcached.so.11
            ${CMAKE_CURRENT_BINARY_DIR}/libmemcached/.libs/libmemcached.so.11.0.0
            ${CMAKE_CURRENT_BINARY_DIR}/libmemcached/.libs/libmemcachedutil.so
            ${CMAKE_CURRENT_BINARY_DIR}/libmemcached/.libs/libmemcachedutil.so.2
            ${CMAKE_CURRENT_BINARY_DIR}/libmemcached/.libs/libmemcachedutil.so.2.0.0
            DESTINATION lib)

        set(LIBMEMCACHED_LIBRARIES $<TARGET_FILE:libmemcached> $<TARGET_FILE:libmemcachedutil>)
        set(LIBMEMCACHED_INCLUDE_DIR ${CMAKE_CURRENT_BINARY_DIR}/ext/src/generate-libmemcached)
        if("${LIBMEMCACHED_VERSION}" VERSION_EQUAL "${LIBMEMCACHED_FIND_VERSION}" OR "${LIBMEMCACHED_VERSION}" VERSION_GREATER "${LIBMEMCACHED_FIND_VERSION}")
            set(LIBMEMCACHED_VERSION_OK 1)
            set(MSG "${DEFAULT_MSG}")
        else()
            set(LIBMEMCACHED_VERSION_OK 0)
            set(MSG "libmemcached version '${LIBMEMCACHED_VERSION}' incompatible with min version>=${LIBMEMCACHED_FIND_VERSION}")
        endif()

        include(FindPackageHandleStandardArgs)
        find_package_handle_standard_args(libmemcached ${MSG}
            LIBMEMCACHED_LIBRARIES
            LIBMEMCACHED_INCLUDE_DIR
            LIBMEMCACHED_VERSION_OK)
    else()
        set(MEMCACHED_USE_EXTERNAL_LIBRARY OFF)
    endif()

    mark_as_advanced(LIBMEMCACHED_INCLUDE_DIRS LIBMEMCACHED_LIBRARIES)
endif()

