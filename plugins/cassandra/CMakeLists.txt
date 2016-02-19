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

# Component: cassandraembed

#####################################################
# Description:
# ------------
#    Cmake Input File for cassandraembed
#####################################################

project(cassandraembed)

if(CASSANDRAEMBED)
    # There is not yet a standard package for cassandra cpp client, and only very modern distros have libuv-dev
    # When there is (and the distros catch up) we may want to add them as dependencies here
    ADD_PLUGIN(cassandraembed)
    if(MAKE_CASSANDRAEMBED)

    set(
        LIBUV_SRCS
        libuv/src/unix/async.c
        libuv/src/unix/core.c
        libuv/src/unix/dl.c
        libuv/src/unix/error.c
        libuv/src/unix/fs.c
        libuv/src/unix/getaddrinfo.c
        libuv/src/unix/loop.c
        libuv/src/unix/loop-watcher.c
        libuv/src/unix/pipe.c
        libuv/src/unix/poll.c
        libuv/src/unix/process.c
        libuv/src/unix/signal.c
        libuv/src/unix/stream.c
        libuv/src/unix/tcp.c
        libuv/src/unix/thread.c
        libuv/src/unix/threadpool.c
        libuv/src/unix/timer.c
        libuv/src/unix/tty.c
        libuv/src/unix/udp.c
        libuv/src/fs-poll.c
        libuv/src/uv-common.c
        libuv/src/inet.c
        libuv/src/version.c
        libuv/src/unix/linux-core.c
        libuv/src/unix/linux-inotify.c
        libuv/src/unix/linux-syscalls.c
        libuv/src/unix/proctitle.c)

    HPCC_ADD_LIBRARY(uv SHARED ${LIBUV_SRCS})
    target_compile_options(uv PRIVATE --std=c89 -pedantic -Wall -Wextra -Wno-unused-parameter -g -fPIC -c)
    target_compile_definitions(uv PRIVATE -D_GNU_SOURCE -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64)
    set(LIBUV_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/libuv/include/)
    set(LIBUV_LIBRARY $<TARGET_FILE:uv>)

    # Build the Cassandra cpp driver, only presently available as source
    if(NOT EXISTS "${PROJECT_SOURCE_DIR}/cpp-driver/CMakeLists.txt")
        message(FATAL_ERROR
"   The cpp-driver submodule is not available.
   This normally indicates that the git submodule has not been fetched.
   Please run git submodule update --init --recursive")
    endif()

    option(CASS_INSTALL_HEADER "Install header file" OFF)
    option(CASS_BUILD_STATIC "Build static library" OFF)
    option(CASS_BUILD_EXAMPLES "Build examples" OFF)
    set_property(GLOBAL PROPERTY FIND_LIBRARY_USE_LIB64_PATHS FALSE)
    set(_SAVE_CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
    if(NOT WIN32)
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-format-nonliteral") # Work around cassandra build error
    endif()
    add_subdirectory(cpp-driver)
    add_dependencies(cassandra uv)
    set(CMAKE_CXX_FLAGS "${_SAVE_CMAKE_CXX_FLAGS}")

    set(
        SRCS
        cassandraembed.cpp
        cassandrawu.cpp)

    include_directories(
        ./../../system/include
        ./../../rtl/eclrtl
        ./../../roxie/roxiemem
        ./../../rtl/include
        ./../../rtl/nbcd
        ./../../common/deftype
        ./../../common/workunit
        ./../../system/jlib
        ./../../system/security/shared 
        ./../../system/mp
        ./../../dali/base 
        ./cpp-driver/include
        ./libuv/src
        ./libuv/include
        ./libuv/include/uv-private)

    add_definitions(-D_USRDLL -DCASSANDRAEMBED_EXPORTS)

    HPCC_ADD_LIBRARY(cassandraembed SHARED ${SRCS})
    if(NOT FORCE_WORKUNITS_TO_CASSANDRA)
        if(${CMAKE_VERSION} VERSION_LESS "2.8.9")
            message(WARNING "Cannot set NO_SONAME. shlibdeps will give warnings when package is installed")
        elseif(NOT APPLE)
            set_target_properties(cassandraembed PROPERTIES NO_SONAME 1)
        endif()
    endif()

    install(
        TARGETS cassandraembed
        DESTINATION plugins
        COMPONENT Runtime)
    install(
        TARGETS uv
        DESTINATION ${LIB_DIR}
        COMPONENT Runtime)

    target_link_libraries(
        cassandraembed
        uv
        cassandra
        eclrtl
        roxiemem
        dalibase
        workunit
        jlib)
    endif()
endif()

if(PLATFORM)
    install(
        FILES ${CMAKE_CURRENT_SOURCE_DIR}/cassandra.ecllib
        DESTINATION plugins
        COMPONENT Runtime)
endif()
