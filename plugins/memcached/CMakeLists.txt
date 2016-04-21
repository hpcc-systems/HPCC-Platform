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

# Component: memcached

#####################################################
# Description:
# ------------
#    Cmake Input File for memcached
#####################################################

project( memcached )

if (MEMCACHED)
    ADD_PLUGIN(memcached)
    if(MAKE_MEMCACHED)
        # Currently libmemcached versions are not sufficient on ubuntu 12.04 and 14.04 LTS
        # until then, we build the required libraries from source

        set(LIBMEMCACHED_VERSION "1.0.10")
        add_custom_command(
            OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/libmemcached/.libs/libmemcached.so
            COMMAND wget -q https://launchpad.net/libmemcached/1.0/${LIBMEMCACHED_VERSION}/+download/libmemcached-${LIBMEMCACHED_VERSION}.tar.gz
            COMMAND tar -xzf ${CMAKE_CURRENT_BINARY_DIR}/libmemcached-${LIBMEMCACHED_VERSION}.tar.gz
            COMMAND cd ${CMAKE_CURRENT_BINARY_DIR}/libmemcached-${LIBMEMCACHED_VERSION}
            COMMAND ${CMAKE_CURRENT_BINARY_DIR}/libmemcached-${LIBMEMCACHED_VERSION}/configure > /dev/null
            COMMAND make -j > /dev/null
            WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
            COMMENT "fetching and building libmemcached-${LIBMEMCACHED_VERSION}")
        add_custom_target(generate-libmemcached
            DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/libmemcached/.libs/libmemcached.so)
        add_library(libmemcached SHARED IMPORTED)
        set_target_properties(libmemcached
            PROPERTIES IMPORTED_LOCATION ${CMAKE_CURRENT_BINARY_DIR}/libmemcached/.libs/libmemcached.so.11.0.0)
        set(LIBMEMCACHED_INCLUDE_DIR ${CMAKE_CURRENT_BINARY_DIR}/libmemcached-${LIBMEMCACHED_VERSION})

        set(
            SRCS
            memcachedplugin.hpp
            memcachedplugin.cpp)

        include_directories(
            ./../../system/include
            ./../../rtl/eclrtl
            ./../../rtl/include
            ./../../common/deftype
            ./../../system/jlib
            ${LIBMEMCACHED_INCLUDE_DIR})

        add_definitions(-D_USRDLL -DECL_MEMCACHED_EXPORTS)

        #libmemcached-1.0.18 includes the header <cinttypes> which translates to <bits/cinttypes> requiring ISO C++ 2011 standard to build.
        #All lesser versions explicitly include <tr1/cinttypes> which does not.
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=gnu++0x")

        HPCC_ADD_LIBRARY(memcached SHARED ${SRCS})
        if(${CMAKE_VERSION} VERSION_LESS "2.8.9")
            message(WARNING "Cannot set NO_SONAME. shlibdeps will give warnings when package is installed")
        elseif(NOT APPLE)
            set_target_properties(memcached PROPERTIES NO_SONAME 1)
        endif()

        add_dependencies(memcached generate-libmemcached)
        target_link_libraries(
            memcached
            eclrtl
            jlib
            libmemcached)

        install(CODE "set(ENV{LD_LIBRARY_PATH} \"\$ENV{LD_LIBRARY_PATH}:${PROJECT_BINARY_DIR}:${PROJECT_BINARY_DIR}/libmemcached/.libs\")")
        install(
            TARGETS memcached
            DESTINATION plugins)
        install(FILES
            ${CMAKE_CURRENT_BINARY_DIR}/libmemcached/.libs/libmemcached.so
            ${CMAKE_CURRENT_BINARY_DIR}/libmemcached/.libs/libmemcached.so.11
            ${CMAKE_CURRENT_BINARY_DIR}/libmemcached/.libs/libmemcached.so.11.0.0
            DESTINATION lib
            PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE)
    endif()
endif()

if(PLATFORM)
    install(
        FILES ${CMAKE_CURRENT_SOURCE_DIR}/lib_memcached.ecllib
        DESTINATION plugins
        COMPONENT Runtime)
endif()
