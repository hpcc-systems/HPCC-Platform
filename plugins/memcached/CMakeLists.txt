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
    ADD_PLUGIN(memcached PACKAGES LIBMEMCACHED OPTION MAKE_MEMCACHED MINVERSION 1.0.10)
    if(MAKE_MEMCACHED)
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

        install(
            TARGETS memcached
            DESTINATION plugins)

        target_link_libraries(
            memcached
            eclrtl
            jlib
            ${LIBMEMCACHED_LIBRARIES})
    endif()
endif()

if(PLATFORM)
    install(
        FILES ${CMAKE_CURRENT_SOURCE_DIR}/lib_memcached.ecllib
        DESTINATION plugins
        COMPONENT Runtime)
endif()
