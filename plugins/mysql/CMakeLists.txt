################################################################################
#    HPCC SYSTEMS software Copyright (C) 2013 HPCC Systems®.
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

# Component: mysqlembed

#####################################################
# Description:
# ------------
#    Cmake Input File for mysqlembed
#####################################################

project(mysqlembed)

if(MYSQLEMBED)
    ADD_PLUGIN(mysqlembed)
    if(MAKE_MYSQLEMBED)
        find_package(MYSQL REQUIRED)
        find_package(ZLIB REQUIRED)
        set(
            SRCS
            mysqlembed.cpp)

        include_directories(
            ./../../system/include
            ./../../rtl/eclrtl
            ./../../roxie/roxiemem
            ./../../rtl/include
            ./../../rtl/nbcd
            ./../../common/deftype
            ./../../system/jlib
            ${MYSQL_INCLUDE_DIR})

        add_definitions(-D_USRDLL -DMYSQLEMBED_EXPORTS)

        HPCC_ADD_LIBRARY(mysqlembed SHARED ${SRCS})
        if(${CMAKE_VERSION} VERSION_LESS "2.8.9")
            message(WARNING "Cannot set NO_SONAME. shlibdeps will give warnings when package is installed")
        elseif(NOT APPLE)
            set_target_properties(mysqlembed PROPERTIES NO_SONAME 1)
        endif()

        install(
            TARGETS mysqlembed
            DESTINATION plugins
            CALC_DEPS)
        target_link_libraries(
            mysqlembed
            ${MYSQL_LIBRARIES}
            eclrtl
            roxiemem
            jlib
            ZLIB::ZLIB
        )
    endif()
endif()

if(PLATFORM OR CLIENTTOOLS_ONLY)
    install(
        FILES ${CMAKE_CURRENT_SOURCE_DIR}/mysql.ecllib
        DESTINATION plugins
        COMPONENT Runtime)
endif()
