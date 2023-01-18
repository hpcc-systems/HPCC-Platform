################################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.
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

# Component: v8embed

#####################################################
# Description:
# ------------
#    Cmake Input File for v8embed
#####################################################

project(v8embed)

if(V8EMBED)
    ADD_PLUGIN(v8embed PACKAGES V8)
    if(MAKE_V8EMBED)
        set(
            SRCS
            v8embed.cpp)

        include_directories(
            ${V8_INCLUDE_DIR}
            ./../../system/include
            ./../../rtl/eclrtl
            ./../../rtl/nbcd
            ./../../rtl/include
            ./../../common/deftype
            ./../../roxie/roxiemem
            ./../../system/jlib)

        add_definitions(-D_USRDLL -DV8EMBED_EXPORTS)

        HPCC_ADD_LIBRARY(v8embed SHARED ${SRCS})
        if(${CMAKE_VERSION} VERSION_LESS "2.8.9")
            message(WARNING "Cannot set NO_SONAME. shlibdeps will give warnings when package is installed")
        elseif(NOT APPLE)
            set_target_properties(v8embed PROPERTIES NO_SONAME 1)
        endif()

        install(
            TARGETS v8embed
            DESTINATION plugins
            CALC_DEPS)
        target_link_libraries(
            v8embed
            ${V8_LIBRARIES}
            roxiemem
            eclrtl
            jlib)
    else()
        message(WARNING "Cannot build v8embed plugin")
    endif()
endif()

if(PLATFORM OR CLIENTTOOLS_ONLY)
    install(
        FILES ${CMAKE_CURRENT_SOURCE_DIR}/javascript.ecllib
        DESTINATION plugins
        COMPONENT Runtime)
endif()
