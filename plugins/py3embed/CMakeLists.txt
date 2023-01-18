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


# Component: py3embed

#####################################################
# Description:
# ------------
#    Cmake Input File for py3embed
#####################################################

project(py3embed)

find_package(Python3 3.6 REQUIRED COMPONENTS Interpreter Development)

set(
    SRCS
    py3embed.cpp)

include_directories(
    "${Python3_INCLUDE_DIRS}"
    ./../../system/include
    ./../../rtl/eclrtl
    ./../../rtl/include
    ./../../rtl/nbcd
    ./../../common/deftype
    ./../../common/thorhelper
    ./../../dali/base
    ./../../system/mp
    ./../../roxie/roxiemem
    ./../../system/jlib)

add_definitions(-D_USRDLL -DPY3EMBED_EXPORTS)

HPCC_ADD_LIBRARY(py3embed SHARED ${SRCS})
if(${CMAKE_VERSION} VERSION_LESS "2.8.9")
    message(WARNING "Cannot set NO_SONAME. shlibdeps will give warnings when package is installed")
elseif(NOT APPLE)
    set_target_properties(py3embed PROPERTIES NO_SONAME 1)
endif()

if (CONTAINERIZED)
install(
    TARGETS py3embed
    DESTINATION plugins
    CALC_DEPS)
else()
install(
    TARGETS py3embed
    DESTINATION versioned/python3
    CALC_DEPS)
endif()

target_link_libraries(
    py3embed
    eclrtl
    roxiemem
    jlib
    ${Python3_LIBRARIES}
)

if(PLATFORM OR CLIENTTOOLS_ONLY)
    install(
        FILES ${CMAKE_CURRENT_SOURCE_DIR}/python3.ecllib
        DESTINATION plugins
        COMPONENT Runtime)
endif()
