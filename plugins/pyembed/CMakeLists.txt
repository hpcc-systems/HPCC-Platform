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


# Component: py2embed

#####################################################
# Description:
# ------------
#    Cmake Input File for py2embed
#####################################################

set(debug_python OFF)   # A lot slower but can assist in debugging...
set(DEBUG_PYTHON_LIBRARY "/usr/lib/libpython2.7_d.so")

project(py2embed)

find_package(Python2 2.6 REQUIRED COMPONENTS Interpreter Development)

set(
    SRCS
    pyembed.cpp)

include_directories(
    "${Python2_INCLUDE_DIRS}"
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

add_definitions(-D_USRDLL -DPY2EMBED_EXPORTS)
if(debug_python)
    add_definitions(-DPy_DEBUG)
endif()

HPCC_ADD_LIBRARY(py2embed SHARED ${SRCS})
if(${CMAKE_VERSION} VERSION_LESS "2.8.9")
    message(WARNING "Cannot set NO_SONAME. shlibdeps will give warnings when package is installed")
elseif(NOT APPLE)
    set_target_properties(py2embed PROPERTIES NO_SONAME 1)
endif()

if (CONTAINERIZED)
install(
    TARGETS py2embed
    DESTINATION plugins
    CALC_DEPS)
else()
install(
    TARGETS py2embed
    DESTINATION versioned/python2
    CALC_DEPS)
endif()

if(debug_python)
    target_link_libraries(py2embed ${DEBUG_PYTHON2_LIBRARY})
else()
    target_link_libraries(py2embed ${Python2_LIBRARIES})
endif()

target_link_libraries(
    py2embed
    eclrtl
    roxiemem
    jlib)

if ( PLATFORM )
    install ( PROGRAMS pyembed.install DESTINATION etc/init.d/install COMPONENT Runtime )
    install ( PROGRAMS pyembed.uninstall DESTINATION etc/init.d/uninstall COMPONENT Runtime )
endif()

if(PLATFORM OR CLIENTTOOLS_ONLY)
    install(
        FILES ${CMAKE_CURRENT_SOURCE_DIR}/python.ecllib
        DESTINATION plugins
        COMPONENT Runtime)
endif()
