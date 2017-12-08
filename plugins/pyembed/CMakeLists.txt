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

if(PY2EMBED)
    unset(PYTHONLIBS_FOUND CACHE)
    unset(PYTHON_LIBRARY CACHE)
    unset(PYTHON_LIBRARIES CACHE)
    unset(PYTHON_INCLUDE_PATH CACHE)
    unset(PYTHON_INCLUDE_DIR CACHE)
    unset(PYTHON_DEBUG_LIBRARIES CACHE)
    unset(PYTHONLIBS_VERSION_STRING CACHE)
    ADD_PLUGIN(py2embed PACKAGES PythonLibs MINVERSION 2.6 MAXVERSION 2.7)
    set(PYTHON2LIBS_FOUND ${PYTHONLIBS_FOUND} CACHE BOOL "Found Python2 Libraries")
    set(PYTHON2_LIBRARY ${PYTHON_LIBRARY} CACHE PATH "Library")
    set(PYTHON2_LIBRARIES ${PYTHON_LIBRARIES} CACHE PATH "Libraries")
    set(PYTHON2_INCLUDE_PATH ${PYTHON_INCLUDE_PATH} CACHE PATH "Include path")
    set(PYTHON2_INCLUDE_DIR ${PYTHON_INCLUDE_DIR} CACHE PATH "Include dir")
    set(PYTHON2_DEBUG_LIBRARIES ${PYTHON_DEBUG_LIBRARIES} CACHE PATH "Include Debug Libraries")
    set(PYTHON2LIBS_VERSION_STRING ${PYTHONLIBS_VERSION_STRING} CACHE STRING "Version")
    message("Python2 library is ${PYTHON2_LIBRARY}")
    if(MAKE_PY2EMBED)
        set(
            SRCS
            pyembed.cpp)

        include_directories(
            "${PYTHON2_INCLUDE_DIR}"
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

        install(
            TARGETS py2embed
            DESTINATION versioned/python2)
        if(debug_python)
            target_link_libraries(py2embed ${DEBUG_PYTHON2_LIBRARY})
        else()
            target_link_libraries(py2embed ${PYTHON2_LIBRARY})
        endif()

        target_link_libraries(
            py2embed
            eclrtl
            roxiemem
            jlib)
    endif()
    if ( PLATFORM )
        install ( PROGRAMS pyembed.install DESTINATION etc/init.d/install COMPONENT Runtime )
        install ( PROGRAMS pyembed.uninstall DESTINATION etc/init.d/uninstall COMPONENT Runtime )
    endif()
endif()
if(PLATFORM OR CLIENTTOOLS_ONLY)
    install(
        FILES ${CMAKE_CURRENT_SOURCE_DIR}/python.ecllib
        DESTINATION plugins
        COMPONENT Runtime)
endif()
