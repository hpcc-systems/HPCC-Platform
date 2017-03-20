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
if(PY3EMBED)
    unset(PYTHONLIBS_FOUND CACHE)
    unset(PYTHON_LIBRARY CACHE)
    unset(PYTHON_LIBRARIES CACHE)
    unset(PYTHON_INCLUDE_PATH CACHE)
    unset(PYTHON_INCLUDE_DIR CACHE)
    unset(PYTHON_DEBUG_LIBRARIES CACHE)
    unset(PYTHONLIBS_VERSION_STRING CACHE)
    ADD_PLUGIN(py3embed PACKAGES PythonLibs MINVERSION 3.3)
    set(PYTHON3LIBS_FOUND ${PYTHONLIBS_FOUND})
    set(PYTHON3_LIBRARY ${PYTHON_LIBRARY})
    set(PYTHON3_LIBRARIES ${PYTHON_LIBRARIES})
    set(PYTHON3_INCLUDE_PATH ${PYTHON_INCLUDE_PATH})
    set(PYTHON3_INCLUDE_DIR ${PYTHON_INCLUDE_DIR})
    set(PYTHON3_DEBUG_LIBRARIES ${PYTHON_DEBUG_LIBRARIES})
    set(PYTHON3LIBS_VERSION_STRING ${PYTHONLIBS_VERSION_STRING})
    message("Python3 library is ${PYTHON3_LIBRARY}")

    if(MAKE_PY3EMBED)
        set(
            SRCS
            py3embed.cpp)

        include_directories(
            "${PYTHON3_INCLUDE_DIR}"
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

        install(
            TARGETS py3embed
            DESTINATION versioned/python3)

        target_link_libraries(py3embed ${PYTHON3_LIBRARY})

        target_link_libraries(
            py3embed
            eclrtl
            roxiemem
            jlib)
    endif()
endif()
if(PLATFORM OR CLIENTTOOLS_ONLY)
    install(
        FILES ${CMAKE_CURRENT_SOURCE_DIR}/python3.ecllib
        DESTINATION plugins
        COMPONENT Runtime)
endif()
