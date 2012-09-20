###############################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.
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

#
#########################################################
# Description:
# ------------
#           This file is the top level handling for
#           cmake based compilation and build process.
#
#   To build for Linux:
#   1. Check out sources (for example, to directory ~/hpcc)
#   2. Create a build directory - either as a child of hpcc or elsewhere
#   3. cd to the build directory
#   4a.To create makefiles to build native release version for local machine, run 
#       cmake ~/hpcc
#   4b.To create makefiles to build native debug version, run
#       cmake -DCMAKE_BUILD_TYPE=Debug ~/hpcc
#   4c.To create makefiles to build 32-bit version from 64-bit host, run
#       cmake -DCMAKE_C_FLAGS:STRING="-m32 -march=i386" -DCMAKE_CXX_FLAGS:STRING="-m32 -march=i386" ~/hpcc
#   5. To build the makefiles just created above, run
#       make
#   6. Executables will be created in ./<releasemode>/bin and ./<releasemode>/libs
#
#   To build for Windows:
#   1. Check out sources (for example, to directory c:\hpcc)
#   2. Create a build directory - either as a child of hpcc or elsewhere
#   3. cd to the build directory
#   4. To create a Visual Studio project, run 
#       cmake c:\hpcc -G "Visual Studio 9 2008"
#      The sln file hpccsystems-platform.sln will be created in the current directory, and will support debug and release targets
#   5. To build the project, load the solution into the visual studio IDE and build in the usual way.
#   6. Executables will be created in .\hpcc\bin\<release_mode>
#
#########################################################

project (hpccsystems-platform)
cmake_minimum_required (VERSION 2.6)

include(CTest)
ENABLE_TESTING()

set (HPCC_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR})
set(CMAKE_MODULE_PATH "${HPCC_SOURCE_DIR}/cmake_modules/")
include(${HPCC_SOURCE_DIR}/version.cmake)

###
## Build Level
###
if( NOT BUILD_LEVEL )
    set ( BUILD_LEVEL "COMMUNITY" )
endif()
###

include(${HPCC_SOURCE_DIR}/cmake_modules/commonSetup.cmake)

if ( NOT MAKE_DOCS_ONLY )
    HPCC_ADD_SUBDIRECTORY (tools)
    if ( NOT DEVEL )
        if ( CMAKE_SYSTEM MATCHES Linux )
            HPCC_ADD_SUBDIRECTORY (initfiles)
        endif ()
        HPCC_ADD_SUBDIRECTORY (common)
        HPCC_ADD_SUBDIRECTORY (dali)
        HPCC_ADD_SUBDIRECTORY (deploy "PLATFORM")
        HPCC_ADD_SUBDIRECTORY (deployment)
        HPCC_ADD_SUBDIRECTORY (ecl)
        HPCC_ADD_SUBDIRECTORY (ecllibrary)
        HPCC_ADD_SUBDIRECTORY (esp)
        HPCC_ADD_SUBDIRECTORY (plugins)
        HPCC_ADD_SUBDIRECTORY (roxie "PLATFORM")
        HPCC_ADD_SUBDIRECTORY (rtl)
        HPCC_ADD_SUBDIRECTORY (services "PLATFORM")
        HPCC_ADD_SUBDIRECTORY (system)
        HPCC_ADD_SUBDIRECTORY (thorlcr "PLATFORM")
        if (APPLE)
          HPCC_ADD_SUBDIRECTORY (lib2)
        endif (APPLE)
    else()
        include(${HPCC_SOURCE_DIR}/devel.cmake)
    endif()
endif()
HPCC_ADD_SUBDIRECTORY (docs "PLATFORM")

include(${HPCC_SOURCE_DIR}/cmake_modules/cpackSetup.cmake)
###
## Below are the non-compile based install scripts required for
## the hpcc platform.
###
if ( NOT DEVEL )
    Install ( FILES ${HPCC_SOURCE_DIR}/${LICENSE_FILE} DESTINATION ${OSSDIR} COMPONENT Runtime )
endif()
