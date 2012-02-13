###############################################################################
#    Copyright (C) 2011 HPCC Systems.
#
#    All rights reserved. This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU Affero General Public License as
#    published by the Free Software Foundation, either version 3 of the
#    License, or (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU Affero General Public License for more details.
#
#    You should have received a copy of the GNU Affero General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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

###
## Version Information
###
set ( HPCC_PROJECT "community" )
set ( HPCC_MAJOR 3 )
set ( HPCC_MINOR 6 )
set ( HPCC_POINT 0 )
set ( HPCC_MATURITY "rc" )
set ( HPCC_SEQUENCE 1 )
###

###
## Build Level
###
if( NOT BUILD_LEVEL )
    set ( BUILD_LEVEL "COMMUNITY" )
endif()
###


###
## Config Block
###
option(PREFIX "Set the install prefix")
option(EXEC_PREFIX "Set the execution prefix")
option(CONFIG_PREFIX "Set the configuration prefix")
option(DIR_NAME "Set the install directory name")
option(LIB_DIR "Set the library install dir")
option(EXEC_DIR "Set the executable install dir")
option(COMPONENTFILES_DIR "Set the componentfiles dir")
option(ADMIN_DIR "Set the admin dir")
option(PLUGINS_DIR "Set the plugins dir")
option(CONFIG_SOURCE_DIR "Set the configuration source dir")
option(RUNTIME_DIR "Set the runtime dir")
option(HOME_DIR "Set the home dir")
option(LOCK_DIR "Set the lock dir")
option(PID_DIR "Set the pid dir")
option(LOG_DIR "Set the log dir")
option(RUNTIME_USER "Set the runtime username")
option(RUNTIME_GROUP "Set the runtime group")
option(ENV_XML_FILE "Set the environment xml file name.")
option(ENV_CONF_FILE "Set the environment conf file name.")
option(LICENSE_FILE "Set the license file to use.")

if( NOT LICENSE_FILE )
    set(LICENSE_FILE "LICENSE.txt")
endif()

include(${HPCC_SOURCE_DIR}/cmake_modules/optionDefaults.cmake)
###

###
## The following sets the install directories and names.
###
set ( OSSDIR "${DIR_NAME}" )
set ( CPACK_INSTALL_PREFIX "${PREFIX}" )
set ( CPACK_PACKAGING_INSTALL_PREFIX "${PREFIX}" )
set ( CMAKE_INSTALL_PREFIX "${PREFIX}" )
SET(CMAKE_SKIP_BUILD_RPATH  FALSE)
SET(CMAKE_BUILD_WITH_INSTALL_RPATH FALSE) 
SET(CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_PREFIX}/${OSSDIR}/lib")
SET(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

include(${HPCC_SOURCE_DIR}/cmake_modules/commonSetup.cmake)

if ( CMAKE_SYSTEM MATCHES Linux )
    add_subdirectory (initfiles)
endif ()
add_subdirectory (tools)
add_subdirectory (common)
add_subdirectory (dali)
add_subdirectory (deploy)
add_subdirectory (deployment)
add_subdirectory (ecl)
add_subdirectory (ecllibrary)
add_subdirectory (esp)
add_subdirectory (plugins)
add_subdirectory (roxie)
add_subdirectory (rtl)
add_subdirectory (services)
add_subdirectory (system)
add_subdirectory (thorlcr)

###
## CPack install and packaging setup.
###
INCLUDE(InstallRequiredSystemLibraries)
SET(CPACK_PACKAGE_VERSION_MAJOR ${majorver})
SET(CPACK_PACKAGE_VERSION_MINOR ${minorver})
SET(CPACK_PACKAGE_VERSION_PATCH ${point}${stagever})
set ( CPACK_PACKAGE_CONTACT "HPCCSystems <ossdevelopment@lexisnexis.com>" )
set( CPACK_SOURCE_GENERATOR TGZ )
set ( CPACK_RPM_PACKAGE_VERSION "${projname}")
SET(CPACK_RPM_PACKAGE_RELEASE "${version}")
if ( ${ARCH64BIT} EQUAL 1 )
    set ( CPACK_RPM_PACKAGE_ARCHITECTURE "x86_64")
else( ${ARCH64BIT} EQUAL 1 )
    set ( CPACK_RPM_PACKAGE_ARCHITECTURE "i386")
endif ( ${ARCH64BIT} EQUAL 1 )
set(CPACK_SYSTEM_NAME "${CMAKE_SYSTEM_NAME}-${CPACK_RPM_PACKAGE_ARCHITECTURE}")
if ("${CMAKE_BUILD_TYPE}" STREQUAL "Release")
    set(CPACK_STRIP_FILES TRUE)
endif()
if ( CMAKE_SYSTEM MATCHES Linux )
    EXECUTE_PROCESS (
                COMMAND ${HPCC_SOURCE_DIR}/cmake_modules/distrocheck.sh
                    OUTPUT_VARIABLE packageManagement
                        ERROR_VARIABLE  packageManagement
                )
    EXECUTE_PROCESS (
                COMMAND ${HPCC_SOURCE_DIR}/cmake_modules/getpackagerevisionarch.sh
                    OUTPUT_VARIABLE packageRevisionArch
                        ERROR_VARIABLE  packageRevisionArch
                )
    EXECUTE_PROCESS (
                COMMAND ${HPCC_SOURCE_DIR}/cmake_modules/getpackagerevisionarch.sh --noarch
                    OUTPUT_VARIABLE packageRevision
                        ERROR_VARIABLE  packageRevision
                )
		
    message ( "-- Auto Detecting Packaging type")
    message ( "-- distro uses ${packageManagement}, revision is ${packageRevisionArch}" )

    if ( ${packageManagement} STREQUAL "DEB" )
        set(CPACK_PACKAGE_FILE_NAME	"${CMAKE_PROJECT_NAME}_${CPACK_RPM_PACKAGE_VERSION}-${version}-${stagever}${packageRevisionArch}")
    elseif ( ${packageManagement} STREQUAL "RPM" )
        set(CPACK_PACKAGE_FILE_NAME	"${CMAKE_PROJECT_NAME}_${CPACK_RPM_PACKAGE_VERSION}-${version}-${stagever}.${packageRevisionArch}")
        else()
        set(CPACK_PACKAGE_FILE_NAME "${CMAKE_PROJECT_NAME}_${CPACK_RPM_PACKAGE_VERSION}_${version}-${stagever}${CPACK_SYSTEM_NAME}")
    endif ()
endif ( CMAKE_SYSTEM MATCHES Linux )
MESSAGE ("-- Current release version is ${CPACK_PACKAGE_FILE_NAME}")
set ( CPACK_SOURCE_PACKAGE_FILE_NAME "${CMAKE_PROJECT_NAME}_${CPACK_RPM_PACKAGE_VERSION}-${version}" )
set( CPACK_SOURCE_GENERATOR TGZ )
set(CPACK_SOURCE_IGNORE_FILES
        "~$"
        "\\\\.cvsignore$"
        "^${PROJECT_SOURCE_DIR}.*/CVS/"
        "^${PROJECT_SOURCE_DIR}.*/.svn/"
        "^${PROJECT_SOURCE_DIR}.*/.git/"
        "^${PROJECT_SOURCE_DIR}/ln/"
        "^${PROJECT_SOURCE_DIR}/externals/"
        "^${PROJECT_SOURCE_DIR}.*/*.mk$"
        "^${PROJECT_SOURCE_DIR}/makefile$"
        "^${PROJECT_SOURCE_DIR}/make.common$"
        "^${PROJECT_SOURCE_DIR}/make.post$"
        "^${PROJECT_SOURCE_DIR}/build$"
        "^${PROJECT_SOURCE_DIR}/buildall$"
        "^${PROJECT_SOURCE_DIR}/lastbuilds$"
        "^${PROJECT_SOURCE_DIR}/imerge$"
        "^${PROJECT_SOURCE_DIR}/tmerge$"
        "^${PROJECT_SOURCE_DIR}/tmerge.bat$"
        "^${PROJECT_SOURCE_DIR}/tag$"
        "^${PROJECT_SOURCE_DIR}/tag_build$"
        "^${PROJECT_SOURCE_DIR}/old_tag$"
        "^${PROJECT_SOURCE_DIR}/ecl/regress/"
    "^${PROJECT_SOURCE_DIR}/testing/"
        )

###
## Run file configuration to set build tag along with install lines for generated
## config files.
###
set( BUILD_TAG "${CPACK_RPM_PACKAGE_VERSION}_${CPACK_RPM_PACKAGE_RELEASE}-${stagever}")
if (USE_GIT_DESCRIBE OR CHECK_GIT_TAG)
    execute_process(COMMAND "${GIT_COMMAND}" describe --exact --tags --dirty
        WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}"
        OUTPUT_VARIABLE GIT_BUILD_TAG
        ERROR_QUIET
        OUTPUT_STRIP_TRAILING_WHITESPACE)
    if (CHECK_GIT_TAG)
        if(NOT "${GIT_BUILD_TAG}" STREQUAL "${BUILD_TAG}")
            message(FATAL_ERROR "Git tag ${GIT_BUILD_TAG} does not match source version ${BUILD_TAG}" )
        endif()
    else()
        if ("${GIT_BUILD_TAG}" STREQUAL "")
            execute_process(COMMAND "${GIT_COMMAND}" describe --always --tags --all --abbrev=6 --dirty --long
                WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}"
                OUTPUT_VARIABLE GIT_BUILD_TAG
                ERROR_QUIET
                OUTPUT_STRIP_TRAILING_WHITESPACE)
        endif()
    endif()
    message ("-- Git tag is '${GIT_BUILD_TAG}'")
    if(NOT "${GIT_BUILD_TAG}" STREQUAL "${BUILD_TAG}")
        if(NOT "${GIT_BUILD_TAG}" STREQUAL "") # probably means being built from a tarball...
            set( BUILD_TAG "${BUILD_TAG}[${GIT_BUILD_TAG}]")
        endif()
    endif()
endif()
message ("-- Build tag is '${BUILD_TAG}'")
configure_file(${HPCC_SOURCE_DIR}/build-config.h.cmake "build-config.h" )

#set( CPACK_DEBIAN_PACKAGE_SHLIBDEPS ON )
#set( CPACK_DEB_PACKAGE_COMPONENT ON )


###
## CPack commands in this section require cpack 2.8.1 to function.
## When using cpack 2.8.1, the command "make package" will create
## an RPM.
###

if (NOT "${CMAKE_VERSION}" VERSION_LESS "2.8.1")
    if ( CMAKE_SYSTEM MATCHES Linux )
        if ( ${packageManagement} STREQUAL "DEB" )
            if ("${CMAKE_VERSION}" VERSION_EQUAL "2.8.2")
                message("WARNING: CMAKE 2.8.2  would not build DEB package")
            else ()
                set ( CPACK_GENERATOR "${packageManagement}" )
                message("-- Will build DEB package")
                ###
                ## CPack instruction required for Debian
                ###
                message ("-- Packing BASH installation files")
                set ( CPACK_DEBIAN_PACKAGE_CONTROL_EXTRA "${CMAKE_CURRENT_BINARY_DIR}/initfiles/bash/sbin/deb/postinst;${CMAKE_CURRENT_BINARY_DIR}/initfiles/sbin/prerm;${CMAKE_CURRENT_BINARY_DIR}/initfiles/bash/sbin/deb/postrm" )
            endif ()

        elseif ( ${packageManagement} STREQUAL "RPM" )
            set ( CPACK_GENERATOR "${packageManagement}" )
            ###
            ## CPack instruction required for RPM 
            ###
            message("-- Will build RPM package")
            message ("-- Packing BASH installation files")
            set ( CPACK_RPM_POST_INSTALL_SCRIPT_FILE "${CMAKE_CURRENT_BINARY_DIR}/initfiles/bash/sbin/deb/postinst" )

            set ( CPACK_RPM_PRE_UNINSTALL_SCRIPT_FILE "${CMAKE_CURRENT_BINARY_DIR}/initfiles/sbin/prerm" )
            set ( CPACK_RPM_POST_UNINSTALL_SCRIPT_FILE "${CMAKE_CURRENT_BINARY_DIR}/initfiles/bash/sbin/deb/postrm" )
                else()
            message("WARNING: Unsupported package ${packageManagement}.")
        endif ()

    endif ( CMAKE_SYSTEM MATCHES Linux )
    if ( EXISTS ${HPCC_SOURCE_DIR}/cmake_modules/dependencies/${packageRevision}.cmake )
        include( ${HPCC_SOURCE_DIR}/cmake_modules/dependencies/${packageRevision}.cmake )
    else()
        message("-- WARNING: DEPENDENCY FILE FOR ${packageRevision} NOT FOUND, Using deps template.")
        include( ${HPCC_SOURCE_DIR}/cmake_modules/dependencies/template.cmake )
    endif()
else()
    message("WARNING: CMAKE 2.8.1 or later required to create RPMs from this project")
endif()

###
## Below are the non-compile based install scripts required for
## the hpcc platform.
###

Install ( FILES ${HPCC_SOURCE_DIR}/${LICENSE_FILE} DESTINATION ${OSSDIR} COMPONENT Runtime )
include (CPack)
