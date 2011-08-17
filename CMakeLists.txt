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
set ( HPCC_MINOR 2 )
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
option(LICENSE_FILE "Set the liscense file to use.")

if( NOT LICENSE_FILE )
    set(LICENSE_FILE "LICENSE.txt")
endif()

include(${HPCC_SOURCE_DIR}/cmake_modules/optionDefaults.cmake)
###

###
## The following sets the install directories and names.
###
set ( OSSDIR "${DIR_NAME}" )
set ( CPACK_SET_DESTDIR "ON" )
set ( CPACK_INSTALL_PREFIX "${PREFIX}" )

SET(CMAKE_INSTALL_PREFIX "${INSTALL_DIR}")
SET(CMAKE_SKIP_BUILD_RPATH  FALSE)
SET(CMAKE_BUILD_WITH_INSTALL_RPATH FALSE) 
SET(CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_PREFIX}/lib")
SET(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

include(${HPCC_SOURCE_DIR}/cmake_modules/commonSetup.cmake)

add_subdirectory (initfiles)

add_subdirectory (tools/esdl)
add_subdirectory (tools/hidl)

add_subdirectory (esp/scm)
add_subdirectory (common/commonext)
add_subdirectory (common/deftype)
add_subdirectory (common/dllserver)
add_subdirectory (common/environment)
add_subdirectory (common/fileview2)
add_subdirectory (common/monitoring/prosysinfo)
add_subdirectory (common/remote)
add_subdirectory (common/roxiecommlib)
add_subdirectory (common/roxiehelper)
add_subdirectory (common/roxiemanager)
add_subdirectory (common/thorhelper)
add_subdirectory (common/workunit)
add_subdirectory (dali/base)
add_subdirectory (dali/dafilesrv)
add_subdirectory (dali/dalidiag)
add_subdirectory (dali/dalistop)
add_subdirectory (dali/datest)
if (CPPUNIT)
    add_subdirectory (dali/daunittest)
endif()
add_subdirectory (dali/dfu)
add_subdirectory (dali/dfuplus)
add_subdirectory (dali/dfuxref)
add_subdirectory (dali/dfuXRefLib)
add_subdirectory (dali/ft)
add_subdirectory (dali/regress)
add_subdirectory (dali/sdsfix)
add_subdirectory (dali/daliadmin)
add_subdirectory (dali/server)
add_subdirectory (dali/treeview)
add_subdirectory (dali/updtdalienv)
add_subdirectory (deployment/configgen)
add_subdirectory (deployment/deploy)
add_subdirectory (deployment/deployutils)
add_subdirectory (deployment/envgen)
add_subdirectory (ecl/agentexec)
add_subdirectory (ecl/eclagent)
add_subdirectory (ecl/eclcc)
add_subdirectory (ecl/eclccserver)
add_subdirectory (ecl/eclscheduler)
add_subdirectory (ecl/eclplus)
add_subdirectory (ecl/hql)
add_subdirectory (ecl/hqlcpp)
add_subdirectory (ecl/hthor)
add_subdirectory (ecl/scheduleadmin)
add_subdirectory (ecl/schedulectrl)
add_subdirectory (ecl/wutest)
add_subdirectory (esp/bindings/SOAP/soaplib)
add_subdirectory (esp/clients/LoggingClient)
add_subdirectory (esp/clients/wsecl)
add_subdirectory (esp/clients/WUManager)
add_subdirectory (esp/platform)
add_subdirectory (esp/protocols/http)
add_subdirectory (esp/services/ecldirect)
add_subdirectory (esp/services/ws_access)
add_subdirectory (esp/services/ws_account)
add_subdirectory (esp/services/ws_config)
add_subdirectory (esp/services/ws_dfu)
add_subdirectory (esp/services/ws_ecl)
add_subdirectory (esp/services/ws_fileio)
add_subdirectory (esp/services/ws_fs)
add_subdirectory (esp/services/ws_machine)
add_subdirectory (esp/services/ws_roxiequery)
add_subdirectory (esp/services/ws_smc)
add_subdirectory (esp/services/ws_topology)
add_subdirectory (esp/services/ws_workunits)
add_subdirectory (esp/services/WsDeploy)
add_subdirectory (esp/smc/SMCLib)
add_subdirectory (esp/test/httptest)
add_subdirectory (esp/tools/soapplus)
add_subdirectory (plugins/auditlib)
add_subdirectory (plugins/debugservices)
add_subdirectory (plugins/fileservices)
add_subdirectory (plugins/logging)
add_subdirectory (plugins/parselib)
add_subdirectory (plugins/stringlib)
add_subdirectory (plugins/unicodelib)
add_subdirectory (plugins/workunitservices)
add_subdirectory (roxie/ccd)
add_subdirectory (roxie/roxie)
add_subdirectory (roxie/roxieclient)
add_subdirectory (roxie/roxieclienttest)
add_subdirectory (roxie/roxiemem)
add_subdirectory (roxie/roxiepipe)
add_subdirectory (roxie/udplib)
add_subdirectory (rtl/eclrtl)
add_subdirectory (rtl/nbcd)
add_subdirectory (dali/sasha)
add_subdirectory (services/runagent)
add_subdirectory (system/hrpc)
add_subdirectory (system/jhtree)
add_subdirectory (system/jlib)
add_subdirectory (system/lzma)
add_subdirectory (system/mp)
add_subdirectory (system/mp/test)
add_subdirectory (system/security/LdapSecurity)
add_subdirectory (system/security/securesocket)
add_subdirectory (system/security/test/ldapsecuritytest)
add_subdirectory (system/security/test/myssl)
if (USE_ZLIB)
  add_subdirectory (system/security/zcrypt)
endif()
add_subdirectory (system/xmllib)

add_subdirectory (thorlcr/activities)
add_subdirectory (thorlcr/graph)
add_subdirectory (thorlcr/master)
add_subdirectory (thorlcr/mfilemanager)
add_subdirectory (thorlcr/msort)
add_subdirectory (thorlcr/slave)
add_subdirectory (thorlcr/thorcodectx)
add_subdirectory (tools/backupnode)
add_subdirectory (tools/combine)
add_subdirectory (tools/dumpkey)
add_subdirectory (tools/keydiff)
add_subdirectory (tools/pstart)
add_subdirectory (tools/pskill)
add_subdirectory (tools/testsocket)
add_subdirectory (tools/swapnode)
add_subdirectory (tools/vkey)
add_subdirectory (tools/xmlsize)
add_subdirectory (tools/wuget)
add_subdirectory (tools/copyexp)
add_subdirectory (tools/genht)


# start-stop-daemon is linux only.
if ( CMAKE_SYSTEM MATCHES Linux )
    message ("-- Building start-stop-daemon")
    add_subdirectory (tools/start-stop-daemon)
endif()

###
## CPack install and packaging setup.
###
INCLUDE(InstallRequiredSystemLibraries)
SET(CPACK_PACKAGE_VERSION_MAJOR ${majorver})
SET(CPACK_PACKAGE_VERSION_MINOR ${minorver})
SET(CPACK_PACKAGE_VERSION_PATCH ${point}${stagever})
set ( CPACK_PACKAGE_CONTACT "ossdevelopment@lexisnexis.com" )
set ( CPACK_SOURCE_PACKAGE_FILE_NAME "${CMAKE_PROJECT_NAME}-${CPACK_RPM_PACKAGE_VERSION}_${version}" )
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
        set(CPACK_PACKAGE_FILE_NAME "${CMAKE_PROJECT_NAME}-${CPACK_RPM_PACKAGE_VERSION}_${version}-${stagever}${packageRevisionArch}")
    elseif ( ${packageManagement} STREQUAL "RPM" )
        set(CPACK_PACKAGE_FILE_NAME "${CMAKE_PROJECT_NAME}-${CPACK_RPM_PACKAGE_VERSION}_${version}-${stagever}.${packageRevisionArch}")
        else()
        set(CPACK_PACKAGE_FILE_NAME "${CMAKE_PROJECT_NAME}-${CPACK_RPM_PACKAGE_VERSION}_${version}-${stagever}-${CPACK_SYSTEM_NAME}")
    endif ()
endif ( CMAKE_SYSTEM MATCHES Linux )
MESSAGE ("-- Current release version is ${CPACK_PACKAGE_FILE_NAME}")
set ( CPACK_SOURCE_PACKAGE_FILE_NAME "${CMAKE_PROJECT_NAME}-${CPACK_RPM_PACKAGE_VERSION}_${version}" )
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
## Run file configuration to set build tag
###
set( BUILD_TAG "${CPACK_RPM_PACKAGE_VERSION}_${CPACK_RPM_PACKAGE_RELEASE}-${stagever}")
configure_file(${HPCC_SOURCE_DIR}/build-config.h.cmake "build-config.h" )
configure_file("initfiles/etc/DIR_NAME/version.in" "version")
configure_file("initfiles/etc/DIR_NAME/environment.conf.in" "environment.conf")
configure_file("initfiles/etc/DIR_NAME/environment.xml.in" "environment.xml")
configure_file("initfiles/etc/DIR_NAME/configmgr/configmgr.conf.in" "configmgr.conf")
configure_file("initfiles/etc/DIR_NAME/configmgr/esp.xml.in" "esp.xml")
if ( ${HPCC_PROJECT} STREQUAL "community" )
    configure_file("initfiles/componentfiles/configxml/buildsetCC.xml.in" "buildset.xml")
else ()
    configure_file("initfiles/componentfiles/configxml/buildsetEE.xml.in" "buildset.xml")
endif()


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
                set ( CPACK_DEBIAN_PACKAGE_CONTROL_EXTRA "${CMAKE_BINARY_DIR}/initfiles/bash/sbin/deb/postinst;${CMAKE_BINARY_DIR}/initfiles/sbin/prerm;${CMAKE_BINARY_DIR}/initfiles/bash/sbin/deb/postrm" )
            endif ()

        elseif ( ${packageManagement} STREQUAL "RPM" )
            set ( CPACK_GENERATOR "${packageManagement}" )
            ###
            ## CPack instruction required for RPM 
            ###
            message("-- Will build RPM package")
            message ("-- Packing BASH installation files")
            set ( CPACK_RPM_POST_INSTALL_SCRIPT_FILE "${CMAKE_BINARY_DIR}/initfiles/bash/sbin/deb/postinst" )

            set ( CPACK_RPM_PRE_UNINSTALL_SCRIPT_FILE "${CMAKE_BINARY_DIR}/initfiles/sbin/prerm" )
            set ( CPACK_RPM_POST_UNINSTALL_SCRIPT_FILE "${CMAKE_BINARY_DIR}/initfiles/bash/sbin/deb/postrm" )
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

Install ( FILES ${HPCC_SOURCE_DIR}/${LICENSE_FILE} DESTINATION ${OSSDIR} PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE )
include( install_directory/overall.install )
include( install_directory/bash_startup.install )
include( install_directory/WsReferenceTracker.install )
include( install_directory/dali.install )
include( install_directory/dfuplus.install )
include( install_directory/dfuserver.install )
include( install_directory/dkcslave.install ) #: MORE - file name is wrong!
include( install_directory/eclagent.install )       
include( install_directory/ecldirect.install )
include( install_directory/ecllibrary.install )
include( install_directory/eclplus.install )
include( install_directory/eclcc.install )      
include( install_directory/eclccserver.install )      
include( install_directory/eclscheduler.install )
include( install_directory/esp.install )             
include( install_directory/espsmc.install )          
include( install_directory/ftslave.install )
#include( install_directory/regressionSuite.install )
include( install_directory/roxie.install )           
include( install_directory/sasha.install )           
include( install_directory/thor.install )            
include( install_directory/ws_account.install )
include( install_directory/ws_watchlist.install )
include( install_directory/ws_ecl.install )
include( install_directory/configgen.install )
include( install_directory/configmgr.install )
include( install_directory/agentexec.install )
include (CPack)
