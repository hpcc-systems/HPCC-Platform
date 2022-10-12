###############################################################################
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
#   To create a development installation on Linux:
#   1. Check out sources (for example, to directory ~/hpcc)
#   2. Create a build directory alongside your source
#   3. To create makefiles and have the ability to run the applications as non-superuser, execute
#       cmake -DRUNTIME_USER=$USER -DRUNTIME_GROUP=groupname -DDESTDIR=$HOME/myruntime ~/hpcc
#   4. To build the makefiles just created above, run
#       make
#   5. To install the project under DESTDIR, run
#       make install
#   6. To populate environment xml's and conf files necessary for running your development installation, run
#       make configure
#   7a.To start up the platform, execute all binaries and scripts under DESTDIR as a regular user, example
#       $HOME/myruntime/etc/init.d/hpcc-init start
#   7b.All runtime files will be under $HOME/myruntime/var/{log,lib,run,lock}
#   8a.To patch your running installation, simply modify the source, and then from the build directory run
#       make install
#      Which will recompile any changes and only install files into the platform that have been modified
#   8b.Restart the platform (or specific component that had modifications), example
#       $HOME/myruntime/etc/init.d/hpcc-init restart
#
#########################################################

cmake_minimum_required(VERSION 3.22.1)

set(HPCC_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR})
include(${HPCC_SOURCE_DIR}/version.cmake)

set(VCPKG_FILES_DIR "${CMAKE_BINARY_DIR}" CACHE STRING "Folder for vcpkg download, build and installed files")
set(CMAKE_TOOLCHAIN_FILE ${HPCC_SOURCE_DIR}/vcpkg/scripts/buildsystems/vcpkg.cmake)
set(VCPKG_ROOT ${HPCC_SOURCE_DIR}/vcpkg)
set(VCPKG_OVERLAY_PORTS ${HPCC_SOURCE_DIR}/vcpkg/overlays)
set(VCPKG_INSTALLED_DIR "${VCPKG_FILES_DIR}/vcpkg_installed")
set(VCPKG_INSTALL_OPTIONS "--downloads-root=${VCPKG_FILES_DIR}/vcpkg_downloads;--x-buildtrees-root=${VCPKG_FILES_DIR}/vcpkg_buildtrees;--x-packages-root=${VCPKG_FILES_DIR}/vcpkg_packages")
set(VCPKG_VERBOSE OFF)
file(REMOVE ${HPCC_SOURCE_DIR}/vcpkg/vcpkg)
if(APPLE OR WIN32)
elseif(UNIX)
    set(VCPKG_TARGET_TRIPLET "x64-linux-dynamic" CACHE STRING "target triplet" FORCE)
endif()

project(hpccsystems-platform LANGUAGES C CXX
    VERSION ${HPCC_MAJOR}.${HPCC_MINOR}.${HPCC_POINT}.${HPCC_SEQUENCE}
)

# Stupid workaround. See https://gitlab.kitware.com/cmake/cmake/-/issues/21378
# If cmake runs twice, cmake might not correctly set the compiler version variables
unset ( ENV{CC} )
unset ( ENV{CXX} )

set(TOP_LEVEL_PROJECT ON)
if(NOT CMAKE_PROJECT_NAME STREQUAL PROJECT_NAME)
    set(TOP_LEVEL_PROJECT OFF)
endif(NOT CMAKE_PROJECT_NAME STREQUAL PROJECT_NAME)

include(CTest)
enable_testing()

set(CMAKE_MODULE_PATH "${HPCC_SOURCE_DIR}/cmake_modules/")

###
## Build Level
###
if(NOT BUILD_LEVEL)
    set(BUILD_LEVEL "COMMUNITY")
endif(NOT BUILD_LEVEL)
if ("${BUILD_LEVEL}" STREQUAL "COMMUNITY")
    set ( HPCC_PROJECT_ABBR  "CC")
endif()

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
option(DESTDIR "Set the alternate root installation path.")

if(NOT LICENSE_FILE)
    if(REMBED)
        set(LICENSE_FILE "R-LICENSE.txt")
    else ()
        set(LICENSE_FILE "LICENSE.txt")
    endif()
endif()

include(${HPCC_SOURCE_DIR}/cmake_modules/optionDefaults.cmake)

set(CMAKE_INSTALL_MESSAGE LAZY)

if ( REMBED AND (NOT INCLUDE_PLUGINS) )
  # various components that are not needed if only making the R plugin
  SET(USE_LIBARCHIVE 0)
  SET(USE_APR 0)
endif()

include(${HPCC_SOURCE_DIR}/cmake_modules/commonSetup.cmake)

if ( PLUGIN )
    HPCC_ADD_SUBDIRECTORY (roxie/roxiemem)
    HPCC_ADD_SUBDIRECTORY (rtl/nbcd)
    HPCC_ADD_SUBDIRECTORY (rtl/eclrtl)
    HPCC_ADD_SUBDIRECTORY (system/jlib)
    HPCC_ADD_SUBDIRECTORY (dali/base)
    HPCC_ADD_SUBDIRECTORY (plugins/Rembed "REMBED")
    HPCC_ADD_SUBDIRECTORY (plugins/v8embed "V8EMBED")
    HPCC_ADD_SUBDIRECTORY (plugins/memcached "MEMCACHED")
    HPCC_ADD_SUBDIRECTORY (plugins/redis "REDIS")
    HPCC_ADD_SUBDIRECTORY (plugins/javaembed "JAVAEMBED")
    HPCC_ADD_SUBDIRECTORY (plugins/kafka "KAFKA")
    HPCC_ADD_SUBDIRECTORY (plugins/sqs "SQS")
    HPCC_ADD_SUBDIRECTORY (plugins/sqlite3 "SQLITE3EMBED")
    HPCC_ADD_SUBDIRECTORY (plugins/mysql "MYSQLEMBED")
    HPCC_ADD_SUBDIRECTORY (plugins/exampleplugin "EXAMPLEPLUGIN")
    HPCC_ADD_SUBDIRECTORY (plugins/couchbase "COUCHBASEEMBED")
    HPCC_ADD_SUBDIRECTORY (plugins/spark "SPARK")
    HPCC_ADD_SUBDIRECTORY (plugins/h3 "H3")
    HPCC_ADD_SUBDIRECTORY (plugins/nlp "NLP")
    HPCC_ADD_SUBDIRECTORY (plugins/mongodb "MONGODBEMBED")
elseif ( NOT MAKE_DOCS_ONLY )
    HPCC_ADD_SUBDIRECTORY (system)
    HPCC_ADD_SUBDIRECTORY (initfiles)
    HPCC_ADD_SUBDIRECTORY (tools)
    HPCC_ADD_SUBDIRECTORY (common)
    HPCC_ADD_SUBDIRECTORY (dali)
    HPCC_ADD_SUBDIRECTORY (ecl)
    HPCC_ADD_SUBDIRECTORY (ecllibrary)
    HPCC_ADD_SUBDIRECTORY (esp)
    HPCC_ADD_SUBDIRECTORY (fs)
    HPCC_ADD_SUBDIRECTORY (plugins)
    HPCC_ADD_SUBDIRECTORY (roxie)
    HPCC_ADD_SUBDIRECTORY (rtl)
    HPCC_ADD_SUBDIRECTORY (services "PLATFORM")
    HPCC_ADD_SUBDIRECTORY (thorlcr "PLATFORM")
    HPCC_ADD_SUBDIRECTORY (system/metrics)
    HPCC_ADD_SUBDIRECTORY (testing)
    if (NOT CONTAINERIZED)
      HPCC_ADD_SUBDIRECTORY (deploy)
      HPCC_ADD_SUBDIRECTORY (deployment)
      HPCC_ADD_SUBDIRECTORY (configuration)
    endif()
    
    if ( WIN32 )
        HPCC_ADD_SUBDIRECTORY (clienttools/IDEPlugins "CLIENTTOOLS_ONLY")
        HPCC_ADD_SUBDIRECTORY (clienttools/win "CLIENTTOOLS_ONLY")
    else()
        HPCC_ADD_SUBDIRECTORY (clienttools "CLIENTTOOLS_ONLY")
   endif()
endif()

HPCC_ADD_SUBDIRECTORY(docs "PLATFORM")

if(APPLE)
    HPCC_ADD_SUBDIRECTORY(package)
endif(APPLE)

###
## CPack install and packaging setup.
###
if ( WIN32 )
    set ( CMAKE_INSTALL_SYSTEM_RUNTIME_LIBS_SKIP TRUE )
    if (MSVC_VERSION EQUAL 1930 AND MSVC_TOOLSET_VERSION EQUAL 142)
        cmake_host_system_information(RESULT VS_DIR QUERY VS_17_DIR)
        file(GLOB MSVC_REDIST_LIBRARIES "${VS_DIR}/VC/Redist/MSVC/*/${MSVC_C_ARCHITECTURE_ID}/Microsoft.VC143.CRT/*.dll")
        list(APPEND CMAKE_INSTALL_SYSTEM_RUNTIME_LIBS "${MSVC_REDIST_LIBRARIES}")
        set(CMAKE_INSTALL_SYSTEM_RUNTIME_LIBS_NO_WARNINGS TRUE)
    endif()
    include(InstallRequiredSystemLibraries)
    if (MSVC_VERSION GREATER_EQUAL 1900)
        message("MSVC_REDIST_DIR: ${MSVC_REDIST_DIR} (${CMAKE_MSVC_ARCH})")
        string(REPLACE 2022 2019 MSVC2019_REDIST_DIR ${MSVC_REDIST_DIR})
        find_file(MSVC_REDIST "vcredist_${CMAKE_MSVC_ARCH}.exe" HINTS ${MSVC_REDIST_DIR} ${MSVC2019_REDIST_DIR})
        if (NOT EXISTS "${MSVC_REDIST}")
            find_file(MSVC_REDIST "vc_redist.${CMAKE_MSVC_ARCH}.exe" HINTS ${MSVC_REDIST_DIR} ${MSVC2019_REDIST_DIR})
        endif ()
        if (EXISTS "${MSVC_REDIST}")
            install ( PROGRAMS ${MSVC_REDIST} DESTINATION tmp )
            get_filename_component(MSVC_REDIST_NAME ${MSVC_REDIST} NAME)
            list ( APPEND CPACK_NSIS_EXTRA_INSTALL_COMMANDS " 
                ExecWait '$INSTDIR\\\\tmp\\\\${MSVC_REDIST_NAME} /S'
            ")
        else ()
            MESSAGE(FATAL_ERROR "-- Unable to locate vcredist_${CMAKE_MSVC_ARCH}.exe")
        endif ()
    else ()
        MESSAGE(FATAL_ERROR "-- Unknown compiler version")
    endif ()
endif ()

set(VER_SEPARATOR "-")
if("${stagever}" MATCHES "^rc[0-9]+$")
    set(VER_SEPARATOR "~")
endif()

if(TOP_LEVEL_PROJECT)
    if(PLUGIN)
        set(CPACK_PACKAGE_NAME "hpccsystems-plugin-${pluginname}")
        set(PACKAGE_FILE_NAME_PREFIX "hpccsystems-plugin-${pluginname}")
    elseif(PLATFORM)
        set(CPACK_PACKAGE_NAME "hpccsystems-platform")
        set(PACKAGE_FILE_NAME_PREFIX "hpccsystems-platform-${projname}")
    else()
        set(CPACK_PACKAGE_NAME "hpccsystems-clienttools-${majorver}.${minorver}")
        set(PACKAGE_FILE_NAME_PREFIX  "hpccsystems-clienttools-${projname}")
    endif()
    if(NOT "${CUSTOM_PACKAGE_SUFFIX}" STREQUAL "")
        set(CPACK_PACKAGE_NAME "${CPACK_PACKAGE_NAME}-${CUSTOM_PACKAGE_SUFFIX}")
        set(PACKAGE_FILE_NAME_PREFIX "${PACKAGE_FILE_NAME_PREFIX}-${CUSTOM_PACKAGE_SUFFIX}")
    endif()

    set(CPACK_PACKAGE_VERSION_MAJOR ${majorver})
    set(CPACK_PACKAGE_VERSION_MINOR ${minorver})
    set(CPACK_PACKAGE_VERSION_PATCH ${point}${VER_SEPARATOR}${stagever})
    set(CPACK_PACKAGE_VERSION ${CPACK_PACKAGE_VERSION_MAJOR}.${CPACK_PACKAGE_VERSION_MINOR}.${CPACK_PACKAGE_VERSION_PATCH})
    set(CPACK_PACKAGE_CONTACT "HPCCSystems <ossdevelopment@lexisnexis.com>")
    set(CPACK_SOURCE_GENERATOR TGZ)

    set(CPACK_RPM_PACKAGE_VERSION "${version}")
    set(CPACK_RPM_PACKAGE_RELEASE "${stagever}")
    set(CPACK_RPM_PACKAGE_VENDOR "HPCC Systems®")
    set(CPACK_PACKAGE_VENDOR "HPCC Systems®")
    set(CPACK_PACKAGE_VENDOR_WITHOUT_TRADEMARK "HPCC Systems")

    if(WIN32)
        set(CPACK_PACKAGE_VENDOR "HPCC Systems")
        if(${ARCH64BIT} EQUAL 1)
            set(CPACK_RPM_PACKAGE_ARCHITECTURE "x86_64")
        else(${ARCH64BIT} EQUAL 1)
            set(CPACK_RPM_PACKAGE_ARCHITECTURE "i386")
        endif(${ARCH64BIT} EQUAL 1)
    else(WIN32)
        set(CPACK_RPM_PACKAGE_ARCHITECTURE ${CMAKE_SYSTEM_PROCESSOR})
        if("${CPACK_RPM_PACKAGE_ARCHITECTURE}" STREQUAL "i686")
            set(CPACK_RPM_PACKAGE_ARCHITECTURE "i386")
        endif()
    endif(WIN32)

    set(CPACK_SYSTEM_NAME "${CMAKE_SYSTEM_NAME}-${CPACK_RPM_PACKAGE_ARCHITECTURE}")
    if("${CMAKE_BUILD_TYPE}" STREQUAL "Release")
        set(CPACK_STRIP_FILES TRUE)
    endif()

    if (NOT CPACK_STRIP_FILES) 
        set(packageStrippedLabel "_withsymbols")
    endif()

    if (CONTAINERIZED) 
        set(packageContainerizedLabel "_k8s")
    endif()
    
    if(UNIX AND NOT APPLE)
        execute_process(
            COMMAND ${HPCC_SOURCE_DIR}/cmake_modules/distrocheck.sh
            OUTPUT_VARIABLE packageManagement
            ERROR_VARIABLE  packageManagement
            )
        execute_process(
            COMMAND ${HPCC_SOURCE_DIR}/cmake_modules/getpackagerevisionarch.sh
            OUTPUT_VARIABLE packageRevisionArch
            ERROR_VARIABLE  packageRevisionArch
            )
        execute_process(
            COMMAND ${HPCC_SOURCE_DIR}/cmake_modules/getpackagerevisionarch.sh --noarch
            OUTPUT_VARIABLE packageRevision
            ERROR_VARIABLE  packageRevision
            )

        message("-- Auto Detecting Packaging type")
        message("-- distro uses ${packageManagement}, revision is ${packageRevisionArch}")

        if("${packageManagement}" STREQUAL "DEB")
            set(CPACK_PACKAGE_FILE_NAME "${PACKAGE_FILE_NAME_PREFIX}_${CPACK_RPM_PACKAGE_VERSION}-${stagever}${packageRevisionArch}${packageStrippedLabel}${packageContainerizedLabel}")
        elseif("${packageManagement}" STREQUAL "RPM")
            set(CPACK_PACKAGE_FILE_NAME "${PACKAGE_FILE_NAME_PREFIX}_${CPACK_RPM_PACKAGE_VERSION}-${stagever}.${packageRevisionArch}${packageStrippedLabel}${packageContainerizedLabel}")
        else()
            set(CPACK_PACKAGE_FILE_NAME "${PACKAGE_FILE_NAME_PREFIX}_${CPACK_RPM_PACKAGE_VERSION}_${stagever}${CPACK_SYSTEM_NAME}${packageStrippedLabel}${packageContainerizedLabel}")
        endif()
    endif ()

    message("-- Current release version is ${CPACK_PACKAGE_FILE_NAME}")
    set(CPACK_SOURCE_PACKAGE_FILE_NAME "${PACKAGE_FILE_NAME_PREFIX}_${CPACK_RPM_PACKAGE_VERSION}-${stagever}")
    set(CPACK_SOURCE_GENERATOR TGZ)
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
endif(TOP_LEVEL_PROJECT)

###
## Run file configuration to set build tag along with install lines for generated
## config files.
###
if(NOT BUILD_TAG)
    set(BUILD_TAG "${projname}_${version}-${stagever}")
    if(USE_GIT_DESCRIBE OR CHECK_GIT_TAG)
        FETCH_GIT_TAG(${CMAKE_SOURCE_DIR} ${projname}_${version} GIT_BUILD_TAG)
        message("-- Git tag is '${GIT_BUILD_TAG}'")
        if(NOT "${GIT_BUILD_TAG}" STREQUAL "${BUILD_TAG}")
            if(CHECK_GIT_TAG)
                message(FATAL_ERROR "Git tag '${GIT_BUILD_TAG}' does not match source version '${BUILD_TAG}'")
            else()
                if(NOT "${GIT_BUILD_TAG}" STREQUAL "") # probably means being built from a tarball...
                    set(BUILD_TAG "${BUILD_TAG}[${GIT_BUILD_TAG}]")
                endif()
            endif()
        endif()
    endif()
endif()
message("-- Build tag is '${BUILD_TAG}'")
if(NOT "${BASE_BUILD_TAG}" STREQUAL "")
    set(BASE_BUILD_TAG "${BUILD_TAG}")
endif()
message("-- Base build tag is '${BASE_BUILD_TAG}'")
configure_file(${HPCC_SOURCE_DIR}/build-config.h.cmake "build-config.h")

if(USE_SHLIBDEPS)
    set(CPACK_DEBIAN_PACKAGE_SHLIBDEPS ON)
else()
    set(CPACK_DEBIAN_PACKAGE_SHLIBDEPS OFF)
endif()
#set( CPACK_DEB_PACKAGE_COMPONENT ON )

if(TOP_LEVEL_PROJECT)
    if (UNIX AND NOT APPLE)
        if("${packageManagement}" STREQUAL "DEB")
            set(CPACK_GENERATOR "${packageManagement}")
            message("-- Will build DEB package")
            ###
            ## CPack instruction required for Debian
            ###
            message("-- Packing BASH installation files")
            if(CLIENTTOOLS_ONLY)
                set(CPACK_DEBIAN_PACKAGE_CONTROL_EXTRA
"${CMAKE_CURRENT_BINARY_DIR}/clienttools/install/postinst;${CMAKE_CURRENT_BINARY_DIR}/clienttools/install/prerm; ${CMAKE_CURRENT_BINARY_DIR}/clienttools/install/postrm")
            endif(CLIENTTOOLS_ONLY)
            if(PLATFORM)
                set(CPACK_DEBIAN_PACKAGE_CONTROL_EXTRA "${CMAKE_CURRENT_BINARY_DIR}/initfiles/bash/sbin/deb/postinst;${CMAKE_CURRENT_BINARY_DIR}/initfiles/sbin/prerm;${CMAKE_CURRENT_BINARY_DIR}/initfiles/bash/sbin/deb/postrm")
            endif(PLATFORM)

            # Standard sections values:
            # https://www.debian.org/doc/debian-policy/ch-archive.html/#s-subsections
            set(CPACK_DEBIAN_PACKAGE_SECTION "devel")

        elseif("${packageManagement}" STREQUAL "RPM")
            set(CPACK_GENERATOR "${packageManagement}")
            ###
            ## CPack instruction required for RPM
            ###
            set(CPACK_RPM_SPEC_MORE_DEFINE
"%define _use_internal_dependency_generator 0
%define __getdeps() while read file; do /usr/lib/rpm/rpmdeps -%{1} ${file} | %{__grep} -v libantlr3c.so | %{__grep} -v libelasticlient.so | %{__grep} -v libcpr.so | %{__grep} -v libjsoncpp.so | %{__grep} -v libtbb.so ; done | /bin/sort -u
%define __find_provides /bin/sh -c '%{__getdeps P}'
%define __find_requires /bin/sh -c '%{__getdeps R}'
%undefine __brp_mangle_shebangs
%global __os_install_post %(echo '%{__os_install_post}' | sed -e 's!/usr/lib[^[:space:]]*/brp-python-bytecompile[[:space:]].*$!!g')")
            message("-- Will build RPM package")
            message("-- Packing BASH installation files")
            if(CLIENTTOOLS_ONLY)
                set(CPACK_RPM_POST_INSTALL_SCRIPT_FILE "${CMAKE_CURRENT_BINARY_DIR}/clienttools/install/postinst")
                set(CPACK_RPM_PRE_UNINSTALL_SCRIPT_FILE "${CMAKE_CURRENT_BINARY_DIR}/clienttools/install/prerm")
                set(CPACK_RPM_POST_UNINSTALL_SCRIPT_FILE "${CMAKE_CURRENT_BINARY_DIR}/clienttools/install/postrm")
                set(CPACK_RPM_PACKAGE_GROUP "development/libraries")
                set(CPACK_RPM_PACKAGE_SUMMARY "HPCC Systems® Client Tools.")
            endif()
            if(PLATFORM)
                set(CPACK_RPM_POST_INSTALL_SCRIPT_FILE "${CMAKE_CURRENT_BINARY_DIR}/initfiles/bash/sbin/deb/postinst")
                set(CPACK_RPM_PRE_UNINSTALL_SCRIPT_FILE "${CMAKE_CURRENT_BINARY_DIR}/initfiles/sbin/prerm")
                set(CPACK_RPM_POST_UNINSTALL_SCRIPT_FILE "${CMAKE_CURRENT_BINARY_DIR}/initfiles/bash/sbin/deb/postrm")
                # Standard group names: http://fedoraroject.org/wiki/RPMGroups
                set(CPACK_RPM_PACKAGE_GROUP "development/system")
                set(CPACK_RPM_PACKAGE_SUMMARY "${PACKAGE_FILE_NAME_PREFIX}")
            endif() 
            set(CPACK_RPM_EXCLUDE_FROM_AUTO_FILELIST_ADDITION /usr/etc)
        else()
            message("WARNING: Unsupported package ${packageManagement}.")
        endif ()
    endif(UNIX AND NOT APPLE)

    if(PLUGIN)
        if("${packageManagement}" STREQUAL "RPM")
            SET_DEPENDENCIES(CPACK_RPM_PACKAGE_REQUIRES "hpccsystems-platform = ${CPACK_RPM_PACKAGE_VERSION}")
        elseif("${packageManagement}" STREQUAL "DEB")
            SET_DEPENDENCIES(CPACK_DEBIAN_PACKAGE_DEPENDS "hpccsystems-platform (= ${CPACK_PACKAGE_VERSION})")
        else()
            message(WARNING "Plugin Static Dependencies not set")
        endif()
    else()
        if(EXISTS ${HPCC_SOURCE_DIR}/cmake_modules/dependencies/${packageRevision}.cmake)
            include(${HPCC_SOURCE_DIR}/cmake_modules/dependencies/${packageRevision}.cmake)
        else()
            message("-- WARNING: DEPENDENCY FILE FOR ${packageRevision} NOT FOUND, Using deps template.")
            include(${HPCC_SOURCE_DIR}/cmake_modules/dependencies/template.cmake)
        endif()
    endif()

    if(APPLE)
        set(CPACK_PACKAGING_INSTALL_PREFIX "/opt/HPCCSystems/${CPACK_PACKAGE_VERSION}/clienttools")
    elseif(UNIX)
        set(CPACK_PACKAGING_INSTALL_PREFIX "${CMAKE_INSTALL_PREFIX}")
    endif(APPLE)

    if(PLATFORM OR PLUGIN)
        set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "${PACKAGE_FILE_NAME_PREFIX}")
    else()
        if (APPLE)
            set(CPACK_GENERATOR "productbuild")
        endif ()
        if(APPLE OR WIN32)
            set(CPACK_PACKAGE_FILE_NAME "${PACKAGE_FILE_NAME_PREFIX}_${version}-${stagever}${CPACK_SYSTEM_NAME}${packageStrippedLabel}${packageContainerizedLabel}")
        endif()
        file(WRITE "${PROJECT_BINARY_DIR}/welcome.txt"
            "HPCC Systems® - Client Tools\r"
            "===========================\r\r"
            "This installer will install the HPCC Systems® Client Tools.")
        set(CPACK_RESOURCE_FILE_README "${PROJECT_BINARY_DIR}/welcome.txt")
        set(CPACK_RESOURCE_FILE_LICENSE "${HPCC_SOURCE_DIR}/${LICENSE_FILE}")
        set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "HPCC Systems® Client Tools.")
        if(WIN32)
            set(CPACK_MONOLITHIC_INSTALL TRUE)
            if("${SIGN_DIRECTORY}" STREQUAL "")
                set(SIGN_DIRECTORY "${HPCC_SOURCE_DIR}/../sign")
            endif()

            set(CPACK_NSIS_DISPLAY_NAME "Client Tools")
            set(CPACK_PACKAGE_INSTALL_DIRECTORY "${DIR_NAME}\\\\${version}\\\\clienttools")
            set(CPACK_PACKAGE_INSTALL_REGISTRY_KEY "clienttools_${version}")
            set(CPACK_NSIS_ENABLE_UNINSTALL_BEFORE_INSTALL "ON")
            set(CPACK_NSIS_URL_INFO_ABOUT "http:\\\\\\\\hpccsystems.com")
            set(CPACK_NSIS_CONTACT ${CPACK_PACKAGE_CONTACT})
            set(CPACK_NSIS_DEFINES "
                !define MUI_STARTMENUPAGE_DEFAULTFOLDER \\\"${CPACK_PACKAGE_VENDOR_WITHOUT_TRADEMARK}\\\\${version}\\\\${CPACK_NSIS_DISPLAY_NAME}\\\"
                !define MUI_FINISHPAGE_NOAUTOCLOSE
            ")

           list ( APPEND CPACK_NSIS_CREATE_ICONS_EXTRA "
                CreateShortCut \\\"$SMPROGRAMS\\\\$STARTMENU_FOLDER\\\\ECL Command Prompt.lnk\\\" '%comspec%' '/k \\\"$INSTDIR\\\\ECLCmd.bat\\\"'
           ")
           list ( APPEND CPACK_NSIS_DELETE_ICONS_EXTRA "
                Delete \\\"$SMPROGRAMS\\\\$MUI_TEMP\\\\ECL CMD.lnk\\\"
           ")

            set(CPACK_NSIS_MODIFY_PATH "ON")

            if (EXISTS "${SIGN_DIRECTORY}/passphrase.txt")

                file(STRINGS "${SIGN_DIRECTORY}/passphrase.txt" PFX_PASSWORD LIMIT_COUNT 1)

                add_custom_target(SIGN
                    COMMAND signtool sign /f "${SIGN_DIRECTORY}/hpcc_code_signing.pfx" /fd "SHA256" /p "${PFX_PASSWORD}" /tr "http://timestamp.digicert.com" "${CMAKE_BINARY_DIR}/${PACKAGE_FILE_NAME_PREFIX}*.exe"
                    COMMENT "Digital Signature"
                )
                add_dependencies(SIGN PACKAGE)
                set_property(TARGET SIGN PROPERTY FOLDER "CMakePredefinedTargets")
            endif()
        endif()
    endif(PLATFORM OR PLUGIN)
endif(TOP_LEVEL_PROJECT)


###
## Below are the non-compile based install scripts required for
## the hpcc platform.
###
if(PLATFORM OR CLIENTTOOLS OR REMBED)
    install(FILES ${HPCC_SOURCE_DIR}/${LICENSE_FILE} DESTINATION "." COMPONENT Runtime)
endif()

add_subdirectory(devel)

#uninstall target
configure_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/cmake_uninstall.cmake.in"
    "${CMAKE_CURRENT_BINARY_DIR}/cmake_uninstall.cmake"
    IMMEDIATE @ONLY)
add_custom_target(uninstall
    COMMAND ${CMAKE_COMMAND} -P ${CMAKE_CURRENT_BINARY_DIR}/cmake_uninstall.cmake)

add_custom_target(localinstall
    COMMAND ${CMAKE_MAKE_PROGRAM} install
    )

include(CPack)
