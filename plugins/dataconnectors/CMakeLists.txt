project (hpccsystems-dataconnector)
cmake_minimum_required (VERSION 2.6)

set ( HPCC_DATACONNECTOR_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR})
set ( HPCC_SOURCE_DIR ${HPCC_DATACONNECTOR_SOURCE_DIR}/../../)
include(${HPCC_SOURCE_DIR}/version.cmake)

set ( CMAKE_MODULE_PATH "${HPCC_SOURCE_DIR}/cmake_modules")
set ( EXECUTABLE_OUTPUT_PATH "${CMAKE_BINARY_DIR}/${CMAKE_BUILD_TYPE}/bin" )
set ( PRODUCT_PREFIX "hpccsystems" )

SET(CMAKE_SKIP_BUILD_RPATH  FALSE)
SET(CMAKE_BUILD_WITH_INSTALL_RPATH FALSE)
SET(CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_PREFIX}/${OSSDIR}/lib")
SET(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

#include(${HPCC_SOURCE_DIR}/cmake_modules/optionDefaults.cmake)

if ("${CMAKE_BUILD_TYPE}" STREQUAL "")
    set ( CMAKE_BUILD_TYPE "Release" )
elseif (NOT "${CMAKE_BUILD_TYPE}" MATCHES "Debug|Release")
    message (FATAL_ERROR "Unknown build type $ENV{CMAKE_BUILD_TYPE}")
endif ()
message ("-- Making ${CMAKE_BUILD_TYPE} system")

if (CMAKE_SIZEOF_VOID_P EQUAL 8)
   set ( ARCH64BIT 1 )
else ()
   set ( ARCH64BIT 0 )
endif ()
message ("-- 64bit architecture is ${ARCH64BIT}")

set (CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -D_DEBUG -DDEBUG")

include(${HPCC_SOURCE_DIR}/cmake_modules/optionDefaults.cmake)
include(${HPCC_SOURCE_DIR}/cmake_modules/commonSetup.cmake)

INCLUDE(InstallRequiredSystemLibraries)
SET(CPACK_PACKAGE_VERSION_MAJOR ${majorver})
SET(CPACK_PACKAGE_VERSION_MINOR ${minorver})
SET(CPACK_PACKAGE_VERSION_PATCH ${point}${stagever})
set ( CPACK_PACKAGE_CONTACT "HPCCSystems <ossdevelopment@lexisnexis.com>" )
set( CPACK_SOURCE_GENERATOR TGZ )
set ( CPACK_RPM_PACKAGE_VERSION "${projname}")
SET(CPACK_RPM_PACKAGE_RELEASE "${version}${stagever}")
if ( ${ARCH64BIT} EQUAL 1 )
    set ( CPACK_RPM_PACKAGE_ARCHITECTURE "x86_64")
else( ${ARCH64BIT} EQUAL 1 )
    set ( CPACK_RPM_PACKAGE_ARCHITECTURE "i386")
endif ( ${ARCH64BIT} EQUAL 1 )
set(CPACK_SYSTEM_NAME "${CMAKE_SYSTEM_NAME}-${CPACK_RPM_PACKAGE_ARCHITECTURE}")


if ("${CMAKE_BUILD_TYPE}" STREQUAL "Release")
    set(CPACK_STRIP_FILES TRUE)
endif()

set ( CPACK_INSTALL_CMAKE_PROJECTS "${CMAKE_CURRENT_BINARY_DIR};hdfsconnector;ALL;/")

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
        set(CPACK_PACKAGE_FILE_NAME     "${CMAKE_PROJECT_NAME}_${CPACK_RPM_PACKAGE_VERSION}-${version}-${stagever}${packageRevisionArch}")
    elseif ( ${packageManagement} STREQUAL "RPM" )
        set(CPACK_PACKAGE_FILE_NAME     "${CMAKE_PROJECT_NAME}_${CPACK_RPM_PACKAGE_VERSION}-${version}-${stagever}.${packageRevisionArch}")
        else()
        set(CPACK_PACKAGE_FILE_NAME "${CMAKE_PROJECT_NAME}_${CPACK_RPM_PACKAGE_VERSION}_${version}-${stagever}${CPACK_SYSTEM_NAME}")
    endif ()
endif ( CMAKE_SYSTEM MATCHES Linux )

MESSAGE ("-- Current release version is ${CPACK_PACKAGE_FILE_NAME}")

set( CPACK_SOURCE_GENERATOR TGZ )

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

add_subdirectory (hdfsconnector)

INCLUDE(CPack)
