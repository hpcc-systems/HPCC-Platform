###############################################################################
#    HPCC SYSTEMS software Copyright (C) 2023 HPCC Systems®.
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
#     This file constructs PACKAGE_FILE_NAME, to be used
#     as a package file name and as a prefix for the
#     vcpkg catalog file.
#
#   Default behavior assumes a platform build, with recognition of platform, plugin, and client
#   tool variants. Top level projects that are not one platform builds, or customized platform
#   builds, may override these default values:
#   - PACKAGE_PROJECT_NAME ["hpccsystems-platform"]
#   - PACKAGE_PROJECT ["${HPCC_PROJECT}"]
#   - PACKAGE_MAJOR_VERSION ["${HPCC_MAJOR}"]
#   - PACKAGE_MINOR_VERSION ["${HPCC_MINOR}"]
#   - PACKAGE_POINT_VERSION ["${HPCC_POINT}"]
#   - PACKAGE_MATURITY ["${HPCC_MATURITY}"]
#   - PACKAGE_SEQUENCE ["${HPCC_SEQUENCE}"]
#   - PACKAGE_VENDOR ["HPCC Systems" (WIN32) | "HPCC Systems®" (NOT WIN32)]
#   - PACKAGE_CONTACT ["HPCCSystems <ossdevelopment@lexisnexis.com>"]
#   - PACKAGE_FILE_NAME_PREFIX [N/A]
#   - CUSTOM_PACKAGE_SUFFIX [N/A]
#
#   Projects may define overridden values at any point prior to the inclusion of this file. A file
#   named ${CMAKE_SOURCE_DIR}/cmake_modules/prePackageSetup.cmake will be included immediately
#   before this file is included.
#
#   Variables defined on completion:
#   - PACKAGE_FILE_NAME
#   - PACKAGE_PROJECT_NAME
#   - PACKAGE_PROJECT
#   - PACKAGE_MAJOR_VERSION
#   - PACKAGE_MINOR_VERSION
#   - PACKAGE_POINT_VERSION
#   - PACKAGE_MATURITY
#   - PACKAGE_SEQUENCE
#   - PACKAGE_VENDOR
#   - PACKAGE_CONTACT
#   - PACKAGE_STAGE
#   - PACKAGE_PATCH_SEPARATOR
#   - PACKAGE_PATCH_VERSION
#   - PACKAGE_FILE_NAME_PREFIX
#   - RPM_PACKAGE_ARCHITECTURE
#   - PACKAGE_SYSTEM_NAME
#   - PACKAGE_STRIP_FILES
#   - PACKAGE_STRIPPED_LABEL
#   - PACKAGE_CONTAINERIZED_LABEL
#   - PACKAGE_MANAGEMENT
#   - PACKAGE_REVISION_ARCH
#   - PACKAGE_REVISION
#   - CPACK_PACKAGE_NAME
#   - CPACK_PACKAGE_FILE_NAME
#   - CPACK_PACKAGE_VERSION_MAJOR
#   - CPACK_PACKAGE_VERSION_MINOR
#   - CPACK_PACKAGE_VERSION_PATCH
#   - CPACK_PACKAGE_VERSION
#   - CPACK_PACKAGE_CONTACT
#   - CPACK_RPM_PACKAGE_VERSION
#   - CPACK_RPM_PACKAGE_RELEASE
#   - CPACK_PACKAGE_VENDOR
#   - CPACK_SOURCE_PACKAGE_FILE_NAME
#   - CPACK_SOURCE_GENERATOR
#   - CPACK_SOURCE_IGNORE_FILES
#########################################################

if ("${PACKAGE_FILE_NAME}" STREQUAL "")
    # ensure default values are set
    if ("${PACKAGE_PROJECT_NAME}" STREQUAL "")
        set(PACKAGE_PROJECT_NAME "hpccsystems-platform")
    endif()
    if ("${PACKAGE_PROJECT}" STREQUAL "")
        set(PACKAGE_PROJECT ${HPCC_PROJECT})
    endif()
    if ("${PACKAGE_MAJOR_VERSION}" STREQUAL "")
        set(PACKAGE_MAJOR_VERSION ${HPCC_MAJOR})
    endif()
    if ("${PACKAGE_MINOR_VERSION}" STREQUAL "")
        set(PACKAGE_MINOR_VERSION ${HPCC_MINOR})
    endif()
    if ("${PACKAGE_POINT_VERSION}" STREQUAL "")
        set(PACKAGE_POINT_VERSION ${HPCC_POINT})
    endif()
    if ("${PACKAGE_MATURITY}" STREQUAL "")
        set(PACKAGE_MATURITY ${HPCC_MATURITY})
    endif()
    if ("${PACKAGE_SEQUENCE}" STREQUAL "")
        set(PACKAGE_SEQUENCE ${HPCC_SEQUENCE})
    endif()
    if ("${PACKAGE_VENDOR}" STREQUAL "")
        if (WIN32)
            set(PACKAGE_VENDOR "HPCC Systems")
        else()
            set(PACKAGE_VENDOR "HPCC Systems®")
        endif()
    endif()
    if ("${PACKAGE_CONTACT}" STREQUAL "")
        set(PACKAGE_CONTACT "HPCCSystems <ossdevelopment@lexisnexis.com>")
    endif()

    # assemble compound version values
    set(PACKAGE_VERSION ${PACKAGE_MAJOR_VERSION}.${PACKAGE_MINOR_VERSION}.${PACKAGE_POINT_VERSION})
    if ("${PACKAGE_MATURITY}" STREQUAL "release")
        set(PACKAGE_STAGE "${PACKAGE_SEQUENCE}")
    else()
        set(PACKAGE_STAGE "${PACKAGE_MATURITY}${PACKAGE_SEQUENCE}")
    endif()
    if ("${CMAKE_BUILD_TYPE}" STREQUAL "Debug")
        set(PACKAGE_STAGE "${PACKAGE_STAGE}Debug" )
    endif()
    set(PACKAGE_PATCH_SEPARATOR "-")
    if ("${PACKAGE_STAGE}" MATCHES "^rc[0-9]+$")
        set(PACKAGE_PATCH_SEPARATOR "~")
    endif ()
    set(PACKAGE_PATCH_VERSION ${PACKAGE_POINT_VERSION}${PACKAGE_PATCH_SEPARATOR}${PACKAGE_STAGE})

    # assemble the file name prefix if not already set
    if ("${PACKAGE_FILE_NAME_PREFIX}" STREQUAL "")
        if (NOT "${PACKAGE_PROJECT_NAME}" STREQUAL "hpccsystems-platform")
            set(PACKAGE_FILE_NAME_PREFIX "${PACKAGE_PROJECT_NAME}")
            set(CPACK_PACKAGE_NAME "${PACKAGE_FILE_NAME_PREFIX}")
        elseif (PLUGIN)
            set(PACKAGE_FILE_NAME_PREFIX "hpccsystems-plugin-${pluginname}")
            set(CPACK_PACKAGE_NAME "${PACKAGE_FILE_NAME_PREFIX}")
        elseif (PLATFORM)
            set(PACKAGE_FILE_NAME_PREFIX "hpccsystems-platform-${PACKAGE_PROJECT}")
            set(CPACK_PACKAGE_NAME "${PACKAGE_FILE_NAME_PREFIX}")
        else ()
            set(PACKAGE_FILE_NAME_PREFIX "hpccsystems-clienttools-${PACKAGE_PROJECT}")
            set(CPACK_PACKAGE_NAME "hpccsystems-clienttools-${PACKAGE_MAJOR_VERSION}.${PACKAGE_MINOR_VERSION}")
        endif ()
    endif ("${PACKAGE_FILE_NAME_PREFIX}" STREQUAL "")
    if(NOT "${CUSTOM_PACKAGE_SUFFIX}" STREQUAL "")
        set(PACKAGE_FILE_NAME_PREFIX "${PACKAGE_FILE_NAME_PREFIX}-${CUSTOM_PACKAGE_SUFFIX}")
        set(CPACK_PACKAGE_NAME "${CPACK_PACKAGE_NAME}-${CUSTOM_PACKAGE_SUFFIX}")
    endif ()

    # identify the target environment
    if(WIN32)
        if(CMAKE_SIZEOF_VOID_P EQUAL 8)
            set(RPM_PACKAGE_ARCHITECTURE "x86_64")
        else(CMAKE_SIZEOF_VOID_P EQUAL 8)
            set(RPM_PACKAGE_ARCHITECTURE "i386")
        endif(CMAKE_SIZEOF_VOID_P EQUAL 8)
    else(WIN32)
        set(RPM_PACKAGE_ARCHITECTURE ${CMAKE_SYSTEM_PROCESSOR})
        if("${RPM_PACKAGE_ARCHITECTURE}" STREQUAL "i686")
            set(RPM_PACKAGE_ARCHITECTURE "i386")
        endif()
    endif(WIN32)

    set(PACKAGE_SYSTEM_NAME "${CMAKE_SYSTEM_NAME}-${RPM_PACKAGE_ARCHITECTURE}")
    if("${CMAKE_BUILD_TYPE}" STREQUAL "Release")
        set(PACKAGE_STRIP_FILES TRUE)
    endif()

    if (NOT PACKAGE_STRIP_FILES)
        set(PACKAGE_STRIPPED_LABEL "_withsymbols")
    endif()

    if (CONTAINERIZED)
        set(PACKAGE_CONTAINERIZED_LABEL "_k8s")
    endif()

    # construct the package name
    if(UNIX AND NOT APPLE)
        execute_process(
            COMMAND ${HPCC_SOURCE_DIR}/cmake_modules/distrocheck.sh
            OUTPUT_VARIABLE PACKAGE_MANAGEMENT
            ERROR_VARIABLE  PACKAGE_MANAGEMENT
            )
        execute_process(
            COMMAND ${HPCC_SOURCE_DIR}/cmake_modules/getpackagerevisionarch.sh
            OUTPUT_VARIABLE PACKAGE_REVISION_ARCH
            ERROR_VARIABLE  PACKAGE_REVISION_ARCH
            )
        execute_process(
            COMMAND ${HPCC_SOURCE_DIR}/cmake_modules/getpackagerevisionarch.sh --noarch
            OUTPUT_VARIABLE PACKAGE_REVISION
            ERROR_VARIABLE  PACKAGE_REVISION
            )

        if("${PACKAGE_MANAGEMENT}" STREQUAL "DEB")
            set(PACKAGE_FILE_NAME "${PACKAGE_FILE_NAME_PREFIX}_${PACKAGE_VERSION}-${PACKAGE_STAGE}${PACKAGE_REVISION_ARCH}${PACKAGE_STRIPPED_LABEL}${PACKAGE_CONTAINERIZED_LABEL}")
        elseif("${PACKAGE_MANAGEMENT}" STREQUAL "RPM")
            set(PACKAGE_FILE_NAME "${PACKAGE_FILE_NAME_PREFIX}_${PACKAGE_VERSION}-${PACKAGE_STAGE}.${PACKAGE_REVISION_ARCH}${PACKAGE_STRIPPED_LABEL}${PACKAGE_CONTAINERIZED_LABEL}")
        else()
            set(PACKAGE_FILE_NAME "${PACKAGE_FILE_NAME_PREFIX}_${PACKAGE_VERSION}_${PACKAGE_STAGE}${PACKAGE_SYSTEM_NAME}${PACKAGE_STRIPPED_LABEL}${PACKAGE_CONTAINERIZED_LABEL}")
        endif()
    elseif (APPLE OR WIN32)
        set(PACKAGE_FILE_NAME "${PACKAGE_FILE_NAME_PREFIX}_${PACKAGE_VERSION}-${PACKAGE_STAGE}${PACKAGE_SYSTEM_NAME}${PACKAGE_STRIPPED_LABEL}${PACKAGE_CONTAINERIZED_LABEL}")
    endif ()
    set(CPACK_PACKAGE_FILE_NAME ${PACKAGE_FILE_NAME})

    # configure related CPack values
    set(CPACK_PACKAGE_VERSION_MAJOR ${PACKAGE_MAJOR_VERSION})
    set(CPACK_PACKAGE_VERSION_MINOR ${PACKAGE_MINOR_VERSION})
    set(CPACK_PACKAGE_VERSION_PATCH ${PACKAGE_PATCH_VERSION})
    set(CPACK_PACKAGE_VERSION ${CPACK_PACKAGE_VERSION_MAJOR}.${CPACK_PACKAGE_VERSION_MINOR}.${CPACK_PACKAGE_VERSION_PATCH})
    set(CPACK_PACKAGE_CONTACT ${PACKAGE_CONTACT})
    set(CPACK_RPM_PACKAGE_VERSION ${PACKAGE_VERSION})
    set(CPACK_RPM_PACKAGE_RELEASE ${PACKAGE_STAGE})
    set(CPACK_PACKAGE_VENDOR ${PACKAGE_VENDOR})
    set(CPACK_SOURCE_PACKAGE_FILE_NAME "${PACKAGE_FILE_NAME_PREFIX}_${CPACK_RPM_PACKAGE_VERSION}-${PACKAGE_STAGE}")
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

endif("${PACKAGE_FILE_NAME}" STREQUAL "")
