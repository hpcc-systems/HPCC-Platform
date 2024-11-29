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

project ( eclwatch ) 

file(REMOVE_RECURSE
    ${CMAKE_CURRENT_BINARY_DIR}/packages
)

file(COPY
    ${CMAKE_CURRENT_SOURCE_DIR}/packages
    ${CMAKE_CURRENT_SOURCE_DIR}/lerna.json
    ${CMAKE_CURRENT_SOURCE_DIR}/package.json
    ${CMAKE_CURRENT_SOURCE_DIR}/package-lock.json
    DESTINATION ${CMAKE_CURRENT_BINARY_DIR})

set(LEGACY_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/packages/legacy)
set(LEGACY_BINARY_DIR ${CMAKE_CURRENT_BINARY_DIR}/packages/legacy)

configure_file(${LEGACY_SOURCE_DIR}/src/BuildInfo.ts ${LEGACY_BINARY_DIR}/src/BuildInfo.ts @ONLY)

file(GLOB_RECURSE SRCS
    ${CMAKE_CURRENT_BINARY_DIR}/packages/*
)

if ( "${CMAKE_BUILD_TYPE}" STREQUAL "Debug" )
    set ( ECLWATCH_BUILD "build-dev" )
else ()
    set ( ECLWATCH_BUILD "build" )
endif ()

set (NPM_ERROR "")
if (WIN32)
    execute_process(COMMAND cmd /c "npm --version" OUTPUT_VARIABLE _VERSION RESULT_VARIABLE _NPM_VERSION_RESULT)
else ()
    execute_process(COMMAND npm --version OUTPUT_VARIABLE _VERSION RESULT_VARIABLE _NPM_VERSION_RESULT)
endif ()
if (NOT _NPM_VERSION_RESULT)
    string (REPLACE "v" "" NPM_VERSION_STRING "${_VERSION}")
    string (REPLACE "." ";" _VERSION_LIST "${NPM_VERSION_STRING}")
    list (GET _VERSION_LIST 0 NPM_VERSION_MAJOR)
    if (NPM_VERSION_MAJOR LESS 6)
        set (NPM_ERROR "NPM version ${NPM_VERSION_MAJOR} is too old (expected >= 6), please install NodeJS as per https://github.com/hpcc-systems/HPCC-Platform/wiki/Building-HPCC#prerequisites" )
    endif ()
else ()
    set (NPM_ERROR "Unable to locate node/npm, please install NodeJS as per https://github.com/hpcc-systems/HPCC-Platform/wiki/Building-HPCC#prerequisites" )
endif ()

if ( NOT "${NPM_ERROR}" STREQUAL "")
    message ( FATAL_ERROR "${NPM_ERROR}" )
endif ()

add_custom_command (
    OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/no.file
    COMMAND npm run --silent clean
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
)

add_custom_target( eclwatch_clean
    DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/no.file
)

add_custom_command (
    OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/lib/package-lock.json
    COMMAND npm ci --silent && ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_BINARY_DIR}/package-lock.json ${CMAKE_CURRENT_BINARY_DIR}/lib/package-lock.json
    DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/package-lock.json
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
)

add_custom_target( eclwatch_fetchdeps
    DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/lib/package-lock.json
)

add_custom_command (
    OUTPUT ${LEGACY_BINARY_DIR}/build/dist/stub.eclwatch.js
    COMMAND npm run --silent ${ECLWATCH_BUILD}
    DEPENDS ${SRCS}
    DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/lib/package-lock.json
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
)

add_custom_target ( eclwatch ALL 
    DEPENDS ${LEGACY_BINARY_DIR}/build/dist/stub.eclwatch.js
)

add_dependencies ( eclwatch eclwatch_fetchdeps )

install ( DIRECTORY "${LEGACY_BINARY_DIR}/build/." DESTINATION componentfiles/files COMPONENT Runtime USE_SOURCE_PERMISSIONS )
