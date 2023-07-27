project ( portal-js ) 

file(REMOVE_RECURSE
    ${CMAKE_CURRENT_BINARY_DIR}/src
)

file(COPY
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${CMAKE_CURRENT_SOURCE_DIR}/package-lock.json
    ${CMAKE_CURRENT_SOURCE_DIR}/package.json
    DESTINATION ${CMAKE_CURRENT_BINARY_DIR})

set ( SRCS
    ${CMAKE_CURRENT_BINARY_DIR}/src/index.ts
    ${CMAKE_CURRENT_SOURCE_DIR}/package-lock.json
    ${CMAKE_CURRENT_SOURCE_DIR}/package.json
)

if ( "${CMAKE_BUILD_TYPE}" STREQUAL "Debug" )
    set ( PORTALJS_BUILD "build-dev" )
else ()
    set ( PORTALJS_BUILD "build" )
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

add_custom_target( portaljs_clean
    DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/no.file
)

add_custom_command (
    OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/node_modules/.package-lock.json
    COMMAND npm ci 
    # --silent
    DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/package-lock.json
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
)

add_custom_target( portaljs_fetchdeps
    DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/node_modules/.package-lock.json
)

add_custom_command (
    OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/dist/index.js
    COMMAND npm run --silent ${PORTALJS_BUILD}
    DEPENDS ${SRCS}
    DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/node_modules/.package-lock.json
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
)

add_custom_target ( portal-js ALL 
    DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/dist/index.js
)

add_dependencies ( portal-js portaljs_fetchdeps )

# install ( DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/build/." DESTINATION componentfiles/files COMPONENT Runtime USE_SOURCE_PERMISSIONS )
