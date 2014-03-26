################################################################################
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

project ( eclwatch ) 

set ( ECLWATCH_BUILD_STRATEGY "" CACHE STRING "Override ECL Watch Build Strategy ('SKIP', 'SOURCES', 'IF_MISSING')" )
if ( "${ECLWATCH_BUILD_STRATEGY}" STREQUAL "SKIP" )
    message ( "-- ECL Watch:  Skipping..." )
elseif ( "${ECLWATCH_BUILD_STRATEGY}" STREQUAL "SOURCES" )
    message( "-- ECL Watch:  Packaging Sources..." )
    install ( DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/" DESTINATION componentfiles/files COMPONENT Runtime USE_SOURCE_PERMISSIONS PATTERN "CMakeLists.txt" EXCLUDE )
else ()
    message ( "-- ECL Watch:  Packaging Optimized/Built Sources..." )
    set ( ECLWATCH_BUILD_DEST "${CMAKE_CURRENT_BINARY_DIR}/build" )
    set ( ECLWATCH_BUILD_OUT "${CMAKE_CURRENT_BINARY_DIR}/eclwatch_build_out.txt" )
    set ( ECLWATCH_BUILD_ERR "${CMAKE_CURRENT_BINARY_DIR}/eclwatch_build_err.txt" )
    install ( CODE "
        if ( ( NOT \"${ECLWATCH_BUILD_STRATEGY}\" STREQUAL \"IF_MISSING\" ) OR ( NOT EXISTS \"${ECLWATCH_BUILD_DEST}\" ) )
            message ( \"-- ECL Watch:  Rebuilding Site\" )
            message ( \"---- Build Target: ${ECLWATCH_BUILD_DEST}\" )
            message ( \"---- Output log:   ${ECLWATCH_BUILD_OUT}\" )
            message ( \"---- Error log:    ${ECLWATCH_BUILD_ERR}\" )
            if ( NOT EXISTS \"${CMAKE_CURRENT_SOURCE_DIR}/util/buildscripts\" )
                message ( FATAL_ERROR \"Can't find Dojo build tools -- did you initialise submodules? (git submodule update --init --recursive)\" )
            else ()
                execute_process ( COMMAND \"${CMAKE_CURRENT_SOURCE_DIR}/../build.sh\" \"${ECLWATCH_BUILD_DEST}\" OUTPUT_FILE \"${ECLWATCH_BUILD_OUT}\" ERROR_FILE \"${ECLWATCH_BUILD_ERR}\" )
                execute_process ( COMMAND \"tail\" \"--lines=4\" \"${ECLWATCH_BUILD_DEST}/build-report.txt\" )
                message ( \"\" )
            endif ()
        else ()
            message( \"-- ECL Watch:  Reusing (old) Site\" )
        endif ()
    " COMPONENT Runtime)
    Install ( DIRECTORY "${ECLWATCH_BUILD_DEST}/" DESTINATION componentfiles/files COMPONENT Runtime USE_SOURCE_PERMISSIONS PATTERN "build-report.txt" EXCLUDE PATTERN "*.map" EXCLUDE  PATTERN "*.uncompressed.js" EXCLUDE )
endif ()

