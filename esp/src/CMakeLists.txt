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

set ( ECLWATCH_BUILD_STRATEGY "" CACHE STRING "Override ECL Watch Build Strategy ('SKIP', 'IF_MISSING', 'NO_COMPRESS')" )
if ( "${ECLWATCH_BUILD_STRATEGY}" STREQUAL "SKIP" )
    message ( "-- ECL Watch:  Skipping..." )
else ()
    message ( "-- ECL Watch:  Packaging Optimized/Built Sources..." )

    set ( ECLWATCH_BUILD_TMP "${CMAKE_CURRENT_BINARY_DIR}/tmp" )
    set ( ECLWATCH_BUILD_DEST "${CMAKE_CURRENT_BINARY_DIR}/build" )
    set ( ECLWATCH_BUILD_INSTALL_OUT "${CMAKE_CURRENT_BINARY_DIR}/build_install.txt" )
    set ( ECLWATCH_BUILD_COPY_OUT "${CMAKE_CURRENT_BINARY_DIR}/build_copy.txt" )
    set ( ECLWATCH_BUILD_BUNDLE_OUT "${CMAKE_CURRENT_BINARY_DIR}/build_bundle.txt" )
    set ( ECLWATCH_BUILD_OUT "${CMAKE_CURRENT_BINARY_DIR}/build.txt" )

    install ( CODE "
        if ( ( NOT \"${ECLWATCH_BUILD_STRATEGY}\" STREQUAL \"IF_MISSING\" ) OR ( NOT EXISTS \"${ECLWATCH_BUILD_DEST}\" ) )

            set (NODE_ERROR \"\")
            execute_process(COMMAND node --version OUTPUT_VARIABLE _VERSION RESULT_VARIABLE _NODE_VERSION_RESULT)
            if (NOT _NODE_VERSION_RESULT)
                string (REPLACE \"v\" \"\" NODE_VERSION_STRING \"\${_VERSION}\")
                string (REPLACE \".\" \";\" _VERSION_LIST \"\${NODE_VERSION_STRING}\")
                list (GET _VERSION_LIST 0 NODE_VERSION_MAJOR)
                if (NODE_VERSION_MAJOR LESS 8)
                    set (NODE_ERROR \"Node version \${NODE_VERSION_MAJOR} is too old, please install NodeJS as per https://github.com/hpcc-systems/HPCC-Platform/wiki/Building-HPCC#prerequisites\" )
                endif ()
            else ()
                set (NODE_ERROR \"Unable to locate node/npm, please install NodeJS as per https://github.com/hpcc-systems/HPCC-Platform/wiki/Building-HPCC#prerequisites\" )
            endif ()

            if ( NOT \"\${NODE_ERROR}\" STREQUAL \"\")
                message ( FATAL_ERROR \"\${NODE_ERROR}\" )
            else ()
                if ( (\"${CMAKE_BUILD_TYPE}\" STREQUAL \"Debug\") OR (\"${ECLWATCH_BUILD_STRATEGY}\" STREQUAL \"NO_COMPRESS\") )
                    set ( ECLWATCH_BUILD_MODE \"dev\" )
                else ()
                    set ( ECLWATCH_BUILD_MODE \"prod\" )
                endif ()
                message ( \"-- ECL Watch:  Rebuilding Site\" )
                message ( \"---- Build Target: ${ECLWATCH_BUILD_DEST}\" )
                message ( \"---- Creating out of source build environment\" )
                execute_process ( COMMAND \"${CMAKE_COMMAND}\" -E remove_directory \"${ECLWATCH_BUILD_TMP}\")
                execute_process ( COMMAND \"${CMAKE_COMMAND}\" -E copy_directory \"${CMAKE_CURRENT_SOURCE_DIR}/.\" \"${ECLWATCH_BUILD_TMP}\")
                execute_process ( COMMAND \"${CMAKE_COMMAND}\" -E remove_directory \"${ECLWATCH_BUILD_TMP}/node_modules\" )
                message ( \"---- Installing third party libraries\" )
                execute_process ( WORKING_DIRECTORY \"${ECLWATCH_BUILD_TMP}\" COMMAND \"npm\" \"install\" \"--loglevel=error\" \"--prefer-offline\" OUTPUT_FILE \"${ECLWATCH_BUILD_INSTALL_OUT}\" RESULT_VARIABLE _BUILD_RESULT )
                if (_BUILD_RESULT)
                    message ( FATA_ERROR \"npm install failed see above and ${ECLWATCH_BUILD_INSTALL_OUT} for details.\" )
                endif ()
                message ( \"---- Copy resources\" ) 
                execute_process ( WORKING_DIRECTORY \"${ECLWATCH_BUILD_TMP}\" COMMAND \"npm\" \"-s\" \"run\" \"copy-res\" OUTPUT_FILE \"${ECLWATCH_BUILD_COPY_OUT}\" RESULT_VARIABLE _BUILD_RESULT )
                if (_BUILD_RESULT)
                    message ( FATAL_ERROR \"npm run copy-res failed see above and ${ECLWATCH_BUILD_COPY_OUT} for details.\" )
                endif ()
                message ( \"---- Build website\" )
                execute_process ( WORKING_DIRECTORY \"${ECLWATCH_BUILD_TMP}\" COMMAND \"npm\" \"-s\" \"run\" \"compile\" RESULT_VARIABLE _BUILD_RESULT )
                if (_BUILD_RESULT)
                    message ( FATAL_ERROR \"npm run compile failed see above for details.\" )
                endif ()
                execute_process ( WORKING_DIRECTORY \"${ECLWATCH_BUILD_TMP}\" COMMAND \"npm\" \"-s\" \"run\" \"bundle\" \"--\" \"--bail\" \"--display\" \"errors-only\" \"--env.build\" \"${ECLWATCH_BUILD_MODE}\" OUTPUT_FILE \"${ECLWATCH_BUILD_BUNDLE_OUT}\" RESULT_VARIABLE _BUILD_RESULT )
                if (_BUILD_RESULT)
                    message ( FATAL_ERROR \"npm run bundle failed see above and ${ECLWATCH_BUILD_BUNDLE_OUT} for details.\" )
                endif ()
                execute_process ( COMMAND \"${CMAKE_COMMAND}\" -E remove_directory \"${ECLWATCH_BUILD_DEST}\")
                execute_process ( COMMAND \"${CMAKE_COMMAND}\" -E copy_directory \"${ECLWATCH_BUILD_TMP}/build/.\" \"${ECLWATCH_BUILD_DEST}\")
                execute_process ( COMMAND \"${CMAKE_COMMAND}\" -E remove_directory \"${ECLWATCH_BUILD_TMP}\")
            endif ()
        else ()
            message( \"-- ECL Watch:  Reusing (old) Site\" )
        endif ()
    " COMPONENT Runtime)
    Install ( DIRECTORY "${ECLWATCH_BUILD_DEST}/" DESTINATION componentfiles/files COMPONENT Runtime USE_SOURCE_PERMISSIONS )
endif ()

