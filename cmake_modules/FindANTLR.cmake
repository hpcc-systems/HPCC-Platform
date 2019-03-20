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
# - Attempt to find the ANTLR jar needed to build and run ANTLR Lexers and Parsers
# Once done this will define
#
# ANTLR_FOUND - ANTLR found in local system
# ANTLR_BUILDTIME_JAR - The jar needed to build/generate ANTLR Lexers and Parsers
# ANTLR_RUNTIME_JAR - The jar needed to build/generate ANTLR Lexers and Parsers
################################################################################

include(UseJava)

set(ANTLR_BUILDTIME_DEP "antlr-3.4-complete" CACHE STRING "ANTLR buildtime jar file name.")
set(ANTLR_RUNTIME_DEP "antlr-runtime-3.4" CACHE STRING "ANTLR runtime jar file name.")
set(ANTLR_PATH "${HPCC_SOURCE_DIR}/esp/services/ws_sql/website-antlr3/download" CACHE PATH "Location of ANTLR jar files.")
set(ANTLR_PKG_FIND_ERROR_MSG "Could not locate jars.\nPlease run `git submodule update --init --recursive`\n")


find_jar(ANTLR_BUILDTIME_JAR ${ANTLR_BUILDTIME_DEP} PATHS ${ANTLR_PATH})
find_jar(ANTLR_RUNTIME_JAR ${ANTLR_RUNTIME_DEP} PATHS ${ANTLR_PATH})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(
    ANTLR
    ${ANTLR_PKG_FIND_ERROR_MSG}
    ANTLR_BUILDTIME_JAR
    ANTLR_RUNTIME_JAR
    )
mark_as_advanced(ANTLR_BUILDTIME_JAR ANTLR_RUNTIME_JAR)

function(ANTLR_TARGET)
    # argument setup
    set(options "")
    set(oneValueArgs GRAMMAR_PREFIX DESTINATION)
    set(multiValueArgs GRAMMAR_FILES)
    cmake_parse_arguments(antlr "${options}" "${oneValueArgs}" "${multiValueArgs}" ${ARGN})
    list(GET ARGN 0 target_name)

    if(antlr_DESTINATION)
        # normalize path
        string(REGEX REPLACE "[/]$" "" antlr_DESTINATION "${antlr_DESTINATION}")
        set(antlr_options ${antlr_options} -o ${antlr_DESTINATION})
    else()
        set(antlr_options ${antlr_options} -o ${CMAKE_CURRENT_BINARY_DIR})
    endif()

    set(generated_sources
        ${antlr_DESTINATION}/${antlr_GRAMMAR_PREFIX}Lexer.c
        ${antlr_DESTINATION}/${antlr_GRAMMAR_PREFIX}Parser.c
        )
    set(generated_headers
        ${antlr_DESTINATION}/${antlr_GRAMMAR_PREFIX}Lexer.h
        ${antlr_DESTINATION}/${antlr_GRAMMAR_PREFIX}Parser.h
        )
    set(generated_misc
        ${antlr_DESTINATION}/${antlr_GRAMMAR_PREFIX}.tokens
        )

    add_custom_command(OUTPUT ${generated_sources} ${generated_headers}
        COMMAND ${Java_JAVA_EXECUTABLE} -jar ${ANTLR_BUILDTIME_JAR} ${antlr_GRAMMAR_FILES} ${antlr_options}
        COMMENT "Generated ANTLR3 Lexer and Parser from grammars"
        DEPENDS ${antlr_GRAMMAR_FILES}
        VERBATIM
        )
    add_custom_target("${target_name}"
        DEPENDS ${generated_sources} ${generated_headers}
        )
    set_source_files_properties(
        ${generated_sources}
        ${generated_headers}
        PROPERTIES GENERATED TRUE
        )
    if(antlr_DESTINATION)
        set_property(DIRECTORY PROPERTY INCLUDE_DIRECTORIES ${antlr_DESTINATION})
    endif()
    set_property(DIRECTORY PROPERTY
        ADDITIONAL_MAKE_CLEAN_FILES
            ${generated_sources}
            ${generated_headers}
            ${generated_misc}
        )
    set(ANTLR_${target_name}_OUTPUTS ${generated_sources} ${generated_headers} ${generated_misc} PARENT_SCOPE)
    set(ANTLR_${target_name}_SOURCES ${generated_sources} PARENT_SCOPE)
    set(ANTLR_${target_name}_HEADERS ${generated_headers} PARENT_SCOPE) 
    set(ANTLR_${target_name}_MISC    ${generated_misc}    PARENT_SCOPE) 
endfunction(ANTLR_TARGET)
