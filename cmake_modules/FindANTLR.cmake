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

set(ANTLR_BUILDTIME_DEP_URL "https://github.com/antlr/website-antlr3/raw/gh-pages/download/antlr-3.4-complete.jar" CACHE STRING "Antlr3 Buildtime dependency url")
set(ANTLR_RUNTIME_DEP_URL   "https://github.com/antlr/website-antlr3/raw/gh-pages/download/antlr-runtime-3.4.jar" CACHE STRING "Antlr3 Buildtime dependency url")
set(ANTLR_BUILDTIME_DEP "antlr-3.4-complete" CACHE STRING "ANTLR buildtime jar file name.")
set(ANTLR_RUNTIME_DEP "antlr-runtime-3.4" CACHE STRING "ANTLR runtime jar file name.")
set(ANTLR_PATH "/usr/local/ANTLR/3.4" CACHE PATH "Location of ANTLR runtime and buildtime jar files.")

find_jar(ANTLR_BUILDTIME_JAR ${ANTLR_BUILDTIME_DEP} PATHS ${ANTLR_PATH})
if(ANTLR_BUILDTIME_JAR STREQUAL "ANTLR_BUILDTIME_JAR-NOTFOUND")
    file(DOWNLOAD ${ANTLR_BUILDTIME_DEP_URL}
        ${CMAKE_BINARY_DIR}/downloads/antlr-3.4-complete.jar
        TIMEOUT 20
        INACTIVITY_TIMEOUT 5)
    find_jar(ANTLR_BUILDTIME_JAR antlr-3.4-complete PATHS ${CMAKE_BINARY_DIR}/downloads)
endif()

find_jar(ANTLR_RUNTIME_JAR ${ANTLR_RUNTIME_DEP} PATHS ${ANTLR_PATH})
if(ANTLR_RUNTIME_JAR STREQUAL "ANTLR_RUNTIME_JAR-NOTFOUND")
    file(DOWNLOAD ${ANTLR_RUNTIME_DEP_URL}
        ${CMAKE_BINARY_DIR}/downloads/antlr-runtime-3.4.jar
        TIMEOUT 20
        INACTIVITY_TIMEOUT 5)
    find_jar(ANTLR_RUNTIME_JAR antlr-runtime-3.4 PATHS ${CMAKE_BINARY_DIR}/downloads)
endif()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(
    ANTLR DEFAULT_MSG
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
