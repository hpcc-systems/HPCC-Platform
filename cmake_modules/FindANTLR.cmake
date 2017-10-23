################################################################################
# Copyright (C) 2013 HPCC Systems.
#
# All rights reserved. This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
################################################################################
# - Attempt to find the ANTLR jar needed to build and run ANTLR Lexers and Parsers
# Once done this will define
#
# ANTLR_FOUND - ANTLR found in local system
# ANTLR_BUILDTIME_JAR - The jar needed to build/generate ANTLR Lexers and Parsers
# ANTLR_RUNTIME_JAR - The jar needed to build/generate ANTLR Lexers and Parsers
################################################################################

include(UseJava)

option(ANTLR_VER "ANTLR runtime and buildtime version.")
if(NOT ANTLR_VER)
    set(ANTLR_VER 3.4)
endif()

option(ANTLR_BUILDTIME_DEP "ANTLR buildtime jar file name.")
if(NOT ANTLR_BUILDTIME_DEP)
    set(ANTLR_BUILDTIME_DEP "antlr-${ANTLR_VER}-complete")
    message(STATUS "Option ANTLR_BUILDTIME_DEP not set, setting to: ${ANTLR_BUILDTIME_DEP}")
    message(STATUS "The jar can be downloaded directly from:\n\thttps://github.com/antlr/website-antlr3/raw/gh-pages/download/antlr-3.4-complete.jar")
endif()

option(ANTLR_RUNTIME_DEP "ANTLR runtime jar file name.")
if(NOT ANTLR_RUNTIME_DEP)
    set(ANTLR_RUNTIME_DEP "antlr-runtime-${ANTLR_VER}")
    message(STATUS "Option ANTLR_RUNTIME_DEP not set, setting to: ${ANTLR_RUNTIME_DEP}")
    message(STATUS "The jar can be downloaded directly from:\n\thttps://github.com/antlr/website-antlr3/raw/gh-pages/download/antlr-runtime-3.4.jar")
endif()

option(ANTLR_PATH "Location of ANTLR runtime and buildtime jar files.")
if(NOT ANTLR_PATH)
    set(ANTLR_PATH "/usr/local/ANTLR/${ANTLR_VER}")
    message(STATUS "Option ANTLR_PATH not set, setting to: ${ANTLR_PATH}")
endif()

find_jar(ANTLR_RUNTIME_JAR ${ANTLR_RUNTIME_DEP} PATHS ${ANTLR_PATH})
find_jar(ANTLR_BUILDTIME_JAR ${ANTLR_BUILDTIME_DEP} PATHS ${ANTLR_PATH})

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
