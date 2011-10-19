################################################################################
#    Copyright (C) 2011 HPCC Systems.
#
#    All rights reserved. This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU Affero General Public License as
#    published by the Free Software Foundation, either version 3 of the
#    License, or (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU Affero General Public License for more details.
#
#    You should have received a copy of the GNU Affero General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.
################################################################################

cmake_minimum_required(VERSION 2.8)
PROJECT(initfiles)

ADD_EXECUTABLE(processor processor.cpp)

MACRO(GENERATE_BASH processor bash-vars in out)
    GET_TARGET_PROPERTY(processorLocation processor LOCATION)
    MESSAGE(STATUS "Process file: ${in}")
    STRING(REGEX REPLACE ".in\$" "" outfileName "${in}")
    SET(outfile "${CMAKE_CURRENT_BINARY_DIR}/${outfileName}")
    MESSAGE(STATUS "Output file: ${outfile}")
    SET(infile "${CMAKE_CURRENT_SOURCE_DIR}/${in}")
    ADD_CUSTOM_COMMAND(OUTPUT "${outfile}"
        COMMAND ${processorLocation}
        "${bash-vars}" "${infile}" "${outfile}"
        DEPENDS "${infile}" processor # depends on the 'processor'
        COMMENT "Generating: ${outfile}")
    SET(${out} ${${out}} "${outfile}")
ENDMACRO(GENERATE_BASH)

configure_file("${CMAKE_CURRENT_SOURCE_DIR}/bash-vars.in" "${CMAKE_BINARY_DIR}/bash-vars")
set(bash-vars "${CMAKE_BINARY_DIR}/bash-vars")

ADD_SUBDIRECTORY(bash)
ADD_SUBDIRECTORY(bin)
ADD_SUBDIRECTORY(sbin)
ADD_SUBDIRECTORY(componentfiles)

Install ( DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/examples DESTINATION ${OSSDIR} COMPONENT Runtime USE_SOURCE_PERMISSIONS PATTERN ".svn" EXCLUDE )
