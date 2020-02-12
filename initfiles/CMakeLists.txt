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

cmake_minimum_required(VERSION 2.8)
PROJECT(initfiles)

if ( PLATFORM AND UNIX )
    HPCC_ADD_EXECUTABLE(processor processor.cpp)

    MACRO(GENERATE_BASH processor bash-vars in out)
        MESSAGE(STATUS "Process file: ${in}")
        STRING(REGEX REPLACE ".in\$" "" outfileName "${in}")
        SET(outfile "${CMAKE_CURRENT_BINARY_DIR}/${outfileName}")
        MESSAGE(STATUS "Output file: ${outfile}")
        SET(infile "${CMAKE_CURRENT_SOURCE_DIR}/${in}")
        ADD_CUSTOM_COMMAND(OUTPUT "${outfile}"
            COMMAND $<TARGET_FILE:processor>
            "${bash-vars}" "${infile}" "${outfile}"
            COMMAND chmod 0755 ${outfile}
            DEPENDS "${infile}" processor # depends on the 'processor'
            COMMENT "Generating: ${outfile}")
        SET(${out} ${${out}} "${outfile}")
    ENDMACRO(GENERATE_BASH)

    configure_file("${CMAKE_CURRENT_SOURCE_DIR}/bash-vars.in" "${CMAKE_BINARY_DIR}/bash-vars")
    set(bash-vars "${CMAKE_BINARY_DIR}/bash-vars")

    ADD_SUBDIRECTORY(etc)
    ADD_SUBDIRECTORY(bash)
    ADD_SUBDIRECTORY(bin)
    ADD_SUBDIRECTORY(sbin)
endif ()

if ( PLATFORM OR CLIENTTOOLS_ONLY )
   ADD_SUBDIRECTORY(componentfiles)
endif ()

Install ( DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/examples DESTINATION "." COMPONENT Runtime USE_SOURCE_PERMISSIONS PATTERN ".svn" EXCLUDE )

find_package(Java 1.8 COMPONENTS Development)
set(example_java_sources examples/embed/JavaCat.java)
set(example_java_classes ${CMAKE_CURRENT_BINARY_DIR}/JavaCat.class ${CMAKE_CURRENT_BINARY_DIR}/JavaCat\$NestedClass.class)
add_custom_command ( OUTPUT ${example_java_classes}
    COMMAND ${Java_JAVAC_EXECUTABLE} ${example_java_sources} -d ${CMAKE_CURRENT_BINARY_DIR} -source 1.8 -target 1.8
    DEPENDS ${example_java_sources}
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)
add_custom_target(build-example-java-classes ALL
  DEPENDS ${example_java_classes}
)
Install ( FILES ${example_java_classes} DESTINATION "classes" COMPONENT Runtime )
