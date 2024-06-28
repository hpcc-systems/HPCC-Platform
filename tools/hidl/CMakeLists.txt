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

# Component: hidl
#####################################################
# Description:
# ------------
#    Cmake Input File for hidl
#####################################################

project( hidl )

if(CMAKE_BUILD_TYPE STREQUAL "Debug" OR COLLECT_SERVICE_METRICS)
    add_definitions(-DENABLE_DEFAULT_EXECUTION_PROFILING)
endif()

add_custom_command ( OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/hidlgram.cpp ${CMAKE_CURRENT_BINARY_DIR}/hidlgram.h
    COMMAND ${bisoncmdprefix} ${bisoncmd} --defines=${CMAKE_CURRENT_BINARY_DIR}/hidlgram.h --output=${CMAKE_CURRENT_BINARY_DIR}/hidlgram.cpp ${CMAKE_CURRENT_SOURCE_DIR}/hidlgram.y
    DEPENDS hidlgram.y
)

add_custom_command ( OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/hidllex.cpp
    COMMAND ${flexcmdprefix} ${flexcmd} --outfile=${CMAKE_CURRENT_BINARY_DIR}/hidllex.cpp ${CMAKE_CURRENT_SOURCE_DIR}/hidllex.l
    DEPENDS hidllex.l
)

set (    SRCS
         ${CMAKE_CURRENT_BINARY_DIR}/hidlgram.cpp
         ${CMAKE_CURRENT_BINARY_DIR}/hidllex.cpp
         hidl_esp_ng.cpp
         hidl_utils.cpp
         hidlcomp.cpp
         main.cpp
    )

include_directories (
         ${CMAKE_CURRENT_BINARY_DIR}   # for generated .h file
         ${HPCC_SOURCE_DIR}/system/include
         ${CMAKE_CURRENT_SOURCE_DIR}
    )

set_source_files_properties (${CMAKE_CURRENT_BINARY_DIR}/hidlgram.cpp PROPERTIES COMPILE_FLAGS -fno-strict-aliasing)

SET (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${STRICT_CXX_FLAGS}")

if (CMAKE_COMPILER_IS_GNUCC OR CMAKE_COMPILER_IS_CLANG)
    set_source_files_properties(hidlgram.cpp PROPERTIES COMPILE_FLAGS "-Wno-free-nonheap-object")
endif ()

ADD_DEFINITIONS( -D_CONSOLE )
HPCC_ADD_EXECUTABLE ( hidl ${SRCS} )
#install ( TARGETS hidl RUNTIME DESTINATION ${EXEC_DIR} )
