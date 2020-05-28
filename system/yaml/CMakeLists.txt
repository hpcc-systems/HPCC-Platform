################################################################################
#    HPCC SYSTEMS software Copyright (C) 2020 HPCC Systems®.
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

project( yaml_include)

remove_definitions(-fvisibility=hidden)

if(WIN32 OR APPLE)
    set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
else()
    set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})
endif()

add_subdirectory(${HPCC_SOURCE_DIR}/system/yaml/libyaml ${CMAKE_BINARY_DIR}/system/libyaml EXCLUDE_FROM_ALL)

if(NOT PLUGIN)
    if(WIN32)
        install(PROGRAMS ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/$<CONFIG>/yaml${CMAKE_SHARED_LIBRARY_SUFFIX}
            DESTINATION ${EXEC_DIR})
        install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/libyaml/LICENSE
            RENAME libyaml-LICENSE.txt
            DESTINATION ${EXEC_DIR})
    else()
        install(PROGRAMS ${LIBRARY_OUTPUT_PATH}/libyaml${CMAKE_SHARED_LIBRARY_SUFFIX}
            DESTINATION ${LIB_DIR})
        install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/libyaml/LICENSE
            RENAME libyaml-LICENSE.txt
            DESTINATION ${LIB_DIR})
    endif()
endif(NOT PLUGIN)
