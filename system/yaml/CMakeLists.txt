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

if(WIN32)
    set(LIBYAML_OUTPUT_DIR ${CMAKE_BINARY_DIR}/bin)
else()
    set(LIBYAML_OUTPUT_DIR ${CMAKE_BINARY_DIR})
endif()

remove_definitions(-fvisibility=hidden)
include(ExternalProject)
ExternalProject_Add(
    libyaml
    SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/libyaml
    BINARY_DIR ${CMAKE_CURRENT_BINARY_DIR}/libyaml
    CMAKE_ARGS -DCMAKE_RUNTIME_OUTPUT_DIRECTORY=${LIBYAML_OUTPUT_DIR}
    BUILD_COMMAND ${CMAKE_MAKE_PROGRAM} LDFLAGS=-Wl,-rpath-link,${LIB_PATH} yaml
    INSTALL_COMMAND "")

add_library(yaml SHARED IMPORTED GLOBAL)
set_property(TARGET yaml PROPERTY IMPORTED_LOCATION ${CMAKE_CURRENT_BINARY_DIR}/libyaml/libyaml${CMAKE_SHARED_LIBRARY_SUFFIX})
add_dependencies(yaml libyaml)

if ( NOT PLUGIN )
    install(CODE "set(ENV{LD_LIBRARY_PATH} \"\$ENV{LD_LIBRARY_PATH}:${CMAKE_CURRENT_BINARY_DIR}/libyaml:${PROJECT_BINARY_DIR}\")")
    install(PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/libyaml/libyaml${CMAKE_SHARED_LIBRARY_SUFFIX} DESTINATION lib)
    install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/libyaml/LICENSE RENAME libyaml-LICENSE.txt DESTINATION lib)
endif ( NOT PLUGIN )
