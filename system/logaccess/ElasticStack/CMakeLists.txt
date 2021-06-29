################################################################################
#    HPCC SYSTEMS software Copyright (C) 2021 HPCC Systems®.
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

project(ElasticStackLogAccess)


# elasticlient dependency submodule
include(ExternalProject)
ExternalProject_Add(
  elasticlient-build
  SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/elasticlient
  BINARY_DIR ${CMAKE_CURRENT_BINARY_DIR}/elasticlient
  CMAKE_ARGS -DBUILD_ELASTICLIENT_TESTS=OFF -DBUILD_ELASTICLIENT_EXAMPLE=OFF -DUSE_ALL_SYSTEM_LIBS=OFF -DUSE_SYSTEM_CURL=ON -DBUILD_CPR_TESTS=OFF
  BUILD_COMMAND ${CMAKE_MAKE_PROGRAM} cpr jsoncpp_lib elasticlient
  BUILD_BYPRODUCTS
    ${CMAKE_CURRENT_BINARY_DIR}/elasticlient/lib/libjsoncpp${CMAKE_SHARED_LIBRARY_SUFFIX}
    ${CMAKE_CURRENT_BINARY_DIR}/elasticlient/lib/libelasticlient${CMAKE_SHARED_LIBRARY_SUFFIX}
    ${CMAKE_CURRENT_BINARY_DIR}/elasticlient/lib/libcpr${CMAKE_SHARED_LIBRARY_SUFFIX}
  INSTALL_COMMAND "")

add_library(elasticlient SHARED IMPORTED GLOBAL)
set_property(TARGET elasticlient
  PROPERTY IMPORTED_LOCATION ${CMAKE_CURRENT_BINARY_DIR}/elasticlient/lib/libelasticlient${CMAKE_SHARED_LIBRARY_SUFFIX})
add_dependencies(elasticlient elasticlient-build)

add_library(jsoncpp SHARED IMPORTED GLOBAL)
set_property(TARGET jsoncpp
  PROPERTY IMPORTED_LOCATION ${CMAKE_CURRENT_BINARY_DIR}/elasticlient/lib/libjsoncpp${CMAKE_SHARED_LIBRARY_SUFFIX})
add_dependencies(jsoncpp elasticlient-build)

add_library(cpr SHARED IMPORTED GLOBAL)
set_property(TARGET cpr
  PROPERTY IMPORTED_LOCATION ${CMAKE_CURRENT_BINARY_DIR}/elasticlient/lib/libcpr${CMAKE_SHARED_LIBRARY_SUFFIX})
add_dependencies(cpr elasticlient-build)


# elastistacklogaccess library
# update cxx_flags to be less restrictive when building against elasticlient header files
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fpermissive")

set(srcs
  ${CMAKE_CURRENT_SOURCE_DIR}/ElasticStackLogAccess.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/elasticlient/include/elasticlient/client.h
)

include_directories(
  ${CMAKE_CURRENT_SOURCE_DIR}/elasticlient/include
  ${CMAKE_CURRENT_SOURCE_DIR}/elasticlient/external/jsoncpp/include
  ${CMAKE_CURRENT_SOURCE_DIR}/elasticlient/external/cpr/include
  ${HPCC_SOURCE_DIR}/system/include
  ${HPCC_SOURCE_DIR}/system/jlib
)

add_definitions(-DELASTICSTACKLOGACCESS_EXPORTS)
HPCC_ADD_LIBRARY(elasticstacklogaccess SHARED ${srcs})
target_link_libraries(elasticstacklogaccess
  jlib
  elasticlient
  jsoncpp
  cpr
)

install(TARGETS elasticstacklogaccess
  RUNTIME DESTINATION ${EXEC_DIR}
  LIBRARY DESTINATION ${LIB_DIR}
)


if(NOT PLUGIN)
  install(CODE "set(ENV{LD_LIBRARY_PATH} \"\$ENV{LD_LIBRARY_PATH}:${PROJECT_BINARY_DIR}:${PROJECT_BINARY_DIR}/elasticlient/lib\")")
  install(FILES
    ${CMAKE_CURRENT_SOURCE_DIR}/elasticlient/LICENSE.txt
    DESTINATION ${LIB_DIR}
    RENAME elasticlient-LICENSE.txt)
  install(CODE "file(RPATH_CHANGE
    FILE \"${CMAKE_CURRENT_BINARY_DIR}/elasticlient/lib/libelasticlient${CMAKE_SHARED_LIBRARY_SUFFIX}\"
    OLD_RPATH \"${PROJECT_BINARY_DIR}/elasticlient/lib:\"
    NEW_RPATH \"${LIB_PATH}\")")
  install(CODE "file(RPATH_CHANGE
    FILE \"${CMAKE_CURRENT_BINARY_DIR}/elasticlient/lib/libcpr${CMAKE_SHARED_LIBRARY_SUFFIX}\"
    OLD_RPATH \"${PROJECT_BINARY_DIR}/elasticlient/lib:\"
    NEW_RPATH \"${LIB_PATH}\")")
  install(FILES
    ${CMAKE_CURRENT_BINARY_DIR}/elasticlient/lib/libelasticlient${CMAKE_SHARED_LIBRARY_SUFFIX}.2.1.0
    ${CMAKE_CURRENT_BINARY_DIR}/elasticlient/lib/libelasticlient${CMAKE_SHARED_LIBRARY_SUFFIX}.2
    ${CMAKE_CURRENT_BINARY_DIR}/elasticlient/lib/libelasticlient${CMAKE_SHARED_LIBRARY_SUFFIX}
    ${CMAKE_CURRENT_BINARY_DIR}/elasticlient/lib/libjsoncpp${CMAKE_SHARED_LIBRARY_SUFFIX}.1.8.4
    ${CMAKE_CURRENT_BINARY_DIR}/elasticlient/lib/libjsoncpp${CMAKE_SHARED_LIBRARY_SUFFIX}.19
    ${CMAKE_CURRENT_BINARY_DIR}/elasticlient/lib/libjsoncpp${CMAKE_SHARED_LIBRARY_SUFFIX}
    ${CMAKE_CURRENT_BINARY_DIR}/elasticlient/lib/libcpr${CMAKE_SHARED_LIBRARY_SUFFIX}.1.5.1
    ${CMAKE_CURRENT_BINARY_DIR}/elasticlient/lib/libcpr${CMAKE_SHARED_LIBRARY_SUFFIX}.1
    ${CMAKE_CURRENT_BINARY_DIR}/elasticlient/lib/libcpr${CMAKE_SHARED_LIBRARY_SUFFIX}
    DESTINATION ${LIB_DIR})
endif(NOT PLUGIN)
