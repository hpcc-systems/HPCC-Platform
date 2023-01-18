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

find_path(ELASTIC_CLIENT_INCLUDE_DIRS "elasticlient/logging.h" HINTS "${VCPKG_INSTALLED_DIR}")
find_library(ELASTIC_CLIENT_LIBRARY NAMES "elasticlient")
find_package(jsoncpp CONFIG REQUIRED)
find_package(cpr CONFIG REQUIRED)

# elastistacklogaccess library
# update cxx_flags to be less restrictive when building against elasticlient header files
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fpermissive")

set(srcs
  ${CMAKE_CURRENT_SOURCE_DIR}/ElasticStackLogAccess.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/ElasticStackLogAccess.hpp
)

include_directories(
  ${HPCC_SOURCE_DIR}/system/include
  ${HPCC_SOURCE_DIR}/system/jlib
  ${ELASTIC_CLIENT_INCLUDE_DIRS}
)

add_definitions(-DELASTICSTACKLOGACCESS_EXPORTS)
HPCC_ADD_LIBRARY(elasticstacklogaccess SHARED ${srcs})
target_link_libraries(elasticstacklogaccess
  jlib
  ${ELASTIC_CLIENT_LIBRARY}
  JsonCpp::JsonCpp
  cpr::cpr
)

install(TARGETS elasticstacklogaccess
  RUNTIME DESTINATION ${EXEC_DIR}
  LIBRARY DESTINATION ${LIB_DIR}
  CALC_DEPS
)
