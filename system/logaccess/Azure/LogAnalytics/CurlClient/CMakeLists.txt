###############################################################################
#    HPCC SYSTEMS software Copyright (C) 2022 HPCC Systems®.
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

project(AzureLogAnalyticsCurllogaccess)

# Required installed libraries
find_package(CURL REQUIRED)

set(srcs
  ${CMAKE_CURRENT_SOURCE_DIR}/AzureLogAnalyticsCurlClient.cpp
)

include_directories(
  ${HPCC_SOURCE_DIR}/system/include
  ${HPCC_SOURCE_DIR}/system/jlib
  ${CURL_INCLUDE_DIR}
)

add_definitions(-DAZURE_LOGANALYTICS_CURL_LOGACCESS_EXPORTS)

HPCC_ADD_LIBRARY(${PROJECT_NAME} SHARED ${srcs})

target_link_libraries(${PROJECT_NAME}
   PRIVATE jlib
   PRIVATE ${CURL_LIBRARIES}
)

install(TARGETS ${PROJECT_NAME}
  RUNTIME DESTINATION ${EXEC_DIR}
  LIBRARY DESTINATION ${LIB_DIR}
  CALC_DEPS
)
