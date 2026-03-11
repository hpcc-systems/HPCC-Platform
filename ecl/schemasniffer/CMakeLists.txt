################################################################################
#    HPCC SYSTEMS software Copyright (C) 2026 HPCC Systems®.
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

# Component: schemasniffer

project(schemasniffer)

set(SRCS_COMMON
    common.hpp
    common.cpp
)

set(SRCS_JSON2ECL
    json2ecl.hpp
    json2ecl.cpp
)

set(SRCS_XML2ECL
    xml2ecl.hpp
    xml2ecl.cpp
)

include_directories(
    ${CMAKE_BINARY_DIR}
    ${CMAKE_BINARY_DIR}/oss
    ${HPCC_SOURCE_DIR}/system/include
    ${HPCC_SOURCE_DIR}/system/jlib
    ${HPCC_SOURCE_DIR}/ecl/hql
)

ADD_DEFINITIONS(-D_CONSOLE)

# Build json2ecl executable
HPCC_ADD_EXECUTABLE(json2ecl ${SRCS_COMMON} ${SRCS_JSON2ECL} json2eclmain.cpp)
install(TARGETS json2ecl RUNTIME DESTINATION ${EXEC_DIR})
target_link_libraries(json2ecl
    jlib
    hql
)

# Build xml2ecl executable
HPCC_ADD_EXECUTABLE(xml2ecl ${SRCS_COMMON} ${SRCS_XML2ECL} xml2eclmain.cpp)
install(TARGETS xml2ecl RUNTIME DESTINATION ${EXEC_DIR})
target_link_libraries(xml2ecl
    jlib
    hql
)
