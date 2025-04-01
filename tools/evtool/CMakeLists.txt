################################################################################
#    HPCC SYSTEMS software Copyright (C) 2025 HPCC Systems®.
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

# Component: evtool

#####################################################
# Description:
# ------------
#    Cmake Input File for evtool
#####################################################
project(evtool)

include_directories (
    ${HPCC_SOURCE_DIR}/system/jlib
    ${HPCC_SOURCE_DIR}/system/include
)

set (  SRCS
       evtmain.cpp
       evtool.cpp
       evtdump.cpp
    )

ADD_DEFINITIONS(-D_CONSOLE)

HPCC_ADD_EXECUTABLE ( evtool ${SRCS} )

install ( TARGETS evtool RUNTIME DESTINATION ${EXEC_DIR} )

add_dependencies ( evtool jlib )

target_link_libraries( evtool
    jlib
)
