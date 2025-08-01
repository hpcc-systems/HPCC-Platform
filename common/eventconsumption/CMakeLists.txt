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

project(eventconsumption)

include_directories (
    ${HPCC_SOURCE_DIR}/system/jlib
    ${HPCC_SOURCE_DIR}/system/include
    ${HPCC_SOURCE_DIR}/testing/unittests
)

set (
    SRCS
    ${CMAKE_CURRENT_SOURCE_DIR}/eventdump.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/eventdumpstream.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/eventdumpptree.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/eventfilter.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/eventindexhotspot.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/eventindexhotspotbuckets.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/eventindexmodel.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/eventindexmodelexpansion.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/eventindexmodelstorage.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/eventindexsummarize.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/eventiterator.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/eventmodeling.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/eventoperation.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/eventunittests.cpp
)

ADD_DEFINITIONS( -D_USRDLL -DEVENTCONSUMPTION_EXPORTS )

HPCC_ADD_LIBRARY(eventconsumption SHARED ${SRCS})
install ( TARGETS eventconsumption RUNTIME DESTINATION ${EXEC_DIR} LIBRARY DESTINATION ${LIB_DIR} )
target_link_libraries ( eventconsumption jlib ${CppUnit_LIBRARIES})
