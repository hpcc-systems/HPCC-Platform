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


# Component: eclagent
#####################################################
# Description:
# ------------
#    Cmake Input File for eclagent
#####################################################

project( eclagent )

set ( SRCS
      eclagentmain.cpp
    )

include_directories (
         .
         ${HPCC_SOURCE_DIR}/ecl/hthor
         ${HPCC_SOURCE_DIR}/system/jlib
         ${HPCC_SOURCE_DIR}/system/include
         ${HPCC_SOURCE_DIR}/rtl/include
         ${CMAKE_BINARY_DIR}
         ${CMAKE_BINARY_DIR}/oss
    )

ADD_DEFINITIONS( -DNO_SYBASE -D_CONSOLE )

HPCC_ADD_EXECUTABLE ( eclagent ${SRCS} )
install ( TARGETS eclagent RUNTIME DESTINATION ${EXEC_DIR} )
target_link_libraries ( eclagent
      hthor           
      ${CPPUNIT_LIBRARIES}
    )

Install ( PROGRAMS ${CMAKE_CURRENT_SOURCE_DIR}/start_eclagent DESTINATION ${EXEC_DIR} COMPONENT Runtime )
