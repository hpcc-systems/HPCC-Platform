################################################################################
#    HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems®.
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


# Component: toposerver

#####################################################
# Description:
# ------------
#    Cmake Input File for toposerver
#####################################################


project( toposerver ) 

set (   SRCS 
        toposerver.cpp 
    )

include_directories ( 
         .
         ${HPCC_SOURCE_DIR}/system/jlib
         ${HPCC_SOURCE_DIR}/system/include
    )

if (CMAKE_COMPILER_IS_GNUCC OR CMAKE_COMPILER_IS_CLANG)
  SET (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Werror -Wno-unused-parameter")
endif()

if (CMAKE_COMPILER_IS_CLANGXX)
  SET (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror=reorder")
endif()

ADD_DEFINITIONS( -D_CONSOLE )

if (WIN32)
    set (CMAKE_EXE_LINKER_FLAGS "/STACK:65536 ${CMAKE_EXE_LINKER_FLAGS}")
endif()

HPCC_ADD_EXECUTABLE ( toposerver ${SRCS} )
install ( 
  TARGETS toposerver 
  RUNTIME DESTINATION ${EXEC_DIR} 
  COMPONENT Runtime
)

target_link_libraries ( toposerver
         jlib 
    )
