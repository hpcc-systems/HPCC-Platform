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

# Component: dafilesrv 
#####################################################
# Description:
# ------------
#    Cmake Input File for dafilesrv
#####################################################

project( dafilesrv ) 

set (    SRCS 
         dafilesrv.cpp 
    )

include_directories ( 
         ./../../system/hrpc 
         ./../../common/remote 
         ./../../system/include 
         ./../../system/jlib 
         ${CMAKE_BINARY_DIR}
         ${CMAKE_BINARY_DIR}/oss
    )

if (WIN32)
    set (CMAKE_EXE_LINKER_FLAGS "/STACK:65536 ${CMAKE_EXE_LINKER_FLAGS}")
endif()

HPCC_ADD_EXECUTABLE ( dafilesrv ${SRCS} )
set_target_properties (dafilesrv PROPERTIES COMPILE_FLAGS -D_CONSOLE)
install ( TARGETS dafilesrv RUNTIME DESTINATION ${EXEC_DIR} )
target_link_libraries ( dafilesrv
         jlib
         remote
    )

