################################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.
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


# Component: roxie

#####################################################
# Description:
# ------------
#    Cmake Input File for roxie
#####################################################


project( roxie ) 

set (   SRCS 
        roxie.cpp 
        roxie.hpp        
    )

include_directories ( 
         .
         ./../../roxie/ccd
         ./../../system/include 
         ./../../system/jlib
         ./../../rtl/include 
         ./../../rtl/eclrtl

         ./../../roxie/udplib 
         ./../../roxie/roxie 
         ./../../roxie/roxiemem 
         ./../../common/thorhelper
         ./../../common/workunit
    )

ADD_DEFINITIONS( -D_CONSOLE )

if (WIN32)
    set (CMAKE_EXE_LINKER_FLAGS "/STACK:65536 ${CMAKE_EXE_LINKER_FLAGS}")
endif()

HPCC_ADD_EXECUTABLE ( roxie ${SRCS} )
install ( TARGETS roxie RUNTIME DESTINATION ${EXEC_DIR} )

target_link_libraries ( roxie
         ccd
         jlib 
    )



