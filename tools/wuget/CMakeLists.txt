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

# Component: wuget
#####################################################
# Description:
# ------------
#    Cmake Input File for wuget
#####################################################

project( wuget ) 

include_directories (            
         ./../../system/jlib
         ./../../common/dllserver
         ./../../system/include 
         ./../../common/workunit
    )

ADD_DEFINITIONS( -DMODULE_PRIORITY=1 -D_CONSOLE )

set ( SRCS wuget.cpp )
HPCC_ADD_EXECUTABLE ( wuget ${SRCS} )
target_link_libraries ( wuget
         jlib
         dllserver
         workunit
         )
install ( TARGETS wuget RUNTIME DESTINATION ${EXEC_DIR} )

