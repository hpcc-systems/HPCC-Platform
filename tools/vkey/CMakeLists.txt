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

# Component: vkey 
#####################################################
# Description:
# ------------
#    Cmake Input File for vkey
#####################################################

project( vkey ) 

include_directories (            
         ./../../system/jlib
         ./../../system/jhtree 
         ./../../system/include 
         ./../../rtl/include 
         ./../../rtl/eclrtl 
    )

ADD_DEFINITIONS( -DMODULE_PRIORITY=1 -D_CONSOLE )

set ( SRCS vkey.cpp )
HPCC_ADD_EXECUTABLE ( vkey ${SRCS} )
target_link_libraries ( vkey 
         jlib
         )
install ( 
    TARGETS vkey RUNTIME DESTINATION ${EXEC_DIR}
    COMPONENT Runtime
)

