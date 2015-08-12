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

# Component: frunagent 

#####################################################
# Description:
# ------------
#    Cmake Input File for frunagent
#####################################################


project( frunagent ) 

include (${HPCC_SOURCE_DIR}/common/hohidl/hohidl.cmake)

set (    SRCS 
         ${HOHIDL_GENERATED_DIR}/hagent.cpp 
         hodisp_base.cpp 
         ../../system/hrpc/hrpc.cpp 
         ../../system/hrpc/hrpcsock.cpp 
         ../../system/hrpc/hrpcutil.cpp 
         frunagent.cpp 
    )

include_directories ( 
         ./../../system/hrpc 
         ./../../common/homisc 
         ./../../system/include 
         ./../../system/jlib 
    )

ADD_DEFINITIONS ( -D_CONSOLE )

HPCC_ADD_EXECUTABLE ( frunagent ${SRCS} )
install ( TARGETS frunagent RUNTIME DESTINATION ${EXEC_DIR} )
target_link_libraries ( frunagent 
         jlib
         homisc 
         mp 
         hrpc 
    )


