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

# Component: mp 

#####################################################
# Description:
# ------------
#    Cmake Input File for mp
#####################################################


project( mp ) 

set (    SRCS 
         mpbase.cpp 
         mpcomm.cpp 
         mplog.cpp 
         mptag.cpp 
    )

include_directories ( 
         ./../include 
         .
         ./../jlib
         ./../security/securesocket
    )

ADD_DEFINITIONS( -DLOGMSGCOMPONENT=2 -D_USRDLL -DMP_EXPORTS )

HPCC_ADD_LIBRARY( mp SHARED ${SRCS} )
install ( TARGETS mp RUNTIME DESTINATION ${EXEC_DIR} LIBRARY DESTINATION ${LIB_DIR} )
target_link_libraries ( mp 
         jlib
    )
    if (USE_OPENSSL)
        target_link_libraries(mp securesocket)
    endif()

HPCC_ADD_SUBDIRECTORY(test "PLATFORM")
