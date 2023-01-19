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

# Component: securesocket 

#####################################################
# Description:
# ------------
#    Cmake Input File for securesocket
#####################################################


project( securesocket ) 

set (    SRCS 
         securesocket.cpp 
    )

include_directories ( 
         ./../../include 
         ./../../jlib 
         ./../../security/shared
         ${OPENSSL_INCLUDE_DIR}
    )

ADD_DEFINITIONS( -D_USRDLL -DSECURESOCKET_EXPORTS )

HPCC_ADD_LIBRARY( securesocket SHARED ${SRCS} )
install ( TARGETS securesocket RUNTIME DESTINATION ${EXEC_DIR} LIBRARY DESTINATION ${LIB_DIR} CALC_DEPS)
target_link_libraries ( securesocket
         jlib
         ${OPENSSL_LIBRARIES} 
    )


