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


# Component: eclccserver

#####################################################
# Description:
# ------------
#    Cmake Input File for eclccserver
#####################################################


project( eclccserver ) 

set (    SRCS 
         eclccserver.cpp 
    )

set (    INCLUDES
    )

include_directories ( 
         ./../../common/remote 
         ./../../common/workunit 
         ./../../common/environment
         ./../../system/include 
         ./../../system/mp 
         ./../../dali/base 
         ./../../common/dllserver 
         ./../../system/jlib
         ./../../system/security/shared
         ${HPCC_SOURCE_DIR}/system/codesigner
    )

HPCC_ADD_EXECUTABLE ( eclccserver ${SRCS} ${INCLUDES} )
install ( TARGETS eclccserver RUNTIME DESTINATION ${EXEC_DIR} )

target_link_libraries ( eclccserver 
         jlib
         workunit
    )

HPCC_ADD_SUBDIRECTORY (vchooks "PLATFORM")

