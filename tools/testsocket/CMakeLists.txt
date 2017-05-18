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

# Component: testsocket
#####################################################
# Description:
# ------------
#    Cmake Input File for testsocket
#####################################################

project( testsocket ) 

include_directories (            
         ./../../system/jlib
         ./../../system/include 
         ./../../system/security/securesocket
    )

ADD_DEFINITIONS( -D_CONSOLE )

set ( SRCS testsocket.cpp )
HPCC_ADD_EXECUTABLE ( testsocket ${SRCS} )
target_link_libraries ( testsocket
         jlib
         )

IF (USE_OPENSSL)
    target_link_libraries ( testsocket
         securesocket
         )
ENDIF()

install ( TARGETS testsocket RUNTIME DESTINATION ${EXEC_DIR} )

if ( PLATFORM )
    install ( PROGRAMS testsocket.install DESTINATION etc/init.d/install COMPONENT Runtime )
    install ( PROGRAMS testsocket.uninstall DESTINATION etc/init.d/uninstall COMPONENT Runtime )
endif()
