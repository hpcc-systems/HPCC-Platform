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


# Component: esp 
#####################################################
# Description:
# ------------
#    Cmake Input File for esp
#####################################################

project( esp ) 

set (    SRCS 
         application_config.cpp
         espcfg.cpp 
         espp.cpp 
    )

include_directories ( 
         ./../bindings/SOAP/xpp 
         ./../../system/xmllib 
         ./../../system/security/shared 
         ./../../system/security/securesocket 
         ./../bindings 
         ./../platform 
         ./../../system/include 
         ./../../dali/base 
         ./../../system/jlib 
         ./../bindings/http/platform 
         ./../../common/environment 
         ./../../system/mp 
         ./../../fs/dafsclient
         ${CMAKE_BINARY_DIR}
         ${CMAKE_BINARY_DIR}/oss
    )

ADD_DEFINITIONS( -D_CONSOLE -DESPCFG_EXPORTS)

HPCC_ADD_EXECUTABLE ( esp ${SRCS} )
install ( TARGETS esp RUNTIME DESTINATION ${EXEC_DIR} )
target_link_libraries ( esp 
         jlib
         mp 
         ${XALAN_LIBRARIES} ${XERCES_LIBRARIES}
         xmllib 
         esphttp 
         hrpc 
         remote 
         dalibase 
         environment 
    ) 
