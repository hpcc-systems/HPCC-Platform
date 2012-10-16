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

# Component: soapplus 

#####################################################
# Description:
# ------------
#    Cmake Input File for soapplus
#####################################################


project( soapplus ) 

set (    SRCS 
         EspLogDeserializer.cpp 
         http.cpp 
         httpproxy.cpp 
         main.cpp 
         msggenerator.cpp 
         xmldiff.cpp 
    )

include_directories ( 
         ./../../../system/include 
         ./../../../system/security/securesocket 
         ./../../bindings/http/platform 
         ./../../../system/xmllib 
         ./../../../system/jlib 
    )

ADD_DEFINITIONS( -D_CONSOLE )

HPCC_ADD_EXECUTABLE ( soapplus ${SRCS} )
install ( TARGETS soapplus RUNTIME DESTINATION ${EXEC_DIR} COMPONENT Runtime)

target_link_libraries ( soapplus 
         jlib
         xmllib 
         securesocket 
    )

FOREACH( iFILES
    ${CMAKE_CURRENT_SOURCE_DIR}/EspMethods.txt
    ${CMAKE_CURRENT_SOURCE_DIR}/README.txt
)
    Install ( FILES ${iFILES} DESTINATION componentfiles COMPONENT Runtime )
ENDFOREACH ( iFILES )

if ( PLATFORM )
    install ( PROGRAMS soapplus.install DESTINATION etc/init.d/install COMPONENT Runtime )
    install ( PROGRAMS soapplus.uninstall DESTINATION etc/init.d/uninstall COMPONENT Runtime )
endif()
