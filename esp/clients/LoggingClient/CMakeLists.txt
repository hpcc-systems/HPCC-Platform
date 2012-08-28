################################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
################################################################################


# Component: loggingclient 
#####################################################
# Description:
# ------------
#    Cmake Input File for loggingclient
#####################################################

project( loggingclient ) 

include(${HPCC_SOURCE_DIR}/esp/scm/espscm.cmake)

set (    SRCS 
         ../edwin.cpp 
         ${ESPSCM_GENERATED_DIR}/esploggingservice_esp.cpp 
         LocalDataLogger.cpp 
         LogFailSafe.cpp 
         loggingserviceclient.cpp 
         LogSerializer.cpp 
         LogThread.cpp 
    )

include_directories ( 
         ./../../platform 
         ./../../../system/jlib 
         ./../../../system/security/shared
         ./../../bindings/SOAP/client 
         ./../../bindings/SOAP/Platform 
         ./../../../system/xmllib 
         ./../../../system/include 
         ./../../clients 
         ./../../clients/LoggingClient 
         ./../../bindings 
         ./../../bindings/SOAP/xpp 
    )

ADD_DEFINITIONS( -D_USRDLL -DLOGGINGCLIENT_EXPORTS )

if (RELEASE_BUILD EQUAL 1)
    ADD_DEFINITIONS( -DISC_NO_MAIN -DLOGGING_EXPORTS )
endif (RELEASE_BUILD EQUAL 1)

HPCC_ADD_LIBRARY( loggingclient SHARED ${SRCS} )
add_dependencies( loggingclient espscm )
install ( TARGETS loggingclient RUNTIME DESTINATION ${EXEC_DIR} LIBRARY DESTINATION ${LIB_DIR} )
target_link_libraries ( loggingclient
         ${XALAN_LIBRARIES} ${XERCES_LIBRARIES}
         jlib
         xmllib 
         esphttp 
    )

