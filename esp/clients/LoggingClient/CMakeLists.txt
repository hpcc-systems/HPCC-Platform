################################################################################
#    Copyright (C) 2011 HPCC Systems.
#
#    All rights reserved. This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU Affero General Public License as
#    published by the Free Software Foundation, either version 3 of the
#    License, or (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU Affero General Public License for more details.
#
#    You should have received a copy of the GNU Affero General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
         ./../dataaccesslib 
         ./../RemoteNSClient 
         ./../../clients 
         ./../../clients/LoggingClient 
         ./../../bindings 
         ./../../bindings/SOAP/LoggingService 
         ./../../bindings/SOAP/xpp 
    )

ADD_DEFINITIONS( -D_USRDLL -DLOGGINGCLIENT_EXPORTS )

if (RELEASE_BUILD EQUAL 1)
    ADD_DEFINITIONS( -DISC_NO_MAIN -DLOGGING_EXPORTS )
endif (RELEASE_BUILD EQUAL 1)

HPCC_ADD_LIBRARY( loggingclient SHARED ${SRCS} )
add_dependencies( loggingclient espscm )
install ( TARGETS loggingclient DESTINATION ${OSSDIR}/lib )
target_link_libraries ( loggingclient
         ${XALAN_LIBRARIES} ${XERCES_LIBRARIES}
         jlib
         xmllib 
         esphttp 
    )

