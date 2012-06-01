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


# Component: ws_ecl_client 

#####################################################
# Description:
# ------------
#    Cmake Input File for ws_ecl_client
#####################################################


project( ws_ecl_client ) 

set (    SRCS 
         ../edwin.cpp 
         ws_ecl_client.cpp 
    )

include_directories ( 
         ./../../../system/include 
         ./../../../system/xmllib
         ./../../../system/security/shared 
         ./../../../system/security/securesocket 
         ./../../bindings 
         ./.. 
         ./../../bindings/SOAP/client 
         ./../../bindings/SOAP/xpp 
         ./../../../system/jlib 
         ./../../platform 
         ./../../bindings/SOAP/Platform 
         ./../../bindings/SOAP/ws_ecl_client 
    )

ADD_DEFINITIONS( -D_USRDLL -DWS_ECL_CLIENT_EXPORTS )

if ( NOT CLIENTTOOLS_ONLY )
    HPCC_ADD_LIBRARY( ws_ecl_client SHARED ${SRCS} )
    install ( TARGETS ws_ecl_client DESTINATION ${OSSDIR}/lib )
    target_link_libraries (  ws_ecl_client 
         ${XALAN_LIBRARIES} ${XERCES_LIBRARIES}
         jlib
         xmllib 
         esphttp 
    )
endif()

