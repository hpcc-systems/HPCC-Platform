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


# Component: esphttp 

#####################################################
# Description:
# ------------
#    Cmake Input File for esphttp
#####################################################


project( esphttp ) 

set (    SRCS 
         ../../bindings/bindutil.cpp 
         ../../bindings/http/client/httpclient.cpp 
         ../../bindings/http/platform/htmlpage.cpp 
         ../../bindings/http/platform/httpbinding.cpp 
         ../../bindings/http/platform/httpprot.cpp 
         ../../bindings/http/platform/httpservice.cpp 
         ../../bindings/http/platform/httptransport.cpp 
         ../../bindings/http/platform/mime.cpp 
         ../../bindings/http/platform/msgbuilder.cpp 
         ../../bindings/SOAP/client/soapclient.cpp 
         ../../bindings/SOAP/Platform/soapbind.cpp 
         ../../bindings/SOAP/Platform/soapmessage.cpp 
         ../../bindings/SOAP/Platform/soapservice.cpp 
         ../../platform/espcontext.cpp 
         ../../platform/espprotocol.cpp 
         ../../platform/espthread.cpp 
         ../../platform/sechandler.cpp 
         mapinfo.cpp 
         plugin.cpp 
    )

include_directories ( 
         ./../../../system/include 
         ./../../../system/security/shared
         ./../../../system/security/securesocket 
         ./../../bindings 
         ./../../bindings/SOAP/xpp 
         ./../../../system/xmllib 
         ./../../../system/jlib 
         ./../../platform 
         ./../../../system/security/shared 
         ./../../../system/security/LdapSecurity
    )

ADD_DEFINITIONS( -DESPHTTP_EXPORTS -DESP_TIMING -D_USRDLL -DESP_PLUGIN )

HPCC_ADD_LIBRARY( esphttp SHARED ${SRCS} )
add_dependencies (esphttp  espscm )
install ( TARGETS esphttp DESTINATION ${OSSDIR}/lib )
target_link_libraries ( esphttp 
     ${XALAN_LIBRARIES} ${XERCES_LIBRARIES}
     jlib
     xmllib 
)
IF (USE_OPENLDAP)
    target_link_libraries ( esphttp LdapSecurity )
ENDIF(USE_OPENLDAP)

