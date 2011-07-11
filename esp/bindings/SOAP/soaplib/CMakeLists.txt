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


# Component: soaplib 
#####################################################
# Description:
# ------------
#    Cmake Input File for soaplib
#####################################################


project( soaplib ) 

set (    SRCS 
         ../../../platform/espcontext.cpp 
         ../../../platform/espprotocol.cpp 
         ../../../platform/espthread.cpp 
         ../../../platform/sechandler.cpp 
         ../../../protocols/http/mapinfo.cpp 
         ../../bindutil.cpp 
         ../../http/client/httpclient.cpp 
         ../../http/platform/htmlpage.cpp 
         ../../http/platform/httpbinding.cpp 
         ../../http/platform/httpprot.cpp 
         ../../http/platform/httpservice.cpp 
         ../../http/platform/httptransport.cpp 
         ../../http/platform/mime.cpp 
         ../../http/platform/msgbuilder.cpp 
         ../client/soapclient.cpp 
         ../Platform/soapbind.cpp 
         ../Platform/soapmessage.cpp 
         ../Platform/soapservice.cpp 
    )

include_directories ( 
         ./../../../../system/include 
         ./../../../../system/security/shared
         ./../../../../system/security/securesocket 
         ./../../../../system/jlib 
         ./../../../bindings 
         ./../../../platform 
         ./../../../../system/xmllib 
         ./.. 
         ./../../../bindings/SOAP/xpp 
    )


if (WIN32)
    ADD_DEFINITIONS( -D_LIB )
    HPCC_ADD_LIBRARY( soaplib STATIC ${SRCS} )
    add_dependencies (soaplib 
        jlib
        espscm
        )
else (WIN32)
    HPCC_ADD_LIBRARY( soaplib SHARED ${SRCS} )
    install ( TARGETS soaplib DESTINATION ${OSSDIR}/lib )
    target_link_libraries (soaplib 
        jlib
        )
endif (WIN32)
add_dependencies (soaplib 
    jlib
    )

