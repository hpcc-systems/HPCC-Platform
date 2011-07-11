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


# Component: SMCLib 
#####################################################
# Description:
# ------------
#    Cmake Input File for SMCLib
#####################################################

project( SMCLib ) 

include(${HPCC_SOURCE_DIR}/esp/scm/smcscm.cmake)

set (    SRCS 
         ${ESPSCM_GENERATED_DIR}/ws_topology_esp.cpp 
         LogicFileWrapper.cpp 
         TpWrapper.cpp 
         WUXMLInfo.cpp 
    )

include_directories ( 
         ./../../../system/mp 
         ./../../platform 
         ./../../../system/jlib 
         ./../../bindings/SOAP/client 
         ./../../../common/environment 
         ./../../services 
         ./../../../dali/ft 
         ./../../bindings/SOAP/Platform 
         ./../../../system/xmllib 
         ./../../../ecl/attrserver 
         ./../../../system/security/securesocket 
         ./../../../system/security/shared
         ./../../../system/include 
         ./../../../common/workunit 
         ./../../../common/remote 
         ./../../clients 
         ./../../services/fdleservice 
         ./../../../dali/base 
         ./../../clients/LoggingClient 
         ./../../bindings 
         ./../../bindings/SOAP/xpp 
         ./.. 
    )

ADD_DEFINITIONS( -D_USRDLL -DSMCLIB_EXPORTS )

HPCC_ADD_LIBRARY( SMCLib SHARED ${SRCS} )
install ( TARGETS SMCLib DESTINATION ${OSSDIR}/lib )
target_link_libraries ( SMCLib
         jlib
         remote 
         xmllib 
         esphttp 
         dalibase 
         environment 
         dalift 
         dllserver 
         nbcd 
         eclrtl 
         deftype 
         workunit 
    )
