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


# Component: ws_topology 

#####################################################
# Description:
# ------------
#    Cmake Input File for ws_topology
#####################################################


project( ws_topology ) 

include(${HPCC_SOURCE_DIR}/esp/scm/smcscm.cmake)

set (    SRCS 
         ../../../tools/swapnode/swapnodemain.cpp 
         ${ESPSCM_GENERATED_DIR}/ws_topology_esp.cpp 
         ws_topologyPlugin.cpp 
         ws_topologyService.cpp 
    )

include_directories ( 
         ./../../../system/mp 
         ./../../platform 
         ./../../../system/jlib 
         ./../../../common/environment 
         ./../../services 
         ./../../../system/xmllib 
         ./../../../system/security/securesocket         
         ./../../../system/security/shared 
         ./../../../system/security/zcrypt 
         ./../../../system/include 
         ./../../../common/workunit 
         ./../../../common/remote 
         ./../../clients 
         ./../../../tools/swapnode 
         ./../../../dali/base 
         ./../ws_workunits 
         ./../../bindings 
         ./../../smc/SMCLib 
         ./../../bindings/SOAP/xpp 
         ./../..//esplib
    )

ADD_DEFINITIONS( -D_USRDLL -DWS_TOPOLOGY_EXPORTS )

HPCC_ADD_LIBRARY( ws_topology SHARED ${SRCS} )
install ( TARGETS ws_topology DESTINATION ${OSSDIR}/lib )
target_link_libraries ( ws_topology
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
         SMCLib 
         LdapSecurity 
         securesocket 
         )
         
IF (USE_ZLIB)
  target_link_libraries ( ws_topology
      ${ZLIB_LIBRARIES}
      zcrypt
      )
ENDIF()


