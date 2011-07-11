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


# Component: ws_workunits 
#####################################################
# Description:
# ------------
#    Cmake Input File for ws_workunits
#####################################################

project( ws_workunits ) 

include(${HPCC_SOURCE_DIR}/esp/scm/smcscm.cmake)

set (    SRCS 
         ../../../dali/sasha/sacmd.cpp 
         ${ESPSCM_GENERATED_DIR}/ws_workunits_esp.cpp 
         ${HPCC_SOURCE_DIR}/esp/scm/ws_workunits.ecm
         ws_workunitsPlugin.cpp 
         ws_workunitsService.cpp 
         ws_workunitsService.hpp 
    )

include_directories ( 
         ./../../esplib 
         ./../../../system/mp 
         ./../../platform 
         ./../../../dali/sasha 
         ./../../../common/roxiemanager 
         ./../../../system/jlib 
         ./../../../common/environment 
         ./../../services 
         ./../common 
         ./../../../system/xmllib 
         ./../../../common/deftype 
         ./../../../ecl/hql 
         ./../../../system/security/securesocket 
         ./../../../system/security/shared   
         ./../../../system/include 
         ./../../../common/workunit 
         ./../../../ecl/schedulectrl 
         ./../../clients 
         ./../../../common/fileview2 
         ./../../../dali/base 
         ./../../../system/security/zcrypt 
         ./../../../rtl/include 
         ./../../../common/dllserver 
         ./../../bindings 
         ./../../smc/SMCLib 
         ./../../bindings/SOAP/xpp 
    )

ADD_DEFINITIONS( -D_USRDLL -DWS_WORKUNITS_EXPORTS )

HPCC_ADD_LIBRARY( ws_workunits SHARED ${SRCS} )
install ( TARGETS ws_workunits DESTINATION ${OSSDIR}/lib )
target_link_libraries ( ws_workunits 
         jlib
         mp 
         hrpc 
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
         schedulectrl 
         roxiecommlib 
         roxiemanager 
         LdapSecurity 
         hql 
         jhtree 
         fileview2 
         securesocket 
    )

IF (USE_ZLIB)
  target_link_libraries ( ws_workunits
      ${ZLIB_LIBRARIES}
      zcrypt
      )
ENDIF()

