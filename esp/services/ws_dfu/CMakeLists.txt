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


# Component: ws_dfu 
#####################################################
# Description:
# ------------
#    Cmake Input File for ws_dfu
#####################################################

project( ws_dfu ) 

include(${HPCC_SOURCE_DIR}/esp/scm/smcscm.cmake)

set (    SRCS 
         ../../../dali/dfu/dfuutil.cpp 
         ${ESPSCM_GENERATED_DIR}/ws_dfu_esp.cpp 
         ${ESPSCM_GENERATED_DIR}/ws_dfuXref_esp.cpp 
         ws_dfuPlugin.cpp 
         ws_dfuService.cpp 
         ws_dfuView.cpp 
         ws_dfuXRefService.cpp 
    )

include_directories ( 
         ./../../../dali/dfu 
         ./../../../system/mp 
         ./../../platform 
         ./../ws_topology 
         ./../../../system/jlib 
         ./../../../rtl/eclrtl 
         ./../../../common/environment 
         ./../../services 
         ./../../../dali/ft 
         ./../common 
         ./../../../dali/dfuXRefLib 
         ./../../../system/xmllib 
         ./../../../common/deftype 
         ./../../../ecl/hql 
         ./../../../system/security/securesocket 
         ./../../../system/security/LdapSecurity 
         ./../../../system/security/shared 
         ./../../../system/include 
         ./../../../common/workunit 
         ./../../../common/remote 
         ./../../clients 
         ./../../../dali/base 
         ./../ws_workunits 
         ./../../bindings 
         ./../../smc/SMCLib 
         ./../../../common/fileview2 
         ./../../bindings/SOAP/xpp 
    )

ADD_DEFINITIONS( -D_USRDLL -DWS_DFU_EXPORTS )

HPCC_ADD_LIBRARY( ws_dfu SHARED ${SRCS} )
add_dependencies ( ws_dfu espscm )
install ( TARGETS ws_dfu DESTINATION ${OSSDIR}/lib )
target_link_libraries ( ws_dfu
         jlib
         mp 
         hrpc 
         remote 
         nbcd 
         eclrtl 
         jhtree 
         dalibase 
         environment 
         dllserver 
         deftype 
         workunit 
         hql   
         fileview2 
         xmllib 
         esphttp 
         dalift 
         SMCLib 
         dfuXRefLib 
         LdapSecurity 
         dfuwu 
         securesocket 
         roxiecommlib 
    )


