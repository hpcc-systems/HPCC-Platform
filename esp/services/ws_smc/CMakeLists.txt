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


# Component: ws_smc 
#####################################################
# Description:
# ------------
#    Cmake Input File for ws_smc
#####################################################

project( ws_smc ) 

include(${HPCC_SOURCE_DIR}/esp/scm/smcscm.cmake)

set (    SRCS 
         ${ESPSCM_GENERATED_DIR}/ws_smc_esp.cpp 
         ws_smcPlugin.cpp 
         ws_smcService.cpp 
    )

include_directories ( 
         ./../../../dali/dfu 
         ./../../../system/mp 
         ./../../platform 
         ./../../../system/jlib 
         ./../../../common/environment 
         ./../../services 
         ./../common 
         ./../../../system/xmllib 
         ./../../../system/include 
         ./../../../system/security/securesocket 
         ./../../../system/security/shared 
         ./../../../common/workunit 
         ./../../../common/remote 
         ./../../clients 
         ./../../../tools/swapnode 
         ./../../../dali/base 
         ./../ws_workunits 
         ./../../bindings 
         ./../../smc/SMCLib 
         ./../../bindings/SOAP/xpp 
         ./../../../system/security/LdapSecurity
    )

# NOTE - this should not be needed, it's the result of poor encapsulation and using CLdapSecManager directly 
include_directories ( ${OPENLDAP_INCLUDE_DIR} )

ADD_DEFINITIONS( -D_USRDLL -DWS_SMC_EXPORTS )

if ( NOT CLIENTTOOLS_ONLY )
    HPCC_ADD_LIBRARY( ws_smc SHARED ${SRCS}  )
    add_dependencies ( ws_smc espscm )
    install ( TARGETS ws_smc DESTINATION ${OSSDIR}/lib )
    target_link_libraries ( ws_smc 
         jlib
         xmllib 
         esphttp 
         mp 
         hrpc 
         remote 
         dalibase 
         environment 
         securesocket 
         ws_machine 
         dalift 
         dllserver 
         nbcd 
         eclrtl 
         deftype 
         workunit 
         SMCLib 
         jhtree 
         hql 
         fileview2 
         dfuXRefLib 
         dfuwu 
         roxiecommlib 
         ws_dfu 
         ws_fs 
         ws_topology 
         schedulectrl 
         roxiemanager 
         ws_workunits 
    )
    IF (USE_OPENLDAP)
        target_link_libraries ( ws_smc LdapSecurity )
    ENDIF(USE_OPENLDAP)
endif()

