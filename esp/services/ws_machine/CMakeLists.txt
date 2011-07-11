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


# Component: ws_machine 
#####################################################
# Description:
# ------------
#    Cmake Input File for ws_machine
#####################################################

project( ws_machine ) 

include(${HPCC_SOURCE_DIR}/esp/scm/espscm.cmake)

set (    SRCS 
         ${ESPSCM_GENERATED_DIR}/ws_machine_esp.cpp 
         ws_machineBinding.cpp 
         ws_machinePlugin.cpp 
         ws_machineService.cpp 
         ws_machineServiceMetrics.cpp 
         ws_machineServiceRexec.cpp 
    )

include_directories ( 
         ./../.. 
         ./../../../system/mp 
         ./../../../system/jlib 
         ./../../services 
         ./../../../system/xmllib 
         ./../../../system/security/securesocket 
         ./../../../system/security/shared   
         ./../../../system/include 
         ./../../../common/remote 
         ./../../../common/environment
         ./../../clients 
         ./../../../dali/base 
         ./../../bindings 
         ./../../bindings/SOAP/xpp 
         ./../../../esp/platform 
         ./../../smc/SMCLib 
    )

ADD_DEFINITIONS( -D_USRDLL )

HPCC_ADD_LIBRARY( ws_machine SHARED ${SRCS} )
add_dependencies( ws_machine espscm )
install ( TARGETS ws_machine DESTINATION ${OSSDIR}/lib )
target_link_libraries ( ws_machine
         jlib
         roxiecommlib
         xmllib 
         esphttp 
         LdapSecurity 
         mp 
         hrpc 
         remote 
         dalibase 
         environment 
         securesocket 
    )


