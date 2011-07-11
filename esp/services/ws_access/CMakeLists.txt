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


# Component: ws_access 
#####################################################
# Description:
# ------------
#    Cmake Input File for ws_access
#####################################################

project( ws_access ) 

include(${HPCC_SOURCE_DIR}/esp/scm/additional.cmake)

set (    SRCS 
         ws_accessPlugin.cpp 
         ws_accessService.cpp 
    )

include_directories ( 
         ./../.. 
         ./../../platform 
         ./../../../system/jlib 
         ./../../services 
         ./../../../system/xmllib 
         ./../../../system/security/securesocket 
         ./../../../system/include 
         ./../../clients 
         ./../../../system/security/LdapSecurity 
         ./../../../system/security/shared 
         ./../../bindings 
         ./../../bindings/SOAP/xpp
         ./../../smc/SMCLib  
    )

# NOTE - this should not be needed, it's the result of poor encapsulation and using CLdapSecManager directly 
include_directories ( ${OPENLDAP_INCLUDE_DIR} )

ADD_DEFINITIONS( -D_USRDLL )

HPCC_ADD_LIBRARY( ws_access SHARED ${SRCS} )
install ( TARGETS ws_access DESTINATION ${OSSDIR}/lib )
add_dependencies (ws_access espscm)
target_link_libraries ( ws_access 
         jlib
         xmllib 
         esphttp 
         LdapSecurity 
         securesocket 
    )

