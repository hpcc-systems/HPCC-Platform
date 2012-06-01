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

include(${HPCC_SOURCE_DIR}/esp/scm/espscm.cmake)
include(${HPCC_SOURCE_DIR}/esp/scm/additional.cmake)

set (    SRCS 
         ${ESPSCM_GENERATED_DIR}/ws_access_esp.cpp
         ws_accessPlugin.cpp 
         ws_accessService.cpp 
    )

include_directories ( 
         ${HPCC_SOURCE_DIR}/esp/platform
         ${HPCC_SOURCE_DIR}/system/jlib
         ${HPCC_SOURCE_DIR}/esp/services
         ${HPCC_SOURCE_DIR}/system/xmllib
         ${HPCC_SOURCE_DIR}/system/security/securesocket
         ${HPCC_SOURCE_DIR}/system/include
         ${HPCC_SOURCE_DIR}/esp/clients
         ${HPCC_SOURCE_DIR}/system/security/LdapSecurity
         ${HPCC_SOURCE_DIR}/system/security/shared
         ${HPCC_SOURCE_DIR}/esp/bindings
         ${HPCC_SOURCE_DIR}/esp/bindings/SOAP/xpp
         ${HPCC_SOURCE_DIR}/esp/smc/SMCLib
    )

# NOTE - this should not be needed, it's the result of poor encapsulation and using CLdapSecManager directly 
include_directories ( ${OPENLDAP_INCLUDE_DIR} )

ADD_DEFINITIONS( -D_USRDLL )

if ( NOT CLIENTTOOLS_ONLY )
    HPCC_ADD_LIBRARY( ws_access SHARED ${SRCS} )
    install ( TARGETS ws_access DESTINATION ${OSSDIR}/lib COMPONENT Runtime)
    add_dependencies (ws_access espscm)
    target_link_libraries ( ws_access 
         jlib
         xmllib 
         esphttp 
         LdapSecurity 
         securesocket 
    )
endif()
