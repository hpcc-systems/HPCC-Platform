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


# Component: ws_account 
#####################################################
# Description:
# ------------
#    Cmake Input File for ws_account
#####################################################

project( ws_account ) 

include(${HPCC_SOURCE_DIR}/esp/scm/additional.cmake)

set (    SRCS 
         ${ESPSCM_GENERATED_DIR}/ws_account_esp.cpp
         ws_accountPlugin.cpp 
         ws_accountService.cpp 
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

if ( NOT CLIENTTOOLS_ONLY )
    HPCC_ADD_LIBRARY( ws_account SHARED ${SRCS} )
    install ( TARGETS ws_account DESTINATION ${OSSDIR}/lib COMPONENT Runtime )
    add_dependencies (ws_account espscm)
    target_link_libraries ( ws_account 
         jlib
         xmllib 
         esphttp 
         securesocket 
    )
    IF (USE_OPENLDAP)
        target_link_libraries ( ws_account LdapSecurity )
    ENDIF(USE_OPENLDAP)
endif()

