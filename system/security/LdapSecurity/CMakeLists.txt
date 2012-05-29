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


# Component: LdapSecurity 
#####################################################
# Description:
# ------------
#    Cmake Input File for LdapSecurity
#####################################################

project( LdapSecurity ) 

set (    SRCS 
         ../shared/authmap.cpp 
         ../shared/caching.cpp 
         ../shared/basesecurity.cpp
         ../shared/defaultsecuritymanager.cpp
         aci.cpp 
         ldapconnection.cpp 
         ldapsecurity.cpp 
         permissions.cpp 
    )

include_directories ( 
         ./../../include 
         ./../shared 
         ./../../jlib 
         ./../../../esp/platform 
         ${OPENLDAP_INCLUDE_DIR}
    )

ADD_DEFINITIONS( -DLDAPSECURITY_EXPORTS -D_USRDLL )

HPCC_ADD_LIBRARY( LdapSecurity SHARED ${SRCS} )
install ( TARGETS LdapSecurity DESTINATION ${OSSDIR}/lib )
target_link_libraries ( LdapSecurity
     jlib
     ${OPENLDAP_LIBRARIES}
)

