################################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
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
install ( TARGETS LdapSecurity RUNTIME DESTINATION ${EXEC_DIR} LIBRARY DESTINATION ${LIB_DIR} )
target_link_libraries ( LdapSecurity
         jlib
         ${OPENLDAP_LIBRARIES}
    )


