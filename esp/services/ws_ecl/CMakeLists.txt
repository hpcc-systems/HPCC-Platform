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


# Component: ws_ecl 
#####################################################
# Description:
# ------------
#    Cmake Input File for ws_ecl
#####################################################

project( ws_ecl ) 

set (    SRCS 
         ws_ecl_plugin.cpp 
         ws_ecl_service.cpp 
         ws_ecl_json.cpp
         JSON_parser.c
         wswuinfo.cpp 
    )

include_directories ( 
         ./../../esplib 
         ./../../platform 
         ./../../services 
         ./../../services/common 
         ./../../bindings 
         ./../../bindings/SOAP/xpp 
         ./../../bindings/http/client 
         ./../../clients 
         ./../../../system/mp 
         ./../../../system/jlib 
         ./../../../common/environment
         ./../../../system/hrpc 
         ./../../../system/xmllib 
         ./../../../system/security/shared 
         ./../../../system/security/securesocket 
         ./../../../system/include 
         ./../../../common/workunit 
         ./../../../roxie/roxieclient 
         ./../../../common/fileview2 
         ./../../../dali/base 
    )

ADD_DEFINITIONS( -D_USRDLL )

HPCC_ADD_LIBRARY( ws_ecl SHARED ${SRCS} )
add_dependencies( ws_ecl espscm )
install ( TARGETS ws_ecl DESTINATION ${OSSDIR}/lib )
target_link_libraries ( ws_ecl  
         jlib
         xmllib 
         esphttp 
         LdapSecurity 
         mp 
         hrpc 
         remote 
         dalibase 
         environment 
         dllserver 
         nbcd 
         eclrtl 
         deftype 
         workunit 
         jhtree 
         hql 
         fileview2 
         securesocket 
         xmllib 
    )

