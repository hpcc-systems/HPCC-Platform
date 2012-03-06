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


# Component: EclDirect 
#####################################################
# Description:
# ------------
#    Cmake Input File for EclDirect
#####################################################

project( EclDirect ) 

include(${HPCC_SOURCE_DIR}/esp/scm/espscm.cmake)

set (    SRCS 
         ${ESPSCM_GENERATED_DIR}/ecldirect_esp.cpp 
         EclDirectPlugin.cpp 
         EclDirectService.cpp 
    )

include_directories ( 
         ./../.. 
         ./../../../system/mp 
         ./../../../system/jlib 
         ./../../services 
         ./../../platform 
         ./../../../system/xmllib 
         ./../../../system/security/shared 
         ./../../../system/security/securesocket 
         ./../../../system/include 
         ./../../../common/environment 
         ./../../../common/workunit 
         ./../../../common/wuwebview
         ./../../clients 
         ./../../../common/fileview2 
         ./../../../dali/base 
         ./../../bindings 
         ./../../bindings/SOAP/xpp 
    )

ADD_DEFINITIONS( -D_USRDLL )

HPCC_ADD_LIBRARY( EclDirect SHARED ${SRCS} )
add_dependencies( EclDirect espscm )
install ( TARGETS EclDirect DESTINATION ${OSSDIR}/lib COMPONENT Runtime)
target_link_libraries ( EclDirect  
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
         wuwebview
         jhtree 
         hql 
         fileview2 
         securesocket 
    )

