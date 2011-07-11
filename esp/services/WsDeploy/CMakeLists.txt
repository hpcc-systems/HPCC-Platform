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


# Component: WsDeploy 
#####################################################
# Description:
# ------------
#    Cmake Input File for WsDeploy
#####################################################

project( WsDeploy ) 

include(${HPCC_SOURCE_DIR}/esp/scm/additional.cmake)

set (    SRCS 
         ${ESPSCM_GENERATED_DIR}/WsDeploy_esp.cpp 
         WsDeployPlugin.cpp 
         WsDeployEngine.cpp 
         WsDeployBinding.cpp 
         WsDeployService.cpp 
    )

include_directories ( 
         ./../../esplib 
         ./../.. 
         ./../../platform 
         ./../../../system/jlib 
         ./../../../common/environment 
         ./../../services 
         ./../common 
         ./../../../system/icu/include 
         ./../../../system/xmllib 
         ./../../../system/security/securesocket 
         ./../../../system/security/shared 
         ./../../../system/include 
         ./../../../common/remote 
         ./../../clients 
         ./../../../dali/base 
         ./../../../common/dllserver 
         ./../../bindings 
         ./../../bindings/SOAP/xpp 
         ./../../bindings/http/client
         ./../../../system/mp 
         ../../../deployment/deploy 
         ../../../deployment/deployutils 
         ${CMAKE_BINARY_DIR}
         ${CMAKE_BINARY_DIR}/oss
    )

ADD_DEFINITIONS( -D_USRDLL )

HPCC_ADD_LIBRARY( WsDeploy SHARED ${SRCS}  )
add_dependencies (WsDeploy espscm)
install ( TARGETS WsDeploy DESTINATION ${OSSDIR}/lib )
target_link_libraries ( WsDeploy  
         jlib
         xmllib 
         esphttp 
         dalibase 
         environment 
         dllserver 
         eclrtl 
         hql 
         securesocket 
     deploy 
     deployutils     
    )


