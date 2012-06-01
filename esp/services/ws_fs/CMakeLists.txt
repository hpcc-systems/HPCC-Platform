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


# Component: ws_fs 

#####################################################
# Description:
# ------------
#    Cmake Input File for ws_fs
#####################################################


project( ws_fs ) 

include(${HPCC_SOURCE_DIR}/esp/scm/smcscm.cmake)

set (    SRCS 
         ../../../dali/dfu/dfuutil.cpp 
         ../../../dali/sasha/sacmd.cpp 
         ${ESPSCM_GENERATED_DIR}/ws_fs_esp.cpp 
         ws_fsBinding.cpp 
         ws_fsPlugin.cpp 
         ws_fsService.cpp 
    )

include_directories ( 
         ./../../../dali/dfu 
         ./../../../system/mp 
         ./../../platform 
         ./../../../dali/sasha 
         ./../../../system/jlib 
         ./../../../common/environment 
         ./../../../dali/ft 
         ./../common 
         ./../../../esp/bindings/http/platform 
         ./../../../system/security/securesocket 
         ./../../../system/security/shared 
         ./../../../system/include 
         ./../../../system/xmllib
         ./../../clients 
         ./../../../esp/esplib 
         ./../../../dali/base 
         ./../../bindings 
         ./../../smc/SMCLib 
         ./../../bindings/SOAP/xpp 
         ./../../../common/remote 
    )

ADD_DEFINITIONS( -D_USRDLL )

HPCC_ADD_LIBRARY( ws_fs SHARED ${SRCS} )
add_dependencies ( ws_fs espscm )
install ( TARGETS ws_fs DESTINATION ${OSSDIR}/lib )
target_link_libraries ( ws_fs
     jlib
     esphttp 
     dalibase 
     environment 
     SMCLib 
     dfuwu 
)
