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


# Component: ws_config 
#####################################################
# Description:
# ------------
#    Cmake Input File for ws_config
#####################################################

project( ws_config ) 

include(${HPCC_SOURCE_DIR}/esp/scm/additional.cmake)

set (    SRCS 
         ${ESPSCM_GENERATED_DIR}/ws_config_esp.cpp 
         ws_configPlugin.cpp 
         ws_configService.cpp 
    )

include_directories ( 
         ./../../esplib 
         ./../.. 
         ./../../platform 
         ./../../../system/jlib 
         ./../../services 
         ./../../../system/security/securesocket 
         ./../../../system/security/shared 
         ./../../../system/include 
         ./../../../system/xmllib
         ./../../../system/mp 
         ./../../../common/environment
         ./../../clients 
         ./../../../dali/base 
         ./../../bindings 
         ./../../bindings/SOAP/xpp 
    )

ADD_DEFINITIONS( -D_USRDLL )

HPCC_ADD_LIBRARY( ws_config SHARED ${SRCS} )
install ( TARGETS ws_config DESTINATION ${OSSDIR}/lib )
add_dependencies (ws_config espscm)
target_link_libraries ( ws_config 
         jlib
         mp 
         hrpc 
         remote 
         dalibase 
         environment 
         ${XALAN_LIBRARIES} ${XERCES_LIBRARIES}
         xmllib 
         esphttp 
    )


