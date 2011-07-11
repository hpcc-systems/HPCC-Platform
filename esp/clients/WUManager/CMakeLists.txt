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


# Component: wumanager 
#####################################################
# Description:
# ------------
#    Cmake Input File for wumanager
#####################################################

project( wumanager ) 

include(${HPCC_SOURCE_DIR}/esp/scm/smcscm.cmake)

set (    SRCS 
         ${ESPSCM_GENERATED_DIR}/ws_workunits_esp.cpp
         WUManager.cpp 
    )

include_directories ( 
         ./../../../system/security/shared
         ./../../../system/include 
         ./../../bindings 
         ./../../bindings/SOAP/xpp 
         ./../../../system/xmllib 
         ./../../../system/jlib 
         ./../../platform 
         ./../../clients 
    )

ADD_DEFINITIONS( -D_CONSOLE )

add_executable ( wumanager ${SRCS} )
#install ( TARGETS wumanager DESTINATION ${OSSDIR}/bin ) # do not install - this is an example program

target_link_libraries ( wumanager 
         ${XALAN_LIBRARIES} ${XERCES_LIBRARIES}
         jlib
         xmllib 
         esphttp
         securesocket
    )


