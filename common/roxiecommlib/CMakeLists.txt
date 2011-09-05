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


# Component: roxiecommlib 
#####################################################
# Description:
# ------------
#    Cmake Input File for roxiecommlib
#####################################################

project( roxiecommlib ) 

include(${HPCC_SOURCE_DIR}/esp/scm/espscm.cmake)

set (    SRCS 
         ${ESPSCM_GENERATED_DIR}/roxiecommlibscm_esp.cpp 
                 ${HPCC_SOURCE_DIR}/esp/scm/roxiecommlibscm.ecm
         roxiecommunicationclient.cpp 
    )

include_directories ( 
         ./../../common/remote 
         ./../../system/mp 
         ./../../common/workunit 
         ./../../common/environment 
         ./../../roxie/ccd 
         ./../../common/fileview2 
         ./../../system/include 
         ./../../system/security/shared
         ./../../dali/base 
         ./../../rtl/include 
         ./../../common/dllserver 
         ./../../esp/platform 
         ./../../esp/bindings 
         ./../../esp/clients 
         ./../../esp/bindings/SOAP/xpp 
         ./../../system/jlib 
         ./../../rtl/eclrtl 
    )

ADD_DEFINITIONS( -D_USRDLL -DROXIECOMMLIB_EXPORTS )

HPCC_ADD_LIBRARY( roxiecommlib SHARED ${SRCS} )
add_dependencies( roxiecommlib espscm )
install ( TARGETS roxiecommlib DESTINATION ${OSSDIR}/lib )
target_link_libraries ( roxiecommlib
         jlib 
    )


