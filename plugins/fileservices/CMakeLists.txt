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


# Component: fileservices 
#####################################################
# Description:
# ------------
#    Cmake Input File for fileservices
#####################################################

project( fileservices ) 
include(${HPCC_SOURCE_DIR}/esp/scm/smcscm.cmake)

set ( SRCS 
      ${ESPSCM_GENERATED_DIR}/ws_dfu_esp.cpp 
      ${ESPSCM_GENERATED_DIR}/ws_fs_esp.cpp 
      fileservices.cpp 
      ../../esp/bindings/bindutil.cpp 
      ../../dali/dfuplus/dfuplus.cpp 
    )

include_directories ( 
         .
         ./../../common/remote 
         ./../../system/jhtree 
         ./../../system/mp 
         ./../../common/workunit 
         ./../../esp/clients 
         ./../../dali/ft 
         ./../../system/security/shared 
         ./../../esp/bindings/SOAP/xpp 
         ./../../common/deftype 
         ./../../system/include 
         ./../../esp/bindings 
         ./../../dali/base 
         ./../../ecl/eclagent 
         ./../../rtl/include 
         ./../../system/xmllib 
         ./../../esp/platform 
         ./../../system/jlib 
         ./../../common/thorhelper 
         ./../../rtl/eclrtl 
         ./../../dali/dfu 
         ./../../dali/dfuplus
            )

ADD_DEFINITIONS( -D_USRDLL -DFILESERVICES_EXPORTS )

if ( NOT CLIENTTOOLS_ONLY )
    HPCC_ADD_LIBRARY( fileservices SHARED ${SRCS} )  
    add_dependencies ( fileservices espscm )
    install ( TARGETS fileservices DESTINATION ${OSSDIR}/plugins )
    target_link_libraries ( fileservices
         jlib
         remote 
         dalibase 
         environment 
         dllserver 
         nbcd 
         eclrtl  
         deftype 
         workunit 
         soaplib 
    )
endif()

