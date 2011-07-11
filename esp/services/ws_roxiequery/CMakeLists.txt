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


# Component: ws_roxiequery 
#####################################################
# Description:
# ------------
#    Cmake Input File for ws_roxiequery
#####################################################

project( ws_roxiequery ) 

include(${HPCC_SOURCE_DIR}/esp/scm/smcscm.cmake)

set (    SRCS 
         ${ESPSCM_GENERATED_DIR}/ws_roxiequery_esp.cpp 
         ws_roxiequeryplugin.cpp 
         ws_roxiequeryservice.cpp 
         
         ws_roxiequeryservice.cpp
         
         ${HPCC_SOURCE_DIR}/esp/scm/ws_roxiequery.ecm
    )

include_directories ( 
         ./../../../system/mysql 
         ./../../../dali/dfu 
         ./../../../system/mp 
         ./../../platform 
         ./../../../system/jlib 
         ./../../../common/environment 
         ./../../services 
         ./../../../dali/ft 
         ./../common 
         ./../../../system/icu/include 
         ./../../../system/xmllib 
         ./../../../esp/bindings/http/platform 
         ./../../../ecl/attrserver 
         ./../../../system/security/securesocket 
         ./../../../system/security/shared 
         ./../../../system/include 
         ./../../../common/workunit 
         ./../../../common/remote 
         ./../../clients 
         ./../../../roxie/roxieconfig 
         ./../../../esp/esplib 
         ./../../../dali/base 
         ./../../bindings/SOAP/scrubbed 
         ./../ws_workunits 
         ./../../../rtl/include 
         ./../../../common/dllserver 
         ./../../bindings 
         ./../../smc/SMCLib 
         ./../../../system/mysql/include 
         ./../../bindings/SOAP/xpp 
         ./../../../common/fileview2 
         ./../../../rtl/eclrtl 
    )

ADD_DEFINITIONS( -D_USRDLL )

HPCC_ADD_LIBRARY( ws_roxiequery SHARED ${SRCS} )
install ( TARGETS ws_roxiequery DESTINATION ${OSSDIR}/lib )
target_link_libraries ( ws_roxiequery 
         jlib
         fileview2
         remote 
         ${XALAN_LIBRARIES} ${XERCES_LIBRARIES}
         xmllib 
         esphttp 
         dalibase 
         environment 
         dalift 
         dllserver 
         nbcd 
         eclrtl 
         deftype 
         workunit 
         SMCLib 
         roxiecommlib 
         roxiemanager
    )

