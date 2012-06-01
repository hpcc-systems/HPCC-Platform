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


# Component: ws_fileio 
#####################################################
# Description:
# ------------
#    Cmake Input File for ws_fileio
#####################################################

project( ws_fileio ) 

include(${HPCC_SOURCE_DIR}/esp/scm/additional.cmake)

set (    SRCS 
         ${ESPSCM_GENERATED_DIR}/ws_fileio_esp.cpp 
         ws_fileioplugin.cpp 
         ws_fileioservice.cpp 
    )

include_directories ( 
         ./../../../dali/dfu 
         ./../../../system/mp 
         ./../../platform 
         ./../../../system/jlib 
         ./../../../common/environment 
         ./../../services 
         ./../../../dali/ft 
         ./../common 
         ./../../../system/xmllib 
         ./../../../esp/bindings/http/platform 
         ./../../../system/security/securesocket 
         ./../../../system/security/shared 
         ./../../../system/include 
         ./../../../common/workunit 
         ./../../../common/remote 
         ./../../clients 
         ./../../../esp/esplib 
         ./../../../dali/base 
         ./../../bindings/SOAP/scrubbed 
         ./../../../rtl/include 
         ./../../../common/dllserver 
         ./../../bindings 
         ./../../smc/SMCLib 
         ./../../bindings/SOAP/xpp 
    )

ADD_DEFINITIONS( -D_USRDLL )

if ( NOT CLIENTTOOLS_ONLY )
    HPCC_ADD_LIBRARY( ws_fileio SHARED ${SRCS} )
    install ( TARGETS ws_fileio DESTINATION ${OSSDIR}/lib )
    add_dependencies (ws_fileio espscm)
    target_link_libraries ( ws_fileio 
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
endif()

