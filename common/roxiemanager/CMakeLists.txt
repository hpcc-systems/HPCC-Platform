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


# Component: roxiemanager 
#####################################################
# Description:
# ------------
#    Cmake Input File for roxiemanager
#####################################################

project( roxiemanager ) 

include(${HPCC_SOURCE_DIR}/esp/scm/espscm.cmake)

set (    SRCS 
         ../../dali/dfu/dfuutil.cpp 
         ${ESPSCM_GENERATED_DIR}/roxiecommlibscm_esp.cpp 
         ${ESPSCM_GENERATED_DIR}/roxiemanagerscm_esp.cpp 
         roxiequerycompiler.cpp 
         roxiequerymanager.cpp 
         roxiewuprocessor.cpp 
                 
                 roxiemanager.hpp
                 roxiequerycompiler.hpp
                 roxiewuprocessor.hpp
                 
                ${HPCC_SOURCE_DIR}/esp/scm/roxiemanagerscm.ecm
    )

include_directories ( 
         ./../../system/security/shared
         ./../../common/remote 
         ./../../system/mp 
         ./../../common/workunit 
         ./../../dali/dfu 
         ./../../common/environment 
         ./../../roxie/roxie
         ./../../roxie/ccd
         ./../../common/fileview2 
         ./../../system/include 
         ./../../dali/base 
         ./../../rtl/include 
         ./../../common/dllserver 
         ./../../system/xmllib
         ./../../esp/clients 
         ./../../esp/platform 
         ./../../esp/bindings 
         ./../../esp/bindings/SOAP/xpp 
         ./../../system/jlib 
         ./../../rtl/eclrtl 
    )

ADD_DEFINITIONS( -D_USRDLL -DROXIEMANAGER_EXPORTS )

HPCC_ADD_LIBRARY( roxiemanager SHARED ${SRCS} )
add_dependencies( roxiemanager espscm )
install ( TARGETS roxiemanager DESTINATION ${OSSDIR}/lib )
target_link_libraries ( roxiemanager
         jlib 
         roxiecommlib 
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
    )


