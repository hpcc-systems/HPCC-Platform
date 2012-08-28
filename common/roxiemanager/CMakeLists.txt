################################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
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
install ( TARGETS roxiemanager RUNTIME DESTINATION ${EXEC_DIR} LIBRARY DESTINATION ${LIB_DIR} )
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


