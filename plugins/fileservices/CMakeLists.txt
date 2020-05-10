################################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
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
      ${ESPSCM_GENERATED_DIR}/ws_dfu_common_esp.cpp
      ${ESPSCM_GENERATED_DIR}/ws_dfu_esp.cpp 
      ${ESPSCM_GENERATED_DIR}/ws_fs_esp.cpp 
      fileservices.cpp 
      ../../esp/bindings/bindutil.cpp 
      ../../dali/dfuplus/dfuplus.cpp 
    )

include_directories ( 
         .
         ./../../common/environment
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
         ./../../system/jlib
            )

ADD_DEFINITIONS( -D_USRDLL -DFILESERVICES_EXPORTS )

HPCC_ADD_LIBRARY( fileservices SHARED ${SRCS} )  
add_dependencies ( fileservices espscm )
install ( TARGETS fileservices DESTINATION plugins )
target_link_libraries ( fileservices
         jlib
         remote 
         dalibase 
         environment 
         esphttp
         dllserver 
         nbcd 
         eclrtl  
         deftype 
         workunit 
    )


