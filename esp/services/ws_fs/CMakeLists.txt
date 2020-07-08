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
         ./../../../fs/dafsclient 
         ./../../../common/workunit
         ${HPCC_SOURCE_DIR}/common/thorhelper
    )

ADD_DEFINITIONS( -D_USRDLL )

HPCC_ADD_LIBRARY( ws_fs SHARED ${SRCS} )
add_dependencies ( ws_fs espscm )
install ( TARGETS ws_fs RUNTIME DESTINATION ${EXEC_DIR} LIBRARY DESTINATION ${LIB_DIR} )
target_link_libraries ( ws_fs
         jlib
         esphttp 
         dalibase 
         environment 
         workunit
         SMCLib 
         dfuwu 
    )
