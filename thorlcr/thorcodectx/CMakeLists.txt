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

# Component: thorcodectx_lcr 
#####################################################
# Description:
# ------------
#    Cmake Input File for thorcodectx_lcr
#####################################################

project( thorcodectx_lcr ) 

set (    SRCS 
         thcodectx.cpp 
    )

include_directories ( 
         ./../thorutil 
         ./../../common/remote 
         ./../../system/mp 
         ./../master 
         ./../../common/workunit 
         ./../graph 
         ./../../common/deftype 
         ./../../system/include 
         ./../../dali/base 
         ./../../rtl/include 
         ./../../common/dllserver 
         ./../../system/jlib 
         ./../../common/thorhelper 
         ./../../rtl/eclrtl 
         ./../../thorlcr/shared 
         ./../../roxie/roxiemem
         ./../../system/security/shared
    )

ADD_DEFINITIONS( -DTHORCODECTX_EXPORTS -D_USRDLL )

HPCC_ADD_LIBRARY( thorcodectx_lcr SHARED ${SRCS} )
install ( TARGETS thorcodectx_lcr RUNTIME DESTINATION ${EXEC_DIR} LIBRARY DESTINATION ${LIB_DIR} )
target_link_libraries ( thorcodectx_lcr
        graph_lcr
        jlib
        nbcd 
        eclrtl 
        deftype 
        thorhelper 
        remote 
        dalibase 
        dllserver 
        workunit 
        )


