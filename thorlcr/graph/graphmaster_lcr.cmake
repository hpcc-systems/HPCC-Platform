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

# Component: graphmaster_lcr 

#####################################################
# Description:
# ------------
#    Cmake Input File for graphmaster_lcr
#####################################################


project( graphmaster_lcr ) 

set (    SRCS 
         thgraphmaster.cpp 
    )

include_directories ( 
         ./../thorutil 
         ./../../system/jhtree 
         ./../../system/mp 
         ./../master 
         ./../../common/workunit 
         ./../shared 
         ./../graph 
         ./../../common/deftype 
         ./../../system/include 
         ./../../dali/base 
         ./../../rtl/include 
         ./../../common/dllserver 
         ./../slave 
         ./../../system/jlib 
         ./../thorcodectx 
         ./../mfilemanager 
         ./../activities 
         ./../../rtl/eclrtl 
         ./../../common/thorhelper 
    )

HPCC_ADD_LIBRARY( graphmaster_lcr SHARED ${SRCS} )
set_target_properties(graphmaster_lcr PROPERTIES 
    COMPILE_FLAGS -D_USRDLL
    DEFINE_SYMBOL GRAPHMASTER_EXPORTS )
install ( TARGETS graphmaster_lcr RUNTIME DESTINATION ${EXEC_DIR} LIBRARY DESTINATION ${LIB_DIR} )
target_link_libraries ( graphmaster_lcr
         jlib
         jhtree 
         nbcd 
         eclrtl 
         deftype 
         thorhelper 
         remote 
         dalibase 
         dllserver 
         workunit 
         thorcodectx_lcr 
         graph_lcr 
         dalift 
         mfilemanager_lcr 
    )


