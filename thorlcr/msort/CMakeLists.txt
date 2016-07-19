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

# Component: thorsort_lcr 

#####################################################
# Description:
# ------------
#    Cmake Input File for thorsort_lcr
#####################################################

project( thorsort_lcr ) 

set (    SRCS 
         tsorta.cpp 
         tsortl.cpp 
         tsortm.cpp 
         tsorts.cpp 
         tsorts1.cpp 
         tsortmp.cpp 
    )

include_directories ( 
         ./
         ./..
         ./../slave 
         ./../../system/mp 
         ./../graph 
         ./../../rtl/include 
         ./../../rtl/eclrtl 
         ./../../system/include 
         ./../../dali/base 
         ./../shared 
         ./../../system/jlib 
         ./../../common/workunit 
         ./../thorutil 
         ./../../common/thorhelper 
         ./../../common/dllserver 
         ./../../roxie/roxiemem
         ${CMAKE_CURRENT_BINARY_DIR}
    )


ADD_DEFINITIONS( -D_LIB )
ADD_DEFINITIONS( -DTHORSORT_EXPORTS )

HPCC_ADD_LIBRARY( thorsort_lcr SHARED ${SRCS} )
install ( TARGETS thorsort_lcr RUNTIME DESTINATION ${EXEC_DIR} LIBRARY DESTINATION ${LIB_DIR} )
target_link_libraries ( thorsort_lcr
        jlib
        eclrtl
        thorhelper
        mp
        dalibase
        graph_lcr
        roxiemem
)

