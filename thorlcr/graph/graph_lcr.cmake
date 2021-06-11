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

# Component: graph_lcr 
#####################################################
# Description:
# ------------
#    Cmake Input File for graph_lcr
#####################################################

project( graph_lcr ) 

set (    SRCS 
         ../thorutil/thbuf.cpp 
         ../thorutil/thcompressutil.cpp 
         ../thorutil/thmem.cpp 
         ../thorutil/thorport.cpp
         ../thorutil/thalloc.cpp 
         ../thorutil/thormisc.cpp 
         thgraph.cpp 

         ../thorutil/thorport.hpp
    )

include_directories ( 
         ./../thorutil 
         ./../../system/jhtree 
         ./../../system/mp 
         ./../../rtl/include 
         ./../../common/workunit 
         ./../shared 
         ./../../common/deftype 
         ./../../system/include 
         ./../../dali/base 
         ./../../rtl/include 
         ./../../common/dllserver 
         ./../../system/jlib 
         ./../thorcodectx 
         ./../mfilemanager 
         ./../../rtl/eclrtl 
         ./../../common/thorhelper 
         ./../../roxie/roxiemem
         ./../../system/security/shared
    )

HPCC_ADD_LIBRARY( graph_lcr SHARED ${SRCS} )
set_target_properties(graph_lcr PROPERTIES 
    COMPILE_FLAGS -D_USRDLL
    DEFINE_SYMBOL GRAPH_EXPORTS )
install ( TARGETS graph_lcr RUNTIME DESTINATION ${EXEC_DIR} LIBRARY DESTINATION ${LIB_DIR} )
target_link_libraries ( graph_lcr 
         jlib
         jhtree 
         remote 
         dalibase 
         dllserver 
         nbcd 
         eclrtl 
         deftype 
         workunit 
         thorhelper
         roxiemem
    )

if (USE_TBBMALLOC)
   add_dependencies ( graph_lcr tbb)
   target_link_libraries ( graph_lcr libtbbmalloc_proxy libtbbmalloc)
endif()


