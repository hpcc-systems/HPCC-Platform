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

# Component: graph_lcr 
#####################################################
# Description:
# ------------
#    Cmake Input File for graph_lcr
#####################################################

project( graph_lcr ) 

set (    SRCS 
         ../thorutil/mcorecache.cpp 
         ../thorutil/thbuf.cpp 
         ../thorutil/thcompressutil.cpp 
         ../thorutil/thmem.cpp 
         ../thorutil/thalloc.cpp 
         ../thorutil/thormisc.cpp 
         thgraph.cpp 
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
         ./../../common/commonext 
         ./../../rtl/eclrtl 
         ./../../common/thorhelper 
         ./../../roxie/roxiemem
    )

HPCC_ADD_LIBRARY( graph_lcr SHARED ${SRCS} )
set_target_properties(graph_lcr PROPERTIES 
    COMPILE_FLAGS -D_USRDLL
    DEFINE_SYMBOL GRAPH_EXPORTS )
install ( TARGETS graph_lcr DESTINATION ${OSSDIR}/lib )
target_link_libraries ( graph_lcr 
         jlib
         jhtree 
         remote 
         dalibase 
         environment 
         dllserver 
         nbcd 
         eclrtl 
         deftype 
         workunit 
         commonext 
         thorhelper
         roxiemem
    )


