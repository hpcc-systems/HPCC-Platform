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

# Component: thormaster_lcr 
#####################################################
# Description:
# ------------
#    Cmake Input File for thormaster_lcr
#####################################################

project( thormaster_lcr ) 

set (    SRCS 
         mawatchdog.cpp 
         thdemonserver.cpp 
         thgraphmanager.cpp 
         thmastermain.cpp 
    )

include_directories ( 
         ./../thorutil 
         ./../../common/remote 
         ./../../system/jhtree 
         ./../../system/mp 
         ./../thorcrc 
         ./../../common/workunit 
         ./../shared 
         ./../graph 
         ./../../common/environment 
         ./../../common/deftype 
         ./../../system/include 
         ./../../dali/base 
         ./../../rtl/include 
         ./../../common/dllserver 
         ./../../system/jlib 
         ./../thorcodectx 
         ./../mfilemanager 
         ./../../common/commonext 
         ./../activities 
         ./../../rtl/eclrtl 
         ./../master 
         ./../../common/thorhelper 
         ./../../tools/swapnode
         ./../../roxie/roxiemem
         ${CMAKE_BINARY_DIR}
         ${CMAKE_BINARY_DIR}/oss
    )

ADD_DEFINITIONS( -D_CONSOLE )

if (WIN32)
    set (CMAKE_EXE_LINKER_FLAGS "/STACK:65536 ${CMAKE_EXE_LINKER_FLAGS}")
endif()

add_executable ( thormaster_lcr ${SRCS} )
install ( TARGETS thormaster_lcr DESTINATION ${OSSDIR}/bin )
target_link_libraries (  thormaster_lcr 
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
         graph_lcr 
         thorsort_lcr 
         thorhelper 
         thorcodectx_lcr 
         dalift 
         mfilemanager_lcr 
         graphmaster_lcr 
         activitymasters_lcr 
         swapnodelib
    )



