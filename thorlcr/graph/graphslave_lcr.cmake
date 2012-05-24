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

# Component: graphslave_lcr 
#####################################################
# Description:
# ------------
#    Cmake Input File for graphslave_lcr
#####################################################

project( graphslave_lcr ) 

set (    SRCS 
         thgraphslave.cpp 
    )

include_directories ( 
         ./../thorutil 
         ./../../common/remote 
         ./../../system/jhtree 
         ./../../system/mp 
         ./../../common/workunit 
         ./../shared 
         ./../../common/deftype 
         ./../../system/include 
         ./../../dali/base 
         ./../../rtl/include 
         ./../../common/dllserver 
         ./../slave 
         ./../../system/jlib 
         ./../mfilemanager 
         ./../../common/commonext 
         ./../../rtl/eclrtl 
         ./../../common/thorhelper 
    )

HPCC_ADD_LIBRARY( graphslave_lcr SHARED ${SRCS} )
set_target_properties(graphslave_lcr PROPERTIES 
    COMPILE_FLAGS -D_USRDLL
    DEFINE_SYMBOL GRAPHSLAVE_EXPORTS )
install ( TARGETS graphslave_lcr DESTINATION ${OSSDIR}/lib )
target_link_libraries ( graphslave_lcr 
         jlib
         commonext 
         nbcd 
         eclrtl 
         deftype 
         thorhelper 
         remote 
         dalibase 
         environment 
         dllserver 
         workunit 
         thorcodectx_lcr 
         jhtree 
         graph_lcr 
    )


