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
         ./../../common/commonext 
         ./../activities 
         ./../../rtl/eclrtl 
         ./../../common/thorhelper 
    )

HPCC_ADD_LIBRARY( graphmaster_lcr SHARED ${SRCS} )
set_target_properties(graphmaster_lcr PROPERTIES 
    COMPILE_FLAGS -D_USRDLL
    DEFINE_SYMBOL GRAPHMASTER_EXPORTS )
install ( TARGETS graphmaster_lcr DESTINATION ${OSSDIR}/lib )
target_link_libraries ( graphmaster_lcr
         jlib
         commonext 
         jhtree 
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
         graph_lcr 
         dalift 
         mfilemanager_lcr 
    )


