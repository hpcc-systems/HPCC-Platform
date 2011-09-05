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

# Component: mfilemanager_lcr 

#####################################################
# Description:
# ------------
#    Cmake Input File for mfilemanager_lcr
#####################################################

project( mfilemanager_lcr ) 

set (    SRCS 
         ../mfilemanager/thmfilemanager.cpp 
    )

include_directories ( 
         ./../thorutil 
         ./../../common/remote 
         ./../../system/mp 
         ./../master 
         ./../thorcrc 
         ./../../common/workunit 
         ./../shared 
         ./../graph 
         ./../../common/environment 
         ./../../dali/ft 
         ./../../common/deftype 
         ./../../system/include 
         ./../../dali/base 
         ./../../rtl/include 
         ./../../rtl/eclrtl 
         ./../../common/dllserver 
         ./../../system/jlib 
         ./../thorcodectx 
         ./../mfilemanager 
         ./../../common/commonext 
         ./../../common/thorhelper 
    )

ADD_DEFINITIONS( -D_USRDLL -DMFILEMANAGER_EXPORTS )

HPCC_ADD_LIBRARY( mfilemanager_lcr SHARED ${SRCS} )
install ( TARGETS mfilemanager_lcr DESTINATION ${OSSDIR}/lib )
target_link_libraries ( mfilemanager_lcr
         jlib
         remote 
         dalibase 
         environment 
         dalift 
         dllserver 
         nbcd 
         eclrtl 
         deftype 
         workunit 
         jhtree 
         commonext 
         graph_lcr 
    )


