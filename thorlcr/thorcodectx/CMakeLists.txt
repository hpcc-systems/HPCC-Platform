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
    )

ADD_DEFINITIONS( -DTHORCODECTX_EXPORTS -D_USRDLL )

if ( NOT CLIENTTOOLS_ONLY )
    HPCC_ADD_LIBRARY( thorcodectx_lcr SHARED ${SRCS} )
    install ( TARGETS thorcodectx_lcr DESTINATION ${OSSDIR}/lib )
    target_link_libraries ( thorcodectx_lcr
        graph_lcr
        jlib
        nbcd 
        eclrtl 
        deftype 
        thorhelper 
        remote 
        dalibase 
        environment 
        dllserver 
        workunit 
    )
endif()

