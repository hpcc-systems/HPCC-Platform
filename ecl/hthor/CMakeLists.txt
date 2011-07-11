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


# Component: hthor 

#####################################################
# Description:
# ------------
#    Cmake Input File for hthor
#####################################################


project( hthor ) 

set (    SRCS 
         hthor.cpp 
         hthorkey.cpp 
         hthorstep.cpp 
         ../eclagent/eclagent.cpp
         ../eclagent/eclgraph.cpp
         sourcedoc.xml
    )

set (    INCLUDES 
         hthor.ipp 
         hthor.hpp
         hthorstep.ipp 
         ../eclagent/agentctx.hpp
         ../eclagent/eclagent.ipp
    )

include_directories ( 
         .
         ./../../common/remote 
         ./../../system/jhtree 
         ./../../system/hrpc 
         ./../../system/mp 
         ./../../common/ns 
         ./../../common/workunit 
         ./../../system/icu/include 
         ./../../common/deftype 
         ./../../system/include 
         ./../../dali/base 
         ./../../rtl/include 
         ./../../ecl/eclagent 
         ./../../system/jlib 
         ./../../common/thorhelper 
         ./../../rtl/eclrtl 
         ./../../roxie/roxiemem 
         ./../../roxie/roxie
         ./../../roxie/ccd
         ./../../common/roxiehelper 
         ./../../common/dllserver
         ./../../common/environment
         ./../schedulectrl
         ./../../common/commonext
    )

ADD_DEFINITIONS( -D_USRDLL -DHTHOR_EXPORTS -DSTARTQUERY_EXPORTS )

HPCC_ADD_LIBRARY( hthor SHARED ${SRCS} ${INCLUDES} )
install ( TARGETS hthor DESTINATION ${OSSDIR}/lib )
target_link_libraries ( hthor 
         jlib
         mp 
         hrpc 
         remote 
         dalibase 
         environment 
         dllserver 
         nbcd 
         eclrtl 
         deftype 
         workunit 
         jhtree 
         securesocket 
         thorhelper 
         roxiemem 
         roxiehelper 
         commonext
         schedulectrl
    )

