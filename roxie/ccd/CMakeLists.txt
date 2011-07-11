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


# Component: ccd 

#####################################################
# Description:
# ------------
#    Cmake Input File for ccd
#####################################################


project( ccd ) 

set (   SRCS 
        ccdactivities.cpp 
        ccddali.cpp 
        ccdserver.cpp 
        ccdfile.cpp 
        ccdkey.cpp 
        ccdmain.cpp 
        ccdquery.cpp 
        ccdqueue.cpp 
        ccdsnmp.cpp 
        ccdstate.cpp 
         
        ccd.hpp
        ccdactivities.hpp
        ccddali.hpp
        ccdserver.hpp
        ccdfile.hpp
        ccdkey.hpp
        ccdquery.hpp
        ccdqueue.ipp
        ccdsnmp.hpp
        ccdstate.hpp 
        
                sourcedoc.xml
    )

include_directories ( 
         .
         ./../../common/remote
         ./../../system/jhtree 
         ./../../system/mp 
         ./../../common/ns 
         ./../../common/workunit 
         ./../../roxie/udplib 
         ./../../roxie/roxie 
         ./../../common/environment 
         ./../../ecl/hthor 
         ./../../system/icu/include 
         ./../../rtl/nbcd 
         ./../../common/deftype 
         ./../../system/include 
         ./../../dali/base 
         ./../../roxie/roxiemem 
         ./../../common/dllserver 
         ./../../system/jlib 
         ./../../common/thorhelper 
         ./../../rtl/eclrtl 
         ./../../common/roxiehelper 
         ./../../rtl/include
         ${CMAKE_BINARY_DIR}
         ${CMAKE_BINARY_DIR}/oss
    )

ADD_DEFINITIONS( -D_USRDLL -DCCD_EXPORTS -DSTARTQUERY_EXPORTS )

HPCC_ADD_LIBRARY( ccd SHARED ${SRCS} )
install ( TARGETS ccd DESTINATION ${OSSDIR}/lib )

target_link_libraries ( ccd 
         jlib
         nbcd 
         roxiemem 
         udplib 
         remote 
         eclrtl 
         dalibase 
         deftype 
         securesocket 
         thorhelper 
         jhtree 
         environment 
         dllserver 
         roxiehelper 
         workunit 
    )



