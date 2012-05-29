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
         ./../../common/commonext 
         ./../thorutil 
         ./../../common/thorhelper 
         ./../../common/dllserver 
         ./../../roxie/roxiemem
         ${CMAKE_CURRENT_BINARY_DIR}
    )


ADD_DEFINITIONS( -D_LIB )

if ( NOT CLIENTTOOLS_ONLY )
    if (WIN32)
        HPCC_ADD_LIBRARY( thorsort_lcr STATIC ${SRCS} )
        add_dependencies( thorsort_lcr
            jlib
        )
    else ()
        HPCC_ADD_LIBRARY( thorsort_lcr SHARED ${SRCS} )
        install ( TARGETS thorsort_lcr DESTINATION ${OSSDIR}/lib )
        target_link_libraries ( thorsort_lcr
            jlib
            eclrtl
            commonext
            thorhelper
            mp
            dalibase
            graph_lcr
            roxiemem
        )
    endif (WIN32)
endif()
