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

# Component: udplib 

#####################################################
# Description:
# ------------
#    Cmake Input File for udplib
#####################################################


project( udplib ) 

set (    SRCS 
         udpmsgpk.cpp 
         udpsha.cpp 
         udptrr.cpp 
         udptrs.cpp 
    )

include_directories ( 
         ./../../roxie/roxiemem 
         ./../../system/include 
         ./../../system/jlib 
         ./../../roxie/ccd
         ./../../roxie/roxie
    )

if (${CMAKE_COMPILER_IS_GNUCXX})
    ADD_DEFINITIONS( -Wno-invalid-offsetof )
endif (${CMAKE_COMPILER_IS_GNUCXX})
HPCC_ADD_LIBRARY( udplib SHARED ${SRCS} )
set_target_properties( udplib PROPERTIES 
    COMPILE_FLAGS -D_USRDLL
    DEFINE_SYMBOL UDPLIB_EXPORTS )
install ( TARGETS udplib DESTINATION ${OSSDIR}/lib )
target_link_libraries ( udplib 
         jlib
         roxiemem 
    )


