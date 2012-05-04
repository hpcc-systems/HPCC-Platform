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

# Component: roxiemem 
#####################################################
# Description:
# ------------
#    Cmake Input File for roxiemem
#####################################################

project( roxiemem ) 

set (    SRCS 
         roxierow.cpp
         roxiemem.cpp 
         roxierowbuff.cpp
    )
         
set (    INCLUDES
         roxierow.hpp
         roxiemem.hpp
         roxierowbuff.hpp
    )

include_directories (
         ./../../system/include
         ./../../system/jlib
         ./../../rtl/include
         ./../../rtl/eclrtl
         ./../../common/thorhelper
    )

ADD_DEFINITIONS( -D_USRDLL -DROXIEMEM_EXPORTS)
if (${CMAKE_COMPILER_IS_GNUCXX})
    ADD_DEFINITIONS( -Wno-invalid-offsetof )
endif (${CMAKE_COMPILER_IS_GNUCXX})

HPCC_ADD_LIBRARY( roxiemem SHARED ${SRCS} ${INCLUDES})
install ( TARGETS roxiemem DESTINATION ${OSSDIR}/lib )
target_link_libraries ( roxiemem
         jlib
         thorhelper
         eclrtl
         ${CPPUNIT_LIBRARIES}
    )


