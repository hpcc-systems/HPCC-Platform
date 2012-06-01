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

# Component: frunagent 

#####################################################
# Description:
# ------------
#    Cmake Input File for frunagent
#####################################################


project( frunagent ) 

include (${HPCC_SOURCE_DIR}/common/hohidl/hohidl.cmake)

set (    SRCS 
         ${HOHIDL_GENERATED_DIR}/hagent.cpp 
         hodisp_base.cpp 
         ../../system/hrpc/hrpc.cpp 
         ../../system/hrpc/hrpcsock.cpp 
         ../../system/hrpc/hrpcutil.cpp 
         frunagent.cpp 
    )

include_directories ( 
         ./../../system/hrpc 
         ./../../common/homisc 
         ./../../system/include 
         ./../../system/jlib 
    )

ADD_DEFINITIONS ( -D_CONSOLE )

if ( NOT CLIENTTOOLS_ONLY )
    add_executable ( frunagent ${SRCS} )
    install ( TARGETS frunagent DESTINATION ${OSSDIR}/bin )
    target_link_libraries ( frunagent 
         jlib
         homisc 
         mp 
         hrpc 
    )
endif()

