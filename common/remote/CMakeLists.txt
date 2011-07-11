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


# Component: remote 
#####################################################
# Description:
# ------------
#    Cmake Input File for remote
#####################################################

project( remote ) 

set (    SRCS 
         rmtfile.cpp 
         rmtpass.cpp 
         rmtspawn.cpp 
         rmtssh.cpp 
         rmtsmtp.cpp 
         sockfile.cpp 
         
         remoteerr.hpp
         rmtfile.hpp
         rmtpass.hpp
         rmtsmtp.hpp
         rmtspawn.hpp
         rmtssh.hpp
         sockfile.hpp
    )

include_directories ( 
         ./../../system/hrpc 
         ./../../system/mp 
         ./../../system/include 
         ./../../system/jlib 
    )

ADD_DEFINITIONS( -D_USRDLL -DREMOTE_EXPORTS )

HPCC_ADD_LIBRARY( remote SHARED ${SRCS}  )
install ( TARGETS remote DESTINATION ${OSSDIR}/lib )

target_link_libraries ( remote 
    jlib 
    mp 
    )
