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


# Component: commonext 
#####################################################
# Description:
# ------------
#    Cmake Input File for commonext
#####################################################

project( commonext ) 

set (    SRCS 
         thorport.cpp 
         commonext.cpp 
         
         commonext.hpp 
    )

include_directories ( 
         ./../../rtl/include 
         ./../../system/include 
         ./../workunit 
         ./../../system/jlib 
    )

ADD_DEFINITIONS( -D_USRDLL -DCOMMONEXT_EXPORTS )

HPCC_ADD_LIBRARY( commonext SHARED ${SRCS} )
add_dependencies (commonext espscm )
install ( TARGETS commonext DESTINATION ${OSSDIR}/lib )
target_link_libraries ( commonext 
         jlib 
    )


