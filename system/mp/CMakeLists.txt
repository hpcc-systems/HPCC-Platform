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

# Component: mp 

#####################################################
# Description:
# ------------
#    Cmake Input File for mp
#####################################################


project( mp ) 

set (    SRCS 
         mpbase.cpp 
         mpcomm.cpp 
         mplog.cpp 
         mptag.cpp 
    )

include_directories ( 
         ./../include 
         .
         ./../jlib
    )

ADD_DEFINITIONS( -DLOGMSGCOMPONENT=2 -D_USRDLL -DMP_EXPORTS )

HPCC_ADD_LIBRARY( mp SHARED ${SRCS} )
install ( TARGETS mp DESTINATION ${OSSDIR}/lib )
target_link_libraries ( mp 
         jlib
    )

add_subdirectory(test)
