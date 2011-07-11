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

# Component: nbcd 

#####################################################
# Description:
# ------------
#    Cmake Input File for nbcd
#####################################################


project( nbcd ) 

set ( SRCS 
      nbcd.cpp 
      nbcds.cpp 
      nbcdtest.cpp 
      sourcedoc.xml
    )

include_directories ( 
      ./../../system/include 
      ./../../system/jlib
    )

ADD_DEFINITIONS( -D_USRDLL -DNBCD_EXPORTS )

HPCC_ADD_LIBRARY( nbcd SHARED ${SRCS} )
install ( TARGETS nbcd DESTINATION ${OSSDIR}/lib )
target_link_libraries ( nbcd 
      jlib
      ${CPPUNIT_LIBRARIES}
    )


