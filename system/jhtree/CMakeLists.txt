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

# Component: jhtree 

#####################################################
# Description:
# ------------
#    Cmake Input File for jhtree
#####################################################


project( jhtree ) 

set (    SRCS 
         hlzw.cpp 
         ctfile.cpp 
         jhrtl.cpp 
         jhtree.cpp 
         jhutil.cpp 
         keybuild.cpp 
         keydiff.cpp 
         sourcedoc.xml
    )

set (    INCLUDES
         ctfile.hpp
         hlzw.h
         jhtree.hpp
         jhutil.hpp
         keybuild.hpp
         keydiff.hpp
    )

include_directories ( 
         .
         ./../../common/deftype 
         ./../../common/thorhelper 
         ./../../rtl/eclrtl 
         ./../../system/include 
         ./../../system/jlib 
    )

ADD_DEFINITIONS( -D_USRDLL -DJHTREE_EXPORTS -DECLRTL_LOCAL )

HPCC_ADD_LIBRARY( jhtree SHARED ${SRCS} ${INCLUDES} )
install ( TARGETS jhtree DESTINATION ${OSSDIR}/lib )
target_link_libraries ( jhtree 
     jlib
     ${CPPUNIT_LIBRARIES}
)

