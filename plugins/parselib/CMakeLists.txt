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


# Component: parselib 

#####################################################
# Description:
# ------------
#    Cmake Input File for parselib
#####################################################

project( parselib ) 

set (    SRCS 
         parselib.cpp 
    )

include_directories ( 
         ./../../common/thorhelper 
         ./../../rtl/eclrtl 
         ./../../system/include 
         ./../../system/jlib 
    )

ADD_DEFINITIONS( -D_USRDLL -DPARSELIB_EXPORTS )

if ( NOT CLIENTTOOLS_ONLY )
    HPCC_ADD_LIBRARY( parselib SHARED ${SRCS} )
    install ( TARGETS parselib DESTINATION ${OSSDIR}/plugins )
    target_link_libraries ( parselib 
         jlib
    )
endif()

