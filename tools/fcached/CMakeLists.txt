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

# Component: fcached 
#####################################################
# Description:
# ------------
#    Cmake Input File for fcached
#####################################################

project( fcached ) 

include_directories (            ./../../common/deftype 
         ./../../system/mp 
         ./../../system/jhtree 
         ./../../rtl/eclrtl 
         ./../../rtl/include 
         ./../../system/include 
         ./../../dali/base 
         ./../../system/jlib 
    )

ADD_DEFINITIONS( -DMODULE_PRIORITY=1 -D_CONSOLE )

set ( SRCS fcached.cpp )

add_executable ( fcached ${SRCS} )
install ( TARGETS fcached DESTINATION ${OSSDIR}/bin )


