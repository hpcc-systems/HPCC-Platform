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

# Component: esdl 

#####################################################
# Description:
# ------------
#    Cmake Input File for esdl
#####################################################


project( esdl ) 

include_directories ( 
         ./../../system/include 
         ./../../system/jlib 
    )

ADD_DEFINITIONS ( -D_CONSOLE )

add_custom_command ( OUTPUT esdlgram.cpp esdlgram.h
    COMMAND ../pcyacc/pcyacc -v -Desdlgram.h -Cesdlgram.cpp hidlgram.y
    DEPENDS esdlgram.h
)

add_custom_command ( OUTPUT esdllex.cpp 
    COMMAND ../pcyacc/pclex -i -Cesdllex.cpp esdllex.l
    DEPENDS esdllex.l
)

set ( SRCS esdlgram.cpp esdllex.cpp main.cpp esdlcomp.cpp esdl_utils.cpp )
# esdlgram.y esdllex.l main.cpp esdlcomp.cpp esdl_utils.cpp 
add_executable ( esdl ${SRCS} )
install ( TARGETS esdl DESTINATION ${OSSDIR}/bin )
#add_dependencies ( esdl esdlgram.cpp esdlgram.h esdllex.cpp )

