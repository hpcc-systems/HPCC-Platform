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

add_custom_command ( OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/esdlgram.cpp ${CMAKE_CURRENT_BINARY_DIR}/esdlgram.h
    COMMAND ${bisoncmdprefix} ${bisoncmd} --defines=${CMAKE_CURRENT_BINARY_DIR}/esdlgram.h --output=${CMAKE_CURRENT_BINARY_DIR}/esdlgram.cpp ${CMAKE_CURRENT_SOURCE_DIR}/esdlgram.y
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/esdlgram.y
)

add_custom_command ( OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/esdllex.cpp 
    COMMAND ${flexcmdprefix} ${flexcmd} --outfile=${CMAKE_CURRENT_BINARY_DIR}/esdllex.cpp ${CMAKE_CURRENT_SOURCE_DIR}/esdllex.l
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/esdllex.l
)

set ( SRCS 
        ${CMAKE_CURRENT_BINARY_DIR}/esdlgram.cpp 
        ${CMAKE_CURRENT_BINARY_DIR}/esdllex.cpp 
        ${CMAKE_CURRENT_SOURCE_DIR}/main.cpp 
        ${CMAKE_CURRENT_SOURCE_DIR}/esdlcomp.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/esdl_utils.cpp 
    )

include_directories ( 
         ${CMAKE_CURRENT_BINARY_DIR}   # for generated .h file
         ${HPCC_SOURCE_DIR}/system/include 
         ${CMAKE_CURRENT_SOURCE_DIR}
    )

ADD_DEFINITIONS( -D_CONSOLE -O0 )

add_executable ( esdl ${SRCS} )
#install ( TARGETS esdl DESTINATION ${OSSDIR}/bin )

