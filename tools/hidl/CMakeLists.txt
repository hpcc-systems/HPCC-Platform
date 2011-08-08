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

# Component: hidl 
#####################################################
# Description:
# ------------
#    Cmake Input File for hidl
#####################################################

project( hidl ) 

add_custom_command ( OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/hidlgram.cpp ${CMAKE_CURRENT_BINARY_DIR}/hidlgram.h
    COMMAND ${bisoncmdprefix} ${bisoncmd} --defines=${CMAKE_CURRENT_BINARY_DIR}/hidlgram.h --output=${CMAKE_CURRENT_BINARY_DIR}/hidlgram.cpp ${CMAKE_CURRENT_SOURCE_DIR}/hidlgram.y
    DEPENDS hidlgram.y
)

add_custom_command ( OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/hidllex.cpp
    COMMAND ${flexcmdprefix} ${flexcmd} --outfile=${CMAKE_CURRENT_BINARY_DIR}/hidllex.cpp ${CMAKE_CURRENT_SOURCE_DIR}/hidllex.l
    DEPENDS hidllex.l
)

set (    SRCS 
         ${CMAKE_CURRENT_BINARY_DIR}/hidlgram.cpp
         ${CMAKE_CURRENT_BINARY_DIR}/hidllex.cpp
         hidl_esp_ng.cpp 
         hidl_utils.cpp 
         hidlcomp.cpp 
         main.cpp 
    )

include_directories ( 
         ${CMAKE_CURRENT_BINARY_DIR}   # for generated .h file
         ${HPCC_SOURCE_DIR}/system/include 
         ${CMAKE_CURRENT_SOURCE_DIR}
    )

set_source_files_properties (${CMAKE_CURRENT_BINARY_DIR}/hidlgram.cpp PROPERTIES COMPILE_FLAGS -fno-strict-aliasing)

ADD_DEFINITIONS( -D_CONSOLE)
add_executable ( hidl ${SRCS} )
#install ( TARGETS hidl DESTINATION ${OSSDIR}/bin )
