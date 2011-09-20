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


# Component: eclcc 
#####################################################
# Description:
# ------------
#    Cmake Input File for eclcc
#####################################################


project( eclcc ) 

set (    SRCS 
         eclcc.cpp 
    )

include_directories ( 
         ${CMAKE_BINARY_DIR}
         ${CMAKE_BINARY_DIR}/oss
         ./../../ecl/hqlcpp 
         ./../../common/workunit 
         ./../../common/deftype 
         ./../../system/include 
         ./../../ecl/hql 
         ./../../rtl/include 
         ./../../system/jlib 
         ./../../common/commonext 
    )

ADD_DEFINITIONS( -D_CONSOLE )

#The transformations can use a lot of stack space processing very deep graphs
if (WIN32)
    set (CMAKE_EXE_LINKER_FLAGS "/STACK:10000000 ${CMAKE_EXE_LINKER_FLAGS}")
endif()

add_executable ( eclcc ${SRCS} )
install ( TARGETS eclcc DESTINATION ${OSSDIR}/bin )
target_link_libraries ( eclcc 
         jlib
         nbcd 
         eclrtl 
         deftype 
         hql 
         commonext 
         remote 
         dalibase 
         environment 
         dllserver 
         workunit 
         thorhelper 
         hqlcpp 
    )

