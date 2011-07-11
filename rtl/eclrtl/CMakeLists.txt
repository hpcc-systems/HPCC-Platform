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

# Component: eclrtl 

#####################################################
# Description:
# ------------
#    Cmake Input File for eclrtl
#####################################################


project( eclrtl ) 

set (    SRCS 
         eclrtl.cpp 
         rtldistr.cpp 
         rtlds.cpp 
         rtlint.cpp 
         rtlkey.cpp 
         rtlqstr.cpp 
         rtlrank.cpp 
         rtlfield.cpp 
         rtlread.cpp 
         rtltype.cpp 
         rtlxml.cpp 
         workflow.cpp 
    )

include_directories ( 
         ./../../rtl/include 
         ./../../rtl/nbcd 
         ./../../system/include 
         ./../../common/workunit
         ./../../system/jlib 
         ${ICU_INCLUDE_DIR}
         ${BOOST_REGEX_INCLUDE_DIR}
    )

ADD_DEFINITIONS( -D_USRDLL -DECLRTL_EXPORTS )

if (WIN32)
else ()
    ADD_DEFINITIONS( -DBOOST_DYN_LINK )
endif ()

IF (BOOST_REGEX_FOUND)
  link_directories(${BOOST_REGEX_LIBRARY_DIR})
ENDIF()

HPCC_ADD_LIBRARY( eclrtl SHARED ${SRCS} )
install ( TARGETS eclrtl DESTINATION ${OSSDIR}/lib )
target_link_libraries ( eclrtl 
      ${BOOST_REGEX_LIBRARIES}
      ${ICU_LIBRARIES}
      jlib
      nbcd 
    )


