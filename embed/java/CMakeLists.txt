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


# Component: javaembed

project( javaembed )

find_package(JNI)

set (    SRCS
         javaembed.cpp
         javaembed.hpp
    )

include_directories (
         ${HPCC_SOURCE_DIR}/system/include
         ${HPCC_SOURCE_DIR}/system/jlib
    )

include_directories ( ${JNI_INCLUDE_DIRS} )

ADD_DEFINITIONS( -D_USRDLL -DJAVAEMBED_EXPORTS )

HPCC_ADD_LIBRARY( javaembed SHARED ${SRCS}  )
install ( TARGETS javaembed DESTINATION embedhooks )

target_link_libraries ( javaembed
    jlib
    ${JNI_LIBRARIES}
    )
