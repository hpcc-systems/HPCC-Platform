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


# Component: archivefile
#####################################################
# Description:
# ------------
#    Cmake Input File for archivefile
#####################################################

project( archivefile )

set (    SRCS
         archive.cpp
         archive.hpp
    )

include_directories (
         ${HPCC_SOURCE_DIR}/system/include
         ${HPCC_SOURCE_DIR}/system/jlib
    )

include_directories ( ${LIBARCHIVE_INCLUDE_DIR} )

ADD_DEFINITIONS( -D_USRDLL -DARCHIVEFILE_EXPORTS )

if ( NOT CLIENTTOOLS_ONLY )
    HPCC_ADD_LIBRARY( archivefile SHARED ${SRCS}  )
    install ( TARGETS archivefile DESTINATION ${OSSDIR}/filehooks )

    target_link_libraries ( archivefile
        jlib
        archive
    )
endif()
