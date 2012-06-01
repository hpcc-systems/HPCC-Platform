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


# Component: wuwebview
#####################################################
# Description:
# ------------
#    Cmake Input File for wuwebview
#####################################################

project( wuwebview )

set (    SRCS
         wuwebview.cpp
         wuwebview.hpp
         wuweberror.hpp
    )

include_directories (
         ./../../common/workunit
         ./../../common/fileview2
         ./../../system/include
         ./../../common/dllserver
         ./../../system/jlib
         ./../../system/xmllib
    )

if ( NOT CLIENTTOOLS_ONLY )
    HPCC_ADD_LIBRARY( wuwebview SHARED ${SRCS} )
    set_target_properties(wuwebview PROPERTIES
        COMPILE_FLAGS -D_USRDLL
        DEFINE_SYMBOL WUWEBVIEW_EXPORTS )
    install ( TARGETS wuwebview DESTINATION ${OSSDIR}/lib )
    target_link_libraries ( wuwebview
         jlib
         workunit
         fileview2
         dllserver
         xmllib
    )
endif()
