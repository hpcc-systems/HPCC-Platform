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

# Component: swapnodelib
#####################################################
# Description:
# ------------
#    Cmake Input File for swapnodelib
#####################################################

project( swapnodelib )

set (    SRCS
         swapnodelib.cpp
    )

include_directories (
         ./../../common/remote
         ./../../system/mp
         ./../../system/include
         ./../../dali/base
         ./../../system/jlib
         ./../../common/environment
         ./../../common/workunit
    )

HPCC_ADD_LIBRARY( swapnodelib SHARED ${SRCS} )
set_target_properties(swapnodelib PROPERTIES
    COMPILE_FLAGS -D_USRDLL
    DEFINE_SYMBOL SWAPNODELIB_EXPORTS )
install ( TARGETS swapnodelib DESTINATION ${OSSDIR}/lib )
target_link_libraries ( swapnodelib
         jlib
         remote
         dalibase
         workunit
         environment
    )
