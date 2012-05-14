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


# Component: workunit 
#####################################################
# Description:
# ------------
#    Cmake Input File for workunit
#####################################################

project( workunit ) 

set (    SRCS 
         workunit.cpp 
         wujobq.cpp
                 
         workunit.hpp 
         wuerror.hpp
         wujobq.hpp
    )

include_directories ( 
         ./../../common/workunit
         ./../../common/remote
         ./../../system/mp 
         ./../../dali/ft 
         ./../../common/deftype 
         ./../../system/include 
         ./../../dali/base 
         ./../../rtl/include 
         ./../../common/dllserver 
         ./../../system/jlib
         ./../../system/security/shared 
         ./../../common/thorhelper 
         ./../../rtl/eclrtl 
         ./../../rtl/nbcd
    )

HPCC_ADD_LIBRARY( workunit SHARED ${SRCS} )
set_target_properties(workunit PROPERTIES 
    COMPILE_FLAGS -D_USRDLL
    DEFINE_SYMBOL WORKUNIT_EXPORTS )
install ( TARGETS workunit DESTINATION ${OSSDIR}/lib )
target_link_libraries ( workunit 
         jlib 
         dalibase 
         dllserver 
         nbcd 
         eclrtl 
         deftype 
    )


