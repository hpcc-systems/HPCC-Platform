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


# Component: dfuwu 

#####################################################
# Description:
# ------------
#    Cmake Input File for dfuwu
#####################################################


project( dfuwu ) 

set (    SRCS 
         dfuwu.cpp 
    )

include_directories ( 
         ./../../common/remote 
         ./../../system/mp 
         ./../base 
         ./../../system/include 
         ./../../system/jlib 
         ./../../common/workunit 
    )

HPCC_ADD_LIBRARY( dfuwu SHARED ${SRCS} )
set_target_properties ( dfuwu PROPERTIES 
        COMPILE_FLAGS "-DLOGMSGCOMPONENT=3 -D_USRDLL"
        DEFINE_SYMBOL DALI_EXPORTS 
        )
install ( TARGETS dfuwu DESTINATION ${OSSDIR}/lib )
target_link_libraries ( dfuwu 
         workunit
         jlib
         mp 
         hrpc 
         remote 
         dalibase 
    )

