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


# Component: workunitservices 
#####################################################
# Description:
# ------------
#    Cmake Input File for workunitservices
#####################################################

project( workunitservices ) 

set (    SRCS 
         workunitservices.cpp 
         ../../dali/sasha/sacmd.cpp 
    )

include_directories ( 
         ./../../common/remote 
         ./../../system/mp 
         ./../../common/workunit 
         ./../../system/include 
         ./../../dali/base 
         ./../../rtl/include 
         ./../../ecl/eclagent 
         ./../../common/thorhelper 
         ./../../common/deftype 
         ./../../rtl/eclrtl 
         ./../../system/jhtree 
         ./../../system/jlib 
         ./../../dali/sasha 
    )

ADD_DEFINITIONS( -D_USRDLL -DWORKUNITSERVICES_EXPORTS )

HPCC_ADD_LIBRARY( workunitservices SHARED ${SRCS} )
install ( TARGETS workunitservices DESTINATION ${OSSDIR}/plugins )
target_link_LIBRARies ( workunitservices 
         jlib
         mp 
         hrpc 
         remote 
         dalibase 
         environment 
         workunit 
    )


