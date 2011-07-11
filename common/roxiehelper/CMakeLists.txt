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


# Component: roxiehelper 
#####################################################
# Description:
# ------------
#    Cmake Input File for roxiehelper
#####################################################

project( roxiehelper ) 

include_directories (            ./../../system/security/securesocket 
         ./../../common/remote 
         ./../../system/jhtree 
         ./../../rtl/eclrtl 
         ./../../common/thorhelper 
         ./../../roxie/roxiemem 
         ./../../roxie/roxie
         ./../../roxie/ccd
         ./../../system/include 
         ./../workunit 
         ./../../system/jlib 
         ./../deftype 
         ./../../system/icu/include 
         ./../../rtl/include
         ./../../system/mp 
         ./../../dali/base 
    )

ADD_DEFINITIONS( -DROXIEHELPER_EXPORTS -D_USRDLL -DMODULE_PRIORITY=5 )

set ( SRCS roxiehelper.cpp roxiedebug.cpp roxiehelper.hpp roxiedebug.hpp roxielmj.hpp)

HPCC_ADD_LIBRARY( roxiehelper SHARED ${SRCS} )
install ( TARGETS roxiehelper DESTINATION ${OSSDIR}/lib )
target_link_libraries ( roxiehelper
         jlib 
         eclrtl 
         thorhelper 
         roxiemem 
         dalibase 
 ) 

