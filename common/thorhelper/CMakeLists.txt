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


# Component: thorhelper 

#####################################################
# Description:
# ------------
#    Cmake Input File for thorhelper
#####################################################

project( thorhelper ) 

set (    SRCS 
         csvsplitter.cpp 
         layouttrans.cpp 
         thorcommon.cpp 
         thorfile.cpp 
         thorparse.cpp 
         thorpipe.cpp 
         thorrparse.cpp 
         thorsoapcall.cpp 
         thorstep.cpp 
         thorstep2.cpp 
         thortalgo.cpp 
         thortlex.cpp 
         thortparse.cpp 
         thorxmlread.cpp 
         thorxmlwrite.cpp 
         
         csvsplitter.hpp 
         layouttrans.hpp 
         thorcommon.hpp 
         thorfile.hpp 
         thorparse.hpp 
         thorpipe.hpp 
         thorrparse.hpp 
         thorsoapcall.hpp 
         thorstep.hpp 
         thorxmlread.hpp 
         thorxmlwrite.hpp 
                 
    )

include_directories ( 
         ./../../system/security/securesocket 
         ./../../common/remote 
         ./../../system/jhtree 
         ./../../rtl/eclrtl 
         ./../../rtl/nbcd 
         ./../../system/include 
         ./../workunit 
         ./../../system/jlib 
         ./../deftype 
         ./../../system/icu/include 
         ./../../rtl/include 
    )

ADD_DEFINITIONS( -DTHORHELPER_EXPORTS -D_USRDLL )

HPCC_ADD_LIBRARY( thorhelper SHARED ${SRCS} )
install ( TARGETS thorhelper DESTINATION ${OSSDIR}/lib )
target_link_libraries ( thorhelper 
         jlib 
         nbcd 
         eclrtl 
         deftype 
         securesocket 
    )


