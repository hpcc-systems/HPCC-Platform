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

# Component: dfuserver 

#####################################################
# Description:
# ------------
#    Cmake Input File for dfuserver
#####################################################


project( dfuserver ) 

set (    SRCS 
         dfurun.cpp 
         dfurunkdp.cpp 
         dfuserver.cpp 
         dfurepl.cpp 
    )

include_directories ( 
         ./../../common/remote 
         ./../../system/mp 
         ./../../system/jhtree 
         ./../../rtl/eclrtl 
         ./../../ecl/schedulectrl 
         ./../../rtl/include 
         ./../base 
         ./../../system/include 
         ./../../system/jlib 
         ./../ft 
         ./../../common/environment 
         ./../../common/workunit 
    )

if ( NOT CLIENTTOOLS_ONLY )
    add_executable ( dfuserver ${SRCS} )
    set_target_properties ( dfuserver PROPERTIES 
        COMPILE_FLAGS "-D_CONSOLE -D_DFUSERVER"
        )
    install ( TARGETS dfuserver DESTINATION ${OSSDIR}/bin )
    target_link_libraries ( dfuserver 
         jlib
         mp 
         hrpc 
         remote 
         dalibase 
         environment 
         dllserver 
         nbcd 
         eclrtl 
         deftype 
         workunit 
         schedulectrl 
         dalift 
         jhtree 
         dfuwu 
    )
endif()

