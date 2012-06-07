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


# File      : CMakeLists.txt
# Component: fileview2 
#####################################################
# Description:
# ------------
#    Cmake Input File for fileview2 and fvserver
#####################################################

cmake_policy( SET CMP0011 NEW )

project( fileview2 ) 

set (    SRCS 
         fvdisksource.cpp 
         fvdsremote.cpp 
         fvidxsource.cpp 
         fvquerysource.cpp 
         fvrelate.cpp 
         fvresultset.cpp 
         fvsource.cpp 
         fvtransform.cpp 
         fvwugen.cpp 
         fvwusource.cpp 
         
         fvdisksource.ipp 
         fvdsremote.ipp 
         fvidxsource.ipp 
         fvquerysource.ipp 
         fvrelate.ipp 
         fvresultset.ipp 
         fvsource.ipp 
         fvtransform.ipp
         fvwugen.ipp
         fvwusource.ipp 
         
                 fileview.hpp 
         fvdatasource.hpp 
         fverror.hpp 
         fvrelate.hpp 
         fvwugen.hpp 
                 
         sourcedoc.xml
    )

include_directories ( 
         ./../../system/mp 
         ./../../system/jhtree 
         ./../dllserver 
         ./../../rtl/eclrtl 
         ./../../rtl/include 
         ./../../rtl/nbcd 
         ./../../system/include 
         ./../../system/security/shared
         ./../../dali/base 
         ./../../system/jlib 
         ./../deftype 
         ./../../ecl/hql 
         ./../remote 
         ./../../common/workunit 
         ./../../common/environment 
    )

HPCC_ADD_LIBRARY( fileview2 SHARED ${SRCS} )
set_target_properties(fileview2 PROPERTIES 
    COMPILE_FLAGS -D_USRDLL
    DEFINE_SYMBOL FILEVIEW2_EXPORTS )
install ( TARGETS fileview2 DESTINATION ${OSSDIR}/lib )
target_link_libraries ( fileview2 
         jlib 
         mp 
         hrpc 
         remote 
         nbcd 
         eclrtl 
         jhtree 
         dalibase 
         environment 
         dllserver 
         deftype 
         workunit 
         thorhelper
         hql 
    )

set (    SRCS 
         fvserver.cpp 
    )

ADD_DEFINITIONS ( -D_CONSOLE )

if( PLATFORM )
    add_executable ( fvserver ${SRCS} )
    set_target_properties(fvserver PROPERTIES 
        COMPILE_FLAGS -D_CONSOLE
    )
    #install ( TARGETS fvserver DESTINATION ${OSSDIR}/bin ) #not used at present

    target_link_libraries ( fvserver
         jlib 
         mp 
         hrpc 
         remote 
         dalibase 
         environment 
         dllserver 
         jhtree 
         nbcd 
         eclrtl 
         deftype 
         workunit 
         hql 
         fileview2 
    )
endif()


