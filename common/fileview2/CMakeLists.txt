################################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
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
         ${CMAKE_BINARY_DIR}
         ${CMAKE_BINARY_DIR}/oss
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
         ./../../common/thorhelper
    )

HPCC_ADD_LIBRARY( fileview2 SHARED ${SRCS} )
set_target_properties(fileview2 PROPERTIES 
    COMPILE_FLAGS -D_USRDLL
    DEFINE_SYMBOL FILEVIEW2_EXPORTS )
install ( TARGETS fileview2 RUNTIME DESTINATION ${EXEC_DIR} LIBRARY DESTINATION ${LIB_DIR} )
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
    HPCC_ADD_EXECUTABLE ( fvserver ${SRCS} )
    set_target_properties(fvserver PROPERTIES 
        COMPILE_FLAGS -D_CONSOLE
    )
    #install ( TARGETS fvserver RUNTIME DESTINATION ${EXEC_DIR} ) #not used at present

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


