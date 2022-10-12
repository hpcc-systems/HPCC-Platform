################################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.
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


# Component: wutool
#####################################################
# Description:
# ------------
#    Cmake Input File for wutool
#####################################################

project( wutool ) 

set (    SRCS 
         wutool.cpp 
    )

include_directories ( 
         ./../../system/mp 
         ./../../common/dllserver 
         ./../../system/include 
         ./../../dali/base 
         ./../../system/jlib 
         ./../../common/environment 
         ./../../common/workunit 
         ./../../common/wuanalysis
         ./../../common/thorhelper
         ./../../rtl/include
         ./../../testing/unittests
         ./../../system/security/shared
    )

if ( USE_CPPUNIT )
  include_directories(
         ./../../plugins/workunitservices
         ./../../rtl/include
         ./../../rtl/eclrtl
    )
endif()

ADD_DEFINITIONS( -D_CONSOLE )

HPCC_ADD_EXECUTABLE ( wutool ${SRCS} )
target_link_libraries ( wutool 
         jlib
         remote 
         dalibase 
         dllserver 
         nbcd 
         eclrtl 
         deftype
         workunit
         wuanalysis
         ${CPPUNIT_LIBRARIES}
    )
    
if ( USE_CPPUNIT )
  target_link_libraries ( wutool workunitservices )
endif()

install ( TARGETS wutool RUNTIME DESTINATION ${EXEC_DIR} )
