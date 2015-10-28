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


# Component: wutest 
#####################################################
# Description:
# ------------
#    Cmake Input File for wutest
#####################################################

project( wutest ) 

set (    SRCS 
         wutest.cpp 
    )

include_directories ( 
         ./../../system/mp 
         ./../../common/dllserver 
         ./../../system/include 
         ./../../dali/base 
         ./../../system/jlib 
         ./../../common/environment 
         ./../../common/workunit 
         ./../../testing/unittests 
    )

if ( USE_CPPUNIT )
  include_directories(
         ./../../plugins/workunitservices
         ./../../rtl/include
         ./../../rtl/eclrtl
    )
endif()

ADD_DEFINITIONS( -D_CONSOLE )
if ( FORCE_WORKUNITS_TO_CASSANDRA )
  ADD_DEFINITIONS( -DFORCE_WORKUNITS_TO_CASSANDRA )
endif()


HPCC_ADD_EXECUTABLE ( wutest ${SRCS} )
target_link_libraries ( wutest 
         jlib
         remote 
         dalibase 
         environment 
         dllserver 
         nbcd 
         eclrtl 
         deftype
         workunit 
         ${CPPUNIT_LIBRARIES}
    )
    
if ( FORCE_WORKUNITS_TO_CASSANDRA )
  target_link_libraries ( wutest cassandraembed )
endif ()

if ( USE_CPPUNIT )
  target_link_libraries ( wutest workunitservices )
endif()